from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from evidence_review.clock import FrozenClock
from evidence_review.errors import ReviewConflict
from evidence_review.jsonio import load_json
from evidence_review.service import EvidenceReviewService
from evidence_review.storage import connect, initialize


ROOT = Path(__file__).resolve().parents[1]


class ReviewConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="review-concurrency-")
        self.database = Path(self.temporary.name) / "review.sqlite3"
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = EvidenceReviewService(connect(self.database), self.clock)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("stat2", "statistician"),
            ("auditor", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        protocol = load_json(ROOT / "fixtures" / "demo_evidence_protocol.json")
        rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_evidence_items.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.service.register_device("operator", "device-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "device-a", "1.0", "b" * 64)
        self.service.publish_evidence_protocol("stat", protocol)
        self.service.create_batch("operator", "batch-a", "demo-evidence-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)
        self.service.import_evidence_items("operator", "batch-a", "key-1", rows)
        self.evidence_item_id = self.service.connection.execute(
            "SELECT evidence_item_id FROM evidence_items ORDER BY evidence_item_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", self.evidence_item_id, "现场记录失效")
        self.exclusion_id = requested["exclusion_id"]
        self.service.connection.close()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _open_service(self) -> EvidenceReviewService:
        return EvidenceReviewService(connect(self.database), self.clock)

    def test_identical_network_retry_returns_effective_decision(self) -> None:
        service = self._open_service()
        first = service.review_exclusion("stat", self.exclusion_id, True, "证据充分")
        self.assertEqual(first["status"], "approved")
        self.assertEqual(first["review_outcome"], "effective")

        # 完全相同的网络重试：返回已经生效的决定，而不是再次处理。
        retry = service.review_exclusion("stat", self.exclusion_id, True, "证据充分")
        self.assertEqual(retry["review_outcome"], "replay")
        self.assertEqual(retry["status"], "approved")
        self.assertEqual(retry["decided_by"], "stat")
        self.assertEqual(retry["decided_at"], first["decided_at"])

        decision_events = service.connection.execute(
            "SELECT event_type FROM audit_events WHERE entity_type='exclusion'"
        ).fetchall()
        self.assertEqual([row[0] for row in decision_events], ["exclusion.approved"])
        outcomes = [
            row[0] for row in service.connection.execute(
                "SELECT outcome FROM review_attempts ORDER BY attempt_id"
            ).fetchall()
        ]
        self.assertEqual(outcomes, ["effective", "replay"])
        service.connection.close()

    def test_idempotency_key_replay_reuses_effective_decision(self) -> None:
        service = self._open_service()
        first = service.review_exclusion("stat", self.exclusion_id, False, "初审驳回", idempotency_key="rev-1")
        self.assertEqual(first["review_outcome"], "effective")
        # 同一幂等键即便备注措辞不同，也视为同一次请求的网络重放。
        retry = service.review_exclusion("stat", self.exclusion_id, False, "重试时的备注", idempotency_key="rev-1")
        self.assertEqual(retry["review_outcome"], "replay")
        self.assertEqual(retry["status"], "rejected")
        self.assertEqual(
            service.connection.execute(
                "SELECT count(*) FROM audit_events WHERE entity_type='exclusion'"
            ).fetchone()[0],
            1,
        )
        service.connection.close()

    def test_serial_competing_decisions_second_gets_clear_conflict(self) -> None:
        service = self._open_service()
        approved = service.review_exclusion("stat", self.exclusion_id, True, "批准意见")
        self.assertEqual(approved["status"], "approved")

        with self.assertRaises(ReviewConflict) as caught:
            service.review_exclusion("stat2", self.exclusion_id, False, "相反的驳回意见")
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(caught.exception.code, "review_conflict")
        self.assertEqual(caught.exception.details["existing_status"], "approved")
        self.assertEqual(caught.exception.details["existing_decided_by"], "stat")

        # 申请状态保持有效决定；只有一条决定审计，竞争失败留下台账而非虚假审计。
        status = service.connection.execute(
            "SELECT status FROM exclusion_requests WHERE exclusion_id=?", (self.exclusion_id,)
        ).fetchone()[0]
        self.assertEqual(status, "approved")
        events = [
            row[0] for row in service.connection.execute(
                "SELECT event_type FROM audit_events WHERE entity_type='exclusion' ORDER BY event_id"
            ).fetchall()
        ]
        self.assertEqual(events, ["exclusion.approved"])
        outcomes = [
            (row[0], row[1]) for row in service.connection.execute(
                "SELECT outcome,requested_decision FROM review_attempts ORDER BY attempt_id"
            ).fetchall()
        ]
        self.assertEqual(outcomes, [("effective", "approved"), ("lost_conflict", "rejected")])
        service.connection.close()

    def test_concurrent_decisions_across_connections_yield_single_effective(self) -> None:
        barrier = threading.Barrier(2)
        results: dict[str, object] = {}

        def worker(name: str, approve: bool, note: str) -> None:
            service = self._open_service()
            try:
                barrier.wait()
                results[name] = service.review_exclusion(name, self.exclusion_id, approve, note)
            except ReviewConflict as exc:
                results[name] = exc
            except BaseException as exc:  # 记录锁错误等意外
                results[name] = exc
            finally:
                service.connection.close()

        threads = [
            threading.Thread(target=worker, args=("stat", True, "并发批准")),
            threading.Thread(target=worker, args=("stat2", False, "并发驳回")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        effective_actors = [
            name for name, result in results.items()
            if isinstance(result, dict) and result["review_outcome"] == "effective"
        ]
        conflicts = [name for name, result in results.items() if isinstance(result, ReviewConflict)]
        self.assertEqual(len(effective_actors), 1, results)
        self.assertEqual(len(conflicts), 1, results)

        service = self._open_service()
        status = service.connection.execute(
            "SELECT status FROM exclusion_requests WHERE exclusion_id=?", (self.exclusion_id,)
        ).fetchone()[0]
        winner = effective_actors[0]
        self.assertEqual(status, "approved" if winner == "stat" else "rejected")

        # 数据库层面只有一个有效决定、一条决定审计。
        self.assertEqual(
            service.connection.execute("SELECT count(*) FROM review_decisions").fetchone()[0], 1
        )
        audit_rows = service.connection.execute(
            "SELECT event_type FROM audit_events WHERE entity_type='exclusion'"
        ).fetchall()
        self.assertEqual(len(audit_rows), 1)
        self.assertEqual(audit_rows[0][0], f"exclusion.{status}")

        outcomes = {
            row[0]: row[1] for row in service.connection.execute(
                "SELECT actor_id,outcome FROM review_attempts"
            ).fetchall()
        }
        self.assertEqual(outcomes[winner], "effective")
        self.assertEqual(outcomes[conflicts[0]], "lost_conflict")

        # 竞争失败响应中的现有决定必须与真实有效决定一致。
        lost_error = results[conflicts[0]]
        assert isinstance(lost_error, ReviewConflict)
        self.assertEqual(lost_error.details["existing_status"], status)
        self.assertEqual(lost_error.details["existing_decided_by"], winner)
        service.connection.close()

    def test_state_survives_process_restart_and_report_distinguishes_outcomes(self) -> None:
        service = self._open_service()
        service.review_exclusion("stat", self.exclusion_id, True, "生效批准")
        service.review_exclusion("stat", self.exclusion_id, True, "生效批准")  # 重试
        with self.assertRaises(ReviewConflict):
            service.review_exclusion("stat2", self.exclusion_id, False, "竞争驳回")
        service.connection.close()

        # 模拟进程重启：重新打开同一个数据库文件。
        restarted = self._open_service()
        retry = restarted.review_exclusion("stat", self.exclusion_id, True, "生效批准")
        self.assertEqual(retry["review_outcome"], "replay")
        self.assertEqual(retry["status"], "approved")
        with self.assertRaises(ReviewConflict):
            restarted.review_exclusion("stat2", self.exclusion_id, False, "重启后竞争驳回")

        report = restarted.report("auditor", "batch-a")
        self.assertEqual(report["review_summary"], {"effective": 1, "replay": 2, "lost_conflict": 2})
        entry = next(
            item for item in report["exclusions"] if item["exclusion_id"] == self.exclusion_id
        )
        self.assertEqual(entry["status"], "approved")
        self.assertEqual(entry["effective_decision"]["decision"], "approved")
        self.assertEqual(entry["effective_decision"]["decided_by"], "stat")
        self.assertEqual(
            [attempt["outcome"] for attempt in entry["review_attempts"]],
            ["effective", "replay", "lost_conflict", "replay", "lost_conflict"],
        )
        restarted.connection.close()

    def test_legacy_decided_rows_are_backfilled_on_initialize(self) -> None:
        service = self._open_service()
        # 模拟旧版本（schema v2）留下的终态：只翻转了状态、没有裁决表记录。
        service.connection.execute(
            "UPDATE exclusion_requests SET status='approved',reviewed_by='stat',"
            "reviewed_at=?,review_note='旧版本批准' WHERE exclusion_id=?",
            ("2026-09-24T08:05:00+00:00", self.exclusion_id),
        )
        service.connection.commit()
        service.connection.close()

        legacy = connect(self.database)
        try:
            initialize(legacy)
            decision = legacy.execute(
                "SELECT decision,decided_by,note FROM review_decisions WHERE exclusion_id=?",
                (self.exclusion_id,),
            ).fetchone()
            self.assertIsNotNone(decision)
            self.assertEqual((decision["decision"], decision["decided_by"], decision["note"]),
                             ("approved", "stat", "旧版本批准"))
            outcomes = [
                row[0] for row in legacy.execute(
                    "SELECT outcome FROM review_attempts WHERE exclusion_id=?",
                    (self.exclusion_id,),
                ).fetchall()
            ]
            self.assertEqual(outcomes, ["effective"])
            version = legacy.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()[0]
            self.assertEqual(version, "3")
        finally:
            legacy.close()


if __name__ == "__main__":
    unittest.main()
