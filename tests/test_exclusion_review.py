from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from evidence_review.clock import FrozenClock
from evidence_review.errors import Conflict, Forbidden, NotFound, ServiceError
from evidence_review.jsonio import load_json
from evidence_review.service import EvidenceReviewService
from evidence_review.storage import connect


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)


class ExclusionReviewConcurrencyTests(unittest.TestCase):
    """复核并发语义必须只依赖数据库状态，跨连接与重启保持一致。"""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "reviews.sqlite3"
        connection, service = self._service()
        for user_id, role in (
            ("operator", "operator"),
            ("stat-a", "statistician"),
            ("stat-b", "statistician"),
            ("auditor", "auditor"),
        ):
            service.create_user(user_id, user_id, role)
        service.register_device("operator", "device-a", "A 型", "厂商")
        service.register_build("operator", "build-a", "device-a", "1.0", "b" * 64)
        service.publish_evidence_protocol("stat-a", load_json(ROOT / "fixtures" / "demo_evidence_protocol.json"))
        service.create_batch("operator", "batch-a", "demo-evidence-v1", 1, "build-a")
        service.start_batch("operator", "batch-a", 1)
        rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_evidence_items.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        service.import_evidence_items("operator", "batch-a", "key-1", rows)
        evidence_item_id = connection.execute(
            "SELECT evidence_item_id FROM evidence_items ORDER BY evidence_item_id LIMIT 1"
        ).fetchone()[0]
        self.exclusion_id = service.request_exclusion("operator", evidence_item_id, "现场记录失效")["exclusion_id"]
        connection.close()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _service(self) -> tuple[sqlite3.Connection, EvidenceReviewService]:
        connection = connect(self.database)
        return connection, EvidenceReviewService(connection, FrozenClock(NOW))

    def _review_in_thread(self, results: dict, name: str, actor: str, approve: bool, note: str) -> None:
        connection, service = self._service()
        try:
            try:
                results[name] = ("ok", service.review_exclusion(actor, self.exclusion_id, approve, note))
            except ServiceError as exc:
                results[name] = ("error", exc)
        finally:
            connection.close()

    def _race(self, first: tuple, second: tuple) -> dict:
        results: dict = {}
        threads = [
            threading.Thread(target=self._review_in_thread, args=(results, "first", *first)),
            threading.Thread(target=self._review_in_thread, args=(results, "second", *second)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return results

    def _audit_types(self) -> list[str]:
        connection, _ = self._service()
        try:
            rows = connection.execute(
                "SELECT event_type FROM audit_events WHERE entity_type='exclusion' ORDER BY event_id"
            ).fetchall()
            return [row[0] for row in rows]
        finally:
            connection.close()

    def test_concurrent_reviewers_produce_single_decision(self) -> None:
        results = self._race(("stat-a", True, "证据充分"), ("stat-b", False, "证据不足"))
        outcomes = sorted(name for name, _ in results.values())
        self.assertEqual(outcomes, ["error", "ok"])
        winner = next(payload for kind, payload in results.values() if kind == "ok")
        loser = next(payload for kind, payload in results.values() if kind == "error")
        self.assertEqual(winner["outcome"], "applied")
        self.assertIsInstance(loser, Conflict)

        connection, service = self._service()
        try:
            row = connection.execute(
                "SELECT status, reviewed_by FROM exclusion_requests WHERE exclusion_id=?",
                (self.exclusion_id,),
            ).fetchone()
            self.assertEqual(row["status"], winner["status"])
            # 失败方没有留下任何相反决定的审计事件
            self.assertEqual(self._audit_types(), [f"exclusion.{winner['status']}"])
            attempts = connection.execute(
                "SELECT outcome, count(*) FROM exclusion_review_attempts GROUP BY outcome"
            ).fetchall()
            self.assertEqual(dict(attempts), {"applied": 1, "conflict": 1})
            report = service.report("auditor", "batch-a")
            self.assertEqual(
                report["exclusions"][0]["attempt_counts"], {"applied": 1, "duplicate": 0, "conflict": 1}
            )
            self.assertEqual(
                [attempt["outcome"] for attempt in report["review_attempts"]], ["applied", "conflict"]
            )
        finally:
            connection.close()

    def test_concurrent_identical_retries_share_one_decision(self) -> None:
        results = self._race(("stat-a", True, "证据充分"), ("stat-a", True, "证据充分"))
        kinds = sorted(payload["outcome"] for kind, payload in results.values() if kind == "ok")
        self.assertEqual(kinds, ["applied", "duplicate"])
        statuses = {payload["status"] for kind, payload in results.values() if kind == "ok"}
        self.assertEqual(statuses, {"approved"})
        self.assertEqual(self._audit_types(), ["exclusion.approved"])

    def test_identical_retry_after_restart_returns_effective_decision(self) -> None:
        connection, service = self._service()
        applied = service.review_exclusion("stat-a", self.exclusion_id, True, "证据充分")
        self.assertEqual(applied["outcome"], "applied")
        connection.close()

        # 模拟进程重启：全新连接上重放完全相同的请求
        connection, service = self._service()
        try:
            replayed = service.review_exclusion("stat-a", self.exclusion_id, True, "证据充分")
            self.assertEqual(replayed["outcome"], "duplicate")
            self.assertEqual(replayed["status"], applied["status"])
            # 变更任何字段的重试都是竞争请求
            with self.assertRaises(Conflict):
                service.review_exclusion("stat-a", self.exclusion_id, False, "证据充分")
            with self.assertRaises(Conflict):
                service.review_exclusion("stat-a", self.exclusion_id, True, "改动的备注")
            with self.assertRaises(Conflict):
                service.review_exclusion("stat-b", self.exclusion_id, True, "证据充分")
            self.assertEqual(self._audit_types(), ["exclusion.approved"])
            report = service.report("auditor", "batch-a")
            self.assertEqual(
                report["exclusions"][0]["attempt_counts"], {"applied": 1, "duplicate": 1, "conflict": 3}
            )
        finally:
            connection.close()

    def test_losing_request_leaves_no_false_audit(self) -> None:
        connection, service = self._service()
        try:
            service.review_exclusion("stat-a", self.exclusion_id, True, "证据充分")
            with self.assertRaises(Conflict):
                service.review_exclusion("stat-b", self.exclusion_id, False, "证据不足")
            row = connection.execute(
                "SELECT status, reviewed_by FROM exclusion_requests WHERE exclusion_id=?",
                (self.exclusion_id,),
            ).fetchone()
            self.assertEqual(dict(row), {"status": "approved", "reviewed_by": "stat-a"})
            self.assertEqual(self._audit_types(), ["exclusion.approved"])
        finally:
            connection.close()

    def test_review_guards_still_hold(self) -> None:
        connection, service = self._service()
        try:
            with self.assertRaises(NotFound):
                service.review_exclusion("stat-a", 9999, True, "不存在")
            with self.assertRaises(Forbidden):
                service.review_exclusion("operator", self.exclusion_id, True, "自我复核")
            # 未决申请上的越权尝试不会留下尝试记录
            count = connection.execute("SELECT count(*) FROM exclusion_review_attempts").fetchone()[0]
            self.assertEqual(count, 0)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
