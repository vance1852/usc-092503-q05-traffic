from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path

from evidence_review.api import JsonApplication
from evidence_review.jsonio import load_json
from evidence_review.service import EvidenceReviewService


ROOT = Path(__file__).resolve().parents[1]


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.app = JsonApplication(EvidenceReviewService(self.connection))

    def tearDown(self) -> None:
        self.connection.close()

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")

    def test_json_error_shape(self) -> None:
        response = self.app.handle("POST", "/users", body=b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")

    def test_user_route(self) -> None:
        payload = json.dumps({"user_id": "u1", "display_name": "操作员", "role": "operator"}).encode()
        response = self.app.handle("POST", "/users", body=payload)
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["role"], "operator")

    def _prepare_exclusion(self) -> int:
        service = self.app.service
        service.create_user("operator", "操作员", "operator")
        service.create_user("stat-a", "复核甲", "statistician")
        service.create_user("stat-b", "复核乙", "statistician")
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
        evidence_item_id = self.connection.execute(
            "SELECT evidence_item_id FROM evidence_items LIMIT 1"
        ).fetchone()[0]
        return service.request_exclusion("operator", evidence_item_id, "现场记录失效")["exclusion_id"]

    def test_exclusion_review_conflict_and_report_over_http(self) -> None:
        exclusion_id = self._prepare_exclusion()
        url = f"/exclusions/{exclusion_id}/review"
        approve = json.dumps({"approve": True, "note": "证据充分"}).encode()
        first = self.app.handle("POST", url, {"X-Actor-Id": "stat-a"}, approve)
        self.assertEqual(first.status, 200)
        self.assertEqual(first.body["outcome"], "applied")
        replay = self.app.handle("POST", url, {"X-Actor-Id": "stat-a"}, approve)
        self.assertEqual(replay.status, 200)
        self.assertEqual(replay.body["outcome"], "duplicate")
        self.assertEqual(replay.body["status"], first.body["status"])
        competing = self.app.handle(
            "POST", url, {"X-Actor-Id": "stat-b"}, json.dumps({"approve": False, "note": "证据不足"}).encode()
        )
        self.assertEqual(competing.status, 409)
        self.assertEqual(competing.body["error"]["code"], "conflict")
        report = self.app.handle("GET", "/batches/batch-a/report", {"X-Actor-Id": "stat-a"})
        self.assertEqual(report.status, 200)
        self.assertEqual(
            [attempt["outcome"] for attempt in report.body["review_attempts"]],
            ["applied", "duplicate", "conflict"],
        )
        self.assertEqual(
            report.body["exclusions"][0]["attempt_counts"], {"applied": 1, "duplicate": 1, "conflict": 1}
        )


if __name__ == "__main__":
    unittest.main()
