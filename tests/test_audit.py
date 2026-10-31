"""Unit/integration tests for etl_audit.py against the disposable warehouse.

Covers: idempotent schema creation, the RUNNING -> terminal lifecycle,
double-close protection, credential sanitisation, the concurrency lock,
watermark upserts, and rejection of invalid values.
"""

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ["DW_NAME"] = "aviation_dw_ci"

from tests import dbutil  # noqa: E402

import etl_audit  # noqa: E402
from sqlalchemy import text  # noqa: E402


class TestAudit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()

    # ---------- schema ----------

    def test_ensure_schema_is_idempotent(self):
        etl_audit.ensure_schema()
        etl_audit.ensure_schema()
        self.assertTrue(etl_audit.audit_table_exists())

    # ---------- run lifecycle ----------

    def test_start_run_inserts_running_row(self):
        info = etl_audit.start_run("csv")
        row = self._fetch(info["run_id"])
        self.assertEqual(row["status"], "RUNNING")
        self.assertEqual(row["source_name"], "csv")
        self.assertEqual(len(row["batch_guid"]), 36)
        self.assertIsNotNone(row["started_at"])
        self.assertIsNone(row["finished_at"])
        self.assertEqual(row["engine_version"] is not None, True)

    def test_finish_run_writes_terminal_state_and_metrics(self):
        info = etl_audit.start_run("oltp")
        etl_audit.finish_run(
            info["run_id"],
            "SUCCESS",
            rows_read=8,
            rows_before=8,
            rows_after=8,
            rows_inserted=0,
            rows_updated=2,
            rows_skipped=0,
            notes="all good",
        )
        row = self._fetch(info["run_id"])
        self.assertEqual(row["status"], "SUCCESS")
        self.assertIsNotNone(row["finished_at"])
        self.assertEqual(row["rows_read"], 8)
        self.assertEqual(row["rows_updated"], 2)
        self.assertEqual(row["notes"], "all good")
        self.assertIsNone(row["error_detail"])

    def test_finish_run_twice_is_rejected(self):
        info = etl_audit.start_run("csv")
        etl_audit.finish_run(info["run_id"], "SUCCESS")
        with self.assertRaises(etl_audit.AuditError):
            etl_audit.finish_run(info["run_id"], "FAILED")
        self.assertEqual(self._fetch(info["run_id"])["status"], "SUCCESS")

    def test_invalid_terminal_status_rejected(self):
        info = etl_audit.start_run("csv")
        with self.assertRaises(etl_audit.AuditError):
            etl_audit.finish_run(info["run_id"], "RUNNING")
        with self.assertRaises(etl_audit.AuditError):
            etl_audit.finish_run(info["run_id"], "BANANA")

    def test_unknown_source_rejected(self):
        with self.assertRaises(etl_audit.AuditError):
            etl_audit.start_run("definitely_not_a_source")

    def test_failed_stage_error_is_sanitised_and_persisted(self):
        info = etl_audit.start_run("weather")
        etl_audit.finish_run(
            info["run_id"],
            "FAILED",
            error="connect failed: mysql://admin:SuperSecret1@host/db",
        )
        stored = self._fetch(info["run_id"])["error_detail"]
        self.assertNotIn("SuperSecret1", stored)
        self.assertIn("***", stored)

    def test_recent_runs_filter_by_source(self):
        a = etl_audit.start_run("csv")
        etl_audit.finish_run(a["run_id"], "SUCCESS")
        b = etl_audit.start_run("weather")
        etl_audit.finish_run(b["run_id"], "FAILED", error="x")
        csv_runs = etl_audit.recent_runs("csv")
        self.assertEqual([r["run_id"] for r in csv_runs], [a["run_id"]])
        self.assertEqual(len(etl_audit.recent_runs()), 2)

    # ---------- sanitisation ----------

    def test_sanitize_error_redacts_credentials(self):
        cases = [
            ("mysql://user:pass123@host:3306/db", "pass123"),
            ("Password=othersecret in message", "othersecret"),
            ("MYSQL_PWD: hunter2", "hunter2"),
        ]
        for message, secret in cases:
            cleaned = etl_audit.sanitize_error(message)
            self.assertNotIn(secret, cleaned, message)
        self.assertIsNone(etl_audit.sanitize_error(None))
        self.assertEqual(len(etl_audit.sanitize_error("x" * 9000)), 4000)

    # ---------- lock ----------

    def test_lock_acquire_conflict_and_release(self):
        first = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(first)
        second = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNone(second, "second acquire must fail while held")
        first.release()
        third = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(third, "lock must be free after release")
        third.release()

    def test_lock_release_is_idempotent(self):
        lock = etl_audit.acquire_run_lock(timeout=0)
        lock.release()
        lock.release()  # must not raise
        self.assertTrue(lock.released)

    # ---------- watermarks ----------

    def test_watermark_roundtrip_and_upsert(self):
        self.assertEqual(etl_audit.get_watermarks("weather"), {})
        etl_audit.record_watermark("weather", "CMBB", "2024-03-01 00:00:00", "g1")
        etl_audit.record_watermark("weather", "CMBB", "2024-03-05 00:00:00", "g2")
        etl_audit.record_watermark("weather", "KEAA", "2024-03-02 00:00:00", "g2")
        marks = etl_audit.get_watermarks("weather")
        self.assertEqual(
            marks,
            {"CMBB": "2024-03-05 00:00:00", "KEAA": "2024-03-02 00:00:00"},
        )

    # ---------- helpers ----------

    def _fetch(self, run_id):
        from database import dw_engine

        with dw_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT run_id, batch_guid, source_name, status, started_at,"
                    " finished_at, rows_read, rows_after, rows_updated, notes,"
                    " error_detail, engine_version "
                    "FROM etl_batch_log WHERE run_id = :r"
                ),
                {"r": run_id},
            ).mappings().one()
        return dict(row)


if __name__ == "__main__":
    unittest.main(verbosity=2)
