"""Behavioural tests for refresh_orchestrator.py.

The ETL stages themselves are stubbed (they are exercised for real in
test_etl_regression.py); what is verified here is the orchestration
contract: locking, audit trail, exit codes, source selection, verification
gating and log capture. Every run targets the disposable warehouse.
"""

import copy
import glob
import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ["DW_NAME"] = "aviation_dw_ci"

from tests import dbutil  # noqa: E402

import etl_audit  # noqa: E402
import refresh_orchestrator as ro  # noqa: E402
import verify_warehouse  # noqa: E402
from sqlalchemy import text  # noqa: E402


def _fresh_config():
    return copy.deepcopy(ro.load_config())


def _stub_stages(fail=(), weather_output=None):
    """Deterministic stage stubs shared by test classes (offline)."""
    calls = []

    def make(name):
        def stage(**kwargs):
            calls.append((name, kwargs))
            if name == "weather" and weather_output:
                print(weather_output)
            if name in fail:
                raise RuntimeError(f"{name} exploded")
        return stage

    return {n: make(n) for n in ("csv", "weather", "oltp")}, calls


def _passing_verifier(conn):
    return [verify_warehouse.Check("unit_stub", "PASS", "stubbed")]


def _failing_verifier(conn):
    return [verify_warehouse.Check("unit_stub", "FAIL", "stubbed failure")]


class TestOrchestrator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()

    # ---------- helpers ----------

    def _stages(self, fail=(), weather_output=None):
        calls = []

        def make(name):
            def stage(**kwargs):
                calls.append((name, kwargs))
                if name == "weather" and weather_output:
                    print(weather_output)
                if name in fail:
                    raise RuntimeError(f"{name} exploded")
            return stage

        return {n: make(n) for n in ("csv", "weather", "oltp")}, calls

    def _runs(self):
        from database import dw_engine

        with dw_engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT source_name, status, rows_read, error_detail "
                    "FROM etl_batch_log ORDER BY run_id"
                )
            ).fetchall()

    # ---------- success and failure paths ----------

    def test_all_stages_success_exits_zero_with_audit_trail(self):
        stages, calls = self._stages()
        # The oltp rows_read metric is stubbed here so this unit test does
        # not depend on the live source's current row count; real source
        # counting and the full OLTP stage are covered by
        # tests/test_etl_regression.py.
        with mock.patch.object(ro, "_rows_read_oltp", return_value=8):
            code = ro.run_refresh(
                _fresh_config(),
                stages=stages,
                verifier=_passing_verifier,
            )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual([name for name, _ in calls], ["csv", "weather", "oltp"])
        rows = self._runs()
        self.assertEqual(
            [(r[0], r[1]) for r in rows],
            [
                ("csv", "SUCCESS"),
                ("weather", "SUCCESS"),
                ("oltp", "SUCCESS"),
                ("verify", "SUCCESS"),
            ],
        )
        by_source = {r[0]: r for r in rows}
        self.assertEqual(by_source["csv"][2], 500)   # CSV data rows read
        self.assertEqual(by_source["oltp"][2], 8)    # stubbed source count
        self.assertIsNone(
            by_source["weather"][2],
            "weather has no single countable input table; rows_read stays NULL",
        )

    def test_required_stage_failure_exits_one(self):
        stages, _ = self._stages(fail=("weather",))
        code = ro.run_refresh(
            _fresh_config(),
            stages=stages,
            verifier=_passing_verifier,
        )
        self.assertEqual(code, ro.EXIT_STAGE_FAILED)
        statuses = [(r[0], r[1]) for r in self._runs()]
        self.assertIn(("weather", "FAILED"), statuses)
        self.assertIn(("csv", "SUCCESS"), statuses,
                      "continue-on-stage-failure: later stages still run")

    def test_optional_stage_failure_still_exits_zero(self):
        config = _fresh_config()
        config["sources"]["weather"]["required"] = False
        stages, _ = self._stages(fail=("weather",))
        code = ro.run_refresh(
            config, stages=stages, verifier=_passing_verifier
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertIn(("weather", "FAILED"), [(r[0], r[1]) for r in self._runs()])

    def test_disabled_source_is_skipped(self):
        config = _fresh_config()
        config["sources"]["oltp"]["enabled"] = False
        stages, calls = self._stages()
        code = ro.run_refresh(config, stages=stages, verifier=_passing_verifier)
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual([n for n, _ in calls], ["csv", "weather"])

    def test_enable_changes_without_oltp_source_warns(self):
        """--enable-changes with oltp disabled/excluded must warn loudly
        instead of silently doing nothing (operator-misconfiguration guard)."""
        import contextlib
        import io

        config = _fresh_config()
        config["sources"]["oltp"]["enabled"] = False
        stages, calls = self._stages()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = ro.run_refresh(
                config, stages=stages, verifier=_passing_verifier,
                enable_changes=True,
            )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual([n for n, _ in calls], ["csv", "weather"])
        self.assertIn("--enable-changes was requested but source 'oltp'",
                      out.getvalue())

    def test_source_subset_selection(self):
        stages, calls = self._stages()
        code = ro.run_refresh(
            _fresh_config(),
            stages=stages,
            verifier=_passing_verifier,
            sources={"oltp"},
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual([n for n, _ in calls], ["oltp"])

    def test_unknown_source_exits_two(self):
        code = ro.run_refresh(
            _fresh_config(),
            stages={},
            verifier=_passing_verifier,
            sources={"bogus"},
        )
        self.assertEqual(code, ro.EXIT_PREFLIGHT)

    # ---------- locking ----------

    def test_concurrent_run_is_refused_with_exit_three(self):
        holder = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(holder)
        try:
            stages, calls = self._stages()
            code = ro.run_refresh(
                _fresh_config(),
                stages=stages,
                verifier=_passing_verifier,
            )
            self.assertEqual(code, ro.EXIT_LOCKED)
            self.assertEqual(calls, [], "no stage may run while locked out")
            self.assertEqual(self._runs(), [], "no audit rows for a locked-out run")
        finally:
            holder.release()

    # ---------- audit fail-closed ----------

    def test_audit_start_failure_prevents_stage_and_exits_four(self):
        stages, calls = self._stages()
        with mock.patch.object(
            etl_audit, "start_run", side_effect=etl_audit.AuditError("disk full")
        ):
            code = ro.run_refresh(
                _fresh_config(),
                stages=stages,
                verifier=_passing_verifier,
            )
        self.assertEqual(code, ro.EXIT_AUDIT_START)
        self.assertEqual(calls, [], "stage must NOT execute without an audit row")

    def test_audit_finish_failure_exits_five(self):
        stages, _ = self._stages()
        with mock.patch.object(
            etl_audit, "finish_run",
            side_effect=etl_audit.AuditError("audit write lost"),
        ):
            code = ro.run_refresh(
                _fresh_config(),
                stages=stages,
                verifier=_passing_verifier,
            )
        self.assertEqual(code, ro.EXIT_AUDIT_FINISH)

    # ---------- verification gating ----------

    def test_verification_failure_exits_six_and_records_audit(self):
        stages, _ = self._stages()
        code = ro.run_refresh(
            _fresh_config(),
            stages=stages,
            verifier=_failing_verifier,
        )
        self.assertEqual(code, ro.EXIT_VERIFY_FAILED)
        self.assertIn(
            ("verify", "FAILED"), [(r[0], r[1]) for r in self._runs()]
        )

    def test_no_verify_skips_verification_row(self):
        stages, _ = self._stages()
        code = ro.run_refresh(
            _fresh_config(),
            stages=stages,
            verifier=_failing_verifier,
            verify=False,
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertNotIn("verify", [r[0] for r in self._runs()])

    def test_config_can_disable_verification(self):
        config = _fresh_config()
        config["run_verification"] = False
        stages, _ = self._stages()
        code = ro.run_refresh(
            config, stages=stages, verifier=_failing_verifier
        )
        self.assertEqual(code, ro.EXIT_OK)

    # ---------- weather PARTIAL status ----------

    def test_weather_partial_status_is_reported_from_stage_output(self):
        stages, _ = self._stages(
            weather_output="airports failed : ['ZZZZ']"
        )
        code = ro.run_refresh(
            _fresh_config(),
            stages=stages,
            verifier=_passing_verifier,
        )
        statuses = [(r[0], r[1]) for r in self._runs()]
        self.assertIn(("weather", "PARTIAL"), statuses)
        self.assertEqual(code, ro.EXIT_STAGE_FAILED)  # weather is required

    def test_weather_clean_output_is_success(self):
        stages, _ = self._stages(weather_output="airports failed : []")
        code = ro.run_refresh(
            _fresh_config(),
            stages=stages,
            verifier=_passing_verifier,
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertIn(("weather", "SUCCESS"), [(r[0], r[1]) for r in self._runs()])

    # ---------- metrics honesty ----------

    def test_metrics_reflect_insert_only_idempotency(self):
        stages, _ = self._stages()
        ro.run_refresh(
            _fresh_config(), stages=stages, verifier=_passing_verifier
        )
        from database import dw_engine

        with dw_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT rows_read, rows_before, rows_after, rows_inserted, "
                    "rows_skipped FROM etl_batch_log "
                    "WHERE source_name='csv' ORDER BY run_id DESC LIMIT 1"
                )
            ).one()
        read_, before, after, inserted, skipped = row
        self.assertEqual(read_, 500)          # data/flight.csv data rows
        self.assertEqual(before, 0)           # disposable warehouse, pre-stage
        self.assertEqual(after, 0)            # stub stage wrote nothing
        self.assertEqual(inserted, 0)
        self.assertEqual(skipped, 500)        # read - inserted

    # ---------- logs and dry run ----------

    def test_stage_log_file_is_written(self):
        before = set(glob.glob(os.path.join(ROOT, "logs", "*.log")))
        stages, _ = self._stages()
        ro.run_refresh(
            _fresh_config(), stages=stages, verifier=_passing_verifier,
            sources={"csv"},
        )
        after = set(glob.glob(os.path.join(ROOT, "logs", "*.log")))
        self.assertTrue(after - before, "expected a new stage log file")

    def test_dry_run_executes_no_stage_and_no_audit_row(self):
        stages, calls = self._stages()
        # inject stub stages so dry-run cannot fall through to real ETL code
        with mock.patch.object(ro, "_default_stages", return_value=stages):
            code = ro.run_refresh(
                _fresh_config(),
                stages=stages,
                verifier=_passing_verifier,
                sources={"csv"},
                dry_run=True,
            )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual(calls, [])
        self.assertEqual(self._runs(), [])

    def test_lock_is_released_after_run(self):
        stages, _ = self._stages()
        ro.run_refresh(
            _fresh_config(), stages=stages, verifier=_passing_verifier,
            sources={"csv"},
        )
        lock = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(lock, "lock must be free after a completed run")
        lock.release()


class TestReadOnlyModesAndInterruption(unittest.TestCase):
    """Regression tests for the safety audit fixes:

    * --dry-run / --verify-only never execute DDL, never take the lock,
      never write audit rows (they are strictly read-only);
    * KeyboardInterrupt closes the open audit row and exits 130;
    * config stale_running_minutes reaches the verifier;
    * invalid stale_running_minutes is rejected at config load."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()

    def _runs(self):
        from database import dw_engine

        with dw_engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT source_name, status, rows_read, error_detail "
                    "FROM etl_batch_log ORDER BY run_id"
                )
            ).fetchall()

    def test_dry_run_never_calls_ensure_schema_or_lock(self):
        stages, calls = _stub_stages()
        with mock.patch.object(ro, "_default_stages", return_value=stages), \
                mock.patch.object(
                    etl_audit, "ensure_schema",
                    side_effect=AssertionError("DDL must not run in dry-run"),
                ), mock.patch.object(
                    etl_audit, "acquire_run_lock",
                    side_effect=AssertionError("lock must not be taken in dry-run"),
                ):
            code = ro.run_refresh(
                _fresh_config(), stages=stages, verifier=_passing_verifier,
                sources={"csv"}, dry_run=True,
            )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual(calls, [], "no stage may execute in dry-run")
        self.assertEqual(self._runs(), [], "no audit rows in dry-run")

    def test_verify_only_never_creates_audit_schema_or_writes_rows(self):
        with mock.patch.object(
            etl_audit, "ensure_schema",
            side_effect=AssertionError("DDL must not run in verify-only"),
        ), mock.patch.object(
            ro, "_run_verification",
            return_value=[verify_warehouse.Check("stub", "PASS", "ok")],
        ):
            code = ro.main(["--verify-only"])
        self.assertEqual(code, 0)
        self.assertEqual(self._runs(), [], "verify-only writes no audit rows")

    def test_interrupted_stage_is_recorded_and_exits_130(self):
        def interrupting(**kwargs):
            raise KeyboardInterrupt

        stages, _ = _stub_stages()
        stages["csv"] = interrupting
        code = ro.run_refresh(
            _fresh_config(), stages=stages, verifier=_passing_verifier,
        )
        self.assertEqual(code, ro.EXIT_INTERRUPTED)
        rows = self._runs()
        self.assertEqual(
            [(r[0], r[1]) for r in rows],
            [("csv", "FAILED")],
            "interrupted stage is closed FAILED; later stages never start",
        )
        self.assertIn("interrupted by operator", rows[0][3] or "")
        lock = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(lock, "lock must be released after interrupt")
        lock.release()

    def test_config_stale_running_minutes_is_passed_to_verifier(self):
        config = _fresh_config()
        config["stale_running_minutes"] = 42
        stages, _ = _stub_stages()
        seen = {}

        def fake_checks(conn, baseline_path=None, stale_minutes=240):
            seen["stale_minutes"] = stale_minutes
            return [verify_warehouse.Check("stub", "PASS", "ok")]

        with mock.patch.object(
            verify_warehouse, "run_checks", side_effect=fake_checks
        ):
            code = ro.run_refresh(config, stages=stages)  # verifier=None path
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual(seen["stale_minutes"], 42)

    def test_load_config_rejects_non_positive_stale_running_minutes(self):
        import json
        import tempfile

        bad = _fresh_config()
        bad["stale_running_minutes"] = 0
        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False
        ) as handle:
            json.dump(bad, handle)
            path = handle.name
        try:
            with self.assertRaises(ValueError):
                ro.load_config(path)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
