"""Tests for verify_warehouse.py - both the clean-DB baseline behaviour and
the ability of each check to DETECT a real violation (injected into the
disposable warehouse, then removed).

Read-only contract: these tests only INSERT corruption into the test
database; verify_warehouse itself is never called in a mode that writes.
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
import verify_warehouse as vw  # noqa: E402
from sqlalchemy import text  # noqa: E402

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"


def _run_checks(**kwargs):
    from database import dw_engine

    with dw_engine.connect() as conn:
        return vw.run_checks(conn, **kwargs)


def _by_name(checks):
    return {c.name: c for c in checks}


class TestVerifyCleanWarehouse(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def test_fresh_warehouse_has_no_failures(self):
        checks = _run_checks()
        failures = [c.name for c in checks if c.status == FAIL]
        self.assertEqual(failures, [], failures)
        statuses = _by_name(checks)
        self.assertEqual(statuses["tables_present"].status, PASS)
        self.assertEqual(statuses["schema_baseline"].status, PASS,
                         "test schema built from sql/schema.sql must match "
                         "the recorded baseline")
        self.assertEqual(statuses["audit_table"].status, PASS)
        self.assertEqual(statuses["fact_source_identity_unique"].status, PASS)

    def test_expected_warnings_on_empty_test_warehouse(self):
        checks = _by_name(_run_checks())
        self.assertEqual(checks["row_counts"].status, WARN)   # empty fact
        self.assertEqual(checks["dim_weather_identity"].status, WARN)  # empty

    def test_missing_baseline_reports_skip_not_pass(self):
        checks = _by_name(
            _run_checks(baseline_path=os.path.join(ROOT, "config", "nope.json"))
        )
        self.assertEqual(checks["schema_baseline"].status, SKIP)

    def test_checks_never_write(self):
        from database import dw_engine

        with dw_engine.connect() as conn:
            before = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = DATABASE()"
                )
            ).scalar()
            vw.run_checks(conn)
            after = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = DATABASE()"
                )
            ).scalar()
        self.assertEqual(before, after)


class TestVerifyDetectsViolations(unittest.TestCase):
    """Each test injects one real violation, asserts the check fails/warns,
    and removes the injection (leaving the test DB clean)."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()

    def test_row_count_decrease_detected(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO etl_batch_log "
                    "(batch_guid, source_name, status, started_at, finished_at,"
                    " rows_after) VALUES "
                    "('g', 'csv', 'SUCCESS', NOW(3), NOW(3), 999999)"
                )
            )
        try:
            checks = _by_name(_run_checks())
            check = checks["row_count_not_decreased[csv]"]
            self.assertEqual(check.status, FAIL)
            self.assertIn("999999", check.detail)
        finally:
            dbutil.clear_audit()

    def test_duplicate_dim_date_detected(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO dim_date (date_key, full_date) VALUES "
                    "(20990101, '2099-01-01'), (20990102, '2099-01-01')"
                )
            )
        try:
            checks = _by_name(_run_checks())
            self.assertEqual(checks["dimension_uniqueness"].status, FAIL)
            self.assertIn("dim_date", checks["dimension_uniqueness"].detail)
        finally:
            with dw_engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM dim_date WHERE date_key >= 20990101")
                )

    def test_duplicate_source_identity_detected_when_index_removed(self):
        from database import dw_engine

        self._seed_dimensions()
        with dw_engine.begin() as conn:
            conn.execute(text("ALTER TABLE fact_flight DROP INDEX uq_fact_source"))
            conn.execute(
                text(
                    "INSERT INTO fact_flight "
                    "(source_system, source_flight_id, date_key, airline_key,"
                    " route_key) VALUES "
                    "('csv', 'dup', 20990101, 1, 1), "
                    "('csv', 'dup', 20990101, 1, 1)"
                )
            )
        try:
            checks = _by_name(_run_checks())
            self.assertEqual(checks["fact_source_identity_unique"].status, FAIL)
        finally:
            with dw_engine.begin() as conn:
                conn.execute(
                    text(
                        "DELETE FROM fact_flight WHERE source_flight_id = 'dup'"
                    )
                )
                conn.execute(
                    text(
                        "ALTER TABLE fact_flight ADD UNIQUE KEY uq_fact_source "
                        "(source_system, source_flight_id)"
                    )
                )
            self._clear_dimensions()

    def test_unapproved_column_drift_detected_and_not_repaired(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text("ALTER TABLE dim_airline ADD COLUMN probe_col INT NULL")
            )
        try:
            checks = _by_name(_run_checks())
            self.assertEqual(checks["schema_baseline"].status, FAIL)
            self.assertIn("probe_col", checks["schema_baseline"].detail)
            # read-only contract: the drift is reported, never repaired
            with dw_engine.connect() as conn:
                still_there = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = DATABASE() "
                        "AND table_name='dim_airline' AND column_name='probe_col'"
                    )
                ).scalar()
            self.assertEqual(still_there, 1)
        finally:
            with dw_engine.begin() as conn:
                conn.execute(
                    text("ALTER TABLE dim_airline DROP COLUMN probe_col")
                )

    def test_stale_running_row_is_warned_not_mutated(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO etl_batch_log "
                    "(batch_guid, source_name, status, started_at) VALUES "
                    "('stale', 'weather', 'RUNNING', "
                    " NOW(3) - INTERVAL 2 DAY)"
                )
            )
        try:
            checks = _by_name(_run_checks())
            stale = checks["audit_no_stale_runs"]
            self.assertEqual(stale.status, WARN)
            # read-only contract: the stale row is reported, never changed
            with dw_engine.connect() as conn:
                status = conn.execute(
                    text(
                        "SELECT status FROM etl_batch_log "
                        "WHERE batch_guid='stale'"
                    )
                ).scalar()
            self.assertEqual(status, "RUNNING")
        finally:
            dbutil.clear_audit()

    def test_latest_failed_run_is_warned(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO etl_batch_log "
                    "(batch_guid, source_name, status, started_at, finished_at)"
                    " VALUES ('f1', 'oltp', 'FAILED', NOW(3), NOW(3))"
                )
            )
        try:
            checks = _by_name(_run_checks())
            self.assertEqual(checks["audit_latest_status"].status, WARN)
        finally:
            dbutil.clear_audit()

    def test_failed_run_followed_by_success_is_not_warned(self):
        # regression: the check must compare the LATEST terminal run per
        # source, not merely "did this source ever fail"
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO etl_batch_log "
                    "(batch_guid, source_name, status, started_at, finished_at)"
                    " VALUES ('f2', 'oltp', 'FAILED', NOW(3) - INTERVAL 5 MINUTE,"
                    " NOW(3) - INTERVAL 5 MINUTE)"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO etl_batch_log "
                    "(batch_guid, source_name, status, started_at, finished_at)"
                    " VALUES ('f3', 'oltp', 'SUCCESS', NOW(3), NOW(3))"
                )
            )
        try:
            checks = _by_name(_run_checks())
            self.assertEqual(checks["audit_latest_status"].status, PASS)
        finally:
            dbutil.clear_audit()

    def test_null_required_key_is_impossible_but_constraint_holds(self):
        # date_key/airline_key/route_key are NOT NULL: the database itself
        # must reject a violation, which is what makes the check a stable
        # PASS. Assert both layers agree.
        from database import dw_engine
        from sqlalchemy.exc import IntegrityError

        with self.assertRaises(IntegrityError):
            with dw_engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO fact_flight "
                        "(source_system, source_flight_id, date_key, "
                        "airline_key, route_key) VALUES "
                        "('probe', 'p1', NULL, 1, 1)"
                    )
                )
        checks = _by_name(_run_checks())
        self.assertEqual(checks["fact_required_keys"].status, PASS)

    # ---------- fixtures ----------

    def _seed_dimensions(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT IGNORE INTO dim_date (date_key, full_date) "
                    "VALUES (20990101, '2099-01-01')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO dim_airline (airline_code, airline_name) "
                    "VALUES ('PROBE', 'Probe Air')"
                )
            )
            airport = conn.execute(
                text(
                    "INSERT INTO dim_airport (airport_code, airport_name) "
                    "VALUES ('PBAA', 'Probe A'), ('PBAB', 'Probe B')"
                )
            )
            # need explicit keys for the route insert
            keys = conn.execute(
                text(
                    "SELECT airport_key FROM dim_airport "
                    "WHERE airport_code IN ('PBAA','PBAB') "
                    "ORDER BY airport_code"
                )
            ).fetchall()
            conn.execute(
                text(
                    "INSERT INTO dim_route "
                    "(origin_airport_key, destination_airport_key) "
                    "VALUES (:o, :d)"
                ),
                {"o": keys[0][0], "d": keys[1][0]},
            )

    def _clear_dimensions(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(text("DELETE FROM dim_route WHERE route_key > 0"))
            conn.execute(text("DELETE FROM dim_airline WHERE airline_code='PROBE'"))
            conn.execute(
                text(
                    "DELETE FROM dim_airport "
                    "WHERE airport_code IN ('PBAA','PBAB')"
                )
            )
            conn.execute(text("DELETE FROM dim_date WHERE date_key=20990101"))


class TestAuditHealthThresholds(unittest.TestCase):
    """stale_running_minutes must be an honoured, boundary-accurate
    parameter (wired from config/refresh_policy.json by the orchestrator)."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()

    def _insert_running(self, minutes_ago, guid="stale-b"):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO etl_batch_log "
                    "(batch_guid, source_name, status, started_at) VALUES "
                    "(:g, 'weather', 'RUNNING', "
                    f"NOW() - INTERVAL {int(minutes_ago)} MINUTE)"
                ),
                {"g": guid},
            )

    def _health(self, stale_minutes):
        from database import dw_engine

        with dw_engine.connect() as conn:
            return {
                c.name: c for c in vw.check_audit_health(
                    conn, stale_minutes=stale_minutes
                )
            }

    def test_stale_threshold_boundary_around_configured_minutes(self):
        self._insert_running(239)
        self.assertEqual(
            self._health(240)["audit_no_stale_runs"].status, PASS,
            "239 min old is not stale at a 240 min threshold",
        )
        self.assertEqual(
            self._health(200)["audit_no_stale_runs"].status, WARN,
            "239 min old IS stale at a 200 min threshold",
        )

    def test_custom_threshold_changes_verdict(self):
        self._insert_running(100)
        self.assertEqual(self._health(90)["audit_no_stale_runs"].status, WARN)
        self.assertEqual(self._health(120)["audit_no_stale_runs"].status, PASS)


class TestBaselineWriteGuard(unittest.TestCase):
    """--write-baseline must refuse to snapshot a schema without an
    explicit operator confirmation flag."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def test_write_baseline_refused_without_confirmation(self):
        import tempfile

        directory = tempfile.mkdtemp(prefix="vwtest_")
        path = os.path.join(directory, "baseline.json")
        try:
            code = vw.main(["--write-baseline", path])
            self.assertEqual(code, 2)
            self.assertFalse(
                os.path.exists(path),
                "refused --write-baseline must create no file",
            )
            code = vw.main(["--write-baseline", path, "--allow-baseline-write"])
            self.assertEqual(code, 0)
            self.assertTrue(os.path.exists(path))
        finally:
            if os.path.exists(path):
                os.unlink(path)
            os.rmdir(directory)


if __name__ == "__main__":
    unittest.main(verbosity=2)
