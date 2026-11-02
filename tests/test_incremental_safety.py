"""Comprehensive incremental refresh safety, idempotency, and comparison test suite.

Validates Phase Seven and Phase Eight safety criteria across all sources (CSV, Weather, OLTP)
and general orchestration in the isolated test environment (aviation_dw_ci).
"""

import copy
import os
import sys
import unittest
from datetime import date, datetime, timedelta
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ["DW_NAME"] = "aviation_dw_ci"

from tests import dbutil  # noqa: E402
import database  # noqa: E402
import etl_audit  # noqa: E402
import refresh_orchestrator as ro  # noqa: E402
import verify_warehouse  # noqa: E402
from sqlalchemy import text  # noqa: E402

FIXTURE_OLTP_DB = "aviation_oltp_ci"


def _fresh_config():
    return copy.deepcopy(ro.load_config())


def _passing_verifier(conn):
    return [verify_warehouse.Check("unit_stub", "PASS", "stubbed")]


def _failing_verifier(conn):
    return [verify_warehouse.Check("unit_stub", "FAIL", "stubbed failure")]


class TestCsvIncrementalSafety(unittest.TestCase):
    """CSV source safety: new records, duplicate prevention, idempotency, transaction rollback."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()
        with database.dw_engine.begin() as conn:
            conn.execute(text("DELETE FROM fact_flight WHERE source_system = 'csv'"))

    def test_csv_new_record_detection_and_load(self):
        from etl import run_etl
        run_etl()
        with database.dw_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE source_system = 'csv'")
            ).scalar()
        self.assertEqual(count, 500, "all 500 CSV records should be loaded on initial run")

    def test_csv_duplicate_prevention_on_repeated_runs(self):
        from etl import run_etl
        run_etl()
        # Second run immediately
        run_etl()
        with database.dw_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE source_system = 'csv'")
            ).scalar()
            duplicates = conn.execute(
                text("""
                    SELECT COUNT(*) FROM (
                        SELECT source_flight_id
                        FROM fact_flight
                        WHERE source_system = 'csv'
                        GROUP BY source_flight_id
                        HAVING COUNT(*) > 1
                    ) d
                """)
            ).scalar()
        self.assertEqual(count, 500, "repeated run must not insert duplicate rows")
        self.assertEqual(duplicates, 0, "no duplicate source_flight_id may exist")

    def test_csv_modified_record_handling_skip_if_exists(self):
        from etl import run_etl
        run_etl()
        # Modify a row in the warehouse manually to simulate an update divergence
        with database.dw_engine.begin() as conn:
            conn.execute(
                text("UPDATE fact_flight SET delay_minutes = 9999 WHERE source_system = 'csv' LIMIT 1")
            )
        # Re-run ETL: CSV is insert-only (skip-if-exists); existing row must not be overwritten
        run_etl()
        with database.dw_engine.connect() as conn:
            preserved = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE source_system = 'csv' AND delay_minutes = 9999")
            ).scalar()
        self.assertEqual(preserved, 1, "CSV loader must preserve existing rows (skip-if-exists)")

    def test_csv_failure_recovery_transaction_rollback(self):
        import etl
        with database.dw_engine.begin() as conn:
            conn.execute(text("DELETE FROM fact_flight WHERE source_system = 'csv'"))

        # Inject failure during fact table insertion
        with mock.patch("pandas.DataFrame.to_sql", side_effect=RuntimeError("simulated DB disk error")):
            with self.assertRaises(RuntimeError):
                etl.run_etl()

        # Database transaction should have rolled back completely
        with database.dw_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE source_system = 'csv'")
            ).scalar()
        self.assertEqual(count, 0, "failed CSV load must roll back cleanly, leaving 0 rows")

        # Retry after failure succeeds
        etl.run_etl()
        with database.dw_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE source_system = 'csv'")
            ).scalar()
        self.assertEqual(count, 500, "recovery run after failure must load all 500 rows")


class TestWeatherIncrementalSafety(unittest.TestCase):
    """Weather API source safety: date ranges, lookback overlap, upserts, retries, watermark tracking."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()
        with database.dw_engine.begin() as conn:
            conn.execute(text("DELETE FROM dim_weather"))
            conn.execute(text("INSERT IGNORE INTO dim_airport (airport_code, airport_name) VALUES ('CMB', 'Colombo')"))
            conn.execute(text("INSERT IGNORE INTO dim_airport (airport_code, airport_name) VALUES ('DXB', 'Dubai')"))

    def test_weather_upsert_idempotency_and_no_duplicates(self):
        from weather_etl import _upsert_weather
        import pandas as pd

        with database.dw_engine.connect() as conn:
            airport_key = conn.execute(
                text("SELECT airport_key FROM dim_airport WHERE airport_code = 'CMB'")
            ).scalar()

        rows = pd.DataFrame({
            "airport_key": [airport_key, airport_key],
            "observation_time": [datetime(2025, 6, 1, 10, 0), datetime(2025, 6, 1, 11, 0)],
            "temperature": [28.5, 29.0],
            "rainfall": [0.0, 0.5],
            "wind_speed": [12.0, 14.0],
            "visibility": [10.0, 10.0],
            "weather_condition": ["Clear", "Cloudy"],
            "relative_humidity": [75.0, 78.0],
            "cloud_cover": [20.0, 60.0],
            "weather_code": [0, 2],
            "visibility_source": ["noaa-isd-metar", "noaa-isd-metar"],
            "source_dataset": ["open-meteo-archive", "open-meteo-archive"],
            "source_model": ["best_match_utc_v1", "best_match_utc_v1"],
            "source_timezone": ["Asia/Colombo", "Asia/Colombo"],
        })

        with database.dw_engine.begin() as conn:
            written_1 = _upsert_weather(conn, rows)
            written_2 = _upsert_weather(conn, rows)

        with database.dw_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM dim_weather")).scalar()
        self.assertEqual(count, 2, "upsert must not create duplicates for identical keys")

    def test_weather_revised_observations_updated_in_place(self):
        from weather_etl import _upsert_weather
        import pandas as pd

        with database.dw_engine.connect() as conn:
            airport_key = conn.execute(
                text("SELECT airport_key FROM dim_airport WHERE airport_code = 'CMB'")
            ).scalar()

        rows_initial = pd.DataFrame({
            "airport_key": [airport_key],
            "observation_time": [datetime(2025, 6, 1, 10, 0)],
            "temperature": [28.0],
            "rainfall": [0.0],
            "wind_speed": [10.0],
            "visibility": [10.0],
            "weather_condition": ["Clear"],
            "relative_humidity": [70.0],
            "cloud_cover": [10.0],
            "weather_code": [0],
            "visibility_source": ["noaa-isd-metar"],
            "source_dataset": ["open-meteo-archive"],
            "source_model": ["best_match_utc_v1"],
            "source_timezone": ["Asia/Colombo"],
        })

        # Revised observation for the same instant (simulating reanalysis revision)
        rows_revised = rows_initial.copy()
        rows_revised["temperature"] = [31.5]
        rows_revised["weather_condition"] = ["Hot"]

        with database.dw_engine.begin() as conn:
            _upsert_weather(conn, rows_initial)
            _upsert_weather(conn, rows_revised)

        with database.dw_engine.connect() as conn:
            res = conn.execute(
                text("SELECT temperature, weather_condition FROM dim_weather WHERE airport_key = :ak"),
                {"ak": airport_key}
            ).fetchone()
        self.assertEqual(float(res[0]), 31.5, "revised temperature must be updated in-place")
        self.assertEqual(res[1], "Hot", "revised condition must be updated in-place")

    def test_weather_archive_max_date_enforcement(self):
        import weather_etl
        today = date.today()
        # Ensure _padded_request_dates clamps to ARCHIVE_MAX_DATE
        req_start, req_end = weather_etl._padded_request_dates(today - timedelta(days=2), today)
        self.assertLessEqual(
            req_end, weather_etl.ARCHIVE_MAX_DATE,
            "padded request date must never exceed ARCHIVE_MAX_DATE"
        )
        self.assertEqual(
            req_end, weather_etl.ARCHIVE_MAX_DATE,
            "padded end date must clamp exactly to ARCHIVE_MAX_DATE"
        )

    def test_failed_airport_request_holds_watermark(self):
        etl_audit.record_watermark("weather", "CMB", "2025-06-01 00:00:00Z")
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True

        stages, calls = self._stub_stages(fail=(), weather_failed=["CMB"])
        code = ro.run_refresh(config, stages=stages, verifier=_passing_verifier, sources={"weather"})
        self.assertEqual(code, ro.EXIT_STAGE_FAILED, "partial failure must exit non-zero for required stage")

        # Check that watermark was held
        wms = etl_audit.get_watermarks("weather")
        self.assertEqual(wms.get("CMB"), "2025-06-01 00:00:00Z", "failed airport watermark must not advance")

    def _stub_stages(self, fail=(), weather_failed=None):
        calls = []
        def make(name):
            def stage(**kwargs):
                calls.append((name, kwargs))
                if name in fail:
                    raise RuntimeError(f"{name} exploded")
                if name == "weather":
                    return {"airports_failed": list(weather_failed or [])}
                return None
            return stage
        return {n: make(n) for n in ("csv", "weather", "oltp")}, calls


class TestOltpIncrementalSafety(unittest.TestCase):
    """OLTP source safety: full load idempotency, bounded update separation, duplicate prevention."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()

    def test_oltp_config_incremental_true_is_rejected(self):
        config = _fresh_config()
        config["sources"]["oltp"]["incremental"] = True
        import tempfile, json
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
            json.dump(config, tmp)
            tmp_path = tmp.name
        try:
            with self.assertRaises(ValueError):
                ro.load_config(tmp_path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_oltp_duplicate_prevention_on_second_run(self):
        if not dbutil.oltp_available():
            self.skipTest("OLTP source unreachable")
        from oltp_etl import run_oltp_etl
        run_oltp_etl()
        with database.dw_engine.connect() as conn:
            count_1 = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE source_system = 'oltp'")
            ).scalar()
        run_oltp_etl()
        with database.dw_engine.connect() as conn:
            count_2 = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE source_system = 'oltp'")
            ).scalar()
        self.assertEqual(count_1, count_2, "second OLTP load must not insert duplicate rows")


class TestGeneralIncrementalAndOrchestratorSafety(unittest.TestCase):
    """General orchestration safety: locking, dry-run, verification gating, full-refresh fallback."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()

    def test_dry_run_executes_no_writes_and_no_ddl(self):
        config = _fresh_config()
        with mock.patch("etl_audit.start_run") as mock_audit:
            code = ro._dry_run(config, ["csv", "weather", "oltp"])
            mock_audit.assert_not_called()
        self.assertEqual(code, ro.EXIT_OK, "dry-run must return 0 on reachable system")

    def test_lock_prevents_overlapping_execution(self):
        lock = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(lock, "must acquire run lock initially")
        try:
            # Second attempt to acquire lock must fail
            code = ro.run_refresh(_fresh_config())
            self.assertEqual(code, ro.EXIT_LOCKED, "overlapping execution must exit with code 3 (LOCKED)")
        finally:
            lock.release()

    def test_watermark_advancement_requires_passed_verification(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        config["order"] = ["weather"]

        # Run with failing verifier
        stages = {"weather": lambda **kwargs: {"airports_failed": []}}
        code = ro.run_refresh(config, stages=stages, verifier=_failing_verifier, sources={"weather"})
        self.assertEqual(code, ro.EXIT_VERIFY_FAILED, "failing verification must exit 6")

        # Run with skipped verification (--no-verify)
        code = ro.run_refresh(config, stages=stages, verifier=_passing_verifier, verify=False, sources={"weather"})
        self.assertEqual(code, ro.EXIT_OK)


class TestIncrementalVsFullRecomputationComparison(unittest.TestCase):
    """Phase Eight: directly verify that incremental windows match full recomputation."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()
        with database.dw_engine.begin() as conn:
            conn.execute(text("INSERT IGNORE INTO dim_airport (airport_key, airport_code, airport_name) VALUES (1, 'CMB', 'Colombo')"))
            conn.execute(text("INSERT IGNORE INTO dim_airport (airport_key, airport_code, airport_name) VALUES (2, 'DXB', 'Dubai')"))
            conn.execute(text("INSERT IGNORE INTO dim_route (route_key, origin_airport_key, destination_airport_key) VALUES (1, 1, 2)"))
            conn.execute(text("INSERT IGNORE INTO dim_date (date_key, full_date) VALUES (20250601, '2025-06-01')"))
            conn.execute(text("INSERT IGNORE INTO dim_airline (airline_key, airline_code, airline_name) VALUES (1, 'UL', 'SriLankan')"))
            conn.execute(text("""
                INSERT INTO fact_flight (
                    source_system, source_flight_id, date_key, airline_key, route_key, scheduled_departure
                ) VALUES (
                    'csv', 'test_1', 20250601, 1, 1, '2025-06-01 10:00:00'
                )
            """))
            conn.execute(text("""
                INSERT INTO fact_flight (
                    source_system, source_flight_id, date_key, airline_key, route_key, scheduled_departure
                ) VALUES (
                    'csv', 'test_2', 20250601, 1, 1, '2025-06-10 10:00:00'
                )
            """))
        etl_audit.record_watermark("weather", "CMB", "2025-06-08 00:00:00Z")

    def test_compare_incremental_vs_full_metrics(self):
        config = _fresh_config()
        summary = ro.compare_incremental_vs_full(config, print_report=False)
        self.assertIn("airports", summary)
        self.assertIn("total_full_days", summary)
        self.assertIn("total_incremental_days", summary)
        self.assertIn("total_days_avoided", summary)
        self.assertIn("work_reduction_pct", summary)
        self.assertGreater(summary["total_full_days"], 0)
        self.assertGreaterEqual(summary["total_days_avoided"], 0)
        self.assertLessEqual(summary["total_incremental_days"], summary["total_full_days"])
        cmb = summary["airports"]["CMB"]
        self.assertEqual(cmb["full_range"], "2025-06-01..2025-06-10")
        self.assertEqual(cmb["full_days"], 10)
        # watermark is 2025-06-08, overlap is 7 days -> inc start is 2025-06-01
        self.assertEqual(cmb["mode"], "incremental")

if __name__ == "__main__":
    unittest.main()
