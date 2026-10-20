"""Regression tests for the REAL ETL stages against the disposable warehouse.

These tests exercise the unmodified etl.py / oltp_etl.py code paths end to
end (extract -> transform -> load) to prove that the Data Refresh Policy
work did not change ETL behaviour:

* CSV load: exactly the 500 source rows land in fact_flight, dimensions
  populate, and a second run inserts nothing and updates nothing
  (insert-only / skip-if-exists preserved: a manually mutated row stays
  mutated).
* OLTP load: exactly the source flight count lands, second run is a
  no-op (requires reachable Aiven source; skipped otherwise).
* weather_etl: module loads, default batch size unchanged (a real archive
  fetch is network-bound and opt-in via AVIATION_TESTS_WEATHER_NET=1).
* main_etl: still importable and exposes the same entry point (never
  executed here because it orchestrates the network-bound weather stage).

All writes go to aviation_dw_ci (forced in tests/__init__.py).
"""

import contextlib
import hashlib
import io
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ["DW_NAME"] = "aviation_dw_ci"

from tests import dbutil  # noqa: E402

from sqlalchemy import text  # noqa: E402

CSV_SOURCE_ROWS = 500  # data/flight.csv data rows (Phase-0 baseline)


def _count(sql, params=None):
    from database import dw_engine

    with dw_engine.connect() as conn:
        return conn.execute(text(sql), params or {}).scalar()


def _fingerprint(sql, params=None):
    from database import dw_engine

    with dw_engine.connect() as conn:
        rows = conn.execute(text(sql), params or {}).fetchall()
    payload = "\n".join("|".join(repr(v) for v in row) for row in rows)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _quiet(fn, *args, **kwargs):
    """Run an ETL stage with its (verbose) stdout captured."""
    with contextlib.redirect_stdout(io.StringIO()) as captured:
        result = fn(*args, **kwargs)
    return result, captured.getvalue()


class TestCsvEtlRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def test_full_load_then_idempotent_second_run(self):
        from etl import run_etl

        _, first_output = _quiet(run_etl)
        self.assertIn("Existing CSV source records skipped", first_output)
        self.assertEqual(
            _count("SELECT COUNT(*) FROM fact_flight WHERE source_system='csv'"),
            CSV_SOURCE_ROWS,
        )
        # the CSV stage also maintains legacy dim_weather rows (NULL identity)
        self.assertGreater(_count("SELECT COUNT(*) FROM dim_weather"), 0)

        fact_fp = _fingerprint(
            "SELECT * FROM fact_flight WHERE source_system='csv' "
            "ORDER BY flight_key"
        )
        weather_count = _count("SELECT COUNT(*) FROM dim_weather")

        # second run: insert-only, nothing added, nothing rewritten
        _, second_output = _quiet(run_etl)
        self.assertEqual(
            _count("SELECT COUNT(*) FROM fact_flight WHERE source_system='csv'"),
            CSV_SOURCE_ROWS,
        )
        self.assertEqual(_count("SELECT COUNT(*) FROM dim_weather"), weather_count)
        self.assertEqual(
            _fingerprint(
                "SELECT * FROM fact_flight WHERE source_system='csv' "
                "ORDER BY flight_key"
            ),
            fact_fp,
            "second CSV run must not modify existing fact rows",
        )
        self.assertIn(
            f"skipped: {CSV_SOURCE_ROWS}", second_output.replace("  ", " "),
            "second run must report all source rows as skipped",
        )

    def test_existing_rows_are_never_updated(self):
        from etl import run_etl

        _, _ = _quiet(run_etl)
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE fact_flight SET delay_minutes = 77777 "
                    "WHERE source_system='csv' "
                    "ORDER BY flight_key LIMIT 1"
                )
            )
        _quiet(run_etl)
        self.assertEqual(
            _count(
                "SELECT COUNT(*) FROM fact_flight "
                "WHERE source_system='csv' AND delay_minutes = 77777"
            ),
            1,
            "CSV refresh must not overwrite existing rows (skip-if-exists)",
        )

    def test_source_identity_stays_unique_and_populated(self):
        dupes = _count(
            "SELECT COUNT(*) FROM ("
            "SELECT 1 FROM fact_flight WHERE source_system='csv' "
            "GROUP BY source_system, source_flight_id "
            "HAVING COUNT(*) > 1) d"
        )
        self.assertEqual(dupes, 0)
        nulls = _count(
            "SELECT COUNT(*) FROM fact_flight "
            "WHERE source_system='csv' AND source_flight_id IS NULL"
        )
        self.assertEqual(nulls, 0)


class TestOltpEtlRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def test_oltp_load_then_idempotent_second_run(self):
        if not dbutil.oltp_available():
            self.skipTest("OLTP source unreachable")
        from oltp_etl import run_oltp_etl

        _quiet(run_oltp_etl)
        from database import oltp_engine

        with oltp_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM flights")).scalar()

        target = _count(
            "SELECT COUNT(*) FROM fact_flight WHERE source_system='oltp'"
        )
        self.assertEqual(target, source_count)

        fp = _fingerprint(
            "SELECT * FROM fact_flight WHERE source_system='oltp' "
            "ORDER BY flight_key"
        )
        _quiet(run_oltp_etl)
        self.assertEqual(
            _count("SELECT COUNT(*) FROM fact_flight WHERE source_system='oltp'"),
            target,
            "second OLTP run must not insert duplicates",
        )
        self.assertEqual(
            _fingerprint(
                "SELECT * FROM fact_flight WHERE source_system='oltp' "
                "ORDER BY flight_key"
            ),
            fp,
            "second OLTP run must not modify existing rows",
        )
        # documented policy: OLTP rows carry no weather_key
        self.assertEqual(
            _count(
                "SELECT COUNT(*) FROM fact_flight WHERE source_system='oltp' "
                "AND weather_key IS NOT NULL"
            ),
            0,
        )


class TestWeatherAndMasterEntrypoints(unittest.TestCase):
    def test_weather_module_defaults_unchanged(self):
        import weather_etl

        self.assertEqual(
            weather_etl.INSERT_CHUNK_SIZE,
            1000,
            "default weather batch size must stay at the Phase-0 value",
        )
        self.assertTrue(callable(weather_etl.run_weather_etl))

    def test_master_pipeline_entrypoint_intact(self):
        import main_etl

        self.assertTrue(callable(main_etl.run_master_pipeline))

    @unittest.skipUnless(
        os.environ.get("AVIATION_TESTS_WEATHER_NET") == "1",
        "real weather fetch is opt-in (AVIATION_TESTS_WEATHER_NET=1)",
    )
    def test_weather_dry_run_smoke(self):
        from weather_etl import run_weather_etl

        _, output = _quiet(run_weather_etl, dry_run=True, airport_filter=["CMBB"])
        self.assertIsInstance(output, str)


if __name__ == "__main__":
    unittest.main(verbosity=2)
