"""P2 tests: per-airport weather incremental windows, watermark safety
(partial/failed/unverified/coverage-gap runs never advance), the additive
weather override kwargs, batch-size preservation, and the bounded OLTP
change updater.

The change updater is tested twice:
* against a writable local fixture source DB (`aviation_oltp_ci`) - the
  live Aiven source is NEVER written to, and the fixture proves the
  updater's read-only use of whatever source it is given;
* against the real OLTP source (read-only) for end-to-end convergence
  - skipped when unreachable.
"""

import contextlib
import copy
import io
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

import etl_audit  # noqa: E402
import refresh_orchestrator as ro  # noqa: E402
import verify_warehouse  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402

FIXTURE_DB = "aviation_oltp_ci"


def _fresh_config():
    return copy.deepcopy(ro.load_config())


def _passing_verifier(conn):
    return [verify_warehouse.Check("stub", "PASS", "stub")]


def _failing_verifier(conn):
    return [verify_warehouse.Check("stub", "FAIL", "stub failure")]


def _fixture_engine():
    url = __import__("database").dw_conn_str.rsplit("/", 1)[0] + "/" + FIXTURE_DB
    return create_engine(url)


def _fixture_source(engine):
    """Reset the writable local fixture source (engine already targets
    aviation_oltp_ci - created in setUpClass)."""
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS flights ("
            " flight_id INT PRIMARY KEY AUTO_INCREMENT,"
            " flight_number VARCHAR(20), status VARCHAR(20),"
            " scheduled_departure DATETIME,"
            " actual_departure DATETIME NULL,"
            " actual_arrival DATETIME NULL)"
        ))
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS flight_delays ("
            " delay_id INT PRIMARY KEY AUTO_INCREMENT,"
            " flight_id INT, delay_minutes INT)"
        ))
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS flight_cancellations ("
            " cancellation_id INT PRIMARY KEY AUTO_INCREMENT, flight_id INT)"
        ))
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS bookings ("
            " booking_id INT PRIMARY KEY AUTO_INCREMENT,"
            " flight_id INT, booking_status VARCHAR(20))"
        ))
        for table in ("flights", "flight_delays", "flight_cancellations",
                      "bookings"):
            conn.execute(text(f"DELETE FROM {table}"))


def _seed_fixture_flight(fixture):
    """One fully-populated flight with all mutable-attribute inputs."""
    with fixture.begin() as conn:
        conn.execute(text(
            "INSERT INTO flights (flight_id, flight_number, status,"
            " scheduled_departure, actual_departure, actual_arrival) VALUES"
            " (9001, 'FX9001', 'ACTIVE', '2024-05-01 08:00:00',"
            " '2024-05-01 08:10:00', '2024-05-01 10:10:00')"
        ))
        conn.execute(text(
            "INSERT INTO flight_delays (flight_id, delay_minutes) "
            "VALUES (9001, 25)"
        ))
        conn.execute(text(
            "INSERT INTO bookings (flight_id, booking_status) VALUES"
            " (9001, 'CONFIRMED'), (9001, 'CONFIRMED'),"
            " (9001, 'CANCELLED')"
        ))


class TestWeatherWatermarkLogic(unittest.TestCase):
    FAKE_AIRPORTS = {
        code: {"lat": 0.0, "lon": 0.0, "tz": "UTC", "icao": code}
        for code in (
            "WTOA", "WTWB", "WTCA", "WTCB", "WTCC", "WTCD", "WTCF",
            "WTC1", "WTC2", "WTC3", "WTC4", "WTC5", "WTCP", "WTCX",
            "WTCY", "WTCH", "WTCDUP", "WTFU", "WTST", "WTMF",
        )
    }
    FAKE_AIRPORTS["WTLH"] = {
        "lat": 51.4700, "lon": -0.4543, "tz": "Europe/London", "icao": "EGLL"
    }
    FAKE_AIRPORTS["WTSF"] = {
        "lat": 51.4700, "lon": -0.4543, "tz": "Europe/London", "icao": "EGLL"
    }
    FAKE_AIRPORTS["WTFB"] = {
        "lat": 51.4700, "lon": -0.4543, "tz": "Europe/London", "icao": "EGLL"
    }
    FAKE_AIRPORTS["WTCO"] = {
        "lat": 6.9271, "lon": 79.8612, "tz": "Asia/Colombo", "icao": "VCBI"
    }

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()
        import weather_etl

        weather_etl.AIRPORT_INFO.update(self.FAKE_AIRPORTS)

    # ---------- window derivation ----------

    def test_incremental_off_always_returns_none(self):
        etl_audit.record_watermark("weather", "CMBB", "2024-03-05 00:00:00")
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = False
        self.assertIsNone(ro.weather_start_overrides(config))

    def test_incremental_on_without_watermarks_returns_none(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        self.assertIsNone(
            ro.weather_start_overrides(config),
            "first run must be full-range (establishes watermarks)",
        )

    def test_windows_are_per_airport_watermark_minus_overlap(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        config["weather_incremental"]["overlap_days"] = 7
        etl_audit.record_watermark("weather", "CMBB", "2024-03-05 00:00:00")
        etl_audit.record_watermark("weather", "KEAA", "2024-03-01 12:00:00")
        overrides = ro.weather_start_overrides(config)
        self.assertEqual(
            overrides,
            {
                "CMBB": date(2024, 3, 5) - timedelta(days=7),
                "KEAA": date(2024, 3, 1) - timedelta(days=7),
            },
            "each airport must use its own watermark, not one global minimum",
        )
        self.assertNotEqual(
            overrides["CMBB"], overrides["KEAA"],
            "a single global window would give both airports the same start",
        )

    def test_utc_watermark_start_override_uses_airport_local_date(self):
        import weather_etl

        airport = "WTCE"
        weather_etl.AIRPORT_INFO[airport] = {
            "lat": 6.9271, "lon": 79.8612,
            "tz": "Asia/Colombo", "icao": "VCBI",
        }
        etl_audit.record_watermark(
            "weather", airport, "2024-06-03 20:00:00Z"
        )

        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True

        self.assertEqual(
            ro.weather_start_overrides(config),
            {airport: date(2024, 5, 28)},
            "UTC watermark must be converted to the airport local date before "
            "subtracting the overlap",
        )

    def test_zero_overlap_uses_the_watermark_date(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        config["weather_incremental"]["overlap_days"] = 0
        etl_audit.record_watermark("weather", "CMBB", "2024-03-05 23:00:00")
        self.assertEqual(
            ro.weather_start_overrides(config), {"CMBB": date(2024, 3, 5)}
        )

    def test_airports_without_watermarks_are_absent_from_overrides(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        etl_audit.record_watermark("weather", "CMBB", "2024-03-05 00:00:00")
        overrides = ro.weather_start_overrides(config)
        self.assertEqual(set(overrides), {"CMBB"},
                         "a missing watermark must stay absent so that airport "
                         "fetches its full history")

    def test_unreadable_watermark_falls_back_to_full_history(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        etl_audit.record_watermark("weather", "CMBB", "not-a-date")
        etl_audit.record_watermark("weather", "KEAA", "2024-03-01 12:00:00")
        overrides = ro.weather_start_overrides(config)
        self.assertEqual(
            set(overrides), {"KEAA"},
            "an unreadable watermark must not produce a guessed window",
        )
        self.assertNotIn("CMBB", overrides)

    def test_aware_watermark_is_converted_to_airport_local_date(self):
        self.assertEqual(
            ro._parse_watermark_date("2024-03-05T20:00:00+00:00", "CMB"),
            date(2024, 3, 6),
            "CMB is UTC+5:30, so 20:00Z is the next local calendar day",
        )

    def test_negative_overlap_is_rejected(self):
        import json
        import tempfile

        config = _fresh_config()
        config["weather_incremental"]["overlap_days"] = -1
        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False
        ) as handle:
            json.dump(config, handle)
            path = handle.name
        try:
            with self.assertRaises(ValueError):
                ro.load_config(path)
        finally:
            os.unlink(path)

    # ---------- advancement gating ----------

    def test_watermark_advances_only_when_fetch_is_sufficient(self):
        airport_a, airport_b = self._seed_airports_and_flights()

        from database import dw_engine

        advanced, held = ro.advance_weather_watermarks("batch-1")
        marks = etl_audit.get_watermarks("weather")

        self.assertIn(airport_a, advanced,
                      "complete coverage through the expected end -> advance")
        self.assertNotIn(airport_a, held)
        self.assertIn(airport_b, held, "incomplete fetch -> hold")
        self.assertNotIn(airport_b, marks)

        # persisted value is the observed max, tagged with the batch
        with dw_engine.connect() as conn:
            stored = conn.execute(
                text(
                    "SELECT watermark_value, last_batch_guid "
                    "FROM etl_watermark "
                    "WHERE source_name='weather' AND window_key=:a"
                ),
                {"a": airport_a},
            ).one()
        self.assertTrue(stored[0].startswith("2024-04-01"))
        self.assertEqual(stored[1], "batch-1")

        # a later insufficient observation keeps the OLD watermark (no
        # self-degradation on partial data)
        ro.advance_weather_watermarks("batch-2")
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport_a], stored[0]
        )

    def test_orchestrator_passes_per_airport_windows_only_when_incremental(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO etl_watermark (source_name, window_key,"
                " watermark_value, updated_at, last_batch_guid) VALUES"
                " ('weather', 'WWWW', '2024-03-05 00:00:00', NOW(3), 'b0'),"
                " ('weather', 'VVVV', '2024-03-01 00:00:00', NOW(3), 'b0')"
            ))
        seen = []

        def weather_stage(**kwargs):
            seen.append(kwargs)
            print("airports failed : []")

        stages = {"weather": weather_stage}
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        config["order"] = ["weather"]

        code = ro.run_refresh(
            config, stages=stages, verifier=_passing_verifier,
            sources={"weather"},
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual(len(seen), 1)
        self.assertEqual(
            seen[0].get("start_date_overrides"),
            {
                "WWWW": date(2024, 3, 5) - timedelta(days=7),
                "VVVV": date(2024, 3, 1) - timedelta(days=7),
            },
        )
        self.assertNotIn("start_date_override", seen[0])

        # same config with incremental disabled: kwargs must NOT be passed
        seen.clear()
        config["weather_incremental"]["enabled"] = False
        code = ro.run_refresh(
            config, stages=stages, verifier=_passing_verifier,
            sources={"weather"},
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual(seen, [{}],
                         "default call path must stay override-free")

    # ---------- verification gate on watermark advancement ----------

    def _incremental_weather_config(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        config["order"] = ["weather"]
        return config

    @staticmethod
    def _clean_weather_stage(**kwargs):
        return {"airports_failed": []}

    def test_watermark_advances_only_when_stage_and_verification_succeed(self):
        airport_a, _ = self._seed_airports_and_flights()
        code = ro.run_refresh(
            self._incremental_weather_config(),
            stages={"weather": self._clean_weather_stage},
            verifier=_passing_verifier, sources={"weather"},
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertIn(airport_a, etl_audit.get_watermarks("weather"))

    def test_watermark_not_advanced_when_verification_skipped(self):
        airport_a, _ = self._seed_airports_and_flights()
        code = ro.run_refresh(
            self._incremental_weather_config(),
            stages={"weather": self._clean_weather_stage},
            verifier=_passing_verifier, sources={"weather"},
            verify=False,
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertNotIn(
            airport_a, etl_audit.get_watermarks("weather"),
            "--no-verify must suppress watermark advancement",
        )

    def test_watermark_not_advanced_when_verification_fails(self):
        airport_a, _ = self._seed_airports_and_flights()
        code = ro.run_refresh(
            self._incremental_weather_config(),
            stages={"weather": self._clean_weather_stage},
            verifier=_failing_verifier, sources={"weather"},
        )
        self.assertEqual(code, ro.EXIT_VERIFY_FAILED)
        self.assertNotIn(
            airport_a, etl_audit.get_watermarks("weather"),
            "failed verification must suppress watermark advancement",
        )

    def test_partial_weather_stage_never_advances_watermarks(self):
        airport_a, airport_b = self._seed_airports_and_flights()

        def partial_stage(**kwargs):
            return {"airports_failed": ["WTOA"]}

        code = ro.run_refresh(
            self._incremental_weather_config(),
            stages={"weather": partial_stage},
            verifier=_passing_verifier,
            sources={"weather"},
        )

        self.assertEqual(code, ro.EXIT_STAGE_FAILED)
        marks = etl_audit.get_watermarks("weather")
        self.assertNotIn(
            airport_a, marks,
            "a PARTIAL weather stage must not advance any watermark",
        )
        self.assertNotIn(
            airport_b, marks,
            "a PARTIAL weather stage must not advance any watermark",
        )

    def test_unexpected_advancement_failure_is_reported_and_fails_safely(self):
        self._seed_airports_and_flights()
        with mock.patch.object(
            ro, "advance_weather_watermarks",
            side_effect=RuntimeError("coverage query failed"),
        ):
            code = ro.run_refresh(
                self._incremental_weather_config(),
                stages={"weather": self._clean_weather_stage},
                verifier=_passing_verifier,
                sources={"weather"},
            )
        self.assertEqual(code, ro.EXIT_AUDIT_FINISH)
        self.assertEqual(etl_audit.get_watermarks("weather"), {})

    def test_watermark_write_failure_after_commit_is_reported(self):
        airport_a, _ = self._seed_airports_and_flights()
        with mock.patch.object(
            etl_audit, "record_watermark",
            side_effect=etl_audit.AuditError("controlled write failure"),
        ):
            code = ro.run_refresh(
                self._incremental_weather_config(),
                stages={"weather": self._clean_weather_stage},
                verifier=_passing_verifier,
                sources={"weather"},
            )

        self.assertEqual(code, ro.EXIT_AUDIT_FINISH)
        self.assertNotIn(airport_a, etl_audit.get_watermarks("weather"))

        from database import dw_engine

        with dw_engine.connect() as conn:
            persisted = conn.execute(text(
                "SELECT COUNT(*) FROM dim_weather w "
                "JOIN dim_airport a ON w.airport_key=a.airport_key "
                "WHERE a.airport_code=:a"
            ), {"a": airport_a}).scalar()
        self.assertGreaterEqual(
            persisted, 1,
            "committed weather data must survive a watermark write failure",
        )

    def test_watermark_not_advanced_when_stage_fails(self):
        airport_a, _ = self._seed_airports_and_flights()

        def exploding(**kwargs):
            raise RuntimeError("weather exploded")

        code = ro.run_refresh(
            self._incremental_weather_config(),
            stages={"weather": exploding},
            verifier=_passing_verifier, sources={"weather"},
        )
        self.assertEqual(code, ro.EXIT_STAGE_FAILED)
        self.assertNotIn(airport_a, etl_audit.get_watermarks("weather"))

    # ---------- independent coverage checks ----------

    def test_incomplete_persisted_range_holds_watermark(self):
        airport = self._seed_airport_series(
            "WTCA",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[date(2024, 6, 1), date(2024, 6, 3)],
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-gap", _fresh_config()
        )
        self.assertIn(airport, held, "a missing calendar day must block advance")
        self.assertNotIn(airport, advanced)
        self.assertNotIn(airport, etl_audit.get_watermarks("weather"))

    def test_complete_persisted_range_advances_to_expected_end(self):
        airport = self._seed_airport_series(
            "WTCB",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[
                date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
            ],
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-complete", _fresh_config()
        )
        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        marks = etl_audit.get_watermarks("weather")
        self.assertIn(airport, marks)
        self.assertTrue(marks[airport].startswith("2024-06-03"))

    def test_future_flights_and_stale_rows_cannot_inflate_watermark(self):
        import weather_etl

        airport = self._seed_airport_series(
            "WTCC",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 30)],
            observation_dates=[
                date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3),
                date(2024, 6, 10),
            ],
        )
        with mock.patch.object(
            weather_etl, "ARCHIVE_MAX_DATE", date(2024, 6, 3)
        ):
            advanced, held = ro.advance_weather_watermarks(
                "batch-cap", _fresh_config()
            )
        self.assertIn(airport, advanced)
        marks = etl_audit.get_watermarks("weather")
        self.assertTrue(
            marks[airport].startswith("2024-06-03"),
            "the expected end is capped at archive coverage and the stale "
            "persisted maximum (2024-06-10) must not be used",
        )

    def test_legacy_rows_cannot_mask_utc_coverage_gap(self):
        airport = self._seed_airport_series(
            "WTCA",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[
                date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
            ],
        )
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[date(2024, 6, 1)],
            source_model="best_match_utc_v1",
        )

        advanced, held = ro.advance_weather_watermarks(
            "batch-mixed-gap", _fresh_config()
        )

        self.assertIn(airport, held)
        self.assertIn("incomplete persisted coverage", held[airport])
        self.assertNotIn(airport, advanced)
        self.assertNotIn(airport, etl_audit.get_watermarks("weather"))

    def test_utc_rows_outside_window_still_force_utc_basis(self):
        airport = self._seed_airport_series(
            "WTCF",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[
                date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
            ],
        )
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[date(2024, 1, 1)],
            source_model="best_match_utc_v1",
        )

        advanced, held = ro.advance_weather_watermarks(
            "batch-utc-outside-window", _fresh_config()
        )

        self.assertIn(airport, held)
        self.assertIn("in the UTC model", held[airport])
        self.assertNotIn(airport, advanced)
        self.assertNotIn(airport, etl_audit.get_watermarks("weather"))

    def test_utc_coverage_advances_with_utc_watermark_basis(self):
        airport = self._seed_airport_series(
            "WTCB",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[date(2024, 6, 1)],
        )
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[
                date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
            ],
            source_model="best_match_utc_v1",
            hourly=True,
        )

        advanced, held = ro.advance_weather_watermarks(
            "batch-utc-complete", _fresh_config()
        )

        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        mark = etl_audit.get_watermarks("weather")[airport]
        self.assertEqual(mark, "2024-06-03 23:00:00Z")

    def test_utc_future_observation_does_not_inflate_watermark(self):
        airport = "WTFU"
        expected = ro.generate_expected_utc_instants(
            date(2024, 6, 1), date(2024, 6, 3), "UTC"
        )
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=expected + [expected[-1] + timedelta(days=30)],
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-utc-future", _fresh_config()
        )
        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2024-06-03 23:00:00Z",
            "a future out-of-window observation must not advance the watermark",
        )

    def test_utc_stale_observation_does_not_inflate_watermark(self):
        airport = "WTST"
        expected = ro.generate_expected_utc_instants(
            date(2024, 6, 1), date(2024, 6, 3), "UTC"
        )
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=expected + [expected[0] - timedelta(days=30)],
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-utc-stale", _fresh_config()
        )
        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2024-06-03 23:00:00Z",
            "a stale out-of-window observation must not change candidate selection",
        )

    def test_utc_missing_final_observation_holds_watermark(self):
        airport = "WTMF"
        expected = ro.generate_expected_utc_instants(
            date(2024, 6, 1), date(2024, 6, 3), "UTC"
        )
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=expected[:-1],
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-utc-missing-final", _fresh_config()
        )
        self.assertIn(airport, held)
        self.assertNotIn(airport, advanced)
        self.assertIn("1 observation(s) missing", held[airport])
        self.assertIn(str(expected[-1]), held[airport])
        self.assertNotIn(airport, etl_audit.get_watermarks("weather"))

    def test_lhr_summer_start_padded_coverage_advances_to_expected_end(self):
        airport = "WTLH"
        expected = ro.generate_expected_utc_instants(
            date(2024, 6, 1), date(2024, 6, 3), "Europe/London"
        )
        self.assertEqual(len(expected), 72)
        self.assertEqual(expected[-1], datetime(2024, 6, 3, 22, 0))
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=expected,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-lhr-summer", _fresh_config()
        )
        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2024-06-03 22:00:00Z",
        )

    def test_leap_day_expected_instants(self):
        self.assertEqual(
            len(ro.generate_expected_utc_instants(
                date(2024, 2, 28), date(2024, 3, 1), "UTC"
            )),
            72,
        )
        self.assertEqual(
            len(ro.generate_expected_utc_instants(
                date(2024, 2, 29), date(2024, 2, 29), "UTC"
            )),
            24,
        )

    def test_lhr_full_archive_span_expected_instants(self):
        expected = ro.generate_expected_utc_instants(
            date(2025, 1, 9), date(2025, 12, 21), "Europe/London"
        )
        self.assertEqual(len(expected), 8328)

    def test_legacy_local_watermark_is_compared_in_utc(self):
        import weather_etl

        airport = "WTCE"
        weather_etl.AIRPORT_INFO[airport] = {
            "lat": 6.9271, "lon": 79.8612,
            "tz": "Asia/Colombo", "icao": "VCBI",
        }
        expected_instants = ro.generate_expected_utc_instants(
            date(2024, 6, 1), date(2024, 6, 3), "Asia/Colombo"
        )
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=expected_instants,
        )
        etl_audit.record_watermark(
            "weather", airport, "2024-06-03 20:00:00"
        )

        advanced, held = ro.advance_weather_watermarks(
            "batch-local-to-utc", _fresh_config()
        )

        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2024-06-03 17:30:00Z",
        )
    def test_watermark_never_moves_backwards_when_window_shrinks(self):
        airport = self._seed_airport_series(
            "WTCD",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 15)],
            observation_dates=[
                date(2024, 6, 1) + timedelta(days=offset)
                for offset in range(15)
            ],
        )
        etl_audit.record_watermark(
            "weather", airport, "2024-06-20 12:00:00"
        )

        advanced, held = ro.advance_weather_watermarks(
            "batch-shrink", _fresh_config()
        )

        self.assertIn(airport, held)
        self.assertNotIn(airport, advanced)
        marks = etl_audit.get_watermarks("weather")
        self.assertTrue(
            marks[airport].startswith("2024-06-20"),
            "a shorter validated window must not rewind an existing watermark",
        )

    def test_repeated_runs_keep_watermark_value_and_audit_rows(self):
        airport, _ = self._seed_airports_and_flights()
        config = self._incremental_weather_config()
        for _ in range(2):
            code = ro.run_refresh(
                config,
                stages={"weather": self._clean_weather_stage},
                verifier=_passing_verifier,
                sources={"weather"},
            )
            self.assertEqual(code, ro.EXIT_OK)
        marks = etl_audit.get_watermarks("weather")
        self.assertIn(airport, marks)
        self.assertTrue(marks[airport].startswith("2024-04-01"))
        from database import dw_engine

        with dw_engine.connect() as conn:
            runs = conn.execute(text(
                "SELECT status FROM etl_batch_log "
                "WHERE source_name='weather' ORDER BY run_id"
            )).fetchall()
        self.assertEqual([row[0] for row in runs], ["SUCCESS", "SUCCESS"])

    # ---------- full-span UTC coverage verification gate ----------

    def test_full_span_coverage_succeeds_only_when_entire_utc_range_present(self):
        airport_a = self._seed_airport_series(
            "WTC1",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[
                date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
            ],
            source_model="best_match_utc_v1",
            hourly=True,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-full-span", _fresh_config()
        )
        self.assertIn(airport_a, advanced)
        self.assertNotIn(airport_a, held)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport_a],
            "2024-06-03 23:00:00Z",
        )

        # Case B: Omit an interior hour
        instants_71 = ro.generate_expected_utc_instants(
            date(2024, 6, 1), date(2024, 6, 3), "UTC"
        )
        missing_instant = instants_71.pop(35)
        airport_b = self._seed_airport_series(
            "WTC2",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=instants_71,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-interior-gap", _fresh_config()
        )
        self.assertIn(airport_b, held)
        self.assertNotIn(airport_b, advanced)
        self.assertIn("incomplete persisted coverage", held[airport_b])
        self.assertIn(str(missing_instant), held[airport_b])

    def test_missing_early_historical_day_blocks_advancement_even_if_latest_seven_days_complete(self):
        flight_dates = [date(2024, 6, 1) + timedelta(days=i) for i in range(10)]
        obs_dates = [date(2024, 6, 4) + timedelta(days=i) for i in range(7)]
        airport = self._seed_airport_series(
            "WTC3",
            flight_dates=flight_dates,
            observation_dates=obs_dates,
            source_model="best_match_utc_v1",
            hourly=True,
        )
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        config["weather_incremental"]["overlap_days"] = 7
        advanced, held = ro.advance_weather_watermarks(
            "batch-early-gap", config
        )
        self.assertIn(airport, held)
        self.assertNotIn(airport, advanced)
        self.assertIn("incomplete persisted coverage", held[airport])
        self.assertIn("2024-06-01", held[airport])

    def test_legacy_rows_cannot_satisfy_utc_coverage(self):
        airport = self._seed_airport_series(
            "WTC4",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[date(2024, 6, 1)],
            source_model="best_match",
            hourly=True,
        )
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[date(2024, 6, 2), date(2024, 6, 3)],
            source_model="best_match_utc_v1",
            hourly=True,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-legacy-mask-full", _fresh_config()
        )
        self.assertIn(airport, held)
        self.assertNotIn(airport, advanced)
        self.assertIn("incomplete persisted coverage", held[airport])
        self.assertIn("2024-06-01", held[airport])

    def test_legacy_watermark_ahead_of_utc_time_cannot_bypass_verification(self):
        airport = self._seed_airport_series(
            "WTC5",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 10)],
            observation_dates=[
                date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
            ],
            source_model="best_match_utc_v1",
            hourly=True,
        )
        etl_audit.record_watermark("weather", airport, "2024-06-20 12:00:00")

        advanced, held = ro.advance_weather_watermarks(
            "batch-ahead-watermark", _fresh_config()
        )
        self.assertIn(airport, held)
        self.assertNotIn(airport, advanced)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2024-06-20 12:00:00",
        )

    def test_dst_spring_forward_boundary(self):
        airport = "WTSF"
        expected = ro.generate_expected_utc_instants(
            date(2025, 3, 30), date(2025, 3, 30), "Europe/London"
        )
        self.assertEqual(len(expected), 23)

        self._seed_airport_series(
            airport,
            flight_dates=[date(2025, 3, 30)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=expected,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-dst-spring", _fresh_config()
        )
        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2025-03-30 22:00:00Z",
        )

    def test_dst_fall_back_boundary(self):
        airport = "WTFB"
        expected = ro.generate_expected_utc_instants(
            date(2025, 10, 26), date(2025, 10, 26), "Europe/London"
        )
        self.assertEqual(len(expected), 25)

        self._seed_airport_series(
            airport,
            flight_dates=[date(2025, 10, 26)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=expected,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-dst-fall", _fresh_config()
        )
        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2025-10-26 23:00:00Z",
        )

    def test_half_hour_timezone_offsets(self):
        airport = "WTCO"
        expected = ro.generate_expected_utc_instants(
            date(2025, 1, 9), date(2025, 1, 9), "Asia/Colombo"
        )
        self.assertEqual(len(expected), 24)
        for t in expected:
            self.assertEqual(t.minute, 30)

        self._seed_airport_series(
            airport,
            flight_dates=[date(2025, 1, 9)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=expected,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-half-hour", _fresh_config()
        )
        self.assertIn(airport, advanced)
        self.assertNotIn(airport, held)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2025-01-09 17:30:00Z",
        )

    def test_duplicate_utc_instants_block_advancement(self):
        instants = ro.generate_expected_utc_instants(
            date(2024, 6, 1), date(2024, 6, 1), "UTC"
        )
        fake_rows = [
            (t, "best_match_utc_v1", 2 if i == 5 else 1)
            for i, t in enumerate(instants)
        ]
        mock_conn = mock.MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = fake_rows
        gate = ro.verify_utc_weather_coverage(
            mock_conn, "WTCDUP", date(2024, 6, 1), date(2024, 6, 1), "UTC"
        )
        self.assertFalse(gate.passed)
        self.assertIn("duplicate UTC instants detected", gate.reason)
        self.assertEqual(gate.duplicate_count, 1)

    def test_missing_provider_data_and_incomplete_archive_responses(self):
        airport = "WTCP"
        partial_instants = ro.generate_expected_utc_instants(
            date(2024, 6, 1), date(2024, 6, 1), "UTC"
        )[:12]
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1)],
            observation_dates=[],
            source_model="best_match_utc_v1",
            observation_times=partial_instants,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-provider-truncated", _fresh_config()
        )
        self.assertIn(airport, held)
        self.assertNotIn(airport, advanced)
        self.assertIn("incomplete persisted coverage", held[airport])
        self.assertIn("12 observation(s) missing", held[airport])

    def test_wrong_source_models_and_invalid_timestamps(self):
        from database import dw_engine

        airport_x = self._seed_airport_series(
            "WTCX",
            flight_dates=[date(2024, 6, 1)],
            observation_dates=[date(2024, 6, 1)],
            source_model="reanalysis_v1",
            hourly=True,
        )
        with dw_engine.connect() as conn:
            gate_x = ro.verify_utc_weather_coverage(
                conn, airport_x, date(2024, 6, 1), date(2024, 6, 1), "UTC"
            )
        self.assertFalse(gate_x.passed)
        self.assertIn("in the UTC model", gate_x.reason)

        airport_y = "WTCY"
        self._seed_airport_series(
            airport_y,
            flight_dates=[date(2024, 6, 1)],
            observation_dates=[date(2024, 6, 1)],
            source_model="best_match_utc_v1",
            hourly=True,
        )
        with dw_engine.begin() as conn:
            airport_key = conn.execute(
                text("SELECT airport_key FROM dim_airport WHERE airport_code=:c"),
                {"c": airport_y},
            ).scalar()
            conn.execute(
                text(
                    "INSERT INTO dim_weather (airport_key, observation_time, temperature, source_dataset, source_model) "
                    "VALUES (:k, NULL, 25.0, 'open-meteo-archive', 'best_match_utc_v1')"
                ),
                {"k": airport_key},
            )
        with dw_engine.connect() as conn:
            gate_y = ro.verify_utc_weather_coverage(
                conn, airport_y, date(2024, 6, 1), date(2024, 6, 1), "UTC"
            )
        self.assertFalse(gate_y.passed)
        self.assertIn("invalid timestamps detected", gate_y.reason)

    def test_watermark_remains_unchanged_when_any_gate_fails(self):
        airport = "WTCH"
        etl_audit.record_watermark("weather", airport, "2024-05-01 12:00:00")
        self._seed_airport_series(
            airport,
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[date(2024, 6, 1)],
            source_model="best_match_utc_v1",
            hourly=True,
        )
        advanced, held = ro.advance_weather_watermarks(
            "batch-fail-unchanged", _fresh_config()
        )
        self.assertIn(airport, held)
        self.assertNotIn(airport, advanced)
        self.assertEqual(
            etl_audit.get_watermarks("weather")[airport],
            "2024-05-01 12:00:00",
        )

    def test_successful_verification_proposes_advancement_without_production_write(self):
        from database import dw_engine

        airport = self._seed_airport_series(
            "WTC1",
            flight_dates=[date(2024, 6, 1), date(2024, 6, 3)],
            observation_dates=[
                date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
            ],
            source_model="best_match_utc_v1",
            hourly=True,
        )
        with dw_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM etl_watermark WHERE source_name='weather' AND window_key=:a"),
                {"a": airport},
            )
        advanced, held = ro.advance_weather_watermarks(
            "batch-propose-dry", _fresh_config(), dry_run=True
        )
        self.assertIn(airport, advanced)
        self.assertEqual(advanced[airport], "2024-06-03 23:00:00Z")
        self.assertNotIn(airport, held)
        marks = etl_audit.get_watermarks("weather")
        self.assertNotIn(airport, marks)

    def test_coverage_gate_result_reporting_format(self):
        res = ro.CoverageGateResult(
            airport_code="LHR",
            passed=False,
            coverage_basis="utc",
            expected_range=(datetime(2024, 6, 1, 0, 0), datetime(2024, 6, 3, 23, 0)),
            verified_range=(datetime(2024, 6, 1, 0, 0), datetime(2024, 6, 2, 23, 0)),
            total_expected=72,
            total_verified=48,
            missing_count=24,
            missing_intervals=[(datetime(2024, 6, 3, 0, 0), datetime(2024, 6, 3, 23, 0))],
            duplicate_count=2,
            reason="incomplete persisted coverage: 24 observation(s) missing",
        )
        summary = res.summary()
        self.assertIn("LHR", res.format_report())
        self.assertIn("incomplete persisted coverage", summary)
        self.assertIn("expected:", summary)
        self.assertIn("verified:", summary)
        self.assertIn("missing_intervals:", summary)
        self.assertIn("duplicates: 2", summary)
        as_dict = res.as_dict()
        self.assertEqual(as_dict["airport_code"], "LHR")
        self.assertFalse(as_dict["passed"])
        self.assertEqual(as_dict["duplicate_count"], 2)


    def test_launchd_refresh_plist_template_is_valid_and_single_daily_job(self):
        import plistlib

        plist_path = os.path.join(
            ROOT, "scripts", "launchd", "com.aviation.dw.refresh.plist.template"
        )
        retired_path = os.path.join(
            ROOT, "scripts", "launchd", "com.aviation.dw.weather.plist.template"
        )
        self.assertTrue(os.path.exists(plist_path))
        self.assertFalse(
            os.path.exists(retired_path),
            "the duplicate weather-only LaunchAgent must stay retired",
        )
        with open(plist_path, "r", encoding="utf-8") as handle:
            content = handle.read()
        lowered = content.lower()
        self.assertIn("current system timezone", lowered)
        self.assertIn("asia/colombo", lowered)
        self.assertNotIn("guarantee", lowered)
        self.assertIn("launchctl bootstrap", content)
        self.assertIn("launchctl bootout", content)
        self.assertIn("launchctl print", content)
        self.assertIn("launchctl kickstart", content)
        self.assertNotIn("launchctl load", content)

        rendered = content.replace("__REPO_PATH__", ROOT)
        data = plistlib.loads(rendered.encode("utf-8"))
        self.assertEqual(data.get("Label"), "com.aviation.dw.refresh")
        self.assertEqual(data.get("WorkingDirectory"), ROOT)
        interval = data.get("StartCalendarInterval")
        self.assertEqual(interval.get("Hour"), 2)
        self.assertEqual(interval.get("Minute"), 0)
        self.assertIn(
            "scripts/scheduled_refresh.sh",
            data.get("ProgramArguments", [""])[0],
        )
        path_value = data.get("EnvironmentVariables", {}).get("PATH", "")
        self.assertIn("/opt/homebrew/bin", path_value)

    def test_scheduled_refresh_script_syntax_and_structure(self):
        import subprocess

        script_path = os.path.join(ROOT, "scripts", "scheduled_refresh.sh")
        self.assertTrue(os.path.exists(script_path))
        proc = subprocess.run(
            ["bash", "-n", script_path], capture_output=True, text=True
        )
        self.assertEqual(proc.returncode, 0, f"bash -n failed: {proc.stderr}")

        with open(script_path, "r", encoding="utf-8") as handle:
            script_text = handle.read()
        self.assertIn('PYTHON="$REPO/venv/bin/python"', script_text)
        self.assertNotIn("command -v python3", script_text)
        self.assertIn(
            "import pymysql, sqlalchemy, pandas, dotenv, requests",
            script_text,
        )
        self.assertIn("ETL_LOG_RETENTION_DAYS", script_text)
        self.assertNotIn("find ", script_text)
        self.assertNotIn("-delete", script_text)
        self.assertIn('exit "$CODE"', script_text)

    def test_prune_old_logs(self):
        import tempfile
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            fresh_log = os.path.join(tmpdir, "scheduled_refresh_20261031_000000.log")
            with open(fresh_log, "w", encoding="utf-8") as h:
                h.write("fresh log")

            old_log = os.path.join(tmpdir, "scheduled_refresh_20260901_000000.log")
            with open(old_log, "w", encoding="utf-8") as h:
                h.write("old log")
            old_time = time.time() - (45 * 86400)
            os.utime(old_log, (old_time, old_time))

            ro.prune_old_logs(tmpdir, retention_days=30, keep_latest=1)
            self.assertTrue(os.path.exists(fresh_log), "fresh log must be retained")
            self.assertFalse(os.path.exists(old_log), "log older than 30 days must be pruned")

    def test_prune_old_logs_keeps_launchd_and_newest_ten(self):
        import tempfile
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            old_time = time.time() - (45 * 86400)
            launchd_log = os.path.join(tmpdir, "launchd_refresh.log")
            launchd_err = os.path.join(tmpdir, "launchd_refresh.err")
            for path in (launchd_log, launchd_err):
                with open(path, "w", encoding="utf-8") as h:
                    h.write("launchd-owned")
                os.utime(path, (old_time, old_time))

            refresh_logs = []
            for index in range(12):
                path = os.path.join(
                    tmpdir, f"refresh_20260101_{index:02d}.log"
                )
                with open(path, "w", encoding="utf-8") as h:
                    h.write(f"run {index}")
                os.utime(path, (old_time, old_time))
                refresh_logs.append(path)

            ro.prune_old_logs(tmpdir, retention_days=30, keep_latest=10)
            remaining = sorted(
                name for name in os.listdir(tmpdir)
                if name.startswith("refresh_")
            )
            self.assertEqual(len(remaining), 10)
            self.assertIn("refresh_20260101_11.log", remaining)
            self.assertNotIn("refresh_20260101_00.log", remaining)
            self.assertTrue(os.path.exists(launchd_log))
            self.assertTrue(os.path.exists(launchd_err))

    def test_prune_old_logs_invalid_env_falls_back_to_30_days(self):
        import tempfile
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            fresh_log = os.path.join(tmpdir, "refresh_20261031_000000.log")
            with open(fresh_log, "w", encoding="utf-8") as h:
                h.write("fresh log")
            old_log = os.path.join(tmpdir, "refresh_20260901_000000.log")
            with open(old_log, "w", encoding="utf-8") as h:
                h.write("old log")
            old_time = time.time() - (45 * 86400)
            os.utime(old_log, (old_time, old_time))

            with mock.patch.dict(
                os.environ, {"ETL_LOG_RETENTION_DAYS": "not-an-integer"}
            ):
                ro.prune_old_logs(tmpdir, keep_latest=1)
            self.assertFalse(os.path.exists(old_log))
            self.assertTrue(os.path.exists(fresh_log))

    def test_prune_old_logs_zero_disables_and_explicit_value_wins(self):
        import tempfile
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            fresh_log = os.path.join(tmpdir, "refresh_20261031_000000.log")
            with open(fresh_log, "w", encoding="utf-8") as h:
                h.write("fresh log")
            old_log = os.path.join(tmpdir, "refresh_20260901_000000.log")
            with open(old_log, "w", encoding="utf-8") as h:
                h.write("old log")
            old_time = time.time() - (45 * 86400)
            os.utime(old_log, (old_time, old_time))

            with mock.patch.dict(os.environ, {"ETL_LOG_RETENTION_DAYS": "0"}):
                ro.prune_old_logs(tmpdir, keep_latest=1)
            self.assertTrue(os.path.exists(old_log), "0 must disable pruning")

            ro.prune_old_logs(tmpdir, retention_days=30, keep_latest=1)
            self.assertFalse(
                os.path.exists(old_log),
                "an explicit retention_days value must win over the environment",
            )

    def test_show_refresh_status_runs_without_error(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = ro.show_refresh_status(limit=2)
        self.assertEqual(code, 0)
        output = out.getvalue()
        self.assertIn("RECENT REFRESH RUNS & WATERMARKS", output)
    # ---------- stage-level per-airport windows ----------

    @staticmethod
    def _archive_frame(start_date):
        import pandas as pd

        return pd.DataFrame({
            "time_utc": [pd.Timestamp(f"{start_date} 00:00:00", tz="UTC")],
            "temperature_2m": [25.0],
            "relative_humidity_2m": [60.0],
            "precipitation": [0.0],
            "wind_speed_10m": [5.0],
            "cloud_cover": [10.0],
            "weather_code": [0],
        })

    def _run_weather_stage(self, overrides=None, shared_override=None,
                           failing=()):
        calls, _, _ = self._run_weather_stage_capture(
            overrides=overrides, shared_override=shared_override,
            failing=failing,
        )
        return calls

    def _run_weather_stage_capture(self, overrides=None, shared_override=None,
                                   failing=()):
        import pandas as pd

        import weather_etl

        flights = pd.DataFrame({
            "source_system": ["csv"] * 6,
            "source_flight_id": [f"w{i}" for i in range(6)],
            "scheduled_departure": pd.to_datetime([
                "2024-03-01 10:00:00", "2024-03-31 10:00:00",
                "2024-03-01 11:00:00", "2024-03-31 11:00:00",
                "2024-03-01 12:00:00", "2024-03-31 12:00:00",
            ]),
            "airport_code": ["CMB", "CMB", "DXB", "DXB", "LHR", "LHR"],
        })
        calls = []

        def fake_archive(code, info, start_date, end_date):
            calls.append((code, start_date, end_date))
            if code in failing:
                raise RuntimeError("controlled archive failure")
            return self._archive_frame(start_date)

        def fake_visibility(icao, tz, start_date, end_date):
            return pd.DataFrame(columns=["time_utc", "visibility_km"])

        out = io.StringIO()
        result = None
        with mock.patch.object(
            weather_etl, "_read_flights", return_value=flights
        ), mock.patch.object(
            weather_etl, "_read_airport_keys",
            return_value={"CMB": 1, "DXB": 2, "LHR": 3},
        ), mock.patch.object(
            weather_etl, "fetch_archive_weather", side_effect=fake_archive
        ), mock.patch.object(
            weather_etl, "fetch_iem_visibility", side_effect=fake_visibility
        ), contextlib.redirect_stdout(out):
            result = weather_etl.run_weather_etl(
                dry_run=True,
                start_date_override=shared_override,
                start_date_overrides=overrides,
            )
        return calls, out.getvalue(), result

    def test_weather_etl_applies_each_airports_own_override(self):
        calls = self._run_weather_stage(
            overrides={"CMB": "2024-03-10", "DXB": date(2024, 3, 20)}
        )
        self.assertEqual(
            calls,
            [
                ("CMB", "2024-03-10", "2024-03-31"),
                ("DXB", "2024-03-20", "2024-03-31"),
                ("LHR", "2024-03-01", "2024-03-31"),
            ],
            "airports without a watermark keep their full span",
        )

    def test_weather_etl_defaults_to_full_range(self):
        calls = self._run_weather_stage()
        self.assertEqual(
            {code: start for code, start, _ in calls},
            {"CMB": "2024-03-01", "DXB": "2024-03-01", "LHR": "2024-03-01"},
        )

    def test_weather_etl_partial_fetch_reports_failed_airport(self):
        calls, output, result = self._run_weather_stage_capture(failing={"CMB"})
        self.assertEqual(
            [code for code, _, _ in calls], ["CMB", "DXB", "LHR"],
            "a failed airport must not stop the other airports",
        )
        self.assertEqual(result["airports_failed"], ["CMB"])
        self.assertIn("['CMB']", output,
                      "the stage summary must still name the failed airport")

    def test_weather_etl_shared_override_still_supported(self):
        calls = self._run_weather_stage(shared_override=date(2024, 3, 5))
        self.assertEqual(
            {code: start for code, start, _ in calls},
            {"CMB": "2024-03-05", "DXB": "2024-03-05", "LHR": "2024-03-05"},
        )

    def test_invalid_per_airport_override_fails_before_fetch(self):
        import weather_etl

        with mock.patch.object(weather_etl, "_read_flights") as read:
            with self.assertRaises(ValueError):
                weather_etl.run_weather_etl(
                    dry_run=True,
                    start_date_overrides={"CMB": "not-a-date"},
                )
            read.assert_not_called()

        with self.assertRaises(TypeError):
            weather_etl.run_weather_etl(
                dry_run=True, start_date_overrides=["CMB"]
            )

    def test_default_weather_stage_forwards_overrides(self):
        import weather_etl

        overrides = {"CMB": date(2024, 3, 10)}
        with mock.patch.object(weather_etl, "run_weather_etl") as fake:
            stages = ro._default_stages(_fresh_config())
            stages["weather"](start_date_overrides=overrides)
            fake.assert_called_once_with(start_date_overrides=overrides)

        with mock.patch.object(weather_etl, "run_weather_etl") as fake:
            stages = ro._default_stages(_fresh_config())
            stages["weather"]()
            fake.assert_called_once_with()

    # ---------- additive signature ----------

    def test_run_weather_etl_signature_is_additive(self):
        import inspect

        import weather_etl

        params = inspect.signature(weather_etl.run_weather_etl).parameters
        self.assertIn("start_date_override", params)
        self.assertIsNone(params["start_date_override"].default)
        self.assertIn("start_date_overrides", params)
        self.assertIsNone(params["start_date_overrides"].default)
        self.assertIs(params["dry_run"].default, False)
        self.assertIsNone(params["airport_filter"].default)

    def test_config_chunk_size_preserves_default(self):
        import weather_etl

        original = weather_etl.INSERT_CHUNK_SIZE
        try:
            ro._default_stages(_fresh_config())
            self.assertEqual(weather_etl.INSERT_CHUNK_SIZE, 1000,
                             "policy default must equal the original value")
            config = _fresh_config()
            config["weather_chunk_size"] = 250
            ro._default_stages(config)
            self.assertEqual(weather_etl.INSERT_CHUNK_SIZE, 250)
        finally:
            weather_etl.INSERT_CHUNK_SIZE = original

    # ---------- fixtures ----------

    def _seed_airport_series(self, code, flight_dates, observation_dates,
                             source_model="best_match",
                             observation_hour=12,
                             hourly=False,
                             observation_times=None):
        """Seed one airport with multiple flight days and observation days."""
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(text(
                "INSERT IGNORE INTO dim_airport "
                "(airport_code, airport_name) VALUES (:c, :c)"
            ), {"c": code})
            airport_key = conn.execute(text(
                "SELECT airport_key FROM dim_airport WHERE airport_code=:c"
            ), {"c": code}).scalar()
            conn.execute(text(
                "INSERT IGNORE INTO dim_airline (airline_code, airline_name)"
                " VALUES ('WTS', 'Watermark Series Air')"
            ))
            airline_key = conn.execute(text(
                "SELECT airline_key FROM dim_airline WHERE airline_code='WTS'"
            )).scalar()
            conn.execute(text(
                "INSERT IGNORE INTO dim_route (origin_airport_key,"
                " destination_airport_key) VALUES (:k, :k)"
            ), {"k": airport_key})
            route_key = conn.execute(text(
                "SELECT route_key FROM dim_route WHERE origin_airport_key=:k"
            ), {"k": airport_key}).scalar()
            for index, flight_date in enumerate(flight_dates):
                date_key = int(flight_date.strftime("%Y%m%d"))
                conn.execute(text(
                    "INSERT IGNORE INTO dim_date (date_key, full_date)"
                    " VALUES (:dk, :fd)"
                ), {"dk": date_key, "fd": flight_date})
                conn.execute(text(
                    "INSERT IGNORE INTO fact_flight (source_system,"
                    " source_flight_id, date_key, airline_key, route_key,"
                    " scheduled_departure) VALUES"
                    " ('csv', :sid, :dk, :ak, :rk, :sd)"
                ), {
                    "sid": f"{code.lower()}-{index}",
                    "dk": date_key,
                    "ak": airline_key,
                    "rk": route_key,
                    "sd": f"{flight_date} 10:00:00",
                })
            times_to_insert = []
            if observation_times is not None:
                times_to_insert = [
                    t.strftime("%Y-%m-%d %H:%M:%S") if isinstance(t, datetime) else str(t)
                    for t in observation_times
                ]
            elif hourly:
                for observation_date in observation_dates:
                    for hour in range(24):
                        times_to_insert.append(f"{observation_date} {hour:02d}:00:00")
            else:
                for observation_date in observation_dates:
                    times_to_insert.append(f"{observation_date} {observation_hour:02d}:00:00")

            for obs in times_to_insert:
                conn.execute(text(
                    "INSERT IGNORE INTO dim_weather (airport_key,"
                    " observation_time, temperature, source_dataset,"
                    " source_model) VALUES"
                    " (:k, :obs, 25.0, 'open-meteo-archive', :model)"
                ), {
                    "k": airport_key,
                    "obs": obs,
                    "model": source_model,
                })
        return code

    def _seed_airports_and_flights(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            for code in ("WTOA", "WTWB"):
                conn.execute(text(
                    "INSERT IGNORE INTO dim_airport "
                    "(airport_code, airport_name) VALUES (:c, :c)"
                ), {"c": code})
            keys = {
                r[1]: r[0]
                for r in conn.execute(text(
                    "SELECT airport_key, airport_code FROM dim_airport "
                    "WHERE airport_code IN ('WTOA','WTWB')"
                )).fetchall()
            }
            conn.execute(text(
                "INSERT IGNORE INTO dim_date (date_key, full_date) "
                "VALUES (20240401, '2024-04-01'), (20240410, '2024-04-10')"
            ))
            conn.execute(text(
                "INSERT IGNORE INTO dim_airline (airline_code, airline_name)"
                " VALUES ('WTL', 'Watermark Test Air')"
            ))
            airline_key = conn.execute(text(
                "SELECT airline_key FROM dim_airline WHERE airline_code='WTL'"
            )).scalar()
            for code, date_key, obs in (
                ("WTOA", 20240401, "2024-04-01 09:00:00"),  # observed fully
                ("WTWB", 20240410, "2024-04-01 09:00:00"),  # observed too early
            ):
                conn.execute(text(
                    "INSERT IGNORE INTO dim_route (origin_airport_key,"
                    " destination_airport_key) VALUES (:k, :k)"
                ), {"k": keys[code]})
                route_key = conn.execute(text(
                    "SELECT route_key FROM dim_route "
                    "WHERE origin_airport_key = :k"
                ), {"k": keys[code]}).scalar()
                conn.execute(text(
                    "INSERT IGNORE INTO fact_flight (source_system,"
                    " source_flight_id, date_key, airline_key, route_key,"
                    " scheduled_departure) VALUES"
                    " ('csv', :sid, :dk, :ak, :rk, :sd)"
                ), {
                    "sid": f"w-{code}",
                    "dk": date_key,
                    "ak": airline_key,
                    "rk": route_key,
                    "sd": f"{date_key // 10000:04d}-{(date_key // 100) % 100:02d}-"
                          f"{date_key % 100:02d} 10:00:00",
                })
                conn.execute(text(
                    "INSERT IGNORE INTO dim_weather (airport_key,"
                    " observation_time,"
                    " temperature, source_dataset, source_model) VALUES"
                    " (:k, :obs, 25.0, 'open-meteo-archive', 'reanalysis_v1')"
                ), {"k": keys[code], "obs": obs})
        return "WTOA", "WTWB"


class TestOltpChangeUpdater(unittest.TestCase):
    """Updater behaviour on a WRITABLE local fixture source + live
    read-only convergence. The live source is never written."""

    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()
        dbutil._require_test_name(FIXTURE_DB)  # guard: only *_ci/*_test
        server = dbutil.server_engine()
        with server.begin() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{FIXTURE_DB}` "
                              "CHARACTER SET utf8mb4"))
        server.dispose()

    def setUp(self):
        self.fixture = _fixture_engine()
        _fixture_source(self.fixture)
        _seed_fixture_flight(self.fixture)
        # Reset any target-fact drift a previous test may have left, so
        # every test starts from the aligned baseline state.
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM fact_flight "
                "WHERE source_system='oltp' AND source_flight_id='9001'"
            ))
        self._seed_target_fact("9001")

    def _seed_target_fact(self, source_flight_id,
                          delay=25, duration=120, passengers=2,
                          cancelled=0, dep="2024-05-01 08:10:00",
                          arr="2024-05-01 10:10:00"):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(text(
                "INSERT IGNORE INTO dim_date (date_key, full_date)"
                " VALUES (20240501, '2024-05-01')"
            ))
            conn.execute(text(
                "INSERT IGNORE INTO dim_airport (airport_code, airport_name)"
                " VALUES ('FIXA', 'Fixture A')"
            ))
            airport_key = conn.execute(text(
                "SELECT airport_key FROM dim_airport WHERE airport_code='FIXA'"
            )).scalar()
            conn.execute(text(
                "INSERT IGNORE INTO dim_route (origin_airport_key,"
                " destination_airport_key) VALUES (:k, :k)"
            ), {"k": airport_key})
            route_key = conn.execute(text(
                "SELECT route_key FROM dim_route WHERE origin_airport_key=:k"
            ), {"k": airport_key}).scalar()
            conn.execute(text(
                "INSERT IGNORE INTO dim_airline (airline_code, airline_name)"
                " VALUES ('FIX', 'Fixture Air')"
            ))
            airline_key = conn.execute(text(
                "SELECT airline_key FROM dim_airline WHERE airline_code='FIX'"
            )).scalar()
            conn.execute(text(
                "INSERT IGNORE INTO fact_flight (source_system,"
                " source_flight_id, date_key, airline_key, route_key,"
                " delay_minutes, arrival_delay_minutes,"
                " flight_duration_minutes, passenger_count, cancelled_flag,"
                " departure_time, arrival_time) VALUES"
                " ('oltp', :sid, 20240501, :ak, :rk, :delay, 0, :dur,"
                " :pax, :cxl, :dep, :arr)"
            ), {
                "sid": source_flight_id, "ak": airline_key, "rk": route_key,
                "delay": delay, "dur": duration, "pax": passengers,
                "cxl": cancelled, "dep": dep, "arr": arr,
            })

    def _fact_row(self, source_flight_id="9001"):
        from database import dw_engine

        with dw_engine.connect() as conn:
            row = conn.execute(text(
                "SELECT delay_minutes, flight_duration_minutes,"
                " passenger_count, cancelled_flag, departure_time,"
                " arrival_time, arrival_delay_minutes, flight_key"
                " FROM fact_flight WHERE source_system='oltp'"
                " AND source_flight_id = :s"
            ), {"s": source_flight_id}).mappings().one()
        return dict(row)

    def _fact_count(self):
        from database import dw_engine

        with dw_engine.connect() as conn:
            return conn.execute(text(
                "SELECT COUNT(*) FROM fact_flight WHERE source_system='oltp'"
            )).scalar()

    def _source_snapshot(self):
        with self.fixture.connect() as conn:
            rows = conn.execute(text(
                "SELECT flight_id, status, scheduled_departure,"
                " actual_departure, actual_arrival FROM flights "
                "ORDER BY flight_id"
            )).fetchall()
            delays = conn.execute(text(
                "SELECT flight_id, delay_minutes FROM flight_delays "
                "ORDER BY flight_id"
            )).fetchall()
            bookings = conn.execute(text(
                "SELECT flight_id, booking_status FROM bookings "
                "ORDER BY flight_id"
            )).fetchall()
        return [tuple(r) for r in rows], [tuple(r) for r in delays], \
            [tuple(r) for r in bookings]

    def test_convergence_after_aligned_target_updates_nothing(self):
        from oltp_change_updater import update_changed_facts

        before = self._source_snapshot()
        result = update_changed_facts(
            source_engine=self.fixture,
            target_engine=__import__("database").dw_engine,
        )
        self.assertEqual(result["rows_updated"], 0,
                         "aligned target must produce zero changes")
        self.assertEqual(result["rows_missing_in_dw"], 0)
        self.assertEqual(result["rows_considered"], 1)
        # read-only contract: the (writable) fixture source is untouched
        self.assertEqual(self._source_snapshot(), before)

    def test_injected_drift_is_detected_and_reverted(self):
        from database import dw_engine
        from oltp_change_updater import update_changed_facts

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET delay_minutes = 1,"
                " flight_duration_minutes = 2, passenger_count = 3,"
                " cancelled_flag = 1, departure_time = NULL,"
                " arrival_time = NULL"
                " WHERE source_system='oltp' AND source_flight_id='9001'"
            ))
        result = update_changed_facts(
            source_engine=self.fixture, target_engine=dw_engine
        )
        self.assertEqual(result["rows_updated"], 1)
        row = self._fact_row()
        self.assertEqual(row["delay_minutes"], 25)
        self.assertEqual(row["flight_duration_minutes"], 120)
        self.assertEqual(row["passenger_count"], 2)
        self.assertEqual(row["cancelled_flag"], 0)
        self.assertEqual(str(row["departure_time"]), "2024-05-01 08:10:00")
        self.assertEqual(str(row["arrival_time"]), "2024-05-01 10:10:00")
        # excluded columns stay loader-owned
        self.assertEqual(row["arrival_delay_minutes"], 0)

        # second run converges
        self.assertEqual(
            update_changed_facts(
                source_engine=self.fixture, target_engine=dw_engine
            )["rows_updated"],
            0,
        )

    def test_source_flights_without_fact_rows_are_reported_not_inserted(self):
        from database import dw_engine
        from oltp_change_updater import update_changed_facts

        with self.fixture.begin() as conn:
            conn.execute(text(
                "INSERT INTO flights (flight_id, flight_number, status,"
                " scheduled_departure) VALUES"
                " (9002, 'FX9002', 'ACTIVE', '2024-05-02 08:00:00')"
            ))
        before = self._fact_count()
        result = update_changed_facts(
            source_engine=self.fixture, target_engine=dw_engine
        )
        self.assertEqual(result["rows_missing_in_dw"], 1)
        self.assertEqual(result["rows_updated"], 0)
        self.assertEqual(self._fact_count(), before,
                         "updater must never insert rows")

    def test_dry_run_reports_without_writing(self):
        from database import dw_engine
        from oltp_change_updater import update_changed_facts

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET passenger_count = 99"
                " WHERE source_system='oltp' AND source_flight_id='9001'"
            ))
        frozen = self._fact_row()
        try:
            result = update_changed_facts(
                source_engine=self.fixture, target_engine=dw_engine,
                dry_run=True,
            )
            self.assertEqual(result["rows_pending"], 1)
            self.assertEqual(result["rows_updated"], 0)
            self.assertEqual(self._fact_row(), frozen,
                             "dry run must not write")
        finally:
            # leave the fixture target aligned for subsequent tests
            update_changed_facts(
                source_engine=self.fixture, target_engine=dw_engine
            )

    # ---------- standalone CLI (fail-closed contract) ----------

    def _oltp_audit_count(self):
        from database import dw_engine

        with dw_engine.connect() as conn:
            return conn.execute(text(
                "SELECT COUNT(*) FROM etl_batch_log WHERE source_name='oltp'"
            )).scalar()

    def test_cli_default_is_dry_run_and_writes_nothing(self):
        from database import dw_engine
        from oltp_change_updater import main as updater_main
        from oltp_change_updater import update_changed_facts

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET passenger_count = 99 "
                "WHERE source_system='oltp' AND source_flight_id='9001'"
            ))
        frozen = self._fact_row()
        audits_before = self._oltp_audit_count()
        code = updater_main(
            [], source_engine=self.fixture, target_engine=dw_engine
        )
        self.assertEqual(code, 0)
        self.assertEqual(self._fact_row(), frozen,
                         "default CLI invocation must not write")
        self.assertEqual(self._oltp_audit_count(), audits_before,
                         "dry run writes no audit rows")
        # converge back for subsequent tests
        update_changed_facts(source_engine=self.fixture, target_engine=dw_engine)

    def test_cli_execute_applies_updates_with_lock_and_audit(self):
        from database import dw_engine
        from oltp_change_updater import main as updater_main

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET delay_minutes = 1, "
                "passenger_count = 99 "
                "WHERE source_system='oltp' AND source_flight_id='9001'"
            ))
        verified = []

        def verifier(conn):
            verified.append(True)
            return [verify_warehouse.Check("unit_stub", "PASS", "stubbed")]

        code = updater_main(
            ["--execute"], source_engine=self.fixture, target_engine=dw_engine,
            verifier=verifier,
        )
        self.assertEqual(code, 0)
        self.assertEqual(verified, [True],
                         "--execute must run post-update verification "
                         "before reporting success")
        row = self._fact_row()
        self.assertEqual(row["delay_minutes"], 25)
        self.assertEqual(row["passenger_count"], 2)
        with dw_engine.connect() as conn:
            status = conn.execute(text(
                "SELECT status FROM etl_batch_log WHERE source_name='oltp' "
                "ORDER BY run_id DESC LIMIT 1"
            )).scalar()
        self.assertEqual(status, "SUCCESS",
                         "--execute must leave an audit row")
        lock = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(lock, "lock must be released after --execute")
        lock.release()

    def test_cli_execute_verification_failure_keeps_updates_and_exits_6(self):
        from database import dw_engine
        from oltp_change_updater import main as updater_main
        from oltp_change_updater import update_changed_facts

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET delay_minutes = 1 "
                "WHERE source_system='oltp' AND source_flight_id='9001'"
            ))

        def failing_verifier(conn):
            return [verify_warehouse.Check("unit_stub", "FAIL", "stubbed")]

        code = updater_main(
            ["--execute"], source_engine=self.fixture, target_engine=dw_engine,
            verifier=failing_verifier,
        )
        self.assertEqual(code, 6,
                         "verification failure must exit 6, not success")
        row = self._fact_row()
        self.assertEqual(row["delay_minutes"], 25,
                         "updates remain committed - not rolled back on "
                         "verification failure")
        with dw_engine.connect() as conn:
            status, err = conn.execute(text(
                "SELECT status, error_detail FROM etl_batch_log "
                "WHERE source_name='oltp' ORDER BY run_id DESC LIMIT 1"
            )).one()
        self.assertEqual(status, "FAILED")
        self.assertIn("verification FAILED", err or "")
        self.assertIn("NOT rolled back", err or "")
        lock = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(lock, "lock must be released on verification failure")
        lock.release()
        # converge back for subsequent tests
        update_changed_facts(source_engine=self.fixture, target_engine=dw_engine)

    def test_cli_execute_verifier_exception_exits_6_and_audit_failed(self):
        from database import dw_engine
        from oltp_change_updater import main as updater_main
        from oltp_change_updater import update_changed_facts

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET delay_minutes = 1 "
                "WHERE source_system='oltp' AND source_flight_id='9001'"
            ))

        def broken_verifier(conn):
            raise RuntimeError("verifier exploded")

        code = updater_main(
            ["--execute"], source_engine=self.fixture, target_engine=dw_engine,
            verifier=broken_verifier,
        )
        self.assertEqual(code, 6,
                         "a verifier that cannot run must not report success")
        with dw_engine.connect() as conn:
            status, err = conn.execute(text(
                "SELECT status, error_detail FROM etl_batch_log "
                "WHERE source_name='oltp' ORDER BY run_id DESC LIMIT 1"
            )).one()
        self.assertEqual(status, "FAILED")
        self.assertIn("could not complete", err or "")
        self.assertIn("NOT rolled back", err or "")
        update_changed_facts(source_engine=self.fixture, target_engine=dw_engine)

    def test_cli_execute_warn_verifier_still_succeeds(self):
        from database import dw_engine
        from oltp_change_updater import main as updater_main

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET delay_minutes = 1 "
                "WHERE source_system='oltp' AND source_flight_id='9001'"
            ))

        def warn_verifier(conn):
            return [verify_warehouse.Check("unit_stub", "WARN", "warn only")]

        code = updater_main(
            ["--execute"], source_engine=self.fixture, target_engine=dw_engine,
            verifier=warn_verifier,
        )
        self.assertEqual(code, 0,
                         "WARN checks are informational - only FAIL blocks")
        with dw_engine.connect() as conn:
            status = conn.execute(text(
                "SELECT status FROM etl_batch_log WHERE source_name='oltp' "
                "ORDER BY run_id DESC LIMIT 1"
            )).scalar()
        self.assertEqual(status, "SUCCESS")

    def test_cli_execute_audit_close_failure_exits_5(self):
        from database import dw_engine
        from oltp_change_updater import main as updater_main

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET delay_minutes = 1 "
                "WHERE source_system='oltp' AND source_flight_id='9001'"
            ))

        def verifier(conn):
            return [verify_warehouse.Check("unit_stub", "PASS", "stubbed")]

        original_finish = etl_audit.finish_run

        def failing_finish(run_id, status, **kwargs):
            raise etl_audit.AuditError("audit close unavailable")

        etl_audit.finish_run = failing_finish
        try:
            code = updater_main(
                ["--execute"], source_engine=self.fixture,
                target_engine=dw_engine, verifier=verifier,
            )
        finally:
            etl_audit.finish_run = original_finish
        self.assertEqual(code, 5,
                         "an unclosable audit row must not be reported "
                         "as success")
        lock = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(lock)
        lock.release()

    def test_cli_execute_refuses_when_lock_is_held(self):
        from database import dw_engine
        from oltp_change_updater import main as updater_main
        from oltp_change_updater import update_changed_facts

        with dw_engine.begin() as conn:
            conn.execute(text(
                "UPDATE fact_flight SET delay_minutes = 1 "
                "WHERE source_system='oltp' AND source_flight_id='9001'"
            ))
        frozen = self._fact_row()
        holder = etl_audit.acquire_run_lock(timeout=0)
        self.assertIsNotNone(holder)
        try:
            code = updater_main(
                ["--execute"], source_engine=self.fixture, target_engine=dw_engine
            )
            self.assertEqual(code, 3)
            self.assertEqual(self._fact_row(), frozen,
                             "locked-out --execute must not write")
        finally:
            holder.release()
        update_changed_facts(source_engine=self.fixture, target_engine=dw_engine)

    @unittest.skipUnless(
        dbutil.oltp_available(), "OLTP source unreachable"
    )
    def test_live_source_convergence_after_fresh_load(self):
        """Fresh oltp_etl load then updater -> 0 rows updated: proves the
        updater's derivations stay aligned with oltp_etl.py's loader."""
        from oltp_change_updater import update_changed_facts
        from oltp_etl import run_oltp_etl

        import contextlib
        import io

        with contextlib.redirect_stdout(io.StringIO()):
            run_oltp_etl()
        result = update_changed_facts()
        self.assertEqual(result["rows_updated"], 0,
                         "updater must be a no-op right after a full load")
        self.assertEqual(result["rows_missing_in_dw"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
