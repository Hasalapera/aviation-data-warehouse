"""P2 tests: weather incremental watermarks, the additive start_date_override
kwarg, batch-size preservation, and the bounded OLTP change updater.

The change updater is tested twice:
* against a writable local fixture source DB (`aviation_oltp_ci`) - the
  live Aiven source is NEVER written to, and the fixture proves the
  updater's read-only use of whatever source it is given;
* against the real OLTP source (read-only) for end-to-end convergence
  - skipped when unreachable.
"""

import copy
import os
import sys
import unittest
from datetime import date, datetime, timedelta

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
    @classmethod
    def setUpClass(cls):
        dbutil.recreate_schema()

    def setUp(self):
        dbutil.clear_audit()

    # ---------- window derivation ----------

    def test_incremental_off_always_returns_none(self):
        etl_audit.record_watermark("weather", "CMBB", "2024-03-05 00:00:00")
        config = _fresh_config()
        self.assertIsNone(ro.weather_start_override(config))

    def test_incremental_on_without_watermarks_returns_none(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        self.assertIsNone(
            ro.weather_start_override(config),
            "first run must be full-range (establishes watermarks)",
        )

    def test_window_is_min_watermark_minus_overlap(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        config["weather_incremental"]["overlap_days"] = 7
        etl_audit.record_watermark("weather", "CMBB", "2024-03-05 00:00:00")
        etl_audit.record_watermark("weather", "KEAA", "2024-03-01 12:00:00")
        self.assertEqual(
            ro.weather_start_override(config), date(2024, 3, 1) - timedelta(days=7)
        )

    # ---------- advancement gating ----------

    def test_watermark_advances_only_when_fetch_is_sufficient(self):
        airport_a, airport_b = self._seed_airports_and_flights()

        from database import dw_engine

        advanced, held = ro.advance_weather_watermarks("batch-1")
        marks = etl_audit.get_watermarks("weather")

        self.assertIn(airport_a, advanced,
                      "observed through expected end - 1 day -> advance")
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

    def test_orchestrator_passes_window_only_when_incremental(self):
        from database import dw_engine

        with dw_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO etl_watermark (source_name, window_key,"
                " watermark_value, updated_at, last_batch_guid) VALUES"
                " ('weather', 'WWWW', '2024-03-05 00:00:00', NOW(3), 'b0')"
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
            seen[0].get("start_date_override"),
            date(2024, 3, 5) - timedelta(days=7),
        )

        # same config with incremental disabled: kwarg must NOT be passed
        seen.clear()
        config["weather_incremental"]["enabled"] = False
        code = ro.run_refresh(
            config, stages=stages, verifier=_passing_verifier,
            sources={"weather"},
        )
        self.assertEqual(code, ro.EXIT_OK)
        self.assertEqual(seen, [{}],
                         "default call path must stay start_date_override-free")

    # ---------- verification gate on watermark advancement ----------

    def _incremental_weather_config(self):
        config = _fresh_config()
        config["weather_incremental"]["enabled"] = True
        config["order"] = ["weather"]
        return config

    @staticmethod
    def _clean_weather_stage(**kwargs):
        print("airports failed : []")

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

    # ---------- additive signature ----------

    def test_run_weather_etl_signature_is_additive(self):
        import inspect

        import weather_etl

        params = inspect.signature(weather_etl.run_weather_etl).parameters
        self.assertIn("start_date_override", params)
        self.assertIsNone(params["start_date_override"].default)
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
