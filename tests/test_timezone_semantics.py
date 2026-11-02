import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import pandas as pd

import etl
import refresh_orchestrator as ro
import weather_etl
import weather_linker
from timestamp_semantics import (
    AmbiguousLocalTime,
    FLIGHT_TIMESTAMP_SEMANTICS,
    NonexistentLocalTime,
    local_to_utc,
    utc_to_local,
)


UTC = timezone.utc


def epoch_series(start, end):
    values = []
    current = start
    while current <= end:
        values.append(int(current.timestamp()))
        current += timedelta(hours=1)
    return values


def archive_payload(start, end, tz_name):
    start_utc = datetime.combine(start, datetime.min.time(), tzinfo=UTC)
    end_utc = datetime.combine(end, datetime.max.time(), tzinfo=UTC)
    end_utc = end_utc.replace(minute=0, second=0, microsecond=0)
    times = epoch_series(start_utc, end_utc)
    return {
        "timezone": tz_name,
        "utc_offset_seconds": 0,
        "hourly": {
            "time": times,
            **{name: [1.0] * len(times) for name in weather_etl.HOURLY_VARIABLES},
        },
    }


def archive_frame(*instants):
    return pd.DataFrame(
        {
            "time_utc": pd.to_datetime(list(instants), utc=True),
            "temperature_2m": [10.0] * len(instants),
            "relative_humidity_2m": [60.0] * len(instants),
            "precipitation": [0.0] * len(instants),
            "wind_speed_10m": [5.0] * len(instants),
            "cloud_cover": [10.0] * len(instants),
            "weather_code": [0] * len(instants),
        }
    )


class TestFlightTimestampSemantics(unittest.TestCase):
    def test_csv_and_oltp_semantics_are_explicitly_unresolved(self):
        self.assertEqual(FLIGHT_TIMESTAMP_SEMANTICS["csv"], "unresolved")
        self.assertEqual(FLIGHT_TIMESTAMP_SEMANTICS["oltp"], "unresolved")

    def test_hhmm_is_not_minutes_from_midnight(self):
        values = etl._hhmm_to_minutes(pd.Series([720, 1413, 0, 2400, 1260]))
        self.assertEqual(values.iloc[0], 440)
        self.assertEqual(values.iloc[1], 853)
        self.assertEqual(values.iloc[2], 0)
        self.assertTrue(pd.isna(values.iloc[3]))
        self.assertTrue(pd.isna(values.iloc[4]))
        self.assertEqual(
            weather_linker.combine_flight_date_and_minutes(
                "2025-11-24", values.iloc[0]
            ),
            datetime(2025, 11, 24, 7, 20),
        )
        self.assertEqual(
            weather_linker.combine_flight_date_and_minutes(
                "2025-11-24", values.iloc[1]
            ),
            datetime(2025, 11, 24, 14, 13),
        )

    def test_utc_payload_is_naive_utc_for_storage(self):
        epoch = int(datetime(2025, 1, 12, 5, 0, tzinfo=UTC).timestamp())
        payload = {
            "timezone": "Europe/London",
            "utc_offset_seconds": 0,
            "hourly": {
                "time": [epoch],
                **{name: [1.0] for name in weather_etl.HOURLY_VARIABLES},
            },
        }
        with mock.patch.object(
            weather_etl, "_request_json", return_value=payload
        ) as request:
            frame = weather_etl.fetch_archive_weather(
                "LHR", weather_etl.AIRPORT_INFO["LHR"],
                "2025-01-12", "2025-01-12",
            )
        params = request.call_args.args[1]
        self.assertEqual(params["timeformat"], "unixtime")
        self.assertEqual(frame.iloc[0]["time_utc"], datetime(2025, 1, 12, 5))
        rows = weather_etl.build_weather_rows(
            "LHR", frame, pd.DataFrame(columns=["time_utc", "visibility_km"]),
            timezone_name="Europe/London",
        )
        self.assertEqual(
            rows.iloc[0]["observation_time"], datetime(2025, 1, 12, 5)
        )
        self.assertEqual(
            rows.iloc[0]["source_model"], weather_etl.ARCHIVE_UTC_SOURCE_MODEL
        )

    def test_half_hour_zone_keeps_true_utc_instant(self):
        instant = datetime(2025, 1, 9, 6, 30, tzinfo=UTC)
        payload = {
            "timezone": "Asia/Colombo",
            "utc_offset_seconds": 19800,
            "hourly": {
                "time": [int(instant.timestamp())],
                **{name: [1.0] for name in weather_etl.HOURLY_VARIABLES},
            },
        }
        with mock.patch.object(
            weather_etl, "_request_json", return_value=payload
        ):
            frame = weather_etl.fetch_archive_weather(
                "CMB", weather_etl.AIRPORT_INFO["CMB"],
                "2025-01-09", "2025-01-09",
            )
        rows = weather_etl.build_weather_rows(
            "CMB", frame, None, timezone_name="Asia/Colombo"
        )
        self.assertEqual(
            rows.iloc[0]["observation_time"], datetime(2025, 1, 9, 6, 30)
        )

    def test_archive_request_padding_covers_dst_boundaries(self):
        cases = (
            ("2025-03-30", "2025-03-30", "Europe/London", 23),
            ("2025-10-26", "2025-10-26", "Europe/London", 25),
            ("2024-06-01", "2024-06-03", "Europe/London", 72),
        )
        for start, end, tz_name, expected_count in cases:
            with self.subTest(start=start, end=end):
                logical_start = date.fromisoformat(start)
                logical_end = date.fromisoformat(end)
                request_start, request_end = (
                    weather_etl._padded_request_dates(logical_start, logical_end)
                )
                payload = archive_payload(
                    request_start, request_end, tz_name
                )
                with mock.patch.object(
                    weather_etl, "_request_json", return_value=payload
                ) as request:
                    frame = weather_etl.fetch_archive_weather(
                        "LHR", weather_etl.AIRPORT_INFO["LHR"], start, end
                    )
                params = request.call_args.args[1]
                self.assertEqual(
                    params["start_date"],
                    request_start.strftime("%Y-%m-%d"),
                )
                self.assertEqual(
                    params["end_date"], request_end.strftime("%Y-%m-%d")
                )
                expected = ro.generate_expected_utc_instants(
                    logical_start, logical_end, tz_name
                )
                self.assertEqual(len(expected), expected_count)
                self.assertEqual(
                    set(frame["time_utc"].dt.to_pydatetime()), set(expected)
                )

    def test_archive_request_bounds_are_clamped(self):
        start, end = weather_etl._padded_request_dates(
            date(1940, 1, 1), date(1940, 1, 1)
        )
        self.assertEqual(start, date(1940, 1, 1))
        self.assertEqual(end, date(1940, 1, 2))

        today = date.today()
        start, end = weather_etl._padded_request_dates(today, today)
        self.assertEqual(start, today - timedelta(days=1))
        self.assertEqual(end, today)

    def test_iem_is_requested_in_utc(self):
        body = "station,valid,vsby\nVCBI,2025-01-12 05:50,0.16\n"
        with mock.patch.object(
            weather_etl, "_request_text", return_value=body
        ) as request:
            frame = weather_etl.fetch_iem_visibility(
                "VCBI", "Asia/Colombo", "2025-01-12", "2025-01-12"
            )
        self.assertEqual(request.call_args.args[1]["tz"], "UTC")
        self.assertEqual(
            frame.iloc[0]["time_utc"], pd.Timestamp("2025-01-12 05:50Z")
        )

    def test_gmt_and_bst_are_converted_uniquely(self):
        self.assertEqual(
            local_to_utc(datetime(2025, 1, 15, 12), "Europe/London"),
            datetime(2025, 1, 15, 12, tzinfo=UTC),
        )
        self.assertEqual(
            local_to_utc(datetime(2025, 7, 15, 12), "Europe/London"),
            datetime(2025, 7, 15, 11, tzinfo=UTC),
        )

    def test_dst_gap_is_rejected(self):
        with self.assertRaises(NonexistentLocalTime):
            local_to_utc(datetime(2025, 3, 30, 1, 30), "Europe/London")

    def test_repeated_fall_back_hour_is_rejected(self):
        with self.assertRaises(AmbiguousLocalTime):
            local_to_utc(datetime(2025, 10, 26, 1, 30), "Europe/London")

    def test_repeated_local_hour_maps_from_two_utc_instants(self):
        first = utc_to_local(datetime(2025, 10, 26, 0, 30, tzinfo=UTC),
                             "Europe/London")
        second = utc_to_local(datetime(2025, 10, 26, 1, 30, tzinfo=UTC),
                              "Europe/London")
        self.assertEqual(first, datetime(2025, 10, 26, 1, 30))
        self.assertEqual(second, datetime(2025, 10, 26, 1, 30))

    def test_visibility_merge_keeps_repeated_hours_separate(self):
        archive = archive_frame(
            datetime(2025, 10, 26, 0, tzinfo=UTC),
            datetime(2025, 10, 26, 1, tzinfo=UTC),
        )
        visibility = pd.DataFrame(
            {
                "time_utc": pd.to_datetime(
                    ["2025-10-26 00:50Z", "2025-10-26 01:50Z"], utc=True
                ),
                "visibility_km": [5.0, 2.0],
            }
        )
        rows = weather_etl.build_weather_rows(
            "LHR", archive, visibility, timezone_name="Europe/London"
        )
        self.assertEqual(list(rows["observation_time"]), [
            datetime(2025, 10, 26, 0), datetime(2025, 10, 26, 1)
        ])
        self.assertEqual(list(rows["visibility"]), [5.0, 2.0])

    def test_build_rejects_missing_utc_basis(self):
        frame = archive_frame(datetime(2025, 1, 1, tzinfo=UTC))
        frame = frame.rename(columns={"time_utc": "time"})
        with self.assertRaises(ValueError):
            weather_etl.build_weather_rows("LHR", frame, None)

    def test_dry_run_uses_unresolved_semantics_and_never_upserts(self):
        flights = pd.DataFrame(
            {
                "source_system": ["csv"],
                "source_flight_id": ["1"],
                "airport_code": ["LHR"],
                "scheduled_departure": pd.to_datetime(
                    ["2025-01-12 06:00"]
                ),
            }
        )
        archive = archive_frame(
            datetime(2025, 1, 12, 5, tzinfo=UTC),
            datetime(2025, 1, 12, 6, tzinfo=UTC),
        )
        with mock.patch.object(
            weather_etl, "_read_flights", return_value=flights
        ), mock.patch.object(
            weather_etl, "_read_airport_keys", return_value={"LHR": 1}
        ), mock.patch.object(
            weather_etl, "fetch_archive_weather", return_value=archive
        ), mock.patch.object(
            weather_etl, "fetch_iem_visibility",
            return_value=pd.DataFrame(columns=["time_utc", "visibility_km"]),
        ), mock.patch.object(
            weather_etl, "_upsert_weather"
        ) as upsert, mock.patch("sys.stdout"):
            weather_etl.run_weather_etl(dry_run=True)
        upsert.assert_not_called()

    def test_weather_matching_is_exact_and_never_nearest(self):
        flights = pd.DataFrame(
            {
                "source_system": ["csv"],
                "airport_code": ["LHR"],
                "scheduled_departure": pd.to_datetime(
                    ["2025-01-12 06:00"]
                ),
            }
        )
        weather_index = {
            "LHR": {
                datetime(2025, 1, 12, 5): "five",
                datetime(2025, 1, 12, 6): "six",
            }
        }
        self.assertEqual(
            weather_etl.match_flights_to_weather(
                flights, weather_index, {"csv": "origin_local"}
            ),
            ["six"],
        )
        nearest_only = {
            "LHR": {datetime(2025, 1, 12, 5): "five"}
        }
        self.assertEqual(
            weather_etl.match_flights_to_weather(
                flights, nearest_only, {"csv": "origin_local"}
            ),
            [None],
        )


class TestLinkerTimestampSafety(unittest.TestCase):
    def _row(self, local_hour, **overrides):
        row = {
            "flight_key": 1,
            "source_system": "csv",
            "source_flight_id": "1",
            "scheduled_departure": local_hour,
            "old_weather_key": None,
            "airport_code": "LHR",
            "candidate_weather_key": None,
            "candidate_observation_time": None,
            "candidate_visibility": None,
            "candidate_source_dataset": None,
            "candidate_source_model": None,
        }
        row.update(overrides)
        return row

    def test_unresolved_semantics_hold_even_with_exact_candidate(self):
        fact = self._row(datetime(2025, 1, 12, 6))
        candidate = dict(
            fact,
            candidate_weather_key=101,
            candidate_observation_time=datetime(2025, 1, 12, 6),
            candidate_visibility=8.0,
            candidate_source_dataset=weather_linker.ARCHIVE_SOURCE_DATASET,
            candidate_source_model=weather_linker.ARCHIVE_UTC_SOURCE_MODEL,
        )
        row = weather_linker.build_link_plan([fact, candidate])[0]
        self.assertEqual(
            row["status"], weather_linker.UNRESOLVED_FLIGHT_TIMESTAMP
        )
        self.assertIsNone(row["new_weather_key"])

    def test_exact_utc_local_match_and_no_nearest(self):
        fact = self._row(datetime(2025, 1, 12, 6, 45))
        exact = dict(
            fact,
            candidate_weather_key=101,
            candidate_observation_time=datetime(2025, 1, 12, 6),
            candidate_visibility=8.0,
            candidate_source_dataset=weather_linker.ARCHIVE_SOURCE_DATASET,
            candidate_source_model=weather_linker.ARCHIVE_UTC_SOURCE_MODEL,
        )
        row = weather_linker.build_link_plan(
            [fact, exact], source_semantics={"csv": "origin_local"}
        )[0]
        self.assertEqual(row["status"], weather_linker.MATCH)
        self.assertEqual(
            row["target_utc_hour"], datetime(2025, 1, 12, 6)
        )

        near = dict(exact, candidate_observation_time=datetime(
            2025, 1, 12, 5
        ), candidate_weather_key=202)
        held = weather_linker.build_link_plan(
            [fact, near], source_semantics={"csv": "origin_local"}
        )[0]
        self.assertEqual(held["status"], weather_linker.NO_EXACT_CANDIDATE)

    def test_dst_gap_is_held(self):
        fact = self._row(datetime(2025, 3, 30, 1, 30))
        row = weather_linker.build_link_plan(
            [fact], source_semantics={"csv": "origin_local"}
        )[0]
        self.assertEqual(row["status"], weather_linker.NONEXISTENT_LOCAL_TIME)
        self.assertIsNone(row["new_weather_key"])

    def test_repeated_fall_back_hour_is_held(self):
        fact = self._row(datetime(2025, 10, 26, 1, 30))
        row = weather_linker.build_link_plan(
            [fact], source_semantics={"csv": "origin_local"}
        )[0]
        self.assertEqual(row["status"], weather_linker.AMBIGUOUS_LOCAL_TIME)
        self.assertIsNone(row["new_weather_key"])

    def test_legacy_model_is_never_a_candidate(self):
        fact = self._row(datetime(2025, 1, 12, 6))
        legacy = dict(
            fact,
            candidate_weather_key=101,
            candidate_observation_time=datetime(2025, 1, 12, 6),
            candidate_visibility=8.0,
            candidate_source_dataset=weather_linker.ARCHIVE_SOURCE_DATASET,
            candidate_source_model=weather_etl.ARCHIVE_SOURCE_MODEL,
        )
        row = weather_linker.build_link_plan(
            [fact, legacy], source_semantics={"csv": "origin_local"}
        )[0]
        self.assertEqual(row["status"], weather_linker.NO_EXACT_CANDIDATE)

    def test_apply_refuses_unverified_semantics(self):
        plan = [{
            "flight_key": 1,
            "status": weather_linker.MATCH,
            "source_semantics": "unresolved",
            "old_weather_key": None,
            "new_weather_key": 101,
        }]
        with self.assertRaises(RuntimeError):
            weather_linker.apply_links(plan, engine=object(), dry_run=True)

    def test_recommended_semantics_standards_defined_but_unresolved_by_default(self):
        from timestamp_semantics import (
            FLIGHT_TIMESTAMP_SEMANTICS,
            RECOMMENDED_SEMANTICS,
            PROJECT_STANDARDS,
            ORIGIN_LOCAL,
            UTC_TIMESTAMP,
            UNRESOLVED,
        )

        self.assertEqual(RECOMMENDED_SEMANTICS["csv"], ORIGIN_LOCAL)
        self.assertEqual(RECOMMENDED_SEMANTICS["oltp"], UTC_TIMESTAMP)
        self.assertEqual(PROJECT_STANDARDS, RECOMMENDED_SEMANTICS)
        self.assertEqual(FLIGHT_TIMESTAMP_SEMANTICS["csv"], UNRESOLVED)
        self.assertEqual(FLIGHT_TIMESTAMP_SEMANTICS["oltp"], UNRESOLVED)
    def test_linker_cli_proposed_standards_dry_run_and_refuses_execute(self):
        import io
        import contextlib

        out = io.StringIO()
        mock_engine = mock.MagicMock()
        mock_engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
        mock_engine.url.database = "test_dw"
        with contextlib.redirect_stdout(out):
            code = weather_linker.main(["--dry-run", "--proposed-standards"], target_engine=mock_engine)
        self.assertEqual(code, 0)
        self.assertIn("DRY RUN only", out.getvalue())

        err_out = io.StringIO()
        with contextlib.redirect_stdout(err_out):
            code = weather_linker.main(["--execute", "--proposed-standards"])
        self.assertEqual(code, 2)
        self.assertIn("cannot be executed against production", err_out.getvalue())

if __name__ == "__main__":
    unittest.main(verbosity=2)
