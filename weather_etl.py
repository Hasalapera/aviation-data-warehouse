import os
import sys
import time
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

from database import dw_engine


# ==========================================
# EXTERNAL DATA SOURCES
# ==========================================

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
IEM_ASOS_URL = (
    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
)

# Open-Meteo historical archive is reanalysis / model data. It is NOT a
# direct airport observation. The default "best_match" blend combines
# ECMWF IFS HRES, ERA5 and ERA5-Land.
ARCHIVE_SOURCE_DATASET = "open-meteo-archive"
ARCHIVE_SOURCE_MODEL = "best_match"

# Visibility is absent from the Open-Meteo historical archive. We use real
# METAR/ASOS surface observations from the Iowa Environmental Mesonet
# (NOAA Integrated Surface Database) as an explicitly documented alternative.
IEM_SOURCE_DATASET = "noaa-isd-metar"

# Hourly variables actually supported by the Open-Meteo historical archive.
# NOTE: "visibility" is intentionally excluded - it is not provided by any
# archive model (requesting it returns null / undefined units).
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "cloud_cover",
    "weather_code",
]

REQUEST_TIMEOUT = 30
REQUEST_RETRIES = 3
INSERT_CHUNK_SIZE = 1000

# Open-Meteo historical archive coverage bounds. Requests outside this
# window return HTTP 400, so flight date ranges are clamped to it. Future
# flights simply have no observation available and remain unmatched.
ARCHIVE_MIN_DATE = date(1940, 1, 1)
ARCHIVE_MAX_DATE = date.today() - timedelta(days=1)

# IEM/ASOS reports visibility in statute miles. 1 statute mile = 1.609344 km.
MILES_TO_KM = 1.609344

# IEM/ASOS routine observations saturate at approximately 10 km ("10SM").
# Values at this level mean "10 km or more", not an exact measurement.
IEM_VISIBILITY_CAP_KM = 10.0

# Airport reference data: WGS84 coordinates, IANA timezone and ICAO station
# identifier. Coordinates/timezone/ICAO are verified per airport; a missing
# entry means the airport cannot be processed (it is skipped and reported).
AIRPORT_INFO = {
    "CMB": {
        "lat": 7.1808, "lon": 79.8841,
        "tz": "Asia/Colombo", "icao": "VCBI",
    },
    "DXB": {
        "lat": 25.2532, "lon": 55.3657,
        "tz": "Asia/Dubai", "icao": "OMDB",
    },
    "SIN": {
        "lat": 1.3644, "lon": 103.9915,
        "tz": "Asia/Singapore", "icao": "WSSS",
    },
    "LHR": {
        "lat": 51.4700, "lon": -0.4543,
        "tz": "Europe/London", "icao": "EGLL",
    },
    "KUL": {
        "lat": 2.7456, "lon": 101.7072,
        "tz": "Asia/Kuala_Lumpur", "icao": "WMKK",
    },
    "DEL": {
        "lat": 28.5562, "lon": 77.1000,
        "tz": "Asia/Kolkata", "icao": "VIDP",
    },
    "DOH": {
        "lat": 25.2731, "lon": 51.6081,
        "tz": "Asia/Qatar", "icao": "OTHH",
    },
    "BOM": {
        "lat": 19.0887, "lon": 72.8679,
        "tz": "Asia/Kolkata", "icao": "VABB",
    },
}


# WMO weather interpretation codes mapped to the existing warehouse
# condition categories used by the analytics layer.
WMO_CODE_MAP = {
    0: "Clear",
    1: "Clear",
    2: "Cloudy",
    3: "Cloudy",
    45: "Foggy",
    48: "Foggy",
    51: "Rain",
    53: "Rain",
    55: "Rain",
    56: "Rain",
    57: "Rain",
    61: "Rain",
    63: "Rain",
    65: "Rain",
    66: "Rain",
    67: "Rain",
    71: "Rain",
    73: "Rain",
    75: "Rain",
    77: "Rain",
    80: "Rain",
    81: "Rain",
    82: "Rain",
    85: "Rain",
    86: "Rain",
    95: "Thunderstorm",
    96: "Thunderstorm",
    97: "Thunderstorm",
    99: "Thunderstorm",
}


# ==========================================
# HTTP HELPERS
# ==========================================

def _request_json(url, params):
    """GET a JSON endpoint with retries and explicit error handling."""

    last_error = "unknown error"

    for attempt in range(1, REQUEST_RETRIES + 1):

        try:
            response = requests.get(
                url, params=params, timeout=REQUEST_TIMEOUT
            )
        except requests.RequestException as error:
            last_error = f"request failed: {error}"

        else:

            if response.status_code != 200:
                last_error = f"HTTP {response.status_code}"

            else:

                try:
                    payload = response.json()
                except ValueError:
                    last_error = "invalid JSON response"

                else:

                    if payload.get("error"):
                        last_error = (
                            f"API error: {payload.get('reason')}"
                        )
                    else:
                        return payload

        if attempt < REQUEST_RETRIES:
            time.sleep(min(2 ** attempt, 10))

    raise RuntimeError(last_error)


def _request_text(url, params):
    """GET a text (CSV) endpoint with retries."""

    last_error = "unknown error"

    for attempt in range(1, REQUEST_RETRIES + 1):

        try:
            response = requests.get(
                url, params=params, timeout=REQUEST_TIMEOUT
            )
        except requests.RequestException as error:
            last_error = f"request failed: {error}"

        else:

            if response.status_code != 200:
                last_error = f"HTTP {response.status_code}"
            else:
                return response.text

        if attempt < REQUEST_RETRIES:
            time.sleep(min(2 ** attempt, 10))

    raise RuntimeError(last_error)


# ==========================================
# OPEN-METEO HISTORICAL ARCHIVE
# ==========================================

def fetch_archive_weather(airport_code, info, start_date, end_date):
    """Fetch hourly reanalysis weather for one airport/date range.

    Timezone is requested explicitly (the airport's IANA zone), so returned
    timestamps are in the airport's local time - matching the local
    (timezone-naive) ``scheduled_departure`` stored on fact_flight.
    """

    params = {
        "latitude": info["lat"],
        "longitude": info["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": info["tz"],
    }

    payload = _request_json(ARCHIVE_URL, params)

    hourly = payload.get("hourly")

    if not isinstance(hourly, dict) or "time" not in hourly:
        raise RuntimeError(
            f"archive response for {airport_code} missing hourly data"
        )

    frame = pd.DataFrame(
        {
            "time": pd.to_datetime(hourly["time"], errors="coerce"),
        }
    )

    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if values is None:
            # Missing variable -> genuinely unavailable, kept as NULL.
            frame[variable] = pd.NA
        else:
            frame[variable] = pd.to_numeric(
                pd.Series(values), errors="coerce"
            )

    frame = frame.dropna(subset=["time"]).reset_index(drop=True)

    frame.attrs["resolved_timezone"] = payload.get("timezone")
    frame.attrs["resolved_utc_offset"] = payload.get(
        "utc_offset_seconds"
    )

    return frame


# ==========================================
# IEM / NOAA-ISD METAR VISIBILITY
# ==========================================

def fetch_iem_visibility(icao, timezone_name, start_date, end_date):
    """Fetch observed METAR visibility (converted to km) for one station.

    Source: Iowa Environmental Mesonet ASOS archive (NOAA ISD). Returns an
    empty frame when the station/period has no observations; missing values
    are never fabricated. Visibility is reported in statute miles and
    converted to kilometres; values may be capped near 10 km by the
    reporting standard.
    """

    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)

    params = {
        "station": icao,
        "data": "vsby",
        "year1": start.year,
        "month1": start.month,
        "day1": start.day,
        "year2": end.year,
        "month2": end.month,
        "day2": end.day,
        "tz": timezone_name,
        "format": "onlycomma",
        "latlon": "no",
        "missing": "M",
        "trace": "T",
        "direct": "no",
        "report_type": "3",
    }

    body = _request_text(IEM_ASOS_URL, params)

    if not body.strip():
        return pd.DataFrame(columns=["time", "visibility_km"])

    from io import StringIO

    raw = pd.read_csv(StringIO(body))
    raw.columns = [c.strip() for c in raw.columns]

    if "valid" not in raw.columns or "vsby" not in raw.columns:
        return pd.DataFrame(columns=["time", "visibility_km"])

    frame = pd.DataFrame(
        {
            "time": pd.to_datetime(raw["valid"], errors="coerce"),
            "vsby": pd.to_numeric(raw["vsby"], errors="coerce"),
        }
    )

    frame = frame.dropna(subset=["time", "vsby"])
    frame["visibility_km"] = (
        frame["vsby"] * MILES_TO_KM
    ).clip(upper=IEM_VISIBILITY_CAP_KM)

    return frame[["time", "visibility_km"]]


# ==========================================
# NORMALISATION + MATCHING
# ==========================================

def _floor_to_hour(series):
    return pd.to_datetime(series, errors="coerce").dt.floor("h")


def build_weather_rows(airport_code, archive_frame, visibility_frame):
    """Combine archive reanalysis with observed visibility into rows."""

    frame = archive_frame.copy()
    frame["observation_time"] = _floor_to_hour(frame["time"])

    # One visibility value per local hour (most restrictive observation in
    # that hour), so a low-visibility report is never averaged away.
    if visibility_frame is not None and not visibility_frame.empty:
        vis = visibility_frame.copy()
        vis["observation_time"] = _floor_to_hour(vis["time"])
        vis = (
            vis.groupby("observation_time")["visibility_km"]
            .min()
            .reset_index()
        )
        frame = frame.merge(vis, on="observation_time", how="left")
        frame["visibility_source"] = frame["visibility_km"].apply(
            lambda value: IEM_SOURCE_DATASET if pd.notna(value) else None
        )
    else:
        frame["visibility_km"] = pd.NA
        frame["visibility_source"] = None

    rows = pd.DataFrame(
        {
            "airport_code": airport_code,
            "observation_time": frame["observation_time"],
            "temperature": frame["temperature_2m"],
            "rainfall": frame["precipitation"],
            "wind_speed": frame["wind_speed_10m"],
            "visibility": frame["visibility_km"],
            "relative_humidity": frame["relative_humidity_2m"],
            "cloud_cover": frame["cloud_cover"],
            "weather_code": frame["weather_code"],
            "weather_condition": frame["weather_code"].apply(
                lambda code: WMO_CODE_MAP.get(int(code))
                if pd.notna(code) else None
            ),
            "visibility_source": frame["visibility_source"],
            "source_dataset": ARCHIVE_SOURCE_DATASET,
            "source_model": ARCHIVE_SOURCE_MODEL,
        }
    )

    rows.attrs["resolved_timezone"] = archive_frame.attrs.get(
        "resolved_timezone"
    )

    return rows


def match_flights_to_weather(flights_frame, weather_index, tolerance_minutes=60):
    """Deterministic flight -> weather matching.

    A flight matches the weather observation for its origin airport at the
    hour of its (local, timezone-naive) ``scheduled_departure``. If that hour
    is absent, the nearest observation within +/- tolerance is used, with a
    preference for the earlier observation. No match is returned when
    nothing is available.
    """

    tolerance = timedelta(minutes=tolerance_minutes)
    results = []

    for row in flights_frame.itertuples(index=False):

        target = row.scheduled_departure
        key = (row.airport_code, target)

        if pd.isna(target) or row.airport_code not in weather_index:
            results.append(None)
            continue

        observations = weather_index[row.airport_code]
        floored = target.floor("h")

        if floored in observations:
            results.append(observations[floored])
            continue

        candidates = [
            hour
            for hour in observations
            if abs(hour - floored) <= tolerance
        ]

        if not candidates:
            results.append(None)
            continue

        chosen = sorted(
            candidates, key=lambda hour: (abs(hour - floored), hour)
        )[0]
        results.append(observations[chosen])

    return results


# ==========================================
# DATABASE ACCESS (read-only reads)
# ==========================================

def _read_flights():
    with dw_engine.connect() as conn:
        conn.execute(text("SET SESSION TRANSACTION READ ONLY"))
        try:
            return pd.read_sql(
                text(
                    """
                    SELECT
                        f.source_system,
                        f.source_flight_id,
                        f.scheduled_departure,
                        a.airport_code
                    FROM fact_flight f
                    JOIN dim_route r
                        ON f.route_key = r.route_key
                    JOIN dim_airport a
                        ON r.origin_airport_key = a.airport_key
                    """
                ),
                conn,
            )
        finally:
            conn.execute(text("SET SESSION TRANSACTION READ WRITE"))


def _read_airport_keys():
    with dw_engine.connect() as conn:
        conn.execute(text("SET SESSION TRANSACTION READ ONLY"))
        try:
            frame = pd.read_sql(
                text(
                    "SELECT airport_key, airport_code FROM dim_airport"
                ),
                conn,
            )
        finally:
            conn.execute(text("SET SESSION TRANSACTION READ WRITE"))

    return dict(zip(frame["airport_code"], frame["airport_key"]))


def _identity_columns_present(conn):
    exists = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'dim_weather'
              AND column_name IN
                  (
                      'airport_key', 'observation_time',
                      'source_dataset', 'source_model'
                  )
            """
        )
    ).scalar()
    return exists == 4


def _normalize(value):
    """Convert any scalar missing marker to Python ``None`` (SQL NULL).

    Handles ``None``, Python/NumPy/Pandas ``NaN``, ``NaT`` and ``pd.NA``
    while preserving legitimate values such as ``0``, ``0.0`` and ``False``.
    Only scalar values are expected; array-like values are returned
    unchanged rather than triggering an ambiguous truth-value error.
    """

    if value is None or value is pd.NA or value is pd.NaT:
        return None

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()

    if isinstance(value, datetime):
        return value

    if isinstance(value, np.datetime64):
        if np.isnat(value):
            return None
        return pd.Timestamp(value).to_pydatetime()

    if isinstance(value, (bool, np.bool_)):
        return bool(value)

    if isinstance(value, (int, np.integer)):
        return int(value)

    if isinstance(value, (float, np.floating)):
        return None if np.isnan(value) else float(value)

    if isinstance(value, str):
        return value

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return value


def _prepare_weather_params(row):
    """Build a fully normalized parameter dict for one weather row.

    Every value is normalized so no ``NaN``/``NaT``/``pd.NA`` is handed to
    PyMySQL. Required identity fields are validated and must be present; a
    clear error is raised otherwise so identity can never become NULL
    silently.
    """

    airport_key = _normalize(getattr(row, "airport_key", None))
    observation_time = _normalize(getattr(row, "observation_time", None))
    source_dataset = _normalize(getattr(row, "source_dataset", None))
    source_model = _normalize(getattr(row, "source_model", None))

    if airport_key is None:
        raise ValueError(
            "weather row missing required identity field: airport_key"
        )
    if observation_time is None:
        raise ValueError(
            "weather row missing required identity field: observation_time"
        )
    if not source_dataset:
        raise ValueError(
            "weather row missing required identity field: source_dataset"
        )
    if not source_model:
        raise ValueError(
            "weather row missing required identity field: source_model"
        )

    if not isinstance(observation_time, datetime):
        observation_time = pd.Timestamp(
            observation_time
        ).to_pydatetime()

    return {
        "airport_key": int(airport_key),
        "observation_time": observation_time,
        "temperature": _normalize(getattr(row, "temperature", None)),
        "rainfall": _normalize(getattr(row, "rainfall", None)),
        "wind_speed": _normalize(getattr(row, "wind_speed", None)),
        "visibility": _normalize(getattr(row, "visibility", None)),
        "relative_humidity": _normalize(
            getattr(row, "relative_humidity", None)
        ),
        "cloud_cover": _normalize(getattr(row, "cloud_cover", None)),
        "weather_code": _normalize(getattr(row, "weather_code", None)),
        "weather_condition": _normalize(
            getattr(row, "weather_condition", None)
        ),
        "visibility_source": _normalize(
            getattr(row, "visibility_source", None)
        ),
        "source_dataset": str(source_dataset),
        "source_model": str(source_model),
        "source_timezone": _normalize(
            getattr(row, "source_timezone", None)
        ),
    }


def _upsert_weather(conn, rows):
    """Idempotent upsert.

    Identity is (airport_key, observation_time, source_dataset,
    source_model). A different model/version therefore cannot collide with
    or overwrite another model's row. All parameters are normalized so no
    NaN/NaT/pd.NA reaches the driver.
    """

    if rows.empty:
        return 0

    records = [
        _prepare_weather_params(row)
        for row in rows.itertuples(index=False)
    ]

    statement = text(
        """
        INSERT INTO dim_weather
        (
            airport_key, observation_time, temperature, rainfall,
            wind_speed, visibility, relative_humidity, cloud_cover,
            weather_code, weather_condition, visibility_source,
            source_dataset, source_model, source_timezone
        )
        VALUES
        (
            :airport_key, :observation_time, :temperature, :rainfall,
            :wind_speed, :visibility, :relative_humidity, :cloud_cover,
            :weather_code, :weather_condition, :visibility_source,
            :source_dataset, :source_model, :source_timezone
        ) AS new
        ON DUPLICATE KEY UPDATE
            temperature = new.temperature,
            rainfall = new.rainfall,
            wind_speed = new.wind_speed,
            visibility = new.visibility,
            relative_humidity = new.relative_humidity,
            cloud_cover = new.cloud_cover,
            weather_code = new.weather_code,
            weather_condition = new.weather_condition,
            visibility_source = new.visibility_source,
            source_model = new.source_model,
            source_timezone = new.source_timezone
        """
    )

    written = 0
    for start in range(0, len(records), INSERT_CHUNK_SIZE):
        chunk = records[start:start + INSERT_CHUNK_SIZE]
        conn.execute(statement, chunk)
        written += len(chunk)

    return written


# ==========================================
# WEATHER ETL
# ==========================================

def run_weather_etl(dry_run=False, airport_filter=None,
                    start_date_override=None):
    """Historical weather ETL.

    ``dry_run=True`` performs every step except database writes, allowing
    API + matching validation without mutating the warehouse. This function
    never assigns weather keys to fact_flight - that is a separate, gated
    operation.

    ``start_date_override`` (data-refresh policy, P2): a ``datetime.date``
    that raises the per-airport fetch start to at least that day
    (incremental window; ``refresh_orchestrator`` derives it from the
    per-airport watermarks minus the configured overlap). ``None`` (the
    default) keeps the original full-range behaviour exactly.
    """

    print("\n" + "=" * 70)
    print(" HISTORICAL WEATHER API -> DATA WAREHOUSE ETL")
    print("=" * 70)

    if dry_run:
        print(" [dry-run] No database writes will be performed.")

    if start_date_override is not None:
        print(
            f" Incremental window: fetching from {start_date_override} "
            "(watermark - overlap), never earlier than each airport's span"
        )

    flights = _read_flights()
    airport_keys = _read_airport_keys()

    if airport_filter:
        flights = flights[
            flights["airport_code"].isin(airport_filter)
        ].copy()

    print(f"\n1. Flights with an origin airport: {len(flights)}")

    flights["departure_date"] = pd.to_datetime(
        flights["scheduled_departure"], errors="coerce"
    )

    airports = (
        flights.dropna(subset=["departure_date"])
        .groupby("airport_code")["departure_date"]
        .agg(["min", "max"])
        .sort_index()
    )

    skipped = []
    fetch_failures = []
    weather_records = {}
    matched_hours = {}
    total_rows = 0
    rows_with_visibility = 0

    print("\n2. Fetching historical weather per origin airport...")

    for airport_code, span in airports.iterrows():

        info = AIRPORT_INFO.get(airport_code)

        if not info:
            skipped.append(airport_code)
            print(f" -> {airport_code}: SKIPPED (no coordinates/tz/ICAO)")
            continue

        if airport_code not in airport_keys:
            skipped.append(airport_code)
            print(f" -> {airport_code}: SKIPPED (not in dim_airport)")
            continue

        start_date = max(
            span["min"].date(), ARCHIVE_MIN_DATE
        )
        if start_date_override is not None:
            start_date = max(start_date, start_date_override)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = min(
            span["max"].date(), ARCHIVE_MAX_DATE
        ).strftime("%Y-%m-%d")

        if start_date > end_date:
            skipped.append(airport_code)
            print(
                f" -> {airport_code}: SKIPPED "
                f"(no flights within archive coverage)"
            )
            continue

        try:
            archive = fetch_archive_weather(
                airport_code, info, start_date, end_date
            )
        except RuntimeError as error:
            fetch_failures.append(airport_code)
            print(f" -> {airport_code}: archive FAILED ({error})")
            continue

        try:
            visibility = fetch_iem_visibility(
                info["icao"], info["tz"], start_date, end_date
            )
        except RuntimeError as error:
            visibility = pd.DataFrame(columns=["time", "visibility_km"])
            print(
                f"    {airport_code}: visibility source unavailable "
                f"({error}) - visibility will be NULL"
            )

        built = build_weather_rows(airport_code, archive, visibility)
        built["airport_key"] = airport_keys[airport_code]
        built["source_timezone"] = info["tz"]

        weather_records[airport_code] = built
        total_rows += len(built)
        rows_with_visibility += int(built["visibility"].notna().sum())

        hours = {
            hour: hour for hour in built["observation_time"].dropna()
        }
        matched_hours[airport_code] = hours

        print(
            f" -> {airport_code}: {len(built)} hourly rows "
            f"({start_date}..{end_date}), "
            f"{int(built['visibility'].notna().sum())} with visibility"
        )

    # -------- matching report (no writes) --------

    flights["weather_observation_time"] = match_flights_to_weather(
        flights, matched_hours
    )
    matched = int(flights["weather_observation_time"].notna().sum())
    unmatched = int(len(flights) - matched)

    # -------- persistence (live mode only) --------

    written = 0

    if dry_run:
        print("\n3. [dry-run] Skipping dim_weather upsert.")
    else:
        print("\n3. Upserting weather rows into dim_weather...")
        with dw_engine.begin() as conn:
            if not _identity_columns_present(conn):
                raise RuntimeError(
                    "dim_weather identity columns are missing. Run "
                    "`python setup_database.py migrate-weather` first."
                )
            for airport_code, built in weather_records.items():
                written += _upsert_weather(conn, built)
        print(f" -> Upserted {written} weather rows.")

    # -------- report --------

    print("\n" + "-" * 70)
    print(" WEATHER ETL SUMMARY")
    print("-" * 70)
    print(f"  airports processed     : {len(weather_records)}")
    print(f"  airports skipped       : {skipped}")
    print(f"  airports failed        : {fetch_failures}")
    print(f"  weather rows built     : {total_rows}")
    print(f"  rows with visibility   : {rows_with_visibility}")
    print(f"  weather rows written   : {written} (dry-run: 0)")
    print(f"  flights considered     : {len(flights)}")
    print(f"  flights matched        : {matched}")
    print(f"  flights unmatched      : {unmatched}")
    print(f"  fact weather_key writes: 0 (disabled this phase)")
    print("-" * 70)

    if len(weather_records) == 0 and (skipped or fetch_failures):
        raise RuntimeError(
            "no airport weather could be retrieved "
            f"(skipped={skipped}, failed={fetch_failures})"
        )

    print("\n" + "=" * 70)
    print(" WEATHER ETL COMPLETED SUCCESSFULLY")
    print("=" * 70 + "\n")

    return True


if __name__ == "__main__":

    dry_run = "--dry-run" in sys.argv
    filters = None

    for argument in sys.argv[1:]:
        if argument.startswith("--airports="):
            filters = argument.split("=", 1)[1].split(",")

    run_weather_etl(dry_run=dry_run, airport_filter=filters)
