"""Refresh orchestrator: audited, locked, verified execution of the existing
ETL stages (etl.py / weather_etl.py / oltp_etl.py) - WITHOUT changing them.

Guarantees
----------
* Existing ETL entry points and transformations are invoked as-is; this
  module never reimplements extract/transform/load logic.
* One run at a time: a MySQL named lock (GET_LOCK) serialises refresh runs
  across processes on the same server.
* Every stage attempt is recorded in etl_batch_log (RUNNING -> terminal)
  using a separate autocommit session, so a rolled-back stage still leaves
  its FAILED record behind.
* Audit integrity gates the exit code: if the audit layer cannot write the
  start of a stage, the stage is NOT executed; if it cannot write the
  terminal state, the run stops and exits non-zero even though stage data
  may already be committed.
* Post-load verification (verify_warehouse.py, read-only) runs after the
  stages; failures there exit 6 and prevent watermark advancement.
* Watermarks (weather, config-driven) advance only after the stage transaction
  committed AND verification actually ran and did not fail. Skipping
  verification (--no-verify / run_verification=false) suppresses watermark
  advancement entirely.
* --dry-run and --verify-only are strictly read-only: they never execute
  DDL, never create audit tables, never insert audit rows, never take the
  run lock and never invoke an ETL stage.

Exit codes
----------
   0  all required stages SUCCESS and audit trail complete (verification
      passed or explicitly skipped)
   1  a REQUIRED stage did not finish SUCCESS (FAILED/PARTIAL/SKIPPED)
   2  preflight failed (configuration, connectivity)
   3  another refresh run holds the lock
   4  audit layer unavailable before a stage (fail-closed: stage not run)
   5  audit/watermark write failed after stage execution - data may be
      committed; investigate before the next run
   6  stages succeeded but post-load verification did not pass
   130 interrupted by the operator (Ctrl-C); any still-open audit row is
      closed as FAILED, committed stage data is not rolled back

Failure policy (documented)
---------------------------
Continue-on-stage-failure, matching main_etl.py: a failed stage does not
stop later stages, because sources are independent and every load is
idempotent. The exit code still reports the failure. Exit code 0 is never
returned when a required stage or the audit trail failed.

Usage
-----
  python refresh_orchestrator.py                    # all enabled stages
  python refresh_orchestrator.py --source csv,oltp  # subset
  python refresh_orchestrator.py --dry-run          # read-only preflight
  python refresh_orchestrator.py --verify-only      # read-only verification
  python refresh_orchestrator.py --enable-changes   # P2: bounded OLTP updates
"""

import argparse
import csv as csv_module
import io
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import etl_audit
import verify_warehouse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(BASE_DIR, "config", "refresh_policy.json")
LOG_DIR = os.path.join(BASE_DIR, "logs")

EXIT_OK = 0
EXIT_STAGE_FAILED = 1
EXIT_PREFLIGHT = 2
EXIT_LOCKED = 3
EXIT_AUDIT_START = 4
EXIT_AUDIT_FINISH = 5
EXIT_VERIFY_FAILED = 6
EXIT_INTERRUPTED = 130


# =========================================================
# CONFIG
# =========================================================

def _config_bool(value, label):
    """Reject JSON truthiness traps (e.g. "false", 1) at load time."""
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be a boolean")
    return value


def _config_int(value, label, minimum):
    """Accept only real JSON integers, never bools/strings/floats."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    if value < minimum:
        qualifier = "positive" if minimum == 1 else "zero or greater"
        raise ValueError(f"{label} must be {qualifier}")
    return value


def load_config(path=None):
    path = path or DEFAULT_CONFIG
    with open(path, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    order = config.get("order") or ["csv", "weather", "oltp"]
    if not isinstance(order, list) or not all(
        isinstance(name, str) and name for name in order
    ):
        raise ValueError("order must be a list of source names")
    unknown = set(order) - {"csv", "weather", "oltp"}
    if unknown:
        raise ValueError(f"unknown source(s) in order: {sorted(unknown)}")
    config["order"] = order

    sources = config.setdefault("sources", {})
    if not isinstance(sources, dict):
        raise ValueError("sources must be an object")
    for name in order:
        entry = sources.setdefault(name, {})
        if not isinstance(entry, dict):
            raise ValueError(f"sources.{name} must be an object")
        entry.setdefault("enabled", True)
        entry.setdefault("required", True)
        entry["enabled"] = _config_bool(
            entry["enabled"], f"sources.{name}.enabled"
        )
        entry["required"] = _config_bool(
            entry["required"], f"sources.{name}.required"
        )

    config["lock_timeout_seconds"] = _config_int(
        config.get("lock_timeout_seconds", 0),
        "lock_timeout_seconds",
        0,
    )
    config["stale_running_minutes"] = _config_int(
        config.get("stale_running_minutes", 240),
        "stale_running_minutes",
        1,
    )
    config["run_verification"] = _config_bool(
        config.get("run_verification", True), "run_verification"
    )
    config["verification_fails_run"] = _config_bool(
        config.get("verification_fails_run", True),
        "verification_fails_run",
    )
    csv_path = config.get("csv_path", "data/flight.csv")
    if not isinstance(csv_path, str) or not csv_path:
        raise ValueError("csv_path must be a non-empty string")
    config["csv_path"] = csv_path
    config["weather_chunk_size"] = _config_int(
        config.get("weather_chunk_size", 1000),
        "weather_chunk_size",
        1,
    )

    incremental = config.get("weather_incremental", {})
    if not isinstance(incremental, dict):
        raise ValueError("weather_incremental must be an object")
    incremental.setdefault("enabled", False)
    incremental.setdefault("overlap_days", 7)
    incremental["enabled"] = _config_bool(
        incremental["enabled"], "weather_incremental.enabled"
    )
    incremental["overlap_days"] = _config_int(
        incremental["overlap_days"],
        "weather_incremental.overlap_days",
        0,
    )
    config["weather_incremental"] = incremental
    return config


# =========================================================
# OUTPUT CAPTURE (tee: console + per-stage log file)
# =========================================================

class TeeIO(io.TextIOBase):
    def __init__(self, *streams):
        self._streams = streams

    def write(self, text):
        for stream in self._streams:
            try:
                stream.write(text)
                stream.flush()
            except Exception:
                pass
        return len(text)

    def flush(self):
        for stream in self._streams:
            try:
                stream.flush()
            except Exception:
                pass

def _is_prunable_log(fname):
    """Only per-run wrapper/stage logs may be pruned.

    launchd-owned stdout/stderr files are never touched: launchd keeps the
    file handle open and rotates/truncates it itself.
    """
    return fname.endswith((".log", ".err")) and (
        fname.startswith("refresh_")
        or fname.startswith("scheduled_refresh_")
    )


def _resolve_retention_days(retention_days):
    """Resolve the retention policy; invalid values fail safe to 30 days."""
    raw = os.environ.get("ETL_LOG_RETENTION_DAYS", "30") if retention_days is None else retention_days
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 30


def prune_old_logs(log_dir=LOG_DIR, retention_days=None, keep_latest=10):
    """Prune old per-run logs; preserve the newest ``keep_latest`` files.

    ``retention_days`` defaults to ``ETL_LOG_RETENTION_DAYS`` (30 days);
    values <= 0 disable pruning. Only ``refresh_*`` and
    ``scheduled_refresh_*`` files are eligible, so launchd stdout/stderr is
    never deleted and an active wrapper log is protected by recency.
    """
    if not os.path.isdir(log_dir):
        return
    days = _resolve_retention_days(retention_days)
    if days <= 0:
        return
    cutoff = time.time() - (days * 86400)

    candidates = []
    for fname in os.listdir(log_dir):
        if not _is_prunable_log(fname):
            continue
        fpath = os.path.join(log_dir, fname)
        if not os.path.isfile(fpath):
            continue
        try:
            candidates.append((os.path.getmtime(fpath), fname, fpath))
        except OSError:
            continue

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    protected = {
        fname for _, fname, _ in candidates[:max(int(keep_latest or 0), 0)]
    }
    for mtime, fname, fpath in candidates:
        if fname in protected or mtime >= cutoff:
            continue
        try:
            os.unlink(fpath)
        except OSError:
            pass


def _save_stage_log(source, captured, batch_guid):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(
            LOG_DIR, f"refresh_{stamp}_{batch_guid[:8]}_{source}.log"
        )
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(captured)
        return path
    except Exception:
        return None


# =========================================================
# MEASUREMENT (only numbers that can be measured honestly)
# =========================================================

def _count_target(source):
    """Rows in the stage's target table for this source (read-only)."""
    from sqlalchemy import text

    from database import dw_engine

    with dw_engine.connect() as conn:
        if source in ("csv", "oltp"):
            return conn.execute(
                text(
                    "SELECT COUNT(*) FROM fact_flight WHERE source_system = :s"
                ),
                {"s": source},
            ).scalar()
        if source == "weather":
            return conn.execute(text("SELECT COUNT(*) FROM dim_weather")).scalar()
    raise ValueError(f"no target table for source {source!r}")


def _rows_read_csv(path):
    """Data rows in the CSV source (header excluded)."""
    try:
        with open(path, newline="", encoding="utf-8") as handle:
            return max(sum(1 for _ in csv_module.reader(handle)) - 1, 0)
    except OSError:
        return None


def _rows_read_oltp():
    """Source flight rows (read-only). The extract query in oltp_etl.py is
    row-preserving from flights (pre-aggregated 1:1 joins), so COUNT(flights)
    equals the extracted row count."""
    from sqlalchemy import text

    from database import oltp_engine

    try:
        with oltp_engine.connect() as conn:
            return conn.execute(text("SELECT COUNT(*) FROM flights")).scalar()
    except Exception:
        return None


def _measure(source, before, after, rows_read):
    """Derive honest metrics for the audit row.

    csv/oltp are insert-only: inserted = target delta; skipped = source rows
    not inserted (already present or in-batch duplicate).
    weather is an upsert: 'inserted' means NET-NEW rows; updated counts are
    not measurable without instrumenting the stage, so they stay NULL.
    """
    inserted = None
    skipped = None
    if before is not None and after is not None:
        inserted = max(int(after) - int(before), 0)
    if source in ("csv", "oltp") and rows_read is not None and inserted is not None:
        skipped = max(int(rows_read) - inserted, 0)
    return {
        "rows_read": rows_read,
        "rows_before": before,
        "rows_after": after,
        "rows_inserted": inserted,
        "rows_updated": None,
        "rows_skipped": skipped,
    }


# =========================================================
# WEATHER PARTIAL-STATUS FROM THE STAGE RESULT
# =========================================================

def _weather_stage_note(stage_result):
    """Return a PARTIAL note from the weather stage's structured result.

    The stage result is authoritative; console text is never parsed.
    """
    if not isinstance(stage_result, dict):
        return None
    failed = stage_result.get("airports_failed") or []
    if failed:
        rendered = ", ".join(str(airport) for airport in failed)
        return f"archive fetch failed for: [{rendered}]"
    return None


# =========================================================
# WEATHER WATERMARKS (config-driven, per-airport windows)
# =========================================================

def _overlap_days(config):
    inc = config.get("weather_incremental") or {}
    try:
        overlap = int(inc.get("overlap_days", 7))
    except (TypeError, ValueError) as error:
        raise ValueError(
            "weather_incremental.overlap_days must be an integer"
        ) from error
    if overlap < 0:
        raise ValueError(
            "weather_incremental.overlap_days must be zero or greater"
        )
    return overlap


def _parse_watermark_datetime(value, airport_code=None):
    """Parse a stored watermark into the airport's local wall-clock time.

    Legacy watermarks are naive local observation times, matching legacy
    ``dim_weather.observation_time`` rows. UTC-model watermarks are stored
    with a trailing ``Z`` and are converted to the airport's local wall
    clock here for window derivation. Unparseable/empty values are treated
    as missing: the caller then fetches that airport's full history instead
    of guessing an unsafe window."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        tz_name = None
        try:
            import weather_etl

            info = weather_etl.AIRPORT_INFO.get(airport_code) or {}
            tz_name = info.get("tz")
        except Exception:
            tz_name = None
        if tz_name:
            try:
                from zoneinfo import ZoneInfo

                parsed = parsed.astimezone(ZoneInfo(tz_name))
            except Exception:
                pass
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _parse_watermark_utc(value, airport_code=None):
    """Parse a stored watermark into a naive UTC datetime.

    A value with an explicit UTC designator (written after a UTC-model
    advancement) is authoritative UTC. A legacy naive value is converted
    from the airport's local wall clock so old and new watermarks can be
    compared on the same basis. Returns None when the value is missing,
    unparseable, or the legacy conversion would be a DST guess."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(
            tzinfo=None, microsecond=0
        )
    try:
        import weather_etl
        from timestamp_semantics import local_to_utc

        info = weather_etl.AIRPORT_INFO.get(airport_code) or {}
        tz_name = info.get("tz")
        if not tz_name:
            return None
        return local_to_utc(
            parsed.replace(microsecond=0), tz_name
        ).astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _parse_watermark_date(value, airport_code=None):
    parsed = _parse_watermark_datetime(value, airport_code)
    return parsed.date() if parsed is not None else None


def _expected_weather_window(span_start, span_end, watermark_date, overlap,
                             archive_min, archive_max):
    """Return the inclusive (start, end) window a run must cover.

    ``span_start``/``span_end`` are the airport's own flight dates. The
    start is the later of the airport's first flight, archive coverage and
    (when present) its own watermark minus overlap. The end is the earlier
    of the airport's last flight and the archive's latest available day."""
    start = max(span_start, archive_min)
    if watermark_date is not None:
        start = max(start, watermark_date - timedelta(days=overlap))
    end = min(span_end, archive_max)
    return start, end


def weather_start_overrides(config):
    """Per-airport fetch starts derived from each airport's own watermark.

    Returns ``{airport_code: date}`` for airports with a usable watermark,
    or None when incremental mode is off / no watermark exists. Airports
    without a watermark are deliberately ABSENT so ``weather_etl`` keeps
    their full available history (safe first run and recovery behaviour)."""
    inc = config.get("weather_incremental") or {}
    if not inc.get("enabled"):
        return None
    overlap = _overlap_days(config)
    watermarks = etl_audit.get_watermarks("weather")  # AuditError propagates
    if not watermarks:
        return None
    overrides = {}
    for airport, value in watermarks.items():
        mark_date = _parse_watermark_date(value, airport)
        if mark_date is None:
            print(
                f" -> [warn] weather watermark for {airport} is unreadable "
                f"({value!r}); fetching its full history"
            )
            continue
        overrides[airport] = mark_date - timedelta(days=overlap)
    return overrides or None


class CoverageGateResult:
    """Structured outcome from the full-span UTC coverage verification gate."""

    def __init__(
        self,
        airport_code,
        passed,
        coverage_basis="utc",
        expected_range=None,
        verified_range=None,
        total_expected=0,
        total_verified=0,
        missing_count=0,
        missing_intervals=None,
        duplicate_count=0,
        wrong_model_count=0,
        invalid_timestamp_count=0,
        reason=None,
        candidate_watermark=None,
    ):
        self.airport_code = airport_code
        self.passed = bool(passed)
        self.coverage_basis = coverage_basis
        self.expected_range = expected_range
        self.verified_range = verified_range
        self.total_expected = total_expected
        self.total_verified = total_verified
        self.missing_count = missing_count
        self.missing_intervals = missing_intervals or []
        self.duplicate_count = duplicate_count
        self.wrong_model_count = wrong_model_count
        self.invalid_timestamp_count = invalid_timestamp_count
        self.reason = reason
        self.candidate_watermark = candidate_watermark

    def as_dict(self):
        return {
            "airport_code": self.airport_code,
            "passed": self.passed,
            "coverage_basis": self.coverage_basis,
            "expected_range": (
                (self.expected_range[0].isoformat(), self.expected_range[1].isoformat())
                if self.expected_range else None
            ),
            "verified_range": (
                (self.verified_range[0].isoformat(), self.verified_range[1].isoformat())
                if self.verified_range else None
            ),
            "total_expected": self.total_expected,
            "total_verified": self.total_verified,
            "missing_count": self.missing_count,
            "missing_intervals": [
                (start.isoformat(), end.isoformat())
                for start, end in self.missing_intervals
            ],
            "duplicate_count": self.duplicate_count,
            "wrong_model_count": self.wrong_model_count,
            "invalid_timestamp_count": self.invalid_timestamp_count,
            "reason": self.reason,
            "candidate_watermark": (
                self.candidate_watermark.isoformat()
                if self.candidate_watermark else None
            ),
        }

    def summary(self):
        exp_str = (
            f"{self.expected_range[0]}..{self.expected_range[1]}"
            if self.expected_range else "none"
        )
        ver_str = (
            f"{self.verified_range[0]}..{self.verified_range[1]}"
            if self.verified_range else "none"
        )
        if self.missing_intervals:
            intervals_str = ", ".join(
                f"{start}..{end}" for start, end in self.missing_intervals[:3]
            )
            if len(self.missing_intervals) > 3:
                intervals_str += f" (+{len(self.missing_intervals) - 3} more)"
        else:
            intervals_str = "none"

        return (
            f"{self.reason} "
            f"(expected: {exp_str}, verified: {ver_str}, "
            f"missing_intervals: [{intervals_str}], duplicates: {self.duplicate_count})"
        )
    def format_report(self):
        exp_str = (
            f"{self.expected_range[0]}..{self.expected_range[1]}"
            if self.expected_range else "none"
        )
        ver_str = (
            f"{self.verified_range[0]}..{self.verified_range[1]}"
            if self.verified_range else "none"
        )
        if self.missing_intervals:
            intervals_str = ", ".join(
                f"{start}..{end}" for start, end in self.missing_intervals[:3]
            )
            if len(self.missing_intervals) > 3:
                intervals_str += f" (+{len(self.missing_intervals) - 3} more)"
        else:
            intervals_str = "none"

        return (
            f"airport={self.airport_code}, expected={exp_str}, "
            f"verified={ver_str}, missing_count={self.missing_count}, "
            f"missing_intervals=[{intervals_str}], duplicates={self.duplicate_count}, "
            f"wrong_model={self.wrong_model_count}, invalid_ts={self.invalid_timestamp_count}, "
            f"status={'PASS' if self.passed else 'HELD'}"
            + (f", reason={self.reason}" if self.reason else "")
        )


def generate_expected_utc_instants(start_date, end_date, tz_name):
    """Derive expected UTC hourly instants for local calendar dates.

    Open-Meteo's archive API with `timeformat=unixtime` and `timezone=tz_name`
    returns one observation for every hour of each local calendar day from
    start_date through end_date (inclusive).

    This helper accounts for:
    - Airport timezone wall-clock bounds
    - Timezone offsets, including half-hour offsets (e.g. Asia/Colombo, minute=30)
    - DST spring-forward transitions (23-hour local days)
    - DST fall-back transitions (25-hour local days)
    - Consecutive UTC hourly instants spaced by exactly 3600 seconds.

    Returns a list of timezone-naive UTC datetime objects, matching
    ``dim_weather.observation_time``.
    """
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(str(tz_name))
    except Exception as error:
        raise ValueError(f"invalid IANA timezone {tz_name!r}") from error

    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()

    if start_date > end_date:
        return []

    # Start of local start_date: 00:00:00 wall clock
    start_local = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
    start_utc = start_local.replace(tzinfo=tz, fold=0).astimezone(timezone.utc)
    while start_utc.astimezone(tz).date() < start_date:
        start_utc += timedelta(hours=1)
    while (start_utc - timedelta(hours=1)).astimezone(tz).date() >= start_date:
        start_utc -= timedelta(hours=1)

    # End of local end_date: 23:00:00 wall clock
    end_local = datetime(end_date.year, end_date.month, end_date.day, 23, 0, 0)
    end_utc = end_local.replace(tzinfo=tz, fold=1).astimezone(timezone.utc)
    while end_utc.astimezone(tz).date() > end_date:
        end_utc -= timedelta(hours=1)
    while (end_utc + timedelta(hours=1)).astimezone(tz).date() <= end_date:
        end_utc += timedelta(hours=1)

    instants = []
    curr = start_utc
    while curr <= end_utc:
        loc_dt = curr.astimezone(tz)
        if start_date <= loc_dt.date() <= end_date:
            instants.append(curr.astimezone(timezone.utc).replace(tzinfo=None))
        curr += timedelta(hours=1)
    return instants


def verify_utc_weather_coverage(
    conn,
    airport_code,
    flight_start,
    flight_end,
    tz_name,
    stored_watermark=None,
    config=None,
):
    """Verify full-span UTC archive coverage for one airport before watermark advancement.

    The gate independently checks:
    1. The expected archive range is derived from the historical archive/flight start
       through requested end date. Legacy local watermarks NEVER shorten this range.
    2. Only rows with ``source_model = 'best_match_utc_v1'`` satisfy coverage.
    3. Exact expected UTC hourly instants are matched rather than naive local dates.
    4. Timezone shifts, DST spring-forward/fall-back, and half-hour offsets are handled.
    5. Missing observations, duplicate instants, invalid timestamps, and wrong-model
       rows are detected and block advancement.
    6. Incomplete archive ranges and missing provider data block advancement; missing
       data is never silently assumed complete.
    7. Legacy local watermarks cannot satisfy UTC-model coverage, and candidate watermarks
       cannot move backwards.

    Returns a ``CoverageGateResult``.
    """
    from weather_etl import (
        ARCHIVE_MAX_DATE,
        ARCHIVE_MIN_DATE,
        ARCHIVE_UTC_SOURCE_MODEL,
    )

    if config is None:
        config = load_config()

    if isinstance(flight_start, datetime):
        flight_start = flight_start.date()
    if isinstance(flight_end, datetime):
        flight_end = flight_end.date()

    expected_start_date = max(flight_start, ARCHIVE_MIN_DATE)
    configured_start = config.get("weather_start_date")
    if configured_start:
        try:
            if isinstance(configured_start, str):
                configured_start = datetime.fromisoformat(configured_start).date()
            expected_start_date = max(expected_start_date, configured_start)
        except Exception:
            pass

    expected_end_date = min(flight_end, ARCHIVE_MAX_DATE)

    if expected_start_date > expected_end_date:
        return CoverageGateResult(
            airport_code=airport_code,
            passed=False,
            coverage_basis="utc",
            reason=f"no archive coverage required ({expected_start_date}..{expected_end_date})",
        )

    try:
        expected_instants = generate_expected_utc_instants(
            expected_start_date, expected_end_date, tz_name
        )
    except Exception as error:
        return CoverageGateResult(
            airport_code=airport_code,
            passed=False,
            coverage_basis="utc",
            reason=f"failed to compute expected UTC instants: {error}",
        )

    if not expected_instants:
        return CoverageGateResult(
            airport_code=airport_code,
            passed=False,
            coverage_basis="utc",
            reason=f"no archive coverage required ({expected_start_date}..{expected_end_date})",
        )

    expected_range = (expected_instants[0], expected_instants[-1])
    total_expected = len(expected_instants)

    from sqlalchemy import text
    rows = conn.execute(
        text(
            """
            SELECT w.observation_time,
                   w.source_model,
                   COUNT(*) AS cnt
            FROM dim_weather w
            JOIN dim_airport a ON w.airport_key = a.airport_key
            WHERE a.airport_code = :airport
              AND w.source_dataset = 'open-meteo-archive'
            GROUP BY w.observation_time, w.source_model
            """
        ),
        {"airport": airport_code},
    ).fetchall()

    utc_instants_map = {}
    wrong_model_count = 0
    invalid_timestamp_count = 0

    for obs_time, source_model, cnt in rows:
        if obs_time is None:
            invalid_timestamp_count += cnt
            continue
        if source_model == ARCHIVE_UTC_SOURCE_MODEL:
            utc_instants_map[obs_time] = cnt
        else:
            wrong_model_count += cnt

    duplicate_count = sum(cnt - 1 for cnt in utc_instants_map.values() if cnt > 1)
    verified_set = set(utc_instants_map.keys())
    total_verified = len(verified_set)
    verified_range = (min(verified_set), max(verified_set)) if verified_set else None

    missing = [t for t in expected_instants if t not in verified_set]
    missing_count = len(missing)

    missing_intervals = []
    if missing:
        interval_start = missing[0]
        prev = missing[0]
        for t in missing[1:]:
            if t - prev == timedelta(hours=1):
                prev = t
            else:
                missing_intervals.append((interval_start, prev))
                interval_start = t
                prev = t
        missing_intervals.append((interval_start, prev))

    persisted_in_window = [t for t in expected_instants if t in verified_set]
    if not persisted_in_window:
        reason = (
            f"no persisted observations for {expected_start_date}..{expected_end_date} "
            f"in the UTC model (legacy rows ignored)"
        )
        return CoverageGateResult(
            airport_code=airport_code,
            passed=False,
            coverage_basis="utc",
            expected_range=expected_range,
            verified_range=verified_range,
            total_expected=total_expected,
            total_verified=total_verified,
            missing_count=missing_count,
            missing_intervals=missing_intervals,
            duplicate_count=duplicate_count,
            wrong_model_count=wrong_model_count,
            invalid_timestamp_count=invalid_timestamp_count,
            reason=reason,
        )

    if invalid_timestamp_count > 0:
        reason = (
            f"invalid timestamps detected: {invalid_timestamp_count} "
            f"row(s) with NULL observation_time"
        )
        return CoverageGateResult(
            airport_code=airport_code,
            passed=False,
            coverage_basis="utc",
            expected_range=expected_range,
            verified_range=verified_range,
            total_expected=total_expected,
            total_verified=total_verified,
            missing_count=missing_count,
            missing_intervals=missing_intervals,
            duplicate_count=duplicate_count,
            wrong_model_count=wrong_model_count,
            invalid_timestamp_count=invalid_timestamp_count,
            reason=reason,
        )

    if duplicate_count > 0:
        reason = f"duplicate UTC instants detected ({duplicate_count} duplicate rows)"
        return CoverageGateResult(
            airport_code=airport_code,
            passed=False,
            coverage_basis="utc",
            expected_range=expected_range,
            verified_range=verified_range,
            total_expected=total_expected,
            total_verified=total_verified,
            missing_count=missing_count,
            missing_intervals=missing_intervals,
            duplicate_count=duplicate_count,
            wrong_model_count=wrong_model_count,
            invalid_timestamp_count=invalid_timestamp_count,
            reason=reason,
        )

    if missing:
        first_missing = missing[0]
        reason = (
            f"incomplete persisted coverage: {missing_count} observation(s) "
            f"missing from {expected_start_date}..{expected_end_date} "
            f"(first missing {first_missing})"
        )
        return CoverageGateResult(
            airport_code=airport_code,
            passed=False,
            coverage_basis="utc",
            expected_range=expected_range,
            verified_range=verified_range,
            total_expected=total_expected,
            total_verified=total_verified,
            missing_count=missing_count,
            missing_intervals=missing_intervals,
            duplicate_count=duplicate_count,
            wrong_model_count=wrong_model_count,
            invalid_timestamp_count=invalid_timestamp_count,
            reason=reason,
        )

    if verified_range[0] > expected_range[0] or verified_range[1] < expected_range[1]:
        reason = (
            f"incomplete archive range: verified {verified_range[0]}..{verified_range[1]}, "
            f"expected {expected_range[0]}..{expected_range[1]}"
        )
        return CoverageGateResult(
            airport_code=airport_code,
            passed=False,
            coverage_basis="utc",
            expected_range=expected_range,
            verified_range=verified_range,
            total_expected=total_expected,
            total_verified=total_verified,
            missing_count=missing_count,
            missing_intervals=missing_intervals,
            duplicate_count=duplicate_count,
            wrong_model_count=wrong_model_count,
            invalid_timestamp_count=invalid_timestamp_count,
            reason=reason,
        )

    # Candidate watermark evaluation: only expected, verified in-window
    # instants may advance the marker. Stale or future rows are reported
    # above but must never inflate the watermark.
    candidate = max(persisted_in_window).replace(microsecond=0)
    if stored_watermark is not None:
        watermark_utc = _parse_watermark_utc(stored_watermark, airport_code)
        watermark_mark = _parse_watermark_datetime(stored_watermark, airport_code)
        if watermark_mark is not None and watermark_utc is None:
            reason = "cannot reconcile the legacy local watermark to UTC safely"
            return CoverageGateResult(
                airport_code=airport_code,
                passed=False,
                coverage_basis="utc",
                expected_range=expected_range,
                verified_range=verified_range,
                total_expected=total_expected,
                total_verified=total_verified,
                missing_count=missing_count,
                missing_intervals=missing_intervals,
                duplicate_count=duplicate_count,
                wrong_model_count=wrong_model_count,
                invalid_timestamp_count=invalid_timestamp_count,
                reason=reason,
            )
        if watermark_utc is not None and candidate < watermark_utc:
            reason = (
                f"validated UTC data would move the watermark backwards "
                f"({candidate} < {watermark_utc})"
            )
            return CoverageGateResult(
                airport_code=airport_code,
                passed=False,
                coverage_basis="utc",
                expected_range=expected_range,
                verified_range=verified_range,
                total_expected=total_expected,
                total_verified=total_verified,
                missing_count=missing_count,
                missing_intervals=missing_intervals,
                duplicate_count=duplicate_count,
                wrong_model_count=wrong_model_count,
                invalid_timestamp_count=invalid_timestamp_count,
                reason=reason,
            )

    return CoverageGateResult(
        airport_code=airport_code,
        passed=True,
        coverage_basis="utc",
        expected_range=expected_range,
        verified_range=verified_range,
        total_expected=total_expected,
        total_verified=total_verified,
        missing_count=0,
        missing_intervals=[],
        duplicate_count=0,
        wrong_model_count=wrong_model_count,
        invalid_timestamp_count=0,
        reason=None,
        candidate_watermark=candidate,
    )


def advance_weather_watermarks(batch_guid, config=None, dry_run=False):
    """Advance per-airport watermarks AFTER the weather stage committed.

    An airport's watermark moves only when its independently queried
    persisted observations cover the required archive window.

    Coverage is evaluated on a per-model basis:
    - Once an airport has any UTC-model rows (``best_match_utc_v1``) or an
      existing UTC watermark, the model-aware full-span UTC coverage verification
      gate runs. Only ``best_match_utc_v1`` rows can satisfy the check; legacy rows
      cannot mask gaps or satisfy coverage. The expected start is derived from the
      full archive-supported flight history, never from a legacy watermark.
      New UTC watermarks are stored with a trailing ``Z``.
    - For airports with strictly legacy data and legacy watermarks, the legacy
      daily check is preserved unchanged.

    If ``dry_run`` is True, proposed watermarks are returned in ``advanced``
    without performing any write to ``etl_watermark``.

    Returns ``(advanced, held)`` for reporting."""
    from sqlalchemy import text

    from database import dw_engine
    from weather_etl import (
        AIRPORT_INFO,
        ARCHIVE_MAX_DATE,
        ARCHIVE_MIN_DATE,
        ARCHIVE_UTC_SOURCE_MODEL,
    )

    if config is None:
        config = load_config()
    overlap = _overlap_days(config)
    watermarks = etl_audit.get_watermarks("weather")

    advanced = {}
    held = {}
    with dw_engine.connect() as conn:
        spans = {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                text(
                    """
                    SELECT a.airport_code,
                           MIN(DATE(f.scheduled_departure)),
                           MAX(DATE(f.scheduled_departure))
                    FROM fact_flight f
                    JOIN dim_route r ON f.route_key = r.route_key
                    JOIN dim_airport a ON r.origin_airport_key = a.airport_key
                    GROUP BY a.airport_code
                    """
                )
            ).fetchall()
        }

        for airport, (span_start, span_end) in spans.items():
            if airport not in AIRPORT_INFO:
                held[airport] = "no coordinates/timezone/ICAO configured"
                continue

            stored_mark = watermarks.get(airport)
            has_utc_watermark = (
                stored_mark is not None
                and str(stored_mark).strip().endswith("Z")
            )
            has_utc_rows = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM dim_weather w
                    JOIN dim_airport a ON w.airport_key = a.airport_key
                    WHERE a.airport_code = :airport
                      AND w.source_dataset = 'open-meteo-archive'
                      AND w.source_model = :utc_model
                      AND w.observation_time IS NOT NULL
                    LIMIT 1
                    """
                ),
                {
                    "airport": airport,
                    "utc_model": ARCHIVE_UTC_SOURCE_MODEL,
                },
            ).first() is not None

            coverage_basis = "utc" if (has_utc_watermark or has_utc_rows) else "legacy"

            if coverage_basis == "utc":
                info = AIRPORT_INFO[airport]
                tz_name = info.get("tz")
                gate = verify_utc_weather_coverage(
                    conn,
                    airport,
                    span_start,
                    span_end,
                    tz_name,
                    stored_watermark=stored_mark,
                    config=config,
                )
                if not gate.passed:
                    held[airport] = gate.summary()
                    continue

                stored_value = gate.candidate_watermark.strftime(
                    "%Y-%m-%d %H:%M:%SZ"
                )
                if not dry_run:
                    etl_audit.record_watermark(
                        "weather", airport, stored_value, batch_guid,
                    )
                advanced[airport] = stored_value
            else:
                watermark_mark = _parse_watermark_datetime(
                    stored_mark, airport
                )
                if watermark_mark is not None:
                    watermark_mark = watermark_mark.replace(microsecond=0)
                watermark_date = (
                    watermark_mark.date() if watermark_mark is not None else None
                )
                expected_start, expected_end = _expected_weather_window(
                    span_start, span_end, watermark_date, overlap,
                    ARCHIVE_MIN_DATE, ARCHIVE_MAX_DATE,
                )
                if expected_start > expected_end:
                    held[airport] = (
                        f"no archive coverage required "
                        f"({expected_start}..{expected_end})"
                    )
                    continue

                raw_coverage_rows = conn.execute(
                    text(
                        """
                        SELECT w.source_model,
                               DATE(w.observation_time) AS obs_date,
                               MAX(w.observation_time) AS max_obs
                        FROM dim_weather w
                        JOIN dim_airport a ON w.airport_key = a.airport_key
                        WHERE a.airport_code = :airport
                          AND w.source_dataset = 'open-meteo-archive'
                          AND w.observation_time IS NOT NULL
                          AND DATE(w.observation_time) >= :expected_start
                          AND DATE(w.observation_time) <= :expected_end
                        GROUP BY w.source_model, DATE(w.observation_time)
                        """
                    ),
                    {
                        "airport": airport,
                        "expected_start": expected_start,
                        "expected_end": expected_end,
                    },
                ).fetchall()

                if not raw_coverage_rows:
                    held[airport] = (
                        f"no persisted observations for "
                        f"{expected_start}..{expected_end}"
                    )
                    continue

                present_dates = {row[1] for row in raw_coverage_rows}
                max_obs = max(row[2] for row in raw_coverage_rows)
                missing = [
                    expected_start + timedelta(days=offset)
                    for offset in range(
                        (expected_end - expected_start).days + 1
                    )
                    if expected_start + timedelta(days=offset)
                    not in present_dates
                ]
                if missing:
                    held[airport] = (
                        f"incomplete persisted coverage: {len(missing)} day(s) "
                        f"missing from {expected_start}..{expected_end} "
                        f"(first missing {missing[0]})"
                    )
                    continue

                candidate = max_obs.replace(microsecond=0)
                if watermark_mark is not None and candidate < watermark_mark:
                    held[airport] = (
                        f"validated data would move the watermark backwards "
                        f"({candidate} < {watermark_mark})"
                    )
                    continue

                stored_value = candidate
                if not dry_run:
                    etl_audit.record_watermark(
                        "weather", airport, stored_value, batch_guid,
                    )
                advanced[airport] = stored_value

    return advanced, held


# =========================================================
# DEFAULT STAGES (thin wrappers - no transformation logic here)
# =========================================================

def _default_stages(config):
    from etl import run_etl as run_csv_etl
    from oltp_etl import run_oltp_etl
    from weather_etl import run_weather_etl

    # Batch size: config default (1000) equals the module's existing
    # INSERT_CHUNK_SIZE, so default behaviour is byte-for-byte unchanged.
    try:
        import weather_etl

        weather_etl.INSERT_CHUNK_SIZE = int(config["weather_chunk_size"])
    except Exception:
        pass

    def weather_stage(start_date_override=None, start_date_overrides=None):
        if start_date_overrides is not None:
            return run_weather_etl(start_date_overrides=start_date_overrides)
        if start_date_override is not None:
            return run_weather_etl(start_date_override=start_date_override)
        return run_weather_etl()

    return {
        "csv": lambda: run_csv_etl(),
        "weather": weather_stage,
        "oltp": lambda: run_oltp_etl(),
    }


# =========================================================
# MAIN RUNNER (injectable stages/verifier for tests)
# =========================================================

def _selected_sources(config, only=None):
    order = config.get("order") or ["csv", "weather", "oltp"]
    selected = []
    for name in order:
        entry = config["sources"].get(name, {})
        if not entry.get("enabled", True):
            print(f" -> [{name}] disabled by config - skipped")
            continue
        if only and name not in only:
            continue
        selected.append(name)
    if only:
        unknown = set(only) - set(order)
        if unknown:
            raise ValueError(f"unknown source(s): {sorted(unknown)}")
    return selected


def _run_verification(verifier):
    from database import dw_engine

    with dw_engine.connect() as conn:
        checks = verifier(conn)
    print("\n" + "=" * 78)
    print(" POST-LOAD VERIFICATION (read-only)")
    print("=" * 78)
    for check in checks:
        print(f"  [{check.status:4}] {check.name}: {check.detail}")
    counts = {
        status: sum(1 for c in checks if c.status == status)
        for status in ("PASS", "WARN", "FAIL", "SKIP")
    }
    print("-" * 78)
    print(
        f"  PASS={counts['PASS']}  WARN={counts['WARN']}  "
        f"FAIL={counts['FAIL']}  SKIP={counts['SKIP']}"
    )
    print("=" * 78 + "\n")
    return checks


def run_refresh(
    config,
    stages=None,
    verifier=None,
    sources=None,
    verify=True,
    enable_changes=False,
    dry_run=False,
):
    started = time.time()
    batch_guid = str(uuid.uuid4())
    print("\n" + "#" * 78)
    print(f" REFRESH RUN {batch_guid}")
    print("#" * 78)

    # ---------- preflight (no side effects) ----------
    try:
        selected = _selected_sources(config, sources)
    except ValueError as error:
        print(f"\n[FATAL][EXIT {EXIT_PREFLIGHT}] {error}")
        return EXIT_PREFLIGHT
    if not selected:
        print(f"\n[FATAL][EXIT {EXIT_PREFLIGHT}] no sources selected")
        return EXIT_PREFLIGHT

    if enable_changes and "oltp" not in selected:
        # Misconfiguration guard: --enable-changes only has an effect when
        # the oltp stage runs. Never silently ignore the flag.
        print(
            "\n[warn] --enable-changes was requested but source 'oltp' is "
            "not selected (disabled in config/refresh_policy.json or "
            "excluded via --source); the OLTP change updater will NOT run"
        )

    if verifier is None:
        stale_minutes = int(config.get("stale_running_minutes", 240))
        verifier = (
            lambda conn: verify_warehouse.run_checks(
                conn, stale_minutes=stale_minutes
            )
        )

    # Read-only mode: report audit-table existence, never create anything.
    if dry_run:
        return _dry_run(config, selected)

    # Normal run: ensure the additive audit schema. ensure_schema() is a
    # pure existence check when both tables are present - no DDL is
    # executed against an already-initialised warehouse.
    try:
        etl_audit.ensure_schema()
    except etl_audit.AuditError as error:
        print(f"\n[FATAL][EXIT {EXIT_AUDIT_START}] audit schema unavailable: {error}")
        return EXIT_AUDIT_START

    try:
        lock = etl_audit.acquire_run_lock(
            timeout=int(config.get("lock_timeout_seconds", 0))
        )
    except etl_audit.AuditError as error:
        print(f"\n[FATAL][EXIT {EXIT_AUDIT_START}] lock unavailable: {error}")
        return EXIT_AUDIT_START
    if lock is None:
        print(
            f"\n[LOCKED][EXIT {EXIT_LOCKED}] another refresh run holds "
            f"{etl_audit.RUN_LOCK_NAME}"
        )
        return EXIT_LOCKED

    try:
        return _execute(
            config,
            selected,
            batch_guid,
            stages,
            verifier,
            verify,
            enable_changes,
            started,
        )
    except KeyboardInterrupt:
        # Safety net for interrupts outside the handled stage/verify
        # windows: the lock is released below; a still-open RUNNING audit
        # row (if any) is reported as stale by verification after
        # stale_running_minutes and is never mutated automatically.
        print(
            f"\n[INTERRUPTED][EXIT {EXIT_INTERRUPTED}] operator interrupt; "
            "lock released; inspect etl_batch_log for a RUNNING row"
        )
        _print_summary({}, batch_guid, started, aborted=True)
        return EXIT_INTERRUPTED
    finally:
        try:
            lock.release()
        finally:
            prune_old_logs()


def _execute(config, selected, batch_guid, stages, verifier, verify,
             enable_changes, started):
    if stages is None:
        stages = _default_stages(config)

    results = {}          # source -> {"status", "notes", "metrics"}
    audit_failure = None  # (exit_code, message) when audit layer broke
    interrupted = False   # Ctrl-C: close the current stage, stop the run

    for source in selected:
        print("\n" + "-" * 78)
        print(f" >>> STAGE [{source}] (batch {batch_guid[:8]})")
        print("-" * 78)

        # P2: compute per-airport weather fetch windows BEFORE the stage;
        # an audit failure here is fail-closed (stage simply does not run).
        start_overrides = None
        if source == "weather" and (config.get("weather_incremental") or {}).get("enabled"):
            try:
                start_overrides = weather_start_overrides(config)
                if start_overrides:
                    rendered = ", ".join(
                        f"{airport}>={start}"
                        for airport, start in sorted(start_overrides.items())
                    )
                    print(f" -> incremental windows: {rendered}")
                else:
                    print(
                        " -> incremental windows: no usable watermarks - "
                        "full history for every airport"
                    )
            except etl_audit.AuditError as error:
                audit_failure = (
                    EXIT_AUDIT_START,
                    f"cannot read weather watermark (stage not run): {error}",
                )
                break

        try:
            before = _count_target(source)
        except Exception as error:
            before = None
            print(f" -> [warn] could not count target before stage: {error}")

        if source == "csv":
            rows_read = _rows_read_csv(
                os.path.join(BASE_DIR, config["csv_path"])
            )
        elif source == "oltp":
            rows_read = _rows_read_oltp()
        else:
            rows_read = None

        try:
            run_info = etl_audit.start_run(source, batch_guid)
        except etl_audit.AuditError as error:
            audit_failure = (
                EXIT_AUDIT_START,
                f"cannot audit stage start (stage not run): {error}",
            )
            break

        captured = io.StringIO()
        real_stdout = sys.stdout
        sys.stdout = TeeIO(real_stdout, captured)
        stage_error = None
        stage_result = None
        try:
            kwargs = {}
            if start_overrides is not None:
                kwargs["start_date_overrides"] = start_overrides
            stage_result = stages[source](**kwargs)
        except KeyboardInterrupt:
            interrupted = True
            stage_error = KeyboardInterrupt(
                "interrupted by operator (Ctrl-C) before stage completion"
            )
            print(f"\n[!] stage {source} interrupted by operator")
        except Exception as error:  # stage failure: recorded, not swallowed
            stage_error = error
            print(f"\n[!] stage {source} failed: {error}")
        finally:
            sys.stdout = real_stdout
            log_path = _save_stage_log(source, captured.getvalue(), batch_guid)
            if log_path:
                print(f" -> stage log: {os.path.relpath(log_path, BASE_DIR)}")

        try:
            after = _count_target(source)
        except Exception:
            after = None

        metrics = _measure(source, before, after, rows_read)
        notes = []
        error_detail = None
        if stage_error is not None:
            status = "FAILED"
            error_detail = str(stage_error)
        else:
            status = "SUCCESS"
            if source == "weather":
                partial = _weather_stage_note(stage_result)
                if partial:
                    status = "PARTIAL"
                    notes.append(partial)

        # P2: bounded, opt-in change updates for OLTP (insert-only otherwise)
        if (
            status == "SUCCESS"
            and source == "oltp"
            and enable_changes
        ):
            try:
                from oltp_change_updater import update_changed_facts

                update_result = update_changed_facts()
                metrics["rows_updated"] = update_result.get("rows_updated")
                notes.append(
                    f"bounded change-update applied: "
                    f"{update_result.get('rows_updated')} row(s)"
                )
                missing = update_result.get("rows_missing_in_dw") or 0
                if missing:
                    notes.append(
                        f"{missing} source flight(s) not loaded yet "
                        "(insert-only loader owns inserts)"
                    )
                print(
                    f" -> change-update: {update_result.get('rows_updated')} "
                    "existing fact row(s) refreshed"
                )
                if missing:
                    print(
                        f" -> change-update: {missing} source flight(s) have "
                        "no fact row yet (reported, not inserted)"
                    )
            except Exception as error:
                status = "FAILED"
                error_detail = (
                    "inserts committed, but bounded change-update failed: "
                    + str(error)
                )
                notes.append(
                    "stage inserts were committed before the change-update "
                    "failure (insert-only state is valid)"
                )
                print(f"[!] {error_detail}")

        try:
            etl_audit.finish_run(
                run_info["run_id"],
                status,
                notes="; ".join(notes) if notes else None,
                error=error_detail,
                **metrics,
            )
        except etl_audit.AuditError as error:
            audit_failure = (
                EXIT_AUDIT_FINISH,
                f"stage {source} finished with status {status} but the audit "
                f"write failed - remaining stages NOT executed: {error}",
            )
            results[source] = {"status": status, "notes": notes, "metrics": metrics}
            break

        results[source] = {"status": status, "notes": notes, "metrics": metrics}
        print(
            f" -> audit: {source} = {status} "
            f"(read={metrics['rows_read']} inserted={metrics['rows_inserted']} "
            f"skipped={metrics['rows_skipped']})"
        )

        if interrupted:
            _print_summary(results, batch_guid, started, aborted=True)
            print(
                f"\n[INTERRUPTED][EXIT {EXIT_INTERRUPTED}] operator interrupt; "
                "the lock is released; committed stage data is NOT rolled back"
            )
            return EXIT_INTERRUPTED

    # ---------- audit-layer failures dominate ----------
    if audit_failure:
        code, message = audit_failure
        print(f"\n[FATAL][EXIT {code}] {message}")
        _print_summary(results, batch_guid, started, aborted=True)
        return code

    # ---------- verification ----------
    verify_checks = None
    verify_ok = True
    verification_ran = False  # watermark gate: verification must have RUN
    if verify and config.get("run_verification", True):
        verification_ran = True
        try:
            info = etl_audit.start_run("verify", batch_guid)
        except etl_audit.AuditError as error:
            print(
                f"\n[FATAL][EXIT {EXIT_AUDIT_START}] cannot audit verification "
                f"start (verification not run): {error}"
            )
            _print_summary(results, batch_guid, started, aborted=True)
            return EXIT_AUDIT_START

        verify_error = None
        try:
            verify_checks = _run_verification(verifier)
        except KeyboardInterrupt:
            try:
                etl_audit.finish_run(
                    info["run_id"],
                    "FAILED",
                    notes="verification interrupted by operator",
                    error="interrupted by operator (Ctrl-C)",
                )
            except etl_audit.AuditError:
                pass  # row stays RUNNING; stale detection reports it
            _print_summary(results, batch_guid, started, aborted=True)
            print(
                f"\n[INTERRUPTED][EXIT {EXIT_INTERRUPTED}] operator interrupt "
                "during verification; watermarks not advanced"
            )
            return EXIT_INTERRUPTED
        except Exception as error:
            verify_checks = None
            verify_ok = False
            verify_error = f"verification could not complete: {error}"
            print(f"[!] {verify_error}")
        else:
            verify_ok = not any(c.status == "FAIL" for c in verify_checks)

        if verify_checks is None:
            verify_notes = "post-load verification could not complete"
        else:
            verify_notes = (
                "post-load verification "
                + ("passed" if verify_ok else "reported FAIL checks")
            )
        try:
            etl_audit.finish_run(
                info["run_id"],
                "SUCCESS" if verify_ok else "FAILED",
                notes=verify_notes,
                error=None if verify_ok else (
                    verify_error or "one or more checks FAILED"
                ),
            )
        except KeyboardInterrupt:
            try:
                etl_audit.finish_run(
                    info["run_id"],
                    "FAILED",
                    notes="verification interrupted by operator",
                    error="interrupted by operator (Ctrl-C)",
                )
            except etl_audit.AuditError:
                pass  # row stays RUNNING; stale detection reports it
            _print_summary(results, batch_guid, started, aborted=True)
            print(f"\n[INTERRUPTED][EXIT {EXIT_INTERRUPTED}] operator interrupt")
            return EXIT_INTERRUPTED
        except etl_audit.AuditError as error:
            print(f"\n[FATAL][EXIT {EXIT_AUDIT_FINISH}] verify audit failed: {error}")
            _print_summary(results, batch_guid, started, aborted=True)
            return EXIT_AUDIT_FINISH

    # ---------- watermark advance (only after commit + verification) ----------
    # Gate: incremental enabled AND stage successful AND verification
    # actually RAN and did not FAIL. --no-verify / run_verification=false
    # therefore never advances watermarks (documented policy).
    weather_status = (results.get("weather") or {}).get("status")
    if (
        (config.get("weather_incremental") or {}).get("enabled")
        and weather_status == "SUCCESS"
        and verification_ran
        and verify_ok
    ):
        try:
            advanced, held = advance_weather_watermarks(batch_guid, config)
            print(
                f" -> watermarks advanced for {len(advanced)} airport(s); "
                f"held for {len(held)}"
            )
            for airport, reason in sorted(held.items()):
                print(f"    held {airport}: {reason}")
        except etl_audit.AuditError as error:
            print(
                f"\n[FATAL][EXIT {EXIT_AUDIT_FINISH}] weather stage committed "
                f"but watermark write failed (next run re-fetches - safe): {error}"
            )
            _print_summary(results, batch_guid, started, aborted=True)
            return EXIT_AUDIT_FINISH
        except Exception as error:
            print(
                f"\n[FATAL][EXIT {EXIT_AUDIT_FINISH}] weather stage committed "
                f"but watermark advancement check failed "
                f"({type(error).__name__}: {error}); watermarks that were not "
                f"advanced will be re-covered next run"
            )
            _print_summary(results, batch_guid, started, aborted=True)
            return EXIT_AUDIT_FINISH
    elif weather_status and (
        config.get("weather_incremental") or {}
    ).get("enabled") and weather_status == "SUCCESS":
        if not verification_ran:
            print(
                " -> weather watermarks NOT advanced: verification was "
                "skipped (--no-verify / run_verification=false); re-run "
                "with verification to advance them"
            )
        else:
            print(
                " -> weather watermarks unchanged (verification failed)"
            )
    elif weather_status:
        incremental_on = bool(
            (config.get("weather_incremental") or {}).get("enabled")
        )
        if incremental_on and weather_status == "PARTIAL":
            print(
                " -> weather watermarks NOT advanced: weather stage was "
                "PARTIAL (at least one airport failed)"
            )
        elif incremental_on and weather_status != "SUCCESS":
            print(
                f" -> weather watermarks NOT advanced: weather stage was "
                f"{weather_status}"
            )
        else:
            print(
                " -> weather watermarks unchanged (incremental mode is off)"
            )

    # ---------- exit code ----------
    _print_summary(results, batch_guid, started)

    for source, entry in results.items():
        required = config["sources"].get(source, {}).get("required", True)
        if required and entry["status"] != "SUCCESS":
            print(
                f"\n[EXIT {EXIT_STAGE_FAILED}] required stage '{source}' "
                f"ended as {entry['status']}"
            )
            return EXIT_STAGE_FAILED

    if verify and config.get("run_verification", True) and not verify_ok:
        if config.get("verification_fails_run", True):
            print(
                f"\n[EXIT {EXIT_VERIFY_FAILED}] post-load verification "
                "did not pass"
            )
            return EXIT_VERIFY_FAILED
        print("\n[warn] verification failed but verification_fails_run=false")

    print(f"\n[EXIT {EXIT_OK}] refresh completed successfully")
    return EXIT_OK


def _print_summary(results, batch_guid, started, aborted=False):
    print("\n" + "#" * 78)
    label = "REFRESH ABORTED" if aborted else "REFRESH SUMMARY"
    print(f" {label} - batch {batch_guid} ({round(time.time() - started, 2)}s)")
    for source, entry in results.items():
        metrics = entry["metrics"]
        print(
            f"   {source:8} {entry['status']:8} "
            f"read={metrics['rows_read']} inserted={metrics['rows_inserted']} "
            f"updated={metrics['rows_updated']} skipped={metrics['rows_skipped']}"
        )
    if not results:
        print("   (no stage executed)")
    print("#" * 78)


# =========================================================
# DRY RUN (preflight only - performs no data loads)
# =========================================================

def _dry_run(config, selected):
    """Connectivity/config probe. Executes no ETL stage and no audit run row."""
    print("\n[DRY RUN] no ETL stage will be executed.")
    ok = True

    from sqlalchemy import text

    from database import dw_engine

    try:
        with dw_engine.connect() as conn:
            version = conn.execute(text("SELECT VERSION()")).scalar()
        print(f" -> warehouse: reachable (MySQL {version})")
    except Exception as error:
        print(f" -> warehouse: UNREACHABLE ({error})")
        ok = False

    if "oltp" in selected:
        from database import oltp_engine

        try:
            with oltp_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(" -> OLTP source: reachable")
        except Exception as error:
            print(f" -> OLTP source: UNREACHABLE ({etl_audit.sanitize_error(error)})")
            ok = False

    if "csv" in selected:
        path = os.path.join(BASE_DIR, config["csv_path"])
        if os.path.exists(path):
            print(f" -> CSV source: present ({_rows_read_csv(path)} data rows)")
        else:
            print(f" -> CSV source: MISSING ({config['csv_path']})")
            ok = False

    print(" -> weather API: not probed in dry-run (no network side effects)")
    print(f" -> stages selected: {selected}")
    print(f" -> incremental weather: "
          f"{'ON' if config['weather_incremental'].get('enabled') else 'OFF'}")
    try:
        audit_present = etl_audit.audit_table_exists()
    except etl_audit.AuditError:
        audit_present = None  # warehouse unreachable or unreadable
    if audit_present:
        print(" -> audit schema: present (no DDL executed in dry-run)")
    elif audit_present is False:
        print(
            " -> audit schema: MISSING (a normal run creates it once, or "
            "apply sql/audit_schema.sql manually; dry-run never creates it)"
        )
    else:
        print(" -> audit schema: UNKNOWN (warehouse unreachable)")

    print(f"\n[DRY RUN] {'PREFLIGHT OK' if ok else 'PREFLIGHT FAILED'}")
    return EXIT_OK if ok else EXIT_PREFLIGHT

def show_refresh_status(limit=5):
    """Query etl_batch_log and report recent batch runs, stage outcomes, and watermarks."""
    print("\n" + "=" * 78)
    print(" RECENT REFRESH RUNS & WATERMARKS (read-only)")
    print("=" * 78)
    try:
        runs = etl_audit.recent_runs(limit=limit)
        if not runs:
            print(" -> no audit rows found in etl_batch_log")
        else:
            print(f" -> Latest {len(runs)} stage executions:")
            for r in runs:
                bg = str(r.get("batch_guid") or "")[:8] or "none"
                src = r.get("source_name") or "unknown"
                st = r.get("status") or "unknown"
                start = str(r.get("started_at") or "")[:19] or "?"
                err = f" | error: {r.get('error_detail')}" if r.get("error_detail") else ""
                print(
                    f"   [{start}] batch={bg} stage={src:8} status={st:8} "
                    f"read={r.get('rows_read')} inserted={r.get('rows_inserted')} "
                    f"updated={r.get('rows_updated')} skipped={r.get('rows_skipped')}{err}"
                )

        watermarks = etl_audit.get_watermarks("weather")
        print("\n -> Current weather watermarks:")
        if not watermarks:
            print("   (none recorded)")
        else:
            for k, v in sorted(watermarks.items()):
                print(f"   {k:6}: {v}")
    except Exception as error:
        print(f" -> error querying status: {error}")
        return 1
    print("=" * 78)
    return 0


# =========================================================
# CLI
# =========================================================

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Audited refresh of the aviation warehouse (see docs/ETL_OPERATIONS.md)"
    )
    parser.add_argument("--source", metavar="CSV,WEATHER,OLTP",
                        help="run only these stages (default: all enabled)")
    parser.add_argument("--config", metavar="FILE", default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="preflight only; no ETL stage, no audit run row")
    parser.add_argument("--verify-only", action="store_true",
                        help="run read-only verification and exit")
    parser.add_argument("--no-verify", action="store_true",
                        help="skip post-load verification (weather watermarks "
                             "will NOT advance when verification is skipped)")
    parser.add_argument("--enable-changes", action="store_true",
                        help="P2: after a successful OLTP load, apply bounded "
                             "updates to mutable attributes of EXISTING OLTP "
                             "fact rows (requires the 'oltp' stage to run; "
                             "run `python setup_database.py backup` first)")
    parser.add_argument("--status", action="store_true",
                        help="display recent refresh runs and watermark status from etl_batch_log without executing stages")
    args = parser.parse_args(argv)
    if args.status:
        return show_refresh_status()

    try:
        config = load_config(args.config)
    except Exception as error:
        print(f"[FATAL][EXIT {EXIT_PREFLIGHT}] cannot load config: {error}")
        return EXIT_PREFLIGHT

    only = None
    if args.source:
        only = {part.strip().lower() for part in args.source.split(",") if part.strip()}

    if args.verify_only:
        try:
            # Strictly read-only: never create audit tables here. If they
            # are missing, audit-related checks report SKIP with an
            # actionable hint; the operator bootstraps them via a normal
            # run or sql/audit_schema.sql.
            if not etl_audit.audit_table_exists():
                print(
                    "[note] audit table etl_batch_log is missing - audit "
                    "checks will be SKIP. Create it once via a normal "
                    "orchestrator run or by applying sql/audit_schema.sql "
                    "(--verify-only never executes DDL)."
                )
            checks = _run_verification(
                verify_warehouse.run_checks
            )
            return 1 if any(c.status == "FAIL" for c in checks) else 0
        except Exception as error:
            print(f"[FATAL][EXIT {EXIT_PREFLIGHT}] {error}")
            return EXIT_PREFLIGHT

    return run_refresh(
        config,
        sources=only,
        verify=not args.no_verify,
        enable_changes=args.enable_changes,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
