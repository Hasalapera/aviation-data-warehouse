"""Read-only-first linker for fact_flight.weather_key.

The linker uses origin-airport metadata, the versioned UTC Open-Meteo archive
model and warehouse connection settings. It never creates or edits weather
observations and only proposes changes to fact_flight.weather_key.

Flight timestamp semantics
--------------------------
CSV and OLTP flight timestamps have no documented timezone contract in this
repository. They are therefore ``unresolved`` and are never silently treated
as airport-local or UTC. A source becomes linkable only when a verified
semantics declaration says ``origin_local`` or ``utc``. Ambiguous and
nonexistent DST wall-clock times are held.

The source CSV encodes scheduled_departure as HHMM; etl.py already converts
that to the timezone-naive DATETIME stored in fact_flight. The linker reads
the warehouse DATETIME so existing flight timestamps are not rebuilt. The
HHMM conversion requested for the CSV representation is provided by
combine_flight_date_and_minutes() and is validated in tests.

Default mode is a read-only dry run. Database updates require --execute and
are limited to UPDATE fact_flight SET weather_key = ... by flight_key.
"""

from collections import Counter
from datetime import datetime, time, timedelta
from numbers import Integral
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
from sqlalchemy import text

from timestamp_semantics import (
    FLIGHT_TIMESTAMP_SEMANTICS,
    SUPPORTED_SEMANTICS,
    AmbiguousLocalTime,
    InvalidTimestamp,
    NonexistentLocalTime,
    flight_utc_hour,
)
from weather_etl import (
    AIRPORT_INFO,
    ARCHIVE_SOURCE_DATASET,
    ARCHIVE_UTC_SOURCE_MODEL,
)

DEPARTURE_TIMEZONE_ASSUMPTION = (
    "flight source semantics are unresolved by default; only an explicit "
    "verified origin_local or utc declaration enables exact matching"
)

MATCH = "MATCH"
NO_EXACT_CANDIDATE = "NO_EXACT_CANDIDATE"
AMBIGUOUS_CANDIDATE = "AMBIGUOUS_CANDIDATE"
UNSUPPORTED_AIRPORT = "UNSUPPORTED_AIRPORT"
UNSUPPORTED_TIMEZONE = "UNSUPPORTED_TIMEZONE"
INVALID_DEPARTURE = "INVALID_DEPARTURE"
NULL_VISIBILITY = "NULL_VISIBILITY"
UNRESOLVED_FLIGHT_TIMESTAMP = "UNRESOLVED_FLIGHT_TIMESTAMP"
AMBIGUOUS_LOCAL_TIME = "AMBIGUOUS_LOCAL_TIME"
NONEXISTENT_LOCAL_TIME = "NONEXISTENT_LOCAL_TIME"

_STATUSES = (
    MATCH,
    NO_EXACT_CANDIDATE,
    AMBIGUOUS_CANDIDATE,
    UNSUPPORTED_AIRPORT,
    UNSUPPORTED_TIMEZONE,
    INVALID_DEPARTURE,
    NULL_VISIBILITY,
    UNRESOLVED_FLIGHT_TIMESTAMP,
    AMBIGUOUS_LOCAL_TIME,
    NONEXISTENT_LOCAL_TIME,
)

_CANDIDATE_QUERY = """
    SELECT
        f.flight_key,
        f.source_system,
        f.source_flight_id,
        f.scheduled_departure,
        f.weather_key AS old_weather_key,
        a.airport_code,
        w.weather_key AS candidate_weather_key,
        w.observation_time AS candidate_observation_time,
        w.visibility AS candidate_visibility,
        w.source_dataset AS candidate_source_dataset,
        w.source_model AS candidate_source_model
    FROM fact_flight AS f
    LEFT JOIN dim_route AS r
        ON f.route_key = r.route_key
    LEFT JOIN dim_airport AS a
        ON r.origin_airport_key = a.airport_key
    LEFT JOIN dim_weather AS w
        ON w.airport_key = a.airport_key
       AND w.observation_time BETWEEN
               DATE_SUB(f.scheduled_departure, INTERVAL 1 DAY)
               AND DATE_ADD(f.scheduled_departure, INTERVAL 1 DAY)
       AND w.source_dataset = :source_dataset
       AND w.source_model = :source_model
    ORDER BY f.flight_key, w.weather_key
"""

_UPDATE_QUERY = """
    UPDATE fact_flight
    SET weather_key = :new_weather_key
    WHERE flight_key = :flight_key
      AND weather_key <=> :old_weather_key
"""


def _is_missing(value):
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _optional_int(value):
    if _is_missing(value):
        return None
    return int(value)


def _airport_code(value):
    if _is_missing(value):
        return None
    return str(value).strip().upper() or None


def combine_flight_date_and_minutes(flight_date, minutes):
    """Return the naive local datetime for a flight date and minute offset.

    Minutes must be an integral value in [0, 1439]. Values outside that
    range are rejected rather than silently rolling into another day.
    """
    if _is_missing(flight_date):
        raise ValueError("flight_date is missing")
    try:
        date_value = pd.Timestamp(flight_date)
    except (TypeError, ValueError) as error:
        raise ValueError("flight_date is invalid") from error
    if date_value is pd.NaT or pd.isna(date_value):
        raise ValueError("flight_date is invalid")
    if date_value.tzinfo is not None:
        raise ValueError("flight_date must not be timezone-aware")
    if date_value.normalize() != date_value:
        raise ValueError("flight_date must not include a time of day")

    if _is_missing(minutes) or isinstance(minutes, bool):
        raise ValueError("scheduled_departure minutes are missing or invalid")
    if isinstance(minutes, Integral):
        minute_value = int(minutes)
    else:
        try:
            numeric = float(minutes)
        except (TypeError, ValueError) as error:
            raise ValueError(
                "scheduled_departure minutes are invalid"
            ) from error
        if not numeric.is_integer():
            raise ValueError("scheduled_departure minutes must be integral")
        minute_value = int(numeric)

    if minute_value < 0 or minute_value > 1439:
        raise ValueError("scheduled_departure minutes must be in [0, 1439]")

    return datetime.combine(date_value.date(), time()) + timedelta(
        minutes=minute_value
    )


def _parse_hour(value):
    if _is_missing(value):
        return None
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if stamp is pd.NaT or pd.isna(stamp):
        return None
    if stamp.tzinfo is not None:
        return None
    return stamp.floor("h").to_pydatetime()


def _parse_utc_instant(value):
    """Parse a stored UTC instant without flooring it to another hour."""

    if _is_missing(value):
        return None
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if stamp is pd.NaT or pd.isna(stamp):
        return None
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    if stamp.second != 0 or stamp.microsecond != 0:
        return None
    return stamp.to_pydatetime()


def airport_timezone(airport_code):
    """Return (IANA timezone, status) using the existing AIRPORT_INFO map."""
    code = _airport_code(airport_code)
    if code is None or code not in AIRPORT_INFO:
        return None, UNSUPPORTED_AIRPORT
    tz_name = AIRPORT_INFO[code].get("tz")
    if not tz_name:
        return None, UNSUPPORTED_TIMEZONE
    try:
        ZoneInfo(str(tz_name))
    except (ZoneInfoNotFoundError, TypeError, ValueError):
        return None, UNSUPPORTED_TIMEZONE
    return str(tz_name), None


def _new_entry(row):
    return {
        "flight_key": int(row["flight_key"]),
        "source_system": row.get("source_system"),
        "source_flight_id": row.get("source_flight_id"),
        "scheduled_departure": row.get("scheduled_departure"),
        "old_weather_key": _optional_int(row.get("old_weather_key")),
        "airport_code": _airport_code(row.get("airport_code")),
        "candidates": [],
    }


def build_link_plan(candidate_rows, source_semantics=None):
    """Build one proposed-link row per flight from broad candidate rows.

    The SQL stage fetches candidates around each flight date; this function
    performs the exact UTC-instant comparison. Unresolved source semantics,
    DST gaps/repeated hours and non-exact candidates are held, never guessed.
    """

    semantics_map = (
        FLIGHT_TIMESTAMP_SEMANTICS
        if source_semantics is None
        else {**FLIGHT_TIMESTAMP_SEMANTICS, **source_semantics}
    )
    grouped = {}
    order = []
    for raw in candidate_rows:
        row = dict(raw)
        if _is_missing(row.get("flight_key")):
            continue
        flight_key = int(row["flight_key"])
        if flight_key not in grouped:
            grouped[flight_key] = _new_entry(row)
            order.append(flight_key)
        candidate_key = row.get("candidate_weather_key")
        if _is_missing(candidate_key):
            continue
        grouped[flight_key]["candidates"].append(
            {
                "weather_key": int(candidate_key),
                "observation_time": row.get("candidate_observation_time"),
                "visibility": row.get("candidate_visibility"),
                "source_dataset": row.get("candidate_source_dataset"),
                "source_model": row.get("candidate_source_model"),
            }
        )

    return [
        _evaluate(grouped[key], semantics_map) for key in order
    ]


def _evaluate(entry, source_semantics=None):
    semantics_map = (
        FLIGHT_TIMESTAMP_SEMANTICS
        if source_semantics is None
        else {**FLIGHT_TIMESTAMP_SEMANTICS, **source_semantics}
    )
    old_key = entry["old_weather_key"]
    plan = {
        "flight_key": entry["flight_key"],
        "source_system": entry["source_system"],
        "source_flight_id": entry["source_flight_id"],
        "scheduled_departure": entry["scheduled_departure"],
        "airport_code": entry["airport_code"],
        "timezone": None,
        "source_semantics": None,
        "target_hour": None,
        "target_utc_hour": None,
        "old_weather_key": old_key,
        "new_weather_key": None,
        "candidate_count": 0,
        "status": None,
        "reason": None,
    }

    tz_name, tz_status = airport_timezone(entry["airport_code"])
    if tz_status is not None:
        plan["status"] = tz_status
        if tz_status == UNSUPPORTED_AIRPORT:
            plan["reason"] = "origin airport is absent from AIRPORT_INFO"
        else:
            plan["reason"] = (
                f"origin airport {entry['airport_code']} has no valid "
                "IANA timezone"
            )
        return plan
    plan["timezone"] = tz_name

    target_hour = _parse_hour(entry["scheduled_departure"])
    if target_hour is None:
        plan["status"] = INVALID_DEPARTURE
        plan["reason"] = "scheduled_departure is missing or invalid"
        return plan
    plan["target_hour"] = target_hour

    source = str(entry.get("source_system") or "").strip().lower()
    semantics = semantics_map.get(source, "unresolved")
    if semantics not in SUPPORTED_SEMANTICS:
        plan["status"] = UNRESOLVED_FLIGHT_TIMESTAMP
        plan["reason"] = (
            f"source_system={source or 'unknown'} has unresolved timestamp "
            "semantics; no conversion or matching is permitted"
        )
        return plan
    plan["source_semantics"] = semantics

    try:
        target_utc = flight_utc_hour(target_hour, tz_name, semantics)
    except AmbiguousLocalTime as error:
        plan["status"] = AMBIGUOUS_LOCAL_TIME
        plan["reason"] = str(error)
        return plan
    except NonexistentLocalTime as error:
        plan["status"] = NONEXISTENT_LOCAL_TIME
        plan["reason"] = str(error)
        return plan
    except (InvalidTimestamp, ValueError) as error:
        plan["status"] = INVALID_DEPARTURE
        plan["reason"] = str(error)
        return plan

    if target_utc is None:
        plan["status"] = UNRESOLVED_FLIGHT_TIMESTAMP
        plan["reason"] = "timestamp semantics do not permit conversion"
        return plan
    plan["target_utc_hour"] = target_utc.replace(tzinfo=None)

    candidates = {}
    for candidate in entry["candidates"]:
        if (
            candidate["source_dataset"] != ARCHIVE_SOURCE_DATASET
            or candidate["source_model"] != ARCHIVE_UTC_SOURCE_MODEL
        ):
            continue
        observation_utc = _parse_utc_instant(
            candidate["observation_time"]
        )
        if observation_utc != target_utc:
            continue
        candidates[candidate["weather_key"]] = candidate

    plan["candidate_count"] = len(candidates)
    if not candidates:
        plan["status"] = NO_EXACT_CANDIDATE
        plan["reason"] = (
            f"no exact {ARCHIVE_SOURCE_DATASET}/"
            f"{ARCHIVE_UTC_SOURCE_MODEL} observation at UTC "
            f"{target_utc:%Y-%m-%d %H:%M:%S}"
        )
        return plan
    if len(candidates) > 1:
        plan["status"] = AMBIGUOUS_CANDIDATE
        plan["reason"] = (
            f"{len(candidates)} exact candidates at UTC "
            f"{target_utc:%Y-%m-%d %H:%M:%S}"
        )
        return plan

    candidate = next(iter(candidates.values()))
    if _is_missing(candidate["visibility"]):
        plan["status"] = NULL_VISIBILITY
        plan["reason"] = (
            f"exact candidate weather_key={candidate['weather_key']} has "
            "NULL visibility"
        )
        return plan

    new_key = candidate["weather_key"]
    plan["new_weather_key"] = new_key
    plan["status"] = MATCH
    if old_key == new_key:
        plan["reason"] = "already linked to this candidate"
    elif old_key is None:
        plan["reason"] = "add link"
    else:
        plan["reason"] = "replace existing link"
    return plan


def summarize_plan(plan):
    counts = Counter(row["status"] for row in plan)
    pending = [
        row
        for row in plan
        if row["status"] == MATCH
        and row["new_weather_key"] != row["old_weather_key"]
    ]
    currently_linked = sum(
        1 for row in plan if row["old_weather_key"] is not None
    )
    replacing = sum(
        1
        for row in plan
        if row["old_weather_key"] is not None
        and row["new_weather_key"] is not None
        and row["old_weather_key"] != row["new_weather_key"]
    )
    adding = sum(
        1
        for row in plan
        if row["old_weather_key"] is None
        and row["new_weather_key"] is not None
    )
    linked_but_held = sum(
        1
        for row in plan
        if row["old_weather_key"] is not None and row["status"] != MATCH
    )
    by_source = {}
    for row in plan:
        source = row["source_system"] or "unknown"
        by_source.setdefault(source, Counter())[row["status"]] += 1
    return {
        "total": len(plan),
        "matched": counts[MATCH],
        "unmatched": len(plan) - counts[MATCH],
        "changed": len(pending),
        "unchanged": sum(
            1
            for row in plan
            if row["status"] == MATCH
            and row["new_weather_key"] == row["old_weather_key"]
        ),
        "currently_linked": currently_linked,
        "replacing": replacing,
        "adding": adding,
        "linked_but_held": linked_but_held,
        "no_exact_candidate": counts[NO_EXACT_CANDIDATE],
        "ambiguous": counts[AMBIGUOUS_CANDIDATE],
        "unsupported_airport": counts[UNSUPPORTED_AIRPORT],
        "unsupported_timezone": counts[UNSUPPORTED_TIMEZONE],
        "invalid_departure": counts[INVALID_DEPARTURE],
        "null_visibility": counts[NULL_VISIBILITY],
        "unresolved_flight_timestamp": counts[UNRESOLVED_FLIGHT_TIMESTAMP],
        "ambiguous_local_time": counts[AMBIGUOUS_LOCAL_TIME],
        "nonexistent_local_time": counts[NONEXISTENT_LOCAL_TIME],
        "by_source": by_source,
    }


def read_candidate_rows(engine=None):
    if engine is None:
        from database import dw_engine

        engine = dw_engine

    with engine.connect() as conn:
        conn.execute(text("SET SESSION TRANSACTION READ ONLY"))
        try:
            result = conn.execute(
                text(_CANDIDATE_QUERY),
                {
                    "source_dataset": ARCHIVE_SOURCE_DATASET,
                    "source_model": ARCHIVE_UTC_SOURCE_MODEL,
                },
            )
            return [dict(row) for row in result.mappings()]
        finally:
            try:
                conn.execute(text("SET SESSION TRANSACTION READ WRITE"))
            except Exception:
                try:
                    conn.invalidate()
                except Exception:
                    pass


def preview_links(engine=None, source_semantics=None):
    return build_link_plan(
        read_candidate_rows(engine), source_semantics=source_semantics
    )

def pending_link_changes(plan):
    return [
        row
        for row in plan
        if row["status"] == MATCH
        and row["new_weather_key"] != row["old_weather_key"]
    ]


def apply_links(plan, engine=None, dry_run=True):
    """Apply only matched weather_key changes, guarded by the old key."""
    unsafe = [
        row
        for row in plan
        if row["status"] == MATCH
        and row.get("source_semantics") not in SUPPORTED_SEMANTICS
    ]
    if unsafe:
        raise RuntimeError(
            "refusing to apply matches without verified flight timestamp "
            f"semantics (flight_keys={[row['flight_key'] for row in unsafe]})"
        )

    changes = pending_link_changes(plan)
    if dry_run:
        return {
            "rows_pending": len(changes),
            "rows_updated": 0,
            "flight_keys": [row["flight_key"] for row in changes],
        }
    if not changes:
        return {"rows_pending": 0, "rows_updated": 0, "flight_keys": []}

    if engine is None:
        from database import dw_engine

        engine = dw_engine

    try:
        with engine.begin() as conn:
            for row in changes:
                result = conn.execute(
                    text(_UPDATE_QUERY),
                    {
                        "new_weather_key": row["new_weather_key"],
                        "flight_key": row["flight_key"],
                        "old_weather_key": row["old_weather_key"],
                    },
                )
                if result.rowcount != 1:
                    raise RuntimeError(
                        "weather_key changed concurrently for flight_key="
                        f"{row['flight_key']}; transaction rolled back"
                    )
    except Exception as error:
        raise RuntimeError(
            "weather link update failed (transaction rolled back): "
            f"{error}"
        ) from error

    return {
        "rows_pending": 0,
        "rows_updated": len(changes),
        "flight_keys": [row["flight_key"] for row in changes],
    }


def _print_plan(plan, summary):
    print(f"Timestamp semantics: {DEPARTURE_TIMEZONE_ASSUMPTION}")
    print(
        "Candidate identity: "
        f"{ARCHIVE_SOURCE_DATASET}/{ARCHIVE_UTC_SOURCE_MODEL} (UTC)"
    )
    print(
        f"{'STATUS':<26} {'SRC':<4} {'FLIGHT':>6} {'ORIGIN':<6} "
        f"{'LOCAL HOUR':<19} {'OLD':>8} {'NEW':>8}  DETAIL"
    )
    for row in plan:
        target = (
            row["target_hour"].strftime("%Y-%m-%d %H:%M")
            if row["target_hour"] is not None
            else "-"
        )
        if row.get("target_utc_hour") is not None:
            target = f"{target} UTC={row['target_utc_hour']:%Y-%m-%d %H:%M}"
        old_key = "-" if row["old_weather_key"] is None else row["old_weather_key"]
        new_key = "-" if row["new_weather_key"] is None else row["new_weather_key"]
        print(
            f"{row['status']:<26} {str(row['source_system'] or '-'):<4} "
            f"{row['flight_key']:>6} {str(row['airport_code'] or '-'):<6} "
            f"{target:<19} {old_key:>8} {new_key:>8}  {row['reason']}"
        )

    print(
        f"Summary: total={summary['total']} matched={summary['matched']} "
        f"unmatched={summary['unmatched']} changed={summary['changed']} "
        f"unchanged={summary['unchanged']}"
    )
    print(
        f"         currently_linked={summary['currently_linked']} "
        f"replace={summary['replacing']} add={summary['adding']} "
        f"linked_but_held={summary['linked_but_held']}"
    )
    print(
        f"         no_exact_candidate={summary['no_exact_candidate']} "
        f"ambiguous={summary['ambiguous']} "
        f"unsupported_airport={summary['unsupported_airport']} "
        f"unsupported_timezone={summary['unsupported_timezone']} "
        f"invalid_departure={summary['invalid_departure']} "
        f"null_visibility={summary['null_visibility']}"
    )
    print(
        f"         unresolved_flight_timestamp="
        f"{summary['unresolved_flight_timestamp']} "
        f"ambiguous_local_time={summary['ambiguous_local_time']} "
        f"nonexistent_local_time={summary['nonexistent_local_time']}"
    )
    for source in sorted(summary["by_source"]):
        counts = summary["by_source"][source]
        details = ", ".join(
            f"{status}={counts[status]}"
            for status in _STATUSES
            if counts[status]
        )
        print(f"         {source}: {details}")


def _execute(target_engine, verifier):
    import etl_audit

    try:
        etl_audit.ensure_schema()
    except etl_audit.AuditError as error:
        print(f"[FATAL] audit layer unavailable: {error}")
        return 4

    lock = etl_audit.acquire_run_lock(timeout=0)
    if lock is None:
        print(
            f"[LOCKED] another refresh run holds {etl_audit.RUN_LOCK_NAME}; "
            "refusing to apply weather links"
        )
        return 3

    try:
        plan = preview_links(target_engine)
        summary = summarize_plan(plan)
        _print_plan(plan, summary)

        unsafe = [
            row
            for row in plan
            if row["status"] == MATCH
            and row.get("source_semantics") not in SUPPORTED_SEMANTICS
        ]
        if unsafe:
            print(
                "[FATAL] matched rows lack verified flight timestamp "
                "semantics; refusing to execute"
            )
            return 7

        info = etl_audit.start_run("weather")
        try:
            result = apply_links(plan, engine=target_engine, dry_run=False)
        except KeyboardInterrupt:
            try:
                etl_audit.finish_run(
                    info["run_id"],
                    "FAILED",
                    notes="standalone weather linker (CLI --execute)",
                    error="interrupted by operator; committed link updates "
                    "are not rolled back",
                )
            except etl_audit.AuditError:
                pass
            print("[INTERRUPTED] weather link update interrupted")
            return 130
        except Exception as error:
            try:
                etl_audit.finish_run(
                    info["run_id"],
                    "FAILED",
                    notes="standalone weather linker (CLI --execute)",
                    error=str(error),
                )
            except etl_audit.AuditError:
                pass
            print(f"[FAILED] {error}")
            return 1

        if verifier is None:
            import verify_warehouse

            verifier = verify_warehouse.run_checks
        try:
            with target_engine.connect() as conn:
                checks = verifier(conn)
        except Exception as error:
            try:
                etl_audit.finish_run(
                    info["run_id"],
                    "FAILED",
                    rows_updated=result["rows_updated"],
                    notes="standalone weather linker (CLI --execute)",
                    error="links committed; post-update verification could "
                    f"not complete: {error}; links are NOT rolled back",
                )
            except etl_audit.AuditError:
                pass
            print(
                "[FAILED] links committed; verification could not complete: "
                f"{error} (links are NOT rolled back)"
            )
            return 6

        failing = [check for check in checks if check.status == "FAIL"]
        warns = [check for check in checks if check.status == "WARN"]
        if failing:
            names = ", ".join(check.name for check in failing)
            try:
                etl_audit.finish_run(
                    info["run_id"],
                    "FAILED",
                    rows_updated=result["rows_updated"],
                    notes="standalone weather linker (CLI --execute)",
                    error="links committed; post-update verification FAILED: "
                    f"{names}; links are NOT rolled back",
                )
            except etl_audit.AuditError:
                pass
            print(
                f"[FAILED] verification did not pass ({names}); committed "
                "links are NOT rolled back"
            )
            return 6

        try:
            etl_audit.finish_run(
                info["run_id"],
                "SUCCESS",
                rows_updated=result["rows_updated"],
                notes=(
                    "standalone weather linker (CLI --execute); "
                    f"post-update verification passed (WARN={len(warns)})"
                ),
            )
        except etl_audit.AuditError as error:
            print(
                "[FAILED] links applied and verification passed, but the "
                f"audit row could not be closed: {error}"
            )
            return 5

        print(
            f" -> applied={result['rows_updated']} "
            f"(target database: {target_engine.url.database})"
        )
        print(
            f" -> verification passed (FAIL=0 WARN={len(warns)}); "
            "audit row closed as SUCCESS"
        )
        return 0
    finally:
        lock.release()


def main(argv=None, target_engine=None, verifier=None):
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Propose exact UTC-instant weather links for explicitly "
            "declared flight timestamps. Default is a read-only dry run; "
            "--execute applies only matched fact_flight.weather_key changes."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report the proposal without writing (default behaviour)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="apply only matched weather_key changes with lock and audit",
    )
    parser.add_argument(
        "--proposed-standards",
        "--project-standards",
        dest="proposed_standards",
        action="store_true",
        help=(
            "simulate linking under the adopted project standards (csv=origin_local, "
            "oltp=utc). For dry-run simulation only; cannot be combined with --execute."
        ),
    )
    args = parser.parse_args(argv)

    if args.execute and args.dry_run:
        print("[FATAL] choose either --dry-run or --execute, not both")
        return 2
    if args.execute and args.proposed_standards:
        print(
            "[FATAL] project standards linking cannot be executed against production "
            "without explicit operator authorization; use --dry-run only."
        )
        return 2


    if args.execute:
        if target_engine is None:
            from database import dw_engine

            target_engine = dw_engine
        return _execute(target_engine, verifier)
    from timestamp_semantics import RECOMMENDED_SEMANTICS

    source_semantics = (
        RECOMMENDED_SEMANTICS if args.proposed_standards else None
    )
    plan = preview_links(target_engine, source_semantics=source_semantics)
    summary = summarize_plan(plan)
    _print_plan(plan, summary)
    print(
        f"DRY RUN only: pending={summary['changed']} "
        f"target={target_engine.url.database if target_engine else 'configured'}"
    )
    print("No database writes were performed.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
