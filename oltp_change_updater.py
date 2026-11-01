"""Bounded, opt-in change updater for existing OLTP fact rows (P2).

Policy (docs/DATA_REFRESH_POLICY.md): the OLTP loader is insert-only, so
corrected values on flights that were ALREADY loaded never reach the
warehouse. This module closes exactly that gap - and nothing else:

* It re-derives only the MUTABLE attributes from the source using the
  same rules as oltp_etl.py's flight query:
      delay_minutes          COALESCE(flight_delays.max, dep-vs-schedule)
      flight_duration_minutes TIMESTAMPDIFF(actual_departure, actual_arrival)
      passenger_count        COUNT(CONFIRMED bookings)
      cancelled_flag         status='CANCELLED' OR a cancellation exists
      departure_time         flights.actual_departure
      arrival_time           flights.actual_arrival
* It UPDATEs existing rows matched by the stable identity
  (source_system='oltp', source_flight_id=flight_id) via the primary key.
* It never INSERTs, never DELETEs, and never touches identity/key columns
  (date_key, airline_key, route_key, scheduled_departure, flight_number,
  diverted_flag, distance_km) or arrival_delay_minutes (the loader
  hardcodes it to 0; "fixing" it here would diverge from the loader).
* The source database is READ-ONLY here (SELECT only).

Entry points: update_changed_facts() - called by refresh_orchestrator only
with --enable-changes (lock + audit + post-load verification); and the
standalone CLI (main()), which is a DRY RUN unless --execute is given and,
on --execute, runs the complete lifecycle: run lock -> audit row -> bounded
updates -> read-only post-update verification. SUCCESS (and exit 0) is
reported only when verification ran and reported no FAIL checks; a
verification failure exits 6, closes the audit row as FAILED, and states
explicitly that committed updates are NOT rolled back (fail-closed: never
a second unguarded write path). Correctness is enforced by the convergence
test in tests/test_watermark.py: after a fresh oltp_etl run, this updater
must update 0 rows (any divergence between the two derivations fails
there).
"""

import os

import pandas as pd
from sqlalchemy import text

from etl_audit import sanitize_error

# The ONLY fact columns this module may write, with their derivation
# source. Anything not in this dict is insert-only ownership of etl/oltp_etl.
MUTABLE_COLUMNS = (
    "delay_minutes",
    "flight_duration_minutes",
    "passenger_count",
    "cancelled_flag",
    "departure_time",
    "arrival_time",
)

# Read-only derivation query. The mutable-field formulas MUST match
# oltp_etl.py's flight_query (convergence-tested); joins are reduced to
# the ones those formulas need - no dimension joins, because identity
# mapping is owned by the loader.
_SOURCE_QUERY = """
    SELECT
        f.flight_id,
        COALESCE(
            fd.delay_minutes,
            CASE
                WHEN f.actual_departure IS NOT NULL
                 AND f.scheduled_departure IS NOT NULL
                THEN TIMESTAMPDIFF(
                    MINUTE, f.scheduled_departure, f.actual_departure
                )
                ELSE 0
            END
        ) AS delay_minutes,
        CASE
            WHEN f.actual_departure IS NOT NULL
             AND f.actual_arrival IS NOT NULL
            THEN TIMESTAMPDIFF(
                MINUTE, f.actual_departure, f.actual_arrival
            )
            ELSE 0
        END AS flight_duration_minutes,
        COALESCE(bc.passenger_count, 0) AS passenger_count,
        (f.status = 'CANCELLED' OR fc.cancellation_id IS NOT NULL)
            AS cancelled_flag,
        f.actual_departure AS departure_time,
        f.actual_arrival AS arrival_time
    FROM flights f
    LEFT JOIN (
        SELECT flight_id, MAX(delay_minutes) AS delay_minutes
        FROM flight_delays GROUP BY flight_id
    ) fd ON f.flight_id = fd.flight_id
    LEFT JOIN (
        SELECT flight_id, MAX(cancellation_id) AS cancellation_id
        FROM flight_cancellations GROUP BY flight_id
    ) fc ON f.flight_id = fc.flight_id
    LEFT JOIN (
        SELECT flight_id, COUNT(*) AS passenger_count
        FROM bookings WHERE booking_status = 'CONFIRMED'
        GROUP BY flight_id
    ) bc ON f.flight_id = bc.flight_id
"""


def _comparable(value):
    """Normalise SQL/pandas values for equality (bool/int/datetime/NULL)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str) and value.lower() == "nan":
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int,)) or type(value).__name__ == "int64":
        return int(value)
    if type(value).__name__ == "bool":
        return bool(value)
    return value


def _values_differ(source_row, target_row):
    """True when any mutable column actually changed (NULL-safe)."""
    for column in MUTABLE_COLUMNS:
        src = _comparable(source_row[column])
        dst = _comparable(target_row[column])
        if column in ("departure_time", "arrival_time"):
            # MySQL DATETIME has second precision; pandas may carry
            # NaT for NULLs - normalise both sides (None == NULL == NaT).
            src = (
                None
                if src is None or pd.isna(src)
                else pd.Timestamp(src).floor("s")
            )
            dst = (
                None
                if dst is None or pd.isna(dst)
                else pd.Timestamp(dst).floor("s")
            )
            if src != dst:
                return True
            continue
        if column == "cancelled_flag":
            src = None if src is None else bool(src)
            dst = None if dst is None else bool(dst)
        else:
            src = None if src is None else int(src)
            dst = None if dst is None else int(dst)
        if src != dst:
            return True
    return False


def update_changed_facts(source_engine=None, target_engine=None,
                         dry_run=False, batch_guid=None):
    """Apply bounded updates to EXISTING OLTP fact rows.

    Returns {
        "rows_considered": source flights examined,
        "rows_updated":    fact rows whose mutable columns changed,
        "rows_missing_in_dw": source flights not loaded yet (insert-only
                              loader owns them - reported, never inserted),
    }
    Raises RuntimeError (with a sanitised message) on failure; the caller
    decides how a failure is reported (the orchestrator marks the OLTP
    stage FAILED while noting that committed inserts remain valid)."""
    if source_engine is None or target_engine is None:
        from database import dw_engine, oltp_engine

        source_engine = source_engine or oltp_engine
        target_engine = target_engine or dw_engine

    try:
        with source_engine.connect() as conn:
            source_df = pd.read_sql(text(_SOURCE_QUERY), conn)
    except Exception as error:
        raise RuntimeError(
            "cannot read OLTP source for change update: "
            + sanitize_error(error)
        )

    with target_engine.connect() as conn:
        target_df = pd.read_sql(
            text(
                """
                SELECT flight_key, source_flight_id,
                       delay_minutes, flight_duration_minutes,
                       passenger_count, cancelled_flag,
                       departure_time, arrival_time
                FROM fact_flight
                WHERE source_system = 'oltp'
                """
            ),
            conn,
        )

    target_by_id = {
        str(row["source_flight_id"]): row
        for _, row in target_df.iterrows()
    }

    considered = 0
    missing = []
    changes = []  # (flight_key, {column: new_value})
    for _, src in source_df.iterrows():
        considered += 1
        flight_id = str(src["flight_id"])
        target = target_by_id.get(flight_id)
        if target is None:
            missing.append(flight_id)
            continue
        if _values_differ(src, target):
            new_values = {
                column: _comparable(src[column]) for column in MUTABLE_COLUMNS
            }
            changes.append((int(target["flight_key"]), new_values))

    if changes and not dry_run:
        try:
            with target_engine.begin() as conn:
                for flight_key, values in changes:
                    assignments = ", ".join(
                        f"{column} = :{column}" for column in MUTABLE_COLUMNS
                    )
                    conn.execute(
                        text(
                            f"UPDATE fact_flight SET {assignments} "
                            "WHERE flight_key = :flight_key"
                        ),
                        {**values, "flight_key": flight_key},
                    )
        except Exception as error:
            raise RuntimeError(
                "bounded change update failed (transaction rolled back): "
                + sanitize_error(error)
            )

    return {
        "rows_considered": int(considered),
        "rows_updated": 0 if dry_run else len(changes),
        "rows_pending": len(changes) if dry_run else 0,
        "rows_missing_in_dw": len(missing),
        "changed_flight_keys": [key for key, _ in changes][:20],
    }


def main(argv=None, source_engine=None, target_engine=None, verifier=None):
    """Standalone CLI. Fail-closed by design:

    * Default (no flags) = DRY RUN: reports pending changes, writes nothing.
    * Real updates require an explicit --execute flag and run the complete
      lifecycle: the same MySQL run lock as refresh_orchestrator (held for
      the whole run; unavailable -> exit 3, nothing applied), an audit row
      (source 'oltp', SUCCESS only after verification passes), bounded
      updates, then the same read-only post-update verification the
      orchestrator runs (verify_warehouse.run_checks).
    * Exit codes: 0 ok/dry-run, 1 update failed (transaction rolled back),
      3 lock held elsewhere, 4 audit layer unavailable, 6 verification
      did not pass (updates stay COMMITTED - they are not rolled back on
      verification failure), 130 interrupted.
    The recommended path remains `refresh_orchestrator.py --enable-changes`
    (full refresh + changes + verification in one sequence)."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Bounded OLTP change updater. Default is a dry run; "
                    "pass --execute to apply updates (see docs/ETL_OPERATIONS.md)."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="report changes without writing (default behaviour)")
    parser.add_argument("--execute", action="store_true",
                        help="apply updates to EXISTING OLTP fact rows in the "
                             "configured warehouse; takes the refresh run lock, "
                             "writes an audit row, and runs post-update "
                             "verification before reporting success")
    args = parser.parse_args(argv)

    target_name = os.environ.get("DW_NAME", "aviation_dw")

    if not args.execute:
        result = update_changed_facts(
            source_engine=source_engine, target_engine=target_engine,
            dry_run=True,
        )
        print(
            f" -> DRY RUN: considered={result['rows_considered']} "
            f"pending={result['rows_pending']} "
            f"not_loaded_yet={result['rows_missing_in_dw']}"
        )
        print(f" -> target database: {target_name} (no changes applied)")
        print(
            " -> re-run with --execute to apply (lock + audit + "
            "post-update verification), or use "
            "`refresh_orchestrator.py --enable-changes` (recommended)"
        )
        return 0

    import etl_audit

    try:
        etl_audit.ensure_schema()  # no DDL when already initialised
    except etl_audit.AuditError as error:
        print(f"[FATAL] audit layer unavailable: {error}")
        return 4

    lock = etl_audit.acquire_run_lock(timeout=0)
    if lock is None:
        print(
            f"[LOCKED] another refresh run holds {etl_audit.RUN_LOCK_NAME}; "
            "refusing to apply changes"
        )
        return 3

    try:
        info = etl_audit.start_run("oltp")
        try:
            result = update_changed_facts(
                source_engine=source_engine, target_engine=target_engine,
            )
        except KeyboardInterrupt:
            try:
                etl_audit.finish_run(
                    info["run_id"], "FAILED",
                    notes="standalone change-updater run (CLI --execute)",
                    error="interrupted by operator during updates "
                          "(any committed updates are not rolled back)",
                )
            except etl_audit.AuditError:
                pass
            print("[INTERRUPTED] updates interrupted by operator")
            return 130
        except Exception as error:
            try:
                etl_audit.finish_run(
                    info["run_id"], "FAILED",
                    notes="standalone change-updater run (CLI --execute)",
                    error=str(error),
                )
            except etl_audit.AuditError:
                pass
            print(f"[FAILED] {error}")
            return 1

        # Post-update verification is REQUIRED before SUCCESS is claimed
        # (orchestrator parity). Only FAIL checks block success - WARNs
        # are informational, matching the orchestrator's verdict rule.
        if verifier is None:
            import verify_warehouse

            verifier = verify_warehouse.run_checks
        target_engine_for_verify = target_engine
        if target_engine_for_verify is None:
            from database import dw_engine

            target_engine_for_verify = dw_engine
        try:
            with target_engine_for_verify.connect() as conn:
                checks = verifier(conn)
        except KeyboardInterrupt:
            try:
                etl_audit.finish_run(
                    info["run_id"], "FAILED",
                    rows_updated=result["rows_updated"],
                    notes="standalone change-updater run (CLI --execute)",
                    error="interrupted by operator during post-update "
                          "verification; committed updates are NOT rolled back",
                )
            except etl_audit.AuditError:
                pass
            print(
                "[INTERRUPTED] verification interrupted; committed updates "
                "are NOT rolled back - inspect before re-running"
            )
            return 130
        except Exception as error:
            try:
                etl_audit.finish_run(
                    info["run_id"], "FAILED",
                    rows_updated=result["rows_updated"],
                    notes="standalone change-updater run (CLI --execute)",
                    error="updates committed; post-update verification could "
                          "not complete: " + str(error)
                          + "; updates are NOT rolled back",
                )
            except etl_audit.AuditError:
                pass
            print(
                "[FAILED] updates committed; verification could not complete: "
                f"{error} (updates are NOT rolled back)"
            )
            return 6

        failing = [c for c in checks if c.status == "FAIL"]
        warns = [c for c in checks if c.status == "WARN"]
        for check in checks:
            if check.status != "PASS":
                print(f" -> verify [{check.status}] {check.name}: {check.detail}")

        if failing:
            names = ", ".join(c.name for c in failing)
            try:
                etl_audit.finish_run(
                    info["run_id"], "FAILED",
                    rows_updated=result["rows_updated"],
                    notes="standalone change-updater run (CLI --execute)",
                    error="updates committed; post-update verification FAILED: "
                          f"{names}; updates are NOT rolled back",
                )
            except etl_audit.AuditError:
                pass
            print(
                f"[FAILED] verification did not pass ({names}); "
                "committed updates are NOT rolled back - inspect the "
                "warehouse before re-running"
            )
            return 6

        try:
            etl_audit.finish_run(
                info["run_id"], "SUCCESS",
                rows_updated=result["rows_updated"],
                notes=(
                    "standalone change-updater run (CLI --execute); "
                    "post-update verification passed "
                    f"(WARN={len(warns)})"
                ),
            )
        except etl_audit.AuditError as error:
            # Lock/updates/verification succeeded but the audit close
            # failed - report failure (fail-closed; do not claim success).
            print(
                "[FAILED] updates applied and verification passed, but the "
                f"audit row could not be closed: {error}"
            )
            return 5
        print(
            f" -> applied: considered={result['rows_considered']} "
            f"updated={result['rows_updated']} "
            f"not_loaded_yet={result['rows_missing_in_dw']} "
            f"(target database: {target_name})"
        )
        print(
            f" -> verification passed (FAIL=0 WARN={len(warns)}); "
            "audit row closed as SUCCESS"
        )
        return 0
    finally:
        lock.release()


if __name__ == "__main__":
    import sys

    sys.exit(main())
