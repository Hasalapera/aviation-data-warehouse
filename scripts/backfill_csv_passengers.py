"""Bounded, transactional backfill of synthetic CSV passenger counts.

The CSV loader is insert-only, so adding passenger_count to data/flight.csv
does not update the 500 rows already present in fact_flight. This utility
closes exactly that gap and nothing else:

* It updates ONLY passenger_count on rows matched by
  (source_system='csv', source_flight_id).
* It never inserts, deletes, or changes any other column.
* It defaults to a dry run; --execute takes the shared ETL run lock, writes
  an audit row, applies one transaction, verifies before commit, and rolls
  back on any failed check.

Usage:
    venv/bin/python scripts/backfill_csv_passengers.py
    venv/bin/python scripts/backfill_csv_passengers.py --execute
"""

import argparse
import csv
import hashlib
import os
import re
import sys
from collections import Counter
from pathlib import Path

from sqlalchemy import text

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

CSV_PATH = BASE_DIR / "data" / "flight.csv"
EXPECTED_CSV_ROWS = 500

NON_PAX_COLUMNS = (
    "flight_key",
    "source_system",
    "source_flight_id",
    "date_key",
    "airline_key",
    "route_key",
    "weather_key",
    "flight_number",
    "scheduled_departure",
    "departure_time",
    "arrival_time",
    "delay_minutes",
    "arrival_delay_minutes",
    "flight_duration_minutes",
    "distance_km",
    "cancelled_flag",
    "diverted_flag",
)

CSV_REQUIRED_COLUMNS = (
    "flight_id",
    "passenger_count",
    "cancelled_flag",
    "diverted_flag",
)

OLTP_EXPECTED_PAX = {0: 6, 1: 1, 2: 1}


class StopCondition(RuntimeError):
    pass


class VerificationFailure(RuntimeError):
    pass


def _fail(message):
    raise StopCondition(message)


def _read_csv():
    if not CSV_PATH.exists():
        _fail(f"CSV not found: {CSV_PATH}")

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing = [
            column for column in CSV_REQUIRED_COLUMNS
            if column not in fieldnames
        ]
        if missing:
            _fail(f"CSV is missing required columns: {missing}")
        rows = list(reader)

    if len(rows) != EXPECTED_CSV_ROWS:
        _fail(
            f"expected {EXPECTED_CSV_ROWS} CSV rows, "
            f"found {len(rows)}"
        )

    values = {}
    cancelled = {}
    diverted = {}
    for index, row in enumerate(rows, start=2):
        flight_id = (row.get("flight_id") or "").strip()
        if not flight_id:
            _fail(f"empty flight_id at CSV line {index}")
        if flight_id in values:
            _fail(f"duplicate flight_id {flight_id!r} in CSV")

        passenger_raw = (row.get("passenger_count") or "").strip()
        if not re.fullmatch(r"\d+", passenger_raw):
            _fail(
                f"invalid passenger_count for flight_id {flight_id!r}: "
                f"{passenger_raw!r}"
            )
        passenger_count = int(passenger_raw)

        cancelled_flag = (row.get("cancelled_flag") or "").strip()
        diverted_flag = (row.get("diverted_flag") or "").strip()
        if cancelled_flag not in ("0", "1") or diverted_flag not in ("0", "1"):
            _fail(
                f"unexpected status flags for flight_id {flight_id!r}: "
                f"cancelled={cancelled_flag!r}, diverted={diverted_flag!r}"
            )
        if cancelled_flag == "1" and diverted_flag == "1":
            _fail(f"both cancelled and diverted for flight_id {flight_id!r}")

        if cancelled_flag == "1" and passenger_count != 0:
            _fail(
                f"cancelled flight_id {flight_id!r} has "
                f"passenger_count={passenger_count}"
            )
        if cancelled_flag == "0" and passenger_count <= 0:
            _fail(
                f"operated flight_id {flight_id!r} has no passengers"
            )

        values[flight_id] = passenger_count
        cancelled[flight_id] = cancelled_flag
        diverted[flight_id] = diverted_flag

    return values, cancelled, diverted


def _target_rows(conn, lock=False):
    sql = """
        SELECT flight_key, source_flight_id, passenger_count
        FROM fact_flight
        WHERE source_system = 'csv'
        ORDER BY flight_key
    """
    if lock:
        sql += " FOR UPDATE"
    return conn.execute(text(sql)).fetchall()


def _validate_target(target, csv_values):
    by_id = {}
    for flight_key, source_flight_id, passenger_count in target:
        flight_id = str(source_flight_id)
        if flight_id in by_id:
            _fail(f"duplicate warehouse CSV source_flight_id {flight_id!r}")
        by_id[flight_id] = (int(flight_key), passenger_count)

    if len(by_id) != EXPECTED_CSV_ROWS:
        _fail(
            f"expected {EXPECTED_CSV_ROWS} CSV warehouse rows, "
            f"found {len(by_id)}"
        )

    csv_ids = set(csv_values)
    warehouse_ids = set(by_id)
    missing = sorted(csv_ids - warehouse_ids)
    unexpected = sorted(warehouse_ids - csv_ids)
    if missing:
        _fail(f"warehouse is missing CSV flight_ids: {missing[:10]}")
    if unexpected:
        _fail(f"warehouse has unexpected CSV flight_ids: {unexpected[:10]}")

    return by_id


def _pending_updates(by_id, csv_values):
    updates = []
    already_correct = 0
    for flight_id, csv_value in csv_values.items():
        flight_key, warehouse_value = by_id[flight_id]
        if warehouse_value is None or int(warehouse_value) != csv_value:
            updates.append({
                "flight_key": flight_key,
                "source_flight_id": flight_id,
                "passenger_count": csv_value,
            })
        else:
            already_correct += 1
    return updates, already_correct


def _non_pax_fingerprint(conn):
    columns = ", ".join(NON_PAX_COLUMNS)
    rows = conn.execute(
        text(f"SELECT {columns} FROM fact_flight ORDER BY flight_key")
    ).fetchall()
    payload = "\n".join(
        "|".join("" if value is None else repr(value) for value in row)
        for row in rows
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest(), len(rows)


def _validate_after_update(conn, csv_values, cancelled, before_fingerprint):
    total = conn.execute(
        text("SELECT COUNT(*) FROM fact_flight")
    ).scalar()
    if int(total) != 508:
        _fail(f"fact_flight row count changed: {total}")

    target = _target_rows(conn)
    by_id = _validate_target(target, csv_values)

    pending, _ = _pending_updates(by_id, csv_values)
    if pending:
        _fail(f"{len(pending)} CSV rows still have the wrong passenger_count")

    pax = [int(value) for _, value in by_id.values()]
    if min(pax) < 0:
        _fail("negative passenger_count found")
    if sum(1 for value in pax if value == 0) != 9:
        _fail("unexpected number of zero-passenger CSV rows")

    for flight_id, value in csv_values.items():
        _, warehouse_value = by_id[flight_id]
        if int(warehouse_value) != value:
            _fail(f"passenger_count mismatch for flight_id {flight_id!r}")
        if cancelled[flight_id] == "1" and int(warehouse_value) != 0:
            _fail(f"cancelled flight_id {flight_id!r} is not zero")

    cvs_null = conn.execute(
        text(
            "SELECT COUNT(*) FROM fact_flight "
            "WHERE source_system='csv' AND passenger_count IS NULL"
        )
    ).scalar()
    if int(cvs_null) != 0:
        _fail(f"{cvs_null} CSV rows still have NULL passenger_count")

    oltp_rows = conn.execute(
        text("SELECT COUNT(*) FROM fact_flight WHERE source_system='oltp'")
    ).scalar()
    if int(oltp_rows) != 8:
        _fail(f"OLTP row count changed: {oltp_rows}")

    oltp_dist = conn.execute(
        text(
            "SELECT passenger_count, COUNT(*) FROM fact_flight "
            "WHERE source_system='oltp' "
            "GROUP BY passenger_count ORDER BY passenger_count"
        )
    ).fetchall()
    actual_dist = {
        int(value): int(count) for value, count in oltp_dist
    }
    if actual_dist != OLTP_EXPECTED_PAX:
        _fail(f"OLTP passenger distribution changed: {actual_dist}")

    after_fingerprint, after_rows = _non_pax_fingerprint(conn)
    if after_rows != before_fingerprint[1]:
        _fail("non-passenger row count changed during backfill")
    if after_fingerprint != before_fingerprint[0]:
        _fail("non-passenger warehouse fields changed during backfill")

    import verify_warehouse

    checks = verify_warehouse.run_checks(conn)
    failures = [
        check for check in checks if check.status == verify_warehouse.FAIL
    ]
    if failures:
        details = "; ".join(
            f"{check.name}: {check.detail}" for check in failures
        )
        raise VerificationFailure(
            f"warehouse verification failed: {details}"
        )

    return {
        "rows_considered": len(csv_values),
        "rows_updated": 0,
        "rows_skipped": len(csv_values),
        "passenger_total": sum(pax),
        "passenger_min": min(pax),
        "passenger_max": max(pax),
        "distinct_values": len(set(pax)),
    }


def _print_summary(csv_values, cancelled, diverted, by_id, pending):
    distribution = Counter(csv_values.values())
    print(f" -> CSV rows: {len(csv_values)}")
    print(f" -> Warehouse CSV rows: {len(by_id)}")
    print(f" -> Pending updates: {len(pending)}")
    print(f" -> Already correct: {len(csv_values) - len(pending)}")
    print(f" -> Cancelled rows: {sum(1 for v in cancelled.values() if v == '1')}")
    print(f" -> Diverted rows: {sum(1 for v in diverted.values() if v == '1')}")
    print(f" -> Passenger total: {sum(csv_values.values())}")
    print(f" -> Passenger min/max: {min(csv_values.values())}/"
          f"{max(csv_values.values())}")
    print(f" -> Distinct passenger values: {len(distribution)}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="apply updates (default is dry run)",
    )
    args = parser.parse_args(argv)

    from database import dw_engine

    csv_values, cancelled, diverted = _read_csv()

    with dw_engine.connect() as conn:
        target = _target_rows(conn)
        by_id = _validate_target(target, csv_values)
        pending, already_correct = _pending_updates(by_id, csv_values)
        _print_summary(csv_values, cancelled, diverted, by_id, pending)

        if not args.execute:
            print("\n -> DRY RUN: no updates applied")
            print(" -> re-run with --execute to apply the backfill")
            return 0

    import etl_audit

    try:
        etl_audit.ensure_schema()
    except etl_audit.AuditError as error:
        print(f"[FATAL] audit layer unavailable: {error}")
        return 4

    lock = etl_audit.acquire_run_lock(timeout=0)
    if lock is None:
        print(
            f"[LOCKED] another refresh run holds "
            f"{etl_audit.RUN_LOCK_NAME}; refusing to backfill"
        )
        return 3

    run_info = None
    try:
        try:
            run_info = etl_audit.start_run("csv")
        except etl_audit.AuditError as error:
            print(f"[FATAL] cannot record audit row: {error}")
            return 4

        try:
            with dw_engine.begin() as conn:
                target = _target_rows(conn, lock=True)
                by_id = _validate_target(target, csv_values)
                pending, already_correct = _pending_updates(
                    by_id, csv_values
                )
                before_fingerprint = _non_pax_fingerprint(conn)

                if pending:
                    conn.execute(
                        text(
                            "UPDATE fact_flight "
                            "SET passenger_count = :passenger_count "
                            "WHERE flight_key = :flight_key "
                            "AND source_system = 'csv' "
                            "AND source_flight_id = :source_flight_id"
                        ),
                        pending,
                    )

                result = _validate_after_update(
                    conn,
                    csv_values,
                    cancelled,
                    before_fingerprint,
                )
                result["rows_updated"] = len(pending)
                result["rows_skipped"] = already_correct

        except VerificationFailure as error:
            try:
                etl_audit.finish_run(
                    run_info["run_id"],
                    "FAILED",
                    rows_read=len(csv_values),
                    rows_before=len(by_id),
                    rows_after=len(by_id),
                    rows_inserted=0,
                    rows_updated=0,
                    rows_skipped=already_correct,
                    notes="synthetic CSV passenger backfill",
                    error=str(error),
                )
            except etl_audit.AuditError:
                pass
            print(f"[FAILED] {error}")
            return 6
        except (StopCondition, Exception) as error:
            try:
                etl_audit.finish_run(
                    run_info["run_id"],
                    "FAILED",
                    rows_read=len(csv_values),
                    rows_before=len(by_id) if "by_id" in locals() else None,
                    rows_after=len(by_id) if "by_id" in locals() else None,
                    rows_inserted=0,
                    rows_updated=0,
                    rows_skipped=already_correct if "already_correct" in locals() else None,
                    notes="synthetic CSV passenger backfill",
                    error=str(error),
                )
            except etl_audit.AuditError:
                pass
            print(f"[FAILED] {error}")
            return 1

        try:
            etl_audit.finish_run(
                run_info["run_id"],
                "SUCCESS",
                rows_read=len(csv_values),
                rows_before=len(csv_values),
                rows_after=len(csv_values),
                rows_inserted=0,
                rows_updated=result["rows_updated"],
                rows_skipped=result["rows_skipped"],
                notes=(
                    "one-time synthetic enplaned passenger backfill; "
                    "CSV remains insert-only for normal refresh"
                ),
            )
        except etl_audit.AuditError as error:
            print(f"[FAILED] data committed but audit close failed: {error}")
            return 5

        print(
            f" -> UPDATED: {result['rows_updated']} rows; "
            f"already correct: {result['rows_skipped']}"
        )
        print(
            f" -> passenger total/min/max/distinct: "
            f"{result['passenger_total']}/"
            f"{result['passenger_min']}/"
            f"{result['passenger_max']}/"
            f"{result['distinct_values']}"
        )
        print(" -> verification passed; transaction committed")
        return 0

    except KeyboardInterrupt:
        if run_info is not None:
            try:
                etl_audit.finish_run(
                    run_info["run_id"],
                    "FAILED",
                    notes="synthetic CSV passenger backfill",
                    error="interrupted by operator; transaction rolled back",
                )
            except etl_audit.AuditError:
                pass
        print("[INTERRUPTED] transaction rolled back")
        return 130
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
