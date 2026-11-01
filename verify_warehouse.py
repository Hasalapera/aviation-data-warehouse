"""Read-only post-load verification for the Aviation Data Warehouse.

Performs NON-DESTRUCTIVE checks against the warehouse. It never repairs,
updates, deletes or reloads anything. Rules are derived from sql/schema.sql
and the documented ETL behaviour, not guessed:

  * FAIL  = a violation of a schema constraint, an invariant the ETLs are
            designed to guarantee, or unapproved schema drift.
  * WARN  = suspicious but explainable state (e.g. legacy NULL identities).
  * SKIP  = a check that cannot run in this environment (missing baseline,
            missing audit table, ...). Reported explicitly, never hidden.

Usage:
    python verify_warehouse.py                       # text report, exit 0/1
    python verify_warehouse.py --write-baseline F \
        --allow-baseline-write                       # snapshot schema contract
                                                     # (explicit confirmation
                                                     # required)
    python verify_warehouse.py --strict              # warnings also fail

Exit codes: 0 = no failures (warnings allowed unless --strict), 1 = failures.
"""

import argparse
import json
import os
import sys
from datetime import datetime

from sqlalchemy import text

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SCHEMA_BASELINE = os.path.join(BASE_DIR, "config", "schema_baseline.json")

BUSINESS_TABLES = [
    "dim_date",
    "dim_airline",
    "dim_airport",
    "dim_route",
    "dim_weather",
    "fact_flight",
]

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"


class Check:
    def __init__(self, name, status, detail):
        self.name = name
        self.status = status
        self.detail = detail

    def as_dict(self):
        return {"name": self.name, "status": self.status, "detail": self.detail}


# =========================================================
# SCHEMA INVENTORY (information_schema only)
# =========================================================

def _schema_inventory(conn):
    """Structured definition of the six business tables."""
    inventory = {}
    for table in BUSINESS_TABLES:
        columns = conn.execute(
            text(
                """
                SELECT column_name, column_type, is_nullable, column_default,
                       extra, column_key
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = :t
                ORDER BY ordinal_position
                """
            ),
            {"t": table},
        ).fetchall()
        if not columns:
            inventory[table] = None
            continue
        indexes = conn.execute(
            text(
                """
                SELECT index_name, seq_in_index, column_name, non_unique
                FROM information_schema.statistics
                WHERE table_schema = DATABASE() AND table_name = :t
                ORDER BY index_name, seq_in_index
                """
            ),
            {"t": table},
        ).fetchall()
        fks = conn.execute(
            text(
                """
                SELECT constraint_name, column_name,
                       referenced_table_name, referenced_column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = DATABASE()
                  AND table_name = :t
                  AND referenced_table_name IS NOT NULL
                ORDER BY constraint_name, ordinal_position
                """
            ),
            {"t": table},
        ).fetchall()
        inventory[table] = {
            "columns": [
                {
                    "name": r[0],
                    "type": r[1],
                    "nullable": r[2],
                    "default": str(r[3]) if r[3] is not None else None,
                    "extra": r[4],
                    "key": r[5],
                }
                for r in columns
            ],
            "indexes": [
                {"name": r[0], "seq": int(r[1]), "column": r[2], "unique": r[3] == 0}
                for r in indexes
            ],
            "foreign_keys": [
                {
                    "name": r[0],
                    "column": r[1],
                    "ref_table": r[2],
                    "ref_column": r[3],
                }
                for r in fks
            ],
        }
    return inventory


def write_schema_baseline(conn, path):
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database": conn.execute(text("SELECT DATABASE()")).scalar(),
        "tables": BUSINESS_TABLES,
        "inventory": _schema_inventory(conn),
    }
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    return path


def _scalar(conn, sql, params=None):
    return conn.execute(text(sql), params or {}).scalar()


# =========================================================
# CHECKS
# =========================================================

def check_tables_present(conn):
    missing = []
    for table in BUSINESS_TABLES:
        count = _scalar(
            conn,
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = :t",
            {"t": table},
        )
        if count == 0:
            missing.append(table)
    if missing:
        return Check("tables_present", FAIL, f"missing tables: {missing}")
    return Check("tables_present", PASS, f"all {len(BUSINESS_TABLES)} tables present")


# The live warehouse was created through a legacy path (auto-named
# constraints, migrated column order) while sql/schema.sql builds an
# equivalent schema with explicit names. The baseline check therefore
# compares SEMANTIC structure: column definitions (as a name-keyed set),
# index shapes (columns + uniqueness, names ignored) and foreign-key
# targets (constraint names ignored).
# These two indexes exist in sql/schema.sql but never existed on the
# Phase-0 live warehouse; they are tolerated as extras, documented in
# docs/BASELINE_MANIFEST.md. Any other difference is drift.
KNOWN_EXTRA_INDEXES = {
    "dim_airline": {(("airline_code",), True)},
    "fact_flight": {(("flight_number",), False)},
}


def _norm_columns(columns):
    return sorted(
        (
            {k: c[k] for k in ("name", "type", "nullable", "default", "extra")}
            for c in columns
        ),
        key=lambda c: c["name"],
    )


def _norm_indexes(indexes):
    """Index shapes: (ordered column tuple, unique) - names are cosmetic."""
    grouped = {}
    for idx in indexes:
        grouped.setdefault((idx["name"], bool(idx["unique"])), {})[
            int(idx["seq"])
        ] = idx["column"]
    shapes = set()
    for (_name, unique), seqmap in grouped.items():
        shapes.add((tuple(seqmap[k] for k in sorted(seqmap)), unique))
    return shapes


def _norm_fks(fks):
    """FK targets: (column, ref_table, ref_column) - names are cosmetic."""
    return {(fk["column"], fk["ref_table"], fk["ref_column"]) for fk in fks}


def check_schema_baseline(conn, baseline_path=DEFAULT_SCHEMA_BASELINE):
    if not os.path.exists(baseline_path):
        return Check(
            "schema_baseline",
            SKIP,
            f"no baseline recorded at {os.path.relpath(baseline_path, BASE_DIR)}",
        )
    with open(baseline_path, "r", encoding="utf-8") as handle:
        baseline = json.load(handle)
    current = _schema_inventory(conn)
    drifts = []
    for table in BUSINESS_TABLES:
        base = baseline.get("inventory", {}).get(table)
        now = current.get(table)
        if base is None and now is None:
            drifts.append(f"{table}: missing in both baseline and current")
            continue
        if base is None or now is None:
            drifts.append(f"{table}: presence differs (baseline={base is not None}, now={now is not None})")
            continue
        if _norm_columns(now["columns"]) != _norm_columns(base["columns"]):
            base_names = {c["name"]: c for c in base["columns"]}
            now_names = {c["name"]: c for c in now["columns"]}
            changed = sorted(
                set(base_names) ^ set(now_names)
                | {
                    n
                    for n in set(base_names) & set(now_names)
                    if any(
                        base_names[n][k] != now_names[n][k]
                        for k in ("type", "nullable", "default", "extra")
                    )
                }
            )
            drifts.append(f"{table}: column definition changed: {changed}")
        base_idx = _norm_indexes(base["indexes"])
        now_idx = _norm_indexes(now["indexes"])
        missing_idx = sorted(base_idx - now_idx)
        extra_idx = sorted(now_idx - base_idx - KNOWN_EXTRA_INDEXES.get(table, set()))
        if missing_idx:
            drifts.append(f"{table}: indexes missing vs baseline: {missing_idx}")
        if extra_idx:
            drifts.append(f"{table}: unapproved new indexes: {extra_idx}")
        if _norm_fks(now["foreign_keys"]) != _norm_fks(base["foreign_keys"]):
            drifts.append(f"{table}: foreign key targets changed")
    if drifts:
        return Check("schema_baseline", FAIL, "; ".join(drifts))
    return Check(
        "schema_baseline",
        PASS,
        f"6 business tables match baseline of {baseline.get('generated_at')} "
        "(semantic compare: constraint names and column order ignored; "
        "2 documented index deviations allowed)",
    )


def check_fact_source_identity_unique(conn):
    dupes = _scalar(
        conn,
        """
        SELECT COUNT(*) FROM (
            SELECT source_system, source_flight_id
            FROM fact_flight
            WHERE source_flight_id IS NOT NULL
            GROUP BY source_system, source_flight_id
            HAVING COUNT(*) > 1
        ) d
        """,
    )
    if dupes:
        return Check(
            "fact_source_identity_unique",
            FAIL,
            f"{dupes} duplicated (source_system, source_flight_id) groups",
        )
    return Check("fact_source_identity_unique", PASS, "0 duplicated source identities")


def check_fact_identity_nulls(conn):
    nulls = _scalar(
        conn, "SELECT COUNT(*) FROM fact_flight WHERE source_flight_id IS NULL"
    )
    total = _scalar(conn, "SELECT COUNT(*) FROM fact_flight")
    if nulls:
        # schema permits NULL (legacy rows); current ETL always writes identity
        return Check(
            "fact_identity_nulls",
            WARN,
            f"{nulls}/{total} rows have NULL source_flight_id (schema allows; "
            "current ETLs always populate it)",
        )
    return Check("fact_identity_nulls", PASS, f"0/{total} NULL source identities")


def check_fact_required_keys(conn):
    # sql/schema.sql: date_key, airline_key, route_key are NOT NULL
    bad = _scalar(
        conn,
        """
        SELECT COUNT(*) FROM fact_flight
        WHERE date_key IS NULL OR airline_key IS NULL OR route_key IS NULL
        """,
    )
    if bad:
        return Check("fact_required_keys", FAIL, f"{bad} rows with NULL required key")
    return Check("fact_required_keys", PASS, "date_key/airline_key/route_key all non-NULL")


def check_fact_foreign_keys(conn):
    orphans = _scalar(
        conn,
        """
        SELECT COUNT(*) FROM fact_flight f
        LEFT JOIN dim_date d    ON f.date_key    = d.date_key
        LEFT JOIN dim_airline a ON f.airline_key = a.airline_key
        LEFT JOIN dim_route r   ON f.route_key   = r.route_key
        LEFT JOIN dim_weather w ON f.weather_key = w.weather_key
        WHERE d.date_key IS NULL
           OR a.airline_key IS NULL
           OR r.route_key IS NULL
           OR (f.weather_key IS NOT NULL AND w.weather_key IS NULL)
        """,
    )
    if orphans:
        return Check("fact_foreign_keys", FAIL, f"{orphans} fact rows with dangling FK")
    return Check("fact_foreign_keys", PASS, "all fact FK references resolve")


def check_dimension_uniqueness(conn):
    problems = []
    checks = [
        ("dim_airline.airline_code", "SELECT airline_code c FROM dim_airline GROUP BY airline_code HAVING COUNT(*)>1"),
        ("dim_airport.airport_code", "SELECT airport_code c FROM dim_airport GROUP BY airport_code HAVING COUNT(*)>1"),
        (
            "dim_route (origin,destination)",
            "SELECT origin_airport_key c FROM dim_route "
            "GROUP BY origin_airport_key, destination_airport_key HAVING COUNT(*)>1",
        ),
        ("dim_date.full_date", "SELECT full_date c FROM dim_date GROUP BY full_date HAVING COUNT(*)>1"),
        (
            "dim_weather identity",
            "SELECT airport_key c FROM dim_weather "
            "WHERE airport_key IS NOT NULL AND observation_time IS NOT NULL "
            "  AND source_dataset IS NOT NULL AND source_model IS NOT NULL "
            "GROUP BY airport_key, observation_time, source_dataset, source_model "
            "HAVING COUNT(*)>1",
        ),
    ]
    for label, sql in checks:
        n = _scalar(conn, f"SELECT COUNT(*) FROM ({sql}) x")
        if n:
            problems.append(f"{label}: {n} duplicated groups")
    if problems:
        return Check("dimension_uniqueness", FAIL, "; ".join(problems))
    return Check("dimension_uniqueness", PASS, "all dimension business keys unique")


def check_dim_weather_legacy_identity(conn):
    legacy = _scalar(
        conn,
        "SELECT COUNT(*) FROM dim_weather WHERE airport_key IS NULL",
    )
    total = _scalar(conn, "SELECT COUNT(*) FROM dim_weather")
    if total == 0:
        return Check("dim_weather_identity", WARN, "dim_weather is empty")
    if legacy:
        return Check(
            "dim_weather_identity",
            WARN,
            f"{legacy}/{total} legacy rows without natural identity "
            "(CSV-derived, permitted by design); duplicates checked separately",
        )
    return Check("dim_weather_identity", PASS, "all rows carry natural identity")


def check_weather_key_policy(conn):
    oltp_linked = _scalar(
        conn,
        "SELECT COUNT(*) FROM fact_flight WHERE source_system='oltp' "
        "AND weather_key IS NOT NULL",
    )
    csv_unlinked = _scalar(
        conn,
        "SELECT COUNT(*) FROM fact_flight WHERE source_system='csv' "
        "AND weather_key IS NULL",
    )
    parts = []
    status = PASS
    if oltp_linked:
        status = WARN
        parts.append(f"{oltp_linked} OLTP rows have weather_key (design expects NULL)")
    if csv_unlinked:
        status = WARN
        parts.append(
            f"{csv_unlinked} CSV rows lack weather_key (weather-key linking is not "
            "part of the current weather_etl)"
        )
    if not parts:
        parts.append("weather_key linkage matches documented policy")
    return Check("weather_key_policy", status, "; ".join(parts))


def check_row_counts(conn, audit_available):
    fact = _scalar(conn, "SELECT COUNT(*) FROM fact_flight")
    weather = _scalar(conn, "SELECT COUNT(*) FROM dim_weather")
    checks = []
    if fact == 0:
        checks.append(Check("row_counts", WARN, "fact_flight is empty"))
    else:
        checks.append(
            Check("row_counts", PASS, f"fact_flight={fact}, dim_weather={weather}")
        )

    # Anomaly: an insert-only/upsert refresh must never decrease row counts.
    # NOTE (scope): within a refresh run, rows_after was written by the same
    # run before verification, so this comparison is tautological there; it
    # is meaningful for STANDALONE verification between runs (previous
    # audited run vs current table state).
    if audit_available:
        for source, table in (("csv", "fact_flight"), ("oltp", "fact_flight"), ("weather", "dim_weather")):
            prev = _scalar(
                conn,
                """
                SELECT rows_after FROM etl_batch_log
                WHERE source_name = :s AND status IN ('SUCCESS','PARTIAL')
                  AND rows_after IS NOT NULL
                ORDER BY run_id DESC LIMIT 1
                """,
                {"s": source},
            )
            if prev is None:
                continue
            current = (
                _scalar(
                    conn,
                    "SELECT COUNT(*) FROM fact_flight WHERE source_system = :s",
                    {"s": source},
                )
                if table == "fact_flight"
                else _scalar(conn, "SELECT COUNT(*) FROM dim_weather")
            )
            if current < int(prev):
                checks.append(
                    Check(
                        f"row_count_not_decreased[{source}]",
                        FAIL,
                        f"{table} dropped below last audited value "
                        f"({prev} -> {current})",
                    )
                )
            else:
                checks.append(
                    Check(
                        f"row_count_not_decreased[{source}]",
                        PASS,
                        f"{table} {prev} -> {current} (non-decreasing)",
                    )
                )
    else:
        checks.append(
            Check("row_count_not_decreased", SKIP, "etl_batch_log not available")
        )
    return checks


def check_audit_health(conn, stale_minutes=240):
    exists = _scalar(
        conn,
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = 'etl_batch_log'",
    )
    if not exists:
        return [Check("audit_table", SKIP, "etl_batch_log not created yet")]
    checks = [Check("audit_table", PASS, "etl_batch_log present")]

    stale = _scalar(
        conn,
        """
        SELECT COUNT(*) FROM etl_batch_log
        WHERE status = 'RUNNING'
          AND started_at < (NOW() - INTERVAL :m MINUTE)
        """,
        {"m": int(stale_minutes)},
    )
    if stale:
        checks.append(
            Check(
                "audit_no_stale_runs",
                WARN,
                f"{stale} RUNNING row(s) older than {stale_minutes} min "
                "(possible crashed run; inspect before refreshing)",
            )
        )
    else:
        checks.append(Check("audit_no_stale_runs", PASS, "no stale RUNNING rows"))

    failed = _scalar(
        conn,
        """
        SELECT COUNT(*) FROM (
            SELECT source_name FROM etl_batch_log
            WHERE status IN ('SUCCESS','FAILED','PARTIAL')
            GROUP BY source_name
            HAVING MAX(run_id) = MAX(CASE WHEN status IN ('FAILED','PARTIAL')
                                          THEN run_id END)
        ) x
        """,
    )
    if failed:
        checks.append(
            Check(
                "audit_latest_status",
                WARN,
                f"{failed} source(s) whose most recent terminal run FAILED/PARTIAL",
            )
        )
    else:
        checks.append(
            Check("audit_latest_status", PASS, "latest terminal runs not FAILED/PARTIAL")
        )
    return checks


def check_oracle_object_drift(conn):
    """No views/procedures/triggers/events may exist in the warehouse
    (baseline state: none). Additions would be unapproved objects."""
    views = _scalar(
        conn,
        "SELECT COUNT(*) FROM information_schema.views "
        "WHERE table_schema = DATABASE()",
    )
    routines = _scalar(
        conn,
        "SELECT COUNT(*) FROM information_schema.routines "
        "WHERE routine_schema = DATABASE()",
    )
    triggers = _scalar(
        conn,
        "SELECT COUNT(*) FROM information_schema.triggers "
        "WHERE trigger_schema = DATABASE()",
    )
    events = _scalar(
        conn,
        "SELECT COUNT(*) FROM information_schema.events "
        "WHERE event_schema = DATABASE()",
    )
    parts = {"views": views, "routines": routines, "triggers": triggers, "events": events}
    if any(parts.values()):
        return Check(
            "no_unexpected_db_objects",
            FAIL,
            "unexpected analytical objects found: "
            + ", ".join(f"{k}={v}" for k, v in parts.items() if v),
        )
    return Check("no_unexpected_db_objects", PASS, "0 views/routines/triggers/events")


# =========================================================
# RUNNER
# =========================================================

def run_checks(conn, baseline_path=DEFAULT_SCHEMA_BASELINE, stale_minutes=240):
    checks = [check_tables_present(conn)]
    if checks[-1].status == FAIL:
        checks.append(Check("schema_baseline", SKIP, "tables missing; schema compare not run"))
        return checks
    checks.append(check_schema_baseline(conn, baseline_path))
    checks.append(check_oracle_object_drift(conn))
    checks.append(check_fact_source_identity_unique(conn))
    checks.append(check_fact_identity_nulls(conn))
    checks.append(check_fact_required_keys(conn))
    checks.append(check_fact_foreign_keys(conn))
    checks.append(check_dimension_uniqueness(conn))
    checks.append(check_dim_weather_legacy_identity(conn))
    checks.append(check_weather_key_policy(conn))
    audit_exists = _scalar(
        conn,
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = 'etl_batch_log'",
    )
    checks.extend(check_row_counts(conn, audit_available=bool(audit_exists)))
    checks.extend(check_audit_health(conn, stale_minutes=stale_minutes))
    return checks


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-baseline", metavar="FILE",
                        help="snapshot current business-table schema and exit "
                             "(requires --allow-baseline-write)")
    parser.add_argument("--allow-baseline-write", action="store_true",
                        help="explicit confirmation that the target database "
                             "and schema have been reviewed for drift")
    parser.add_argument("--baseline", default=DEFAULT_SCHEMA_BASELINE)
    parser.add_argument("--stale-minutes", type=int, default=240)
    parser.add_argument("--strict", action="store_true",
                        help="treat warnings as failures")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)

    from database import dw_engine

    with dw_engine.connect() as conn:
        if args.write_baseline:
            if not args.allow_baseline_write:
                database = conn.execute(text("SELECT DATABASE()")).scalar()
                print(
                    f"[REFUSED] --write-baseline would snapshot database "
                    f"'{database}' into {args.write_baseline}. Re-run with "
                    "--allow-baseline-write only after reviewing that "
                    "database for unapproved drift."
                )
                return 2
            path = write_schema_baseline(conn, args.write_baseline)
            print(f" -> Schema baseline written: {path}")
            return 0
        checks = run_checks(conn, args.baseline, args.stale_minutes)

    if args.json:
        print(json.dumps([c.as_dict() for c in checks], indent=2))
    else:
        print("\n" + "=" * 78)
        print(" WAREHOUSE VERIFICATION (read-only)")
        print("=" * 78)
        for c in checks:
            print(f"  [{c.status:4}] {c.name}: {c.detail}")
        counts = {s: sum(1 for c in checks if c.status == s) for s in (PASS, WARN, FAIL, SKIP)}
        print("-" * 78)
        print(
            f"  PASS={counts[PASS]}  WARN={counts[WARN]}  "
            f"FAIL={counts[FAIL]}  SKIP={counts[SKIP]}"
        )
        print("=" * 78 + "\n")

    failed = any(c.status == FAIL for c in checks)
    warned = any(c.status == WARN for c in checks)
    return 1 if failed or (args.strict and warned) else 0


if __name__ == "__main__":
    sys.exit(main())
