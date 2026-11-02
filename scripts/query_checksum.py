"""Compute reproducible checksums of the 25 analytical queries and their results.

Used to prove that the Data Refresh Policy implementation did not change any
existing analytical query text or (on unchanged data) any query result.

Usage:
    python scripts/query_checksum.py --out config/query_baseline.json
    python scripts/query_checksum.py --compare config/query_baseline.json

Modes:
    --out FILE      Run every QUERY_* constant in members/*.py against the
                    warehouse and write {sql_sha256, rows, result_sha256}.
    --compare FILE  Re-run and compare against a recorded baseline; exit 1
                    on any mismatch (SQL text or result).

Notes:
    - Query text hashes are always comparable across any environment.
    - Result hashes are only comparable when the underlying warehouse data is
      unchanged; a result mismatch with matching SQL hashes therefore means
      the data changed, not the queries.
    - No credentials are printed; the script only reads via the existing
      database.py connection helper.
"""

import argparse
import glob
import hashlib
import importlib.util
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MEMBERS_DIR = os.path.join(BASE_DIR, "members")


def _load_module(path):
    name = "protected_" + os.path.splitext(os.path.basename(path))[0]
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def collect_queries():
    """Return {query_id: {"file", "sql"}} for every QUERY_* constant."""
    queries = {}
    for path in sorted(glob.glob(os.path.join(MEMBERS_DIR, "*.py"))):
        module = _load_module(path)
        rel = os.path.relpath(path, BASE_DIR)
        for attr in sorted(dir(module)):
            if not attr.startswith("QUERY_"):
                continue
            value = getattr(module, attr)
            if isinstance(value, str):
                queries[f"{rel}:{attr}"] = {"file": rel, "sql": value}
    return queries


def run_checksums(queries):
    from sqlalchemy import text

    from database import dw_engine

    results = {}
    with dw_engine.connect() as conn:
        for query_id in sorted(queries):
            sql = queries[query_id]["sql"]
            try:
                rows = conn.execute(text(sql)).fetchall()
            except Exception as error:  # surfaced in the report, never hidden
                results[query_id] = {
                    "file": queries[query_id]["file"],
                    "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                    "error": str(error)[:300],
                }
                continue
            payload = "\n".join(
                "|".join("" if v is None else repr(v) for v in row)
                for row in rows
            )
            results[query_id] = {
                "file": queries[query_id]["file"],
                "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                "rows": len(rows),
                "result_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            }
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", help="write baseline JSON to this path")
    parser.add_argument("--compare", help="compare against a baseline JSON")
    args = parser.parse_args()

    if not args.out and not args.compare:
        parser.error("either --out or --compare is required")

    queries = collect_queries()
    if len(queries) != 25:
        print(f"[!] Expected 25 QUERY_* constants, found {len(queries)}.")
        return 2

    checksums = run_checksums(queries)

    if args.out:
        payload = {
            "generated_at": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
            "warehouse": os.getenv("DW_NAME", "aviation_dw"),
            "query_count": len(checksums),
            "queries": checksums,
        }
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        print(f" -> Baseline written: {args.out} ({len(checksums)} queries)")
        return 0

    with open(args.compare, "r", encoding="utf-8") as handle:
        baseline = json.load(handle)

    failures = []
    for query_id, base in sorted(baseline["queries"].items()):
        current = checksums.get(query_id)
        if current is None:
            failures.append(f"{query_id}: missing in current run")
            continue
        if current.get("sql_sha256") != base.get("sql_sha256"):
            failures.append(f"{query_id}: SQL TEXT CHANGED")
        elif "error" in current or "error" in base:
            if current.get("error") != base.get("error"):
                failures.append(f"{query_id}: execution error state changed")
        elif current.get("result_sha256") != base.get("result_sha256"):
            failures.append(
                f"{query_id}: RESULT CHANGED "
                f"(rows {base.get('rows')} -> {current.get('rows')})"
            )
    extra = set(checksums) - set(baseline["queries"])
    for query_id in sorted(extra):
        failures.append(f"{query_id}: new query not present in baseline")

    if failures:
        print("[!] Comparison FAILED:")
        for line in failures:
            print("   -", line)
        print(
            "\nNote: SQL-hash matches with result mismatches mean the WAREHOUSE "
            "DATA changed, not the query definitions."
        )
        return 1

    print(f" -> OK: all {len(baseline['queries'])} queries match the baseline.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
