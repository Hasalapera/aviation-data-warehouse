"""Immutability regression for the 25 protected analytical queries.

Three independent layers of protection are asserted here:

1. File integrity  - SHA-256 of every protected file matches the Phase-0
                     baseline recorded in docs/BASELINE_MANIFEST.md.
2. Query integrity - exactly 25 QUERY_* constants exist and their SQL text
                     matches config/query_baseline.json byte-for-byte.
3. Executability   - every query still runs against a fresh warehouse
                     schema built from the unmodified sql/schema.sql
                     (schema-drift guard; the test warehouse is empty).

The result-level checksum comparison (live data unchanged) is NOT done
here because these tests run against the disposable empty warehouse; that
comparison is `scripts/query_checksum.py --compare` and is part of the
final verification (see docs/ETL_OPERATIONS.md).
"""

import hashlib
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ["DW_NAME"] = "aviation_dw_ci"

from tests import dbutil  # noqa: E402  (forces DW_NAME before app imports)
from scripts.query_checksum import collect_queries  # noqa: E402

BASELINE_PATH = os.path.join(ROOT, "config", "query_baseline.json")

# Phase-0 SHA-256 values (docs/BASELINE_MANIFEST.md). Files listed here
# must remain byte-identical for the whole task.
PROTECTED_FILE_HASHES = {
    "members/hasala.py": "0bc084a9694ec86c69233aab683d267da226004c64a5ba37e86b882bbaf8d816",
    "members/gihan.py": "f7964deed5e0b8d307f3518d7feb7e4cbe5a63da46019b60c26f63b77503eafb",
    "members/Kavindu.py": "485695f3febf362559227067e91338fc00dc99cbaedb297f17fd5ff082821617",
    "members/navodya.py": "a0f5ca72981ab234cd07514329d1a09b4d1e2a8b203121e84a6c94adfc0bd4a6",
    "members/sadeepa.py": "699a8f453e94e1f6f8373dae18021ec8246a760cef1e15b783d6139defe0890c",
    "etl.py": "bd661d805996205a22a9521f437225816891f3c7798cbcd45d383c3fd78d4e15",
    "oltp_etl.py": "61ba95c058a9af8ae0543656a891bc6b74bb8e4ca58012aab51f785b436a9bed",
    "main_etl.py": "9fa85f43ac25bc42e50f509319945b1b3099823785623dfaa4c9b35cfc6691cc",
    "setup_database.py": "b1f89b6dcf04adf5d0cd16d6f1eda775836a8a8fd681f6dada987139ddbab05c",
    "database.py": "38023436e009b394d2b2847d5c409a2d3f910c31c82328fc99b84d032a455a5c",
    "sql/schema.sql": "00d518d875ed0db094d702bba7af1435fcc4b24ead435344cec34772744181b8",
    "data/flight.csv": "5838f3fee71d7adb43b815d50516561680491c8a0a9cebd9d316994279e9cdf1",
    "requirements.txt": "dbcffa093707d91ee50c23c43ed1e90c8c6d160ba21fa901e6bfb86548bc44d9",
}

# weather_etl.py permits the approved additive P2 incremental-refresh edits:
# the shared start_date_override kwarg, the reviewed archive API end-date cap
# (yesterday), and the per-airport start_date_overrides mapping. It also
# permits the reviewed UTC/DST storage correction (unixtime archive timestamps,
# UTC IEM requests, versioned UTC source model, exact-only matching). Preserve
# the Phase-0 and previously approved hashes; the newest hash is the review
# target.
ALLOWED_WEATHER_ETL_HASHES = {
    "74455080579eccbeacd771f9f18e27f52312ead8244cfefd5f46d70800b0c61c",
    "c2c15658d52b7d320b3d5fa9a3d4e28791896b209998dce84cf1e362c7ecf83b",
    "a1b4fdd361d2b235b5acfc7ecefdd235edb9cdb8ca2e0edc3151a5ab5f6f4d84",
    "dbc47d62b371322655df1c703d1bac21c848b2f2c4e867a982a573a95cd70aa0",
    "8d3b7cf01650dc85d30bb070c6c5dccc4583bd79419606f9b9f69309f3b3408c",
    "04b8becb5f7741587fc3577ea49ba037d809ff72a2f9bbdb7f799a353aa72dc3",
}


def _sha256_file(path):
    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()


class TestProtectedFileIntegrity(unittest.TestCase):
    def test_protected_files_match_phase0_hashes(self):
        changed = []
        for rel, expected in PROTECTED_FILE_HASHES.items():
            actual = _sha256_file(os.path.join(ROOT, rel))
            if actual != expected:
                changed.append(f"{rel}: {actual} != {expected}")
        self.assertEqual(changed, [], "\n".join(changed))

    def test_weather_etl_edit_was_additive_approved(self):
        actual = _sha256_file(os.path.join(ROOT, "weather_etl.py"))
        self.assertIn(
            actual,
            ALLOWED_WEATHER_ETL_HASHES,
            "weather_etl.py hash is neither the Phase-0 hash nor the "
            "approved post-P2 hash",
        )


class TestQueryDefinitions(unittest.TestCase):
    def test_baseline_file_exists_with_25_queries(self):
        self.assertTrue(os.path.exists(BASELINE_PATH), "run scripts/query_checksum.py --out")
        with open(BASELINE_PATH, "r", encoding="utf-8") as handle:
            baseline = json.load(handle)
        self.assertEqual(baseline.get("query_count"), 25)
        self.assertEqual(len(baseline.get("queries", {})), 25)

    def test_exactly_25_query_constants(self):
        queries = collect_queries()
        self.assertEqual(len(queries), 25, sorted(queries))

    def test_sql_text_matches_baseline_byte_for_byte(self):
        with open(BASELINE_PATH, "r", encoding="utf-8") as handle:
            baseline = json.load(handle)["queries"]
        queries = collect_queries()
        self.assertEqual(set(queries), set(baseline))
        for query_id, entry in queries.items():
            base = baseline[query_id]
            # The baseline stores the SQL-text SHA-256; matching it proves
            # the query text is byte-for-byte identical to Phase 0.
            self.assertEqual(
                hashlib.sha256(entry["sql"].encode("utf-8")).hexdigest(),
                base["sql_sha256"],
                f"SQL text changed for {query_id}",
            )

    def test_queries_execute_on_fresh_schema(self):
        dbutil.recreate_schema()
        from sqlalchemy import text

        from database import dw_engine

        queries = collect_queries()
        failures = []
        with dw_engine.connect() as conn:
            for query_id, entry in sorted(queries.items()):
                try:
                    conn.execute(text(entry["sql"])).fetchall()
                except Exception as error:
                    failures.append(f"{query_id}: {error}")
        self.assertEqual(failures, [], "\n".join(failures))


if __name__ == "__main__":
    unittest.main(verbosity=2)
