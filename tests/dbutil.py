"""Disposable test-database helpers.

The previous test suite (recoverable only as stale .pyc) used the same
pattern: a disposable `aviation_dw_ci` database, recreated from
sql/schema.sql for each test class. Sources were never committed; this
module re-establishes that pattern from scratch.

SAFETY RULES
------------
* DW_NAME is forced to aviation_dw_ci at import time (again, defensively).
* Every drop/create helper refuses database names that do not end with
  `_ci` or `_test`, so a mis-set environment can never touch aviation_dw.
"""

import os
import sys

# Defensive: set again here in case a test module imports dbutil directly
# without going through tests/__init__.py (e.g. `python tests/test_x.py`).
os.environ["DW_NAME"] = "aviation_dw_ci"

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

TEST_DB = os.environ["DW_NAME"]

# Import AFTER the env is forced: database.py resolves DW_NAME at import.
import database  # noqa: E402
from sqlalchemy import text  # noqa: E402

assert database.DW_DB_NAME == TEST_DB, (
    f"refusing to run tests: database.py resolved {database.DW_DB_NAME!r}, "
    f"expected {TEST_DB!r} (live-database protection)"
)


def _require_test_name(name):
    if not str(name).endswith(("_ci", "_test")):
        raise RuntimeError(
            f"refusing to modify database {name!r}: only names ending in "
            "'_ci' or '_test' may be dropped/recreated"
        )


def db_available():
    try:
        with database.dw_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def oltp_available():
    try:
        with database.oltp_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def server_engine():
    """Engine without a default database (needed for CREATE/DROP DATABASE)."""
    from sqlalchemy import create_engine

    server_url = database.dw_conn_str.rsplit("/", 1)[0]
    return create_engine(server_url)


def load_sql_statements(path):
    """Split a schema file into executable statements.

    Strips `--` comments and drops connection-target statements
    (CREATE DATABASE / USE) so the same file can be applied to the
    disposable test schema."""
    with open(path, "r", encoding="utf-8") as handle:
        raw = handle.read()
    lines = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        upper = stripped.upper()
        if upper.startswith("USE ") or upper.startswith("CREATE DATABASE"):
            continue
        lines.append(line)
    statements = []
    for chunk in "\n".join(lines).split(";"):
        body = chunk.strip()
        if body:
            statements.append(body)
    if not statements:
        raise RuntimeError(f"no SQL statements found in {path}")
    return statements


def recreate_schema():
    """Drop + recreate the disposable warehouse from sql/schema.sql, then
    ensure the additive audit schema exists. Returns the test dw_engine.

    Only callable for the guarded test database name."""
    _require_test_name(TEST_DB)
    server = server_engine()
    with server.begin() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS `{TEST_DB}`"))
        conn.execute(text(f"CREATE DATABASE `{TEST_DB}` CHARACTER SET utf8mb4"))
    server.dispose()

    statements = load_sql_statements(os.path.join(ROOT, "sql", "schema.sql"))
    with database.dw_engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))

    import etl_audit

    etl_audit.ensure_schema()  # points at the test DB (env forced above)
    return database.dw_engine


def drop_test_db():
    _require_test_name(TEST_DB)
    server = server_engine()
    with server.begin() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS `{TEST_DB}`"))
    server.dispose()


def clear_audit():
    """Remove audit rows written during a test run (test hygiene)."""
    with database.dw_engine.begin() as conn:
        conn.execute(text("DELETE FROM etl_batch_log"))
        conn.execute(text("DELETE FROM etl_watermark"))
