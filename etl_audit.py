"""ETL audit logging, run locking and watermark persistence.

Design rules (see docs/DATA_REFRESH_POLICY.md):

* The audit tables are ADDITIVE and isolated: no analytical query
  references them and no existing table references them.
* Audit writes always use a SEPARATE pymysql session with autocommit=True,
  never the ETL stage's transaction. A rolled-back load must never roll
  back its own FAILURE record.
* If the audit layer cannot write, the caller (refresh_orchestrator) must
  treat the run as failed - a run is never reported successful when its
  audit trail could not be recorded. Writers here raise AuditError.
* Watermarks are advanced ONLY by the caller AFTER the corresponding load
  transaction has committed. Nothing in this module advances a watermark
  on its own.

Uses only PyMySQL + python-dotenv (already in requirements.txt).
"""

import os
import re
import uuid

import pymysql
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUDIT_DDL_FILE = os.path.join(BASE_DIR, "sql", "audit_schema.sql")

RUN_LOCK_NAME = "aviation_dw.etl_refresh"

VALID_STATUSES = ("RUNNING", "SUCCESS", "FAILED", "PARTIAL")
VALID_SOURCES = ("csv", "weather", "oltp", "verify")


class AuditError(RuntimeError):
    """Raised when an audit write/read could not be completed."""


# =========================================================
# ERROR SANITISATION (never persist credentials)
# =========================================================

_SECRET_PATTERNS = [
    re.compile(r"(://[^:/\s]+):[^@\s]+@"),                       # user:pass@
    re.compile(r"((?:password|passwd|pwd)\s*[=:]\s*)\S+", re.I),
    re.compile(r"(MYSQL_PWD\s*[=:]\s*)\S+", re.I),
]


def sanitize_error(message, limit=4000):
    """Strip credential-like fragments and truncate an error message."""
    if message is None:
        return None
    result = str(message)
    for pattern in _SECRET_PATTERNS:
        result = pattern.sub(r"\1***", result)
    return result[:limit]


# =========================================================
# CONNECTION (separate session, autocommit - see design rules)
# =========================================================

def _connect():
    try:
        return pymysql.connect(
            host=os.getenv("DW_HOST", "localhost"),
            port=int(os.getenv("DW_PORT", 3306)),
            user=os.getenv("DW_USER", "root"),
            password=os.getenv("DW_PASSWORD", ""),
            database=os.getenv("DW_NAME", "aviation_dw"),
            autocommit=True,
        )
    except Exception as error:
        raise AuditError(f"cannot open audit connection: {sanitize_error(error)}")


# =========================================================
# SCHEMA (idempotent)
# =========================================================

def _ddl_statements():
    """Read sql/audit_schema.sql and return executable CREATE statements.

    Connection-specific statements (USE / CREATE DATABASE) are stripped so
    the same DDL applies to the warehouse or to a disposable test schema."""
    if not os.path.exists(AUDIT_DDL_FILE):
        raise AuditError(f"audit DDL file missing: {AUDIT_DDL_FILE}")
    with open(AUDIT_DDL_FILE, "r", encoding="utf-8") as handle:
        raw = handle.read()
    statements = []
    for chunk in raw.split(";"):
        body = "\n".join(
            line for line in chunk.splitlines()
            if not line.strip().upper().startswith("USE ")
            and not line.strip().upper().startswith("CREATE DATABASE")
        ).strip()
        if body.upper().startswith("CREATE TABLE"):
            statements.append(body)
    if not statements:
        raise AuditError("no CREATE TABLE statements found in audit DDL")
    return statements


AUDIT_TABLES = ("etl_batch_log", "etl_watermark")


def ensure_schema():
    """Create etl_batch_log / etl_watermark if absent; safe to repeat.

    When both tables already exist this is a pure existence check and
    executes NO DDL (so an initialised production warehouse never receives
    DDL statements on a normal run). DDL runs only when at least one table
    is missing (bootstrap for first run / CI).

    Raises AuditError on failure (callers must not proceed silently)."""
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() "
                f"AND table_name IN ({', '.join(repr(t) for t in AUDIT_TABLES)})"
            )
            if int(cursor.fetchone()[0]) == len(AUDIT_TABLES):
                return  # fully initialised: no DDL executed
            for statement in _ddl_statements():
                cursor.execute(statement)
    except Exception as error:
        raise AuditError(f"cannot ensure audit schema: {sanitize_error(error)}")
    finally:
        conn.close()


def audit_table_exists():
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = 'etl_batch_log'"
            )
            return bool(cursor.fetchone()[0])
    except Exception as error:
        raise AuditError(f"cannot inspect audit schema: {sanitize_error(error)}")
    finally:
        conn.close()


# =========================================================
# RUN LOG
# =========================================================

def start_run(source_name, batch_guid=None):
    """Insert a RUNNING row. Returns {run_id, batch_guid, started_at}."""
    if source_name not in VALID_SOURCES:
        raise AuditError(f"unknown source_name: {source_name!r}")
    batch_guid = batch_guid or str(uuid.uuid4())
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etl_batch_log
                    (batch_guid, source_name, status, started_at, engine_version)
                VALUES (%s, %s, 'RUNNING', CURRENT_TIMESTAMP(3), VERSION())
                """,
                (batch_guid, source_name),
            )
            run_id = cursor.lastrowid
            cursor.execute(
                "SELECT run_id, started_at FROM etl_batch_log WHERE run_id = %s",
                (run_id,),
            )
            row = cursor.fetchone()
        return {
            "run_id": int(row[0]),
            "batch_guid": batch_guid,
            "started_at": row[1],
        }
    except Exception as error:
        raise AuditError(f"cannot record run start: {sanitize_error(error)}")
    finally:
        conn.close()


def finish_run(
    run_id,
    status,
    rows_read=None,
    rows_before=None,
    rows_after=None,
    rows_inserted=None,
    rows_updated=None,
    rows_skipped=None,
    notes=None,
    error=None,
):
    """Write the terminal state of a run. Raises AuditError on failure."""
    if status not in VALID_STATUSES or status == "RUNNING":
        raise AuditError(f"invalid terminal status: {status!r}")
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            affected = cursor.execute(
                """
                UPDATE etl_batch_log
                SET status = %s,
                    finished_at = CURRENT_TIMESTAMP(3),
                    rows_read = %s,
                    rows_before = %s,
                    rows_after = %s,
                    rows_inserted = %s,
                    rows_updated = %s,
                    rows_skipped = %s,
                    notes = %s,
                    error_detail = %s
                WHERE run_id = %s AND status = 'RUNNING'
                """,
                (
                    status,
                    rows_read,
                    rows_before,
                    rows_after,
                    rows_inserted,
                    rows_updated,
                    rows_skipped,
                    sanitize_error(notes, 500) if notes else None,
                    sanitize_error(error),
                    int(run_id),
                ),
            )
            if affected != 1:
                raise AuditError(
                    f"run {run_id} not found in RUNNING state "
                    "(already closed or missing)"
                )
    except AuditError:
        raise
    except Exception as error:
        raise AuditError(
            "cannot record run finish (stage data may already be committed): "
            + sanitize_error(error)
        )
    finally:
        conn.close()


def recent_runs(source_name=None, limit=20):
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            sql = (
                "SELECT run_id, batch_guid, source_name, status, started_at, "
                "finished_at, rows_read, rows_before, rows_after, "
                "rows_inserted, rows_updated, rows_skipped, notes, error_detail "
                "FROM etl_batch_log"
            )
            params = []
            if source_name:
                sql += " WHERE source_name = %s"
                params.append(source_name)
            sql += " ORDER BY run_id DESC LIMIT %s"
            params.append(int(limit))
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        keys = ("run_id", "batch_guid", "source_name", "status", "started_at",
                "finished_at", "rows_read", "rows_before", "rows_after",
                "rows_inserted", "rows_updated", "rows_skipped", "notes",
                "error_detail")
        return [dict(zip(keys, row)) for row in rows]
    except Exception as error:
        raise AuditError(f"cannot read audit log: {sanitize_error(error)}")
    finally:
        conn.close()


# =========================================================
# RUN LOCK (concurrent-run protection)
# =========================================================

class RunLock:
    """Holds a MySQL named lock for the lifetime of one DB session."""

    def __init__(self, connection, name):
        self._conn = connection
        self.name = name
        self.released = False

    def release(self):
        if self.released:
            return
        try:
            with self._conn.cursor() as cursor:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (self.name,))
        except Exception:
            pass  # closing the session below releases the lock in any case
        finally:
            try:
                self._conn.close()
            except Exception:
                pass
            self.released = True


def acquire_run_lock(timeout=0, lock_name=RUN_LOCK_NAME):
    """Try to acquire the warehouse refresh lock.

    Returns a RunLock, or None if another run currently holds it.
    Raises AuditError if the lock could not be evaluated at all."""
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, %s)", (lock_name, int(timeout)))
            row = cursor.fetchone()
        if row and int(row[0]) == 1:
            return RunLock(conn, lock_name)
        conn.close()
        return None
    except Exception as error:
        try:
            conn.close()
        except Exception:
            pass
        raise AuditError(f"cannot evaluate run lock: {sanitize_error(error)}")


# =========================================================
# WATERMARKS (advanced only by the caller, after load commit)
# =========================================================

def record_watermark(source_name, window_key, watermark_value,
                     batch_guid=None):
    """Upsert one watermark. Caller MUST have committed the load first."""
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etl_watermark
                    (source_name, window_key, watermark_value, updated_at,
                     last_batch_guid)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP(3), %s)
                ON DUPLICATE KEY UPDATE
                    watermark_value = VALUES(watermark_value),
                    updated_at = VALUES(updated_at),
                    last_batch_guid = VALUES(last_batch_guid)
                """,
                (source_name, window_key, str(watermark_value), batch_guid),
            )
    except Exception as error:
        raise AuditError(f"cannot record watermark: {sanitize_error(error)}")
    finally:
        conn.close()


def get_watermarks(source_name):
    """Return {window_key: watermark_value} for one source."""
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT window_key, watermark_value FROM etl_watermark "
                "WHERE source_name = %s",
                (source_name,),
            )
            rows = cursor.fetchall()
        return {row[0]: row[1] for row in rows}
    except Exception as error:
        raise AuditError(f"cannot read watermarks: {sanitize_error(error)}")
    finally:
        conn.close()
