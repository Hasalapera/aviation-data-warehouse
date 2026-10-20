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
* Watermarks (weather, opt-in) advance only after the stage transaction
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
import re
import sys
import time
import uuid
from datetime import datetime, timedelta

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

def load_config(path=None):
    path = path or DEFAULT_CONFIG
    with open(path, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    order = config.get("order") or ["csv", "weather", "oltp"]
    sources = config.setdefault("sources", {})
    for name in order:
        sources.setdefault(name, {"enabled": True, "required": True})
    config.setdefault("lock_timeout_seconds", 0)
    config.setdefault("stale_running_minutes", 240)
    config.setdefault("run_verification", True)
    config.setdefault("verification_fails_run", True)
    config.setdefault("csv_path", "data/flight.csv")
    config.setdefault("weather_chunk_size", 1000)
    config.setdefault("weather_incremental", {"enabled": False, "overlap_days": 7})
    if int(config["weather_chunk_size"]) <= 0:
        raise ValueError("weather_chunk_size must be positive")
    if int(config["stale_running_minutes"]) <= 0:
        raise ValueError("stale_running_minutes must be positive")
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
# WEATHER PARTIAL-STATUS PARSING (from the stage's own report)
# =========================================================

def _weather_partial_note(captured):
    """Detect airports whose archive fetch failed inside a successful stage.

    weather_etl.run_weather_etl returns success even when individual
    airports failed to fetch; its summary line is the only reliable signal
    without modifying the stage. If the format ever changes, this returns
    None and the stage is reported as SUCCESS (documented limitation)."""
    match = re.search(r"airports failed\s*:\s*\[([^\]]*)\]", captured or "")
    if match and match.group(1).strip():
        return f"archive fetch failed for: [{match.group(1).strip()}]"
    return None


# =========================================================
# WEATHER WATERMARKS (P2 - opt-in)
# =========================================================

def weather_start_override(config):
    """Global fetch start date derived from per-airport watermarks.

    Returns None when incremental mode is off or no watermark exists yet
    (=> full-range fetch, i.e. existing behaviour). The overlap is
    subtracted so late-arriving corrections are re-fetched."""
    inc = config.get("weather_incremental") or {}
    if not inc.get("enabled"):
        return None
    watermarks = etl_audit.get_watermarks("weather")  # AuditError propagates
    if not watermarks:
        return None
    try:
        earliest = min(
            datetime.fromisoformat(str(value)) for value in watermarks.values()
        )
    except ValueError:
        return None  # unreadable watermark -> full fetch (safe)
    overlap = int(inc.get("overlap_days", 7))
    return (earliest - timedelta(days=overlap)).date()


def advance_weather_watermarks(batch_guid):
    """Advance per-airport watermarks AFTER the weather stage committed.

    An airport's watermark moves only if its persisted observations reach
    (expected fetch end - 1 day). Airports whose fetch failed keep their
    old watermark, so the next run re-covers them. Watermark writes use the
    separate audit session; a failure raises AuditError (reported as exit
    code 5 - the committed data is still valid).

    Returns (advanced, held) dict/tuple for reporting."""
    from sqlalchemy import text

    from database import dw_engine

    held = {}
    advanced = {}
    with dw_engine.connect() as conn:
        persisted = {
            row[0]: row[1]
            for row in conn.execute(
                text(
                    """
                    SELECT a.airport_code, MAX(w.observation_time)
                    FROM dim_weather w
                    JOIN dim_airport a ON w.airport_key = a.airport_key
                    WHERE w.source_dataset = 'open-meteo-archive'
                    GROUP BY a.airport_code
                    """
                )
            ).fetchall()
        }
        expected = {
            row[0]: row[1]
            for row in conn.execute(
                text(
                    """
                    SELECT a.airport_code, MAX(DATE(f.scheduled_departure))
                    FROM fact_flight f
                    JOIN dim_route r ON f.route_key = r.route_key
                    JOIN dim_airport a ON r.origin_airport_key = a.airport_key
                    GROUP BY a.airport_code
                    """
                )
            ).fetchall()
        }
    for airport, expected_end in expected.items():
        max_obs = persisted.get(airport)
        if max_obs is None:
            held[airport] = "no persisted observations"
            continue
        if max_obs.date() >= (expected_end - timedelta(days=1)):
            etl_audit.record_watermark(
                "weather", airport, max_obs.replace(microsecond=0), batch_guid
            )
            advanced[airport] = str(max_obs)
        else:
            held[airport] = (
                f"persisted {max_obs.date()} < expected {expected_end} "
                "(fetch incomplete)"
            )
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

    def weather_stage(start_date_override=None):
        if start_date_override is not None:
            run_weather_etl(start_date_override=start_date_override)
        else:
            run_weather_etl()

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
        lock.release()


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

        # P2: compute weather fetch window BEFORE the stage; an audit
        # failure here is fail-closed (stage simply does not run).
        start_override = None
        if source == "weather" and (config.get("weather_incremental") or {}).get("enabled"):
            try:
                start_override = weather_start_override(config)
                if start_override is not None:
                    print(f" -> incremental window: start >= {start_override}")
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
        try:
            kwargs = {}
            if start_override is not None:
                kwargs["start_date_override"] = start_override
            stages[source](**kwargs)
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
                partial = _weather_partial_note(captured.getvalue())
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
            verify_checks = _run_verification(verifier)
        except KeyboardInterrupt:
            _print_summary(results, batch_guid, started, aborted=True)
            print(
                f"\n[INTERRUPTED][EXIT {EXIT_INTERRUPTED}] operator interrupt "
                "during verification; watermarks not advanced"
            )
            return EXIT_INTERRUPTED
        except Exception as error:
            verify_checks = None
            verify_ok = False
            print(f"[!] verification could not complete: {error}")
        else:
            verify_ok = not any(c.status == "FAIL" for c in verify_checks)
        # audit the verification attempt itself
        info = None
        try:
            info = etl_audit.start_run("verify", batch_guid)
            etl_audit.finish_run(
                info["run_id"],
                "SUCCESS" if verify_ok else "FAILED",
                notes=(
                    "post-load verification "
                    + ("passed" if verify_ok else "reported FAIL checks")
                ),
                error=None if verify_ok else "one or more checks FAILED",
            )
        except KeyboardInterrupt:
            if info is not None:
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
        and weather_status in ("SUCCESS", "PARTIAL")
        and verification_ran
        and verify_ok
    ):
        try:
            advanced, held = advance_weather_watermarks(batch_guid)
            print(
                f" -> watermarks advanced for {len(advanced)} airport(s); "
                f"held for {len(held)} ({held or 'none'})"
            )
        except etl_audit.AuditError as error:
            print(
                f"\n[FATAL][EXIT {EXIT_AUDIT_FINISH}] weather stage committed "
                f"but watermark write failed (next run re-fetches - safe): {error}"
            )
            _print_summary(results, batch_guid, started, aborted=True)
            return EXIT_AUDIT_FINISH
    elif weather_status and (
        config.get("weather_incremental") or {}
    ).get("enabled") and weather_status in ("SUCCESS", "PARTIAL"):
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
        print(" -> weather watermarks unchanged (incremental off or stage "
              "not successful)")

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
    args = parser.parse_args(argv)

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
