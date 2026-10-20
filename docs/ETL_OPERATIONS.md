# ETL Operations Runbook

All commands run from the repository root with the project venv:

```bash
source venv/bin/activate   # or use venv/bin/python directly
```

Configuration lives in `.env` (never committed) and
`config/refresh_policy.json` (safe to edit; defaults preserve the
original ETL behaviour).

## The refresh command

```bash
venv/bin/python refresh_orchestrator.py                 # all enabled stages
venv/bin/python refresh_orchestrator.py --dry-run       # read-only preflight (no loads, no DDL, no lock)
venv/bin/python refresh_orchestrator.py --source csv    # one stage
venv/bin/python refresh_orchestrator.py --source weather
venv/bin/python refresh_orchestrator.py --no-verify     # skip post-load checks (weather watermarks will NOT advance)
venv/bin/python refresh_orchestrator.py --verify-only   # read-only checks only (never creates audit tables)
venv/bin/python refresh_orchestrator.py --enable-changes # opt-in OLTP updates
venv/bin/python refresh_orchestrator.py --config path/to/policy.json
```

`--dry-run` and `--verify-only` are strictly read-only: they never
execute DDL, never create audit tables, never insert audit rows, never
take the run lock and never invoke an ETL stage. If the audit tables are
missing, a normal run bootstraps them once (`ensure_schema` executes no
DDL when they already exist); the read-only modes only report their
absence.

What one run does, in order:

1. Preflight: validate config/source selection (no side effects).
2. Ensure audit schema (`etl_batch_log`, `etl_watermark` - additive;
   no DDL when already initialised).
3. Acquire the run lock (`GET_LOCK('aviation_dw.etl_refresh')`) - if
   another run holds it, exit `3` without touching anything.
4. Per stage: record `RUNNING` (fail-closed: no audit row -> stage not
   executed) -> run the existing ETL entry point with stdout captured to
   `logs/refresh_<ts>_<batch>_<source>.log` -> measure honest counts ->
   record terminal status + metrics (audit failure -> exit `5`, remaining
   stages skipped). Ctrl-C during a stage closes the audit row as FAILED
   and exits `130` (committed data is not rolled back).
5. Post-load verification (read-only, `verify_warehouse.py`), itself
   audited as source `verify`.
6. Watermarks advance (weather, only if incremental enabled, stage
   successful, verification ran, and verification passed - `--no-verify`
   suppresses advancement).
7. Print summary and exit with the policy exit code.

Exit codes: `0` ok, `1` required stage failed, `2` preflight, `3` locked,
`4` audit unavailable before stage, `5` audit/watermark write failed
after stage (data may be committed), `6` verification failed,
`130` interrupted by operator.

## Standalone verification

```bash
venv/bin/python verify_warehouse.py               # report, exit 0/1
venv/bin/python verify_warehouse.py --strict      # warnings also fail
venv/bin/python verify_warehouse.py --json        # machine-readable
venv/bin/python verify_warehouse.py --stale-minutes 60
venv/bin/python verify_warehouse.py --write-baseline config/schema_baseline.json \
    --allow-baseline-write                        # explicit confirmation required
```

Read-only: it can never repair anything. The only write mode is
`--write-baseline`, and it refuses (exit `2`, no file written) unless
`--allow-baseline-write` is also given - the confirmation that the target
database was reviewed for unapproved drift. Schema contract baseline:
`config/schema_baseline.json` (regenerate only when a schema change is
*approved*).

## Query integrity (before/after any maintenance)

```bash
venv/bin/python scripts/query_checksum.py --compare config/query_baseline.json
```

Exit `1` if any of the 25 analytical queries changed text or (on
unchanged data) changed results. SQL-hash match + result mismatch means
the DATA changed, not the queries - investigate accordingly.

Exception: a mismatch confined to
`members/Kavindu.py:QUERY_CANCELLATIONS_BY_ROUTE` is usually tie-ordering,
not data - that query is `ORDER BY cancelled_flights DESC LIMIT 10` with
ties at the boundary (see "Query-result baseline re-recording" in
`docs/BASELINE_MANIFEST.md`). Confirm by comparing the full grouped
aggregates before concluding anything changed.

## Bounded OLTP change updater (standalone CLI)

```bash
venv/bin/python oltp_change_updater.py             # DRY RUN (default): report only
venv/bin/python oltp_change_updater.py --execute   # lock + audit + updates + verification
```

Fail-closed by design:

- The default invocation changes nothing (no fact UPDATEs, no audit
  rows); it prints how many rows WOULD change.
- `--execute` runs the complete lifecycle: takes the same run lock as
  the orchestrator (a held lock exits `3` and applies nothing), records
  an audit row (`source='oltp'`), applies the bounded updates to
  EXISTING OLTP fact rows, then runs the same read-only post-update
  verification the orchestrator runs.
- Exit codes: `0` ok/dry-run, `1` update failed (transaction rolled
  back), `3` lock held elsewhere, `4` audit layer unavailable,
  `6` verification did not pass (updates stay **committed** - they are
  not rolled back on verification failure), `130` interrupted.
- A verification failure (exit `6`) leaves the audit row FAILED with the
  same explanation: inspect the warehouse before re-running.
- Recommended path remains `refresh_orchestrator.py --enable-changes`
  (the full OLTP stage + updates + verification in one sequence).
  `--enable-changes` only has an effect when the `oltp` stage is
  selected; otherwise the orchestrator prints a loud warning.
- Take a backup first (`venv/bin/python setup_database.py backup`).

## Audit queries

```sql
-- recent runs
SELECT run_id, source_name, status, started_at, finished_at,
       rows_read, rows_inserted, rows_updated, rows_skipped, notes
FROM etl_batch_log ORDER BY run_id DESC LIMIT 20;

-- suspicious states (should be empty)
SELECT * FROM etl_batch_log WHERE status = 'RUNNING'
  AND started_at < NOW() - INTERVAL 4 HOUR;   -- crashed run: inspect, then
                                              -- close manually if confirmed

-- weather watermarks
SELECT * FROM etl_watermark WHERE source_name = 'weather'
 ORDER BY updated_at DESC;
```

## Logs

- Stage output: `logs/refresh_<timestamp>_<batch>_<source>.log`
- Scheduled runs (if enabled): `logs/scheduled_refresh_<date>.log`
- `logs/` is git-ignored.

## Scheduled refresh (P3, disabled by default)

`scripts/scheduled_refresh.sh` wraps the orchestrator (lock, log, exit
code). launchd templates live in `scripts/launchd/` and are **not
installed**; enabling a schedule is an explicit operator action documented
in the template headers.

## Tests

```bash
venv/bin/python -m unittest discover        # from repo root
```

- Tests force `DW_NAME=aviation_dw_ci` (disposable warehouse recreated
  from `sql/schema.sql`); the live `aviation_dw` cannot be written by the
  suite (guarded in `tests/__init__.py` and `tests/dbutil.py`).
- `tests/test_etl_regression.py` runs the real CSV/OLTP stages against
  the test warehouse (OLTP stage skipped when the source is unreachable).
- A real weather fetch is opt-in:
  `AVIATION_TESTS_WEATHER_NET=1 venv/bin/python -m unittest discover`.
- Pre-existing state at Phase 0: `tests/` contained only stale `.pyc`
  files (sources never committed) - the old suite was unrunnable; see
  `docs/BASELINE_MANIFEST.md`.

## Safety rules for operators

- Never run a refresh while `main_etl.py` is running (no shared lock).
- Take a backup before any run that could change row data
  (`venv/bin/python setup_database.py backup`), and **always** before
  `--enable-changes`.
- Do not run `--enable-changes` against production without a reviewed
  backup: it is the only code path that updates existing fact rows.
- Do not hand-edit `config/schema_baseline.json`; regenerate only for
  approved schema changes (`verify_warehouse.py --write-baseline` with
  `--allow-baseline-write`).
- The weather stage writes `dim_weather` upserts: on the live warehouse a
  weather refresh may revise observation values (by design of the
  archive-loader). If analytical results must be frozen, verify checksums
  immediately afterwards.

## Troubleshooting by exit code

| Exit | First action |
|---|---|
| 1 | `SELECT ... FROM etl_batch_log ORDER BY run_id DESC LIMIT 5` - read `error_detail`/`notes`; failed stage is marked; rerun is safe (idempotent). |
| 2 | Check `.env` connectivity, `--source` names, `config/refresh_policy.json`. |
| 3 | Find the holder: another orchestrator/terminal; wait or stop it. |
| 4 | Audit layer down (disk/privileges/table missing): fix MySQL access; nothing was executed. |
| 5 | Stage data may be committed while audit failed: inspect `etl_batch_log`, verify row counts, then rerun (idempotent). |
| 6 | Read the verification report; fix or, if intentional, re-baseline only after approval. |
| 130 | Operator interrupt (Ctrl-C). The stage audit row is FAILED with an interrupt note; committed stage data is not rolled back. Rerun is safe (idempotent). A still-open RUNNING row (interrupt outside a handled window) is reported as stale by verification. |
