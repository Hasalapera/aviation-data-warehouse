# ETL Operations Runbook

All commands run from the repository root with the project venv:

```bash
source venv/bin/activate   # or use venv/bin/python directly
```

The scheduled wrapper requires `venv/bin/python` and validates the
runtime imports; it never falls back to system Python.

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
   `logs/refresh_<ts>_<batch>_<source>.log` -> capture the stage's
   structured result -> measure honest counts -> record terminal status +
   metrics (audit failure -> exit `5`, remaining stages skipped). The
   weather stage is `PARTIAL` when its returned `airports_failed` list is
   non-empty; console text is never parsed. Ctrl-C during a stage closes
   the audit row as FAILED and exits `130` (committed data is not rolled
   back).
5. Post-load verification (read-only, `verify_warehouse.py`), itself
   audited as source `verify`.
6. Watermarks advance per airport (weather, only if incremental enabled,
   the stage ended `SUCCESS`, verification ran, verification passed, and
   that airport's persisted observations cover every expected UTC instant
   of its own window). The candidate is the maximum **verified in-window**
   instant; stale or future rows outside the expected window can never
   advance it. `--no-verify` suppresses advancement; `PARTIAL` / `FAILED`
   stages advance nothing. Held airports are printed with the reason and
   re-covered next run.
7. Print summary and exit with the policy exit code.

Exit codes: `0` ok, `1` required stage failed, `2` preflight, `3` locked,
`4` audit unavailable before stage, `5` audit/watermark write or
advancement check failed after stage (data may be committed), `6`
verification failed, `130` interrupted by operator.

## Incremental weather mode (enabled by policy)

Incremental weather is controlled **only** by
`config/refresh_policy.json`:

```json
"weather_incremental": { "enabled": true, "overlap_days": 7 }
```

- `enabled: true` (shipped) makes each airport fetch from
  `its own watermark date - overlap_days`, clamped to its first flight.
  Airports without a watermark fetch their full available history.
- `enabled: false` restores the original full-range fetch exactly; no
  per-airport override is passed to `weather_etl.py`.
- Enable/disable is a config edit; it does not itself start a refresh.
  Run `--dry-run` first, then a normal audited run.
- `enabled` must be a real JSON boolean and `overlap_days` must be an
  integer `>= 0`; ill-typed values fail config loading with exit `2`.
  The overlap deliberately re-fetches recent days so late archive
  corrections are picked up; it is not a completeness margin for older
  data.
- A watermark only advances when that airport's persisted observations
  cover every expected UTC instant of its window (not merely every calendar
  date) and verification passed. Once an airport has `best_match_utc_v1`
  rows, only UTC-model rows count toward coverage; legacy local labels
  cannot hide a UTC gap. Stale/future rows outside the expected window are
  visible for reporting but are excluded from the candidate. New UTC
  watermarks are written with a trailing `Z` (for example
  `2026-10-22 18:00:00Z`). Inspect progress and withheld airports with:

```sql
SELECT window_key, watermark_value, updated_at, last_batch_guid
FROM etl_watermark WHERE source_name = 'weather'
ORDER BY window_key;
```

- On a first run (no watermarks) or after deleting a watermark, that
  airport is fetched full-range again. This is slower but safe.
- Recovery: if a run is `PARTIAL`, fails verification, or is interrupted,
  no watermark moves for that run. Fix the cause and rerun; incomplete
  airports are re-covered automatically.

Rollout status: the implementation is covered by the full CI suite
(including partial fetches, mocked end-to-end upsert/rollback, and
coverage gaps), and read-only Open-Meteo/IEM validation succeeded for all
eight configured airports on a bounded three-day window. The shipped
policy has `enabled: true`. A first run with no existing watermarks still
fetches full history; enablement does not itself start a refresh.

### First production incremental run and monitoring

1. Take a backup before the first run
   (`venv/bin/python setup_database.py backup`).
2. Confirm `weather_incremental.enabled` is `true` and `overlap_days` is
   an integer `>= 0`.
3. Run `venv/bin/python refresh_orchestrator.py --dry-run`; require exit
   `0` and `incremental weather: ON`. The dry run does not execute a
   stage, take the lock, or write audit rows.
4. Run `venv/bin/python refresh_orchestrator.py --source weather` with
   verification enabled (the default). Do **not** use `--no-verify` on a
   run whose watermarks should advance. Airports without watermarks fetch
   their full available history on this first run.
5. Watch the exit code and output. Success prints
   `watermarks advanced for N airport(s)`; every withheld airport is
   printed separately with its reason. `PARTIAL`, `FAILED`, a verification
   failure, or exit `5`/`6` must be treated as "no advancement for the
   affected airports", not as a reason to bypass verification.
6. Inspect the audited stages and any error text:

```sql
SELECT run_id, batch_guid, source_name, status, started_at, finished_at,
       rows_read, rows_inserted, rows_updated, rows_skipped, notes,
       error_detail
FROM etl_batch_log
WHERE source_name IN ('weather', 'verify')
ORDER BY run_id DESC LIMIT 10;
```

Stage output is written to
`logs/refresh_<timestamp>_<batch>_<source>.log`; scheduled output, if
enabled, is `logs/scheduled_refresh_<date>.log`. Check the watermark
values with the `etl_watermark` query above and verify the actual
per-airport coverage before the next run.

### Verify weather coverage

Run this after a refresh, replacing the dates with the expected window.
Every calendar day required for each airport must appear; do not assume a
fixed row count per day (DST days can have 23 or 25 hourly observations):

```sql
SELECT a.airport_code,
       DATE(w.observation_time) AS observation_date,
       COUNT(*) AS observations,
       MIN(w.observation_time) AS first_observation,
       MAX(w.observation_time) AS last_observation
FROM dim_weather w
JOIN dim_airport a ON w.airport_key = a.airport_key
WHERE w.source_dataset = 'open-meteo-archive'
  AND w.observation_time >= '2024-06-01'
  AND w.observation_time < '2024-06-04'
GROUP BY a.airport_code, DATE(w.observation_time)
ORDER BY a.airport_code, observation_date;
```

The dates in the example are a bounded three-day window, not production
dates. The orchestrator performs the same calendar-day coverage check
against the window it derived before it advances a watermark; this query
is the operator-facing confirmation.

### Return to full-history mode

Set `weather_incremental.enabled` back to `false` and run `--dry-run` to
confirm `incremental weather: OFF`. The next normal run passes no
per-airport override and fetches each airport's full available history
exactly as before. Existing watermarks are ignored while incremental mode
is disabled; keep them if incremental mode may be re-enabled later. Do
not drop `etl_watermark` or `etl_batch_log` merely to disable incremental
mode.

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

## Logs and retention

- Stage output: `logs/refresh_<timestamp>_<batch>_<source>.log`
- Scheduled runs (if enabled): `logs/scheduled_refresh_<date>.log`
- launchd-level output: `logs/launchd_refresh.log` and `.err`
- `logs/` is git-ignored.

Python owns retention; the wrapper does not run `find`/`-delete`.
`prune_old_logs()` runs once at the end of a normal refresh, after the
database lock is released:

- `ETL_LOG_RETENTION_DAYS` controls the age limit; default `30`.
- Invalid values fall back to `30`; `0` or negative disables pruning.
- Only `refresh_*` and `scheduled_refresh_*` `.log`/`.err` files are
  eligible.
- The newest 10 eligible files are always protected, preserving recent
  failure logs.
- `launchd_*` stdout/stderr is never pruned automatically (launchd owns
  those handles); rotate them manually if needed.

## Scheduled refresh (P3, disabled by default)

`scripts/scheduled_refresh.sh` wraps the orchestrator (lock, log, exit
code) and requires `$REPO/venv/bin/python` with `pymysql`, `sqlalchemy`,
`pandas`, `dotenv`, and `requests`. A missing venv or module exits `2`
and logs the reason; it never runs system Python.

Only one LaunchAgent is supported: `com.aviation.dw.refresh` (daily full
pipeline). The old weekly weather-only template has been retired. Nothing
is installed or activated by this repository.

The template lives at
`scripts/launchd/com.aviation.dw.refresh.plist.template`. Its
`StartCalendarInterval` uses the Mac's **current system timezone**;
launchd has no per-job timezone. The template says 02:00, but that is
Asia/Colombo time only when the host is set to Asia/Colombo. Check with
`readlink /etc/localtime` and adjust `Hour`/`Minute` or the host timezone.

Install (explicit operator action, not performed by this task):

```bash
mkdir -p logs ~/Library/LaunchAgents
sed "s|__REPO_PATH__|$PWD|g" \
  scripts/launchd/com.aviation.dw.refresh.plist.template \
  > ~/Library/LaunchAgents/com.aviation.dw.refresh.plist
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/com.aviation.dw.refresh.plist
launchctl print gui/$(id -u)/com.aviation.dw.refresh
# optional immediate run:
launchctl kickstart -k gui/$(id -u)/com.aviation.dw.refresh
```

If the label is already loaded, `bootstrap` fails; run
`launchctl print` and `launchctl bootout` before bootstrapping again. Do
not install a second job.

Remove:

```bash
launchctl bootout gui/$(id -u)/com.aviation.dw.refresh
rm ~/Library/LaunchAgents/com.aviation.dw.refresh.plist
```

Monitor:

```bash
launchctl print gui/$(id -u)/com.aviation.dw.refresh
tail -n 50 logs/launchd_refresh.log logs/launchd_refresh.err
ls -lt logs/scheduled_refresh_*.log | head
venv/bin/python refresh_orchestrator.py --status
```

Troubleshooting: exit `3` means another refresh holds the lock; exit `2`
usually means the venv/modules or configuration are invalid; non-zero
per-run logs should be inspected with the audit queries above.

## Tests

```bash
ETL_LOG_RETENTION_DAYS=0 venv/bin/python -m unittest discover tests
venv/bin/python -m unittest tests.test_watermark -v
venv/bin/python -m unittest tests.test_query_integrity -v
venv/bin/python refresh_orchestrator.py --dry-run
```

Set `ETL_LOG_RETENTION_DAYS=0` while running tests so the repository's
own `logs/` directory is not pruned.

- Tests force `DW_NAME=aviation_dw_ci` (disposable warehouse recreated
  from `sql/schema.sql`); the live `aviation_dw` cannot be written by the
  suite (guarded in `tests/__init__.py` and `tests/dbutil.py`).
- `tests/test_etl_regression.py` runs the real CSV/OLTP stages against
  the test warehouse (OLTP stage skipped when the source is unreachable).
- `tests/test_watermark.py` covers per-airport windows, overlap, first-run
  behaviour, structured `PARTIAL`/`FAILED`/verification gating, stale and
  future out-of-window observations, missing final observations, DST,
  leap day, half-hour offsets, retention, and launchd/script structure
  using deterministic stubs (no weather network access).
- A real weather fetch is opt-in:
  `AVIATION_TESTS_WEATHER_NET=1 venv/bin/python -m unittest discover`.
- Pre-existing state at Phase 0: `tests/` contained only stale `.pyc`
  files (sources never committed) - the old suite was unrunnable; see
  `docs/BASELINE_MANIFEST.md`.

## Weather timestamp semantics (UTC/DST correction)

- New weather archive rows use `source_model = "best_match_utc_v1"` and store
  the true UTC instant in `dim_weather.observation_time`. The airport's IANA
  `source_timezone` is retained for local display.
- Open-Meteo is requested with `timeformat=unixtime`; its `timezone` response
  offset is resolved once at request time and does not reconstruct historical
  DST across the requested range. With a London request made while the
  request-time offset is GMT, even a summer date can come back with UTC-day
  boundaries.
- To make coverage independent of that request-time offset, the archive fetch
  requests one padded day on each side (clamped to `ARCHIVE_MIN_DATE` and
  `date.today()`), then filters rows by the existing `time_local` field back
  to the logical `start_date..end_date`. Timestamps are never shifted and
  padded rows are never persisted. The exact-UTC-instant coverage gate still
  holds on any genuine missing observation.
- IEM visibility is requested with `tz=UTC` and merged by local hour plus UTC
  offset, so repeated fall-back hours remain distinct.
- Legacy `best_match` rows are left untouched and are never linker
  candidates. They retain their old fixed-offset labels.
- CSV and OLTP flight timestamp semantics are explicitly `unresolved` until a
  source contract is documented and verified. The linker therefore makes no
  proposals and updates no `weather_key` values by default.
- DST gaps and repeated local hours are held; no nearest-hour matching is
  used.
- Before the first UTC ingest, reconcile the weather watermark. Existing
  values are legacy fixed-offset local labels, so they are ahead of the
  equivalent UTC instant by that airport's UTC offset (up to 8 hours across
  the configured airports; CMB/DEL/BOM +5:30, DXB +4, DOH +3, SIN/KUL +8,
  LHR +0/+1). Watermark code converts a legacy local value to UTC for the
  no-rewind comparison once UTC rows exist, and stores the new value with a
  `Z` suffix. It still must not be rewritten blindly.
- An incremental first UTC run starts at the old local watermark minus the
  overlap, so it creates only a partial UTC history. For a full UTC backfill,
  set `weather_incremental.enabled=false` for that first run; re-enable it
  only after the UTC stage and verification succeed.
- Current schema limitation: there is no separate UTC/offset column. The
  versioned `source_model` separates legacy and UTC rows, but analytics that
  group by `observation_time` without `source_timezone` will see UTC wall
  times. Protected analytics files have not been modified.

## Historical backfill prerequisites

This remediation does **not** perform a historical weather backfill and does
not authorize one. Before any backfill:

1. Take and verify a database backup.
2. Confirm Power BI / reporting does not directly consume `dim_weather` in a
   way that assumes local wall-clock `observation_time`. Direct consumption
   remains **unverified**; the 25 protected queries are unchanged.
3. Establish and document authoritative CSV and OLTP flight timestamp
   conventions. They remain `unresolved`, so weather linking and
   `fact_flight.weather_key` updates remain deferred. Do not run
   `weather_linker.py --execute`.
4. Reconcile legacy local watermarks to UTC (or run with incremental mode
   disabled for the first full UTC pass) and review the expected per-airport
   date span.
5. Run the full test suite, query checksum, and warehouse verification; review
   the coverage report for every airport before advancing production
   watermarks.
6. Run the backfill as an explicit, separately approved operation - not as
   part of this code patch.

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
| 1 | `SELECT ... FROM etl_batch_log ORDER BY run_id DESC LIMIT 5` - read `error_detail`/`notes`; failed or `PARTIAL` stage is marked; rerun is safe (idempotent). |
| 2 | Check the repository venv/imports, `.env` connectivity, `--source` names, `config/refresh_policy.json`. |
| 3 | Find the holder: another orchestrator/terminal; wait or stop it. |
| 4 | Audit layer down (disk/privileges/table missing): fix MySQL access; nothing was executed. |
| 5 | Stage data may be committed while audit failed: inspect `etl_batch_log`, verify row counts, then rerun (idempotent). |
| 6 | Read the verification report; fix or, if intentional, re-baseline only after approval. |
| 130 | Operator interrupt (Ctrl-C). The stage audit row is FAILED with an interrupt note; committed stage data is not rolled back. Rerun is safe (idempotent). A still-open RUNNING row (interrupt outside a handled window) is reported as stale by verification. |
