# Data Refresh Policy

Policy version 1 (`config/refresh_policy.json`). This document defines how
each data source in the Aviation Data Warehouse is refreshed, why the
approach was chosen, and the guarantees every refresh must uphold.

## Guarantees (non-negotiable)

1. The 25 analytical queries in `members/*.py` are immutable; their SQL
   text and (on unchanged data) their results are protected by
   `scripts/query_checksum.py --compare config/query_baseline.json`.
2. The warehouse schema is not migrated: the only objects ever added are
   the audit tables `etl_batch_log` / `etl_watermark` (see
   `sql/audit_schema.sql`). No existing table is altered, dropped or
   truncated by the refresh machinery.
3. Existing ETL code (`etl.py`, `weather_etl.py`, `oltp_etl.py`,
   `main_etl.py`) keeps its behaviour. Refresh orchestration wraps those
   entry points; it never reimplements extract/transform/load logic.
   `weather_etl.py` received additive, default-preserving incremental-window
   keywords (`start_date_override=None`, `start_date_overrides=None`) and the
   separately reviewed UTC/DST storage correction (UTC archive rows under
   `source_model=best_match_utc_v1`, UTC IEM requests, exact-only matching).
4. Every stage attempt leaves an audit row, even when the stage
   transaction rolls back (audit writes use a separate autocommit
   session).
5. A run that cannot write its audit trail is never reported successful
   (fail-closed exit codes 4/5).
6. The Aiven OLTP source is read-only for the warehouse's ETL and for
   `oltp_change_updater.py` (SELECT only - no code path writes to any
   source database). The only writes the updater performs are bounded
   UPDATEs to the **warehouse** (`fact_flight`), and only when explicitly
   enabled: via `refresh_orchestrator.py --enable-changes` (run lock +
   audit row + post-load verification) or the standalone CLI with an
   explicit `--execute` flag (run lock + audit row + the same read-only
   post-update verification; SUCCESS is reported only when verification
   passes, and the CLI is a dry run by default). Tests exercise the
   updater against a local fixture source/target; the live source is
   never written.

## Per-source policy

| Source | Method | Incremental? | Rationale |
|---|---|---|---|
| CSV (`data/flight.csv`) | Full load, **insert-only** | No | Static snapshot file, no change feed. Existing rows are skipped by identity `(source_system, source_flight_id)`; nothing is ever updated or deleted (`etl.py` skip-if-exists). Re-running is a no-op (proven by `tests/test_etl_regression.py`). |
| Weather archive | Full range on first run, then **incremental window** (enabled; opt-out) | Yes | 86k+ observations; incremental needs overlap to catch late corrections. Design below. |
| OLTP (Aiven `flights`) | **Full refresh, insert-only** | No | Incremental was evaluated and **rejected with evidence** (below). |

### OLTP incremental: rejected with evidence

Measured on the live source (recorded in `docs/BASELINE_MANIFEST.md`):

- `flights.updated_at` is nullable, has **no** `ON UPDATE CURRENT_TIMESTAMP`
  clause and **no** maintaining trigger -> it does not reliably change when
  a row changes, so a watermark on it would silently miss updates.
- `flight_delays` and `flight_cancellations` have **no timestamp columns
  at all** -> there is no key or column set that can serve as a change
  window for those facts.
- The warehouse loader is insert-only by design (skip-if-exists), so even
  with a perfect watermark the current load path would not apply updates.

Conclusion: OLTP is refreshed fully and idempotently. A bounded,
opt-in change updater (`--enable-changes`) exists for operators who need
mutable-attribute corrections (P2, see below); deletion sync is explicitly
out of policy.

### Weather incremental design (enabled by policy)

Config: `weather_incremental = {"enabled": true, "overlap_days": 7}`.
`overlap_days` must be an integer `>= 0` and `enabled` must be a real
JSON boolean; invalid values fail config loading (exit `2`) instead of
silently using a different window or accidentally activating the mode.

- Watermarks are per origin airport, stored in `etl_watermark`
  (`window_key = airport_code`, `watermark_value = MAX(observation_time)`
  within the validated window). Legacy values are naive airport-local
  labels; after an airport has UTC-model observations, values are stored as
  UTC instants with a trailing `Z` so the basis is never guessed.
- Fetch start is computed **per airport** from that airport's own
  watermark: `watermark_date - overlap_days`, then raised to the airport's
  first flight and the archive's earliest available day. One airport's
  watermark never shortens another airport's window.
- An airport with no watermark (first run, newly added airport, or an
  unreadable watermark) is absent from the per-airport override map and
  keeps its **full available history**. This is deliberately safe: the
  first run establishes watermarks, and recovery never relies on a guessed
  partial range.
- Fetch end per airport is `min(airport's last flight date, archive's
  latest available day)`. Flights outside archive coverage (e.g. future
  flights) never inflate a watermark.
- Watermarks advance **only after** the weather stage completed as
  `SUCCESS` **and** post-load verification actually ran and did not fail.
  A `PARTIAL` or `FAILED` weather stage, a failed verification, or skipped
  verification (`--no-verify` / `run_verification=false`) advances **no**
  weather watermark.
- Advancement is independently re-checked against persisted data, not the
  stage's exit code or its requested range. For each airport, every
  calendar day in
  `[max(first flight, watermark - overlap) .. min(last flight, archive max)]`
  must have a persisted `open-meteo-archive` observation. The new
  watermark is the maximum observation timestamp **inside that window**
  (never a stale row beyond the expected end).
- Coverage is model-aware. Once an airport has any
  `best_match_utc_v1` row, only UTC-model rows can satisfy its coverage;
  legacy `best_match`/reanalysis labels cannot mask a UTC gap. Before that
  first UTC row, the legacy models remain the coverage basis unchanged.
- A candidate older than the stored watermark is rejected, so a shrinking
  flight window can never rewind a watermark. When UTC-model coverage is
  present, a legacy local watermark is converted to UTC for this comparison
  (a DST-ambiguous legacy value is held rather than guessed). Airports
  failing any part of this check keep their old watermark, so the next run
  re-covers them (self-healing).
- Incremental mode changes only *which window* is fetched; the upsert,
  chunking, retries and dry-run behaviour of `weather_etl.py` are
  untouched. With incremental disabled, no override is passed and the
  original full-range fetch is used exactly.
- Known limitation: completeness is proven at calendar-day granularity
  (plus the window's latest timestamp). Interior missing hours are not
  individually detected, and detection of a failed airport inside a
  successful stage still relies on the stage's summary line
  (`airports failed : [...]`); if that format changes, the independent
  coverage check remains the safety net and withholds incomplete airports.

Current rollout: `weather_incremental.enabled` is **true** in the shipped
policy after the safety audit. The rules above are covered by the full CI
suite, including mocked end-to-end upsert/rollback tests, and read-only
Open-Meteo/IEM validation succeeded for all eight configured airports
(CMB, DXB, SIN, LHR, KUL, DEL, DOH, BOM; 2024-06-01..03). A first
production run with no existing watermarks still fetches full history and
then establishes per-airport watermarks. Existing legacy watermarks are
local labels, so an incremental first UTC run only covers their overlap
window; set `weather_incremental.enabled=false` for a full UTC backfill.
After that succeeds, later runs can re-enable incremental mode. To roll
back, set `weather_incremental.enabled=false` (see
`docs/ETL_OPERATIONS.md`).

### OLTP bounded change updater (opt-in, default OFF)

`python refresh_orchestrator.py --enable-changes` runs
`oltp_change_updater.py` **after a successful OLTP stage**:

- Updates a fixed mutable set on **existing OLTP fact rows only**:
  `delay_minutes`, `passenger_count`, `cancelled_flag`,
  `departure_time`, `arrival_time`, `flight_duration_minutes`.
- Excluded on purpose: `arrival_delay_minutes` (loader hardcodes 0),
  schedule/route/airline keys (they move row identity), and anything the
  insert-only stage owns.
- Never inserts, never deletes. Identity stays `(source_system,
  source_flight_id)` and `uq_fact_source` still applies.
- Correctness is proven two ways: a convergence test (fresh load then
  updater updates 0 rows) and a writable local fixture source in tests -
  the live Aiven source is never written to.
- Updater failure after committed inserts marks the OLTP stage FAILED
  (exit 1) and states that insert-only state is still valid.

Standalone CLI (`python oltp_change_updater.py`) is fail-closed:

- Default (no flags) is a **dry run**: it reports pending changes and
  writes nothing (no fact UPDATEs, no audit rows).
- Real updates require an explicit `--execute` flag. `--execute` runs
  the complete lifecycle: the same MySQL run lock as the orchestrator
  (a held lock refuses with exit 3 and applies nothing), an audit row
  (`source='oltp'`), the bounded updates, then the same read-only
  post-update verification the orchestrator runs. SUCCESS (exit 0) is
  reported only when verification passes; a verification failure exits
  6, closes the audit row as FAILED, and states explicitly that the
  committed updates are **not** rolled back.
- The orchestrator path (`--enable-changes`) remains recommended: it
  also runs the OLTP load and verification in one audited sequence, and
  `--enable-changes` requires the `oltp` stage to be selected (a
  misconfiguration prints a loud warning instead of silently no-oping).
- Take a backup first (`venv/bin/python setup_database.py backup`).

## Audit trail

Table `etl_batch_log` (additive; no analytical query references it).

- One row per stage attempt: `RUNNING -> SUCCESS | FAILED | PARTIAL`.
- Sources: `csv`, `weather`, `oltp`, `verify`.
- Metrics: `rows_read`, `rows_before`, `rows_after`, `rows_inserted`,
  `rows_updated`, `rows_skipped` (NULL means "not measurable honestly" -
  e.g. weather has no single countable input table and its upserts are not
  distinguishable from inserts without instrumenting the stage).
- `PARTIAL` is weather-specific: the stage itself reported
  `airports failed : [...]` while exiting successfully.
- `error_detail` and `notes` are sanitised (`etl_audit.sanitize_error`):
  password/URL-credential fragments are replaced with `***`, truncated to
  4000 chars.
- A `RUNNING` row older than `stale_running_minutes` (240) is reported as
  a **WARN** by verification and is never mutated by any component.

## Concurrency

`GET_LOCK('aviation_dw.etl_refresh', timeout)` held for the whole run.
A second run exits `3` before executing any stage. `main_etl.py` does not
take this lock (it is unmodified by policy); run one or the other, not
both concurrently.

## Verification (post-load, read-only)

`verify_warehouse.py` checks (PASS/WARN/FAIL/SKIP):

| Check | FAIL means |
|---|---|
| `tables_present` | a business table is missing |
| `schema_baseline` | semantic schema drift vs `config/schema_baseline.json` (constraint names/column order ignored; 2 documented index deviations allowed) |
| `no_unexpected_db_objects` | a view/routine/trigger/event appeared (baseline: none) |
| `fact_source_identity_unique` | duplicate `(source_system, source_flight_id)` |
| `fact_required_keys` | NULL `date_key`/`airline_key`/`route_key` |
| `fact_foreign_keys` | dangling FK reference (NULL `weather_key` allowed) |
| `dimension_uniqueness` | duplicate business keys in a dimension |
| `row_count_not_decreased[*]` | row count dropped below last audited value |
| `weather_key_policy` / `fact_identity_nulls` / `dim_weather_identity` / `row_counts` / `audit_*` | WARN-only conditions by design |

WARN is never mutated or "repaired" - the tool has no write code path.

## Exit codes (`refresh_orchestrator.py`)

| Code | Meaning |
|---|---|
| 0 | all required stages SUCCESS, audit trail complete, verification passed/skipped |
| 1 | a REQUIRED stage ended FAILED/PARTIAL |
| 2 | preflight/configuration/connectivity failure |
| 3 | another run holds the lock |
| 4 | audit layer unavailable **before** a stage (stage not executed) |
| 5 | audit/watermark write failed **after** stage execution (data may be committed) |
| 6 | stages succeeded but post-load verification did not pass |
| 130 | interrupted by the operator (Ctrl-C); any still-open audit row is closed as FAILED, committed stage data is not rolled back |

Failure policy: continue-on-stage-failure (sources are independent and
loads are idempotent), matching `main_etl.py`; the exit code still reports
any required-stage failure, so exit 0 is never returned for a run that had
a required-stage or audit-trail failure.

## Security and data handling

- No credentials in logs or audit rows (sanitisation above; `.env` is
  git-ignored; tests never print connection strings).
- Audit connection = separate autocommit session, same warehouse
  credentials as the ETL.
- The Aiven source is used read-only via the existing `database.py`
  connection (TLS with `ca.pem`).
