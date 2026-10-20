# Baseline Manifest (Phase 0)

Captured at the start of the Data Refresh Policy work. Everything below is
the **pre-existing state** that later phases must preserve.

## Repository state

| Item | Value |
|---|---|
| Branch | `hasala` |
| HEAD | `8f34392748170025204b2bfbb91d413e9b1f4975` (`8f34392 Improve historical weather ETL and database setup`) |
| Working tree at capture | clean (no modified tracked files) |
| Pre-existing tracked files | 16 |
| Commits made by this work | none (no commits until explicitly requested) |

## Protected analytical queries

25 queries (5 `QUERY_*` constants in each of 5 member files). SHA-256 of
each file, captured **before any change** and re-verified afterwards:

| File | SHA-256 |
|---|---|
| `members/hasala.py` | `0bc084a9694ec86c69233aab683d267da226004c64a5ba37e86b882bbaf8d816` |
| `members/gihan.py` | `f7964deed5e0b8d307f3518d7feb7e4cbe5a63da46019b60c26f63b77503eafb` |
| `members/Kavindu.py` | `485695f3febf362559227067e91338fc00dc99cbaedb297f17fd5ff082821617` |
| `members/navodya.py` | `a0f5ca72981ab234cd07514329d1a09b4d1e2a8b203121e84a6c94adfc0bd4a6` |
| `members/sadeepa.py` | `699a8f453e94e1f6f8373dae18021ec8246a760cef1e15b783d6139defe0890c` |

## Protected core files (byte-identical required)

| File | SHA-256 |
|---|---|
| `etl.py` | `bd661d805996205a22a9521f437225816891f3c7798cbcd45d383c3fd78d4e15` |
| `weather_etl.py` | `74455080579eccbeacd771f9f18e27f52312ead8244cfefd5f46d70800b0c61c`* |
| `oltp_etl.py` | `61ba95c058a9af8ae0543656a891bc6b74bb8e4ca58012aab51f785b436a9bed` |
| `main_etl.py` | `9fa85f43ac25bc42e50f509319945b1b3099823785623dfaa4c9b35cfc6691cc` |
| `setup_database.py` | `b1f89b6dcf04adf5d0cd16d6f1eda775836a8a8fd681f6dada987139ddbab05c` |
| `database.py` | `38023436e009b394d2b2847d5c409a2d3f910c31c82328fc99b84d032a455a5c` |
| `sql/schema.sql` | `00d518d875ed0db094d702bba7af1435fcc4b24ead435344cec34772744181b8` |
| `data/flight.csv` | `5838f3fee71d7adb43b815d50516561680491c8a0a9cebd9d316994279e9cdf1` |
| `requirements.txt` | `dbcffa093707d91ee50c23c43ed1e90c8c6d160ba21fa901e6bfb86548bc44d9` |
| `.gitignore` | `3a2fd2a67ee4ba95d67d7cc689e6effe5b26b0f1e5bd11cd77412ef0c1b5ebf9`* |

\* `.gitignore`: exactly one approved change - two appended lines
(`logs/`, `scripts/launchd/*.plist`) made when the log/scheduling
artifacts were introduced (P3). Everything else must match.

\* `weather_etl.py` is the **one** protected-adjacent file approved for an
additive, default-preserving edit in P2 (`start_date_override=None`
keyword). Final hash after the approved edit:
`c2c15658d52b7d320b3d5fa9a3d4e28791896b209998dce84cf1e362c7ecf83b`
(Phase-0 hash: `74455080...`; diff = kwarg + docstring + window raise,
behaviour unchanged for `start_date_override=None`). Every other file
above must match exactly.

## Query result baseline

`config/query_baseline.json` — produced by
`scripts/query_checksum.py --out` against the live warehouse. Contains, for
each of the 25 queries: SQL-text SHA-256, result SHA-256, row count, and
elapsed time. Re-check at the end of all phases with:

```bash
venv/bin/python scripts/query_checksum.py --compare config/query_baseline.json
```

A mismatch means an analytical result changed — an automatic fail of the
overall task.

## Warehouse (live MySQL, `aviation_dw`) at capture

| Table | Rows | Notes |
|---|---|---|
| `fact_flight` | 508 | 500 csv + 8 oltp source rows; 0 duplicate `(source_system, source_flight_id)`; 0 NULL source ids; unique index `uq_fact_source` |
| `dim_weather` | 86,636 | 86,064 API-identity rows + 572 legacy NULL-identity rows (permitted by design) |
| `dim_date` | 277 | |
| `dim_airline` | 10 | |
| `dim_airport` | 9 | |
| `dim_route` | 19 | |
| (backup snapshots) | 8 tables | created by `setup_database.py backup` earlier — pre-existing |
| events / routines / triggers / views | 0 / 0 / 0 / 0 | `event_scheduler=ON`, no scheduled events |

Schema snapshot of the 6 business tables (columns, types, nullability,
indexes, FKs): `config/schema_baseline.json`, written by
`verify_warehouse.py --write-baseline`. Verification check
`schema_baseline` diffs the live schema against this file using a
**semantic comparison** (constraint names and column order are ignored —
the live DB was created via a legacy/migrated path with auto-named
constraints, while `sql/schema.sql` names them explicitly).

Two pre-existing, documented deviations between `sql/schema.sql` and the
live warehouse (both tolerated explicitly in `verify_warehouse.py`;
everything else, including any new index or column, is drift and FAILs):

| Table | `sql/schema.sql` has | Live (Phase 0) has |
|---|---|---|
| `dim_airline` | `uq_airline_code` unique index | no index on `airline_code` (uniqueness only enforced by the ETL's existence check + `dimension_uniqueness` verification query) |
| `fact_flight` | `idx_fact_flight_number` | no index on `flight_number` |

`sql/schema.sql` is a protected file, so these were recorded, not fixed.

### Query-result baseline re-recording (tie-order, data unchanged)

One of the 25 baselined results — `members/Kavindu.py:QUERY_CANCELLATIONS_BY_ROUTE`
— had to be re-recorded in `config/query_baseline.json` on 2026-10-20. The
query ends with `ORDER BY cancelled_flights DESC LIMIT 10`, and the data has
ties both among the leading rows (one route with 2 cancellations, eight with
1) and at the LIMIT boundary (ten routes with 0 cancellations for the last
slot). After the additive audit DDL was applied, the chosen valid ordering
changed, so the result hash flipped while `rows` stayed 10 and the SQL hash
stayed identical.

The data did **not** change: the pre-change hash
`50083315c0418ea7a8485441068655f619fbe42f238e3eae50c09a2d73dd7d4a` was
reproduced from the *current* rows by exhaustively searching all 156,630
orderings that satisfy the query's `ORDER BY` (permutations of the eight tied
rows × ten candidate routes for the boundary slot). The only difference was
which count-0 route filled slot 10 (`SIN-CMB` before, `CMB-DXB` after).

The baseline entry keeps `previous_result_sha256` plus a `rerecorded` note.
A future mismatch confined to this query indicates tie ordering, not data —
re-verify by the same re-derivation (or compare the full 19-row grouped
aggregates), never by editing `members/*.py` to add a deterministic tie-break.

500 CSV rows carry a `weather_key` (legacy wiring from an earlier
iteration; current `weather_etl.py` never writes `fact_flight` —
"disabled this phase"). Preserved as-is. Note that the CSV stage
(`etl.py`) itself maintains legacy `dim_weather` rows (NULL identity) and
links them to facts — verified by `tests/test_etl_regression.py`.

## Aiven OLTP source (read-only verified)

- 8 flights, 4 airlines, 5 airports, 4 bookings, 3 delays, 1 cancellation.
- `flights.updated_at` is nullable, has **no** `ON UPDATE` clause and **no**
  maintaining trigger → unreliable watermark.
- `flight_delays` / `flight_cancellations` have **no** timestamp columns.
- Conclusion recorded in the policy doc: OLTP incremental extraction is
  **rejected with evidence**; full refresh is the policy.

## Additive objects introduced by this work (P0)

Created on the live warehouse (additive only, nothing dropped/altered):

- `etl_batch_log` — audit rows (`RUNNING/SUCCESS/FAILED/PARTIAL`,
  sources `csv/weather/oltp/verify`, CHECK-constrained).
- `etl_watermark` — per-source/per-window watermarks (used from P2).

Neither table is referenced by any analytical query, view or Power BI
asset. Smoke-test audit rows written while validating the orchestrator
remain in `etl_batch_log` as honest history.

## Pre-existing test-suite limitation (failure recorded, not hidden)

`tests/` contained **only stale `.pyc` files** — the test sources were
never committed. The `.pyc` artifacts reveal the previous pattern (a
disposable `aviation_dw_ci` database), but the sources are unrecoverable.
State at capture: **the pre-existing suite is unrunnable** — recorded as a
pre-existing failure. The new suite added by this work lives alongside the
`.pyc` files and forces `DW_NAME=aviation_dw_ci` unconditionally.
