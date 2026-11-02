# Synthetic Passenger Counts for CSV Flights

## Status

The 500 CSV-sourced flights in `fact_flight` carry **synthetic enplaned
passenger counts**. These are generated values for analytical coursework and
are not observed bookings, check-ins, or aircraft load factors. They must not
be presented as real airline passenger data.

OLTP-sourced passenger counts are unchanged and remain derived from confirmed
bookings by the OLTP ETL.

## Metric definition

`passenger_count` for CSV rows means **synthetic enplaned passengers**:
passengers assumed to have boarded an operated flight. Cancelled flights are
zero. Diverted flights are treated as operated and receive a normal generated
value. Delayed flights are not adjusted.

## Approved generation configuration

```text
SEED       = aviation_dw_synthetic_pax_v1
LF_STEPS   = 55%, 60%, 65%, 70%, 75%, 80%, 85%, 90%
```

| Band | CSV `distance_km` | Nominal capacity | Generated values |
|---|---:|---:|---|
| Short | `< 3000` | 100 | 55, 60, 65, 70, 75, 80, 85, 90 |
| Medium | `3000–5999` | 297 | 163, 178, 193, 208, 223, 238, 252, 267 |
| Long | `>= 6000` | 396 | 218, 238, 257, 277, 297, 317, 337, 356 |

For each operated flight:

```text
digest    = SHA-256(f"{SEED}:{flight_id}")
fraction  = int(digest[:8], 16) / 2**32
load      = LF_STEPS[int(fraction * 8)]
passenger = floor(nominal_capacity * load + 0.5)
```

Cancelled flights are assigned `0` without using the generator.

The nominal capacities are **synthetic route-band assumptions**. They were
chosen as a plausible scale reference from OLTP aircraft metadata, not
verified assignments for any CSV flight. The load factors are likewise
assumptions, not observed load factors.

No relationship with weather, delay, time of day, or airline demand is
introduced.

## Dataset summary

- CSV rows: 500
- Short / medium / long: 217 / 219 / 64
- Cancelled CSV rows: 9, all zero
- Diverted CSV rows: 6, normal non-zero values
- Total synthetic passengers: 81,427
- Minimum / maximum: 0 / 356
- Distinct values: 24 including zero; 23 positive
- Warehouse CSV passenger distribution: query `fact_flight` in the
  `aviation_dw` database.

## Where the values live

1. `data/flight.csv` carries the `passenger_count` column appended as the
   final column. Existing columns, row order, and `flight_id` values are
   unchanged.
2. `etl.py` maps and validates the column for future CSV inserts.
3. Existing warehouse rows were updated by
   `scripts/backfill_csv_passengers.py`; this was a one-time bounded backfill,
   not a change to the CSV insert-only refresh policy.
4. `scripts/generate_synthetic_passengers.py` reproduces the CSV values from
   the seed and configuration.

## Backfill operation

```bash
venv/bin/python scripts/backfill_csv_passengers.py           # dry run
venv/bin/python scripts/backfill_csv_passengers.py --execute # apply
```

The execute path takes the shared ETL run lock, writes `csv` audit row
`notes` identifying the synthetic backfill, updates only `passenger_count`,
verifies before commit, and rolls back on any failed check. A second run
reports zero pending updates.

## Analytical consequences

- Five protected query results were intentionally re-recorded after the
  backfill: hasala Q1/Q3 and gihan Q22/Q23/Q24. All 25 SQL-text hashes are
  unchanged.
- Gihan Q24 groups by exact `passenger_count`; the discrete load-factor steps
  keep it to a manageable number of rows. The zero group contains cancelled
  CSV flights plus existing OLTP zero-passenger rows.
- CSV and OLTP passenger measures should be segmented by `source_system` in
  dashboards. Their scales are not comparable: CSV is synthetic
  commercial-scale, OLTP is the small observed test-scale fixture.
- `verify_warehouse.py` does not validate passenger-count realism. Validation
  is owned by the generator/backfill scripts and their checks.

## Labeling requirement

Any report or dashboard using CSV passenger measures must label them as
synthetic. `source_system='csv'` is the warehouse-level provenance marker.

## Rollback

Preferred narrow rollback:

```sql
UPDATE fact_flight f
JOIN fact_flight_backup_20261102_200940 b
  ON b.flight_key = f.flight_key
SET f.passenger_count = b.passenger_count
WHERE f.source_system = 'csv';
```

The snapshot above was created immediately before the approved backfill.
Revert the CSV/ETL/governance files to their pre-change versions as well.
