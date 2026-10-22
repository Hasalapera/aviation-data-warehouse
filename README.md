# Aviation Data Warehouse

MySQL data warehouse for aviation analytics: historical CSV flights, an
Aiven-hosted OLTP flight-management source, historical weather archive,
and six dimensions/facts served to Power BI through 25 analytical queries
(`members/*.py`).

## Layout

| Path | Purpose |
|---|---|
| `sql/schema.sql` | Warehouse schema (6 tables, protected) |
| `etl.py`, `weather_etl.py`, `oltp_etl.py`, `main_etl.py` | Existing ETL stages (protected; `weather_etl.py` has approved additive, default-preserving incremental-window kwargs) |
| `members/*.py` | 25 immutable analytical queries (5 files x 5) |
| `refresh_orchestrator.py` | Refresh entry point: lock -> audit -> stages -> verify (see docs) |
| `verify_warehouse.py` | Read-only post-load verification |
| `etl_audit.py`, `sql/audit_schema.sql` | Audit log / run lock / watermarks (additive tables) |
| `oltp_change_updater.py` | Opt-in bounded OLTP change updates (`--enable-changes`) |
| `config/refresh_policy.json` | Refresh policy settings (incremental weather enabled; set false for full-range) |
| `scripts/query_checksum.py` | Before/after query SQL + result checksums |
| `scripts/scheduled_refresh.sh`, `scripts/launchd/` | Optional scheduling (disabled by default) |
| `tests/` | unittest suite (runs against disposable `aviation_dw_ci`) |
| `docs/` | Policy, runbook, baseline manifest |

## Quick start

```bash
python -m venv venv && venv/bin/pip install -r requirements.txt
# credentials come from .env (git-ignored): DW_* for MySQL, OLTP_* for Aiven
venv/bin/python setup_database.py          # create/load warehouse (existing tool)
venv/bin/python refresh_orchestrator.py --dry-run    # read-only preflight
venv/bin/python refresh_orchestrator.py              # audited refresh
venv/bin/python verify_warehouse.py                  # read-only checks
venv/bin/python -m unittest discover                 # test suite
```

## Documentation

- `docs/DATA_REFRESH_POLICY.md` - what is refreshed, how, and why
  (evidence included, e.g. why OLTP stays full-refresh).
- `docs/ETL_OPERATIONS.md` - runbook: commands, exit codes, logs,
  scheduled refresh, troubleshooting.
- `docs/BASELINE_MANIFEST.md` - Phase-0 protected-file hashes, query
  checksum baseline, database inventory.
