#!/usr/bin/env bash
#
# Scheduled refresh wrapper (P3).
#
# Runs refresh_orchestrator.py under a timestamped log file and forwards
# the policy exit code (see docs/ETL_OPERATIONS.md). Concurrency is
# already handled inside the orchestrator via the MySQL run lock - a
# scheduled run that collides with a manual one exits 3 (locked) instead
# of doing anything.
#
# NOT enabled by default: nothing invokes this script until an operator
# installs a schedule (see scripts/launchd/*.plist.template).
#
# Usage:
#   scripts/scheduled_refresh.sh                 # full pipeline + verify
#   scripts/scheduled_refresh.sh --dry-run       # preflight only
#   scripts/scheduled_refresh.sh --source weather
#
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$REPO/venv/bin/python"
LOG_DIR="$REPO/logs"
export ETL_LOG_RETENTION_DAYS="${ETL_LOG_RETENTION_DAYS:-30}"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/scheduled_refresh_${STAMP}.log"

mkdir -p "$LOG_DIR"
cd "$REPO" || exit 2

if [[ ! -x "$PYTHON" ]]; then
    echo "[scheduled_refresh] required venv python not found: $PYTHON" | tee "$LOG_FILE"
    exit 2
fi

if ! "$PYTHON" -c 'import pymysql, sqlalchemy, pandas, dotenv, requests'         >> "$LOG_FILE" 2>&1; then
    echo "[scheduled_refresh] venv is missing required modules; run: $PYTHON -m pip install -r requirements.txt" | tee -a "$LOG_FILE"
    exit 2
fi
{
    echo "=== scheduled_refresh $(date '+%Y-%m-%d %H:%M:%S') ==="
    echo "args: $*"
} >> "$LOG_FILE"

if [[ $# -gt 0 ]]; then
    "$PYTHON" refresh_orchestrator.py "$@" >> "$LOG_FILE" 2>&1
else
    "$PYTHON" refresh_orchestrator.py >> "$LOG_FILE" 2>&1
fi
CODE=$?

{
    echo "=== exit code: $CODE ==="
    echo
} >> "$LOG_FILE"

exit "$CODE"
