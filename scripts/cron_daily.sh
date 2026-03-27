#!/usr/bin/env bash
# Stats Bluenoser — daily pipeline cron wrapper
#
# Crontab entry (08:31 ET weekdays):
#   31 8 * * 1-5 /path/to/stats-bluenoser/scripts/cron_daily.sh >> /path/to/stats-bluenoser/logs/pipeline.log 2>&1
#
# On macOS, use America/Halifax (AT) which is ET+1 during standard time:
#   CRON_TZ=America/Toronto
#   31 8 * * 1-5 /path/to/stats-bluenoser/scripts/cron_daily.sh >> /path/to/stats-bluenoser/logs/pipeline.log 2>&1

set -euo pipefail

# Resolve project root (parent of scripts/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load environment variables
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
fi

# Ensure logs directory exists
mkdir -p "$PROJECT_DIR/logs"

# Timestamp
echo ""
echo "=========================================="
echo "  Pipeline run: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "=========================================="

# Run the pipeline
cd "$PROJECT_DIR"
python3 -m pipeline.run_daily "$@"

EXIT_CODE=$?
echo "Exit code: $EXIT_CODE"
exit $EXIT_CODE
