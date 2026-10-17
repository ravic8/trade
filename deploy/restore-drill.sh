#!/usr/bin/env bash
set -Eeuo pipefail
APP_DIR="${TRADE_APP_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
exec python3 "$APP_DIR/scripts/restore_daily_backup.py" "$@"
