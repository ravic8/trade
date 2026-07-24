#!/usr/bin/env bash
set -euo pipefail

# Read-only Phase 0 audit for the Ubuntu production host.
#
# This script does not source, print, or copy the production env file. Docker
# Compose consumes it directly. Override the defaults only with explicit paths:
#
#   TASK_PROD_APP_DIR=/opt/trade/app \
#   TASK_PROD_ENV_FILE=/opt/trade/.env \
#   bash scripts/audit_production_readonly.sh

TASK_PROD_APP_DIR="${TASK_PROD_APP_DIR:-/opt/trade/app}"
TASK_PROD_ENV_FILE="${TASK_PROD_ENV_FILE:-/opt/trade/.env}"
TASK_PROD_COMPOSE_FILE="${TASK_PROD_COMPOSE_FILE:-docker-compose.prod.yml}"
TASK_PROD_ROOT="$(dirname "${TASK_PROD_APP_DIR}")"

if [[ ! -d "${TASK_PROD_APP_DIR}" ]]; then
  echo "Production app directory not found: ${TASK_PROD_APP_DIR}" >&2
  exit 2
fi
if [[ ! -f "${TASK_PROD_ENV_FILE}" ]]; then
  echo "Production env file not found: ${TASK_PROD_ENV_FILE}" >&2
  exit 2
fi
if [[ ! -f "${TASK_PROD_APP_DIR}/${TASK_PROD_COMPOSE_FILE}" ]]; then
  echo "Production Compose file not found." >&2
  exit 2
fi

cd "${TASK_PROD_APP_DIR}"

compose=(
  docker compose
  --env-file "${TASK_PROD_ENV_FILE}"
  -f "${TASK_PROD_COMPOSE_FILE}"
)

echo "audit_timestamp_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "host=$(hostname)"
echo "deployed_commit=$(git rev-parse HEAD)"
echo "branch=$(git branch --show-current)"
echo "worktree_changes=$(git status --short | wc -l | tr -d ' ')"

echo
echo "[compose services]"
"${compose[@]}" config --services

echo
echo "[compose ps]"
"${compose[@]}" ps

echo
echo "[compose images]"
"${compose[@]}" images

echo
echo "[alembic head]"
"${compose[@]}" exec -T api alembic current

echo
echo "[dagster schedules for current repository selector]"
"${compose[@]}" exec -T dagster-daemon \
  dagster schedule list -m trade_research.dagster.definitions

echo
echo "[dagster schedule storage debug]"
# `schedule list` alone can miss running records attached to an older
# repository-origin identifier after a deployment.
"${compose[@]}" exec -T dagster-daemon \
  dagster schedule debug

echo
echo "[dagster daemon recent logs]"
"${compose[@]}" logs --since 24h --no-color dagster-daemon | tail -n 500

echo
echo "[host capacity]"
df -h "${TASK_PROD_ROOT}"
du -sh "${TASK_PROD_ROOT}/data" "${TASK_PROD_ROOT}/backups" \
  "${TASK_PROD_ROOT}/dagster_home" 2>/dev/null || true

echo
echo "[latest backup timestamps]"
if [[ -d "${TASK_PROD_ROOT}/backups" ]]; then
  find "${TASK_PROD_ROOT}/backups" -maxdepth 1 -type f \
    -printf '%TY-%Tm-%TdT%TH:%TM:%TSZ %f\n' | sort | tail -n 10
else
  echo "No backup directory found."
fi

echo
echo "[database aggregates]"
"${compose[@]}" exec -T postgres sh -lc \
  'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
\pset pager off
\timing on

SELECT version_num AS alembic_version FROM alembic_version;

SELECT
    pg_size_pretty(pg_database_size(current_database())) AS database_size;

SELECT
    source,
    exchange,
    MIN(date) AS first_session,
    MAX(date) AS latest_session,
    COUNT(*) AS candle_rows,
    COUNT(DISTINCT symbol) AS symbols
FROM ohlcv_daily
GROUP BY source, exchange
ORDER BY source, exchange;

SELECT
    source,
    exchange,
    target_version,
    MAX(date) AS latest_session,
    COUNT(*) FILTER (
        WHERE date = (
            SELECT MAX(t2.date)
            FROM opportunity_targets_daily AS t2
            WHERE t2.source = opportunity_targets_daily.source
              AND t2.exchange = opportunity_targets_daily.exchange
              AND t2.target_version = opportunity_targets_daily.target_version
        )
    ) AS latest_session_rows
FROM opportunity_targets_daily
GROUP BY source, exchange, target_version
ORDER BY source, exchange, target_version;

WITH latest AS (
    SELECT
        source,
        exchange,
        target_version,
        MAX(date) AS latest_session
    FROM opportunity_targets_daily
    GROUP BY source, exchange, target_version
)
SELECT
    target.source,
    target.exchange,
    target.target_version,
    latest.latest_session,
    target.quality_status,
    COUNT(*) AS rows
FROM opportunity_targets_daily AS target
JOIN latest
  ON latest.source = target.source
 AND latest.exchange = target.exchange
 AND latest.target_version = target.target_version
 AND latest.latest_session = target.date
GROUP BY
    target.source,
    target.exchange,
    target.target_version,
    latest.latest_session,
    target.quality_status
ORDER BY target.source, target.exchange, target.target_version, target.quality_status;

SELECT
    provider,
    exchange,
    status,
    COUNT(*) AS work_items,
    COUNT(DISTINCT canonical_instrument_id) AS symbols,
    MAX(updated_at) AS latest_update
FROM pipeline_work_items
GROUP BY provider, exchange, status
ORDER BY provider, exchange, status;

SELECT
    source,
    exchange,
    status,
    COUNT(*) AS runs,
    SUM(items_processed) AS processed,
    SUM(items_succeeded) AS succeeded,
    SUM(items_failed) AS failed,
    MAX(started_at) AS latest_start
FROM ingestion_runs
WHERE started_at >= NOW() - INTERVAL '24 hours'
GROUP BY source, exchange, status
ORDER BY source, exchange, status;

SELECT DISTINCT ON (exchange)
    exchange,
    source,
    status,
    symbol_count,
    fetched_at,
    snapshot_id
FROM universe_snapshots
ORDER BY exchange, fetched_at DESC;

SELECT
    exchange,
    MAX(session_date) FILTER (WHERE is_trading_day) AS latest_materialized_open,
    MAX(generated_at) AS latest_generated_at,
    COUNT(*) AS rows
FROM exchange_sessions
GROUP BY exchange
ORDER BY exchange;

SELECT
    provider,
    credential_type,
    validation_status,
    last_validated_at,
    updated_at,
    updated_by
FROM provider_credentials
ORDER BY provider, credential_type;
SQL

echo
echo "Read-only audit complete. Review output for secrets before sharing."
