#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${TRADE_APP_DIR:-/opt/trade/app}"
ENV_FILE="${TRADE_ENV_FILE:-/opt/trade/.env}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

log() {
  printf '[trade-backup] %s\n' "$*"
}

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    printf '[trade-backup] missing required file: %s\n' "$path" >&2
    exit 1
  fi
}

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    printf '[trade-backup] missing required command: %s\n' "$name" >&2
    exit 1
  fi
}

tar_if_exists() {
  local source_path="$1"
  local archive_name="$2"
  if [[ -d "$source_path" ]]; then
    log "archiving $source_path"
    tar -C "$(dirname "$source_path")" -czf "$BACKUP_DIR/$archive_name" "$(basename "$source_path")"
  else
    log "skipping missing directory $source_path"
  fi
}

require_command docker
require_command sha256sum
require_command tar
require_file "$ENV_FILE"
require_file "$APP_DIR/docker-compose.prod.yml"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

BACKUP_ROOT="${TRADE_BACKUP_DIR:-${PROD_BACKUP_DIR:-/opt/trade/backups}}"
FINAL_BACKUP_DIR="$BACKUP_ROOT/$STAMP"
BACKUP_DIR="$BACKUP_ROOT/.incomplete-$STAMP"
if [[ -e "$BACKUP_DIR" || -e "$FINAL_BACKUP_DIR" ]]; then
  printf '[trade-backup] backup path already exists for stamp: %s\n' "$STAMP" >&2
  exit 1
fi
mkdir -p "$BACKUP_DIR"
compose=(docker compose --env-file "$ENV_FILE" -f "$APP_DIR/docker-compose.prod.yml")

db_name="${PROD_POSTGRES_DB:-trade_research}"
db_user="${PROD_POSTGRES_USER:-trade}"

filing_worker_was_running=false
dagster_daemon_was_running=false
dagster_webserver_was_running=false
cloudbeaver_was_running=false
qdrant_was_running=false
minio_was_running=false
clickhouse_was_running=false

restart_quiesced_services() {
  local status=0
  set +e
  if [[ "$clickhouse_was_running" == true ]]; then
    log "restarting ClickHouse after backup"
    if "${compose[@]}" --profile research up -d --no-deps clickhouse; then
      clickhouse_was_running=false
    else
      status=$?
    fi
  fi
  if [[ "$minio_was_running" == true ]]; then
    log "restarting MinIO after backup"
    if "${compose[@]}" up -d --no-deps minio; then
      minio_was_running=false
    else
      status=$?
    fi
  fi
  if [[ "$qdrant_was_running" == true ]]; then
    log "restarting Qdrant after backup"
    if "${compose[@]}" up -d --no-deps qdrant; then
      qdrant_was_running=false
    else
      status=$?
    fi
  fi
  if [[ "$filing_worker_was_running" == true ]]; then
    log "restarting filing worker after backup"
    if "${compose[@]}" up -d filing-worker; then
      filing_worker_was_running=false
    else
      status=$?
    fi
  fi
  if [[ "$dagster_daemon_was_running" == true ]]; then
    log "restarting Dagster daemon after backup"
    if "${compose[@]}" up -d dagster-daemon; then
      dagster_daemon_was_running=false
    else
      status=$?
    fi
  fi
  if [[ "$dagster_webserver_was_running" == true ]]; then
    log "restarting Dagster webserver after backup"
    if "${compose[@]}" up -d dagster-webserver; then
      dagster_webserver_was_running=false
    else
      status=$?
    fi
  fi
  if [[ "$cloudbeaver_was_running" == true ]]; then
    log "restarting CloudBeaver after backup"
    if "${compose[@]}" up -d --no-deps cloudbeaver; then
      cloudbeaver_was_running=false
    else
      status=$?
    fi
  fi
  set -e
  return "$status"
}

if [[ -n "$("${compose[@]}" ps --status running -q filing-worker)" ]]; then
  log "stopping filing worker before backup"
  "${compose[@]}" stop filing-worker
  filing_worker_was_running=true
fi
if [[ -n "$("${compose[@]}" ps --status running -q dagster-daemon)" ]]; then
  log "stopping Dagster daemon before backup"
  "${compose[@]}" stop dagster-daemon
  dagster_daemon_was_running=true
fi
if [[ -n "$("${compose[@]}" ps --status running -q dagster-webserver)" ]]; then
  log "stopping Dagster webserver before backup"
  "${compose[@]}" stop dagster-webserver
  dagster_webserver_was_running=true
fi
if [[ -n "$("${compose[@]}" ps --status running -q cloudbeaver)" ]]; then
  log "stopping CloudBeaver before backup"
  "${compose[@]}" stop cloudbeaver
  cloudbeaver_was_running=true
fi
if [[ -n "$("${compose[@]}" ps --status running -q qdrant)" ]]; then
  log "stopping Qdrant before backup"
  "${compose[@]}" stop qdrant
  qdrant_was_running=true
fi
if [[ -n "$("${compose[@]}" ps --status running -q minio)" ]]; then
  log "stopping MinIO before backup"
  "${compose[@]}" stop minio
  minio_was_running=true
fi
if [[ -n "$("${compose[@]}" --profile research ps --status running -q clickhouse)" ]]; then
  log "stopping ClickHouse before backup"
  "${compose[@]}" --profile research stop clickhouse
  clickhouse_was_running=true
fi

trap restart_quiesced_services EXIT

log "dumping postgres database $db_name"
"${compose[@]}" exec -T postgres pg_dump -U "$db_user" -d "$db_name" -Fc > "$BACKUP_DIR/postgres.dump"

tar_if_exists "${PROD_TRADE_DATA_DIR:-/opt/trade/data}" "data.tgz"
tar_if_exists "${PROD_TRADE_ARTIFACTS_DIR:-/opt/trade/artifacts}" "artifacts.tgz"
tar_if_exists "${PROD_QDRANT_DATA_DIR:-/opt/trade/qdrant}" "qdrant.tgz"
tar_if_exists "${PROD_DAGSTER_HOME_DIR:-/opt/trade/dagster_home}" "dagster_home.tgz"
tar_if_exists "${PROD_MINIO_DATA_DIR:-/opt/trade/minio}" "minio.tgz"
if [[ "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" == true \
  || "$clickhouse_was_running" == true ]]; then
  tar_if_exists "${PROD_CLICKHOUSE_DATA_DIR:-/opt/trade/clickhouse}" "clickhouse.tgz"
else
  log "skipping ClickHouse archive because Phase 2 deployment is disabled"
fi
tar_if_exists "${PROD_CLOUDBEAVER_WORKSPACE_DIR:-/opt/trade/cloudbeaver}" "cloudbeaver.tgz"

cat > "$BACKUP_DIR/README.txt" <<'EOF'
This backup contains the Postgres database dump, the versioned MinIO filing
and research object stores, ClickHouse analytical storage when present, the
CloudBeaver workspace, and selected persistent runtime directories. Mutable
workflow, analytical, vector-store, object-store, and workspace services are
stopped briefly while their state is archived. Provider
credentials are stored encrypted in Postgres. Restore of those credentials
requires the matching PROD_APP_SECRET_KEY from the server environment, which
is intentionally not copied into this backup. Encrypted MinIO objects likewise
require the matching external KMS configuration, which is not copied here.
EOF

(
  cd "$BACKUP_DIR"
  sha256sum postgres.dump > SHA256SUMS
  for archive in *.tgz; do
    [[ -e "$archive" ]] || continue
    sha256sum "$archive" >> SHA256SUMS
  done
)

restart_quiesced_services
trap - EXIT

mv "$BACKUP_DIR" "$FINAL_BACKUP_DIR"
log "backup complete: $FINAL_BACKUP_DIR"
