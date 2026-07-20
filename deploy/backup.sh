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
require_command tar
require_file "$ENV_FILE"
require_file "$APP_DIR/docker-compose.prod.yml"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

BACKUP_ROOT="${TRADE_BACKUP_DIR:-${PROD_BACKUP_DIR:-/opt/trade/backups}}"
BACKUP_DIR="$BACKUP_ROOT/$STAMP"
mkdir -p "$BACKUP_DIR"
compose=(docker compose --env-file "$ENV_FILE" -f "$APP_DIR/docker-compose.prod.yml")

db_name="${PROD_POSTGRES_DB:-trade_research}"
db_user="${PROD_POSTGRES_USER:-trade}"

log "dumping postgres database $db_name"
"${compose[@]}" exec -T postgres pg_dump -U "$db_user" -d "$db_name" -Fc > "$BACKUP_DIR/postgres.dump"

tar_if_exists "${PROD_TRADE_DATA_DIR:-/opt/trade/data}" "data.tgz"
tar_if_exists "${PROD_TRADE_ARTIFACTS_DIR:-/opt/trade/artifacts}" "artifacts.tgz"
tar_if_exists "${PROD_QDRANT_DATA_DIR:-/opt/trade/qdrant}" "qdrant.tgz"
tar_if_exists "${PROD_DAGSTER_HOME_DIR:-/opt/trade/dagster_home}" "dagster_home.tgz"

cloudbeaver_was_running=false
if [[ -n "$("${compose[@]}" ps --status running -q cloudbeaver)" ]]; then
  cloudbeaver_was_running=true
  log "stopping CloudBeaver for a consistent workspace backup"
  "${compose[@]}" stop cloudbeaver
fi

restart_cloudbeaver() {
  if [[ "$cloudbeaver_was_running" == true ]]; then
    log "restarting CloudBeaver after workspace backup"
    "${compose[@]}" up -d --no-deps cloudbeaver
  fi
}
trap restart_cloudbeaver EXIT
tar_if_exists "${PROD_CLOUDBEAVER_WORKSPACE_DIR:-/opt/trade/cloudbeaver}" "cloudbeaver.tgz"
restart_cloudbeaver
cloudbeaver_was_running=false
trap - EXIT

cat > "$BACKUP_DIR/README.txt" <<'EOF'
This backup contains the Postgres database dump, the CloudBeaver workspace,
and selected persistent runtime directories. CloudBeaver is stopped briefly
while its workspace is archived. Provider credentials are stored encrypted in Postgres.
Restore of those credentials requires the matching PROD_APP_SECRET_KEY from the
server environment, which is intentionally not copied into this backup.
EOF

log "backup complete: $BACKUP_DIR"
