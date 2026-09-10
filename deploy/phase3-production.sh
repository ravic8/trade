#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${TRADE_APP_DIR:-/opt/trade/app}"
ENV_FILE="${TRADE_ENV_FILE:-/opt/trade/.env}"
OPERATION="${1:-status}"

log() {
  printf '[phase3-production] %s\n' "$*"
}

fail() {
  printf '[phase3-production] %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

load_environment() {
  [[ -f "$ENV_FILE" ]] || fail "production environment file is missing: $ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
}

is_secure_value() {
  local value="$1"
  [[ -n "$value" && "$value" != replace-* && "$value" != "minioadmin" ]]
}

set_environment_value() {
  local key="$1"
  local value="$2"
  local directory temporary
  directory="$(dirname "$ENV_FILE")"
  temporary="$(mktemp "$directory/.phase3-env.XXXXXX")"
  chmod 0600 "$temporary"
  awk -v key="$key" -v value="$value" '
    BEGIN { replaced = 0 }
    index($0, key "=") == 1 {
      if (!replaced) {
        print key "=" value
        replaced = 1
      }
      next
    }
    { print }
    END {
      if (!replaced) print key "=" value
    }
  ' "$ENV_FILE" > "$temporary"
  mv "$temporary" "$ENV_FILE"
  printf -v "$key" '%s' "$value"
  export "$key"
}

ensure_generated_secret() {
  local key="$1"
  local kind="$2"
  local current="${!key:-}"
  if is_secure_value "$current"; then
    return
  fi
  local generated
  if [[ "$kind" == "identity" ]]; then
    generated="trade-$(openssl rand -hex 12)"
  else
    generated="$(openssl rand -hex 32)"
  fi
  set_environment_value "$key" "$generated"
  log "generated missing credential for $key"
}

backup_environment() {
  local backup_root timestamp backup_path
  backup_root="${PROD_DEPLOY_STATE_DIR:-/opt/trade/deploy-state}/phase3-env-backups"
  install -m 700 -d "$backup_root"
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  backup_path="$backup_root/production.env.$timestamp"
  install -m 600 "$ENV_FILE" "$backup_path"
  log "saved recoverable production environment backup at $backup_path"
}

credential_status() {
  local key current="${!1:-}"
  key="$1"
  if is_secure_value "$current"; then
    printf '%s=ready\n' "$key"
  else
    printf '%s=missing\n' "$key"
  fi
}

print_status() {
  load_environment
  printf 'PROD_MINIO_KMS_ENABLED=%s\n' "${PROD_MINIO_KMS_ENABLED:-false}"
  printf 'PROD_SELF_MANAGED_KES_ENABLED=%s\n' \
    "${PROD_SELF_MANAGED_KES_ENABLED:-false}"
  printf 'PROD_RESEARCH_STORAGE_DEPLOY_ENABLED=%s\n' \
    "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}"
  printf 'PROD_RESEARCH_STORAGE_ENABLED=%s\n' \
    "${PROD_RESEARCH_STORAGE_ENABLED:-false}"
  printf 'PROD_PHASE3_MARKET_DATA_ENABLED=%s\n' \
    "${PROD_PHASE3_MARKET_DATA_ENABLED:-false}"
  printf 'PROD_YFINANCE_NSE_MINUTE_ENABLED=%s\n' \
    "${PROD_YFINANCE_NSE_MINUTE_ENABLED:-false}"
  printf 'PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED=%s\n' \
    "${PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED:-false}"
  credential_status PROD_CLICKHOUSE_ADMIN_PASSWORD
  credential_status PROD_CLICKHOUSE_MIGRATION_PASSWORD
  credential_status PROD_CLICKHOUSE_DAGSTER_PASSWORD
  credential_status PROD_CLICKHOUSE_API_PASSWORD
  credential_status PROD_CLICKHOUSE_ANALYST_PASSWORD
  credential_status PROD_OBJECT_STORE_DAGSTER_ACCESS_KEY_ID
  credential_status PROD_OBJECT_STORE_DAGSTER_SECRET_ACCESS_KEY
  credential_status PROD_OBJECT_STORE_API_ACCESS_KEY_ID
  credential_status PROD_OBJECT_STORE_API_SECRET_ACCESS_KEY
}

disable_application_plane() {
  set_environment_value PROD_RESEARCH_STORAGE_ENABLED false
  set_environment_value PROD_PHASE3_MARKET_DATA_ENABLED false
  set_environment_value PROD_YFINANCE_NSE_MINUTE_ENABLED false
  set_environment_value PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED false
}

bootstrap_canary_plane() {
  load_environment
  require_command openssl
  require_command awk
  require_command install
  backup_environment

  if [[ "${PROD_MINIO_KMS_ENABLED:-false}" != "true" ]]; then
    log "enabling repository-managed private KES for Phase 3 canary storage"
    set_environment_value PROD_SELF_MANAGED_KES_ENABLED true
    set_environment_value PROD_MINIO_KMS_ENABLED true
  fi
  if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" == "true" ]]; then
    set_environment_value PROD_MINIO_KMS_SERVER "${PROD_MINIO_KMS_SERVER:-https://kes:7373}"
    set_environment_value PROD_MINIO_KMS_ENCLAVE "${PROD_MINIO_KMS_ENCLAVE:-trade-production}"
    set_environment_value PROD_MINIO_KMS_SSE_KEY "${PROD_MINIO_KMS_SSE_KEY:-trade-research-sse}"
  fi
  for key in \
    PROD_MINIO_KMS_SERVER \
    PROD_MINIO_KMS_ENCLAVE \
    PROD_MINIO_KMS_SSE_KEY; do
    is_secure_value "${!key:-}" || fail "$key is not securely configured"
  done
  if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" != "true" ]]; then
    is_secure_value "${PROD_MINIO_KMS_API_KEY:-}" || \
      fail "PROD_MINIO_KMS_API_KEY is not securely configured"
  fi

  ensure_generated_secret PROD_CLICKHOUSE_ADMIN_PASSWORD password
  ensure_generated_secret PROD_CLICKHOUSE_MIGRATION_PASSWORD password
  ensure_generated_secret PROD_CLICKHOUSE_DAGSTER_PASSWORD password
  ensure_generated_secret PROD_CLICKHOUSE_API_PASSWORD password
  ensure_generated_secret PROD_CLICKHOUSE_ANALYST_PASSWORD password
  ensure_generated_secret PROD_OBJECT_STORE_DAGSTER_ACCESS_KEY_ID identity
  ensure_generated_secret PROD_OBJECT_STORE_DAGSTER_SECRET_ACCESS_KEY password
  ensure_generated_secret PROD_OBJECT_STORE_API_ACCESS_KEY_ID identity
  ensure_generated_secret PROD_OBJECT_STORE_API_SECRET_ACCESS_KEY password

  log "staging private ClickHouse and object-storage services"
  disable_application_plane
  set_environment_value PROD_RESEARCH_STORAGE_DEPLOY_ENABLED true
  "$APP_DIR/deploy/deploy.sh"

  log "enabling Phase 3 canary data plane with production activation disabled"
  set_environment_value PROD_RESEARCH_STORAGE_ENABLED true
  set_environment_value PROD_PHASE3_MARKET_DATA_ENABLED true
  set_environment_value PROD_YFINANCE_NSE_MINUTE_ENABLED true
  set_environment_value PROD_PHASE3_PRODUCTION_ACTIVATION_ENABLED false
  if ! "$APP_DIR/deploy/deploy.sh"; then
    log "canary-plane deployment failed; restoring fail-closed application flags"
    disable_application_plane
    "$APP_DIR/deploy/deploy.sh" || true
    fail "Phase 3 canary-plane deployment failed"
  fi
  log "Phase 3 canary data plane is enabled; scheduled production activation remains off"
  print_status
}

print_readiness() {
  load_environment
  [[ "${PROD_RESEARCH_STORAGE_ENABLED:-false}" == "true" ]] || \
    fail "Phase 3 research storage application access is disabled"
  local -a compose=(
    docker compose
    --env-file "$ENV_FILE"
    -f "$APP_DIR/docker-compose.prod.yml"
  )
  if [[ "${PROD_MINIO_KMS_ENABLED:-false}" == "true" ]]; then
    if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" == "true" ]]; then
      compose+=(-f "$APP_DIR/docker-compose.prod.managed-kes.yml")
    else
      compose+=(-f "$APP_DIR/docker-compose.prod.kms.yml")
    fi
  fi
  compose+=(--profile research)
  set +e
  "${compose[@]}" run --rm --no-deps api trade-research phase3-readiness
  local status=$?
  set -e
  if [[ "$status" -ne 0 ]]; then
    log "readiness remains blocked; no activation setting was changed"
  fi
}

case "$OPERATION" in
  status)
    print_status
    ;;
  bootstrap-canary)
    bootstrap_canary_plane
    ;;
  readiness)
    print_readiness
    ;;
  *)
    fail "unsupported operation: $OPERATION"
    ;;
esac
