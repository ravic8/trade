#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${TRADE_APP_DIR:-/opt/trade/app}"
ENV_FILE="${TRADE_ENV_FILE:-/opt/trade/.env}"
BRANCH="${TRADE_DEPLOY_BRANCH:-main}"

log() {
  printf '[trade-deploy] %s\n' "$*"
}

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    printf '[trade-deploy] missing required file: %s\n' "$path" >&2
    exit 1
  fi
}

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    printf '[trade-deploy] missing required command: %s\n' "$name" >&2
    exit 1
  fi
}

mkdir_from_var() {
  local value="$1"
  if [[ -n "$value" ]]; then
    mkdir -p "$value"
  fi
}

require_command git
require_command docker
require_command curl
require_file "$ENV_FILE"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

require_file "$APP_DIR/docker-compose.prod.yml"

mkdir_from_var "${PROD_TRADE_DATA_DIR:-/opt/trade/data}"
mkdir_from_var "${PROD_TRADE_ARTIFACTS_DIR:-/opt/trade/artifacts}"
mkdir_from_var "${PROD_POSTGRES_DATA_DIR:-/opt/trade/postgres}"
mkdir_from_var "${PROD_REDIS_DATA_DIR:-/opt/trade/redis}"
mkdir_from_var "${PROD_QDRANT_DATA_DIR:-/opt/trade/qdrant}"
mkdir_from_var "${PROD_DAGSTER_HOME_DIR:-/opt/trade/dagster_home}"

cd "$APP_DIR"

log "updating $BRANCH in $APP_DIR"
git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

compose=(docker compose --env-file "$ENV_FILE" -f "$APP_DIR/docker-compose.prod.yml")

# The private Dagster UI is an opt-in Compose profile. Preserve that choice
# across deployments, but do not leave a running admin container on an image
# built by an earlier release.
dagster_webserver_was_running=false
if [[ -n "$("${compose[@]}" --profile admin ps --status running -q dagster-webserver)" ]]; then
  dagster_webserver_was_running=true
  log "running Dagster admin webserver detected; including it in this deployment"
fi

log "validating compose config"
"${compose[@]}" config >/dev/null

log "building production images"
"${compose[@]}" build
if [[ "$dagster_webserver_was_running" == true ]]; then
  "${compose[@]}" --profile admin build dagster-webserver
fi

log "starting production stack"
"${compose[@]}" up -d --remove-orphans
if [[ "$dagster_webserver_was_running" == true ]]; then
  log "recreating Dagster admin webserver with the current release image"
  "${compose[@]}" --profile admin up -d \
    --no-deps \
    --force-recreate \
    dagster-webserver
fi

health_url="${TRADE_HEALTH_URL:-http://localhost:${PROD_WEB_PORT:-8080}/api/health}"
log "checking health at $health_url"
if [[ "$dagster_webserver_was_running" == true ]]; then
  dagster_health_url="${TRADE_DAGSTER_HEALTH_URL:-http://127.0.0.1:${PROD_DAGSTER_WEB_PORT:-3000}}"
  log "checking Dagster admin health at $dagster_health_url"
fi

for attempt in {1..30}; do
  api_health_ok=false
  dagster_health_ok=true

  if curl -fsS "$health_url" >/dev/null; then
    api_health_ok=true
  fi
  if [[ "$dagster_webserver_was_running" == true ]]; then
    dagster_health_ok=false
    if curl -fsS "$dagster_health_url" >/dev/null; then
      dagster_health_ok=true
    fi
  fi

  if [[ "$api_health_ok" == true && "$dagster_health_ok" == true ]]; then
    log "health check passed"
    "${compose[@]}" ps
    exit 0
  fi
  sleep 2
  log "health check retry $attempt/30"
done

log "health check failed; recent service state follows"
"${compose[@]}" ps >&2
log_services=(api web)
if [[ "$dagster_webserver_was_running" == true ]]; then
  log_services+=(dagster-webserver)
fi
"${compose[@]}" --profile admin logs --tail=120 "${log_services[@]}" >&2
exit 1
