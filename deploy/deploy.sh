#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${TRADE_APP_DIR:-/opt/trade/app}"
ENV_FILE="${TRADE_ENV_FILE:-/opt/trade/.env}"
BRANCH="${TRADE_DEPLOY_BRANCH:-main}"
DEPLOY_REEXECUTED="${TRADE_DEPLOY_REEXECUTED:-false}"
deploy_started_seconds=$SECONDS

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

require_secure_value() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" || "$value" == replace-* || "$value" == "minioadmin" ]]; then
    printf '[trade-deploy] %s must be set to a non-placeholder value\n' "$name" >&2
    exit 1
  fi
}

require_command git
require_command docker
require_command curl
require_command cmp
require_command install
require_file "$ENV_FILE"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

require_file "$APP_DIR/docker-compose.prod.yml"

if [[ "${PROD_FILING_ENABLED:-true}" == "true" ]]; then
  require_secure_value "PROD_MINIO_ROOT_USER" "${PROD_MINIO_ROOT_USER:-}"
  require_secure_value "PROD_MINIO_ROOT_PASSWORD" "${PROD_MINIO_ROOT_PASSWORD:-}"
  require_secure_value \
    "PROD_FILING_S3_ACCESS_KEY_ID" \
    "${PROD_FILING_S3_ACCESS_KEY_ID:-}"
  require_secure_value \
    "PROD_FILING_S3_SECRET_ACCESS_KEY" \
    "${PROD_FILING_S3_SECRET_ACCESS_KEY:-}"
  if [[ "${#PROD_MINIO_ROOT_PASSWORD}" -lt 16 \
    || "${#PROD_FILING_S3_SECRET_ACCESS_KEY}" -lt 16 ]]; then
    printf '[trade-deploy] MinIO root and application secrets must be at least 16 characters\n' >&2
    exit 1
  fi
  if [[ "$PROD_MINIO_ROOT_USER" == "$PROD_FILING_S3_ACCESS_KEY_ID" ]]; then
    printf '[trade-deploy] filing storage must not use the MinIO root identity\n' >&2
    exit 1
  fi
  if [[ "${PROD_FILING_REQUIRE_WORKSPACE_HEADER:-true}" != "true" ]]; then
    printf '[trade-deploy] production filing workspace enforcement must remain enabled\n' >&2
    exit 1
  fi
  require_command openssl
  alert_token_file="${PROD_ALERT_WEBHOOK_TOKEN_FILE:-$(dirname "$ENV_FILE")/secrets/alertmanager-webhook-token}"
  alert_token_dir="$(dirname "$alert_token_file")"
  mkdir -p "$alert_token_dir"
  chmod 0700 "$alert_token_dir"
  if [[ -L "$alert_token_file" ]]; then
    printf '[trade-deploy] alert webhook token file must not be a symbolic link\n' >&2
    exit 1
  fi
  if [[ ! -e "$alert_token_file" ]]; then
    alert_token_temporary="$(mktemp "$alert_token_dir/.alertmanager-token.XXXXXX")"
    chmod 0600 "$alert_token_temporary"
    if ! openssl rand -hex 32 | tr -d '\r\n' > "$alert_token_temporary"; then
      rm -f "$alert_token_temporary"
      printf '[trade-deploy] unable to generate alert webhook token\n' >&2
      exit 1
    fi
    mv "$alert_token_temporary" "$alert_token_file"
    log "generated private Alertmanager webhook credential"
  fi
  if [[ ! -f "$alert_token_file" || ! -s "$alert_token_file" ]]; then
    printf '[trade-deploy] alert webhook token file is missing or empty\n' >&2
    exit 1
  fi
  chmod 0600 "$alert_token_file"
  alert_token_value="$(tr -d '\r\n' < "$alert_token_file")"
  if [[ "${#alert_token_value}" -lt 32 ]]; then
    printf '[trade-deploy] alert webhook token must contain at least 32 characters\n' >&2
    exit 1
  fi
  unset alert_token_value
  export PROD_ALERT_WEBHOOK_TOKEN_FILE="$alert_token_file"
fi

if [[ "${PROD_LANGFUSE_ENABLED:-false}" == "true" ]]; then
  require_secure_value "PROD_LANGFUSE_PUBLIC_KEY" "${PROD_LANGFUSE_PUBLIC_KEY:-}"
  require_secure_value "PROD_LANGFUSE_SECRET_KEY" "${PROD_LANGFUSE_SECRET_KEY:-}"
fi

if [[ "${PROD_BIGQUERY_ENABLED:-false}" == "true" ]]; then
  bigquery_credential_file="${PROD_BIGQUERY_CREDENTIALS_FILE:-}"
  if [[ -z "$bigquery_credential_file" || ! -f "$bigquery_credential_file" \
    || ! -s "$bigquery_credential_file" || ! -r "$bigquery_credential_file" ]]; then
    printf '[trade-deploy] BigQuery credential file is missing or unreadable\n' >&2
    exit 1
  fi
  credential_mode="$(stat -c '%a' "$bigquery_credential_file")"
  credential_owner="$(stat -c '%U:%G' "$bigquery_credential_file")"
  expected_owner="${PROD_BIGQUERY_CREDENTIALS_OWNER:-inglorious:inglorious}"
  if [[ "$credential_mode" != "600" || "$credential_owner" != "$expected_owner" ]]; then
    printf '[trade-deploy] BigQuery credential ownership or permissions are unsafe\n' >&2
    exit 1
  fi
fi

mkdir_from_var "${PROD_TRADE_DATA_DIR:-/opt/trade/data}"
mkdir_from_var "${PROD_TRADE_ARTIFACTS_DIR:-/opt/trade/artifacts}"
mkdir_from_var "${PROD_POSTGRES_DATA_DIR:-/opt/trade/postgres}"
mkdir_from_var "${PROD_REDIS_DATA_DIR:-/opt/trade/redis}"
mkdir_from_var "${PROD_QDRANT_DATA_DIR:-/opt/trade/qdrant}"
if [[ "${PROD_FILING_ENABLED:-true}" == "true" ]]; then
  mkdir_from_var "${PROD_MINIO_DATA_DIR:-/opt/trade/minio}"
fi
if [[ "${PROD_OTEL_ENABLED:-true}" == "true" ]]; then
  mkdir_from_var "${PROD_PROMETHEUS_DATA_DIR:-/opt/trade/prometheus}"
  mkdir_from_var "${PROD_ALERTMANAGER_DATA_DIR:-/opt/trade/alertmanager}"
fi
mkdir_from_var "${PROD_DAGSTER_HOME_DIR:-/opt/trade/dagster_home}"
cloudbeaver_workspace="${PROD_CLOUDBEAVER_WORKSPACE_DIR:-/opt/trade/cloudbeaver}"
cloudbeaver_connections_dir="$cloudbeaver_workspace/GlobalConfiguration/.dbeaver"
mkdir -p "$cloudbeaver_connections_dir"

cd "$APP_DIR"

starting_revision="$(git rev-parse HEAD)"
log "updating $BRANCH in $APP_DIR"
git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"
synchronized_revision="$(git rev-parse HEAD)"

if [[ "$DEPLOY_REEXECUTED" != true \
  && "$starting_revision" != "$synchronized_revision" ]]; then
  log "restarting deployment with synchronized script at $synchronized_revision"
  exec env TRADE_DEPLOY_REEXECUTED=true "$APP_DIR/deploy/deploy.sh"
fi

compose=(docker compose --env-file "$ENV_FILE" -f "$APP_DIR/docker-compose.prod.yml")

log "installing secret-free CloudBeaver connection policy"
cloudbeaver_policy_changed=false
install_cloudbeaver_policy() {
  local source_file="$1"
  local destination_file="$2"
  if [[ ! -f "$destination_file" ]] || ! cmp -s "$source_file" "$destination_file"; then
    install -m 0644 "$source_file" "$destination_file"
    cloudbeaver_policy_changed=true
  fi
}
install_cloudbeaver_policy \
  "$APP_DIR/deploy/cloudbeaver/data-sources.json" \
  "$cloudbeaver_connections_dir/data-sources.json"
install_cloudbeaver_policy \
  "$APP_DIR/deploy/cloudbeaver/data-sources-permissions.json" \
  "$cloudbeaver_connections_dir/data-sources-permissions.json"

# The private Dagster UI is an opt-in Compose profile. Preserve that choice
# across deployments, but do not leave a running admin container on an image
# built by an earlier release.
dagster_webserver_was_running=false
if [[ -n "$("${compose[@]}" --profile admin ps --status running -q dagster-webserver)" ]]; then
  dagster_webserver_was_running=true
  log "running Dagster admin webserver detected; including it in this deployment"
fi
cloudbeaver_was_running=false
if [[ -n "$("${compose[@]}" ps --status running -q cloudbeaver)" ]]; then
  cloudbeaver_was_running=true
fi

log "validating compose config"
"${compose[@]}" config >/dev/null

log "building production images"
# API, filing worker, Dagster daemon, and the optional Dagster webserver
# intentionally share one image tag. Building api once refreshes every Python
# service.
build_started_seconds=$SECONDS
"${compose[@]}" build api web
log "production image build completed in $((SECONDS - build_started_seconds))s"

log "starting PostgreSQL for schema migration"
"${compose[@]}" up -d postgres

database_ready=false
for attempt in {1..30}; do
  if "${compose[@]}" exec -T postgres \
    pg_isready \
      -U "${PROD_POSTGRES_USER:-trade}" \
      -d "${PROD_POSTGRES_DB:-trade_research}" >/dev/null; then
    database_ready=true
    break
  fi
  sleep 2
  log "PostgreSQL readiness retry $attempt/30"
done

if [[ "$database_ready" != true ]]; then
  log "PostgreSQL did not become ready for schema migration"
  "${compose[@]}" ps postgres >&2
  "${compose[@]}" logs --tail=120 postgres >&2
  exit 1
fi

log "applying database migrations"
migration_started_seconds=$SECONDS
"${compose[@]}" run --rm --no-deps api \
  alembic -c /app/alembic.ini upgrade head
log "database migrations completed in $((SECONDS - migration_started_seconds))s"

log "starting production stack"
"${compose[@]}" up -d --remove-orphans
if [[ "$cloudbeaver_policy_changed" == true \
  && "$cloudbeaver_was_running" == true ]]; then
  log "restarting CloudBeaver to apply updated connection policy"
  "${compose[@]}" restart cloudbeaver
fi
if [[ "$dagster_webserver_was_running" == true ]]; then
  log "recreating Dagster admin webserver with the current release image"
  "${compose[@]}" --profile admin up -d \
    --no-deps \
    --force-recreate \
    dagster-webserver
fi

health_url="${TRADE_HEALTH_URL:-http://localhost:${PROD_WEB_PORT:-8080}/api/health}"
filing_health_url="${TRADE_FILING_HEALTH_URL:-http://localhost:${PROD_WEB_PORT:-8080}/api/filings/health}"
cloudbeaver_health_url="${TRADE_CLOUDBEAVER_HEALTH_URL:-http://localhost:${PROD_WEB_PORT:-8080}/}"
cloudbeaver_host="${PROD_CLOUDBEAVER_HOST:-sql.example.com}"
log "checking health at $health_url"
if [[ "${PROD_FILING_ENABLED:-true}" == "true" ]]; then
  log "checking filing runtime at $filing_health_url"
fi
log "checking CloudBeaver through Caddy at $cloudbeaver_health_url"
if [[ "$dagster_webserver_was_running" == true ]]; then
  dagster_health_url="${TRADE_DAGSTER_HEALTH_URL:-http://127.0.0.1:${PROD_DAGSTER_WEB_PORT:-3000}}"
  log "checking Dagster admin health at $dagster_health_url"
fi

for attempt in {1..30}; do
  api_health_ok=false
  filing_health_ok=true
  cloudbeaver_health_ok=false
  dagster_health_ok=true

  if curl -fsS "$health_url" >/dev/null; then
    api_health_ok=true
  fi
  if [[ "${PROD_FILING_ENABLED:-true}" == "true" ]]; then
    filing_health_ok=false
    filing_health_payload="$(curl -fsS "$filing_health_url" || true)"
    if [[ "$filing_health_payload" == *'"status":"ok"'* \
      && "$filing_health_payload" == *'"queue_mode":"celery"'* \
      && "$filing_health_payload" == *'"checkpoint_backend":"postgresql"'* \
      && "$filing_health_payload" == *'"artifact_backend":"s3"'* \
      && "$filing_health_payload" == *'"workspace_header_required":true'* \
      && -n "$("${compose[@]}" ps --status running -q filing-worker)" \
      && -n "$("${compose[@]}" ps --status running -q minio)" \
      && -n "$("${compose[@]}" ps --status running -q otel-collector)" \
      && -n "$("${compose[@]}" ps --status running -q prometheus)" \
      && -n "$("${compose[@]}" ps --status running -q alertmanager)" ]]; then
      filing_health_ok=true
    fi
  fi
  if curl -fsS -H "Host: $cloudbeaver_host" "$cloudbeaver_health_url" >/dev/null; then
    cloudbeaver_health_ok=true
  fi
  if [[ "$dagster_webserver_was_running" == true ]]; then
    dagster_health_ok=false
    if curl -fsS "$dagster_health_url" >/dev/null; then
      dagster_health_ok=true
    fi
  fi

  if [[ "$api_health_ok" == true \
    && "$filing_health_ok" == true \
    && "$cloudbeaver_health_ok" == true \
    && "$dagster_health_ok" == true ]]; then
    log "health check passed"
    log "deployment completed in $((SECONDS - deploy_started_seconds))s"
    "${compose[@]}" ps
    exit 0
  fi
  sleep 2
  log "health check retry $attempt/30"
done

log "health check failed; recent service state follows"
"${compose[@]}" ps >&2
log_services=(
  api
  web
  cloudbeaver
  filing-worker
  minio
  minio-init
  otel-collector
  prometheus
  alertmanager
)
if [[ "$dagster_webserver_was_running" == true ]]; then
  log_services+=(dagster-webserver)
fi
"${compose[@]}" --profile admin logs --tail=120 "${log_services[@]}" >&2
exit 1
