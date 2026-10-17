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

minio_release_key() {
  local image="$1"
  if [[ "$image" =~ RELEASE\.([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2})-([0-9]{2})-([0-9]{2})Z ]]; then
    printf '%s%s%s%s%s%s\n' \
      "${BASH_REMATCH[1]}" \
      "${BASH_REMATCH[2]}" \
      "${BASH_REMATCH[3]}" \
      "${BASH_REMATCH[4]}" \
      "${BASH_REMATCH[5]}" \
      "${BASH_REMATCH[6]}"
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


if [[ "${PROD_RESEARCH_STORAGE_ENABLED:-false}" == "true" \
  && "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" != "true" ]]; then
  printf '[trade-deploy] research application access requires the storage profile to be deployed\n' >&2
  exit 1
fi

if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" == "true" ]]; then
  "$APP_DIR/deploy/managed-kes.sh" prepare
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [[ "${PROD_MINIO_KMS_ENABLED:-false}" == "true" ]]; then
  minio_kms_names=(
    PROD_MINIO_KMS_SERVER
    PROD_MINIO_KMS_ENCLAVE
    PROD_MINIO_KMS_API_KEY
    PROD_MINIO_KMS_SSE_KEY
  )
  for kms_name in "${minio_kms_names[@]}"; do
    require_secure_value "$kms_name" "${!kms_name:-}"
  done
elif [[ "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" == "true" ]]; then
  printf '[trade-deploy] research storage deployment requires PROD_MINIO_KMS_ENABLED=true\n' >&2
  exit 1
fi

if [[ "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" == "true" ]]; then
  research_secret_names=(
    PROD_CLICKHOUSE_ADMIN_PASSWORD
    PROD_CLICKHOUSE_MIGRATION_PASSWORD
    PROD_CLICKHOUSE_DAGSTER_PASSWORD
    PROD_CLICKHOUSE_API_PASSWORD
    PROD_CLICKHOUSE_ANALYST_PASSWORD
    PROD_OBJECT_STORE_DAGSTER_ACCESS_KEY_ID
    PROD_OBJECT_STORE_DAGSTER_SECRET_ACCESS_KEY
    PROD_OBJECT_STORE_API_ACCESS_KEY_ID
    PROD_OBJECT_STORE_API_SECRET_ACCESS_KEY
  )
  for secret_name in "${research_secret_names[@]}"; do
    require_secure_value "$secret_name" "${!secret_name:-}"
  done
  research_password_names=(
    PROD_CLICKHOUSE_ADMIN_PASSWORD
    PROD_CLICKHOUSE_MIGRATION_PASSWORD
    PROD_CLICKHOUSE_DAGSTER_PASSWORD
    PROD_CLICKHOUSE_API_PASSWORD
    PROD_CLICKHOUSE_ANALYST_PASSWORD
    PROD_OBJECT_STORE_DAGSTER_SECRET_ACCESS_KEY
    PROD_OBJECT_STORE_API_SECRET_ACCESS_KEY
  )
  for password_name in "${research_password_names[@]}"; do
    password_value="${!password_name:-}"
    if [[ "${#password_value}" -lt 16 ]]; then
      printf '[trade-deploy] %s must contain at least 16 characters\n' "$password_name" >&2
      exit 1
    fi
  done
  unset password_value
  if [[ "$PROD_OBJECT_STORE_DAGSTER_ACCESS_KEY_ID" \
      == "$PROD_OBJECT_STORE_API_ACCESS_KEY_ID" ]]; then
    printf '[trade-deploy] Dagster and API object-store identities must differ\n' >&2
    exit 1
  fi
fi



mkdir_from_var "${PROD_TRADE_DATA_DIR:-/opt/trade/data}"
mkdir_from_var "${PROD_TRADE_ARTIFACTS_DIR:-/opt/trade/artifacts}"
mkdir_from_var "${PROD_POSTGRES_DATA_DIR:-/opt/trade/postgres}"
mkdir_from_var "${PROD_REDIS_DATA_DIR:-/opt/trade/redis}"
if [[ "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" == "true" ]]; then
  mkdir_from_var "${PROD_CLICKHOUSE_DATA_DIR:-/opt/trade/clickhouse}"
fi
if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" == "true" ]]; then
  mkdir_from_var "${PROD_KES_CONFIG_DIR:-/opt/trade/kes/config}"
  mkdir_from_var "${PROD_KES_CERT_DIR:-/opt/trade/kes/certs}"
  mkdir_from_var "${PROD_KES_KEY_DIR:-/opt/trade/kes/keys}"
  mkdir_from_var "${PROD_KES_SECRET_DIR:-/opt/trade/kes/secrets}"
fi
if [[ "${PROD_OTEL_ENABLED:-true}" == "true" ]]; then
  mkdir_from_var "${PROD_PROMETHEUS_DATA_DIR:-/opt/trade/prometheus}"
  mkdir_from_var "${alertmanager_data_dir:-${PROD_ALERTMANAGER_DATA_DIR:-/opt/trade/alertmanager}}"
fi
mkdir_from_var "${PROD_DAGSTER_HOME_DIR:-/opt/trade/dagster_home}"
deploy_state_dir="${PROD_DEPLOY_STATE_DIR:-/opt/trade/deploy-state}"
mkdir_from_var "$deploy_state_dir"
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

export PROD_API_IMAGE="${PROD_API_IMAGE:-trade-data-api:$synchronized_revision}"
compose=(docker compose --env-file "$ENV_FILE" -f "$APP_DIR/docker-compose.prod.yml")
if [[ "${PROD_MINIO_KMS_ENABLED:-false}" == "true" ]]; then
  if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" == "true" ]]; then
    compose+=(-f "$APP_DIR/docker-compose.prod.managed-kes.yml")
  else
    compose+=(-f "$APP_DIR/docker-compose.prod.kms.yml")
  fi
fi
if [[ "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" == "true" ]]; then
  compose+=(--profile research)
fi

diagnostic_services=(
  api
  cloudbeaver
  minio
  otel-collector
  prometheus
  alertmanager
)
if [[ "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" == "true" ]]; then
  diagnostic_services+=(clickhouse)
fi
if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" == "true" ]]; then
  diagnostic_services+=(kes)
fi

dump_service_diagnostics() {
  set +e
  log "recent production service state follows"
  "${compose[@]}" ps -a >&2
  "${compose[@]}" --profile admin logs --tail=120 "${diagnostic_services[@]}" >&2
  set -e
}

deployment_error() {
  local status=$?
  trap - ERR
  log "deployment command failed with exit code $status"
  dump_service_diagnostics
  exit "$status"
}

trap deployment_error ERR

minio_rollback_available=false
minio_rollback_tag="trade-production-minio:rollback"
minio_previous_image_ref=""
minio_previous_image_id=""

preserve_minio_rollback_image() {
  local container_id
  local previous_digest
  local state_temporary

  container_id="$("${compose[@]}" ps -a -q minio 2>/dev/null || true)"
  if [[ -z "$container_id" ]]; then
    log "no existing MinIO container found; rollback image is unavailable"
    return
  fi

  minio_previous_image_ref="$(
    docker inspect --format '{{.Config.Image}}' "$container_id" 2>/dev/null || true
  )"
  minio_previous_image_id="$(
    docker inspect --format '{{.Image}}' "$container_id" 2>/dev/null || true
  )"
  if [[ -z "$minio_previous_image_ref" || -z "$minio_previous_image_id" ]]; then
    log "unable to identify the existing MinIO image; rollback image is unavailable"
    return
  fi

  docker image tag "$minio_previous_image_id" "$minio_rollback_tag"
  previous_digest="$(
    docker image inspect \
      --format '{{join .RepoDigests ","}}' \
      "$minio_previous_image_id" 2>/dev/null || true
  )"
  state_temporary="$(mktemp "$deploy_state_dir/.minio-previous.XXXXXX")"
  {
    printf 'image_ref=%s\n' "$minio_previous_image_ref"
    printf 'image_id=%s\n' "$minio_previous_image_id"
    printf 'repo_digests=%s\n' "$previous_digest"
    printf 'revision=%s\n' "$synchronized_revision"
  } > "$state_temporary"
  chmod 0600 "$state_temporary"
  mv "$state_temporary" "$deploy_state_dir/minio-previous-image"
  minio_rollback_available=true
  log "preserved existing MinIO image as $minio_rollback_tag"
}

reject_minio_downgrade() {
  local candidate_image="${PROD_MINIO_IMAGE:-trade-minio:RELEASE.2025-09-07T16-13-09Z}"
  local candidate_release
  local previous_release

  candidate_release="$(minio_release_key "$candidate_image")"
  previous_release="$(minio_release_key "$minio_previous_image_ref")"
  if [[ -n "$candidate_release" \
    && -n "$previous_release" \
    && "$candidate_release" < "$previous_release" \
    && "${PROD_ALLOW_MINIO_DOWNGRADE:-false}" != "true" ]]; then
    printf '[trade-deploy] refusing MinIO downgrade from %s to %s; set PROD_ALLOW_MINIO_DOWNGRADE=true only for a reviewed recovery\n' \
      "$minio_previous_image_ref" \
      "$candidate_image" >&2
    return 1
  fi
}

wait_for_minio() {
  local attempt
  local container_id
  local health_status
  local attempts="${TRADE_MINIO_HEALTH_ATTEMPTS:-30}"

  for ((attempt = 1; attempt <= attempts; attempt++)); do
    container_id="$("${compose[@]}" ps -a -q minio 2>/dev/null || true)"
    if [[ -n "$container_id" ]]; then
      health_status="$(
        docker inspect \
          --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
          "$container_id" 2>/dev/null || true
      )"
      if [[ "$health_status" == "healthy" ]]; then
        return 0
      fi
      if [[ "$health_status" == "exited" || "$health_status" == "dead" ]]; then
        log "MinIO entered terminal state: $health_status"
        return 1
      fi
    fi
    sleep 2
    log "MinIO readiness retry $attempt/$attempts"
  done
  return 1
}

restore_minio_rollback_image() {
  if [[ "$minio_rollback_available" != "true" ]]; then
    log "MinIO rollback skipped because no previous image was captured"
    return 1
  fi

  log "restoring MinIO image $minio_previous_image_ref"
  export PROD_MINIO_IMAGE="$minio_rollback_tag"
  if ! start_managed_kes_if_needed; then
    return 1
  fi
  if ! "${compose[@]}" up -d --no-deps --force-recreate minio; then
    log "MinIO rollback container could not be started"
    return 1
  fi
  if ! wait_for_minio; then
    log "MinIO rollback image did not become healthy"
    return 1
  fi
  log "MinIO rollback succeeded; the application stack was not replaced"
  return 0
}

start_managed_kes_if_needed() {
  if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" != "true" ]]; then
    return 0
  fi

  log "starting repository-managed KES before MinIO"
  if ! "${compose[@]}" up -d --no-deps kes; then
    log "repository-managed KES could not be started"
    return 1
  fi
}

start_and_validate_minio() {
  preserve_minio_rollback_image
  reject_minio_downgrade

  log "pulling the requested MinIO server and client images"
  if [[ "${PROD_MINIO_IMAGE:-trade-minio:RELEASE.2025-09-07T16-13-09Z}" == trade-minio:* ]]; then
    "${compose[@]}" build minio minio-research-init
  elif ! docker image inspect "$PROD_MINIO_IMAGE" >/dev/null 2>&1; then
    "${compose[@]}" pull minio
  fi
  if [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" == "true" ]]; then
    if ! docker image inspect "${PROD_KES_IMAGE:-minio/kes:latest}" >/dev/null 2>&1; then
      "${compose[@]}" pull kes
    fi
  fi
  log "starting MinIO before replacing application services"
  if ! start_managed_kes_if_needed \
    || ! "${compose[@]}" up -d --no-deps minio \
    || ! wait_for_minio; then
    log "requested MinIO image failed its readiness check"
    "${compose[@]}" logs --tail=120 minio >&2 || true
    restore_minio_rollback_image || true
    return 1
  fi


}

record_current_minio_image() {
  local container_id
  local current_digest
  local current_image_id
  local current_image_ref
  local state_temporary

  container_id="$("${compose[@]}" ps -a -q minio)"
  current_image_ref="$(docker inspect --format '{{.Config.Image}}' "$container_id")"
  current_image_id="$(docker inspect --format '{{.Image}}' "$container_id")"
  current_digest="$(
    docker image inspect \
      --format '{{join .RepoDigests ","}}' \
      "$current_image_id" 2>/dev/null || true
  )"
  state_temporary="$(mktemp "$deploy_state_dir/.minio-current.XXXXXX")"
  {
    printf 'image_ref=%s\n' "$current_image_ref"
    printf 'image_id=%s\n' "$current_image_id"
    printf 'repo_digests=%s\n' "$current_digest"
    printf 'revision=%s\n' "$synchronized_revision"
  } > "$state_temporary"
  chmod 0600 "$state_temporary"
  mv "$state_temporary" "$deploy_state_dir/minio-current-image"
}

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
# API, Dagster daemon, and the optional Dagster webserver
# intentionally share one image tag. Building api once refreshes every Python
# service.
build_started_seconds=$SECONDS
"${compose[@]}" build api
log "production image build completed in $((SECONDS - build_started_seconds))s"

# The module rename changes Dagster origins. Do not interrupt running jobs or
# mutate scheduler state while another daemon can still write it.
log "checking Dagster is idle before replacing its code location"
if [[ -n "$("${compose[@]}" ps --status running -q dagster-daemon)" ]]; then
  "${compose[@]}" exec -T dagster-daemon python -c '
from dagster import DagsterInstance, DagsterRunStatus
with DagsterInstance.get() as instance:
    active = instance.get_runs(filters=__import__("dagster").RunsFilter(statuses=[
        DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING, DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING]))
    if active:
        raise SystemExit("Active Dagster runs must finish before deployment")
'
  "${compose[@]}" stop dagster-daemon
fi

log "starting PostgreSQL for schema migration"
"${compose[@]}" up -d --no-recreate postgres

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

if [[ "${PROD_UPGRADE_INFRASTRUCTURE:-false}" == "true" ]]; then
  start_and_validate_minio
fi

if [[ "${PROD_UPGRADE_INFRASTRUCTURE:-false}" == "true" \
  && "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" == "true" ]]; then
  log "starting private daily storage services"
  "${compose[@]}" up -d --no-recreate clickhouse minio
  log "reconciling ClickHouse identities"
  "${compose[@]}" run --rm clickhouse-init
  log "applying ClickHouse migrations"
  "${compose[@]}" run --rm --no-deps clickhouse-migrate
  log "reconciling versioned research object buckets and policies"
  "${compose[@]}" run --rm minio-research-init
fi


log "installing the current Dagster workspace without replacing scheduler storage"
workspace_target="${PROD_DAGSTER_HOME_DIR:-/opt/trade/dagster_home}/workspace.yaml"
if [[ -f "$workspace_target" ]] && ! cmp -s "$APP_DIR/dagster_home/workspace.yaml" "$workspace_target"; then
  cp -p "$workspace_target" "$deploy_state_dir/workspace-before-$synchronized_revision.yaml"
fi
install -m 0644 "$APP_DIR/dagster_home/workspace.yaml" "$workspace_target"

log "waiting for old daemon heartbeats to expire"
"${compose[@]}" run --rm --no-deps --entrypoint python dagster-daemon -c '
import time
from dagster import DagsterInstance
from trade_data.dagster.reconcile import recent_daemon_heartbeats
with DagsterInstance.get() as instance:
    deadline = time.monotonic() + 120
    while recent_daemon_heartbeats(instance):
        if time.monotonic() >= deadline:
            raise SystemExit("Recent daemon heartbeats remain; scheduler not changed")
        time.sleep(5)
'
log "reconciling retained schedules under the new code location"
"${compose[@]}" run --rm --no-deps --entrypoint python dagster-daemon   /app/scripts/reconcile_dagster_schedules.py   --apply --confirm APPLY_SCHEDULE_RECONCILIATION

log "starting production stack"
"${compose[@]}" up -d --no-deps api dagster-daemon
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

health_url="${TRADE_HEALTH_URL:-http://localhost:${PROD_API_PORT:-8000}/api/health}"
cloudbeaver_health_url="${TRADE_CLOUDBEAVER_HEALTH_URL:-http://localhost:${PROD_CLOUDBEAVER_PORT:-8978}/}"
log "checking health at $health_url"
log "checking CloudBeaver directly at $cloudbeaver_health_url"
if [[ "$dagster_webserver_was_running" == true ]]; then
  dagster_health_url="${TRADE_DAGSTER_HEALTH_URL:-http://127.0.0.1:${PROD_DAGSTER_WEB_PORT:-3000}}"
  log "checking Dagster admin health at $dagster_health_url"
fi

for attempt in {1..30}; do
  api_health_ok=false
  cloudbeaver_health_ok=false
  dagster_health_ok=true
  research_storage_health_ok=true

  if curl -fsS "$health_url" >/dev/null; then
    api_health_ok=true
  fi
  if curl -fsS "$cloudbeaver_health_url" >/dev/null; then
    cloudbeaver_health_ok=true
  fi
  if [[ "$dagster_webserver_was_running" == true ]]; then
    dagster_health_ok=false
    if curl -fsS "$dagster_health_url" >/dev/null; then
      dagster_health_ok=true
    fi
  fi
  if [[ "${PROD_RESEARCH_STORAGE_DEPLOY_ENABLED:-false}" == "true" ]]; then
    research_storage_health_ok=false
    if [[ -n "$("${compose[@]}" ps --status running -q clickhouse)" ]] \
      && "${compose[@]}" exec -T clickhouse clickhouse-client \
        --user "${PROD_CLICKHOUSE_API_USER:-api_reader}" \
        --password "$PROD_CLICKHOUSE_API_PASSWORD" \
        --query "SELECT count() FROM research.schema_migrations" >/dev/null; then
      research_storage_health_ok=true
    fi
  fi

  if [[ "$api_health_ok" == true \
    && "$cloudbeaver_health_ok" == true \
    && "$dagster_health_ok" == true \
    && "$research_storage_health_ok" == true ]]; then
    log "health check passed"
    record_current_minio_image
    log "deployment completed in $((SECONDS - deploy_started_seconds))s"
    "${compose[@]}" ps
    exit 0
  fi
  sleep 2
  log "health check retry $attempt/30"
done

log "health check failed"
if [[ "$dagster_webserver_was_running" == true ]]; then
  diagnostic_services+=(dagster-webserver)
fi
dump_service_diagnostics
exit 1
