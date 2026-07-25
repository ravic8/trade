#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${TRADE_APP_DIR:-/opt/trade/app}"
ENV_FILE="${TRADE_ENV_FILE:-/opt/trade/.env}"
KEEP_RESTORE="${TRADE_RESTORE_KEEP:-false}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DRILL_ID="${STAMP}-$$"

log() {
  printf '[trade-restore] %s\n' "$*"
}

fail() {
  printf '[trade-restore] %s\n' "$*" >&2
  exit 1
}

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    fail "missing required command: $name"
  fi
}

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    fail "missing required file: $path"
  fi
}

usage() {
  cat <<'EOF'
Usage: deploy/restore-drill.sh BACKUP_DIRECTORY

Restores a production backup into isolated, unpublished Docker containers,
validates the recovered filing state, writes a JSON report, and removes the
temporary environment after success. Production containers and data paths are
never used as restore targets.
EOF
}

if [[ "$#" -ne 1 ]]; then
  usage >&2
  exit 2
fi

require_command docker
require_command python3
require_command sha256sum
require_file "$ENV_FILE"
require_file "$APP_DIR/docker-compose.prod.yml"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

RESTORE_ROOT="${TRADE_RESTORE_ROOT:-${PROD_RESTORE_ROOT:-/opt/trade/restore-drills}}"
REPORT_ROOT="${TRADE_RESTORE_REPORT_DIR:-${PROD_RESTORE_REPORT_DIR:-/opt/trade/restore-reports}}"
BACKUP_DIR="$(
  python3 - "$1" <<'PY'
from pathlib import Path
import sys

print(Path(sys.argv[1]).expanduser().resolve())
PY
)"
[[ -d "$BACKUP_DIR" ]] || fail "backup directory does not exist: $BACKUP_DIR"
if [[ "$(basename "$BACKUP_DIR")" == .incomplete-* ]]; then
  fail "refusing to restore an incomplete backup: $BACKUP_DIR"
fi

for required in postgres.dump SHA256SUMS README.txt data.tgz minio.tgz; do
  require_file "$BACKUP_DIR/$required"
done

mkdir -p "$RESTORE_ROOT" "$REPORT_ROOT"
RESTORE_ROOT="$(
  python3 - "$RESTORE_ROOT" <<'PY'
from pathlib import Path
import sys

print(Path(sys.argv[1]).expanduser().resolve())
PY
)"
REPORT_ROOT="$(
  python3 - "$REPORT_ROOT" <<'PY'
from pathlib import Path
import sys

print(Path(sys.argv[1]).expanduser().resolve())
PY
)"

WORK_DIR="$RESTORE_ROOT/.incomplete-$DRILL_ID"
RESTORED_DIR="$WORK_DIR/restored"
POSTGRES_DIR="$WORK_DIR/postgres"
REPORT_PATH="$REPORT_ROOT/$DRILL_ID.json"
case "$WORK_DIR" in
  "$RESTORE_ROOT"/.incomplete-*) ;;
  *) fail "unsafe restore work path: $WORK_DIR" ;;
esac
if [[ -e "$WORK_DIR" || -e "$REPORT_PATH" ]]; then
  fail "restore drill path already exists for id: $DRILL_ID"
fi
mkdir -p "$RESTORED_DIR" "$POSTGRES_DIR"
chmod 700 "$WORK_DIR"

NETWORK="trade-restore-$DRILL_ID"
POSTGRES_CONTAINER="$NETWORK-postgres"
MINIO_CONTAINER="$NETWORK-minio"
QDRANT_CONTAINER="$NETWORK-qdrant"
containers=()
network_created=false

result_status="failed"
stage="initializing"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
exit_code=1
checksums_verified=false
archive_names=""
source_document_count=0
source_verified_count=0
source_failed_skipped_count=0
migration_revision=""
timescaledb_version=""
filing_document_count=0
approved_fact_count=0
minio_object_count=0
minio_versioning_verified=false
qdrant_reachable=false
golden_dataset_id=""
golden_case_count=0
golden_expected_fact_count=0
golden_matched_fact_count=0
golden_value_correct_count=0
golden_evidence_correct_count=0
golden_passed=false

write_report() {
  local retained=false
  if [[ -d "$WORK_DIR" ]]; then
    retained=true
  fi
  REPORT_STATUS="$result_status" \
  REPORT_STAGE="$stage" \
  REPORT_EXIT_CODE="$exit_code" \
  REPORT_STARTED_AT="$started_at" \
  REPORT_DRILL_ID="$DRILL_ID" \
  REPORT_BACKUP_DIR="$BACKUP_DIR" \
  REPORT_WORK_DIR="$WORK_DIR" \
  REPORT_RETAINED="$retained" \
  REPORT_CHECKSUMS="$checksums_verified" \
  REPORT_ARCHIVES="$archive_names" \
  REPORT_SOURCE_DOCUMENTS="$source_document_count" \
  REPORT_SOURCE_VERIFIED="$source_verified_count" \
  REPORT_SOURCE_FAILED_SKIPPED="$source_failed_skipped_count" \
  REPORT_MIGRATION="$migration_revision" \
  REPORT_TIMESCALEDB_VERSION="$timescaledb_version" \
  REPORT_FILING_DOCUMENTS="$filing_document_count" \
  REPORT_APPROVED_FACTS="$approved_fact_count" \
  REPORT_MINIO_OBJECTS="$minio_object_count" \
  REPORT_MINIO_VERSIONING="$minio_versioning_verified" \
  REPORT_QDRANT="$qdrant_reachable" \
  REPORT_GOLDEN_DATASET="$golden_dataset_id" \
  REPORT_GOLDEN_CASES="$golden_case_count" \
  REPORT_GOLDEN_EXPECTED="$golden_expected_fact_count" \
  REPORT_GOLDEN_MATCHED="$golden_matched_fact_count" \
  REPORT_GOLDEN_VALUES="$golden_value_correct_count" \
  REPORT_GOLDEN_EVIDENCE="$golden_evidence_correct_count" \
  REPORT_GOLDEN_PASSED="$golden_passed" \
  python3 - "$REPORT_PATH" <<'PY'
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys


def integer(name: str) -> int:
    return int(os.environ[name])


def boolean(name: str) -> bool:
    return os.environ[name].lower() == "true"


payload = {
    "schema_version": 1,
    "drill_id": os.environ["REPORT_DRILL_ID"],
    "status": os.environ["REPORT_STATUS"],
    "stage": os.environ["REPORT_STAGE"],
    "exit_code": integer("REPORT_EXIT_CODE"),
    "started_at": os.environ["REPORT_STARTED_AT"],
    "finished_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "backup_dir": os.environ["REPORT_BACKUP_DIR"],
    "isolation": {
        "work_dir": os.environ["REPORT_WORK_DIR"],
        "work_dir_retained": boolean("REPORT_RETAINED"),
        "host_ports_published": False,
        "production_compose_used": False,
    },
    "integrity": {
        "checksums_verified": boolean("REPORT_CHECKSUMS"),
        "archives_restored": [
            name for name in os.environ["REPORT_ARCHIVES"].split(",") if name
        ],
    },
    "source_manifest": {
        "document_count": integer("REPORT_SOURCE_DOCUMENTS"),
        "verified_document_count": integer("REPORT_SOURCE_VERIFIED"),
        "failed_skipped_count": integer("REPORT_SOURCE_FAILED_SKIPPED"),
    },
    "postgresql": {
        "migration_revision": os.environ["REPORT_MIGRATION"] or None,
        "timescaledb_version": os.environ["REPORT_TIMESCALEDB_VERSION"] or None,
        "filing_document_count": integer("REPORT_FILING_DOCUMENTS"),
        "approved_fact_count": integer("REPORT_APPROVED_FACTS"),
    },
    "object_store": {
        "object_count": integer("REPORT_MINIO_OBJECTS"),
        "versioning_verified": boolean("REPORT_MINIO_VERSIONING"),
    },
    "qdrant": {"reachable": boolean("REPORT_QDRANT")},
    "golden_evaluation": {
        "dataset_id": os.environ["REPORT_GOLDEN_DATASET"] or None,
        "case_count": integer("REPORT_GOLDEN_CASES"),
        "expected_fact_count": integer("REPORT_GOLDEN_EXPECTED"),
        "matched_fact_count": integer("REPORT_GOLDEN_MATCHED"),
        "value_correct_count": integer("REPORT_GOLDEN_VALUES"),
        "evidence_correct_count": integer("REPORT_GOLDEN_EVIDENCE"),
        "passed": boolean("REPORT_GOLDEN_PASSED"),
    },
}

target = Path(sys.argv[1])
target.parent.mkdir(parents=True, exist_ok=True)
temporary = target.with_suffix(".json.tmp")
temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
temporary.replace(target)
PY
}

cleanup() {
  local observed_exit="$?"
  local container
  local cleanup_failed=false
  trap - EXIT
  set +e
  if [[ "$result_status" == "passed" ]]; then
    exit_code=0
  else
    exit_code="$observed_exit"
    [[ "$exit_code" -ne 0 ]] || exit_code=1
  fi
  for ((index=${#containers[@]} - 1; index >= 0; index--)); do
    container="${containers[$index]}"
    if ! docker rm -f "$container" >/dev/null 2>&1; then
      printf '[trade-restore] failed to remove container: %s\n' "$container" >&2
      cleanup_failed=true
    fi
  done
  if [[ "$network_created" == true ]]; then
    if ! docker network rm "$NETWORK" >/dev/null 2>&1; then
      printf '[trade-restore] failed to remove network: %s\n' "$NETWORK" >&2
      cleanup_failed=true
    fi
  fi
  if [[ "$result_status" == "passed" \
    && "$KEEP_RESTORE" != "true" \
    && "$cleanup_failed" == false ]]; then
    if rm -rf -- "$WORK_DIR"; then
      log "removed isolated restore data"
    else
      printf '[trade-restore] failed to remove restore data: %s\n' "$WORK_DIR" >&2
      cleanup_failed=true
    fi
  fi
  if [[ "$cleanup_failed" == true ]]; then
    result_status="failed"
    stage="cleanup"
    exit_code=1
  fi
  if ! write_report; then
    printf '[trade-restore] failed to write report: %s\n' "$REPORT_PATH" >&2
    [[ "$exit_code" -ne 0 ]] || exit_code=1
  fi
  if [[ -d "$WORK_DIR" ]]; then
    log "retained isolated restore data: $WORK_DIR"
  fi
  log "restore report: $REPORT_PATH"
  exit "$exit_code"
}
trap cleanup EXIT

stage="checksum_verification"
log "verifying backup checksums"
(
  cd "$BACKUP_DIR"
  sha256sum -c SHA256SUMS
)
checksums_verified=true

stage="archive_extraction"
log "extracting archives into isolated path $RESTORED_DIR"
for archive in "$BACKUP_DIR"/*.tgz; do
  [[ -e "$archive" ]] || continue
  archive_name="$(basename "$archive")"
  python3 - "$archive" "$RESTORED_DIR" <<'PY'
from pathlib import Path
import sys
import tarfile

archive = Path(sys.argv[1])
destination = Path(sys.argv[2]).resolve()
with tarfile.open(archive, "r:gz") as stream:
    members = stream.getmembers()
    for member in members:
        member_path = (destination / member.name).resolve()
        if not member_path.is_relative_to(destination):
            raise SystemExit(f"unsafe archive path in {archive.name}: {member.name}")
        if member.issym() or member.islnk() or member.isdev():
            raise SystemExit(
                f"unsupported archive member in {archive.name}: {member.name}"
            )
    stream.extractall(destination, members=members)
PY
  if [[ -n "$archive_names" ]]; then
    archive_names="$archive_names,$archive_name"
  else
    archive_names="$archive_name"
  fi
done

expected_company="${TRADE_RESTORE_EXPECTED_COMPANY_ID:-NSE:INFY}"
minimum_source_documents="${TRADE_RESTORE_MIN_SOURCE_DOCUMENTS:-123}"
minimum_filing_documents="${TRADE_RESTORE_MIN_FILING_DOCUMENTS:-26}"
minimum_approved_facts="${TRADE_RESTORE_MIN_APPROVED_FACTS:-1186}"
minimum_minio_objects="${TRADE_RESTORE_MIN_MINIO_OBJECTS:-26}"
manifest_relative_path="${TRADE_RESTORE_MANIFEST_RELATIVE_PATH:-filings/nse/INFY/manifest.json}"
for numeric_value in \
  "$minimum_source_documents" \
  "$minimum_filing_documents" \
  "$minimum_approved_facts" \
  "$minimum_minio_objects"; do
  [[ "$numeric_value" =~ ^[0-9]+$ ]] || fail "restore minimums must be integers"
done
[[ "$expected_company" =~ ^[A-Za-z0-9:_-]+$ ]] \
  || fail "invalid expected company id: $expected_company"
[[ "$manifest_relative_path" != /* && "$manifest_relative_path" != *".."* ]] \
  || fail "unsafe manifest relative path: $manifest_relative_path"

stage="source_manifest_verification"
manifest_result="$(
  python3 - \
    "$RESTORED_DIR/data/$manifest_relative_path" \
    "$minimum_source_documents" <<'PY'
from hashlib import sha256
import json
from pathlib import Path
import sys

manifest_path = Path(sys.argv[1]).resolve()
minimum_documents = int(sys.argv[2])
if not manifest_path.is_file():
    raise SystemExit(f"restored manifest is missing: {manifest_path}")
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
documents = manifest.get("documents")
if not isinstance(documents, list):
    raise SystemExit("restored manifest documents are invalid")
verified = 0
failed_skipped = 0
manifest_root = manifest_path.parent
for document in documents:
    if document.get("acquisition_status") == "failed" or document.get("error"):
        failed_skipped += 1
        continue
    relative_path = document.get("relative_path")
    expected_hash = document.get("sha256")
    if not isinstance(relative_path, str) or not isinstance(expected_hash, str):
        raise SystemExit("restored manifest document lacks path or SHA-256")
    document_path = (manifest_root / relative_path).resolve()
    if not document_path.is_relative_to(manifest_root):
        raise SystemExit(f"manifest path escapes restore root: {relative_path}")
    if not document_path.is_file():
        raise SystemExit(f"restored source document is missing: {relative_path}")
    digest = sha256()
    with document_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected_hash:
        raise SystemExit(f"source document hash mismatch: {relative_path}")
    expected_bytes = document.get("bytes")
    if isinstance(expected_bytes, int) and document_path.stat().st_size != expected_bytes:
        raise SystemExit(f"source document size mismatch: {relative_path}")
    verified += 1

if verified < minimum_documents:
    raise SystemExit(
        f"restored manifest has {verified} valid documents; "
        f"expected at least {minimum_documents}"
    )
declared_documents = manifest.get("document_count")
if isinstance(declared_documents, int) and declared_documents != verified:
    raise SystemExit(
        f"manifest document count mismatch: declared {declared_documents}, "
        f"verified {verified}"
    )
declared_failed = manifest.get("failed_download_count")
if isinstance(declared_failed, int) and declared_failed != failed_skipped:
    raise SystemExit(
        f"manifest failed-download count mismatch: declared {declared_failed}, "
        f"observed {failed_skipped}"
    )
declared_candidates = manifest.get("candidate_count")
if isinstance(declared_candidates, int) and declared_candidates != len(documents):
    raise SystemExit(
        f"manifest candidate count mismatch: declared {declared_candidates}, "
        f"observed {len(documents)}"
    )

print(
    json.dumps(
        {
            "document_count": verified,
            "verified_count": verified,
            "failed_skipped_count": failed_skipped,
        }
    )
)
PY
)"
read -r \
  source_document_count \
  source_verified_count \
  source_failed_skipped_count < <(
  python3 - "$manifest_result" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(
    payload["document_count"],
    payload["verified_count"],
    payload["failed_skipped_count"],
)
PY
)
log \
  "verified $source_verified_count restored source documents; " \
  "skipped $source_failed_skipped_count failed manifest entries"

postgres_image="${TRADE_RESTORE_POSTGRES_IMAGE:-timescale/timescaledb:latest-pg16}"
api_image="${PROD_API_IMAGE:-trade-research-api:local}"
minio_image="${PROD_MINIO_IMAGE:-minio/minio:latest}"
minio_client_image="${PROD_MINIO_CLIENT_IMAGE:-minio/mc:latest}"
qdrant_image="${TRADE_RESTORE_QDRANT_IMAGE:-qdrant/qdrant:latest}"
db_name="trade_restore"
db_user="restore_validator"
db_password="$(
  python3 - <<'PY'
import secrets

print(secrets.token_hex(24))
PY
)"

DB_ENV_FILE="$WORK_DIR/postgres.env"
MINIO_ENV_FILE="$WORK_DIR/minio.env"
CLIENT_ENV_FILE="$WORK_DIR/minio-client.env"
API_ENV_FILE="$WORK_DIR/api.env"
umask 077
printf 'POSTGRES_DB=%s\nPOSTGRES_USER=%s\nPOSTGRES_PASSWORD=%s\n' \
  "$db_name" "$db_user" "$db_password" > "$DB_ENV_FILE"
printf 'MINIO_ROOT_USER=%s\nMINIO_ROOT_PASSWORD=%s\n' \
  "${PROD_MINIO_ROOT_USER:?set PROD_MINIO_ROOT_USER}" \
  "${PROD_MINIO_ROOT_PASSWORD:?set PROD_MINIO_ROOT_PASSWORD}" \
  > "$MINIO_ENV_FILE"
printf '%s\n' \
  "RESTORE_ACCESS_KEY=${PROD_FILING_S3_ACCESS_KEY_ID:?set PROD_FILING_S3_ACCESS_KEY_ID}" \
  "RESTORE_SECRET_KEY=${PROD_FILING_S3_SECRET_ACCESS_KEY:?set PROD_FILING_S3_SECRET_ACCESS_KEY}" \
  "RESTORE_BUCKET=${PROD_FILING_S3_BUCKET:-lens-filings}" \
  "RESTORE_PREFIX=${PROD_FILING_S3_PREFIX:-parsed}" \
  > "$CLIENT_ENV_FILE"
printf '%s\n' \
  "APP_ENV=restore-drill" \
  "DATABASE_URL=postgresql+psycopg://$db_user:$db_password@$POSTGRES_CONTAINER:5432/$db_name" \
  "FILING_ENABLED=true" \
  "FILING_ARTIFACT_BACKEND=s3" \
  "FILING_S3_ENDPOINT_URL=http://$MINIO_CONTAINER:9000" \
  "FILING_S3_REGION=${PROD_FILING_S3_REGION:-us-east-1}" \
  "FILING_S3_BUCKET=${PROD_FILING_S3_BUCKET:-lens-filings}" \
  "FILING_S3_PREFIX=${PROD_FILING_S3_PREFIX:-parsed}" \
  "FILING_S3_ACCESS_KEY_ID=${PROD_FILING_S3_ACCESS_KEY_ID}" \
  "FILING_S3_SECRET_ACCESS_KEY=${PROD_FILING_S3_SECRET_ACCESS_KEY}" \
  "FILING_INDEX_ENABLED=false" \
  "LANGFUSE_ENABLED=false" \
  "OTEL_ENABLED=false" \
  > "$API_ENV_FILE"

stage="isolated_network_creation"
docker network create --label trade.restore-drill="$DRILL_ID" "$NETWORK" >/dev/null
network_created=true

stage="postgresql_start"
log "starting isolated PostgreSQL without published host ports"
docker run -d \
  --name "$POSTGRES_CONTAINER" \
  --network "$NETWORK" \
  --label trade.restore-drill="$DRILL_ID" \
  --env-file "$DB_ENV_FILE" \
  -v "$POSTGRES_DIR:/var/lib/postgresql/data" \
  "$postgres_image" >/dev/null
containers+=("$POSTGRES_CONTAINER")

postgres_ready=false
for _ in {1..60}; do
  if docker exec "$POSTGRES_CONTAINER" \
    pg_isready -U "$db_user" -d "$db_name" >/dev/null 2>&1; then
    postgres_ready=true
    break
  fi
  sleep 2
done
[[ "$postgres_ready" == true ]] || fail "isolated PostgreSQL did not become ready"

stage="postgresql_restore"
log "restoring PostgreSQL custom-format dump"
docker exec "$POSTGRES_CONTAINER" \
  psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -c \
  "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null
timescaledb_version="$(
  docker exec "$POSTGRES_CONTAINER" \
    psql -U "$db_user" -d "$db_name" -Atqc \
    "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';"
)"
[[ -n "$timescaledb_version" ]] \
  || fail "TimescaleDB extension is unavailable in the restore container"
docker exec "$POSTGRES_CONTAINER" \
  psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -c \
  "SELECT timescaledb_pre_restore();" >/dev/null
docker exec -i "$POSTGRES_CONTAINER" \
  pg_restore \
    -U "$db_user" \
    -d "$db_name" \
    --exit-on-error \
    --no-owner \
    --no-privileges \
  < "$BACKUP_DIR/postgres.dump"
docker exec "$POSTGRES_CONTAINER" \
  psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -c \
  "SELECT timescaledb_post_restore();" >/dev/null
docker exec "$POSTGRES_CONTAINER" \
  psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -c \
  "ANALYZE;" >/dev/null

stage="postgresql_validation"
expected_migration="$(
  docker run --rm "$api_image" alembic heads \
    | awk 'NR == 1 {print $1}'
)"
migration_revision="$(
  docker exec "$POSTGRES_CONTAINER" \
    psql -U "$db_user" -d "$db_name" -Atqc \
    "SELECT version_num FROM alembic_version;"
)"
[[ -n "$expected_migration" && "$migration_revision" == "$expected_migration" ]] \
  || fail \
    "restored migration $migration_revision does not match image head $expected_migration"

filing_document_count="$(
  docker exec "$POSTGRES_CONTAINER" \
    psql -U "$db_user" -d "$db_name" -Atqc \
    "SELECT COUNT(*) FROM filing_documents WHERE company_id = '$expected_company';"
)"
approved_fact_count="$(
  docker exec "$POSTGRES_CONTAINER" \
    psql -U "$db_user" -d "$db_name" -Atqc \
    "SELECT COUNT(*) FROM filing_approved_facts WHERE company_id = '$expected_company';"
)"
[[ "$filing_document_count" =~ ^[0-9]+$ ]] \
  || fail "restored filing document count is invalid"
[[ "$approved_fact_count" =~ ^[0-9]+$ ]] \
  || fail "restored approved fact count is invalid"
(( filing_document_count >= minimum_filing_documents )) \
  || fail \
    "restored filing documents $filing_document_count are below $minimum_filing_documents"
(( approved_fact_count >= minimum_approved_facts )) \
  || fail \
    "restored approved facts $approved_fact_count are below $minimum_approved_facts"
log "validated PostgreSQL migration $migration_revision and $approved_fact_count approved facts"

stage="minio_start"
log "starting isolated MinIO from restored storage without published host ports"
docker run -d \
  --name "$MINIO_CONTAINER" \
  --network "$NETWORK" \
  --label trade.restore-drill="$DRILL_ID" \
  --env-file "$MINIO_ENV_FILE" \
  -v "$RESTORED_DIR/minio:/data" \
  "$minio_image" server /data --console-address :9001 >/dev/null
containers+=("$MINIO_CONTAINER")

minio_ready=false
for _ in {1..60}; do
  if docker exec "$MINIO_CONTAINER" \
    curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    minio_ready=true
    break
  fi
  sleep 2
done
[[ "$minio_ready" == true ]] || fail "isolated MinIO did not become ready"

stage="minio_validation"
minio_listing="$(
  docker run --rm \
    --network "$NETWORK" \
    --env-file "$CLIENT_ENV_FILE" \
    --entrypoint /bin/sh \
    "$minio_client_image" \
    -c "
      set -eu
      mc alias set restore http://$MINIO_CONTAINER:9000 \
        \"\$RESTORE_ACCESS_KEY\" \"\$RESTORE_SECRET_KEY\" >/dev/null
      mc version info \"restore/\$RESTORE_BUCKET\" | grep -qi enabled
      mc find \"restore/\$RESTORE_BUCKET/\$RESTORE_PREFIX\" \
        --name parsed_document.json --print
    "
)"
minio_versioning_verified=true
minio_object_count="$(
  printf '%s\n' "$minio_listing" | awk 'NF {count++} END {print count + 0}'
)"
(( minio_object_count >= minimum_minio_objects )) \
  || fail \
    "restored MinIO objects $minio_object_count are below $minimum_minio_objects"
log "validated MinIO versioning and $minio_object_count parsed filing objects"

stage="qdrant_start"
log "starting isolated Qdrant from restored storage without published host ports"
docker run -d \
  --name "$QDRANT_CONTAINER" \
  --network "$NETWORK" \
  --label trade.restore-drill="$DRILL_ID" \
  -v "$RESTORED_DIR/qdrant:/qdrant/storage" \
  "$qdrant_image" >/dev/null
containers+=("$QDRANT_CONTAINER")

qdrant_ready=false
for _ in {1..60}; do
  if docker run --rm --network "$NETWORK" \
    --entrypoint curl "$api_image" \
    -fsS "http://$QDRANT_CONTAINER:6333/collections" >/dev/null 2>&1; then
    qdrant_ready=true
    break
  fi
  sleep 2
done
[[ "$qdrant_ready" == true ]] || fail "isolated Qdrant did not become ready"
qdrant_reachable=true

stage="golden_evaluation"
log "running locked filing golden evaluation against restored services"
golden_result="$(
  docker run --rm \
    --network "$NETWORK" \
    --env-file "$API_ENV_FILE" \
    -v "$RESTORED_DIR/data:/app/data:ro" \
    "$api_image" \
    trade-research evaluate-filing-golden \
      --dataset-path /app/evaluations/filings/infy_m1_golden.json \
      --workspace-id default
)"
read -r \
  golden_dataset_id \
  golden_case_count \
  golden_expected_fact_count \
  golden_matched_fact_count \
  golden_value_correct_count \
  golden_evidence_correct_count \
  golden_passed < <(
  python3 - "$golden_result" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(
    payload["dataset_id"],
    payload["case_count"],
    payload["expected_fact_count"],
    payload["matched_fact_count"],
    payload["value_correct_count"],
    payload["evidence_correct_count"],
    str(payload["passed"]).lower(),
)
PY
)
[[ "$golden_passed" == true ]] || fail "restored golden evaluation did not pass"

result_status="passed"
stage="completed"
exit_code=0
log "restore drill passed"
