#!/usr/bin/env bash
set -euo pipefail

compose_file="${COMPOSE_FILE:-docker-compose.prod.yml}"
env_file="${ENV_FILE:-.env.prod}"
backup_root="${BACKUP_DIR:-backups/production}"
stamp="${BACKUP_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
backup_dir="${backup_root}/${stamp}"

compose() {
  docker compose --env-file "$env_file" -f "$compose_file" "$@"
}

mkdir -p "$backup_dir"

postgres_archive="${backup_dir}/postgres.dump"
echo "Creating Postgres archive at ${postgres_archive}"
compose exec -T postgres sh -lc \
  'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom --no-owner --no-privileges' \
  > "$postgres_archive"

echo "Creating Qdrant collection snapshot"
snapshot_name="$(
  compose exec -T api python - <<'PY'
from qdrant_client import QdrantClient

from trade_research.config import get_settings

settings = get_settings()
client = QdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key or None)
collections = {item.name for item in client.get_collections().collections}
if settings.qdrant_collection not in collections:
    print("__missing__")
    raise SystemExit
snapshot = client.create_snapshot(collection_name=settings.qdrant_collection)
print(snapshot.name)
PY
)"
snapshot_name="${snapshot_name//$'\r'/}"
if [[ -z "$snapshot_name" ]]; then
  echo "Qdrant did not return a snapshot name" >&2
  exit 1
fi

if [[ "$snapshot_name" == "__missing__" ]]; then
  qdrant_snapshot_name="not-created"
  echo "Qdrant collection is not created yet; skipping collection snapshot"
else
  qdrant_snapshot="${backup_dir}/${snapshot_name}"
  qdrant_snapshot_name="$(basename "$qdrant_snapshot")"
  echo "Downloading Qdrant snapshot at ${qdrant_snapshot}"
  compose exec -T api sh -lc '
    if [ -n "$QDRANT_API_KEY" ]; then
      curl -fsS -H "api-key: $QDRANT_API_KEY" \
        "$QDRANT_URL/collections/$QDRANT_COLLECTION/snapshots/$1"
    else
      curl -fsS "$QDRANT_URL/collections/$QDRANT_COLLECTION/snapshots/$1"
    fi
  ' sh "$snapshot_name" > "$qdrant_snapshot"
  echo "Removing downloaded Qdrant snapshot from the Qdrant volume"
  compose exec -T api sh -lc '
    if [ -n "$QDRANT_API_KEY" ]; then
      curl -fsS -X DELETE -H "api-key: $QDRANT_API_KEY" \
        "$QDRANT_URL/collections/$QDRANT_COLLECTION/snapshots/$1" >/dev/null
    else
      curl -fsS -X DELETE \
        "$QDRANT_URL/collections/$QDRANT_COLLECTION/snapshots/$1" >/dev/null
    fi
  ' sh "$snapshot_name"
fi

cat > "${backup_dir}/manifest.txt" <<EOF
created_at_utc=${stamp}
postgres_archive=$(basename "$postgres_archive")
qdrant_collection_snapshot=${qdrant_snapshot_name}
compose_file=${compose_file}
env_file=${env_file}
EOF

echo "Backup complete: ${backup_dir}"
