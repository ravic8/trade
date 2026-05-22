#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 path/to/collection.snapshot" >&2
  exit 2
fi

snapshot="$1"
if [[ ! -s "$snapshot" ]]; then
  echo "Qdrant snapshot not found or empty: ${snapshot}" >&2
  exit 2
fi

compose_file="${COMPOSE_FILE:-docker-compose.prod.yml}"
env_file="${ENV_FILE:-.env.prod}"
snapshot_name="$(basename "$snapshot")"

compose() {
  docker compose --env-file "$env_file" -f "$compose_file" "$@"
}

echo "Uploading Qdrant collection snapshot ${snapshot_name}"
compose exec -T api sh -lc '
  if [ -n "$QDRANT_API_KEY" ]; then
    curl -fsS -X POST -H "api-key: $QDRANT_API_KEY" \
      -F "snapshot=@-;filename=$1" \
      "$QDRANT_URL/collections/$QDRANT_COLLECTION/snapshots/upload?priority=snapshot"
  else
    curl -fsS -X POST \
      -F "snapshot=@-;filename=$1" \
      "$QDRANT_URL/collections/$QDRANT_COLLECTION/snapshots/upload?priority=snapshot"
  fi
' sh "$snapshot_name" < "$snapshot"
echo
echo "Qdrant restore request submitted"
