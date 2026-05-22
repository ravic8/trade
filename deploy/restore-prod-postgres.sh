#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 path/to/postgres.dump" >&2
  exit 2
fi

archive="$1"
if [[ ! -s "$archive" ]]; then
  echo "Postgres archive not found or empty: ${archive}" >&2
  exit 2
fi

compose_file="${COMPOSE_FILE:-docker-compose.prod.yml}"
env_file="${ENV_FILE:-.env.prod}"

compose() {
  docker compose --env-file "$env_file" -f "$compose_file" "$@"
}

echo "Recreating the configured production database before TimescaleDB restore"
compose exec -T postgres sh -lc '
  dropdb --if-exists --force -U "$POSTGRES_USER" "$POSTGRES_DB"
  createdb -U "$POSTGRES_USER" "$POSTGRES_DB"
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "SELECT timescaledb_pre_restore();"
'

finish_restore() {
  compose exec -T postgres sh -lc '
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
      -c "SELECT timescaledb_post_restore();"
  ' >/dev/null || true
}
trap finish_restore EXIT

echo "Restoring Postgres archive into the fresh database"
compose exec -T postgres sh -lc \
  'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-owner --no-privileges' \
  < "$archive"
finish_restore
trap - EXIT
echo "Postgres restore complete"
