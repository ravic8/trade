#!/usr/bin/env bash
set -euo pipefail

compose_file="${COMPOSE_FILE:-docker-compose.prod.yml}"
env_file="${ENV_FILE:-.env.prod}"

compose() {
  docker compose --env-file "$env_file" -f "$compose_file" "$@"
}

echo "Production Compose services"
compose ps

echo "Postgres readiness"
compose exec -T postgres sh -lc 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'

echo "API health"
compose exec -T api curl -fsS http://localhost:8000/api/health
echo

echo "Chat health"
compose exec -T api curl -fsS http://localhost:8000/api/chat/health
echo

echo "Qdrant collections"
compose exec -T api python - <<'PY'
from qdrant_client import QdrantClient

from trade_research.config import get_settings

settings = get_settings()
collections = QdrantClient(url=settings.qdrant_url, api_key=settings.qdrant_api_key or None)
print(", ".join(item.name for item in collections.get_collections().collections) or "(none)")
PY
