# Operator-supplied service endpoints

Merge `docker-compose.external.yml` with the local Compose file to use external
PostgreSQL/TimescaleDB and Redis. Supply `.env.external` privately, using
`.env.external.example` as a template, plus `COMPOSE_DATABASE_URL` and
`COMPOSE_REDIS_URL` for container endpoint overrides. The overlay requires modern
Compose support for `!override` and `!reset`; it removes local core dependencies
and does not provision the external services.

`python scripts/verify_external_services.py` exercises this topology with uniquely
owned, authenticated service containers and temporary application storage. It
checks migration/empty-state acceptance and truthful diagnostics when a required
endpoint fails. Generated credentials are redacted from evidence and only owned
resources are removed. Reports go to `output/external-services/`.

This test uses one Docker engine. Remote TLS, firewalls, access policy and service
capacity require installation-specific acceptance. ClickHouse/object-store
credentials and enabling flags are separate from the required core endpoints.
The frontend is configured independently in trade-web.
