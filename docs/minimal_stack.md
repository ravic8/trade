# Minimal daily backend stack

The default local Compose project runs **API, PostgreSQL/TimescaleDB and Redis**.
No frontend or provider job starts with this core. API startup checks the required
services; run migrations before using database-backed operations.

```sh
cp .env.example .env # first installation only; preserve an existing configuration
# Requires a running Docker daemon and Buildx.
docker compose up -d --build
docker compose exec api python -m alembic upgrade head
docker compose exec api trade-data doctor --check-services
curl -f http://localhost:8000/api/health
```

| Profile | Additional services | Purpose |
| --- | --- | --- |
| `ingestion` | Dagster webserver and daemon | Daily orchestration; provider execution still needs explicit flags |
| `research` | ClickHouse, access/migration bootstrap, MinIO, bucket bootstrap | Daily analytical replica and raw evidence; historical profile name retained |
| `observability` | OpenTelemetry collector and Prometheus | Optional operational telemetry |
| `admin` | CloudBeaver | Private database inspection |

Starting a profile does not enable ingestion. Configure NSE/yfinance flags,
materialized sessions and worker write roles as described in the root README;
then validate a bounded canary before schedule reconciliation. Code defaults keep
all schedules stopped. TSX and US remain disabled.

The operations UI has its own build and deployment in trade-web. For local
browser work, start it separately with its API URL/proxy configuration; see
[frontend integration](frontend_integration.md). No Node build is needed here.

Profiles do not stop previously running services. Preserve existing database and
object-store volumes. Avoid orphan deletion during repository cutover because
older frontend or archived service containers may still be running. Production
rollouts use a separate topology and remain pending acceptance; see the
[implementation journal](repository_split_progress.md).
