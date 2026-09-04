# Phase 2 Storage Foundation

Status: partially implemented; application access and production deployment
are disabled by default.

## Authority boundary

PostgreSQL remains authoritative for provider work, operational state, daily
OHLCV, and all existing API responses. The Phase 2 stores are additive. No
current API endpoint or ingestion pipeline requires ClickHouse or the research
object buckets.

Two independent production gates are intentional:

- `PROD_RESEARCH_STORAGE_DEPLOY_ENABLED` deploys private services and applies
  schemas without sending application traffic.
- `PROD_RESEARCH_STORAGE_ENABLED` grants the API read access and Dagster write
  access after canary, RBAC, backup, and restore evidence is accepted.

## Implemented foundation

- ClickHouse LTS is pinned to `25.8.33.6-jammy`.
- Production publishes no ClickHouse host port and applies a 2 CPU/2 GiB
  default ceiling, bounded query settings, and rotated container logs.
- Dedicated `migration`, `dagster_writer`, `api_reader`, and `analyst_reader`
  identities are reconciled idempotently.
- Versioned, checksummed ClickHouse migrations create the ten Phase 2 tables.
- PostgreSQL Alembic revision `20260905_0014` creates definitions, workflow
  versions/schedules/runs, snapshots, models, experiments, artifact manifests,
  validation runs, and audit events.
- MinIO uses five versioned buckets (`raw`, `datasets`, `models`,
  `experiments`, and `exports`) with separate API-read and Dagster-write
  identities. Neither identity can delete objects.
- Python repositories deny writes by default. Object reads verify SHA-256 and
  immutable key retries reject changed content.

## Capacity decision

The last verified Phase 0 observation recorded a 13 GB PostgreSQL database and
238 GB free on a 455 GB host. That evidence is historical and insufficient for
unbounded co-hosting. Phase 2 therefore starts with a bounded single-node
canary on the existing host: 2 CPUs, 2 GiB RAM, no public listener, and no
production application dependency. Before enabling the deployment gate,
capture fresh CPU, memory, disk, container-volume, PostgreSQL growth, and
backup-duration evidence. Stop the rollout if the ClickHouse canary or backup
headroom would reduce the host below the operating reserve agreed for Phase 1.

## Local validation

Run the real service validation on a host with Docker:

```bash
docker compose --profile research up -d clickhouse minio
docker compose --profile research run --rm clickhouse-init
docker compose run --rm minio-research-init

CLICKHOUSE_ENABLED=true \
CLICKHOUSE_USERNAME=migration \
CLICKHOUSE_PASSWORD=local-migration \
python scripts/run_clickhouse_migrations.py

PHASE2_SERVICE_TESTS=1 python -m pytest \
  tests/integration/test_phase2_services.py -q
```

The CI `research-storage` job runs the migration twice and verifies table
coverage, duplicate reconciliation, read-only denial, bucket versioning,
digest-verified reads, and deletion denial.

## Production rollout order

1. Capture fresh capacity and Phase 1 backup evidence.
2. Install all dedicated credentials in `/opt/trade/.env`.
3. Set only `PROD_RESEARCH_STORAGE_DEPLOY_ENABLED=true` and deploy.
4. Confirm PostgreSQL and ClickHouse migration heads, role isolation, and
   object-store policy isolation.
5. Run a bounded PostgreSQL-to-ClickHouse canary and reconcile exact keys,
   counts, values, nulls, and SHA-256 manifests.
6. Extend backup and isolated restore drills to ClickHouse and all five
   research buckets; keep deletion and lifecycle expiration disabled.
7. Observe resource use through a full canary window. Only then consider the
   separate application-access gate.

## Remaining exit evidence

- fresh production capacity measurements;
- a bounded production canary and benchmark report;
- ClickHouse and research-bucket backup plus isolated restore proof;
- production role-isolation proof;
- post-deployment CPU, memory, disk, and query-latency evidence.
