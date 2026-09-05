# Phase 2 Storage Foundation

Status: foundation and operational tooling implemented; production evidence,
deployment, and application access remain disabled by default.

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
- The bounded canary loads validated PostgreSQL OHLCV, three representative
  long-form features, and a forward-return target; retries the load; reconciles
  exact content digests; benchmarks every WP2.7 query family; and stores a
  digest-verified immutable fixture in the datasets bucket.
- Production backup quiesces and snapshots ClickHouse when it is running. The
  isolated restore drill validates the ClickHouse migration/table set and all
  five versioned research buckets without publishing ports or using production
  paths.

## Capacity decision

The last verified Phase 0 observation recorded a 13 GB PostgreSQL database and
238 GB free on a 455 GB host. That evidence is historical and insufficient for
unbounded co-hosting. Phase 2 therefore starts with a bounded single-node
canary on the existing host: 2 CPUs, 2 GiB RAM, no public listener, and no
production application dependency. Before enabling the deployment gate, use
the read-only capacity collector to capture fresh CPU, memory, disk,
container-volume, PostgreSQL growth, and backup-headroom evidence. Record the
backup duration alongside its report. Stop the rollout if the canary or backup
headroom would reduce the host below the operating reserve agreed for Phase 1.

```bash
python scripts/collect_phase2_capacity.py \
  --projected-feature-count 100 \
  --retention-years 10 \
  --report /opt/trade/artifacts/phase2/capacity.json
```

## Local validation

Run the real service validation on a host with Docker:

```bash
export MINIO_KMS_SECRET_KEY=phase2-local:<base64-encoded-32-byte-test-key>
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
digest-verified reads, SSE-S3 writes, and deletion denial. CI generates an
ephemeral MinIO static KMS key; static keys are never used for production.

## Bounded canary

Run without `--apply` first to validate and describe the PostgreSQL source
slice. This performs no ClickHouse or object-store write:

```bash
python scripts/run_phase2_storage_canary.py \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --exchange NSE \
  --source upstox \
  --instrument-limit 25 \
  --row-limit 25000 \
  --report /opt/trade/artifacts/phase2/canary-plan.json
```

For the write run, export the dedicated Dagster ClickHouse and object-store
credentials, set both enable/write gates, review the bounds, and add `--apply`.
The script performs two identical inserts so `ReplacingMergeTree FINAL`
reconciliation proves retry safety. The report covers exchange/date scans,
percentile distributions, feature/target joins, factor aggregations, dataset
extraction, concurrent reads, and long-versus-wide-family latency.

After backup and isolated restore have captured the canary, remove only its
ClickHouse rows with the migration identity and exact reported
`source_run_id`:

```bash
CLICKHOUSE_ENABLED=true CLICKHOUSE_WRITE_ENABLED=true \
CLICKHOUSE_USERNAME=migration \
python scripts/cleanup_phase2_storage_canary.py \
  --source-run-id phase2-canary-<20-hex-digest> \
  --apply
```

The immutable datasets-bucket fixture is retained as audit and restore
evidence; the application and Dagster identities have no delete permission.

## Backup and isolated restore

`deploy/backup.sh` stops a running research-profile ClickHouse service,
archives its persistent directory as `clickhouse.tgz`, archives the shared
MinIO directory, checksums both, and restarts through its cleanup trap.
`deploy/restore-drill.sh` can still restore older Phase 1 backups without
`clickhouse.tgz`. When that archive is present, report schema version 2 also
requires:

- at least one applied ClickHouse migration and all ten analytical tables;
- a readable `ohlcv_daily` table;
- enabled versioning on raw, datasets, models, experiments, and exports;
- SHA-256 verification of every research object within the configured bound.

Keep `TRADE_RESTORE_MAX_RESEARCH_OBJECTS_PER_BUCKET` above the observed object
count. A Phase 2 exit report must show
`research_storage.clickhouse.backup_present=true`; a legacy restore without
that evidence does not satisfy the Phase 2 exit gate.

## Production rollout order

1. Capture fresh capacity and Phase 1 backup evidence.
2. Install all dedicated credentials in `/opt/trade/.env`.
   Configure `PROD_MINIO_KMS_*` for a production MinIO KMS/KES deployment;
   Phase 2 object writes intentionally fail without a configured key manager.
3. Set only `PROD_RESEARCH_STORAGE_DEPLOY_ENABLED=true` and deploy.
4. Confirm PostgreSQL and ClickHouse migration heads, role isolation, and
   object-store policy isolation.
5. Run a bounded PostgreSQL-to-ClickHouse canary and reconcile exact keys,
   counts, values, nulls, and SHA-256 manifests.
6. Run the extended backup and isolated restore drill for ClickHouse and all
   five research buckets; keep deletion and lifecycle expiration disabled.
7. Observe resource use through a full canary window. Only then consider the
   separate application-access gate.

## Remaining exit evidence

- fresh production capacity measurements produced by the collector;
- a passing bounded production canary and benchmark report;
- a schema-version-2 ClickHouse/research-bucket isolated restore report;
- production role-isolation proof;
- post-deployment CPU, memory, disk, and query-latency evidence.
