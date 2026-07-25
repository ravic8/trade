# Lens M1 Production Acceptance

## Purpose

This runbook closes the gap between deploying the Lens M1 application code and
operating the filing workflow with durable production infrastructure. It is
limited to Infosys (`NSE:INFY`) and must pass before M1 is declared complete or
additional NSE companies are enabled.

The production stack provides:

- PostgreSQL business state and LangGraph checkpoints;
- a dedicated Celery filing worker using Redis;
- a private MinIO object store with bucket versioning;
- a bucket-scoped MinIO application identity distinct from the root identity;
- OpenTelemetry collection and private Prometheus storage;
- optional Langfuse export using server-owned secrets; and
- fail-closed deployment checks for authentication, storage, queue, and
  checkpoint configuration.

None of MinIO, Prometheus, or the OpenTelemetry receiver publishes a host port.

## One-time server configuration

Update `/opt/trade/.env` directly on the Ubuntu server. Do not place real
credentials in Git, CI output, tickets, or chat.

Copy the Lens variables from `.env.prod.example` and replace every
`replace-with-*` value. Generate independent high-entropy values for:

- `PROD_MINIO_ROOT_USER`;
- `PROD_MINIO_ROOT_PASSWORD`;
- `PROD_FILING_S3_ACCESS_KEY_ID`; and
- `PROD_FILING_S3_SECRET_ACCESS_KEY`.

The MinIO root user and filing application access key must differ. Both secrets
must contain at least 16 characters. Keep:

```text
PROD_FILING_ENABLED=true
PROD_FILING_REQUIRE_WORKSPACE_HEADER=true
PROD_OTEL_ENABLED=true
```

Set the existing `PROD_ADMIN_EMAILS` allowlist. To make the complete M1
acceptance gate pass, also create a Langfuse project and set:

```text
PROD_LANGFUSE_ENABLED=true
PROD_LANGFUSE_PUBLIC_KEY=...
PROD_LANGFUSE_SECRET_KEY=...
```

The example permits image overrides. Pin MinIO, the MinIO client,
OpenTelemetry Collector, and Prometheus to reviewed immutable versions before
external traffic.

## Source corpus

Raw filings are not stored in Git or baked into the application image. Copy the
hash-verified pack to:

```text
/opt/trade/data/filings/nse/INFY/
```

The manifest must therefore exist at:

```text
/opt/trade/data/filings/nse/INFY/manifest.json
```

The golden evaluation definition is baked into the API image at:

```text
/app/evaluations/filings/infy_m1_golden.json
```

## Deployment behavior

`deploy/deploy.sh` now refuses a filing-enabled deployment when credentials are
missing, use example placeholders, reuse the root identity, or disable
workspace-header enforcement.

Compose bootstraps the versioned `lens-filings` bucket and attaches a
bucket-scoped policy to the application user. Repeated deployments preserve the
existing user and reapply the policy. Credential rotation should be an explicit
operator action rather than an incidental deployment side effect.

The deployment health loop requires:

- the normal API health endpoint;
- `/api/filings/health` reporting Celery, PostgreSQL checkpoints, S3 artifacts,
  and workspace enforcement;
- running filing-worker and MinIO containers;
- the existing CloudBeaver route; and
- the optional Dagster admin endpoint when that profile was already active.

## Read-only readiness gate

After deployment, run on the Ubuntu host:

```bash
DC=(
  docker compose
  --env-file /opt/trade/.env
  -f /opt/trade/app/docker-compose.prod.yml
)

"${DC[@]}" exec -T api trade-research verify-filing-production
```

The command performs no imports, candidate creation, approvals, or other
business-data writes. It checks:

- production queue, storage, telemetry, and authentication settings;
- source manifest and locked golden dataset presence;
- PostgreSQL connectivity and exact Alembic head;
- Redis connectivity;
- MinIO access and bucket versioning using application credentials;
- Celery worker response;
- OpenTelemetry collector reachability; and
- Langfuse enablement and credential presence.

It emits JSON and exits non-zero if any gate fails. Probe exceptions are
redacted so connection strings and credentials do not reach operator output.

## Backup

Run:

```bash
TRADE_APP_DIR=/opt/trade/app \
TRADE_ENV_FILE=/opt/trade/.env \
PROD_BACKUP_DIR=/opt/trade/backups \
deploy/backup.sh
```

The backup script briefly quiesces the filing worker, Dagster, CloudBeaver,
Qdrant, and MinIO before capturing the PostgreSQL dump and mutable persistent
directories. It writes into a hidden `.incomplete-*` directory, records
SHA-256 checksums, restarts every service that was previously running, and only
then atomically publishes the timestamped backup directory. A backup is not
accepted until its checksums pass and a restore has been exercised on an
isolated production-like host.

## Canary and locked evaluation

Only after the read-only gate passes:

1. import the hash-verified INFY manifest;
2. submit the March 2026 consolidated XBRL filing;
3. require a completed run with 59 candidates, 59 approved facts, and zero
   validation defects;
4. verify exact XBRL concept, context, source hash, and cited analysis;
5. process all 26 unique financial XBRL objects; and
6. run:

```bash
"${DC[@]}" exec -T api trade-research evaluate-filing-golden \
  --dataset-path /app/evaluations/filings/infy_m1_golden.json \
  --workspace-id default
```

The locked gate must pass all 13 quarters, 52 financial values, and 52 evidence
assertions. Two analysts must independently sign off the seeded dataset before
it is described as analyst-approved.

## M1 exit

M1 is complete only when the read-only gate, canary, golden evaluation,
worker-termination recovery, stale-lease recovery, human-review resume,
backup/restore drill, alert verification, and dual-analyst sign-off all pass.
