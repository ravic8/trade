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

## Isolated restore drill

Run the drill against an atomically published backup, never an
`.incomplete-*` directory:

```bash
TRADE_APP_DIR=/opt/trade/app \
TRADE_ENV_FILE=/opt/trade/.env \
PROD_RESTORE_ROOT=/opt/trade/restore-drills \
PROD_RESTORE_REPORT_DIR=/opt/trade/restore-reports \
deploy/restore-drill.sh /opt/trade/backups/<timestamp>
```

The drill does not invoke the production Compose project or publish host ports.
It verifies the backup checksums, safely extracts every archive under a unique
restore directory, and re-hashes every successfully acquired source document
in the INFY manifest. Entries explicitly marked failed, or carrying an
acquisition error, are counted and skipped using the same semantics as the
production manifest importer; the current pack must verify 123 documents and
account for one failed download.
It then:

1. restores `postgres.dump` into a fresh TimescaleDB container using
   `timescaledb_pre_restore()` and `timescaledb_post_restore()`;
2. requires the restored migration to match the deployed API image;
3. requires at least 26 INFY filing documents and 1,186 approved facts;
4. boots the restored MinIO data, verifies bucket versioning through the
   application credentials, and requires at least 26 parsed filing objects;
5. boots restored Qdrant storage and verifies its API; and
6. runs the locked 13-case, 52-fact golden evaluation against the restored
   PostgreSQL and MinIO services.

The supported TimescaleDB logical-restore sequence follows the
[Timescale documentation](https://docs.timescale.com/self-hosted/latest/backup-and-restore/logical-backup/).
Temporary containers and their private Docker network are always removed. A
successful drill also removes its restored data unless
`TRADE_RESTORE_KEEP=true`; a failed drill retains the isolated directory for
diagnosis. Every execution writes a secret-free JSON result under
`/opt/trade/restore-reports`.

## Worker-termination and stale-lease recovery drill

Run the drill with the registered March 2026 consolidated XBRL filing:

```bash
TRADE_APP_DIR=/opt/trade/app \
TRADE_ENV_FILE=/opt/trade/.env \
PROD_RESILIENCE_REPORT_DIR=/opt/trade/resilience-reports \
deploy/filing-resilience-drill.py \
  739dea02-ef41-5b20-88e5-cfdf6bcb61fc \
  --workspace-id default \
  --expected-facts 59
```

The drill first requires the read-only production gate to pass. It dispatches
a bounded diagnostic task with the same late-acknowledgement and
reject-on-worker-loss settings as filing work. After the task records its exact
Celery execution child and PID, the drill validates that the process belongs
to the matched `filing-worker` container and terminates only that child. The
Celery parent remains running. The gate requires the same task identifier to
be redelivered to a replacement child, complete on its second attempt, and
leave the worker responsive to `inspect ping`.

The drill then creates a normal filing run using a unique idempotency key,
claims it with a two-second lease without starting a heartbeat, and invokes
the production stale-run recovery path after expiry. Recovery selection and
the `running -> retrying` transition occur in one workspace-scoped atomic
database statement. The recovered run must:

- complete on exactly its second claim;
- produce at least 59 candidates and approve every candidate;
- produce no validation defects; and
- contain exactly one unique approved fact ID per approved row.

No production container is stopped, no host port is published, and the
diagnostic Redis state expires automatically. Every execution writes a
secret-free JSON report under `/opt/trade/resilience-reports`. Acceptance
requires `status: passed`, `worker_termination.redelivered: true`,
`worker_termination.worker_healthy: true`, and
`stale_lease_recovery.recovered: true`.

## Human-review interrupt and resume drill

Run the drill with an email already present in `PROD_ADMIN_EMAILS` and the
configured identity header:

```bash
TRADE_APP_DIR=/opt/trade/app \
TRADE_ENV_FILE=/opt/trade/.env \
PROD_HUMAN_REVIEW_REPORT_DIR=/opt/trade/human-review-reports \
deploy/filing-human-review-drill.py \
  739dea02-ef41-5b20-88e5-cfdf6bcb61fc \
  --workspace-id default \
  --expected-facts 59 \
  --reviewer-email <allowlisted-admin-email> \
  --actor-header cf-access-authenticated-user-email
```

The drill fails before creating a run unless the reviewer is in the production
admin allowlist and the selected actor header appears in
`ADMIN_EMAIL_HEADERS`. It then submits a uniquely idempotent filing run through
the production API with `force_review=true`. Acceptance requires the LangGraph
workflow to:

1. stop at `human_review` with status `waiting_review` on its first claim;
2. release its worker identity and execution lease;
3. persist exactly 59 candidate facts, at least 59 evidence records, and zero
   validation defects in the pending review packet;
4. accept an approval through the production review API using the allowlisted
   reviewer identity and an explicit reason;
5. persist exactly one matching `review.approve` audit event;
6. resume through Celery and complete on its second claim; and
7. persist 59 unique approved facts carrying the reviewer identity and
   `review_status=approved`.

The drill intentionally creates a reviewed production filing run and updates
the current approved fact rows for that filing. The filing values are already
covered by the locked golden dataset. Every execution writes a secret-free
JSON result under `/opt/trade/human-review-reports`. Acceptance requires
`status: passed`, `interrupt.worker_lease_released: true`,
`decision.audit_verified: true`, and
`resume.worker_lease_released: true`.

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
