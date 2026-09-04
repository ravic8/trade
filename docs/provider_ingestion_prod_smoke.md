# Provider Ingestion Production Smoke

Use this checklist after deploying the provider rate-limit foundation and before
adding new providers or async concurrency.

## Purpose

Verify that production Dagster scheduled ingestion:

- starts with Redis-backed provider rate limiting enabled;
- initializes the new `provider_request_log` table;
- keeps the Upstox daily OHLCV path incremental;
- records provider request logs for each real Upstox historical request;
- leaves the existing downstream validation/features/targets path working.

## Preconditions

- Production `.env` has the normal database, secret, and Upstox values.
- `redis` service is enabled in `docker-compose.prod.yml`.
- The production image has been rebuilt because `redis>=5.2` is now a Python
  dependency.
- Generated data directories are mounted as expected.

Important production defaults:

```text
REDIS_URL=redis://redis:6379/0
PROVIDER_RATE_LIMIT_BACKEND=redis
PROVIDER_RATE_LIMIT_REQUIRE_REDIS=true
UPSTOX_RATE_PER_SECOND=40
UPSTOX_RATE_PER_MINUTE=400
UPSTOX_RATE_PER_30_MINUTES=1600
```

The strict Redis setting is intentional. Production should fail visibly if
Redis is unavailable instead of running uncoordinated provider calls.

## Smoke Steps

### 1. Rebuild And Start Core Services

```bash
docker compose -f docker-compose.prod.yml build api dagster-daemon
docker compose -f docker-compose.prod.yml up -d postgres redis qdrant api dagster-daemon
```

If using the admin Dagster UI:

```bash
docker compose -f docker-compose.prod.yml --profile admin up -d \
  --build \
  --no-deps \
  dagster-webserver
```

Once the admin webserver is running, `deploy/deploy.sh` detects it and includes
its image refresh, container recreation, and loopback HTTP check in subsequent
deployments. The deploy script does not start the admin profile when it is
stopped.

### 2. Apply the versioned schema migration

```bash
docker compose -f docker-compose.prod.yml run --rm api alembic upgrade head
```

This is the deployment-authorized schema path. The guarded maintenance CLI is
not a production migration mechanism.

### 3. Confirm the production CLI guard

Run a harmless invocation of a mutating command without valid input. The guard
must stop it before argument handling or database access:

```bash
docker compose -f docker-compose.prod.yml exec api \
  trade-research init-db
```

Expected: exit code `78` and a `Blocked mutating CLI command` message. Do not
use a CLI bypass for the provider smoke.

### 4. Inspect Provider Request Logs

Inspect the latest Upstox/NSE run:

```bash
docker compose -f docker-compose.prod.yml exec api \
  trade-research provider-request-log --provider upstox --exchange NSE --limit 20
```

For a specific run:

```bash
docker compose -f docker-compose.prod.yml exec api \
  trade-research provider-request-log --run-id <run_id> --provider upstox --endpoint-group historical
```

Pass criteria:

- summary table has `provider=upstox` and `endpoint=historical`;
- request count matches attempted fetch windows;
- failures, if any, appear in the summary and recent rows;
- `wait seconds` is present and non-negative;
- the command does not print `No provider request logs found`.

### 5. Observe or launch the Dagster daily job

Use the authenticated Dagster UI to launch `daily_research_pipeline_job`, or
observe the next reconciled schedule tick. The daemon invokes package services
directly; do not shell into a container to execute the maintenance CLI.

If the job is already current, Upstox may have zero actual provider calls. That
is valid for incremental behavior. A bounded production canary must be a
Dagster launch with recorded run lineage.

### 6. Inspect Dagster Run Logs

Use the same provider request command after the Dagster run:

```bash
docker compose -f docker-compose.prod.yml exec api \
  trade-research provider-request-log --provider upstox --exchange NSE --limit 20
```

Expected:

- If the run fetched missing windows, provider requests are logged.
- If the run was `completed_empty`, no provider request logs for that run is
  acceptable, but fetch coverage should show skipped/current rows.

## Failure Interpretation

### Redis Connection Failure

Production is configured with `PROVIDER_RATE_LIMIT_REQUIRE_REDIS=true`.

If a run fails before fetching and mentions Redis:

- confirm `redis` service is running;
- confirm `REDIS_URL=redis://redis:6379/0` inside `api` and Dagster containers;
- restart `dagster-daemon` after Redis is healthy.

Do not disable Redis strict mode in production except for emergency diagnosis.

### No Provider Logs After Actual Fetches

If rows were fetched but `provider-request-log` is empty:

- confirm `alembic upgrade head` completed during deployment;
- check that the run used the updated image;
- check database permissions for inserting into `provider_request_log`.

### Provider 429 Or Rate Errors

If request logs show Upstox 429 or rate-limit errors:

- reduce `PROD_UPSTOX_RATE_PER_SECOND`;
- reduce `PROD_UPSTOX_RATE_PER_MINUTE`;
- keep the 30-minute limit below the official maximum;
- rerun a small smoke before enabling the full schedule.

## Next After Passing

After the smoke passes:

1. Enable or manually run the normal Dagster daily schedule.
2. Observe at least one regular production run.
3. Then begin Phase 2: async Upstox fetch execution with bounded concurrency.
