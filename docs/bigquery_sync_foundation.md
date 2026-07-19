# BigQuery synchronization foundation

PostgreSQL/TimescaleDB remains the operational source of truth. The application
connects outbound to BigQuery; neither PostgreSQL nor its loopback-only host
port is exposed to Google Cloud.

```text
PostgreSQL -> bounded Dagster extraction -> BigQuery staging
           -> idempotent MERGE -> count/watermark/schema reconciliation
```

The feature is disabled by default. It exports `ohlcv_daily`, `symbols`,
`exchange_sessions`, current `pipeline_health` work state, `ingestion_runs`,
`provider_health`, and `universe_lifecycle`. Each source read is streamed in
`BIGQUERY_BACKFILL_CHUNK_SIZE` batches. A deterministic Dagster run/partition
identity and natural-key MERGE make retries resumable and idempotent.

`ohlcv_daily` is monthly partitioned on `date`, requires a partition filter,
and is clustered by `exchange`, `instrument_key`, and `source`. Its natural key
is `(instrument_key, source, date)`. The other natural keys are `(symbol,
exchange)`, `(exchange, session_date)`, and `work_item_id`.

## Information required before activation

Supply all of the following; no successful cloud deployment is claimed until a
real synchronization and reconciliation complete:

1. Billing-enabled Google Cloud project ID.
2. BigQuery dataset name and immutable location (for example `US`).
3. Authentication choice: Application Default Credentials from an attached
   workload identity is preferred; an external service-account key file is the
   fallback.
4. The runtime service-account principal.
5. Confirmation of `roles/bigquery.jobUser` on the project and
   `roles/bigquery.dataEditor` on this dataset only.
6. Initial exchange/year backfill order and an approved chunk size.

An administrator should create the dataset before activation:

```bash
bq --location=US mk --dataset PROJECT_ID:trade_analytics
bq query --location=US --use_legacy_sql=false \
  'GRANT `roles/bigquery.dataEditor` ON SCHEMA `PROJECT_ID`.trade_analytics
   TO "serviceAccount:SERVICE_ACCOUNT_EMAIL"'
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member=serviceAccount:SERVICE_ACCOUNT_EMAIL \
  --role=roles/bigquery.jobUser
```

If a JSON key is unavoidable, place it at
`/opt/trade/secrets/gcp/service-account.json`, owned by the deployment account
with mode `0600`. Never copy it into the checkout, image, GitHub, or Dagster run
configuration. Production Compose mounts only that secret directory read-only.

```text
PROD_BIGQUERY_ENABLED=false
PROD_BIGQUERY_PROJECT_ID=PROJECT_ID
PROD_BIGQUERY_DATASET=trade_analytics
PROD_BIGQUERY_LOCATION=US
PROD_BIGQUERY_AUTH_METHOD=service_account_file
PROD_BIGQUERY_CREDENTIALS_PATH=/run/secrets/gcp/service-account.json
PROD_BIGQUERY_CREDENTIALS_DIR=/opt/trade/secrets/gcp
PROD_BIGQUERY_BACKFILL_CHUNK_SIZE=10000
PROD_BIGQUERY_RETRY_ATTEMPTS=3
```

For workload identity/ADC, set `PROD_BIGQUERY_AUTH_METHOD=adc`, leave the
credentials path empty, and do not create a key.

## Deployment and verification

1. Deploy with `PROD_BIGQUERY_ENABLED=false`; apply Alembic migrations and
   confirm the Data Console Warehouse tab says Disabled.
2. Create the dataset and IAM bindings above.
3. Validate credentials from the Dagster container with a read-only identity
   check, then set the remaining variables while the flag stays false.
4. Set `PROD_BIGQUERY_ENABLED=true`, recreate the API and Dagster services, and
   materialize `bigquery_export_sync` for one exchange/year with a small chunk.
   Dagster config accepts `exchange`, `year`, and `entities`:

```yaml
ops:
  bigquery_export_sync:
    config:
      exchange: TSX
      year: 2025
      entities:
        - ohlcv_daily
        - symbols
        - exchange_sessions
        - pipeline_health
        - ingestion_runs
        - provider_health
        - universe_lifecycle
```
5. Verify the Dagster metadata and Data Console show matching source and
   destination counts/watermarks, zero count difference, no schema drift, and a
   BigQuery job ID.
6. Query BigQuery with a partition predicate:

```sql
SELECT exchange, source, COUNT(*) AS rows, MAX(date) AS watermark
FROM `PROJECT_ID.trade_analytics.ohlcv_daily`
WHERE date >= DATE '2026-01-01' AND date < DATE '2027-01-01'
GROUP BY exchange, source;
```

7. Backfill one exchange/year at a time. Re-run the same failed Dagster run ID
   when resuming; completed partitions are skipped and incomplete work is safe
   to MERGE again.
8. After backfills reconcile, materialize the default asset daily and only then
   enable `bigquery_daily_sync_schedule` (it is stopped by default).

Failures remain durable in `bigquery_sync_runs` and
`bigquery_sync_partitions`, including error details, counts, watermarks,
inserted/updated/rejected rows, duration, retry count, schema drift, and job ID.
Disable the flag to stop all outbound writes without affecting PostgreSQL.
