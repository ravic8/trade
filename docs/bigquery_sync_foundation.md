# BigQuery production activation

PostgreSQL/TimescaleDB remains authoritative. Only Dagster connects outbound to
BigQuery; PostgreSQL remains loopback-only on the host and is never exposed to
Google Cloud.

```text
PostgreSQL -> bounded Dagster extraction -> core staging -> natural-key MERGE
           -> reconciliation -> reporting authorized views -> Looker Studio
```

## Fixed TradeChain8 resources

- Project: `tradechain8` (`528401589842`)
- Location: `US`
- Core physical dataset: `trade_chain8_analytics`
- Analyst view dataset: `trade_chain8_reporting`
- Exporter: `trade-chain8-bigquery-exporter@tradechain8.iam.gserviceaccount.com`
- Host key: `/opt/trade/secrets/gcp/service-account.json`, owner
  `inglorious:inglorious`, mode `0600`
- Container key: `/run/secrets/gcp/service-account.json`

Neither existing dataset is created, renamed, deleted, or recreated by the
application. Every activation first calls the BigQuery API for both datasets
and verifies their location is `US`. Only after both checks pass can target or
staging tables be written. All physical tables and staging tables are confined
to the core dataset. The exporter never writes reporting views.

The service-account JSON must never be printed, logged, committed, copied into
an environment variable, copied into an image, or placed in the Docker build
context. Compose mounts only the individual file, read-only, into the Dagster
daemon and optional Dagster webserver. A missing, empty, unreadable, incorrectly
permissioned, or wrong-identity key fails closed with a non-secret error.

## IAM prerequisite

The exporter has project `roles/bigquery.jobUser` and core-dataset
`roles/bigquery.dataEditor`. The mandatory reporting-dataset API check also
requires `bigquery.datasets.get`. Grant the exporter dataset-level
`roles/bigquery.metadataViewer` on `trade_chain8_reporting`. This is metadata
access, not table-row access. Without it, the preflight intentionally stops
before any write.

Do not grant the exporter `dataEditor` or `dataViewer` on the reporting dataset.
An administrator creates and authorizes reporting views. Future analysts get
project `roles/bigquery.jobUser` and reporting-dataset
`roles/bigquery.dataViewer`, with no core-dataset role.

## Production environment

Add these non-secret values to `/opt/trade/.env`; do not paste the JSON key:

```text
PROD_BIGQUERY_CREDENTIALS_FILE=/opt/trade/secrets/gcp/service-account.json
PROD_BIGQUERY_ENABLED=true
PROD_BIGQUERY_CANARY_ENABLED=false
PROD_BIGQUERY_PRODUCTION_SYNC_ENABLED=false
PROD_BIGQUERY_PROJECT_ID=tradechain8
PROD_BIGQUERY_CORE_DATASET=trade_chain8_analytics
PROD_BIGQUERY_REPORTING_DATASET=trade_chain8_reporting
PROD_BIGQUERY_LOCATION=US
PROD_BIGQUERY_AUTH_METHOD=service_account_file
PROD_BIGQUERY_CREDENTIALS_PATH=/run/secrets/gcp/service-account.json
PROD_BIGQUERY_EXPECTED_SERVICE_ACCOUNT_EMAIL=trade-chain8-bigquery-exporter@tradechain8.iam.gserviceaccount.com
PROD_BIGQUERY_BACKFILL_CHUNK_SIZE=10000
PROD_BIGQUERY_RETRY_ATTEMPTS=3
```

The three gates are independent:

- `BIGQUERY_ENABLED` permits a read-only preflight.
- `BIGQUERY_CANARY_ENABLED` permits only the fixed TSX canary asset.
- `BIGQUERY_PRODUCTION_SYNC_ENABLED` permits generic incremental/backfill work.

The daily schedule remains stopped. Forex is excluded; all Forex assets, jobs,
and schedules remain disabled.

## Exact activation checklist

1. Confirm the key without displaying it:

   ```bash
   sudo test -s /opt/trade/secrets/gcp/service-account.json
   sudo stat -c '%U:%G %a' /opt/trade/secrets/gcp/service-account.json
   ```

   Expected: `inglorious:inglorious 600`.

2. Apply the migration and recreate the API and Dagster services. Keep canary
   and production gates false.

3. Run the API preflight from the execution container (no BigQuery write):

   ```bash
   docker compose --env-file /opt/trade/.env \
     -f /opt/trade/app/docker-compose.prod.yml \
     run --rm --no-deps dagster-daemon \
     trade-research verify-bigquery-environment
   ```

   It must report the expected service-account email, project, both dataset
   names, and `US` for both locations. A permission failure on reporting means
   the metadata-only IAM prerequisite is missing.

4. Set only `PROD_BIGQUERY_CANARY_ENABLED=true`, recreate Dagster, and manually
   materialize `bigquery_tsx_ohlcv_canary`. It exports only TSX `ohlcv_daily`
   for the latest completed calendar year (2025 when run in 2026).

5. Validate in Dagster and Data Console: authenticated identity, project,
   dataset locations, source/staging/merged/destination counts, inserted,
   updated and rejected rows, zero duplicate business keys, matching minimum
   and maximum dates, matching watermarks, zero count difference, no schema
   drift, duration, retries, BigQuery job ID, and completed status.

6. Materialize the same canary asset again as a new Dagster run. Then run:

   ```bash
   docker compose --env-file /opt/trade/.env \
     -f /opt/trade/app/docker-compose.prod.yml \
     run --rm --no-deps api trade-research bigquery-canary-readiness
   ```

   Production is not eligible until the two latest runs are reconciled and
   have identical counts, date bounds, and watermarks. An idempotent rerun may
   classify existing keys as updates; it must not create duplicates.

7. As `ravi@chain8.org` (administrator), run
   `deploy/bigquery/reporting_views.sql` only after the corresponding core
   tables exist. In BigQuery Console, authorize the reporting dataset's views
   against the core dataset: core dataset > Sharing > Authorize datasets (or
   authorize the individual views). Do not run this as the exporter.

8. Verify reporting access using the admin account. When the friend's Google
   email is supplied, grant project `roles/bigquery.jobUser` and reporting
   dataset `roles/bigquery.dataViewer`; grant no core role. Configure the Looker
   Studio data sources with Viewer credentials against reporting views only.

9. Only after steps 1-8 pass, set
   `PROD_BIGQUERY_PRODUCTION_SYNC_ENABLED=true`. Execute backfills one
   exchange/year partition at a time in this order: NSE 2016-2026, TSX
   2016-2026, US 2016-2026. Keep the daily schedule stopped until every
   backfill reconciles.

   After an exchange finishes, verify all calendar years from durable
   PostgreSQL evidence with the read-only command:

   ```bash
   docker compose --env-file /opt/trade/.env \
     -f /opt/trade/app/docker-compose.prod.yml \
     run --rm --no-deps api \
     trade-research verify-bigquery-backfill US \
       --start-year 2016 \
       --end-year 2026
   ```

   The command exits zero only when every requested year has a completed run
   and partition, positive and matching source/destination counts, matching
   date bounds and watermarks, zero count difference, rejected rows and
   duplicate business keys, no schema drift or recorded errors, and a
   BigQuery job ID. It reads `bigquery_sync_runs` and
   `bigquery_sync_partitions`; it neither calls BigQuery nor writes data.
   Use `--json` for automation or evidence capture. Treat any non-zero exit as
   a stop condition for schedule activation.

   To prove a selected partition is idempotent, materialize that exact
   exchange/year a second time and require equivalent evidence from its two
   latest runs:

   ```bash
   docker compose --env-file /opt/trade/.env \
     -f /opt/trade/app/docker-compose.prod.yml \
     run --rm --no-deps api \
     trade-research verify-bigquery-backfill US \
       --start-year 2026 \
       --end-year 2026 \
       --require-idempotent-rerun
   ```

   Inserted and updated classifications may differ on the rerun. Counts,
   bounds, watermarks, rejection evidence and duplicate-key evidence must be
   identical.

10. After backfills pass, manually test one daily incremental run. Only then
    enable `bigquery_daily_sync_schedule`.

## Warehouse design and operations

`ohlcv_daily` is monthly partitioned by `date`, requires a partition predicate,
and is clustered by `exchange`, `instrument_key`, and `source`. Its natural key
is `(instrument_key, source, date)`. Other exports use their declared natural
keys and idempotent staging/MERGE. Extraction is streamed in bounded chunks;
staging tables expire and are deleted after each merge.

Failed and successful evidence is durable in PostgreSQL
`bigquery_sync_runs` and `bigquery_sync_partitions` and appears in Dagster and
the Data Console Warehouse tab. To stop all outbound writes immediately, set
both canary and production gates false and recreate Dagster; PostgreSQL is
unaffected.

Example partition-filtered validation:

```sql
SELECT exchange, source, COUNT(*) AS rows,
       COUNT(*) - COUNT(DISTINCT FORMAT('%s|%s|%s', instrument_key, source, date))
         AS duplicate_business_keys,
       MIN(date) AS minimum_date, MAX(date) AS maximum_date
FROM `tradechain8.trade_chain8_analytics.ohlcv_daily`
WHERE date >= DATE '2025-01-01' AND date < DATE '2026-01-01'
  AND exchange = 'TSX'
GROUP BY exchange, source;
```
