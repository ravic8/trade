---
document_status: current
last_verified_commit: 7f76df0131911988de377d3df5c974aa0c07e91b
last_verified_date: 2026-10-27
owner: trade-data
replaced_by: null
---

# Operations

## Official NSE source

Use the official CM UDiFF final bhavcopy:
`https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip`.
The parser verifies the archive member, trading date, exchange, segment and schema,
classifies every record, rejects invalid OHLCV and retains exact integer volume.
NSE describes the final file on its [reports page](https://www.nseindia.com/all-reports).
Files are downloaded in bounded memory without creating local CSV/ZIP/Parquet dumps.

`nse_bhavcopy_sessions` records each session's file URL/checksum, expected count,
exclusions, database counts, business digest and completion/failure state. Only a
comparison of both replicas against actual NSE records establishes completeness.
Failed imports remain retryable; publication in one database is not success.
The new authoritative publication is versioned as `nse_bhavcopy`. Earlier
`nse_official_bhavcopy` audit/import records and all other source histories remain
untouched. This separate publication avoids treating current-ticker historical
aliases as additional exchange prints. Original database source labels are preserved. On closed calendar dates the evening
job makes a bounded final-file check: an official dated final file proves a special
session; a 404 is an ordinary absent file, while access failures remain errors. The
job never invents market opening/closing hours from this observation. Applied migrations, old columns and
stable aliases such as `YF|RELIANCE.NS` are compatibility identifiers, not data feeds.

The daily scope includes main-market and SME ordinary equities, including BE/BZ.
Funds/debt/rights/partly paid securities are excluded explicitly. Zero volume is a
separate flag, not automatically an invalid bar. No-print sessions never receive
synthetic candles. New securities are discovered from the file; unresolved issuer
issuer changes fail closed and require review. Missing identities are established
from official records. Same-ticker ordinary-equity serial changes retain a stock's
stable alias only within the same NSDL issuer code; the raw dated ISIN is preserved
and each non-exact binding is recorded in the session proof. This never adjusts
prices or volume. See [NSDL's ISIN structure](https://nsdl.co.in/downloadables/pdf/Demat%20and%20Remat%20of%20Securities%2001062021.pdf). The frozen main-market audited
export remains unchanged and has a different scope from live collection.

## Jobs and schedules

Only two schedules are loaded, both Asia/Kolkata:

- `nse_daily_bhavcopy_schedule`: every half hour, 18:00–23:30, every day, to cover
  ordinary and special sessions. Uses the validated completed-session calendar and
  a 120-minute publication grace; unavailable files retry at the next checkpoint.
- `nse_exchange_sessions_schedule`: 06:00 on the first of each month.

`nse_one_year_repair_job` is manual and resumable. It source-verifies each exchange
session from the day after the prior-year anniversary through the latest eligible
session, skipping only sessions with a completed durable source proof. Daily and
historical jobs serialize the same session with PostgreSQL advisory locks.
Successful replay upserts the same logical keys; it does not duplicate daily bars.
Source revisions to the latest file are rechecked by the scheduled job.

`NSE_DAILY_ENABLED` controls the daily schedule. `NSE_PUBLICATION_GRACE_MINUTES`
(default 120), `NSE_RETRY_LOOKBACK_DAYS` (7) and `NSE_REQUEST_INTERVAL_SECONDS`
(1, minimum 0.5) control bounded work. Both database write planes must be enabled
for ingestion. Object-store provenance snapshots use existing storage when enabled.
Retired schedules are stopped during deployment without deleting their run history.
No alternate-provider, TSX or US collectors are loaded or credentialed by the app.

## API and health

`GET /api/data/candles?instrument_key=YF%7CRELIANCE.NS&start_date=2026-10-01&end_date=2026-10-27`
reads NSE source-labelled data only. Unsupported providers are rejected. Use
`/api/data/instruments/search` to find stable keys, `/api/data/pipeline-health`
for per-session source coverage and `/api/market/status` for stored source dates.
`/api/health` is liveness only. An unavailable coverage database returns 503;
missing coverage is never reported as healthy. Dagster failures and coverage
proofs identify publication lag, identity problems or replica mismatches.

The app monitoring heartbeat through 1 November is read-only. Its single compact
latest-state summary lives with the app automation, not in this repository.

## Deployment and storage protection

Deploy with `deploy/deploy.sh`; it preserves infrastructure, refuses active jobs,
upgrades schema, reconciles schedules and checks application health. Production
CLI mutations are blocked; use the explicit Dagster jobs for ingestion/repair.
`trade-data doctor --check-services` provides sanitized read-only diagnostics.

Never delete or move PostgreSQL/ClickHouse storage, Docker volumes, database dumps,
credentials, encryption keys or server data during cleanup. Never use volume prune,
truncate, retention or database reset commands to clean this repository. Existing
migrations and legacy table definitions remain for schema compatibility and do not
register old application functionality. The new coverage migration is additive.

`deploy/backup.sh` and `deploy/restore-drill.sh` support deliberate recovery.
Preserve external KMS keys, object-store credentials and exact storage versions.
A production database must never be a restore rehearsal target.
