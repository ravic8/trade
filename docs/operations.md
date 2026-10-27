---
document_status: current
last_verified_commit: cfc039b95218e6bc9d0ec7e9ca0b673f564497ce
last_verified_date: 2026-10-27
owner: trade-data
replaced_by: null
---

# Operations

## Scope and current transition

The requested scope is an NSE daily-data backend using the final official
bhavcopy. The current deployment has not been changed by the local file cleanup.
Collector/provider removal and the daily NSE-source cutover require a separate
code change, tests and deployment. Do not infer a completed source migration from
removal of old reports or configuration documentation.

The existing explicit candle read for the audited historical source is:

```
GET /api/data/candles?provider=nse_official_bhavcopy&exchange=NSE&instrument_key=YF%7CRELIANCE.NS&start_date=2025-10-24&end_date=2026-10-23
```

The instrument key is an existing alias, not the origin of these source-labelled
NSE values. This source has not yet been extended by an automatic NSE collector.
The retained Parquet is an immutable audited snapshot. No synthetic candles or
corporate-action-adjusted prices are part of that snapshot.

## Storage protection

Do not delete or move `data/postgres`, any ClickHouse storage, database dumps,
Docker volumes, credentials, encryption keys or server data during file cleanup.
Runtime infrastructure directories are not CSV/Parquet exports. Existing schema
revision IDs, database names and stable instrument IDs remain compatible.

Production configuration is in `docker-compose.prod.yml`; its data mounts can be
outside the checkout. Read the configured paths before any maintenance. Never use
`docker compose down -v`, prune volumes, truncate tables, reset migrations or run
retention/deletion tasks as a repository-cleaning action.

## Deployment and recovery

Application deployment uses `deploy/deploy.sh`. It preserves existing
infrastructure by default and refuses active jobs before changing code locations.
A production deployment is not needed for deleting ignored local artifacts.

Schema migrations are managed with Alembic. `alembic current` is read-only;
`alembic upgrade head` changes schema and is an intentional deployment operation,
not a cleanup step. Do not regenerate or drop applied historical migrations.

`deploy/backup.sh` and `deploy/restore-drill.sh` provide deliberate backup and
isolated recovery workflows. Preserve external KMS keys, object-store credentials
and exact storage image versions. Never use a production database as a restore
rehearsal target. Cleanup does not modify these tools or their stored backups.

## Monitoring

The authorized app heartbeat monitors this chat through 1 November 2026 at
08:30, 12:30, 18:30 and 21:30 IST. Its instructions and small latest-state summary
live with the app automation, not as accumulating reports in this repository.
It performs read-only checks and must not repair data or trigger collectors.

Use actual loaded schedules. Before the pending source migration, the five NSE
schedules are universe refresh at 08:00 weekdays, work planning at 11:30 daily,
completed-session planning at 17:45 weekdays, workers every five minutes, and
exchange-calendar refresh at 06:00 on the first of each month. TSX, US and legacy
collectors remain disabled.

Check API/daemon availability, schedule ticks, queue progress/backoff, source
availability, canonical identities, current eligible-universe membership,
OHLCV/volume validity and PostgreSQL/ClickHouse row counts and digests. Compare
stock cohorts when the universe changes. Exchange sessions without a stock print
do not require a fabricated candle. Replica equality does not prove equality to
NSE; raw price-basis and volume disagreements must remain explicit.

After the daily NSE-source cutover, verify the correct final file's trading date,
schema, identities and complete OHLCV; preserve only deliberately retained data.
Do not let monitoring recreate the old collection of downloaded archives,
exports or audit documents in the checkout.
