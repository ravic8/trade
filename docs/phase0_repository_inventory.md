---
document_status: current
last_verified_commit: afbc5dc1f78803752d013a6db99a76293d01d61e
last_verified_date: 2026-07-24
owner: trade-research-platform
replaced_by: null
---

# Phase 0 Repository, Pipeline, Provider, and Schedule Inventory

This inventory records repository capability and the read-only production
state verified on the Ubuntu host on 24 July 2026. Production observations are
point-in-time evidence, not desired configuration.

## Status vocabulary

- **Implemented:** code and tests exist.
- **Feature-gated:** implemented but disabled unless environment flags allow it.
- **Repository stopped:** Dagster default is stopped.
- **Production verified:** host, PostgreSQL, and Dagster-instance evidence was
  inspected directly.
- **Stale origin:** the active Dagster schedule record belongs to the previous
  repository-origin identifier retained in local Dagster SQLite storage.
- **Historical:** retained for compatibility or comparison.
- **Proposed:** described by milestone plans but not implemented.

## Services

| Service | Local Compose | Production Compose | Role | Repository status |
|---|---:|---:|---|---|
| API | yes | yes | FastAPI application and control/query routes | implemented |
| Web | yes | yes | React application | implemented |
| Dagster daemon | yes | yes | Scheduled orchestration | implemented |
| Dagster webserver | yes | optional admin profile | Private orchestration UI | implemented |
| PostgreSQL/TimescaleDB | yes | yes | Operational and canonical structured data | implemented |
| Redis | yes | yes | Provider rate limiting and coordination | implemented |
| Qdrant | yes | yes | Retrieval experiments | partial |
| DBeaver/CloudBeaver | DBeaver-named local service | CloudBeaver | SQL analysis access | implemented |
| BigQuery | external | external | Optional outbound analytics replica | feature-gated |
| ClickHouse | no | no | Proposed research analytical plane | proposed |
| S3/MinIO | no | no | Proposed immutable artifact plane | proposed |

## Dagster assets

| Group | Asset | Provider/input | Destination/output | Status |
|---|---|---|---|---|
| analytics exports | `bigquery_export_sync` | PostgreSQL analytics entities | BigQuery | implemented, gated |
| analytics exports | `bigquery_tsx_ohlcv_canary` | PostgreSQL TSX OHLCV | BigQuery | implemented, manual/gated |
| exchange calendars | `nse_exchange_sessions` | calendar library/exchange rules | PostgreSQL | implemented |
| exchange calendars | `tsx_exchange_sessions` | calendar library/exchange rules | PostgreSQL | implemented |
| exchange calendars | `us_exchange_sessions` | calendar library/exchange rules | PostgreSQL | implemented |
| equity universes | `nse_universe_snapshot` | NSE universe provider | PostgreSQL | implemented |
| equity universes | `tsx_universe_snapshot` | official TSX source/reconciliation | PostgreSQL | implemented |
| equity universes | `us_universe_snapshot` | US/Nasdaq universe provider | PostgreSQL | implemented |
| yfinance queue | `yfinance_daily_work_plan` | accepted universes/sessions | PostgreSQL work queue | implemented, gated |
| yfinance queue | `yfinance_nse_completed_session_work_plan` | NSE accepted universe/sessions | PostgreSQL work queue | implemented, gated |
| yfinance queue | `yfinance_daily_work_worker` | PostgreSQL work claims/yfinance | PostgreSQL OHLCV and run evidence | implemented, gated |
| daily research | `upstox_daily_ohlcv` | Upstox | PostgreSQL + Parquet | historical/implemented |
| daily research | `nse_daily_ohlcv` | selected NSE primary | PostgreSQL + Parquet validation boundary | implemented, provider-neutral |
| North America daily | `yfinance_us_daily_ohlcv` | seeded US yfinance universe | PostgreSQL + Parquet | implemented direct path |
| North America daily | `yfinance_canada_daily_ohlcv` | seeded Canada yfinance universe | PostgreSQL + Parquet | implemented direct path |
| Opportunities | `nse_opportunity_targets_v1` | yfinance daily OHLCV | PostgreSQL targets | implemented |
| Opportunities | `nse_completed_session_opportunity_targets` | yfinance daily OHLCV | PostgreSQL targets | implemented, coverage-gated |
| Opportunities | `tsx_opportunity_targets_v1` | yfinance daily OHLCV | PostgreSQL targets | implemented |
| Opportunities | `us_opportunity_targets_v1` | yfinance daily OHLCV | PostgreSQL targets | implemented |
| FX intraday | `dukascopy_fx_intraday_ohlcv` | Dukascopy | PostgreSQL + artifact | implemented, disabled by default |
| FX intraday | `fx_intraday_gap_validation` | Dukascopy artifact | CSV/JSON validation | implemented |
| FX intraday | `yfinance_fx_crypto_intraday_ohlcv` | yfinance | PostgreSQL + artifact | implemented, disabled by default |
| FX intraday | `yfinance_fx_intraday_gap_validation` | yfinance artifact | CSV/JSON validation | implemented |
| daily research | `daily_features_v1` | Timescale NSE OHLCV | PostgreSQL + Parquet | implemented |
| daily research | `daily_targets_v1` | Timescale NSE OHLCV | PostgreSQL + Parquet | implemented |
| daily research | `processed_dataset_validation` | generated OHLCV/features/targets | local validation artifacts | implemented |
| daily research | `ml_dataset_v1` | generated validated artifacts | local Parquet/JSON | implemented with static-universe caveat |
| daily research | `factor_research_v1` | generated ML/features/targets | local CSV/JSON | implemented |
| daily research | `daily_pipeline_health` | generated validation/research artifacts | local report + PostgreSQL coverage | implemented |

## Dagster jobs

| Job | Assets | Intended role | Production state |
|---|---|---|---|
| `bigquery_export_sync_job` | BigQuery export | Scheduled reporting replica | Latest 24h run succeeded; 103,714 rows, no rejects/duplicates |
| `bigquery_tsx_ohlcv_canary_job` | TSX BigQuery canary | Manual readiness | Canary disabled in production |
| `daily_research_pipeline_job` | NSE OHLCV through health | Daily NSE research | Running from stale origin; latest run failed at `ml_dataset_v1` because stock-coverage artifact is missing |
| `factor_research_job` | NSE OHLCV through factor research | Factor refresh | Factor artifacts absent |
| `north_america_daily_yfinance_job` | Direct US/Canada plus targets | North America daily refresh | Schedule stopped; durable worker keeps candles current but TSX/US targets are stale |
| `fx_intraday_dukascopy_job` | Dukascopy fetch + validation | FX/crypto intraday | Schedule stopped |
| `yfinance_fx_intraday_job` | yfinance fetch + validation | FX/crypto intraday | Schedule stopped |
| `nse_universe_refresh_job` | NSE universe | Universe refresh | Running from stale origin; latest run succeeded |
| `tsx_universe_refresh_job` | TSX universe | Universe refresh | Running from stale origin; latest run succeeded |
| `us_universe_refresh_job` | US universe | Universe refresh | Running from stale origin; latest run failed on duplicate rows in one upsert batch |
| `yfinance_daily_work_planner_job` | yfinance planner | Durable daily work planning | Running from stale origin; latest run succeeded |
| `yfinance_nse_completed_session_work_planner_job` | NSE planner | Post-grace NSE incremental planning | Running from current origin; latest run succeeded |
| `yfinance_daily_work_worker_job` | yfinance worker | Durable work execution | Running from stale origin; 288 successful Dagster ticks/runs in 24h, with partial business outcomes in PostgreSQL |
| `nse_completed_session_opportunity_targets_job` | NSE Opportunity target refresh | Post-coverage analytics | Running from current origin; 6 successful runs in 24h |
| `nse_exchange_sessions_job` | NSE calendar | Monthly calendar materialization | Running from stale origin; sessions materialized through Dec 2026 |
| `tsx_exchange_sessions_job` | TSX calendar | Monthly calendar materialization | Running from stale origin; sessions materialized through Dec 2027 |
| `us_exchange_sessions_job` | US calendar | Monthly calendar materialization | Running from stale origin; sessions materialized through Dec 2027 |

## Schedule inventory

All 15 schedule definitions use `DefaultScheduleStatus.STOPPED`.

| Schedule | Cron | Timezone | Code default | Configured-intent API | Actual Dagster |
|---|---|---|---|---|---|
| `daily_research_schedule` | `30 19 * * 1-5` | Asia/Kolkata | stopped | derived from NSE provider flags | running, stale origin; latest run failed |
| `north_america_daily_yfinance_schedule` | `30 3 * * 2-6` | Asia/Kolkata | stopped | not fully represented | stopped; no active record |
| `fx_intraday_dukascopy_schedule` | `15 * * * 1-5` | UTC | stopped | not represented | stopped; no active record |
| `yfinance_fx_intraday_schedule` | `20 * * * *` | UTC | stopped | not represented | stopped; no active record |
| `nse_universe_refresh_schedule` | `0 8 * * 1-5` | Asia/Kolkata | stopped | derived from daily yfinance flags | running, stale origin; latest run succeeded |
| `tsx_universe_refresh_schedule` | `0 8 * * 1-5` | America/Toronto | stopped | derived from daily yfinance flags | running, stale origin; latest run succeeded |
| `us_universe_refresh_schedule` | `0 8 * * 1-5` | America/New_York | stopped | derived from daily yfinance flags | running, stale origin; latest run failed |
| `yfinance_daily_work_planner_schedule` | `0 6 * * *` | UTC | stopped | derived from enabled exchanges | running, stale origin; latest run succeeded |
| `yfinance_nse_completed_session_work_planner_schedule` | `15 12 * * 1-5` | UTC | stopped | derived from NSE yfinance flags | running, current origin; latest run succeeded |
| `yfinance_daily_work_worker_schedule` | `*/5 * * * *` | UTC | stopped | derived from enabled exchanges | running, stale origin; 288 successful ticks/runs in 24h |
| `nse_completed_session_opportunity_targets_schedule` | `15 13-18 * * 1-5` | UTC | stopped | derived from NSE yfinance flags | running, current origin; 6 successful runs in 24h |
| `bigquery_daily_sync_schedule` | `30 8 * * *` | UTC | stopped | derived from BigQuery flags | running, current origin; latest run succeeded |
| `nse_exchange_sessions_schedule` | `0 6 1 * *` | Asia/Kolkata | stopped | derived from calendar flag | running, stale origin; no tick expected in 24h |
| `tsx_exchange_sessions_schedule` | `0 6 1 * *` | America/Toronto | stopped | derived from calendar flag | running, stale origin; no tick expected in 24h |
| `us_exchange_sessions_schedule` | `0 6 1 * *` | America/New_York | stopped | derived from calendar flag | running, stale origin; no tick expected in 24h |

The configured-intent API does not query the Dagster instance and does not
prove actual state. Nine active records belong to repository origin
`63a6f79ffb22770d7674fb5bae7e046523eb6332`; three belong to the current origin
`295965ac7d6fd7fad2eece0f88a23f0034a01b86`. Consequently, listing schedules
with only the current repository selector reports active stale-origin
schedules as stopped. `dagster schedule debug` and direct schedule-storage
inspection are required until Phase 1 repairs this drift.

## Pipeline/provider matrix

| Pipeline | Exchange | Provider | Trigger surfaces | Storage | CLI dependency | Main issue |
|---|---|---|---|---|---|---|
| Exchange sessions | NSE/TSX/US | calendar rules | Dagster, CLI | PostgreSQL | CLI available, not required | Active schedules use stale repository origin |
| Universe snapshots | NSE/TSX/US | exchange-specific sources | Dagster, CLI | PostgreSQL | CLI available, not required | US latest run fails on duplicate batch upsert |
| Durable daily planning | NSE/TSX/US | yfinance | Dagster, CLI | PostgreSQL work queue | CLI available, not required | Active schedule uses stale repository origin |
| Durable daily worker | NSE/TSX/US | yfinance | Dagster, CLI | PostgreSQL OHLCV | CLI available, not required | Dagster success masks partially failed business runs |
| NSE daily research boundary | NSE | Upstox primary; yfinance also enabled | Dagster | PostgreSQL + artifact | no CLI required | Mixed-provider authority; downstream asset currently fails |
| Legacy NSE direct ingest | NSE | Upstox | Dagster, API, CLI | PostgreSQL + artifact | multiple mutation surfaces | To be retired after cutover |
| North America direct daily | TSX/US | yfinance | Dagster, CLI | PostgreSQL + artifact | CLI available | Duplicates durable-path responsibility |
| Opportunity targets | NSE/TSX/US | yfinance OHLCV | Dagster, CLI | PostgreSQL | CLI available, not required | Only NSE completed-session target schedule is active; TSX/US targets are stale |
| Daily features | NSE | selected primary OHLCV | Dagster, CLI | PostgreSQL + artifact | CLI available | UI reads artifact, not DB |
| Daily targets | NSE | selected primary OHLCV | Dagster, CLI | PostgreSQL + artifact | CLI available | UI reads artifact, not DB |
| Processed validation | NSE | generated artifacts | Dagster, CLI | local artifacts | CLI available | Artifact-coupled |
| ML dataset v1 | NSE | generated artifacts | Dagster, CLI | local artifacts | CLI available | Static future-aware universe |
| Factor research | NSE | generated artifacts | Dagster, CLI | local artifacts | CLI available | Artifact-backed UI |
| Baseline/LightGBM | NSE | ML artifacts | CLI only | local artifacts | yes | Not Dagster-orchestrated |
| Prediction backtests | NSE | model artifacts | CLI only | local artifacts | yes | Not Dagster-orchestrated |
| Latest predictions | NSE | model artifacts | CLI only | local artifacts | yes | Not Dagster-orchestrated |
| BigQuery export | all supported | PostgreSQL | Dagster, CLI readiness | BigQuery + PostgreSQL state | no CLI required | Enabled; latest daily sync succeeded |

## Storage inventory

### PostgreSQL/TimescaleDB

Implemented domains include:

- instruments and provider mappings;
- OHLCV;
- ingestion and provider request runs;
- provider credentials;
- stock coverage;
- exchange sessions;
- universe snapshots/members/reconciliation;
- lifecycle events;
- durable work queue and attempt history;
- adaptive rate state;
- provider-history evidence;
- Opportunity targets;
- BigQuery sync runs and partitions;
- features and targets.

The single `storage/timescale.py` module currently contains these domains.

### Local `data/` and `artifacts/`

Generated files are mounted into API and Dagster containers. They currently
serve as production inputs for Research Progress, Factors, and Models APIs.
They are not a transactional or immutable artifact registry.

### BigQuery

BigQuery synchronization is outbound-only and independently gated. PostgreSQL
remains authoritative.

### Proposed stores

ClickHouse and object storage exist only in proposed plans.

## Configuration inventory

The single `Settings` class includes:

- API/chat/security;
- PostgreSQL/Redis/Qdrant;
- BigQuery;
- Upstox;
- yfinance rate controls and cutover flags;
- universe and exchange-session controls;
- FX/intraday controls;
- backlog and feed health;
- operational limits.

Important repository defaults:

```text
yfinance_daily_enabled=false
yfinance_nse_enabled=false
yfinance_full_tsx_enabled=false
yfinance_full_us_enabled=false
nse_daily_primary_source=upstox
legacy_upstox_nse_enabled=true
materialized_exchange_sessions_enabled=false
bigquery_enabled=false
forex_pipelines_enabled=false
```

These defaults are safe but do not represent production `.env` values.

## CI inventory

Current CI:

- Ruff;
- Pytest;
- frontend lint;
- frontend production build;
- production Compose configuration;
- Docker build-definition checks.

Missing milestone gates:

- mypy execution;
- coverage threshold;
- PostgreSQL/Timescale integration service;
- Redis integration service;
- Alembic previous-version migration;
- frontend unit tests;
- browser smoke tests;
- dependency/container vulnerability scans;
- future ClickHouse migrations and query contracts.

## Repository truth conclusion

The repository contains substantially more production and yfinance capability
than older handoff documents describe. Authenticated UI evidence confirms that
the durable yfinance worker is active and that Upstox remains in use. It also
contains more production fallback and manual mutation surfaces than the
intended target architecture allows. Host-level schedule, tick, container, and
database evidence remains outstanding and cannot be inferred from code or UI
intent badges.
