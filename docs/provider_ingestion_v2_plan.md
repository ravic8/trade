# Provider Ingestion V2 Plan

This document defines the next data-ingestion architecture for the Trade
Research app. The immediate priority is scheduled, incremental market-data
storage through Dagster. FastAPI/UI workflows come later and should reuse this
foundation instead of creating a second ingestion path.

## Goals

- Keep Dagster as the scheduler and data-pipeline orchestrator.
- Apply provider API rate limits to every outbound provider call, including
  Dagster scheduled runs, CLI runs, and later API-triggered runs.
- Store market data incrementally in TimescaleDB before building richer APIs on
  top of it.
- Add US/Canada daily equities through yfinance and high-frequency FX through
  Dukascopy without destabilizing the current Upstox NSE daily pipeline.
- Size parallelism differently for local development and production hardware.

## Non-Goals

- Do not add Celery as the first background execution layer.
- Do not introduce Spark for the first multi-provider ingestion phase.
- Do not mix daily equity features and high-frequency FX features into one ML
  dataset until both data contracts are validated.
- Do not make FastAPI perform long provider fetches inline.

## Current Baseline

Implemented today:

- Dagster `daily_research_pipeline_job` schedules and executes the NSE daily
  research path.
- Upstox NSE daily OHLCV is fetched incrementally and upserted into TimescaleDB.
- Fetch coverage, ingestion runs, validation artifacts, features, targets, and
  factor research already exist for the India daily path.
- Redis exists in `docker-compose.yml`, but it is not yet used as a provider
  rate-limit coordinator.
- Celery is not currently a project dependency.
- API chat has an in-process rate limiter, but market-data provider calls do not
  yet share a distributed limiter.

The next step should improve the active Dagster path first, then expand
providers.

## Implementation Status

Phase 1 foundation is now implemented for the current Upstox daily path:

- Provider rate-limit settings exist for Upstox, yfinance, and Dukascopy.
- The active Upstox daily and retry pipelines call a shared provider limiter
  before each historical request.
- Local/test runs can fall back to an in-memory limiter.
- Production compose is configured to require Redis-backed provider limiting.
- Provider request logs are persisted in TimescaleDB through
  `provider_request_log`.

The current implementation keeps the Upstox fetch loop synchronous while adding
the rate-limit and request-log guardrails. Async provider clients remain the
next optimization step after the production-safe limiter is deployed and
verified.

Production smoke verification is documented in
`docs/provider_ingestion_prod_smoke.md`.

## Environment Profiles

### Local Development

Observed local machine:

```text
MacBook Air M1, 2020
Memory: 8 GB
```

Use local development for correctness, small smoke runs, and UI validation.
Avoid heavy backfills and wide provider concurrency.

Recommended local limits:

```text
max_active_dagster_runs: 1
dagster_step_parallelism: 1-2
upstox_http_concurrency: 3-4
yfinance_batch_concurrency: 1
dukascopy_http_concurrency: 1-2
timescale_write_chunk_rows: 500-1000
```

### Production Ubuntu CPU

From `CPU.pdf`:

```text
CPU: AMD Ryzen 5 5600X
Physical cores: 6
Logical threads: 12
RAM: ~15 GiB
Storage: ~1.36 TiB across NVMe and SATA SSDs
GPU: NVIDIA RTX 3060, not required for ingestion
```

Use production for scheduled incremental jobs and controlled backfills. The CPU
is strong enough for async network ingestion plus Timescale writes, but the RAM
budget is still modest. Do not run Spark or many heavy Python workers by
default.

Recommended production limits:

```text
max_active_dagster_runs: 1 normal ingestion run
max_active_lightweight_runs: 2 only for read-only/report jobs
dagster_step_parallelism: 3-4
upstox_http_concurrency: 8-12
yfinance_batch_concurrency: 1-2
dukascopy_http_concurrency: 4-6
timescale_write_chunk_rows: 1000-5000
```

These are starting values, not permanent ceilings. Tune them using request
logs, memory usage, database write latency, and provider error rates.

## Architecture

The provider limiter must sit below every execution surface:

```text
Dagster schedule
  -> ingestion asset
  -> provider fetch engine
  -> Redis-backed provider rate limiter
  -> async provider client / bounded sync adapter
  -> normalized frames
  -> Timescale incremental upsert
  -> coverage, audit, and request-log tables
```

Later API-triggered workflows should call the same ingestion planning and
execution code:

```text
FastAPI request
  -> create ingestion run or trigger Dagster run
  -> same provider fetch engine
  -> same Redis-backed limiter
  -> same Timescale tables
```

## Why Dagster First, Not Celery First

Use Dagster first because the current work is scheduled data ingestion:

- Dagster already owns schedules, run history, retries, asset dependencies, and
  data-pipeline visibility.
- The current research pipeline is already modeled as Dagster assets.
- Scheduled provider ingestion, validation, feature generation, and ML dataset
  materialization belong in one lineage-aware system.

Keep Celery as an option for later, not the foundation:

- Add Celery if FastAPI needs many independent background tasks that are not
  naturally Dagster assets.
- Add Celery if provider work must be distributed across multiple machines.
- Add Celery if user-triggered tasks need low-latency queue semantics separate
  from Dagster run orchestration.

For the current single production host, Dagster plus async provider engines and
Redis rate limiting is simpler and easier to observe.

## Rate Limiting

Rate limiting is required for all provider calls, including Dagster schedules.

Use Redis because:

- It is already present in local Docker.
- It can coordinate limits across Dagster daemon, Dagster webserver, CLI, API,
  and any future worker.
- In-memory limiters are not safe once multiple processes exist.

Initial limiter design:

```text
ProviderRateLimiter.acquire(provider, endpoint_group)
  -> checks one or more Redis sliding-window/token-bucket keys
  -> waits or returns retry-after
  -> records permit decision
```

Suggested keys:

```text
provider:upstox:historical:1s
provider:upstox:historical:1m
provider:upstox:historical:30m
provider:yfinance:download:1m
provider:dukascopy:historical:1m
```

Store configured limits in app settings first. A database table can come after
the runtime behavior is stable.

Upstox starting limits:

```text
official standard limit: 50 requests/sec, 500 requests/min, 2000 requests/30min
configured default: 40 requests/sec, 400 requests/min, 1600 requests/30min
```

yfinance starting limits:

```text
batch symbols where possible
1 local batch at a time
1-2 production batches at a time
conservative minute-level limiter
adaptive backoff on HTTP/provider errors
```

Dukascopy starting limits:

```text
small chunked windows
low concurrency
adaptive backoff
separate limits for historical downloads
```

## Async Strategy

Use async for network-bound provider calls, not for CPU-heavy dataframe work.

Upstox:

```text
httpx.AsyncClient
asyncio.Semaphore(provider concurrency)
Redis limiter before every request
tenacity retry/backoff
batch Timescale writes after fetch groups complete
```

Dukascopy:

```text
httpx.AsyncClient or provider-specific async downloader
chunk by instrument/date/hour depending on source format
low semaphore limits
gap detection after each chunk group
```

yfinance:

```text
use yfinance batching through yf.download(...)
keep sync library calls isolated
wrap blocking calls with asyncio.to_thread only when needed
bound thread count tightly
```

Do not make Dagster itself responsible for per-request async scheduling. Dagster
should launch assets; the provider fetch engine should manage async I/O inside
the asset.

## Dagster Jobs

Split provider families into separate jobs so slow intraday or backfill work
does not block the India daily research path.

### India Daily Upstox

```text
india_daily_upstox_job
  -> upstox_daily_ohlcv
  -> processed_dataset_validation
  -> daily_features_v1
  -> daily_targets_v1
  -> ml_dataset_v1
  -> factor_research_v1
  -> daily_pipeline_health
```

This is the current path with a rate-limited async provider fetch underneath.

### North America Daily yfinance

```text
north_america_daily_yfinance_job
  -> yfinance_us_daily_ohlcv
  -> yfinance_canada_daily_ohlcv
  -> daily_equity_validation_v2
```

Start with storage and validation only. Feature and ML integration should be a
later explicit phase.

### FX Intraday Dukascopy

```text
fx_intraday_dukascopy_job
  -> dukascopy_fx_intraday_ohlcv
  -> fx_intraday_gap_validation
```

Initial pairs:

```text
EUR/USD
USD/JPY
USD/CAD
USD/CNY
```

Treat `BTC/USD` as a separate coverage decision after confirming provider
support and data shape.

## Schema Plan

Reuse existing tables where they fit, but stop encoding assumptions that every
dataset is Upstox, NSE, and daily.

### Add Provider Request Log

```text
provider_request_log
  id
  run_id
  provider
  endpoint_group
  request_key
  instrument_key
  symbol
  interval
  window_start
  window_end
  status_code
  status
  error_message
  retry_count
  rate_limited
  duration_ms
  created_at
```

Purpose:

- audit API use
- tune concurrency
- diagnose provider failures
- prove Dagster scheduled runs are rate-limited

### Generalize Instruments

Current `provider_instruments` can remain the provider-specific master table.
Add or evolve normalized instrument metadata:

```text
market_instruments
  instrument_id
  asset_class
  symbol
  display_symbol
  country
  exchange
  currency
  active
```

Keep provider-specific identifiers in `provider_instruments`.

### Daily OHLCV

Current `ohlcv_daily` already has `source`, `exchange`, `instrument_key`, and
`date`. Extend usage to yfinance daily US/Canada if the uniqueness constraints
fit.

Required checks before yfinance:

```text
instrument_key format supports Yahoo symbols
exchange/country metadata is explicit
currency is captured or derivable
corporate-action adjusted vs raw close is represented
```

### Intraday OHLCV

Add a dedicated Timescale hypertable:

```text
ohlcv_intraday
  instrument_key
  source
  exchange
  asset_class
  interval
  ts
  open
  high
  low
  close
  volume
  created_at
  updated_at
```

Primary key:

```text
instrument_key, source, interval, ts
```

This keeps high-frequency FX separate from daily equities.

### Generalized Fetch Coverage

Current `daily_ohlcv_fetch_coverage` is useful but daily-specific. Add a
general table when the second provider/frequency lands:

```text
ingestion_fetch_coverage
  run_id
  provider
  instrument_key
  symbol
  asset_class
  exchange
  interval
  window_start
  window_end
  expected_rows
  observed_rows
  missing_rows
  status
  error_message
  created_at
```

Keep `daily_ohlcv_fetch_coverage` for the current Upstox path until migration is
worth doing.

## Configuration Plan

Add settings with environment-specific defaults:

```text
INGESTION_PROFILE=local|prod
REDIS_URL=redis://localhost:6379/0

UPSTOX_HISTORICAL_CONCURRENCY=4 local, 10 prod
UPSTOX_RATE_PER_SECOND=40
UPSTOX_RATE_PER_MINUTE=400
UPSTOX_RATE_PER_30_MINUTES=1600

YFINANCE_BATCH_CONCURRENCY=1 local, 2 prod
YFINANCE_RATE_PER_MINUTE=30 initial

DUKASCOPY_HISTORICAL_CONCURRENCY=2 local, 5 prod
DUKASCOPY_RATE_PER_MINUTE=60 initial

TIMESCALE_WRITE_CHUNK_ROWS=1000 local, 5000 prod
```

Use conservative defaults in code and override in `.env` or production
environment files.

## Implementation Phases

### Phase 1: Rate-Limited Upstox Under Dagster

Deliverables:

- Redis-backed provider limiter.
- Provider request logging.
- Settings for local/prod concurrency.
- Async Upstox historical client or an adapter around the existing client.
- Current `upstox_daily_ohlcv` Dagster asset uses the limiter.
- Existing Upstox Timescale upsert behavior remains unchanged.

Verification:

```text
.venv/bin/python -m pytest tests/test_upstox_provider.py
.venv/bin/python -m pytest tests/test_daily_ohlcv_pipeline.py
.venv/bin/python -m pytest tests/test_dagster_daily_assets.py
.venv/bin/python -m ruff check
```

Manual run:

```text
.venv/bin/dagster job execute -m trade_research.dagster.definitions -j daily_research_pipeline_job
```

Acceptance:

- Ingestion remains incremental.
- Provider request log records every Upstox request.
- Redis limiter gates each request.
- No FastAPI route is required for this phase.

### Phase 2: Dagster Concurrency Profiles

Deliverables:

- Local and production Dagster run/executor configuration notes.
- Concurrency pool names for provider assets.
- One active ingestion run by default.
- Higher production step parallelism only for independent steps.

Implemented in this branch:

- `fetch-upstox-nse-daily` and `retry-upstox-nse-daily` use an async Upstox
  historical client under a bounded `asyncio.Semaphore`.
- `UPSTOX_HISTORICAL_CONCURRENCY` controls the default provider HTTP
  concurrency; CLI runs can override it with `--max-concurrent-fetches`.
- The Redis/in-memory provider limiter is still acquired immediately before
  every Upstox historical request.
- `provider_request_log` still receives one row per attempted request, including
  async success/error attempts.
- Dagster keeps orchestration coarse-grained: the `upstox_daily_ohlcv` asset
  reads the configured Upstox concurrency, while `dagster_home/dagster.yaml`
  queues runs with `max_concurrent_runs: 1`.
- Local compose defaults to `UPSTOX_HISTORICAL_CONCURRENCY=4`; production
  compose defaults to `PROD_UPSTOX_HISTORICAL_CONCURRENCY=10`.

Smoke examples:

```text
trade-research fetch-upstox-nse-daily \
  --limit 3 \
  --from-date 2026-07-01 \
  --to-date 2026-07-03 \
  --store-db \
  --max-concurrent-fetches 3

trade-research retry-upstox-nse-daily \
  --limit 10 \
  --statuses failed,no_rows \
  --max-concurrent-fetches 3
```

Acceptance:

- Local Mac does not exceed 8 GB memory during smoke runs.
- Production can fetch faster without increasing provider errors or DB lock
  pressure.

### Phase 3: yfinance Daily US/Canada Storage

Deliverables:

- yfinance provider adapter.
- US and Canada instrument universe inputs.
- yfinance daily OHLCV scheduled assets.
- Daily storage into `ohlcv_daily`.
- Daily validation and coverage artifacts.

Phase 3A implemented contract:

- Seed universes: `us_seed` and `canada_seed`, each with 20 large/liquid names.
- CLI command: `trade-research fetch-yfinance-daily --universe us_seed|canada_seed`.
- yfinance daily downloads are batched and pass through the shared provider
  limiter as `provider=yfinance`, `endpoint_group=download`.
- `provider_request_log` records one row per attempted yfinance download batch.
- Raw daily OHLCV is stored in existing `ohlcv_daily` with `source=yfinance`
  and `exchange=US` or `CA`.
- Adjusted close storage is deliberately deferred because the current
  `ohlcv_daily` contract stores one close column. The filter/preview phase
  should decide whether to add adjusted close, raw close plus adjusted close, or
  a separate adjustment table.
- Dagster assets `yfinance_us_daily_ohlcv` and `yfinance_canada_daily_ohlcv`
  are available through `north_america_daily_yfinance_job`, with the schedule
  stopped by default.

Smoke examples:

```text
trade-research fetch-yfinance-daily \
  --universe us_seed \
  --limit 5 \
  --from-date 2026-07-01 \
  --to-date 2026-07-03 \
  --batch-size 5 \
  --store-db

trade-research fetch-yfinance-daily \
  --universe canada_seed \
  --limit 5 \
  --from-date 2026-07-01 \
  --to-date 2026-07-03 \
  --batch-size 5 \
  --store-db
```

Acceptance:

- US/Canada daily rows are stored incrementally.
- yfinance requests are batched and rate-limited.
- Existing NSE Upstox pipeline remains independent.

### Phase 4: Dukascopy FX Intraday Storage

Deliverables:

- Dukascopy provider adapter.
- FX instrument registry.
- `ohlcv_intraday` hypertable.
- FX intraday scheduled asset.
- Gap validation by pair/interval/window.

Acceptance:

- EUR/USD, USD/JPY, USD/CAD, and USD/CNY are stored incrementally.
- Each request is rate-limited and logged.
- Backfills are chunked and resumable.

### Phase 5: APIs And UI

Only after scheduled storage is stable:

- Generalize provider capabilities endpoint.
- Generalize data availability endpoint.
- Add provider request/run observability views.
- Add controlled API-triggered ingestion only if needed.

At that point, decide whether API-triggered work should launch Dagster runs or
use a separate queue. Do not add Celery before this decision point.

## Operational Rules

- Provider calls must go through the limiter. No direct `httpx` or yfinance
  calls from pipeline code.
- Scheduled jobs should fetch only missing windows unless an explicit backfill
  mode is selected.
- Backfills must run separately from daily incremental jobs.
- Production backfills should use lower priority and lower concurrency than
  daily updates.
- Request logs must include failures and rate-limited waits.
- Data quality/gap checks must run before downstream features consume a new
  provider dataset.

## Open Decisions

- Exact yfinance universe source for US and Canada.
- Whether yfinance stores adjusted close, raw close, or both.
- Dukascopy data format and supported coverage for `BTC/USD`.
- Whether to keep provider rate-limit configs only in settings or promote them
  to a database table later.
- Whether API-triggered ingestion should trigger Dagster runs or use a separate
  worker queue after scheduled storage is stable.
