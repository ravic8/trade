# Yfinance Daily Equities: Final Design and Implementation Plan

Status: Finalized
Date: 2026-07-17

## Purpose

This document is the implementation source of truth for automated daily OHLCV
ingestion for the NSE, TSX, and US equity universes. It reconciles the target
architecture with the current Trade Research codebase and defines the database,
pipeline, scheduling, retry, rate-limit, observability, rollout, and testing
work required to reach production.

Forex is explicitly outside this design. Existing Forex code may remain in the
repository, but Forex assets and schedules must not be enabled by this work.

## Final Decisions

| Area | Decision |
| --- | --- |
| Universes | NSE, TSX, and US equities |
| Candle interval | Daily (`1d`) |
| Price provider | Yfinance/Yahoo Finance |
| Historical scope | Up to ten years for symbols active today |
| Historical membership | Do not reconstruct historical exchange membership |
| Incremental loading | Scheduled after each exchange closes |
| Newly added symbols | Detect and enqueue available-history backfill |
| Removed symbols | Confirm, mark inactive, and retain stored history |
| Storage authority | TimescaleDB/PostgreSQL |
| Orchestration | Dagster |
| Rate coordination | One Redis-backed global Yahoo budget |
| Initial logical rate | 300 ticker requests/minute |
| Adaptive ceiling | 600 ticker requests/minute |
| Concurrency | Start with 4 workers; allow at most 8 |
| Retry strategy | Immediate jittered retries plus a durable retry queue |
| Missing rows | Exchange sessions minus stored valid candles |
| Manual retry | Enqueue audited work; never fetch inline in FastAPI |

Yahoo is an unofficial, best-effort provider. The provider boundary must remain
replaceable so another source can be added later without changing storage,
coverage, scheduling, or dashboard contracts.

## Goals

- Maintain a persisted current active universe for each exchange.
- Backfill up to ten years of daily data for every currently active symbol.
- Incrementally load daily candles without re-downloading full history.
- Treat holidays, weekends, pre-listing dates, and provider-pending dates
  correctly when calculating coverage.
- Detect new, removed, renamed, invalid, and temporarily unavailable symbols.
- Recover from retryable failures without routine manual intervention.
- Coordinate every Yahoo caller through one IP-wide rate budget.
- Show freshness, exact gaps, failures, queue state, and provider behaviour in
  the Data Console.
- Keep every migration reversible through the production soak period.

## Non-Goals

- Forex, crypto, or intraday equity ingestion.
- Reconstructing historical exchange membership.
- Inserting artificial null candles into the OHLCV fact table.
- Treating Upstox observations as Yahoo observations.
- Running long provider downloads synchronously in the API process.
- Introducing Spark, Celery, or another orchestration platform.
- Removing legacy paths before the replacement passes production soak.

## Current Codebase Baseline

The implementation extends the existing modular monolith rather than building a
second ingestion system.

### Components to Keep

- `src/trade_research/storage/timescale.py`: OHLCV, ingestion runs, request
  logs, coverage, holidays, symbols, instruments, and feed health.
- `src/trade_research/data/rate_limits.py`: in-memory and Redis distributed
  provider limiting.
- `src/trade_research/data/yfinance_provider.py`: yfinance download and
  normalization boundary.
- `src/trade_research/pipelines/yfinance_daily.py`: incremental/missing window
  planning, audit, upsert, and coverage foundations.
- `src/trade_research/market_calendar.py`: NSE, TSX/CA, and US calendar logic.
- `src/trade_research/universe/`: NSE, TSX, US, and Canada symbol providers.
- `src/trade_research/dagster/`: asset, job, and schedule orchestration.
- `apps/web/src/pages/DataPipelinePage.tsx`: availability, runs, request health,
  and schedule views.

### Gaps to Close

- North America schedules currently use seed universes.
- The primary NSE Dagster path currently uses Upstox.
- Universe providers are called live without persisted successful snapshots.
- Symbol upsert activates symbols, but removals are not reconciled.
- Yfinance instruments are not consistently stored in `provider_instruments`.
- Daily yfinance batching is sequential and has no candle retry policy.
- One limiter permit represents a batch even when Yahoo is called per ticker.
- Daily and intraday yfinance endpoint groups have separate limiter keys.
- Exchange sessions are not materialized as individual dates.
- No durable provider-work queue exists.
- Dagster schedules are stopped by default.
- Schema evolution uses `create_all`; no migration framework is present.

## Target Architecture

```text
Dagster exchange schedule
  -> refresh and validate universe snapshot
  -> reconcile symbol lifecycle
  -> ensure exchange-session calendar
  -> resolve latest completed provider-eligible session
  -> plan incremental, backfill, or repair work
  -> enqueue prioritized durable work items
  -> acquire global weighted Yahoo permit
  -> acquire bounded worker slot
  -> fetch with immediate retries
  -> normalize and validate each ticker
  -> idempotently upsert OHLCV and adjustments
  -> update work item, request log, feed health, and ingestion run
  -> reconcile calendar-aware coverage
  -> enqueue remaining retryable gaps
  -> expose metrics and actions through API/Data Console
```

Dagster, CLI, API-triggered manual retry, automatic retry, and initial backfill
must all produce the same work-item contract and pass through the same executor
and Yahoo governor.

## Domain Conventions

Canonical exchange identifiers are:

```text
NSE
TSX
US
```

The existing `CA` identifier is accepted temporarily as an API compatibility
alias but normalizes to `TSX`. New writes must not use `CA`.

Provider-symbol examples:

```text
NSE   RELIANCE.NS
TSX   SHOP.TO
US    AAPL
```

Provider symbols are aliases, not permanent instrument identities. Ticker
changes must preserve one canonical instrument and its history.

The candle uniqueness contract is:

```text
canonical instrument + source + interval + exchange session date
```

Download with `auto_adjust=False`. Store raw OHLCV and store adjusted-close or
corporate-action-derived values separately. Incremental overlap corrects later
Yahoo revisions by idempotent upsert.

## Universe Lifecycle Design

### Sources

| Universe | Active-symbol source | Yahoo mapping |
| --- | --- | --- |
| NSE | Official NSE equity CSV, EQ series | `<symbol>.NS` |
| US | Nasdaq Trader Nasdaq and other-listed directories | Normalized ticker |
| TSX | Current TSX list source, validated and replaceable | Normalized `.TO` |

Universe acquisition and price acquisition remain separate responsibilities.

### Snapshot Validation

Before a refresh may change lifecycle state, validate:

- Fetch and schema succeeded.
- Symbol count exceeds an absolute minimum.
- Symbol count did not change beyond a configured threshold without override.
- Duplicate provider symbols are resolved or quarantined.
- Yahoo mappings pass syntax validation.

An invalid refresh is recorded as failed and cannot deactivate symbols.

### Reconciliation Rules

- New symbol: create/reactivate, emit an event, and enqueue backfill.
- Existing symbol: refresh metadata and `last_seen_at`.
- Missing once: mark `suspected_inactive`.
- Missing from two consecutive successful snapshots: mark inactive and retain
  all candles.
- Mapping change: add an alias, emit an event, and preserve canonical identity.

### Universe Tables

```text
universe_snapshots
  snapshot_id, exchange, source, status, fetched_at, symbol_count,
  validation_json, error_message

universe_snapshot_members
  snapshot_id, canonical_instrument_id, exchange_symbol, provider_symbol,
  name, raw_metadata

instrument_aliases
  canonical_instrument_id, provider, provider_symbol, valid_from, valid_to,
  is_current

symbol_lifecycle_events
  event_id, canonical_instrument_id, exchange, event_type, old_value,
  new_value, snapshot_id, created_at
```

Existing `symbols` and `provider_instruments` remain compatibility read models
and are updated transactionally with each accepted snapshot.

## Exchange Calendar Design

Keep yearly `exchange_holidays` as the fetched source record and add:

```text
exchange_sessions
  exchange
  session_date
  is_trading_day
  market_open_utc
  market_close_utc
  is_early_close
  source_url
  calendar_version
  validation_status
  generated_at
```

Primary key: `(exchange, session_date)`.

Populate the previous ten years, current year, and at least the next year when
available. Refresh monthly and at year rollover.

Calendar inputs fail closed. Interactive and pipeline calendar requests accept
years from 1990 through the next calendar year and at most 21 inclusive calendar
years per request. An official source response with no closed or early-close
dates is unavailable data, not a valid empty calendar. Empty cached holiday
records are ignored by legacy resolution and shadow comparison, and must never
block a valid materialized calendar. A failed materialization raises from its
Dagster asset so the orchestrator records a failed run rather than a successful
asset containing failure metadata.

If an older deployment cached empty NSE records, remove only records where both
date arrays are empty after deploying these guards and taking a database backup:

```sql
DELETE FROM exchange_holidays
WHERE exchange = 'NSE'
  AND json_array_length(closed_dates) = 0
  AND json_array_length(early_close_dates) = 0;
```

Expected dates per instrument:

```text
coverage_start = max(today - 10 years, first_trade_date)
coverage_end   = latest completed provider-eligible session
expected       = open sessions between start and end
missing        = expected minus stored valid candles
```

When listing date is unavailable, the first valid Yahoo candle is the
provisional first-trade date. Pre-listing dates must not appear missing.

Coverage classifications:

```text
complete
missing_expected
provider_pending
market_not_closed
holiday
weekend
not_yet_listed
inactive_or_delisted
no_trade_or_suspended
provider_failed
```

Today's session remains `provider_pending` until the market has closed and the
Yahoo availability grace period expires.

## Work Planning and Durable Queue

Extract existing incremental and missing-window logic into a shared planner:

```text
src/trade_research/data/daily_work.py
```

```python
class DailyWorkPlanner:
    def plan_incremental(...): ...
    def plan_initial_backfill(...): ...
    def plan_new_symbol_backfill(...): ...
    def plan_gap_repair(...): ...
```

All planners produce the same durable contract:

```text
pipeline_work_items
  work_item_id
  idempotency_key
  work_type
  provider
  exchange
  canonical_instrument_id
  provider_symbol
  interval
  window_start
  window_end
  priority
  status
  attempt_count
  max_attempts
  next_attempt_at
  locked_by
  locked_at
  run_id
  parent_work_item_id
  last_status_code
  last_error_code
  last_error_message
  created_at
  updated_at
  completed_at
```

Statuses are `queued`, `running`, `retry_wait`, `succeeded`, `terminal`, and
`cancelled`. An idempotency key derived from provider, instrument, interval,
window, and work type prevents duplicate active work from schedules or UI
actions.

Priority order:

1. Current daily increment.
2. Retry of a current daily increment.
3. New-symbol backfill.
4. Missing-gap repair.
5. Initial bulk ten-year backfill.

## Backfill Design

For every symbol active in today's accepted snapshot:

```text
start = today - 10 years
end   = latest completed provider-eligible session
```

Existing valid yfinance rows are reused; the planner enqueues only missing
windows. Existing Upstox NSE rows are not relabelled as yfinance rows.

Prefer one full available-history request per symbol. Split a repeatedly failing
large window into yearly or two-year child work items. Matching windows may be
batched, but success, coverage, failure, retry, and token weight are recorded
per ticker. A partial batch commits successful tickers and retries only failures.

## Incremental Design

For each active symbol:

```text
fetch_start = latest stored valid date - 5 trading sessions
fetch_end   = latest completed provider-eligible session
```

The overlap repairs revisions, volume updates, splits, and adjustment changes.
Idempotent database upsert prevents duplicates.

Incremental workflow:

1. Refresh and persist the active universe.
2. Ensure the exchange calendar is valid.
3. Resolve the latest provider-eligible session.
4. Create idempotent work items.
5. Execute through the Yahoo governor and worker pool.
6. Normalize and validate each ticker.
7. Upsert candles and adjustments.
8. Update request, work-item, feed-health, and ingestion-run state.
9. Reconcile coverage and enqueue remaining retryable gaps.

## Yahoo Provider Executor

Replace the sequential fetch loop with a reusable executor while retaining the
current provider protocol for test doubles.

Proposed modules:

```text
src/trade_research/data/yahoo_executor.py
src/trade_research/data/provider_retry.py
src/trade_research/data/adaptive_rate.py
```

Responsibilities:

- Claim durable work safely.
- Acquire a weighted global Yahoo permit.
- Acquire a bounded concurrency slot.
- Use a worker-local curl/yfinance session.
- Run immediate retries.
- Classify provider and symbol failures.
- Normalize results per ticker.
- Persist results and metrics before acknowledging work.

Do not enable uncontrolled yfinance internal threading. Concurrency is owned by
the application executor.

## Adaptive Rate Limiting

### Test Grounding

Local probes using the installed yfinance version established:

- No Yahoo HTTP 429 through a short 1,200 logical ticker RPM burst.
- Reliability degraded at higher rates because of client/session timeouts and
  no-status outcomes.
- 600 logical ticker RPM was the highest practical tested ceiling.
- One logical ticker download can produce more than one Yahoo HTTP request.

These measurements are environment-specific, not an official Yahoo quota.

### Global Budget and Weight

All NSE, TSX, US, scheduled, backfill, automatic retry, and manual retry calls
share:

```text
provider-rate-limit:yfinance:all
```

Endpoint groups remain metrics dimensions, not independent quotas.

Extend the limiter compatibly:

```python
acquire(provider, endpoint_group, weight=1)
report(provider, outcome, duration_ms, status_code=None)
```

For a yfinance batch, weight equals the number of tickers. Every retry must
reacquire tokens.

### Controller

```yaml
initial_rpm: 300
minimum_rpm: 30
maximum_rpm: 600
initial_workers: 4
maximum_workers: 8
evaluation_window_seconds: 60
healthy_windows_before_increase: 2
increase_rpm: 30
```

Startup and adjustment rules:

```text
No history:                         start at 300 RPM
Persisted state:                    start at 80% of last-safe RPM
Two healthy windows:                add 30 RPM
Elevated network/timeout errors:    current RPM * 0.70
Provider 5xx errors:                current RPM * 0.75
HTTP 429:                           current RPM * 0.25 and cooldown
```

Respect `Retry-After`. Symbol 404s, mapping errors, empty pre-listing history,
and delistings do not lower the provider-wide rate.

Persist:

```text
adaptive_rate_state
  provider, current_rpm, last_safe_rpm, minimum_rpm, maximum_rpm,
  current_concurrency, consecutive_healthy_windows, circuit_state,
  cooldown_until, last_429_at, recent_error_rate, latency_baseline_ms,
  updated_at
```

Rollout modes are `fixed`, `observe`, and `adaptive`. Deploy `observe` before
allowing controller decisions to change the enforced rate.

## Retry and Failure Classification

Use Tenacity for immediate retry:

```text
maximum 3 attempts
full-jitter exponential waits
approximately 2s -> 5s -> 15s
```

After immediate attempts are exhausted, use durable retry waits:

```text
5m -> 15m -> 1h -> 4h -> 12h -> 24h
```

| Failure | Action |
| --- | --- |
| HTTP 429 | Respect `Retry-After`, reduce rate, open cooldown/circuit |
| 500/502/503/504 | Retry with jitter; reduce rate if systemic |
| Timeout/network | Retry; reduce rate when window threshold is exceeded |
| 404/invalid ticker | Validate mapping; terminal after classification |
| Empty response | Compare calendar and first-trade date before retrying |
| Partial batch | Commit successes and retry failed tickers only |
| Database failure | Retry the write without downloading Yahoo again |

Terminal classifications include `invalid_mapping`, `renamed`,
`inactive_or_delisted`, `unsupported_security`, and `not_yet_listed`.

## Storage and Migration Strategy

Keep:

```text
symbols
provider_instruments
ohlcv_daily
daily_price_adjustments
ingestion_runs
provider_request_log
daily_ohlcv_fetch_coverage
exchange_holidays
feed_health
```

Add:

```text
exchange_sessions
universe_snapshots
universe_snapshot_members
instrument_aliases
symbol_lifecycle_events
pipeline_work_items
adaptive_rate_state
daily_coverage_summary
```

Introduce Alembic as the first implementation change. `create_all` may remain a
test/local bootstrap helper; production changes require versioned migrations.

Migration sequence:

1. Add new tables and lifecycle columns without changing behaviour.
2. Populate yfinance instruments from accepted universe snapshots.
3. Materialize exchange sessions.
4. Normalize `CA` to `TSX` across relevant tables.
5. Generate missing backfill/repair work from existing coverage.
6. Preserve valid OHLCV; do not run a blind historical reload.

Never insert artificial null OHLCV rows. Calculate expected open sessions minus
stored valid candles, and materialize only summaries for dashboard performance.

## Dagster Design

Create provider-neutral exchange assets:

```text
nse_universe_snapshot
tsx_universe_snapshot
us_universe_snapshot

nse_daily_ohlcv
tsx_daily_ohlcv
us_daily_ohlcv

nse_daily_coverage
tsx_daily_coverage
us_daily_coverage
```

Downstream NSE research assets depend on `nse_daily_ohlcv`, not the legacy
provider-specific `upstox_daily_ohlcv`. Exchange jobs remain isolated so a
calendar or universe problem in one market does not block the others; all still
share one Redis Yahoo budget.

### Proposed Schedules

Use exchange-local Dagster time zones.

| Job | Proposed schedule |
| --- | --- |
| NSE universe refresh | Weekdays before NSE open |
| NSE daily increment | Approximately 17:30 Asia/Kolkata |
| NSE repair pass | Approximately 21:00 Asia/Kolkata |
| US universe refresh | Weekdays before US open |
| US daily increment | Approximately 18:30 America/New_York |
| TSX universe refresh | Weekdays before TSX open |
| TSX daily increment | Approximately 18:30 America/Toronto |
| US/TSX repair pass | Approximately 21:00 local time |
| Calendar refresh | Monthly and at year rollover |
| Backfill worker | Continuous/sensor-driven at lower priority |
| Coverage reconciliation | After ingestion and nightly |

Every scheduled job first checks the materialized session. Holidays produce a
successful no-op rather than a provider request or failure.

Forex modules and historical data remain, but active definitions must honor:

```text
FOREX_PIPELINES_ENABLED=false
```

## API and Data Console

Retain existing availability, runs, request-log, and schedule endpoints where
possible. Switch yfinance availability from seed lists to persisted active
provider instruments.

Coverage responses add:

```text
first_expected_date
first_stored_date
latest_expected_date
latest_stored_date
expected_rows
stored_rows
missing_rows
coverage_pct
freshness_status
exact_missing_sessions
last_successful_run
last_fetch_status
next_retry_at
attempt_count
lifecycle_status
```

Provider health adds:

```text
current_rpm
last_safe_rpm
configured_ceiling
active_workers
requests_per_minute
success_rate
429_count
timeout_rate
p50_duration_ms
p95_duration_ms
circuit_state
cooldown_until
```

Queue endpoints:

```text
GET  /api/data/work-items
POST /api/data/retries/symbol
POST /api/data/retries/run
POST /api/data/retries/gaps
```

A retry endpoint validates authorization and scope, generates an idempotency
key, records actor and reason, and enqueues work. It never contacts Yahoo.

Dashboard views cover:

- Coverage summary and per-symbol exact gaps.
- Provider health and adaptive-rate state.
- Queue state, oldest work age, attempts, and next retry.
- Universe additions, removals, mapping changes, and refresh freshness.
- Audited manual retry actions.

## Configuration

Add settings while retaining existing names through migration:

```text
YFINANCE_DAILY_ENABLED
YFINANCE_ADAPTIVE_RATE_MODE=fixed|observe|adaptive
YFINANCE_INITIAL_RPM=300
YFINANCE_MINIMUM_RPM=30
YFINANCE_MAXIMUM_RPM=600
YFINANCE_INITIAL_CONCURRENCY=4
YFINANCE_MAXIMUM_CONCURRENCY=8
YFINANCE_IMMEDIATE_RETRY_ATTEMPTS=3
YFINANCE_RETRY_WAIT_MULTIPLIER_SECONDS=2
YFINANCE_RETRY_WAIT_MAX_SECONDS=15
YFINANCE_ADAPTIVE_EVALUATION_WINDOW_SECONDS=60
YFINANCE_ADAPTIVE_HEALTHY_WINDOWS_BEFORE_INCREASE=2
YFINANCE_ADAPTIVE_INCREASE_RPM=30
YFINANCE_ADAPTIVE_ERROR_THRESHOLD=0.10
YFINANCE_ADAPTIVE_COOLDOWN_SECONDS=60
YFINANCE_INCREMENTAL_OVERLAP_SESSIONS=5
YFINANCE_PROVIDER_GRACE_MINUTES
YFINANCE_FULL_US_ENABLED
YFINANCE_FULL_TSX_ENABLED
YFINANCE_NSE_ENABLED
MATERIALIZED_EXCHANGE_SESSIONS_ENABLED=false
EXCHANGE_SESSION_HISTORY_YEARS=10
EXCHANGE_SESSION_FUTURE_YEARS=1
EXCHANGE_SESSION_MINIMUM_OPEN_DAYS_PER_YEAR=220
EXCHANGE_SESSION_MAXIMUM_OPEN_DAYS_PER_YEAR=260
EXCHANGE_SESSION_SHADOW_MAX_DISCREPANCIES=5
EXCHANGE_SESSION_OBSERVED_OPEN_MINIMUM_INSTRUMENTS=10
LEGACY_UPSTOX_NSE_ENABLED
FOREX_PIPELINES_ENABLED=false
```

Production requires Redis-backed provider limiting. In-memory limiting is only
for local development and unit tests.

## Implementation and Rollout Plan

### Phase 1: Schema and Compatibility

- Add Alembic.
- Add lifecycle, session, work, and rate-state tables.
- Add exchange alias normalization and feature flags.
- Preserve current pipeline behaviour.

Exit criteria:

- Migrations apply to a representative existing database.
- Existing API, CLI, Dagster, and tests continue to work.
- Data-safe rollback is documented and tested.

### Phase 2: Persisted Universes

- Implement snapshot validation and reconciliation.
- Dual-write `symbols` and `provider_instruments`.
- Add lifecycle events.
- Enqueue new-symbol work behind a disabled execution flag.

Exit criteria:

- Two snapshots reconcile deterministically.
- A failed/truncated source cannot mass-deactivate symbols.
- Yfinance availability no longer requires seeds.

### Phase 3: Materialized Sessions

- Build ten-year NSE, TSX, and US sessions.
- Shadow-compare old and new expected dates.
- Switch planning and coverage after discrepancies are resolved.

Exit criteria:

- Holidays and weekends never appear missing.
- Early closes are represented.
- Pre-listing periods do not lower coverage.

### Phase 4: Yahoo Execution Controls

- Add weighted global acquisition.
- Add worker-local sessions and bounded concurrency.
- Add immediate retry and classification.
- Deploy adaptive governor in `observe` mode.
- Record per-ticker partial-batch outcomes.

Implementation status: implemented on the Phase 4 branch. The daily Yahoo
pipeline now uses `YahooDailyExecutor`, with application-owned worker
concurrency and yfinance internal threading disabled. Every attempt acquires a
ticker-weighted permit from the shared
`provider-rate-limit:yfinance:all` Redis budget. Partial batches commit valid
ticker frames and retry only missing tickers. Provider attempts are classified
and recorded per ticker, including retry number, limiter wait, duration, HTTP
status when available, and terminal/retryable outcome.

The adaptive controller persists recommendations in `adaptive_rate_state`.
Production defaults to `observe`: it enforces 300 logical ticker RPM and four
workers while recording recommended RPM/concurrency changes. `adaptive` mode
can apply controller changes, but must remain disabled until observation data
is reviewed. Database writes use bounded immediate retries against the already
downloaded frames, so a transient write failure does not immediately cause a
second Yahoo download.

Although Forex remains outside this rollout and its schedules stay stopped,
the legacy yfinance intraday path uses the same global weighted budget. This
prevents an accidental/manual legacy run from bypassing the IP-wide Yahoo
guardrail.

Exit criteria:

- All Yahoo triggers share one Redis key.
- Retries reacquire weighted permits.
- Four-worker execution is idempotent and observable.
- Controller recommendations are stable before adaptive activation.

### Phase 5: Durable Queue and Backfill

- Implement claim, heartbeat, stale-lock recovery, and retry scheduling.
- Refactor existing planners to emit work items.
- Generate only missing ten-year work for active symbols.
- Keep incremental work above backfill priority.

Implementation status: implemented on the Phase 5 branch. The shared
`DailyWorkPlanner` emits deterministic work identities for incremental,
new-symbol, gap-repair, and initial-backfill windows. Initial backfill reads
stored yfinance coverage in bounded symbol chunks and enqueues only contiguous
missing exchange-session windows. Incremental work uses the configured
five-session overlap and is claimed ahead of historical backfill.

`TimescaleStore` now provides atomic PostgreSQL claims using `FOR UPDATE SKIP
LOCKED`, worker-owned heartbeats, stale-lock recovery, idempotent enqueue, and
guarded terminal/retry transitions. Durable retry uses the planned `5m -> 15m
-> 1h -> 4h -> 12h -> 24h` ladder and changes an incremental retry to the
second priority tier. Attempts that exhaust `max_attempts` become terminal.

The bounded worker correlates every Phase 4 ticker outcome to its durable work
item, persists candles, adjustments, and provider request logs before
acknowledgement, and retries only failed ticker items. CLI commands are:

```bash
trade-research plan-yfinance-daily-work
trade-research run-yfinance-daily-worker
```

Dagster registers `yfinance_daily_work_planner_schedule` and
`yfinance_daily_work_worker_schedule`, both stopped by default. Do not enable
them during Phase 5 deployment; universe cutover and staged schedule activation
begin in Phase 6. Forex schedules remain stopped.

Exit criteria:

- Executor restarts do not lose or duplicate work.
- Existing valid yfinance data is reused.
- Failed windows progress automatically through durable retry.

### Phase 5.1: Production Migration Hotfix

Production validation found that the API image omitted `alembic.ini` and the
`migrations/` directory, while `deploy/deploy.sh` never invoked Alembic. New
tables had been created by the compatibility `create_all` path, but the legacy
`symbols` table was missing all Phase 1 lifecycle columns and the database had
no `alembic_version` table.

The hotfix packages the complete Alembic runtime in `Dockerfile.api`. Deployment
now builds the new image, starts and waits for PostgreSQL, and executes
`alembic upgrade head` from a one-off container before replacing any application
service. Migration or database-readiness failure aborts the release while the
previous application containers remain running.

The existing Phase 1 migration is intentionally reconciliation-safe: it adds
missing lifecycle columns to an existing `symbols` table and skips foundation
tables that already exist. A regression test covers this exact mixed state and
verifies that revision `20260717_0001` is recorded. Production must take a fresh
backup before first deploying this hotfix, and must not run an Alembic downgrade
during the rollout because populated calendar and queue tables are retained.

Exit criteria:

- The API image contains Alembic configuration and migration scripts.
- Schema migration succeeds before application service replacement.
- A failed migration aborts deployment without replacing the prior release.
- Legacy `symbols` rows gain lifecycle columns without data loss.

### Phase 5.2: Deployment Self-Update Hardening

The first Phase 5.1 deployment started the pre-hotfix `deploy.sh` and pulled its
replacement while that shell process was already running. The running process
continued with the old control flow, rebuilt the application, and skipped the
new migration step. A second invocation from the synchronized checkout applied
the migration successfully.

Automated deployment now synchronizes the server checkout to `origin/main`
before invoking `deploy.sh`. The script also records its starting revision and,
when its own synchronization changes that revision, re-executes the synchronized
script exactly once before building images or changing services. The workflow
pre-sync protects automated releases immediately; the revision guard provides
the same behavior for direct manual invocations.

Exit criteria:

- Automated deployment invokes the script from the release being deployed.
- A manual deployment that pulls a new revision re-executes that revision once.
- Re-execution cannot loop and deployment mutations occur only once.

### Phase 6: US Cutover

- Replace `us_seed` scheduling with the persisted full active US universe.
- Run missing ten-year backfill.
- Enable US incremental and repair schedules.
- Activate adaptive mode only after observe-mode validation.

### Phase 7: TSX Cutover

- Complete `CA` to `TSX` migration.
- Validate TSX mappings and calendar.
- Run active-universe backfill.
- Enable TSX incremental and repair schedules.

### Phase 8: NSE Cutover

- Backfill active NSE symbols through yfinance.
- Add provider-neutral `nse_daily_ohlcv`.
- Move research dependencies from the Upstox asset.
- Disable scheduled Upstox primary ingestion after comparison and soak.

Upstox remains an explicitly labelled fallback/verification source until a
separate removal decision.

### Phase 9: Data Console and Operations

- Add exact gaps, queue, retry, lifecycle, and adaptive-rate views.
- Add audited manual retry endpoints.
- Add freshness, queue-age, calendar, universe, and provider alerts.

### Phase 10: Cleanup

After at least two weeks of successful scheduled operation for all exchanges:

- Remove seed-universe schedules.
- Remove obsolete direct fetch loops.
- Retain compatibility aliases only for the deprecation window.
- Keep rate-probe scripts as diagnostics outside production execution.

## Cutover and Rollback

Cut over in this order:

```text
US -> TSX -> NSE
```

US and TSX already use yfinance paths and are lower-risk proving grounds. NSE
moves last because it changes the current primary provider and downstream
Dagster dependency.

Each exchange has independent feature flags and schedules. Rollback stops the
new exchange schedule and reenables the previous schedule. Rollback never
deletes OHLCV, work, or audit history.

## Observability and Alerts

Required metrics:

- Run duration, requested/succeeded/failed items, and rows written.
- Logical ticker permits and raw Yahoo calls where observable.
- Current/last-safe RPM, concurrency, wait time, and cooldown.
- 429, 5xx, timeout, network, empty, invalid-symbol, and partial-batch counts.
- Request p50/p95 latency.
- Queue depth, retry-wait count, oldest work age, and stale locks.
- Active, added, suspected-inactive, inactive, and invalid-mapping symbols.
- Expected/stored/missing rows and freshness lag by exchange and symbol.
- Calendar version/freshness and universe snapshot freshness.

Alert when:

- A universe refresh fails, becomes stale, or is unexpectedly small.
- A calendar year is missing or invalid.
- A daily increment misses its freshness SLA.
- Retry queue age exceeds its SLA.
- Adaptive rate remains at its floor or the circuit stays open.
- 429, timeout, or 5xx rates exceed thresholds.
- Coverage declines unexpectedly.
- A Dagster schedule does not start or completes unsuccessfully.

## Testing Strategy

### Unit Tests

- Snapshot validation and two-snapshot deactivation.
- Addition, reactivation, rename, and removal lifecycle events.
- Class/preferred-share Yahoo normalization.
- Session generation and latest eligible session.
- Pre-listing, holiday, pending, and genuine-gap classification.
- Incremental overlap and ten-year planning.
- Work idempotency keys.
- Adaptive increase, decrease, cooldown, floor, and ceiling.
- Retryable versus terminal failures.

### Integration Tests

- Redis atomic weighted acquisition across workers.
- One Yahoo budget shared by all exchanges and work types.
- Work claim, heartbeat, stale-lock recovery, and retry scheduling.
- Timescale idempotent candle and adjustment upserts.
- Partial-batch commit and per-ticker retry.
- Database-write retry without another Yahoo download.
- Alembic upgrade from a representative current schema.

### Contract and Simulation Tests

- Existing CLI/API inputs remain valid during compatibility windows.
- Availability reads persisted full universes.
- Manual retry enqueues without fetching inline.
- Dagster definitions exclude Forex when disabled.
- Fake yfinance responses cover success, partial, empty, 404, 429 with
  `Retry-After`, 5xx, timeout, disconnect, rename, and post-download DB failure.

Live rate probes remain manual diagnostics and do not run in CI.

## Service-Level Targets

```text
New active symbol detected:        within one successful daily refresh
New-symbol backfill enqueued:      in the same refresh workflow
Daily candle freshness:            within 2-3 hours after exchange close
Retryable daily failure recovery:  by the next repair cycle
Universe/calendar false gaps:      zero
Routine manual intervention:       none
```

## Definition of Done

The implementation is complete when, for every currently active NSE, TSX, and
US symbol:

- Available history is stored back to at most ten years.
- Expected rows use correct exchange sessions.
- Pre-listing dates, holidays, weekends, and provider-pending dates are not
  genuine missing candles.
- Incremental loads and repair passes run automatically.
- New symbols automatically receive backfill work.
- Removed symbols remain stored and visibly inactive.
- Retryable failures recover through immediate or durable retry.
- Every genuine gap has an exact date and reason.
- Every Yahoo path participates in one weighted adaptive budget.
- Rate, failures, retries, coverage, freshness, lifecycle, and schedules are
  visible in the Data Console.
- Manual retries are authenticated, audited, idempotent, and queue-based.
- Forex remains disabled and outside equity health calculations.
- Legacy paths remain reversible through the documented soak period.
