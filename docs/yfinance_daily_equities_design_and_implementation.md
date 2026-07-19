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
| TSX | Mixed Canadian Yahoo directory, restricted to `.TO` rows | Preserved `.TO` |

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

### Phase 6.1: US Identity and Lifecycle Hotfix

The first full-US backfill completed for 5,611 of 5,613 current directory
members. The two deterministic empty responses exposed two different lifecycle
cases: `SHOT` is a newly listed security whose ticker was previously used by a
different issuer, while `SVA` is still directory-listed but has an unresolved
Nasdaq trading halt. Neither case should consume the generic retry ladder
forever.

The US universe provider now enriches Nasdaq's symbol directory with two
fail-open official sources:

- SEC ticker-to-CIK associations provide a stable issuer identity. The
  canonical source identity combines CIK with a normalized security class so
  multiple share classes from one issuer remain distinct.
- Nasdaq's unresolved trade-halt feed supplies current halt status, reason, and
  effective date. A failure of either enrichment source does not reject an
  otherwise valid symbol-directory snapshot.

Reconciliation matches an existing security by source identity before ticker.
The same identity under a different ticker emits `ticker_changed` and retains
the canonical instrument. The same ticker under a different identity emits
`ticker_reused` and creates a new canonical instrument. Dated yfinance aliases
close when either condition occurs, so a ticker string is never treated as a
permanent security identifier.

The first security using a Yahoo ticker retains the compatible
`YF|<provider-symbol>` storage key. If that ticker is later reused by a
different canonical security, the new security receives a namespaced provider
instrument key containing its canonical ID. This prevents pre-reuse OHLCV rows
from being interpreted as history for the new issuer while preserving all
existing data in place.

Current lifecycle state is persisted on `symbols`:

```text
source_identity
provider_instrument_key
listing_status / listing_status_reason / listing_status_effective_at
pipeline_eligibility
provider_status / provider_status_reason / provider_status_updated_at
```

Unresolved halted securities receive `pipeline_eligibility=none`; pending
yfinance work is cancelled during reconciliation and future planners exclude
them. If a current symbol later disappears from the halt feed, reconciliation
emits `trading_resumed` and makes it incrementally eligible again.

An `empty_response` for a security first seen within the configured 72-hour
new-listing grace period becomes `new_listing_provider_lag`. It remains a
durable retry, but its next attempt is delayed by six hours instead of rapidly
consuming the generic retry ladder. Successful and failed executions update the
persisted provider status for operations and coverage reporting.

Production rollout remains fail-closed: deploy migration `20260718_0002`, run a
fresh US universe refresh, verify `SVA` is halted/ineligible and `SHOT` remains
active with a different source identity from `BNKK`, then preview incremental
planning before restarting the planner and worker schedules.

### Phase 6.2: Bounded US Planner Hotfix

The first US-only incremental planning canary exposed a PostgreSQL dynamic
shared-memory failure. The planner requested latest coverage for all 5,586
eligible US instruments in one `IN (...) GROUP BY` query over approximately
10.34 million daily rows. PostgreSQL attempted a roughly 94 MiB parallel shared
memory allocation while the production container exposed only 64 MiB.

The storage lookup now deduplicates instrument keys and executes bounded chunks
using `YFINANCE_WORK_PLANNER_CHUNK_SIZE` (250 by default). A current full US
universe therefore executes 23 sequential coverage queries instead of one
unbounded statement. Each query is independently bounded regardless of future
universe growth.

Scheduled planning is also cutover-aware. When Dagster invokes the planner
without an explicit exchange list, exchanges are resolved only from:

```text
YFINANCE_FULL_US_ENABLED
YFINANCE_FULL_TSX_ENABLED
YFINANCE_NSE_ENABLED
```

An empty enabled set fails closed before opening the database. The CLI continues
to permit an explicit `--exchanges US` canary independently of schedule flags.
For the Phase 6 cutover, production must enable only
`PROD_YFINANCE_FULL_US_ENABLED=true`; TSX and NSE stay false.

Production Compose now gives PostgreSQL a configurable 512 MiB shared-memory
limit as defense in depth. This setting causes the PostgreSQL container to be
recreated during deployment, so the normal fresh backup and database readiness
checks are mandatory. Query safety does not rely on the larger limit.

SQLAlchemy engines hide bound parameters, and the planner CLI reduces database
failures to the database driver's first diagnostic line. Operational failures
therefore no longer emit thousands of ticker parameters or a full traceback.

Phase 6.2 rollout remains guarded:

1. Keep the US universe, planner, and worker schedules stopped.
2. Take a fresh production backup and deploy the hotfix.
3. Confirm PostgreSQL `/dev/shm` is 512 MiB and the database is healthy.
4. Set `PROD_YFINANCE_FULL_US_ENABLED=true`, leaving TSX/NSE false, then recreate
   the Dagster daemon and webserver so their code-server environments receive
   the flags.
5. Repeat the explicit US incremental-only CLI canary.
6. Inspect inserted work and run a one-item worker canary before enabling any
   schedule.

### Phase 7: TSX Cutover

- Complete `CA` to `TSX` migration.
- Validate TSX mappings and calendar.
- Run active-universe backfill.
- Enable TSX incremental and repair schedules.

#### Phase 7.1: Canonical TSX Foundation

The configured Canadian directory is not a TSX-only feed. A production probe on
2026-07-18 returned 2,632 Yahoo-formatted listings: 885 TSX `.TO` rows, 1,581
TSXV `.V` rows, and 166 Cboe Canada `.NE` rows. The legacy provider treated all
three venues as TSX and appended another `.TO`, which could produce invalid
mappings such as `AAPL-NE.TO`.

TMX publishes an official
[Listed Company Directory](https://www.tsx.com/en/listings/listing-with-us/listed-company-directory).
Phase 7.1 keeps the existing Yahoo-formatted directory only as a non-executing
mapping candidate source. Before Phase 7.2 enables data work, the accepted `.TO`
set must be reconciled with the official current/suspended/delisted views and
the remaining product types (for example CDRs and funds) must have an explicit
inclusion policy.

Phase 7.1 makes the boundary explicit:

- `TSXUniverseProvider` accepts only `.TO` rows from the mixed directory.
- Existing Yahoo `.TO` symbols are preserved exactly; native exchange symbols
  remove `.TO` and convert Yahoo class/unit hyphens back to dots.
- Duplicate, malformed, TSXV, and Cboe Canada rows are counted in source
  diagnostics. Diagnostics are persisted in the snapshot validation JSON and
  emitted in CLI/Dagster metrics.
- Canada seed and full-universe compatibility names now emit canonical
  `exchange=TSX`. API requests using legacy `exchange=CA` remain accepted but
  normalize to and return `TSX`.
- Alembic revision `20260718_0003` changes stored `exchange=CA` values to
  `TSX`. For exchange-bearing primary keys, an existing TSX row wins over its
  legacy CA duplicate. Provider instrument keys and candle history are not
  rewritten.
- Universe refresh may enqueue new-symbol backfills only when that exchange's
  cutover flag is enabled. With `PROD_YFINANCE_FULL_TSX_ENABLED=false`, a TSX
  snapshot can be persisted and validated while the global Yahoo worker safely
  continues serving US work.
- The Data Console uses canonical `TSX`; no new frontend request writes `CA`.

Phase 7.1 rollout remains non-executing:

1. Keep `PROD_YFINANCE_FULL_TSX_ENABLED=false` and
   `tsx_universe_refresh_schedule` stopped.
2. Take a fresh backup, deploy, and upgrade to Alembic revision
   `20260718_0003`.
3. Verify no `exchange='CA'` rows remain in exchange-bearing tables and confirm
   the existing US schedules remain healthy.
4. Run `trade-research refresh-equity-universe TSX`. Expect roughly 885 accepted
   symbols, source diagnostics showing the excluded venues, and zero TSX
   backfills queued.
5. Inspect mappings, lifecycle events, the existing materialized TSX calendar,
   and queue isolation. Do not use `--allow-large-change` until the previous TSX
   snapshot contents have been inspected.
6. Phase 7.2 enables the TSX flag, plans the ten-year backfill, executes bounded
   canaries, and only then enables TSX schedules.

#### Phase 7.2: Official Reconciliation and Bounded Canaries

Phase 7.2 replaces the Phase 7.1 mapping-only refresh with a fail-closed
intersection of two sources:

- the Yahoo-formatted mixed Canadian directory remains the candidate security
  and provider-symbol source;
- the official TMX issuer workbook at `https://www.tsx.com/en/resource/571`
  supplies current TSX issuer identity, root ticker, sector, product type, and
  listing date;
- the official TMX company-directory JSON views supply recently listed,
  recently delisted, and suspended overrides.

The official workbook is fetched transiently. The application persists the
derived reconciliation status, reason, classification, source timestamp, and
stable TMX-backed identity required for operations; it does not check the
workbook into the repository. Official HTTP requests use bounded exponential
retry and a 30-second request timeout. Any workbook/schema/directory failure
rejects the refresh and preserves the previous accepted universe.

The initial inclusion policy is intentionally narrow:

- include active common equity, supported common-share classes, and real-estate
  investment trust `.UN` units;
- exclude TSXV and Cboe Canada before reconciliation;
- exclude CDRs, ETPs, closed-end funds, SPACs, preferred shares, debt,
  warrants, rights, non-REIT units, and alternate-currency `.U` units;
- exclude recent delistings and suspensions immediately;
- leave recent-but-unclassified, candidate-only, and official provider-unmapped
  issuers in review state with `pipeline_eligibility=none`.

The TMX company ID plus the security suffix forms `source_identity`. It remains
stable across root-ticker changes while keeping multiple share classes distinct.
Changes to reconciliation or pipeline eligibility create lifecycle events. The
latest snapshot records aggregate status/type counts and provider-unmapped
samples, while `symbols` records the per-security outcome. Operators can inspect
the result with:

```bash
trade-research tsx-reconciliation-status
```

The API exposes the same aggregate state at:

```text
GET /api/data/universe-reconciliation?exchange=TSX
```

Canary planning is separated from the full-exchange cutover:

```text
PROD_YFINANCE_FULL_TSX_ENABLED=false
PROD_YFINANCE_TSX_CANARY_ENABLED=false
PROD_YFINANCE_TSX_CANARY_MAX_SYMBOLS=100
```

`PROD_YFINANCE_TSX_CANARY_ENABLED` authorizes only the bounded manual planner.
It does not add TSX to the scheduled planner and it does not allow a universe
refresh to enqueue new-symbol work. The planner defaults to dry-run:

```bash
trade-research plan-yfinance-tsx-canary --symbol-limit 1 --dry-run
trade-research plan-yfinance-tsx-canary --symbol-limit 1 --enqueue
```

The configured maximum is enforced even from the CLI. Only records from the
latest accepted snapshot with `reconciliation_status=official_eligible` may be
selected. Full TSX planning continues to require
`PROD_YFINANCE_FULL_TSX_ENABLED=true`.

Guarded Phase 7.2 production rollout:

1. Back up production, deploy, and upgrade to Alembic revision
   `20260718_0004`.
2. Keep `PROD_YFINANCE_FULL_TSX_ENABLED=false`, keep
   `PROD_YFINANCE_TSX_CANARY_ENABLED=false`, and keep
   `tsx_universe_refresh_schedule` stopped.
3. Run `trade-research refresh-equity-universe TSX`. The July 18, 2026 probe
   produced 885 candidates, 645 eligible securities, 240 excluded securities,
   and 21 eligible official issuers without safe candidate mappings. Counts may
   move with the official monthly workbook and live lifecycle directory.
4. Confirm `Backfills queued = 0`, inspect
   `trade-research tsx-reconciliation-status`, and confirm the TSX work queue is
   still empty.
5. Set only `PROD_YFINANCE_TSX_CANARY_ENABLED=true` and recreate the API
   container. Leave the full TSX flag false.
6. Temporarily stop `yfinance_daily_work_worker_schedule`, execute a one-symbol
   dry run, enqueue the one-symbol canary, and run a manual worker with
   `--claim-size 1`. Validate request logs, stored OHLCV, coverage, rate state,
   and queue transitions before restarting the worker schedule.
7. Repeat with limits 5, 25, and 100. Stop escalation on any rate limit,
   terminal failure, suspicious data, unexpected classification, or queue leak.
8. Phase 7.3 enables the full TSX flag, plans the remaining ten-year history in
   bounded batches, soaks incremental operation, and only then starts the TSX
   universe schedule.

#### Phase 7.2.1: Active Listing-Boundary Hotfix

The 25-symbol TSX canary exposed a repeat-planning defect for active securities
whose official listing date falls inside the ten-year calendar. The planner
correctly persisted the TMX date but treated it only as an upper bound for
halted or delisted instruments. Active instruments therefore received
pre-listing work. `AAUC.TO` was re-queued for the sessions before its September
11, 2023 listing, and `ABRA.TO` retried an empty pre-listing window even though
its valid Yahoo history was already stored.

Revision `20260718_0005` makes the lifecycle boundary bidirectional:

- active instruments use `listing_status_effective_at` as the first expected
  session for initial, gap-repair, and incremental planning;
- halted, suspended, and delisted instruments continue to use their effective
  date as the last expected session;
- planner enqueue cancels queued or retry-wait work whose entire window ends
  before an active listing date, using the audited reason
  `outside_listing_window`;
- the worker repeats the same guard after claiming so stale work never reaches
  Yahoo even if it was created by an older application version;
- the data migration cancels existing affected TSX work while preserving valid
  work such as the unprocessed `ABXX.TO` canary item.

Guarded recovery keeps the full TSX and canary flags false and both the TSX
universe and global worker schedules stopped during deployment. After migration
`0005`, operators verify the obsolete `AAUC.TO` and `ABRA.TO` items are
cancelled, re-enable only the bounded canary flag, rerun the cumulative
25-symbol planner, and process the one remaining valid item before advancing to
100.

#### Phase 7.2.2: Provider-History Evidence and Sparse-History Quarantine

The successful 100-symbol canary exposed a second distinction that exchange
calendars and official listing dates cannot represent by themselves. Yahoo can
successfully return all history it has while some calendar sessions remain
absent. The observed examples include provider-history boundaries, provider
symbol identity changes, single-session provider omissions, and a symbol with
implausibly sparse history despite an old official listing. Replanning those
same absent sessions indefinitely creates calls without improving coverage.

Revision `20260718_0006` adds provider-neutral, instrument-key-specific daily
history evidence. Every successful initial backfill, new-symbol backfill, or
gap-repair window records the requested calendar range, first and last returned
candle, expected and observed rows, coverage ratio, provider-unavailable rows,
classification, and originating run. The classifications are:

- `verified_complete`: Yahoo returned every expected session in the successful
  request window;
- `verified_partial`: Yahoo completed the request but does not expose candles
  for every expected exchange session;
- `quarantined_sparse`: an established listing has at least 220 expected
  sessions but at most five returned sessions, which is too sparse to accept as
  normal provider history.

Verified evidence prevents the historical planner from repeatedly requesting
absent dates inside the already-completed provider window. It does not suppress
incremental freshness planning after the latest stored candle. The first
provider candle becomes an additional lower bound only when it came from a
successful backfill-class request. Quarantined instruments are removed from
automatic Yahoo planning, and pending work for them is cancelled with the
audited reason `provider_history_quarantined`. This handles an established
listing such as `AKT-A.TO` differently from a newly observed identity such as
`SHOT`: one returned candle is suspicious for the former but valid evidence of
the currently available provider history for the latter.

The behavior is fail-closed behind:

```text
PROD_YFINANCE_PROVIDER_HISTORY_EVIDENCE_ENABLED=false
PROD_YFINANCE_SPARSE_HISTORY_MINIMUM_EXPECTED_ROWS=220
PROD_YFINANCE_SPARSE_HISTORY_MAXIMUM_OBSERVED_ROWS=5
```

Future successful durable workers write evidence automatically. Existing
successful backfills are classified explicitly:

```bash
trade-research refresh-yfinance-history-evidence TSX --symbol-limit 100
trade-research refresh-yfinance-history-evidence US --symbols SHOT
trade-research provider-history-status TSX
```

The Data Console can consume the same aggregate and quarantine state from:

```text
GET /api/data/provider-history?exchange=TSX&provider=yfinance
```

Guarded Phase 7.2.2 rollout:

1. Keep full TSX, TSX canary, and provider-history evidence flags false. Keep
   the TSX universe and global Yahoo worker schedules stopped. Take and validate
   a quiet backup.
2. Deploy and verify Alembic revision `20260718_0006`.
3. Bootstrap the completed 100-symbol TSX canary and inspect
   `provider-history-status TSX`. `AKT-A.TO` should be quarantined; the other
   successful windows should be verified.
4. Bootstrap `SHOT` in US and verify it is `verified_partial`, not quarantined.
5. Enable only the provider-history evidence flag and bounded TSX canary flag
   in the API container. Keep full TSX false and the worker schedule stopped.
6. Run the 100-symbol TSX canary in dry-run mode. It should produce zero repeat
   historical work, report one quarantined symbol, and preserve incremental
   freshness semantics.
7. Recreate the Dagster daemon/webserver with the evidence flag before any
   scheduled planner or worker is restarted. Repeat the queue, request-log,
   rate-state, and evidence checks.
8. Phase 7.3 may then plan the remaining TSX symbols. Full TSX remains disabled
   until this bootstrap and dry-run gate pass.

### Phase 8: NSE Cutover

- Backfill active NSE symbols through yfinance.
- Add provider-neutral `nse_daily_ohlcv`.
- Move research dependencies from the Upstox asset.
- Disable scheduled Upstox primary ingestion after comparison and soak.

Upstox remains an explicitly labelled fallback/verification source until a
separate removal decision.

#### Phase 8 implementation and rollout gate

Phase 8 is implemented as a reversible provider cutover. The defaults remain
fail-closed:

```text
YFINANCE_NSE_CANARY_ENABLED=false
YFINANCE_NSE_ENABLED=false
NSE_DAILY_PRIMARY_SOURCE=upstox
LEGACY_UPSTOX_NSE_ENABLED=true
```

`plan-yfinance-nse-canary` is independent of the full NSE flag. This permits
deterministic 1, 25, 100, 500, and 1,000-symbol stages while US and TSX retain
their existing schedules and shared Yahoo budget. Enqueueing is rejected unless
the bounded canary flag (or the final full-NSE flag) is enabled.

```bash
trade-research plan-yfinance-nse-canary --symbol-limit 1 --dry-run
trade-research plan-yfinance-nse-canary --symbol-limit 1 --enqueue
trade-research run-yfinance-daily-worker --claim-size 1 --worker-id phase8-nse-1
```

After each stage, inspect the work queue, provider request log, adaptive-rate
state, provider-history evidence, stored row boundaries, and suspicious rows.
Do not increase a stage when requests are rate-limited, retries are rising, the
circuit is open, or durable work remains unexpectedly retryable.

`check-nse-yfinance-cutover` compares raw Yahoo and Upstox candles by NSE symbol
and validated exchange session. It fails unless all configured requirements are
met:

- minimum count of overlapping symbols;
- minimum shared-row coverage;
- minimum raw-close agreement within the configured tolerance; and
- maximum freshness lag for both providers.

The comparison deliberately uses raw close, not adjusted close. Split/dividend
adjustments remain in the provider-specific adjustment table and are not allowed
to make raw-provider disagreement appear healthy.

The Dagster research graph now depends on `nse_daily_ohlcv`, not directly on
`upstox_daily_ohlcv`. With `NSE_DAILY_PRIMARY_SOURCE=upstox`, behavior remains
the same. With `NSE_DAILY_PRIMARY_SOURCE=yfinance`, the asset does not download
inline: it validates the overlap gate, exports the durable Yahoo Timescale
snapshot, and then lets features and targets read `source=yfinance`.

Before changing the primary source:

1. Complete the full active-NSE backfill and verify a repeat planner inserts
   zero historical work.
2. Run the comparison gate across multiple completed sessions and retain its
   output with the deployment evidence.
3. Back up PostgreSQL, data, artifacts, Qdrant, and Dagster state.
4. Recreate API, Dagster daemon, and Dagster webserver with
   `YFINANCE_NSE_ENABLED=true` while keeping
   `NSE_DAILY_PRIMARY_SOURCE=upstox` for the comparison soak.
5. Stop the legacy daily research schedule, set
   `NSE_DAILY_PRIMARY_SOURCE=yfinance`, recreate all three services, and rerun
   the comparison gate.
6. Perform one explicit full feature and target rebuild before restarting the
   incremental research schedule. Existing feature/target rows use Upstox
   instrument keys; the full rebuild is the controlled provider-key transition.
7. Keep `LEGACY_UPSTOX_NSE_ENABLED=true` throughout the rollback window. Disable
   scheduled Upstox fetching only after the agreed soak, not in the same change
   that enables Yahoo primary.

The Yahoo-side rebuild commands are:

```bash
trade-research build-daily-features --input-source timescale \
  --ohlcv-source yfinance --store-db --full-rebuild --replace-exchange
trade-research build-daily-targets --input-source timescale \
  --ohlcv-source yfinance --store-db --full-rebuild --replace-exchange
```

`--replace-exchange` is intentionally rejected for incremental or non-database
runs. It removes the prior provider-keyed rows for that feature/target version
only after the replacement frame has been built successfully, then stores the
new provider-keyed dataset. Run it only with the daily schedule stopped and a
verified backup.

Rollback is configuration-only: stop the daily research schedule, restore
`NSE_DAILY_PRIMARY_SOURCE=upstox`, recreate services, run one full downstream
rebuild from Upstox, and restart the schedule. Yahoo candles and evidence remain
stored for diagnosis; no destructive data cleanup is part of rollback.

#### Phase 8.1: NSE comparison-key and freshness hotfix

The first production NSE canary proved that provider storage identities differ:
Yahoo daily work stores provider symbols such as `20MICRONS.NS`, while Upstox
stores the NSE exchange symbol `20MICRONS`. Phase 8.1 canonicalizes the Yahoo
`.NS` suffix only at the NSE comparison boundary; provider-specific stored rows
and instrument keys remain unchanged and auditable.

Provider freshness is calculated from each complete provider window before the
frames are restricted to shared symbols. Consequently, an empty comparison set
can no longer make a fresh provider appear stale. The readiness report exposes
provider window counts and one primary comparison state:

```text
provider_data_missing
no_symbol_overlap
provider_stale
insufficient_symbol_overlap
insufficient_row_overlap
close_mismatch
ready
```

Row-overlap and close-price thresholds are evaluated only when at least one
canonical symbol overlaps. This prevents cascaded, meaningless price and row
failures when the actual problem is missing data or incompatible identity.

### Phase 9: Data Console and Operations

- Add exact gaps, queue, retry, lifecycle, and adaptive-rate views.
- Add audited manual retry endpoints.
- Add freshness, queue-age, calendar, universe, and provider alerts.

#### Phase 9.1: Read-only operations API

Phase 9.1 establishes the backend contract for the Data Console without adding
any mutation or retry action. The following endpoints read the durable state
already persisted by the equity pipelines:

```text
GET /api/data/operations/overview
GET /api/data/operations/work-items
GET /api/data/operations/lifecycle-events
GET /api/data/operations/rate-limits
```

The overview combines queue depth, provider/exchange freshness, adaptive-rate
and circuit state, the latest accepted universe snapshot, recent ingestion
runs, and recent symbol lifecycle events. Work items and lifecycle events are
filterable and paginated so a frontend does not need to load the entire durable
history. Exchange alias `CA` is normalized to `TSX`; supported equity exchanges
are `NSE`, `TSX`, and `US`.

The existing coverage preview endpoint also accepts yfinance for all three
equity exchanges. It resolves symbols from the persisted active universe and
returns exact expected, stored, and missing session counts. The existing
Upstox/NSE request endpoint remains unchanged and is the only endpoint in this
phase that can start provider work.

Phase 9.1 is intentionally read-only. Phase 9.2 connects these contracts to the
Data Console UI. Phase 9.3 adds authenticated, audited, idempotent queue-based
manual retry actions. Phase 9.4 adds alert evaluation and delivery.

Direct database inspection is supported as a diagnostic complement to the API,
not as an application dependency. Production PostgreSQL is published only on
the server loopback interface and must be reached through SSH or an approved
private network. See [Database access with DBeaver](database_access_with_dbeaver.md).

#### Phase 9.1.1: Evidence reconciliation and exchange-aware run visibility

Provider-history evidence is also an operational terminal state. After the
evidence bootstrap writes active `verified_complete` or `verified_partial`
evidence, and before each enabled planner pass, the system cancels queued or
retry-wait historical work whose entire requested window is covered by one
verified evidence window. The cancellation is durable and uses
`provider_history_verified` as its reason.

This reconciliation applies only to `initial_backfill`,
`new_symbol_backfill`, and `gap_repair`. It never cancels running work,
`daily_incremental` freshness work, a partially overlapping window, inactive
evidence, quarantined evidence, or evidence from another provider, exchange,
instrument, or interval. This makes evidence refreshes and routine planner runs
self-healing without weakening freshness guarantees.

Durable Yahoo workers can process multiple exchanges in one ingestion run, so
their stored run exchange remains truthfully `MULTI`. Exchange-filtered
operations queries additionally match the exchanges of the run's claimed work
items, and each run response exposes `work_item_exchanges`. Consequently, NSE,
TSX, and US console views include the relevant shared worker runs without
rewriting their durable run identity.

#### Phase 9.2: Read-only Data Console UI

The Data route is the operator-facing equity console for NSE, TSX, and US. It
uses yfinance as the displayed daily provider and intentionally excludes Forex
from equity health calculations. The console has five exchange-aware views:

```text
Overview  Coverage  Work Queue  Runs  Lifecycle
```

Overview combines the accepted universe, stored candle freshness, durable open
work, suspicious rows, adaptive RPM/concurrency/circuit state, expected Dagster
schedule intent, recent runs, and recent universe changes. Coverage presents
calendar-aware expected, stored, and missing daily rows over the selected
window. Work Queue and Lifecycle support server-side filters and pagination.
Runs preserve `MULTI` as the durable worker identity while showing the actual
claimed `work_item_exchanges` used by the selected exchange view.

The console polls overview and rate state once per minute and also provides an
explicit refresh action. Coverage and paginated detail endpoints load only when
their view is selected. The UI is read-only: it cannot start a provider call,
change a schedule, or retry work. Authenticated, audited, idempotent manual
retry actions remain Phase 9.3.

The schedule panel reports configuration-derived intended state for the daily
planner, worker, universes, and calendars. It does not claim to be live Dagster
runtime state; runtime schedule and missed-tick alerts remain Phase 9.4.

#### Phase 9.2.1: Data Console correctness and operator diagnostics

Phase 9.2.1 closes the validation gaps found during the first production UI
review. NSE yfinance availability is seeded from the latest accepted persisted
NSE snapshot, so Coverage reports the same active universe used by planning
instead of rejecting NSE or falling back to a static seed. Failed coverage
requests render as unavailable and never as a real zero-percent result.

Every Data Console symbol search uses an exchange-scoped active-universe
typeahead. Suggestions start after two characters, are debounced, and rank an
exact symbol before symbol-prefix, company-name-prefix, provider-symbol-prefix,
and substring matches. The response retains the exchange symbol, Yahoo symbol,
company name, canonical identity, and provider instrument key. Coverage, Work
Queue, and Lifecycle share this behavior.

New durable worker runs persist per-exchange requested, processed, succeeded,
failed, retry-wait, terminal, cancelled, and lost-claim counts in run metadata.
This prevents one exchange from inheriting another exchange's failures from a
shared `MULTI` worker. Older shared runs without this metadata remain visible
but are explicitly labelled as unscoped global runs and are excluded from
exchange health calculations.

Run inspection is read-only and combines the run record, originally claimed
work items, current durable outcomes, provider request logs, HTTP status,
retry/rate-limit evidence, and the next automatic retry. Identical errors are
grouped while retaining affected symbols. A historical failed run whose work
later recovered is labelled recovered rather than left as an unexplained red
badge. Manual mutation remains Phase 9.3.

Initial-universe `added` events are displayed as `First observed`. This reflects
the durable event semantics without implying that the full baseline was newly
listed on the exchange on the snapshot date.

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
