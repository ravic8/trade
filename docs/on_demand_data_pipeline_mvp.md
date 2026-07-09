# On-Demand Data Pipeline MVP

This document defines the first write-enabled data workflow for the Trade
Research Agent UI.

The MVP lets a researcher request NSE daily OHLCV data for selected symbols and
a date range. The backend checks what already exists in TimescaleDB, fetches
only missing Upstox windows, upserts canonical candles, records durable run
state, validates the result, and shows progress plus provider limitations in
the UI.

## Implementation Principle

Do not create new scripts, tables, routes, or services first.

Before adding anything new, inspect and reuse what the current repo can already
do:

```text
1. Identify the existing CLI, pipeline, storage, API, and UI capabilities.
2. Use existing functions and tables where they satisfy the MVP contract.
3. Extend existing modules in place when the missing behavior is small and
   naturally belongs there.
4. Add new modules, scripts, or tables only after confirming the current repo
   does not already provide a suitable home.
5. Keep every new addition tied to a specific gap in the design.
```

The MVP should evolve from the existing daily Upstox pipeline, Timescale store,
FastAPI app, and React shell. New code is allowed, but it must be justified by
a concrete missing capability rather than introduced as a parallel architecture.

Repo-first does not mean repo-constrained. If the existing implementation is a
poor fit for an interactive UI workflow, we should change structure or code
rather than force the MVP through slow, brittle, or hard-to-operate paths.

Acceptable reasons to introduce new structure or replace an existing approach:

```text
latency: UI requests need fast preview, quick status updates, and bounded waits
scalability: many symbols/windows need task-level execution and rate control
performance: coverage checks and upserts need efficient DB access patterns
reliability: run state, retry state, and validation results must survive failure
operability: researchers need clear progress, errors, limits, and retry options
maintainability: UI-triggered jobs should not overload batch-only abstractions
```

The decision rule:

```text
Reuse existing code when it is correct, observable, and fast enough.
Extend existing code when a small change makes it fit the MVP.
Restructure or add new code when the current path would compromise latency,
scalability, performance, reliability, or future maintainability.
```

## Product Goal

Build a local research data control center that answers:

```text
Can I request the market data I need, understand what will be fetched, avoid
refetching data already stored, and see whether the resulting dataset is
research-ready?
```

The first vertical slice is intentionally narrow:

```text
NSE equities
Upstox provider
Historical daily OHLCV only
Selected symbols and date range
Fetch + upsert + validation
UI-triggered run with status and coverage result
```

## Non-Goals For The MVP

Do not include these in the first implementation:

```text
intraday historical fetching
features, targets, factor research, or ML pipeline execution
broker orders or paper trading
autonomous scheduling
multi-provider routing
cloud deployment or auth redesign
full Dagster migration for UI-triggered jobs
```

The schema and UI should leave space for these later, but the first shippable
workflow should stay daily OHLCV-focused.

## Current Repo Fit

Existing pieces to reuse:

```text
src/trade_research/data/upstox.py
  UpstoxHistoricalDataProvider.fetch_daily_candles()

src/trade_research/storage/timescale.py
  ohlcv_daily
  provider_instruments
  ingestion_runs
  daily_ohlcv_fetch_coverage
  data_quality_audits
  TimescaleStore.upsert_daily_ohlcv()

src/trade_research/pipelines/daily_ohlcv.py
  existing batch daily OHLCV fetch planning, auditing, and retry concepts

apps/web
  existing React shell, research pages, API client, and jobs page patterns
```

The MVP should extend these patterns instead of creating a separate ingestion
stack.

Before implementation, perform a repo capability inventory:

```text
CLI commands that already trigger Upstox daily ingestion
pipeline functions that already plan daily fetch windows
TimescaleStore methods for OHLCV, runs, audits, and coverage
existing coverage tables and whether they can support UI-run detail
FastAPI route patterns and schema conventions
React app shell, jobs page, and API client patterns
tests covering daily OHLCV, latest predictions, and storage behavior
```

Only after this inventory should we decide which parts need new code.

## Phase 0 Capability Audit Findings

This audit maps the current repo to the MVP contract before adding feature code.

### Existing Capabilities To Reuse

#### Upstox daily fetch

```text
src/trade_research/data/upstox.py
```

Reusable:

```text
UpstoxHistoricalDataProvider.fetch_daily_candles()
_daily_candles_to_frame()
UpstoxAPIError
```

Fit:

```text
Already uses Upstox V3 historical daily endpoint:
/v3/historical-candle/:instrument_key/days/1/:to_date/:from_date
```

MVP decision:

```text
Reuse for sequential execution.
Do not create a new provider for daily MVP.
Add async/bounded parallel provider later only if Phase 5 requires it.
```

#### Daily OHLCV batch pipeline

```text
src/trade_research/pipelines/daily_ohlcv.py
```

Reusable:

```text
run_upstox_daily_ohlcv_pipeline()
run_upstox_daily_ohlcv_retry_pipeline()
plan_daily_fetch_windows()
build_daily_fetch_coverage()
```

Fit:

```text
The batch pipeline already starts an ingestion run, plans incremental fetches,
fetches Upstox candles, writes audit/failure/coverage artifacts, upserts
ohlcv_daily, inserts data_quality_audits, and stores daily_ohlcv_fetch_coverage.
```

Limitations for UI:

```text
plan_daily_fetch_windows() only uses latest stored date per instrument.
It does not detect gaps inside an existing date range.
It expects a mapping DataFrame instead of resolving ad hoc UI symbols.
It writes Parquet/CSV artifacts as part of the batch workflow.
It is synchronous and sequential with sleep-based throttling.
```

MVP decision:

```text
Reuse planning and coverage helpers where suitable.
Extract or add a UI-oriented coverage planner for exact missing-session preview.
Avoid calling the full batch pipeline directly from the UI because preview,
symbol-level status, and low-latency interaction need a smaller command path.
```

#### CLI commands

```text
src/trade_research/cli.py
```

Reusable:

```text
fetch-upstox-instruments
map-liquid-nse-upstox
fetch-upstox-nse-daily
retry-upstox-nse-daily
init-db
```

Fit:

```text
The CLI is good for local batch operation, smoke tests, and regression checks.
```

Limitations for UI:

```text
CLI commands are not an API boundary.
They do not provide preview-first UX or interactive polling.
They assume file paths and batch artifacts.
```

MVP decision:

```text
Keep CLI as operational tooling.
Do not shell out to CLI from FastAPI.
Call package functions or new package-level services directly.
```

#### Timescale/Postgres storage

```text
src/trade_research/storage/timescale.py
```

Reusable tables:

```text
provider_instruments
ohlcv_daily
ingestion_runs
daily_ohlcv_fetch_coverage
data_quality_audits
exchange_holidays
stock_coverage_runs
stock_coverage_by_window
```

Reusable methods:

```text
initialize()
upsert_provider_instruments()
upsert_daily_ohlcv()
latest_daily_ohlcv_dates()
daily_ohlcv_frame()
start_ingestion_run()
finish_ingestion_run()
latest_runs()
insert_daily_ohlcv_fetch_coverage()
daily_ohlcv_fetch_retry_candidates()
insert_data_quality_audits()
```

Fit:

```text
ohlcv_daily is the right canonical table for daily MVP.
ingestion_runs can represent coarse run history.
daily_ohlcv_fetch_coverage can represent per-symbol fetch outcomes.
data_quality_audits can store validation summaries.
provider_instruments is the right symbol-to-instrument source.
```

Limitations for UI:

```text
ingestion_runs starts at running; it has no queued/planning state.
ingestion_runs has no step timeline.
daily_ohlcv_fetch_coverage is per instrument per run, not per chunk/attempt.
There is no direct method to resolve UI symbols from provider_instruments.
There is no efficient exact coverage method returning stored dates per requested
instrument/date range.
market_status still focuses on hourly ohlcv and older jobs.
```

MVP decision:

```text
Reuse ohlcv_daily, provider_instruments, ingestion_runs, daily_ohlcv_fetch_coverage,
and data_quality_audits for the first MVP.
Add TimescaleStore methods for symbol resolution, exact daily coverage, coverage
preview rows, and run detail.
Only add new tables if these reused tables cannot support adequate UI progress
and retry visibility after the first vertical slice.
```

#### Market calendar

```text
src/trade_research/market_calendar.py
```

Reusable:

```text
ExchangeSessionConfig
ExchangeHolidays
fetch_exchange_holidays()
session_decision()
```

Fit:

```text
The repo already knows NSE timezone, market times, and holiday sources.
```

Limitations for UI:

```text
There is no pure helper that returns expected trading dates for a historical
range using stored/fetched holidays.
fetch_exchange_holidays() may require network access if holidays are not stored.
```

MVP decision:

```text
Add a pure expected-trading-days helper that uses weekday calendar plus stored
exchange_holidays when available. Avoid network calendar fetches in the preview
path unless explicitly requested later.
```

#### FastAPI app

```text
src/trade_research/api/app.py
```

Reusable patterns:

```text
route registration in the main app
Pydantic response_model usage
_store() cached TimescaleStore factory
SQLAlchemy fallback handling for read endpoints
/api/jobs/latest from ingestion_runs
```

Fit:

```text
The app already exposes research, jobs, chat, market, and symbol endpoints.
```

Limitations for UI data pipeline:

```text
No /api/data namespace yet.
No write-enabled pipeline endpoint outside chat feedback.
No BackgroundTasks or durable queued execution pattern.
Current jobs endpoint hides fetch coverage detail.
```

MVP decision:

```text
Add /api/data endpoints in the existing app or a small router module imported by
the app if app.py becomes too large.
Use package services, not CLI subprocesses.
Use response_model schemas in src/trade_research/schemas.py unless the schema
file becomes too broad, in which case introduce a focused API schema module.
```

#### React UI

```text
apps/web/src
```

Reusable:

```text
AppShell navigation
createBrowserRouter route setup
PageHeader
panel/table/status-pill styling
api/client.ts fetchJson pattern
api/hooks.ts React Query patterns
JobsPage run table pattern
ResearchProgressPage progress-detail pattern
```

Fit:

```text
The UI already supports data-heavy internal research pages and job views.
```

Limitations for MVP:

```text
No /data route.
No mutation flow except chat.
JobRun type is too narrow for fetch/validation details.
fetchJson currently always takes a fallback, which is useful for read pages but
write actions should surface errors more explicitly.
```

MVP decision:

```text
Add a /data route using existing layout components and React Query hooks.
Add typed API client methods for capabilities, preview, create run, list runs,
and run detail.
For write endpoints, use a stricter request helper that does not silently return
mock fallback data.
```

#### Tests

Existing coverage:

```text
tests/test_upstox_provider.py
tests/test_daily_ohlcv_pipeline.py
tests/test_timescale_feature_storage.py
tests/test_research_api.py
tests/test_market_calendar.py
```

Reusable patterns:

```text
httpx.MockTransport for provider tests
pure dataframe tests for planning/auditing
Timescale row-normalization tests without live DB
FastAPI TestClient for API smoke tests
```

MVP decision:

```text
Follow existing test style.
Keep Upstox network fully mocked.
Add pure tests before DB-backed integration tests.
```

### MVP Gap List

Implement these gaps before UI fetch execution is considered complete:

```text
provider capability registry for Upstox V3 limits
Pydantic schemas for data pipeline requests/responses
symbol resolution from provider_instruments for ad hoc UI symbols
exact daily stored-date coverage query for requested instruments/range
expected NSE trading date helper for historical ranges
coverage preview service that does not fetch or write data
strict API client helper for write endpoints
/api/data/provider-capabilities/upstox
/api/data/coverage/preview
/api/data/pipeline-requests
/api/data/pipeline-runs
/api/data/pipeline-runs/{run_id}
/data UI route and navigation
tests for capabilities, symbol resolution, exact coverage, preview, and API
```

### Initial Reuse/Change Decision

For the first vertical slice:

```text
Reuse:
  UpstoxHistoricalDataProvider.fetch_daily_candles()
  TimescaleStore.upsert_daily_ohlcv()
  TimescaleStore.start_ingestion_run()/finish_ingestion_run()
  daily_ohlcv_fetch_coverage
  data_quality_audits
  existing React shell and jobs/progress UI patterns

Extend:
  TimescaleStore with symbol resolution and exact daily coverage methods
  daily planning with exact missing-session preview
  schemas.py with data pipeline API models
  app.py or a small imported router with /api/data endpoints
  config.py with data pipeline guardrail settings

Add only if needed:
  new operational tables for queued/planning/step-level progress
  async Upstox provider and bounded parallel executor
  separate frontend request helper for non-fallback mutations
```

### Step 4 Run-State Decision

Implemented run-state visibility without adding new operational tables.

Current reuse path:

```text
ingestion_runs
  -> coarse run summary and status

daily_ohlcv_fetch_coverage
  -> per-symbol fetch outcome for daily OHLCV runs
```

Added backend read surface:

```text
GET /api/data/pipeline-runs
GET /api/data/pipeline-runs/{run_id}
```

Current limitation:

```text
ingestion_runs still has no queued/planning state or step timeline.
daily_ohlcv_fetch_coverage is per symbol, not per provider-call attempt.
```

Decision:

```text
Do not add new run/task tables yet.
Use the existing tables for Step 5 sequential execution.
Revisit new tables only when queued execution, retries, or parallel per-window
attempt tracking require stronger state modeling.
```

### Step 7 Backend Test Coverage

Implemented focused backend tests before UI work.

Covered:

```text
Upstox V3 capability registry and API response
daily coverage preview success path
unresolved symbol reporting
ambiguous symbol reporting
weekday-only warning when stored holidays are unavailable
non-daily request rejection
expected trading date calendar behavior
sequential executor success path
provider failure recording and warning status
missing Upstox token guard
pipeline run list/detail APIs
coverage GET/POST APIs
pipeline request API validation
```

Verification command:

```bash
.venv/bin/python -m pytest \
  tests/test_provider_capabilities.py \
  tests/test_data_coverage_preview.py \
  tests/test_on_demand_pipeline.py \
  tests/test_market_calendar.py \
  tests/test_research_api.py
```

Latest focused result:

```text
29 passed, 1 existing Starlette TestClient deprecation warning
```

### Step 8 UI Implementation

Implemented the first `/data` UI page.

Covered:

```text
/data route and sidebar navigation
NSE daily Upstox request form
Upstox provider limits panel sourced from backend capabilities
coverage preview mutation
preview metrics and planned fetch task table
write-enabled run button for POST /api/data/pipeline-requests
pipeline run history table
selected run detail and fetch coverage table
strict frontend API helper for write/data endpoints
```

Verification command:

```bash
cd apps/web
npm run build
```

Latest focused result:

```text
TypeScript build and Vite production build passed
```

### Step 9 UI Safety And Polish

Implemented safety guardrails around the write-enabled UI.

Covered:

```text
GET /api/data/pipeline-health
Upstox token/config readiness display
Run button blocked until coverage preview succeeds
Run button blocked when there are no missing rows
Run button blocked for unresolved or ambiguous symbols
Visible safety checklist in the request panel
Preview warning panel for unresolved/ambiguous symbols and calendar caveats
Run detail error list for stored provider failures
Strict frontend errors for write/data endpoints
```

Verification:

```text
npm run build passed
backend ruff check passed
focused backend tests passed: 30 passed, 1 existing TestClient warning
browser smoke check /data passed with no console errors
```

### Step 10 Bounded Parallelism

Implemented bounded provider fetch concurrency for the sequential executor.

Design:

```text
coverage planning remains exact and read-only
execution consolidates missing windows to one span per instrument for the
current reused daily_ohlcv_fetch_coverage schema
provider fetch calls may run concurrently
database upserts, fetch coverage inserts, and audit inserts remain sequential
```

Configuration:

```text
DATA_PIPELINE_MAX_CONCURRENT_FETCHES defaults to 1
DATA_PIPELINE_THROTTLE_SECONDS defaults to 0
```

API/UI visibility:

```text
GET /api/data/pipeline-health returns max_concurrent_fetches
/data safety panel shows the configured concurrency limit
```

Latest focused result:

```text
backend ruff check passed
focused backend tests passed: 31 passed, 1 existing TestClient warning
npm run build passed
```

### Step 11 End-To-End Verification

Verified the MVP integration surface after Step 10.

Automated checks:

```text
backend ruff check passed
focused backend tests passed: 31 passed, 1 existing TestClient warning
npm run build passed
```

Local service checks:

```text
FastAPI started on http://127.0.0.1:8000
GET /api/health returned ok
GET /api/data/pipeline-health returned token_configured=true and concurrency=1
GET /api/data/provider-capabilities/upstox returned the documented Upstox limits
```

Browser smoke check:

```text
http://localhost:5173/data rendered the on-demand OHLCV workflow
Upstox limits panel rendered from the backend capability endpoint
Run safety checklist rendered and kept the Run button blocked before preview
No browser console errors were observed during the smoke check
```

Live data validation:

```text
Started Timescale/Postgres with docker compose.
Initialized the Timescale schema with trade-research init-db.
Loaded the Upstox instrument master into provider_instruments.

Previewed RELIANCE for 2024-01-02 through 2024-01-05:
expected_rows=4, already_present_rows=0, missing_rows=4, provider_calls=1.

Executed POST /api/data/pipeline-requests for the same tiny window.
Run 261ea913-c7e6-4b13-9b7d-9dce6a3322c1 completed with:
items_succeeded=1, items_failed=0, rows_fetched=4.

Re-previewed the same request:
expected_rows=4, already_present_rows=4, missing_rows=0, provider_calls=0.

Verified ohlcv_daily directly:
NSE_EQ|INE002A01018 / RELIANCE has 4 rows from 2024-01-02 to 2024-01-05.
```

### Step 12 Documentation Update

Step 12 closes the initial MVP by recording the final implementation choices,
the repo capabilities reused, the changes introduced, known limitations, and
the next technical expansion points.

#### What We Reused

```text
UpstoxHistoricalDataProvider.fetch_daily_candles()
  -> live daily candle fetches from the existing Upstox V3 provider client

provider_instruments
  -> symbol to Upstox instrument-key resolution

ohlcv_daily
  -> canonical daily OHLCV storage, with existing upsert behavior

ingestion_runs
  -> durable pipeline run summary and status

daily_ohlcv_fetch_coverage
  -> per-symbol fetch outcome for run detail and retry visibility

data_quality_audits
  -> validation/audit persistence for fetched daily OHLCV data

FastAPI app structure
  -> added data endpoints to the existing API surface

React app shell and API client patterns
  -> added /data page without creating a separate frontend app
```

#### What We Changed

```text
Added UI-oriented exact coverage preview instead of using only latest-date
batch planning.

Added provider capability metadata so Upstox interval/date/rate limitations are
visible before users run a fetch.

Added request validation and safety gating so Run is blocked until preview,
symbol resolution, token readiness, and missing-row checks pass.

Added bounded provider fetch parallelism while keeping DB writes sequential.

Extended TimescaleStore with targeted query methods for symbol resolution,
coverage dates, pipeline runs, and run fetch coverage.

Added a dedicated /data UI workflow for request, preview, run, run history, and
run detail.
```

#### Tables Used

```text
provider_instruments
ohlcv_daily
ingestion_runs
daily_ohlcv_fetch_coverage
data_quality_audits
exchange_holidays
```

No new database tables were required for the initial MVP. The existing tables
were sufficient for a synchronous, daily-only, missing-data fetch workflow.

#### Endpoints Added

```text
GET  /api/data/provider-capabilities/upstox
GET  /api/data/pipeline-health
POST /api/data/coverage/preview
GET  /api/data/coverage
POST /api/data/pipeline-requests
GET  /api/data/pipeline-runs
GET  /api/data/pipeline-runs/{run_id}
```

#### Frontend Added

```text
/data route
sidebar navigation entry
New Data Request form
Upstox Limits panel
coverage preview summary
run safety checklist
run history
selected run detail
fetch coverage/error display
```

#### Tests Added Or Extended

```text
tests/test_provider_capabilities.py
tests/test_data_coverage_preview.py
tests/test_on_demand_pipeline.py
tests/test_market_calendar.py
tests/test_research_api.py
```

Covered:

```text
provider capability metadata
symbol resolution and unresolved/ambiguous behavior
coverage preview math
weekday fallback when exchange holiday data is unavailable
pipeline request validation
mocked successful fetch and DB upsert path
provider failure recording
run list/detail APIs
bounded parallel fetch execution
```

#### Known Limitations

```text
Only NSE equities are supported.
Only Upstox is supported.
Only daily OHLCV is executable from the UI.
Intraday intervals are displayed as provider documentation but not yet fetched.
Execution is synchronous inside the API request.
Run detail is per symbol/consolidated window, not per provider-call chunk.
No queued/background worker state yet.
No retry button in the UI yet.
No searchable symbol picker yet.
No liquid-universe selector yet.
No available-data inventory table yet.
Holiday-aware previews need exchange_holidays populated; otherwise weekdays are
used with a warning.
```

#### Verified Result

```text
Focused backend tests passed: 31 passed, 1 existing TestClient warning.
Frontend TypeScript/Vite build passed.
FastAPI health and data endpoints responded locally.
The /data UI rendered and passed browser smoke checks with no console errors.

Live Upstox validation succeeded:
RELIANCE / NSE_EQ|INE002A01018
2024-01-02 through 2024-01-05
4 rows fetched
4 rows stored in ohlcv_daily
re-preview showed missing_rows=0 and provider_calls=0
```

#### Next Steps After Initial MVP

```text
Intraday:
  add interval-aware planning, chunking, storage tables, and UI interval
  execution for minute/hour candles.

Features:
  allow researchers to trigger feature generation from stored OHLCV after data
  coverage is complete.

Targets:
  allow researchers to generate forward-return targets after feature datasets
  exist for the selected universe/date range.

Search:
  add symbol autocomplete, bulk paste validation, selected symbol chips, and
  server-side instrument search.

Liquid universes:
  add bounded most-liquid selection with preview-before-run safety.

Available data:
  add a paginated inventory table showing first/latest date, coverage %, missing
  rows, and last fetch status by symbol.

Execution reliability:
  move long-running jobs to a background queue with polling, retries, and
  per-task/chunk status.
```

### Recommended Post-MVP Implementation Slice

Start with read-only discovery improvements before adding larger fetch actions:

```text
1. Add GET /api/data/availability backed by provider_instruments and ohlcv_daily.
2. Add a paginated Available Data table to /data.
3. Add GET /api/data/instruments/search for symbol autocomplete.
4. Replace the textarea with search + selected symbol chips + paste validation.
5. Add liquid-universe read APIs using tradable_universes and members.
6. Add bounded select-all-filtered actions that always require preview before run.
7. Move long-running requests to background execution when requests can exceed
   short API timeouts.
```

This sequence keeps the next work low-risk: researchers first gain visibility
into available data, then selection improves, then larger universe fetches become
safe enough to expose.

## End-To-End Flow

```text
Researcher submits request in UI
  -> FastAPI validates request and provider capabilities
  -> backend creates pipeline_request and pipeline_run records
  -> coverage planner resolves symbols to Upstox instrument keys
  -> planner computes expected NSE trading sessions
  -> planner compares expected sessions with ohlcv_daily
  -> planner creates fetch tasks only for missing windows
  -> background runner executes fetch tasks
  -> Upstox V3 historical daily API returns candles
  -> backend normalizes positional candle arrays into named fields
  -> TimescaleStore.upsert_daily_ohlcv() writes canonical rows
  -> backend records fetch attempts and run steps
  -> validation checks expected vs stored candles
  -> UI polls run status and shows coverage, warnings, and errors
```

## Architecture Decision

Use FastAPI as the UI command layer for the MVP.

```text
React UI
  -> FastAPI data pipeline endpoints
  -> TimescaleDB/PostgreSQL run metadata and market data
  -> in-process background runner for MVP
  -> Upstox V3
```

Dagster remains the right home for scheduled research assets and broader daily
pipelines. For this MVP, UI-triggered runs should be DB-backed FastAPI jobs
because they are parameterized, small, and easier to iterate. The data model
should make a later Dagster handoff straightforward by storing run config,
steps, task status, and provenance in tables.

## Database Design

### Canonical Market Data

Reuse `ohlcv_daily` for the MVP:

```text
instrument_key
source
date
symbol
exchange
open
high
low
close
volume
open_interest
fetched_at
quality_status
```

The current primary key is:

```text
instrument_key + source + date
```

That is sufficient for daily Upstox NSE data. When intraday support arrives,
add a separate canonical intraday table instead of overloading `ohlcv_daily`.

### Operational Run State

First, evaluate whether existing tables can satisfy the UI-triggered run
contract:

```text
ingestion_runs
daily_ohlcv_fetch_coverage
data_quality_audits
stock_coverage_runs
stock_coverage_by_window
```

If these can support MVP visibility with small extensions, prefer extending or
reusing them. Introduce new operational tables only when the existing schema
cannot represent request-level intent, step-level progress, or per-task retry
state cleanly.

### Candidate New Operational Tables

These tables are candidates, not automatic first steps. Add them only after the
repo capability inventory confirms that existing run and coverage tables are
insufficient for the UI-triggered MVP.

#### `data_pipeline_requests`

One user intent from the UI.

```text
request_id primary key
requested_by nullable string
provider string
exchange string
symbols json
unit string
interval string
start_date date
end_date date
steps json
mode string
status string
created_at timestamptz
request_json json
```

Initial values:

```text
provider = upstox
exchange = NSE
unit = days
interval = 1
steps = ["fetch_ohlcv", "validate_ohlcv"]
mode = incremental_missing_only
```

#### `data_pipeline_runs`

One execution attempt for a request.

```text
run_id primary key
request_id
provider
exchange
status
started_at timestamptz
finished_at timestamptz
total_tasks bigint
completed_tasks bigint
succeeded_tasks bigint
failed_tasks bigint
skipped_tasks bigint
rows_fetched bigint
rows_upserted bigint
error_message string
run_metadata json
```

Statuses:

```text
queued
planning
running
validating
succeeded
succeeded_with_warnings
failed
canceled
```

#### `data_pipeline_run_steps`

Step-level timeline for UI progress.

```text
run_id
step_name
status
started_at timestamptz
finished_at timestamptz
message string
metrics_json json
error_message string
```

Step names for MVP:

```text
resolve_symbols
plan_coverage
fetch_ohlcv
upsert_ohlcv
validate_ohlcv
```

#### `ohlcv_fetch_tasks`

One provider call or skipped window.

```text
task_id primary key
run_id
provider
exchange
symbol
instrument_key
unit
interval
fetch_start date
fetch_end date
status
attempt_count bigint
rows_fetched bigint
rows_upserted bigint
skip_reason string
error_code string
error_message string
started_at timestamptz
finished_at timestamptz
created_at timestamptz
```

Statuses:

```text
planned
skipped_already_covered
queued
running
succeeded
empty
failed
rate_limited
```

#### `ohlcv_validation_reports`

Validation summary shown to the researcher.

```text
report_id primary key
run_id
provider
exchange
unit
interval
start_date date
end_date date
status
expected_rows bigint
observed_rows bigint
missing_rows bigint
duplicate_rows bigint
zero_volume_rows bigint
invalid_price_rows bigint
missing_dates_json json
warnings_json json
created_at timestamptz
```

Validation statuses:

```text
pass
warn
fail
```

### Existing Tables To Keep

Keep using:

```text
provider_instruments
ohlcv_daily
data_quality_audits
daily_ohlcv_fetch_coverage
ingestion_runs
```

`daily_ohlcv_fetch_coverage` can remain the batch-pipeline coverage table. The
new `ohlcv_fetch_tasks` table is more granular and UI-run oriented. We can later
converge them if the schemas naturally settle.

## Provider Capability Registry

Provider limitations must live in code and be exposed to the UI through an API.
Do not hardcode separate copies in frontend text.

Initial registry entry:

```json
{
  "provider": "upstox",
  "api_version": "v3",
  "source_url": "https://upstox.com/developer/api-documentation/v3/get-historical-candle-data/",
  "historical": [
    {
      "unit": "minutes",
      "interval_min": 1,
      "interval_max": 15,
      "available_from": "2022-01-01",
      "max_window": "1 month"
    },
    {
      "unit": "minutes",
      "interval_min": 16,
      "interval_max": 300,
      "available_from": "2022-01-01",
      "max_window": "1 quarter"
    },
    {
      "unit": "hours",
      "interval_min": 1,
      "interval_max": 5,
      "available_from": "2022-01-01",
      "max_window": "1 quarter"
    },
    {
      "unit": "days",
      "interval_min": 1,
      "interval_max": 1,
      "available_from": "2000-01-01",
      "max_window": "10 years"
    },
    {
      "unit": "weeks",
      "interval_min": 1,
      "interval_max": 1,
      "available_from": "2000-01-01",
      "max_window": null
    },
    {
      "unit": "months",
      "interval_min": 1,
      "interval_max": 1,
      "available_from": "2000-01-01",
      "max_window": null
    }
  ],
  "rate_limits": {
    "standard_api_per_second": 50,
    "standard_api_per_minute": 500,
    "standard_api_per_30_minutes": 2000
  }
}
```

The UI should render this near the request form as "Upstox API Limits" and link
to the official docs.

## Coverage Planning

For MVP daily data, coverage should be computed at trading-session granularity.

Inputs:

```text
exchange
symbols
instrument_keys
start_date
end_date
unit = days
interval = 1
provider = upstox
```

Planner responsibilities:

```text
1. Resolve symbols to provider instrument keys.
2. Clamp request to provider availability and max per-call range.
3. Build expected NSE trading sessions from market calendar.
4. Query ohlcv_daily for existing dates per instrument.
5. Compute missing dates: expected_sessions - stored_dates.
6. Group missing dates into Upstox-legal fetch windows.
7. Create skipped tasks for already-covered symbols/windows.
8. Estimate provider calls, expected rows, and warnings before execution.
```

The request should fail before execution when:

```text
symbol cannot be resolved to exactly one Upstox NSE equity instrument
start_date > end_date
requested interval is unsupported
requested range is entirely before provider availability
too many symbols are requested for configured MVP limits
```

## Upstox Request Chunking

Daily MVP:

```text
unit = days
interval = 1
max Upstox historical retrieval window = 10 years
```

For daily data, most MVP requests will fit in one call per symbol. The planner
should still use the generic chunking interface so minute/hour support can reuse
it later.

Future examples:

```text
1-minute data for 2 years
  -> split into monthly chunks

30-minute data for 2 years
  -> split into quarterly chunks

hourly data for 1 year
  -> split into quarterly chunks
```

## Bounded Parallelism

Design for parallel execution now, but implement in two steps.

### MVP v1

Use sequential task execution for the first mergeable slice:

```text
for each fetch task:
  call Upstox
  normalize candles
  upsert rows
  record task outcome
```

This keeps run state, validation, and UI progress easy to verify.

### MVP v1.1

Add bounded parallel fetches:

```text
max_concurrent_fetches = 4 by default
internal_request_rate_limit_per_second < Upstox documented limit
internal_request_rate_limit_per_minute < Upstox documented limit
```

Preferred implementation:

```text
asyncio + httpx.AsyncClient + asyncio.Semaphore
```

Rules:

```text
Never use unlimited parallelism.
Record every task attempt before and after provider calls.
Use idempotent upserts into canonical tables.
Run validation only after all tasks finish or are terminal.
Expose concurrency and estimated provider calls in the UI.
```

## FastAPI Contract

### Provider Capabilities

```text
GET /api/data/provider-capabilities/upstox
```

Returns the provider capability registry entry.

### Create Pipeline Request

```text
POST /api/data/pipeline-requests
```

Request:

```json
{
  "provider": "upstox",
  "exchange": "NSE",
  "symbols": ["RELIANCE", "INFY"],
  "unit": "days",
  "interval": 1,
  "start_date": "2024-01-01",
  "end_date": "2026-07-08",
  "steps": ["fetch_ohlcv", "validate_ohlcv"],
  "mode": "incremental_missing_only"
}
```

Response:

```json
{
  "request_id": "req_...",
  "run_id": "run_...",
  "status": "queued"
}
```

### Preview Coverage Plan

```text
POST /api/data/coverage/preview
```

This endpoint performs planning without executing provider calls.

Response:

```json
{
  "provider": "upstox",
  "exchange": "NSE",
  "unit": "days",
  "interval": 1,
  "symbols_requested": 2,
  "symbols_resolved": 2,
  "estimated_provider_calls": 2,
  "expected_rows": 1240,
  "already_present_rows": 840,
  "missing_rows": 400,
  "tasks": [
    {
      "symbol": "RELIANCE",
      "instrument_key": "NSE_EQ|INE002A01018",
      "fetch_start": "2025-11-01",
      "fetch_end": "2026-07-08",
      "status": "queued"
    }
  ],
  "warnings": []
}
```

### List Runs

```text
GET /api/data/pipeline-runs
```

Query parameters:

```text
provider
exchange
status
limit
```

### Run Detail

```text
GET /api/data/pipeline-runs/{run_id}
```

Returns:

```text
request
run summary
steps
fetch tasks
validation report
warnings and errors
```

### Coverage Query

```text
GET /api/data/coverage
```

Query parameters:

```text
provider
exchange
symbols
unit
interval
start_date
end_date
```

Returns observed coverage without creating a run.

## UI Design

Add a data workspace route:

```text
/data
```

Suggested subviews:

```text
Request
Runs
Coverage
Provider Limits
```

### Request View

Controls:

```text
provider select: Upstox
exchange select: NSE
symbol input: comma-separated symbols for MVP
unit/interval select: days / 1 for MVP
date range inputs
steps checkboxes: Fetch OHLCV, Validate OHLCV
preview button
run button
```

The Upstox limits panel should sit beside or below the form and update from
`GET /api/data/provider-capabilities/upstox`.

### Runs View

Show:

```text
run id
created/started/finished time
provider
exchange
symbol count
date range
status
tasks completed
rows fetched
rows upserted
warning/error count
```

### Run Detail View

Show:

```text
step timeline
task table
validation summary
missing dates
provider errors
retry candidates
```

### Coverage View

Show:

```text
symbol
instrument key
date range
expected sessions
stored sessions
missing sessions
coverage percentage
latest stored date
```

## Validation Rules

For daily NSE OHLCV:

```text
expected sessions are NSE trading sessions between start_date and end_date
observed sessions come from ohlcv_daily by instrument_key/source/date
duplicate check uses instrument_key + source + date
OHLC must be positive
high >= low
high >= open and high >= close
low <= open and low <= close
volume >= 0
```

Validation output:

```text
pass: no missing expected sessions and no hard invalid rows
warn: missing sessions or empty provider windows, but at least partial data exists
fail: unresolved instruments, provider failure for all tasks, or hard invalid rows
```

## Error Handling

User-facing errors should be specific:

```text
Unsupported interval for Upstox V3.
Date range exceeds Upstox daily max window; request will be chunked.
Symbol RELIANCE could not be resolved to an NSE equity instrument.
Upstox returned invalid date range for task X.
Upstox access token is not configured.
```

Provider errors should be stored in task rows with:

```text
status
error_code
error_message
attempt_count
```

Never discard failed task details after a run completes.

## Security And Safety

MVP is local-first, but write actions still need guardrails:

```text
No broker order endpoints.
No raw SQL from UI.
No arbitrary provider URL input.
No unsupported interval execution.
Require configured Upstox token on backend only.
Bound max symbols per request with a setting.
Bound max date range using provider capability registry.
```

## Settings

Add settings with conservative defaults:

```text
data_pipeline_enabled = true
data_pipeline_max_symbols_per_request = 25
data_pipeline_default_provider = upstox
data_pipeline_max_concurrent_fetches = 1
data_pipeline_internal_rps_limit = 5
data_pipeline_internal_rpm_limit = 100
```

Parallelism settings exist from the start, but default concurrency stays `1`
until the sequential path is tested.

## Implementation Plan

### Phase 0: Repo Capability Inventory

```text
read current CLI commands for Upstox daily OHLCV
read daily_ohlcv pipeline planning and retry code
read TimescaleStore run, coverage, audit, and OHLCV methods
read FastAPI route and Pydantic schema patterns
read React jobs/research pages and API client patterns
map current capabilities to the MVP contract
identify gaps before adding new files or tables
```

Deliverable:

```text
short implementation map listing reused components, extension points, and
justified new additions
```

### Phase 1: Backend Skeleton

```text
provider capability registry
minimal SQLAlchemy extensions only if existing tables cannot cover the MVP
TimescaleStore methods added only for missing request/run/coverage operations
Pydantic request/response schemas
FastAPI endpoints for capabilities, preview, create run, list runs, run detail
```

### Phase 2: Planner And Sequential Runner

```text
symbol resolution from provider_instruments
daily coverage planner from ohlcv_daily
Upstox-legal chunk planning
sequential background execution
daily candle upsert
validation report writing
```

### Phase 3: UI

```text
/data route
request form
provider limits panel
preview result
run history
run detail
coverage table
```

### Phase 4: Tests And Hardening

```text
provider capability tests
date range and chunking tests
coverage planner tests
API validation tests
runner success/failure tests with mocked Upstox client
validation report tests
```

### Phase 5: Bounded Parallelism

```text
async Upstox provider client
task semaphore
internal rate limiter
parallel runner tests
UI display for concurrency and estimated provider calls
```

## Open Decisions

Resolve these before Phase 2:

```text
Should UI runs reuse ingestion_runs or only use new data_pipeline_runs?
How strict should symbol resolution be when trading_symbol and symbol differ?
Should validation include known NSE holidays from exchange_holidays only, or
also infer missing market sessions from actual stored universe coverage?
Should daily UI requests allow full-refresh overwrite mode in MVP, or only
incremental_missing_only?
```

Recommended MVP answers:

```text
Use new data_pipeline_runs and optionally mirror summary into ingestion_runs.
Require exact symbol/trading_symbol match for MVP unless a unique mapping exists.
Use exchange_holidays and weekday calendar for expected sessions.
Only allow incremental_missing_only in the first UI.
```
