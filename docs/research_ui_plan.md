# Research UI Implementation Plan

This document describes the local research console for the Trade Research
Agent. The first version is read-only and explains the current research
pipeline and factor outputs from existing generated artifacts.

Current implementation note:

```text
The first read-only slice is implemented. It is artifact-backed, not
database-backed. The API routes currently live in src/trade_research/api/app.py,
and artifact parsing lives in src/trade_research/research/artifacts.py.
```

## Goal

Build a local UI that answers two questions:

```text
1. What is the current state of our research pipeline?
2. What is the factor research saying about our features?
```

The UI should make the current data foundation understandable without requiring
manual inspection of many CSV, Parquet, and JSON files.

## Product Shape

Initial pages:

```text
/research/progress
/research/factors
```

The UI is not a trading dashboard. It is an internal research workstation for
pipeline status, evidence review, and next-step decisions.

## Current Inputs

The first implementation should read generated local artifacts only.

Pipeline summary inputs:

```text
data/processed/universe/liquid_nse_universe_summary.json
data/processed/features/daily_v1_ohlcv_technical_summary.json
data/processed/targets/daily_v1_forward_returns_summary.json
data/processed/research/factors/daily_v1_factor_research_summary.json
```

Factor research inputs:

```text
data/processed/research/factors/daily_v1_factor_ic.csv
data/processed/research/factors/daily_v1_factor_quantiles.csv
data/processed/research/factors/daily_v1_factor_hit_rates.csv
data/processed/research/factors/daily_v1_factor_monthly_stability.csv
data/processed/research/factors/daily_v1_factor_research_summary.json
```

Other useful artifacts:

```text
data/processed/instruments/upstox_instruments_audit.csv
data/processed/universe/liquid_nse_upstox_mapping.csv
data/processed/universe/liquid_nse_upstox_unmatched.csv
data/processed/equities/nse_daily_ohlcv_upstox_audit.csv
```

## Non-Goals For V1

Do not add these in the first implementation:

```text
pipeline rerun buttons
database write actions
model training controls
backtest execution controls
broker or order controls
live market data screens
authentication redesign
cloud deployment changes
```

The first UI should be safe, local, and read-only.

## Information Architecture

### Page 1: Research Progress

Route:

```text
/research/progress
```

Purpose:

```text
Show where the project stands from universe selection through factor research.
```

Pipeline steps:

```text
Step 0   Liquid NSE universe
Step 1.0 Upstox instrument master
Step 1.1 Liquid universe to Upstox mapping
Step 1.2 Daily OHLCV
Step 2.0 Daily technical features
Step 2.1 Daily forward-return targets
Step 2.2 Factor research outputs
Next     Signals, backtests, experiment tracking
```

Each step should show:

```text
status
row_count
symbol_count
date_min
date_max
warning_count
failed_count
artifact_paths
timescale_tables
command
notes
last_generated_at
```

Suggested layout:

```text
top summary strip
pipeline timeline
step cards
artifact table
warnings and caveats panel
next decision panel
```

Example step summary:

```text
Step 2.1 Daily Forward Targets

Status: Done
Rows: 126,703
Symbols: 261
Date range: 2024-06-18 to 2026-06-25
Warnings: expected horizon-end nulls on latest dates
Failed rows: 0
Timescale tables: targets_daily, target_runs, target_audits
Command: trade-research build-daily-targets --store-db
```

### Page 2: Factor Research

Route:

```text
/research/factors
```

Purpose:

```text
Show which features appear useful, noisy, or unstable.
```

Tabs:

```text
Overview
IC / Rank IC
Quantiles
Hit Rates
Monthly Stability
Feature Detail
```

Core questions:

```text
Which features have the best mean rank IC?
Which features have stable monthly rank IC?
Which features show useful top-vs-bottom quantile spread?
Which features improve top-forward-return hit rate?
Which features look noisy or unstable?
```

Suggested visuals:

```text
top features by mean rank IC
top features by rank IC t-stat
feature x target heatmap
top-vs-bottom quantile spread chart
hit rate by feature quantile
monthly rank IC stability line chart
feature detail drilldown
```

## Backend API Status And Plan

Read-only FastAPI endpoints live under `/api/research`. The implemented routes
are currently registered in `src/trade_research/api/app.py`.

### Progress Endpoint

```text
GET /api/research/progress
```

Returns:

```json
{
  "generated_at": "2026-06-23T00:00:00Z",
  "overall_status": "done",
  "steps": [
    {
      "step_id": "step_2_1_targets",
      "title": "Daily Forward Targets",
      "status": "done",
      "row_count": 125498,
      "symbol_count": 261,
      "date_min": "2024-06-25",
      "date_max": "2026-06-25",
      "warning_count": 35496,
      "failed_count": 0,
      "command": "trade-research build-daily-targets --store-db",
      "timescale_tables": ["targets_daily", "target_runs", "target_audits"],
      "artifacts": [
        "data/processed/targets/daily_v1_forward_returns.parquet",
        "data/processed/targets/daily_v1_forward_returns_audit.csv",
        "data/processed/targets/daily_v1_forward_returns_summary.json"
      ],
      "notes": ["Warnings are expected near the latest dates because future windows are incomplete."]
    }
  ]
}
```

### Factor Summary Endpoint

```text
GET /api/research/factors/summary
```

Returns the factor research summary JSON plus derived status fields.

### Factor IC Endpoint

```text
GET /api/research/factors/ic
```

Query parameters:

```text
target=forward_ret_20d
sort=mean_rank_ic
direction=desc
limit=50
```

Returns rows from:

```text
daily_v1_factor_ic.csv
```

### Factor Quantiles Endpoint

Status: planned, not implemented.

```text
GET /api/research/factors/quantiles
```

Query parameters:

```text
feature=ret_60d
target=forward_ret_20d
```

Returns rows from:

```text
daily_v1_factor_quantiles.csv
```

### Factor Hit Rates Endpoint

Status: planned, not implemented.

```text
GET /api/research/factors/hit-rates
```

Returns rows from:

```text
daily_v1_factor_hit_rates.csv
```

### Monthly Stability Endpoint

Status: planned, not implemented.

```text
GET /api/research/factors/monthly-stability
```

Query parameters:

```text
feature=ret_60d
target=forward_ret_20d
```

Returns rows from:

```text
daily_v1_factor_monthly_stability.csv
```

### Feature Detail Endpoint

Status: planned, not implemented.

```text
GET /api/research/factors/features/{feature_name}
```

Returns one combined view:

```text
IC and rank IC by target horizon
quantile rows for selected feature
hit-rate rows for selected feature
monthly stability rows for selected feature
derived interpretation metrics
```

## Backend Implementation Notes

Original recommended package location:

```text
src/trade_research/api/research.py
```

Current implementation location:

```text
src/trade_research/api/app.py
```

Implemented helper module:

```text
src/trade_research/research/artifacts.py
```

Responsibilities:

```text
read JSON summary files
read CSV factor files
normalize missing files into clear empty states
sort/filter/paginate rows for API responses
derive simple display metrics
```

Error behavior:

```text
missing artifact -> return status "missing" with expected path
malformed artifact -> return status "error" with parse message
empty artifact -> return status "empty"
```

The API should not recompute features, targets, or factor research.

## Frontend Implementation Plan

Use the existing React app under:

```text
apps/web/
```

The UI should be dense, calm, and research-focused. Avoid a marketing-style
landing page. The first screen should be the actual dashboard.

### Research Navigation

Add navigation entries:

```text
Progress
Factors
```

Possible routes:

```text
/research/progress
/research/factors
```

### Progress Page Components

Suggested components:

```text
PipelineTimeline
PipelineStepCard
ArtifactTable
PipelineCaveats
NextDecisionPanel
```

States:

```text
loading
done
warning
missing artifacts
error
```

### Factor Page Components

Suggested components:

```text
FactorOverview
FactorRankingTable
FactorHeatmap
FactorQuantileChart
FactorHitRateChart
MonthlyStabilityChart
FeatureDetailDrawer
```

Filters:

```text
target horizon
feature family
minimum dates
sort metric
top N
```

Feature families can be inferred by name:

```text
ret_*, log_ret_*                 -> momentum
sma_*, ema_*, close_vs_*         -> trend
volatility_*, atr_*, true_range  -> risk
volume_*, turnover_*             -> liquidity
```

## Factor Interpretation Rules

The UI should not claim that a feature is profitable. It should describe
evidence carefully.

Suggested labels:

```text
Promising: positive mean rank IC and stable monthly behavior
Mixed: signal exists but is unstable or target-specific
Weak: near-zero IC and weak quantile spread
Noisy: large month-to-month sign changes
Needs review: few observations or missing data
```

Useful derived metrics:

```text
top_bottom_spread = top quantile mean target - bottom quantile mean target
rank_ic_stability = percentage of months with positive rank IC
hit_rate_lift = top quantile hit rate - bottom quantile hit rate
```

## Milestones

### Milestone 1: Read-Only Backend

Build:

```text
GET /api/research/progress
GET /api/research/factors/summary
GET /api/research/factors/ic
```

Acceptance:

```text
returns JSON from existing generated artifacts
handles missing files cleanly
unit tests cover summary parsing and missing-file behavior
```

### Milestone 2: Progress UI

Build:

```text
/research/progress
pipeline timeline
step cards
artifact table
```

Acceptance:

```text
shows all completed steps
shows row counts, symbols, date ranges, warnings, failed rows
shows commands and artifact paths
has clear missing-artifact state
```

### Milestone 3: Factor Overview UI

Build:

```text
/research/factors
summary tiles
ranking table
target horizon filter
top features and weakest features panels
```

Acceptance:

```text
can sort by mean rank IC, rank IC t-stat, positive rank IC percentage
can filter by target horizon
shows enough context to choose features for deeper review
```

### Milestone 4: Charts And Drilldown

Build:

```text
feature x target heatmap
quantile chart
hit-rate chart
monthly stability chart
feature detail drawer/page
```

Acceptance:

```text
clicking a feature shows IC, quantiles, hit rates, and monthly stability
charts render without overlap on desktop and mobile widths
```

### Milestone 5: Decision Notes

Add a lightweight local notes artifact later:

```text
data/processed/research/factors/factor_review_notes.json
```

Possible fields:

```text
feature
target
decision
reason
reviewed_at
```

This should come after the read-only UI is useful.

## Recommended First Build Slice

Start with:

```text
Backend:
GET /api/research/progress
GET /api/research/factors/summary
GET /api/research/factors/ic

Frontend:
/research/progress
/research/factors with summary tiles and sortable IC table
```

Do not build charts first. Tables and clear status cards will make the system
usable faster and will expose any data-contract gaps before visual work begins.

## First Build Slice Status

Implemented:

```text
GET /api/research/progress
GET /api/research/factors/summary
GET /api/research/factors/ic

/research/progress
/research/factors
```

The first implementation is read-only and file-backed. It does not recompute
features, targets, factor research outputs, or Dagster runs. It also does not
verify live Timescale row counts.

Current UI behavior:

```text
/research/progress shows pipeline step cards and an artifact table.
/research/factors shows summary tiles, target filtering, sorting, and IC tables.
```

Current verification:

```text
backend tests cover artifact parsing, missing artifacts, API response shape,
and IC sorting/filtering.
frontend build and lint pass.
```

Not implemented yet:

```text
GET /api/research/factors/quantiles
GET /api/research/factors/hit-rates
GET /api/research/factors/monthly-stability
GET /api/research/factors/features/{feature_name}

quantile charts
hit-rate views
monthly stability charts
feature-detail drilldown
local decision notes artifact
```

## Open Decisions

For future extensions, confirm:

```text
Should the file-backed UI also verify Timescale row counts?
Should factor outputs be grouped by feature family in the API or only in UI?
Should we use Plotly for charts immediately, or first build tables?
```

Recommended defaults:

```text
file-backed first, with optional Timescale verification later
feature-family grouping in API and UI
tables first, charts second
```
