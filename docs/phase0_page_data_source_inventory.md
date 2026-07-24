---
document_status: current
last_verified_commit: afbc5dc1f78803752d013a6db99a76293d01d61e
last_verified_date: 2026-07-24
owner: trade-research-platform
replaced_by: null
---

# Phase 0 UI Page and Data-Source Inventory

This document identifies what each current page really reads and how it behaves
when its source is missing.

## Failure-mode vocabulary

- **Strict:** an API error reaches the UI.
- **Empty fallback:** the client substitutes an empty/missing response.
- **Synthetic fallback:** backend or frontend substitutes fabricated sample
  values.
- **Artifact-backed:** the API reads generated files from the production
  `DATA_DIR`.
- **Database-backed:** the API queries PostgreSQL/TimescaleDB.

## Authenticated production observations

The following values were observed read-only at `https://trade.chain8.org` on
24 July 2026 between 11:35 and 11:56 IST. They are UI/API evidence, not direct
SQL or Dagster-instance evidence. The important claims were subsequently
cross-checked against PostgreSQL and the Dagster instance on the production
host between 12:07 and 12:15 IST.

| Surface | Observation | Assessment |
|---|---|---|
| Dashboard | Combined universe 8,885, quality 0.0%, all 8,885 stale; OHLCV timestamps are June or epoch | Incorrect relative to the Data Console |
| Data / NSE | 2,387 active; 4,291,594 candles; 2,405 symbols; latest 23 Jul; 22 open; 19 retrying | Current data with unresolved failures |
| Data / TSX | 885 active; 1,341,900 candles; 645 symbols; latest 23 Jul; 602 open | Current data with large backlog |
| Data / US | 5,613 active; 10,357,583 candles; 5,612 symbols; latest 23 Jul; 5,336 open; 9 retrying | Current data with very large backlog |
| Runs / NSE | 50 rows; 4,402 items; 68.0% success; 16 failed/partial runs | Active, not healthy |
| Runs / TSX | 50 rows; 529 items; 92.0% success; 4 failed/partial runs | Best of the three, still not perfect |
| Runs / US | 50 rows; 4,117 items; 74.0% success; 13 failed/partial runs | Active, not healthy |
| Opportunities / NSE | Latest 23 Jul; 2,378 rows; 99.0% displayed coverage | Current and above 95% gate |
| Opportunities / TSX | Latest 17 Jul; 644 rows; 100% displayed coverage for that old session | Complete session, stale product |
| Opportunities / US | Latest 17 Jul; 5,408 rows; 96.4% displayed coverage; 3 incomplete previous-close inputs | Above gate but stale and imperfect |
| Progress | 15 steps, 3 complete, 12 missing; Upstox OHLCV/features/targets through 22 Jul | Artifact chain is partial and provider-mixed |
| Factors | 0 joined rows, 0 features, 0 IC rows | Required artifacts absent |
| Models | ML dataset not found, 0 models, no backtests | Required artifacts absent |
| Settings | Upstox credential configured in database and valid | Upstox remains an active production capability |

The Data Overview initially showed a transient loading state during navigation,
then resolved. Opportunities exchange changes required several seconds,
especially US. Slow loading must not be confused with empty data, but the UI
should expose an explicit timeout/error state in later phases.

## Page summary

| Page | Main endpoints | Source | Failure mode | Trust assessment |
|---|---|---|---|---|
| Dashboard | `/api/market/status`, `/api/opportunities/daily` | PostgreSQL plus backend/client fallback | Market status synthetic; Opportunities strict | Mixed |
| Data | `/api/data/*` | PostgreSQL and settings | Mostly strict/503; schedule state is configured intent | Operationally useful, schedule label not authoritative |
| Opportunities | `/api/opportunities/daily` | PostgreSQL `opportunity_targets_daily` path | Strict | Strongest current research page |
| Research | `/api/chat/*`, `/api/research/notes` | Chat services plus hard-coded notes | Mixed | Notes are not production evidence |
| Research Progress | `/api/research/progress` | Local artifacts | Empty/artifact status | Not durable truth |
| Factors | `/api/research/factors/*` | Local JSON/CSV | Empty/artifact status | Not durable truth |
| Models | `/api/research/ml/*` | Local JSON/Parquet | Empty/artifact status | Not durable truth |
| Jobs | `/api/jobs/latest` | PostgreSQL runs | Backend and client synthetic fallback | Can be misleading |
| Provider Settings | `/api/admin/provider-credentials/upstox/*` | PostgreSQL encrypted credential plus env fallback | Strict | Real admin mutation surface |
| Screeners | `/api/screeners/intraday-range/latest` | Hard-coded backend rows | Always synthetic | Demo only |
| Symbol | `/api/symbols/{ticker}/candles`, notes | PostgreSQL then synthetic candles; hard-coded notes | Synthetic | Can be misleading |

## Dashboard

### Market status

Backend behavior:

1. Query `TimescaleStore.market_status()`.
2. Return real rows if any exist.
3. On no rows or SQLAlchemy failure, return fabricated NSE/TSX values relative
   to current time.

Frontend behavior:

`getMarketStatus()` uses `fetchJson` and substitutes imported mock data on
non-2xx or network failure.

Result: the dashboard can look healthy while the database or API is
unavailable.

Production finding: the opposite failure mode is also present. The Dashboard
currently looks catastrophically stale while the Data Console shows current
July candles. The displayed 8,885 total includes NSE, TSX, and US, but its label
says “NSE + TSX tracked symbols.” Dashboard status calculations and labels are
not aligned with the yfinance operational tables.

### Opportunities summary

`getDailyOpportunities()` uses strict fetch behavior. The backend queries
PostgreSQL for yfinance Opportunity targets and does not synthesize rows.

Result: this portion fails visibly and is materially more trustworthy.

## Data page

Database-backed views:

- provider runs;
- provider request summary/logs;
- data availability;
- universe snapshots and members;
- durable work items;
- lifecycle events;
- adaptive rate state;
- BigQuery run/partition state;
- run detail;
- freshness and queue summaries.

### Schedule status limitation

`/api/data/schedules/status` derives `intended_status` from `Settings` flags. It
does not query Dagster schedule storage. The API notes this for most rows, but
the UI presents running/stopped badges that can be mistaken for actual state.

Required later change:

- expose `desired_status`;
- expose `actual_status`;
- expose `last_tick`;
- expose `last_successful_run`;
- show drift separately.

Production finding: recurring `yfinance_daily_work_queue` rows are real
database records and demonstrate active processing, including successful runs
at 11:35, 11:40, and 11:45 IST. However, 50-run success rates were 68% NSE, 92%
TSX, and 74% US. The page's “running” schedule badges cannot explain or validate
those runs without Dagster tick linkage.

Direct Dagster inspection found 12 active schedule records, not merely the
states inferred by the API. Nine active records belong to a stale repository
origin while three belong to the currently deployed origin. A normal
current-repository `dagster schedule list` therefore reports several schedules
as stopped even though the daemon is still launching their stale-origin
records. This is configuration/control drift, not an absence of worker
execution.

### Inline mutation

`POST /api/data/pipeline-requests` runs an Upstox OHLCV request synchronously in
the API process and persists results. This conflicts with the selected
Dagster-only production target.

## Opportunities page

The page calls `/api/opportunities/daily` with:

- exchange;
- optional session date;
- symbol;
- sort and direction;
- pagination;
- percentile filters.

Backend rules:

- source defaults to yfinance;
- any non-yfinance source is rejected;
- results come from PostgreSQL;
- session completeness uses
  `OPPORTUNITY_MINIMUM_SESSION_COVERAGE`;
- explicit session and automatic latest-complete-session resolution are
  separated;
- distributions and percentile metadata are calculated server-side.

Frontend request behavior is strict and does not retain data across a different
exchange/session.

Direct PostgreSQL verification confirmed that the application contract is
fail-closed and that its selected-session row counts match persisted target
rows.

Production finding:

- NSE targets are current through 23 July with 2,378 rows and 99.0% displayed
  coverage against the current accepted universe.
- TSX targets stop at 17 July even though yfinance candles are current through
  23 July.
- US targets stop at 17 July even though yfinance candles are current through
  23 July; 3 rows lack previous-close inputs.

Session completeness and product freshness are different properties. A session
can correctly show 100% coverage and still be seven days stale.

The orchestration cause is also verified: the completed-session Opportunity
schedule exists only for NSE and is active. The direct North America schedule
that includes TSX/US targets is stopped, while the active durable worker writes
candles but does not materialize those targets.

For the current NSE session, 2,378 target rows are compared with 2,387 accepted
universe members. The nine absent symbols are:

```text
CMLL.NS, CPL.NS, EUROTEXIND.NS, KALYANI.NS, NIRAJISPAT.NS,
PKTEA.NS, SAYAJIHOTL.NS, SEMAC.NS, THACKER.NS
```

These are not a chart/query defect: direct inspection found provider lag or a
missing expected 23 July bar for all nine. Four affected symbols additionally
hit a duplicate-key cardinality error in one PostgreSQL upsert batch.

## Research page

### Chat

The chat route uses the configured chat orchestrator, tools, quality policy,
citations, and optional LLM answer rewriting. Its completeness depends on the
underlying operational and retrieval data.

### Research notes

`/api/research/notes` always returns three hard-coded notes with timestamps
computed relative to request time. These must never be presented as retrieved
production research evidence.

## Research Progress page

`/api/research/progress` constructs `ResearchArtifactReader(DATA_DIR)` and
checks generated files including:

- universe summaries;
- instrument/mapping audits;
- OHLCV artifacts;
- feature/target summaries;
- processed validation;
- ML dataset;
- walk-forward folds;
- model metrics;
- backtests;
- latest predictions;
- factor research.

The endpoint does not verify current PostgreSQL row counts or Dagster asset
materializations. A mounted stale file can appear complete.

Production finding: 12 of 15 steps are missing. The displayed Upstox research
chain contains 261 symbols and 131,520 OHLCV rows through 22 July, with features
and targets present but downstream validation, ML, factor, and model outputs
missing. The page still advertises CLI commands as the way to create artifacts.

The latest Dagster daily-research run failed at `ml_dataset_v1` because
`/app/data/processed/validation/daily_pipeline_stock_coverage.parquet` does not
exist. Earlier assets in the same failed run did materialize, explaining why
the page shows a partial chain rather than an all-or-nothing result.

## Factors page

Endpoints:

- `/api/research/factors/summary`;
- `/api/research/factors/ic`.

Sources:

- `daily_v1_factor_research_summary.json`;
- `daily_v1_factor_ic.csv`.

Other factor outputs described in historical plans are not fully exposed by
current APIs. The page is a table-oriented artifact viewer.

Target state: ClickHouse-backed distributions, IC, quantiles, stability,
quality, and feature drilldown with version/run lineage.

Production finding: all factor counts are zero and no IC rows are available.
This is downstream of the failed artifact chain, not evidence that the Factors
API is querying a successfully materialized empty dataset.

## Models page

All `/api/research/ml/*` routes construct `MLArtifactReader(DATA_DIR)`.

Sources include:

- ML dataset summary;
- walk-forward summary;
- baseline metrics and predictions;
- LightGBM metrics and predictions;
- backtest metrics and series;
- latest prediction artifacts.

Some API methods derive equity/robustness views in process from local files.
There is no transactional experiment registry, immutable dataset registry, or
model registry.

Target state:

- PostgreSQL workflow/dataset/model/experiment registry;
- ClickHouse predictions, metrics, and backtest series;
- object-storage dataset/model manifests;
- Dagster run linkage.

Production finding: the ML dataset is absent, model count is zero, and no
prediction, equity, backtest, or robustness artifacts are available.
The direct Dagster failure above explains why the expected dataset and
downstream model products were never produced.

## Jobs page

Backend behavior:

1. Query latest PostgreSQL ingestion/provider runs.
2. Return real rows if present.
3. On empty rows or database error, return fabricated completed/running jobs.

Frontend behavior:

`getJobRuns()` also substitutes imported mock jobs on request failure.

Result: this page is not a trustworthy operational monitor until fallbacks are
removed and Dagster run state is included.

Direct inspection also found a semantic mismatch: all 288 Dagster worker runs
in the preceding 24 hours were `SUCCESS`, while PostgreSQL recorded 40 yfinance
business runs completed with failures and 155 failed work items. The Jobs page
must represent orchestration outcome and business outcome separately.

## Provider Settings page

This is an authenticated admin surface for Upstox:

- status checks encrypted PostgreSQL credential state and environment fallback;
- test can call the provider;
- save persists an encrypted token.

This is a real mutation surface. It should be retained only during the Upstox
comparison/retirement period and must remain explicitly authorized/audited.

## Screeners page

`/api/screeners/intraday-range/latest` returns hard-coded NSE/TSX rows. The page
is demo-only and has no production screener dataset.

## Symbol page

### Candles

Backend:

1. Query PostgreSQL candles.
2. If empty/error, generate 70 synthetic daily candles.

Frontend:

substitutes imported mock candles on request failure.

### Notes

Uses the hard-coded research-notes route.

Result: the page must not be used to validate production market data.

## Frontend request policy

The shared `fetchJson` helper converts any non-2xx response or network failure
into a supplied fallback. It is used by most pages. `strictFetchJson` is used
for Opportunities and mutation-sensitive operations.

Target production policy:

- no synthetic data outside an explicit demo/test mode;
- stale, empty, unavailable, partial, and unauthorized states are distinct;
- every response includes source, version, materialization time, and freshness;
- operational schedule state comes from Dagster, not settings intent.
