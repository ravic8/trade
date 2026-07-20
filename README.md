# Trade Research Agent

Local-first market data and research infrastructure for building a systematic
trade research agent focused on Indian equities.

The project is currently a local-first data foundation plus an initial factor
research layer. It builds a clean tradable NSE equity universe, maps symbols to
provider instruments, ingests audited OHLCV data, stores canonical datasets
locally, builds deterministic daily technical features, builds forward-return
target labels, and produces first-pass factor research outputs. Backtesting,
full model experiments, and a mature Lens research agent sit after this audited
research-data foundation.

## Project Vision

The long-term goal is a systematic trade research platform that helps answer
evidence-based questions such as:

- Which liquid NSE stocks are suitable for approximately Rs 1 lakh per-stock
  trades?
- Which technical, liquidity, volatility, and regime features have predictive
  value?
- Which setups can realistically target roughly 1% per trade after costs,
  slippage, and risk controls?
- Which signals are stable out-of-sample and worth paper trading?
- How should a research agent explain candidates, risks, historical evidence,
  and data-quality caveats?

The system is intentionally not a live trading bot yet. The near-term objective
is truthful data, reproducible features, auditable research datasets, and
validation-ready evidence before backtests, models, or paper trading.

## Current Scope

Current focus:

- NSE listed equities.
- Liquid cash-market universe selection using average daily turnover, volume,
  trading consistency, and zero-volume checks.
- Upstox instrument master ingestion.
- Mapping the liquid NSE universe to Upstox instrument keys.
- Batch daily OHLCV ingestion for mapped NSE equities.
- TimescaleDB/PostgreSQL storage for structured market data.
- Parquet and CSV outputs for local analytical workflows.
- Data-quality audits for every major generated dataset.
- Frozen daily technical feature layer v1.0.
- Daily forward-return target layer v1.0.
- Factor research outputs: IC/rank IC, quantile returns, hit rates, and monthly
  stability.
- Run-scoped Upstox daily fetch coverage for retry planning.
- An artifact-backed FastAPI + React research UI for pipeline progress and
  factor IC review.
- A guarded Lens chat foundation with typed market-data tools, citation
  provenance, and optional Gemini answer rewriting.
- Early modeling/target utilities under `src/trade_research/modeling/` and
  exploratory material under `experiments/`.

## Non-Goals For Now

These are deliberately out of scope until the data, feature, and research
layers are trustworthy enough to support validated backtests:

- Realtime streaming, websockets, or live candle appending.
- Broker order placement or automated execution.
- Intraday/minute-level production pipelines for NSE equities.
- Indices and F&O production coverage.
- Fundamentals ingestion.
- Sentiment or alternative data ingestion.
- Autonomous trading agents.
- Cloud/server deployment as the primary operating mode.

## Architecture Overview

The current architecture is a local modular monolith:

```text
Universe selection
    -> provider instrument master
    -> symbol/instrument mapping
    -> batch OHLCV ingestion
    -> data-quality audits
    -> TimescaleDB + Parquet/CSV
    -> daily technical features
    -> forward-return targets
    -> factor research outputs
    -> signal/backtest/model layers later
    -> Lens research agent layer in progress
```

Core components:

- **Python package**: reusable data, storage, feature, target, factor research,
  chat, and modeling code under `src/trade_research/`.
- **Typer CLI**: repeatable local batch commands exposed as `trade-research`.
- **TimescaleDB/PostgreSQL**: canonical structured market-data store.
- **Parquet/CSV**: local analytical outputs and reproducible batch artifacts.
- **Dagster**: asset job and stopped-by-default schedule for the Upstox daily
  research pipeline. The local Python environment must include Dagster for this
  runtime to be active.
- **FastAPI + React**: application shell for dashboard, screeners, Lens chat,
  research progress, and factor IC views. Some non-research endpoints and
  frontend calls intentionally fall back to mock data when live API/DB data is
  unavailable.
- **Qdrant**: vector store helpers for document retrieval experiments. The
  storage/search wrapper exists; a full document-ingestion job or CLI is not
  implemented yet.
- **Local runtime**: Docker Compose starts the full local stack: API, web,
  Dagster, TimescaleDB/PostgreSQL, Redis, Qdrant, and CloudBeaver.

Step 2 feature-layer design docs:

- [Feature Field Guide v1](docs/feature_field_guide_v1.md): concepts,
  formulas, visual sketches, examples, traps, and research usage for each
  feature family.
- [Feature Layer v1 Spec](docs/feature_layer_v1_spec.md): implementation
  contract for the first feature-layer development batches.
- [Handoff Summary](docs/handoff_summary.md): concise current-state starter for
  a fresh Codex chat.
- [Daily Pipeline Handoff](docs/daily_pipeline_handoff.md): detailed daily
  pipeline state, latest verified run ids, and retry/coverage details.
- [ML Dataset v1 Strategy](docs/ml_dataset_v1_strategy.md): frozen contract for
  the first leakage-aware next-day-return dataset, daily walk-forward
  evaluation, and reporting plan.
- [Research UI Implementation Plan](docs/research_ui_plan.md): read-only local
  research console plan for pipeline progress and factor research review.
- [Deployment Speed and Build Caching](docs/deployment_speed.md): implemented
  BuildKit cache design, production measurements, and the future GHCR path.

## Folder Structure

```text
apps/web/                    React frontend
src/trade_research/          Main Python package
src/trade_research/data/     Market-data providers and audits
src/trade_research/storage/  Timescale, Parquet, and vector storage helpers
src/trade_research/universe/ Exchange universe providers
src/trade_research/features/ Feature builders
src/trade_research/targets/  Target/label builders
src/trade_research/research/ Factor research and retrieval utilities
src/trade_research/modeling/ Modeling utilities
src/trade_research/dagster/  Dagster daily assets, schedule, resources
src/trade_research/chat/     Lens chat orchestration and tools
scripts/                     Standalone local scripts
experiments/                 Exploratory research and model experiments
notebooks/                   Legacy/exploratory notebooks
docs/                        Architecture and design notes
data/                        Generated local datasets, gitignored
artifacts/                   Generated model/research artifacts, gitignored
output/                      Generated report/PDF outputs, gitignored
tmp/                         Temporary render/debug outputs, gitignored
```

Production-quality reusable logic should move into `src/trade_research/`.
Notebooks and `experiments/` are for exploration and should call package code
instead of becoming the only source of logic.

## Current Implementation Status

Fully implemented in the current repo:

- Step 0 liquid NSE universe selection.
- Step 1 Upstox instrument master, liquid-universe mapping, and daily OHLCV
  ingestion.
- Step 2.0 daily technical feature layer v1.0.
- Step 2.1 daily forward-return target layer v1.0.
- Step 2.2 factor research outputs.
- Raw-to-processed Upstox validation.
- Reusable processed-dataset validation and cleaned OHLCV generation.
- Daily pipeline health checks through live Upstox fetch, downstream rebuilds,
  alignment checks, factor research, and stock-level coverage.
- SQLAlchemy/Timescale table definitions and storage helpers for daily OHLCV,
  features, targets, audits, run-scoped stock coverage, and run-scoped fetch
  coverage.
- Dagster daily research asset job for incremental Upstox OHLCV ingestion,
  Timescale-backed incremental feature/target computation, factor research,
  validation, and run-scoped coverage.
- Artifact-backed research progress and factor IC endpoints.

Partially implemented:

- Lens chat. The API, policy, tool gateway, provenance objects, rate limit, and
  optional Gemini answer rewriting exist, but planning is deterministic rather
  than an LLM planner and feedback is accepted without persistence.
- React UI. The dashboard, screeners, research chat, progress, factors, jobs,
  and symbol routes exist, but several pages use mock fallback data when API or
  database data is unavailable.
- Qdrant retrieval. Vector storage, embedding, and search helpers exist, but no
  end-to-end document ingestion command or scheduled job is present.
- Local app polish. The repo is intentionally local-first; deployment packaging,
  proxy config, and backup/restore scripts have been removed.

Stubbed or mock-backed:

- The latest intraday screener API currently returns hardcoded rows.
- Research notes currently return hardcoded rows.
- Market status, symbol candles, and job runs query Timescale first, then return
  generated fallback data if no database rows are available.

Planned but not implemented as complete systems:

- Signal generation.
- Backtesting.
- Model training and experiment tracking.
- Paper trading.
- Broker execution.

## Completed Pipeline Progress

The counts below are local artifact snapshots from the generated files under
`data/processed/`. They are useful for reproducing the current research state,
but a fresh clone must rerun the pipeline commands or receive the generated
artifacts separately.

### Step 0: Liquid NSE Universe

Script:

```bash
python3 scripts/select_liquid_nse_universe.py \
  --min-avg-daily-turnover 1000000000 \
  --top-n 1000
```

Purpose:

- Fetch NSE equity universe.
- Use the configured local liquidity artifact as the tradable universe input.
- Compute liquidity using average daily turnover (`close * volume`), average
  daily volume, trading-day coverage, and zero-volume ratio.
- Select the core tradable universe.

Current result:

- NSE universe size: 2374.
- Tickers with data: 2372.
- Core liquid stocks with six-month ADT >= Rs 100 crore/day: 264.
- Duplicate ticker/date rows: 0.

### Step 1.0: Upstox Instrument Master

Command:

```bash
trade-research fetch-upstox-instruments
```

Purpose:

- Fetch the full public Upstox instrument master.
- Store the master locally.
- Audit missing and duplicate instrument keys.
- Optionally upsert instruments into TimescaleDB.

Current result:

- Total Upstox instruments: 140,865.
- NSE equity instruments: 2,424.
- Missing instrument keys: 0.
- Duplicate instrument keys: 0.

### Step 1.1: Liquid Universe To Upstox Mapping

Command:

```bash
trade-research map-liquid-nse-upstox
```

Purpose:

- Join the liquid NSE universe to Upstox NSE equity instruments.
- Persist matched instruments for OHLCV fetching.
- Persist unmatched symbols for manual review.
- Optionally upsert the tradable universe and members into TimescaleDB.

Current result:

- Liquid universe rows: 264.
- Upstox mapped rows: 261.
- Unmatched symbols: `STLTECH`, `KRN`, `PFOCUS`.

### Step 1.2: Upstox Daily OHLCV

Command:

```bash
trade-research fetch-upstox-nse-daily
```

Purpose:

- Fetch daily OHLCV from Upstox for mapped liquid NSE equities.
- Default to incremental fetching from the latest stored date plus one day.
- Use a two-calendar-day settlement lag when `--to-date` is omitted.
- Write audit, failure, and skipped/current-symbol reports.
- Upsert daily candles and data-quality audits into TimescaleDB.

Current Timescale-backed result, as of the latest Dagster daily research run:

- Daily OHLCV rows: 126,704.
- Symbols: 261.
- Date range: 2024-06-18 to 2026-06-25.
- Fetch failures: 0.
- One negative-volume row remains in the raw processed file and is excluded
  from cleaned OHLCV before features, targets, or ML preparation.

### Step 1.3: Raw And Processed Dataset Validation

Commands:

```bash
python scripts/validate_upstox_raw_to_processed.py
trade-research validate-processed-datasets
trade-research validate-daily-pipeline-health --run-live-fetch
```

Purpose:

- Validate the raw-to-processed Upstox conversion path and processed OHLCV
  quality.
- Create a cleaned OHLCV file by excluding invalid processed rows.
- Validate feature and target alignment against cleaned OHLCV.
- Resolve the latest expected NSE trading date using market-calendar logic.
- Run a full daily pipeline health check: Upstox fetch, validation, features,
  targets, factor research, and per-stock coverage.

Current result:

- Latest expected trading date: 2026-06-25.
- Live Upstox fetch reaches latest expected trading date: yes.
- Fetch failures: 0.
- Raw Upstox API payloads are not persisted, so full raw replay validation is
  unavailable.
- Processed OHLCV rows: 126,704.
- Cleaned OHLCV rows: 126,703.
- Invalid processed OHLCV rows: 1 (`IDEA`, negative volume on 2024-08-30).
- Invalid cleaned OHLCV rows: 0.
- Duplicate instrument/date keys: 0.
- Baseline ML readiness: true, with warnings.
- Full-history stock coverage: 243 pass / 7 warn / 11 fail across 261 fetched
  stocks.
- Rolling-window coverage is generated for 6, 9, 12, 15, 18, and 24 months and
  stored per Dagster run in TimescaleDB.

### Step 2.0: Daily Technical Feature Layer v1.0

Command:

```bash
trade-research build-daily-features
```

Store in TimescaleDB:

```bash
trade-research build-daily-features --store-db
```

Purpose:

- Build deterministic no-leakage daily technical features from canonical daily
  OHLCV.
- Keep every feature limited to information available on or before date `T`.
- Write Parquet output, feature audit CSV, and summary JSON.
- Store feature rows, run metadata, and audit metadata in TimescaleDB when
  `--store-db` is passed.

Feature version:

```text
daily_v1_ohlcv_technical_v1_0
```

Feature families:

- base OHLCV
- returns/momentum
- moving averages and EMA trend features
- price-vs-trend features
- moving-average relationship features
- volatility and ATR features
- volume and turnover features

Current result:

- Feature rows: 126,703.
- Symbols: 261.
- Date range: 2024-06-18 to 2026-06-25.
- Invalid OHLCV rows excluded and recorded in summary: 1.
- Duplicate feature keys: 0.
- Inf values: 0.
- Failed rows: 0.

### Step 2.1: Daily Forward-Return Targets v1.0

Command:

```bash
trade-research build-daily-targets
```

Store in TimescaleDB:

```bash
trade-research build-daily-targets --store-db
```

Purpose:

- Build target labels separately from features to avoid leakage.
- Measure future outcomes after each date `T`.
- Write target Parquet output, target audit CSV, and summary JSON.
- Store target rows, run metadata, and audit metadata in TimescaleDB when
  `--store-db` is passed.

Target version:

```text
daily_v1_forward_returns_v1_0
```

Target columns:

- `forward_ret_1d`
- `forward_ret_5d`
- `forward_ret_10d`
- `forward_ret_20d`
- `forward_ret_60d`
- `forward_outperform_universe_20d`
- `top_quantile_forward_return_20d`

Current result:

- Target rows: 126,703.
- Symbols: 261.
- Date range: 2024-06-18 to 2026-06-25.
- Invalid OHLCV rows excluded and recorded in summary: 1.
- Duplicate target keys: 0.
- Inf values: 0.
- Warning rows include expected future-horizon nulls near the latest dates.
- Failed rows: 0.

### Step 2.2: Factor Research Outputs v1.0

Command:

```bash
trade-research build-factor-research
```

Purpose:

- Join features at date `T` with targets after date `T`.
- Measure whether individual features have relationship to future returns.
- Produce first-pass factor research tables for review before signals,
  backtests, or models.

Current result:

- Joined rows: 126,703.
- Symbols: 261.
- Features analyzed: 43.
- Return targets analyzed: 5.
- IC rows: 215.
- Quantile rows: 1,290.
- Hit-rate rows: 215.
- Monthly stability rows: 4,608.

## Data Outputs

Canonical current outputs:

```text
data/processed/universe/liquid_nse_stocks.csv
data/processed/universe/liquid_nse_stock_audit.csv
data/processed/universe/liquid_nse_universe_summary.json

data/processed/instruments/upstox_instruments.parquet
data/processed/instruments/upstox_instruments_audit.csv

data/processed/universe/liquid_nse_upstox_mapping.csv
data/processed/universe/liquid_nse_upstox_unmatched.csv

data/processed/equities/nse_daily_ohlcv_upstox.parquet
data/processed/equities/nse_daily_ohlcv_upstox_audit.csv
data/processed/equities/nse_daily_ohlcv_upstox_failures.csv
data/processed/equities/nse_daily_ohlcv_upstox_skipped.csv

data/processed/validated/ohlcv_daily_validated.parquet
data/processed/validated/ohlcv_daily_validated_metadata.json

data/processed/features/daily_v1_ohlcv_technical.parquet
data/processed/features/daily_v1_ohlcv_technical_audit.csv
data/processed/features/daily_v1_ohlcv_technical_summary.json

data/processed/targets/daily_v1_forward_returns.parquet
data/processed/targets/daily_v1_forward_returns_audit.csv
data/processed/targets/daily_v1_forward_returns_summary.json

data/processed/research/factors/daily_v1_factor_ic.csv
data/processed/research/factors/daily_v1_factor_quantiles.csv
data/processed/research/factors/daily_v1_factor_hit_rates.csv
data/processed/research/factors/daily_v1_factor_monthly_stability.csv
data/processed/research/factors/daily_v1_factor_research_summary.json

data/processed/validation/raw_to_processed_validation_report.md
data/processed/validation/raw_to_processed_metadata.json
data/processed/validation/processed_ohlcv_invalid_rows.parquet
data/processed/validation/processed_dataset_validation_summary.md
data/processed/validation/processed_dataset_validation_summary.json
data/processed/validation/daily_pipeline_health_report.md
data/processed/validation/daily_pipeline_health_report.json
data/processed/validation/daily_pipeline_stock_coverage.parquet
data/processed/validation/daily_pipeline_stock_coverage_windows.parquet
```

`data/` is gitignored because these files are generated and may be large. Keep
the files locally as canonical run artifacts unless intentionally rebuilding
them.

## Database And Storage Design

TimescaleDB/PostgreSQL is the canonical structured store. Important tables:

- `symbols`: exchange symbol master for NSE/TSX universe providers.
- `provider_instruments`: full provider instrument master, currently Upstox.
- `tradable_universes`: named universe definitions and criteria.
- `tradable_universe_members`: universe membership, ranks, liquidity metrics,
  and mapped instrument keys.
- `ohlcv_daily`: daily Upstox OHLCV for mapped liquid NSE equities.
- `features_daily`: frozen daily technical feature rows keyed by
  `instrument_key + date + feature_version`.
- `feature_runs`: daily feature run metadata and summary JSON.
- `feature_audits`: per-feature null/inf audit metadata.
- `targets_daily`: daily target rows keyed by
  `instrument_key + date + target_version`.
- `target_runs`: target run metadata and summary JSON.
- `target_audits`: per-target null/inf audit metadata.
- `stock_coverage_runs`: one coverage summary row per daily Dagster run.
- `stock_coverage_by_window`: per-stock coverage rows by Dagster run and
  rolling window.
- `daily_ohlcv_fetch_coverage`: per-run fetch status for retry planning.
- `data_quality_audits`: row-level summary audit records for generated
  datasets.
- `ingestion_runs`: run history and success/failure counts.
- `exchange_holidays`: cached exchange calendars.

Hypertables:

- `ohlcv_daily` is a Timescale hypertable on `date`.
- `features_daily` is a Timescale hypertable on `date`.
- `targets_daily` is a Timescale hypertable on `date`.
- `ohlcv_hourly` is a Timescale hypertable on `ts`.

Local file storage:

- Parquet is used for reusable analytical datasets.
- CSV/JSON is used for audit reports, summaries, and manual review outputs.
- Qdrant is available for document/vector retrieval experiments, not for
  canonical OHLCV storage.
- Redis is supporting infrastructure for jobs/cache style workloads, not a
  durable market-data store.

## How To Run Locally

Create the Python environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

Start the full local stack:

```bash
docker compose up --build
```

Initialize the database schema after the stack is up:

```bash
docker compose exec api trade-research init-db
```

Run the daily Dagster job from the containerized local stack:

```bash
docker compose exec dagster-webserver \
  dagster job execute -m trade_research.dagster.definitions -j daily_research_pipeline_job
```

You can still run commands from the host venv if preferred:

```bash
trade-research init-db
.venv/bin/dagster job execute -m trade_research.dagster.definitions -j daily_research_pipeline_job
```

Local services:

```text
React web:       http://localhost:5173
FastAPI API:     http://localhost:8000
Dagster UI:      http://localhost:3000
TimescaleDB:     localhost:5432
Qdrant:          localhost:6333
CloudBeaver:     http://localhost:8978
```

Compose services:

- `api`: FastAPI backend and CLI runtime.
- `web`: Vite React research UI.
- `dagster-webserver`: Dagster UI for the daily research assets.
- `dagster-daemon`: Dagster schedule/daemon process; schedules are still
  stopped by default until explicitly enabled.
- `postgres`: TimescaleDB/PostgreSQL source of truth for structured data.
- `redis`: local cache/queue dependency for app experiments.
- `qdrant`: vector store for future document/research retrieval.
- `dbeaver`: optional browser database UI.

## Major Pipeline Commands

Select liquid NSE universe:

```bash
python3 scripts/select_liquid_nse_universe.py \
  --min-avg-daily-turnover 1000000000 \
  --top-n 1000
```

Fetch Upstox instrument master:

```bash
trade-research fetch-upstox-instruments
```

Map liquid NSE universe to Upstox:

```bash
trade-research map-liquid-nse-upstox
```

Fetch or incrementally refresh Upstox NSE daily OHLCV:

```bash
trade-research fetch-upstox-nse-daily
```

Force a full daily OHLCV refresh:

```bash
trade-research fetch-upstox-nse-daily --full-refresh
```

Smoke-test a small number of symbols:

```bash
trade-research fetch-upstox-nse-daily --limit 5
```

Retry only stocks that failed or returned no rows in the latest run-scoped
fetch coverage:

```bash
trade-research retry-upstox-nse-daily
```

Retry a specific coverage run:

```bash
trade-research retry-upstox-nse-daily \
  --coverage-run-id e361cce3-57ea-4d56-bc90-f490812444e0
```

Build Step 2 daily technical features from the canonical daily OHLCV Parquet:

```bash
trade-research build-daily-features
```

Store the frozen v1.0 daily feature set in TimescaleDB:

```bash
trade-research build-daily-features --store-db
```

Build Step 2.1 daily forward-return targets from the canonical daily OHLCV
Parquet:

```bash
trade-research build-daily-targets
```

Store the Step 2.1 target set in TimescaleDB:

```bash
trade-research build-daily-targets --store-db
```

Build factor research outputs by joining frozen daily features with Step 2.1
targets:

```bash
trade-research build-factor-research
```

Validate raw-to-processed Upstox conversion and processed OHLCV quality:

```bash
python scripts/validate_upstox_raw_to_processed.py
```

Validate processed OHLCV, cleaned OHLCV, features, targets, and alignment:

```bash
trade-research validate-processed-datasets
```

Run the end-to-end daily pipeline health check using live Upstox data:

```bash
trade-research validate-daily-pipeline-health --run-live-fetch
```

This command resolves the latest expected NSE trading date, refreshes the
canonical Upstox daily OHLCV file, rebuilds cleaned OHLCV, features, targets,
and factor research, validates feature/target/OHLCV alignment, and writes
stock-level coverage under `data/processed/validation/`.

Open the local research UI after starting the API and web app:

```text
http://localhost:5173/research/progress
http://localhost:5173/research/factors
```

## Data Quality And Audit Rules

The project is audit-driven. Every major dataset should ship with an audit or
summary that can answer:

- How many rows were requested, returned, skipped, and failed?
- What symbols/instruments were included?
- What date window was covered?
- Were there duplicate symbol/date or instrument/date rows?
- Are OHLCV values null, zero, negative, or structurally invalid?
- Are volume and turnover values plausible?
- Which symbols have warnings?
- What provider/source produced the data?
- What criteria were used to include or reject rows?

Do not use a generated dataset for research until its audit has been inspected.
Warnings are allowed, but they must be visible and explainable.

Current validation layers:

- `scripts/validate_upstox_raw_to_processed.py`: validates processed Upstox
  OHLCV quality and conversion-code assumptions. Full raw replay is unavailable
  because raw Upstox API payloads are not persisted.
- `trade-research validate-processed-datasets`: validates processed OHLCV,
  creates/refreshes `data/processed/validated/ohlcv_daily_validated.parquet`,
  checks feature/target key alignment, and writes machine-readable summaries.
- `trade-research validate-daily-pipeline-health`: checks the full daily
  pipeline through latest expected trading date, live fetch status, downstream
  rebuilds, factor research, and per-stock coverage.

Current daily health status:

- Overall status: warning.
- Baseline ML ready: true.
- Production ready: false.
- Latest expected trading date: 2026-06-25.
- Processed OHLCV reaches latest expected trading date.
- Cleaned OHLCV excludes the one invalid negative-volume row.
- Feature/target/OHLCV alignment passes with 126,703 joined keys.
- Full-history per-stock coverage warning remains: 243 pass, 7 warn, 11 fail.
- Rolling coverage is also generated for 6, 9, 12, 15, 18, and 24 months.

## Incremental Vs Full Refresh

The Upstox daily pipeline defaults to incremental mode:

- For each mapped instrument, read the latest stored `ohlcv_daily` date.
- Fetch from `latest_date + 1`.
- Skip symbols already current.
- Write skipped/current rows to
  `data/processed/equities/nse_daily_ohlcv_upstox_skipped.csv`.
- Do not overwrite the canonical full Parquet file when no new rows are fetched.

Use `--full-refresh` when you intentionally want to refetch the full requested
history. Use explicit `--from-date`/`--to-date` windows for controlled rebuilds
or backfills.

## Dagster Usage

The current daily research path is available as a Dagster asset job:

```bash
dagster job execute -m trade_research.dagster.definitions -j daily_research_pipeline_job
```

Asset order:

```text
upstox_daily_ohlcv
  -> daily_features_v1
  -> daily_targets_v1
  -> processed_dataset_validation
  -> factor_research_v1
  -> daily_pipeline_health
```

Operational behavior:

- `upstox_daily_ohlcv` resolves the latest expected completed NSE trading date,
  incrementally fetches missing Upstox daily candles, upserts them into
  TimescaleDB, writes run-scoped fetch coverage, and exports the canonical
  Parquet snapshot.
- `retry-upstox-nse-daily` reads `daily_ohlcv_fetch_coverage` and retries only
  `failed`/`no_rows` windows from the latest or specified source run.
- `daily_features_v1` incrementally computes newly affected feature rows using
  a 320-calendar-day warmup and exports the full post-upsert snapshot.
- `daily_targets_v1` incrementally recomputes the 90-calendar-day target dirty
  window and exports the full post-upsert snapshot.
- `daily_pipeline_health` reads the already-generated artifacts, skips
  duplicate factor rebuild work, writes health reports, and stores run-scoped
  stock coverage using the Dagster run id.
- Rolling stock coverage windows are `6, 9, 12, 15, 18, 24` months.
- The schedule `daily_research_schedule` exists and is currently stopped by
  default until the daily behavior is reviewed and finalized.

The active Python environment must include Dagster for schedules and
Dagster-specific tests/runtime commands to run.

## Quant Research Design References

The next research layer is intentionally modeled after patterns used in
reputable quantitative research rather than ad hoc indicator hunting. The main
lesson across these references is that durable quant work starts with a clean
universe, audited data, clearly defined features/factors, forward-looking labels
kept separate from features, out-of-sample validation, and portfolio/risk
evaluation. It does not start by training many complex models and picking the
best historical accuracy.

Useful references and case studies:

- [Fama-French factor models](https://en.wikipedia.org/wiki/Fama%E2%80%93French_three-factor_model):
  classic asset-pricing work showing how systematic factors such as market,
  size, value, profitability, and investment explain return differences. This
  supports building explicit feature/factor families before modeling.
- [Jegadeesh-Titman momentum research](https://en.wikipedia.org/wiki/Momentum_investing):
  the canonical momentum case study, where prior winners and losers are sorted
  and tested against future returns. This maps directly to cross-sectional
  feature ranks, quantile analysis, and forward-return labels.
- [AQR systematic factor investing](https://en.wikipedia.org/wiki/AQR_Capital_Management):
  institutional example of combining long-term, repeatable style premia such as
  value, momentum, defensive, carry, trend, and risk-balanced construction.
  This reinforces process discipline over high conviction in any one stock.
- [Two Centuries of Trend Following](https://arxiv.org/abs/1404.3274):
  long-horizon evidence for trend-following across asset classes, with emphasis
  on stability across time and markets. This supports testing trend and momentum
  features for robustness, not just one good backtest window.
- [Black-Litterman portfolio construction](https://en.wikipedia.org/wiki/Black%E2%80%93Litterman_model):
  a portfolio-allocation framework showing that forecasts must be combined with
  uncertainty, constraints, and risk-aware allocation. This is a reminder that
  prediction quality alone is not enough.
- [Purged cross-validation](https://en.wikipedia.org/wiki/Purged_cross-validation):
  a financial machine-learning validation method designed to prevent leakage
  when labels use future time windows. This is relevant for forward-return,
  target-before-stop, and walk-forward model evaluation.
- [Backtesting and overfitting warnings](https://en.wikipedia.org/wiki/Backtesting):
  financial backtests are vulnerable to look-ahead bias, path dependence,
  repeated testing, and overfitting. This supports strict audit trails,
  out-of-sample splits, transaction costs, and experiment tracking.

Design implications for this repo:

- Build a **factor research layer**, not only a feature table.
- Keep `features_daily` and labels/targets separate to reduce leakage risk.
- Measure feature influence with IC/rank IC, quantile returns, hit rates,
  t-stats, monthly stability, and later ablation/model contribution.
- Compare every model to simple baselines and rule-based signals.
- Treat LightGBM as a disciplined experiment after feature/label validation.
- Treat LSTM/sequence models as later experiments, not the foundation.
- Judge models by trading usefulness as well as ML metrics: expectancy, profit
  factor, drawdown, turnover, cost sensitivity, and calibration.

## Current Limitations

- Daily production coverage is currently liquid NSE equities only.
- Upstox daily OHLCV requires `UPSTOX_ACCESS_TOKEN`.
- Upstox raw API candle payloads are not persisted, so raw-to-processed replay
  validation cannot be fully reproduced after the fetch.
- Instrument master fetching uses the public Upstox instruments endpoint and
  does not require a token.
- Generated `data/` artifacts are gitignored and local. A fresh clone needs the
  pipeline commands to be rerun before the artifact-backed UI has meaningful
  data.
- TimescaleDB contents cannot be confirmed from the repo alone. The research
  progress API reads generated local artifacts, not live database row counts.
- Some API and UI surfaces are mock-backed when database or API data is missing,
  including screener results, research notes, job runs, and chart candles.
- The current research/modeling layer is early and not yet a complete
  experiment framework.
- `features_daily`, `feature_runs`, and `feature_audits` now exist for the
  frozen Step 2 v1.0 daily technical feature set.
- `targets_daily`, `target_runs`, and `target_audits` now exist for Step 2.1
  forward-return labels.
- Factor research CSV/JSON outputs and a basic IC review UI now exist for the
  frozen feature and target versions, but they still need deeper diagnostics,
  cost-aware validation, and strategy review before use.
- Stock-level fetched coverage is now audited. Low-history newer listings such
  as `BHARATCOAL`, `ICICIAMC`, `MEESHO`, `PINELABS`, `GROWW`, `TMCV`,
  `LENSKART`, and `LGEINDIA` should be handled carefully in ML/backtests.
- Qdrant storage/search helpers exist, but no end-to-end document ingestion
  command or scheduled ingestion job is implemented.
- No MLflow tracking exists yet.
- No validated LightGBM/LSTM model pipeline exists yet.
- No validated backtesting engine exists yet.
- No paper-trading simulator exists yet.
- No live execution is implemented or intended in the near term.
- Corporate actions, survivorship bias, and provider data quirks need deeper
  treatment before serious strategy conclusions.

## Roadmap

Near-term:

1. Extend factor research outputs with deeper diagnostics and visual notebooks.
2. Extend target/label datasets with explicitly defined one-percent
   target/stop labels after the daily forward-return contract is stable.
3. Add local experiment tracking, likely MLflow, for dataset versions, feature
   versions, target definitions, model parameters, metrics, and artifacts.
4. Train baselines, then LightGBM after the feature/label contract is stable.
   Treat LSTM and other sequence models as later experiments.
5. Extend the existing frontend research dashboard beyond progress and IC
   tables into quantile charts, hit-rate views, monthly stability, experiment
   results, signal review, backtest summaries, equity curves, and candidate
   review.

Medium-term:

1. Build a simple backtesting engine with costs, slippage, target/stop rules,
   max holding period, and capital constraints.
2. Add market-regime and breadth features.
3. Add paper-trading simulation without broker execution.
4. Add fundamentals ingestion after the technical research loop exists.
5. Finalize Dagster daily pipeline behavior, schedule policy, coverage gates,
   and failure handling before building ML datasets.

Long-term:

1. Add Lens as a research analyst and explanation layer over audited data,
   features, models, and backtests.
2. Add stronger retrieval over filings, reports, transcripts, and notes.
3. Add an end-to-end Qdrant document ingestion pipeline.
4. Add production hardening, licensed data-provider decisions, monitoring, and
   deployment runbooks.
5. Consider live execution only after paper trading, risk controls, and
   operational checks are mature.

## Development Checks

Run formatting/lint checks:

```bash
ruff check .
```

Run tests:

```bash
pytest
```

Useful focused tests:

```bash
pytest tests/test_upstox_provider.py
pytest tests/test_daily_technical_features.py
pytest tests/test_daily_forward_targets.py
pytest tests/test_factor_research.py
pytest tests/test_research_artifacts.py
pytest tests/test_research_api.py
pytest tests/test_model_targets.py
pytest tests/test_daily_ohlcv_pipeline.py
pytest tests/test_dagster_daily_assets.py
pytest tests/test_dagster_resources.py
pytest tests/test_daily_pipeline_health.py
```

## Notes And Caveats

- This is research software, not financial advice.
- Backtests can look precise while being wrong. Prioritize leakage prevention,
  auditability, and out-of-sample validation.
- A profitable-looking model should be judged by trading metrics as well as ML
  metrics: precision, recall, calibration, expectancy, profit factor,
  drawdown, turnover, and cost sensitivity.
- Keep generated data local and reproducible. Commit code, tests, docs, and
  schemas; do not commit bulky generated datasets.
- Be conservative with cleanup. Keep canonical processed outputs and audit
  files unless intentionally rebuilding them.
- Treat UI fallback data as development/demo data unless the corresponding API
  response is backed by real artifacts or database rows.
