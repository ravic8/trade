# Trade Research Agent

Local-first market data and research infrastructure for building a systematic
trade research agent focused on Indian equities.

The project is currently a data foundation layer plus early research tooling. It
builds a clean tradable NSE equity universe, maps symbols to provider
instruments, ingests audited OHLCV data, stores canonical datasets locally, and
prepares the ground for feature engineering, backtesting, model experiments, and
the future Lens research agent.

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
validated backtests.

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
- Existing Yahoo-based hourly NSE/TSX ingestion through Dagster.
- Early modeling/target experiments under `src/trade_research/modeling/` and
  `experiments/`.

## Non-Goals For Now

These are deliberately out of scope until the data, feature, research, and
backtesting layers are trustworthy:

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
    -> feature/research/backtest layers
    -> Lens research agent later
```

Core components:

- **Python package**: reusable data, storage, feature, chat, and modeling code
  under `src/trade_research/`.
- **Typer CLI**: repeatable local batch commands exposed as `trade-research`.
- **TimescaleDB/PostgreSQL**: canonical structured market-data store.
- **Parquet/CSV**: local analytical outputs and reproducible batch artifacts.
- **Dagster**: scheduled/observable ingestion for the existing hourly Yahoo NSE
  and TSX pipelines.
- **FastAPI + React**: existing application shell for Lens/chat/research UI.
- **Qdrant**: vector store for document retrieval experiments.
- **Docker Compose**: local infrastructure for Postgres/Timescale, Redis,
  Qdrant, API, web, and Dagster.

## Folder Structure

```text
apps/web/                    React frontend
src/trade_research/          Main Python package
src/trade_research/data/     Market-data providers and audits
src/trade_research/storage/  Timescale, Parquet, and vector storage helpers
src/trade_research/universe/ Exchange universe providers
src/trade_research/features/ Feature builders
src/trade_research/modeling/ Modeling/target utilities
src/trade_research/dagster/  Dagster assets, schedules, sensors, resources
src/trade_research/chat/     Lens chat orchestration and tools
scripts/                     Standalone local scripts
experiments/                 Exploratory research and model experiments
notebooks/                   Legacy/exploratory notebooks
docs/                        Architecture and design notes
data/                        Generated local datasets, gitignored
artifacts/                   Generated model/research artifacts, gitignored
output/                      Generated report/PDF outputs, gitignored
tmp/                         Temporary render/debug outputs, gitignored
deploy/                      Deployment and backup helper scripts
dagster_home/                Local Dagster configuration
```

Production-quality reusable logic should move into `src/trade_research/`.
Notebooks and `experiments/` are for exploration and should call package code
instead of becoming the only source of logic.

## Completed Pipeline Progress

### Step 0: Liquid NSE Universe

Script:

```bash
python scripts/select_liquid_nse_universe.py \
  --min-avg-daily-turnover 1000000000 \
  --top-n 1000
```

Purpose:

- Fetch NSE equity universe.
- Pull approximately six months of yfinance daily OHLCV.
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

Current result:

- Daily OHLCV rows: 125,189.
- Symbols: 261.
- Date range: 2024-06-18 to 2026-06-18.
- Fetch failures: 0.
- Audit status: 259 passed, 2 warnings (`IDEA`, `CUPID`).

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
- `ohlcv_hourly`: existing Yahoo hourly OHLCV for NSE/TSX.
- `data_quality_audits`: row-level summary audit records for generated
  datasets.
- `ingestion_runs`: run history and success/failure counts.
- `feed_health`: Yahoo hourly source health by symbol.
- `hourly_backlog_windows`: detected and recovered hourly gaps.
- `exchange_holidays`: cached exchange calendars.

Hypertables:

- `ohlcv_daily` is a Timescale hypertable on `date`.
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
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

Start local infrastructure:

```bash
docker compose up -d postgres redis qdrant
```

Initialize the database schema:

```bash
trade-research init-db
```

Run the full Docker app stack:

```bash
docker compose up --build
```

Local services:

```text
React web:       http://localhost:5173
FastAPI API:     http://localhost:8000
Dagster UI:      http://localhost:3000
TimescaleDB:     localhost:5432
Qdrant:          localhost:6333
```

## Major Pipeline Commands

Select liquid NSE universe:

```bash
python scripts/select_liquid_nse_universe.py \
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

Run existing hourly Yahoo ingestion:

```bash
trade-research ingest-hourly NSE
trade-research ingest-hourly TSX
```

Backfill a wider hourly Yahoo window:

```bash
trade-research backfill-hourly NSE
```

Build legacy range features from a Parquet OHLCV file:

```bash
trade-research features-from-parquet input.parquet output.parquet
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

Dagster currently orchestrates the older Yahoo hourly NSE/TSX path:

- `nse_universe` -> `nse_hourly_ohlcv`
- `tsx_universe` -> `tsx_hourly_ohlcv`
- hourly schedules with exchange time zones
- backlog sensors for missing/partial hourly windows
- bounded recovery jobs

The newer Upstox instrument, mapping, and daily OHLCV pipelines are currently
CLI-driven. Once the daily pipeline and feature layer stabilize, they can be
promoted into Dagster assets for scheduled, observable runs.

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
- Instrument master fetching uses the public Upstox instruments endpoint and
  does not require a token.
- The current research/modeling layer is early and not yet a complete
  experiment framework.
- No production feature table such as `features_daily` exists yet.
- No MLflow tracking exists yet.
- No validated LightGBM/LSTM model pipeline exists yet.
- No paper-trading simulator exists yet.
- No live execution is implemented or intended in the near term.
- Corporate actions, survivorship bias, and provider data quirks need deeper
  treatment before serious strategy conclusions.

## Roadmap

Near-term:

1. Build `features_daily` from `ohlcv_daily`.
2. Add feature audits and feature-version tracking.
3. Build target/label datasets separately from features:
   `forward_ret_*`, universe outperformance, top-quantile labels, and
   one-percent target/stop labels.
4. Build factor research outputs: IC/rank IC, quantile analysis, hit-rate
   tables, t-stats, and monthly stability.
5. Add local experiment tracking, likely MLflow, for dataset versions, feature
   versions, target definitions, model parameters, metrics, and artifacts.
6. Train baselines, then LightGBM after the feature/label contract is stable.
   Treat LSTM and other sequence models as later experiments.
7. Build a frontend research dashboard for experiment results, feature impact,
   signal review, backtest summaries, equity curves, and candidate review.

Medium-term:

1. Build a simple backtesting engine with costs, slippage, target/stop rules,
   max holding period, and capital constraints.
2. Add market-regime and breadth features.
3. Add paper-trading simulation without broker execution.
4. Add fundamentals ingestion after the technical research loop exists.
5. Promote stable batch jobs into Dagster assets.

Long-term:

1. Add Lens as a research analyst and explanation layer over audited data,
   features, models, and backtests.
2. Add stronger retrieval over filings, reports, transcripts, and notes.
3. Add production hardening, licensed data-provider decisions, monitoring, and
   deployment runbooks.
4. Consider live execution only after paper trading, risk controls, and
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
pytest tests/test_model_targets.py
pytest tests/test_dagster_resources.py
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
