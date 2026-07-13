# Repository Architecture

This repo is a small modular monolith for market research, Lens chat, and
predictive trading experiments. The audited daily research pipeline is the most
complete part of the system today; chat, UI, and modeling are present but not
all complete production workflows. The repo is intentionally local-first and no
longer carries deployment packaging.

The next ingestion architecture is tracked in
`docs/provider_ingestion_v2_plan.md`. It keeps Dagster as the scheduled
orchestrator, adds provider API rate limiting below every execution surface,
and introduces local/prod parallelism profiles before adding yfinance and
Dukascopy data sources.

```text
apps/web/              React UI
src/trade_research/    Python package for API, data, storage, features, targets, chat, and jobs
src/trade_research/modeling/
                       Small home for reusable model target utilities
experiments/           Exploratory model work
notebooks/             Original NSE/TSX exploration notebooks
data/                  Generated local datasets, gitignored
artifacts/             Generated model/research artifacts, gitignored
```

Keep production logic in `src/trade_research`. Use notebooks and `experiments/`
to explore, then move reusable code into the package when it becomes stable.

Implemented daily research data flow:

```text
Dagster daily_research_pipeline_job
  -> incremental Upstox daily OHLCV fetch/upsert into TimescaleDB
  -> raw-to-processed validation
  -> cleaned/validated OHLCV
  -> daily_v1_ohlcv_technical_v1_0 features from TimescaleDB
  -> daily_v1_forward_returns_v1_0 targets from TimescaleDB
  -> processed dataset validation
  -> daily_v1_factor_research outputs
  -> daily pipeline health report
  -> per-stock full-history and rolling-window coverage
```

Features and targets are intentionally separate. Feature rows describe what was
known at date `T`; target rows describe outcomes after date `T`.

The frozen first ML dataset and walk-forward evaluation contract is documented
in `docs/ml_dataset_v1_strategy.md`. It defines `ml_dataset_v1` as a separate
post-validation layer depending on `processed_dataset_validation`, with a
static full-history 100% coverage universe for the first next-day return model.

Implementation status:

- Fully implemented: liquid NSE universe selection, Upstox instrument mapping,
  Upstox daily OHLCV ingestion, audited daily features, audited forward-return
  targets, factor research CSV/JSON outputs, raw-to-processed validation,
  processed-dataset validation, daily pipeline health reporting, stock-level
  coverage reporting, Timescale storage helpers, and the Dagster daily research
  asset job.
- Partially implemented: Lens chat, React UI, and Qdrant retrieval helpers.
- Mock-backed in places: screener results, research notes, and several
  dashboard fallback paths when database/API data is unavailable.
- Planned: `ml_dataset_v1`, research signals, backtesting, experiment tracking,
  model training, paper trading, and live execution.

Runtime notes:

- `data/` is local/generated and gitignored; artifact-backed API responses
  depend on those files being present.
- `docker-compose.yml` starts the full local stack: API, web, Dagster
  webserver/daemon, TimescaleDB/PostgreSQL, Redis, Qdrant, and CloudBeaver.
  Qdrant is included for future document/research retrieval integration.
- The current Dagster daily research run reaches the latest expected NSE
  trading date `2026-06-25` and is baseline-ML-ready with warnings.
- The canonical ML-prep OHLCV source is
  `data/processed/validated/ohlcv_daily_validated.parquet`, not the raw
  processed Upstox parquet.
- Per-stock fetched coverage is written to
  `data/processed/validation/daily_pipeline_stock_coverage.parquet`.
- Per-run OHLCV fetch coverage for retry planning is written to
  `data/processed/equities/nse_daily_ohlcv_upstox_fetch_coverage.csv` and
  `daily_ohlcv_fetch_coverage`.
- `trade-research retry-upstox-nse-daily` retries only `failed` and `no_rows`
  windows from the latest or specified fetch coverage run.
- Rolling-window stock coverage is written to
  `data/processed/validation/daily_pipeline_stock_coverage_windows.parquet`.
- TimescaleDB stores run-scoped coverage in `stock_coverage_runs` and
  `stock_coverage_by_window`, keyed by the Dagster run id.
- `/api/research/progress` reads local generated artifacts through
  `ResearchArtifactReader`; it does not independently verify Timescale row
  counts.
- Dagster definitions are focused on the Upstox daily research job; the active
  Python environment must include Dagster for the schedule and Dagster-specific
  tests to run.
