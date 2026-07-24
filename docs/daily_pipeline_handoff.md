---
document_status: historical
last_verified_commit: afbc5dc1f78803752d013a6db99a76293d01d61e
last_verified_date: 2026-07-24
owner: trade-research-platform
replaced_by: docs/current_state.md
---

# Daily Pipeline Handoff

> Historical run snapshot. Use `docs/current_state.md` and
> `docs/phase0_production_audit.md` for current truth.

## Working Context

- Repo: `/Users/raviteja/Downloads/projects/trade`
- Branch: `main`
- Data artifacts under `data/` are generated and gitignored.
- Do not print or commit `.env`; it contains the Upstox access token.
- Current priority: review and finalize daily pipeline behavior before building
  ML datasets, backtesting, or strategy code.

## Implemented Pipeline Shape

The daily research path is now available as a Dagster asset job:

```bash
.venv/bin/dagster job execute -m trade_research.dagster.definitions -j daily_research_pipeline_job
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

Behavior:

- `upstox_daily_ohlcv` resolves the latest expected completed NSE trading date,
  incrementally fetches missing Upstox daily candles, upserts into TimescaleDB,
  exports the canonical Parquet snapshot, and writes run-scoped fetch coverage
  for future retry planning.
- `daily_features_v1` incrementally computes newly affected frozen daily
  technical feature rows from TimescaleDB using a 320-calendar-day warmup
  window, stores rows/run metadata/audits, and exports the full post-upsert
  feature snapshot for validation and research.
- `daily_targets_v1` incrementally recomputes the forward-return dirty window
  from TimescaleDB using a 90-calendar-day lookback, stores rows/run
  metadata/audits, and exports the full post-upsert target snapshot for
  validation and research.
- `processed_dataset_validation` validates processed OHLCV, cleaned OHLCV,
  feature/target alignment, invalid rows, and duplicate keys.
- `factor_research_v1` generates IC, quantile, hit-rate, and monthly-stability
  artifacts.
- `daily_pipeline_health` runs read-only against generated artifacts, skips a
  duplicate factor rebuild, writes the health report, writes stock coverage
  artifacts, and persists run-scoped rolling coverage to TimescaleDB.

The Dagster schedule `daily_research_schedule` exists but is stopped by default
until behavior is reviewed and finalized.

## Local-Only Repo Cleanup

Deployment packaging was removed because this repo is being kept local-first:

```text
deploy/
docker-compose.prod.yml
Dockerfile.api
.env.prod.example
apps/web/Dockerfile
apps/web/nginx.conf
dagster_home/
```

`docker-compose.yml` now starts the full local stack:

```text
api
web
dagster-webserver
dagster-daemon
postgres
redis
qdrant
dbeaver
```

This is local development packaging, not production deployment. Qdrant is
included even though the end-to-end document ingestion workflow is future work.

## Current Live Run

Latest successful Dagster run:

```text
run_id: 0a854e73-6572-45de-8aad-8e6f627382ae
latest_expected_trading_date: 2026-06-25
overall_status: warn
baseline_ml_ready: true
serious_research_ready: false
production_ready: false
```

Timescale row counts after that run:

```text
ohlcv_daily: 126,704
features_daily: 126,703
targets_daily: 126,703
daily_ohlcv_fetch_coverage: 261
stock_coverage_runs: 2
stock_coverage_by_window: 3,132
```

Latest verified incremental OHLCV ingestion run:

```text
ingestion_run_id: e361cce3-57ea-4d56-bc90-f490812444e0
status: completed_empty
items_requested/processed/succeeded/failed: 0 / 0 / 0 / 0
mapped_symbols: 261
skipped_current_symbols: 261
fetch coverage statuses: skipped_current=261
retry candidates from this run: 0
```

Latest verified retry run:

```text
retry_run_id: 15a16c9b-0e35-4ba7-9edb-9da05557009e
source_coverage_run_id: e361cce3-57ea-4d56-bc90-f490812444e0
statuses: failed, no_rows
status: completed_empty
candidate_rows: 0
```

Generated artifact row counts:

```text
processed_ohlcv: 126,704
cleaned_ohlcv: 126,703
features: 126,703
targets: 126,703
date_range: 2024-06-18 to 2026-06-25
```

Known invalid raw processed row:

```text
IDEA
NSE_EQ|INE669E01016
2024-08-30
reason: negative_volume
```

Cleaned OHLCV excludes that row and has zero invalid rows and zero duplicate
instrument/date keys.

## Coverage State

Full-history coverage:

```text
stocks: 261
expected_date_count: 502
pass/warn/fail: 243 / 7 / 11
stocks_missing_latest_expected_date: 0
min/median/max coverage: 0.2131 / 1.0 / 1.0
```

Rolling-window coverage stored per Dagster run:

```text
window  stocks  100%  >=90%  70-90%  <70%
6m      261     260   260    1       0
9m      261     253   254    5       2
12m     261     253   253    0       8
15m     261     250   251    2       8
18m     261     250   250    2       9
24m     261     239   244    6       11
```

Coverage artifacts:

```text
data/processed/equities/nse_daily_ohlcv_upstox_fetch_coverage.csv
data/processed/validation/daily_pipeline_stock_coverage.parquet
data/processed/validation/daily_pipeline_stock_coverage_windows.parquet
```

Coverage Timescale tables:

```text
stock_coverage_runs
stock_coverage_by_window
daily_ohlcv_fetch_coverage
```

`daily_ohlcv_fetch_coverage` is the source for the future retry pipeline. It is
run-scoped and records each mapped stock as `skipped_current`, `fetched`,
`failed`, or `no_rows`, with fetch windows, row counts, and error text.
Use `trade-research retry-upstox-nse-daily` to retry the latest run's
`failed` and `no_rows` stocks, or pass `--coverage-run-id` for a specific
source run.

Low-coverage names are mostly newer listings and need explicit handling in ML
datasets and backtests, either by exclusion, bucket, listing-age feature, or a
minimum-history rule.

## Key Code Paths

```text
src/trade_research/dagster/daily_assets.py
src/trade_research/dagster/definitions.py
src/trade_research/pipelines/
src/trade_research/pipelines/daily_ohlcv.py
src/trade_research/pipelines/daily_pipeline_health.py
src/trade_research/validation/daily_pipeline.py
src/trade_research/validation/processed_datasets.py
src/trade_research/storage/timescale.py
scripts/validate_upstox_raw_to_processed.py
```

Important docs:

```text
README.md
docs/architecture.md
docs/pipeline_validation.md
docs/feature_layer_v1_spec.md
docs/research_ui_plan.md
```

## Review Topics For Next Chat

Before starting ML dataset preparation, review and decide:

- Whether `daily_research_schedule` should remain stopped or be enabled.
- Exact schedule time relative to NSE close, Upstox data availability, and
  settlement lag.
- Whether the current incremental feature warmup should remain a bounded
  320-calendar-day approximation for EMA continuity or move to an explicit
  per-symbol feature state table.
- How to treat symbols that are already current versus symbols with date gaps.
- What should fail the Dagster run versus produce warnings.
- Whether coverage thresholds should block ML dataset materialization.
- How to handle newer listings in ML/backtests: exclusion, buckets,
  listing-age feature, or minimum lookback.
- Whether first ML datasets should read from TimescaleDB, Parquet artifacts, or
  a strict hybrid with DB as source and Parquet as review artifact.
- What run metadata must be captured for reproducibility before model training.

## Verification Commands

Focused checks used after the latest implementation:

```bash
.venv/bin/python -m pytest tests/test_daily_ohlcv_pipeline.py tests/test_dagster_daily_assets.py tests/test_dagster_resources.py tests/test_daily_pipeline_health.py tests/test_timescale_feature_storage.py tests/test_timescale_target_storage.py
.venv/bin/python -m ruff check src/trade_research/pipelines src/trade_research/dagster/daily_assets.py src/trade_research/dagster/definitions.py src/trade_research/cli.py src/trade_research/validation/daily_pipeline.py src/trade_research/storage/timescale.py tests/test_daily_ohlcv_pipeline.py tests/test_dagster_daily_assets.py tests/test_daily_pipeline_health.py tests/test_timescale_feature_storage.py
git diff --check
```

Latest result:

```text
16 passed
ruff passed
git diff --check passed
```
