---
document_status: partially_implemented
last_verified_commit: afbc5dc1f78803752d013a6db99a76293d01d61e
last_verified_date: 2026-07-24
owner: trade-research-platform
replaced_by: docs/stabilization_validation_workflow_implementation_plan.md
---

# Daily Pipeline Validation And Health

This document records the current validation layer for the daily NSE Upstox
research pipeline. It is intentionally separate from model-building notes:
these checks prepare the data foundation for later ML dataset creation, but do
not create ML-ready datasets themselves.

## Validation Commands

Raw-to-processed Upstox validation:

```bash
python scripts/validate_upstox_raw_to_processed.py
```

Processed dataset validation:

```bash
trade-research validate-processed-datasets
```

End-to-end daily pipeline health:

```bash
trade-research validate-daily-pipeline-health --run-live-fetch
```

The health command resolves the latest expected NSE trading date, runs a live
Upstox full refresh when requested, rebuilds cleaned OHLCV, features, targets,
factor research, validation summaries, and stock-level coverage.

Dagster daily research job:

```bash
dagster job execute -m trade_research.dagster.definitions -j daily_research_pipeline_job
```

The Dagster job runs incremental Upstox OHLCV ingestion, rebuilds features and
targets from TimescaleDB, runs validation and factor research, then writes
health and stock-coverage artifacts. The final health asset stores run-scoped
coverage in TimescaleDB using the Dagster run id.

## Current Live Health Snapshot

Latest generated reports:

```text
data/processed/validation/daily_pipeline_health_report.md
data/processed/validation/daily_pipeline_health_report.json
data/processed/validation/daily_pipeline_stock_coverage.parquet
data/processed/validation/daily_pipeline_stock_coverage_windows.parquet
```

Current status:

```text
overall_status: warn
baseline_ml_ready: true
serious_research_ready: false
production_ready: false
latest_expected_trading_date: 2026-06-25
live_upstox_fetch_failures: 0
latest_dagster_run_id: 2ee21c93-f29d-4d05-b0e5-6c0cdadf4bb5
```

Current row counts:

```text
processed_ohlcv: 126,704
cleaned_ohlcv: 126,703
features: 126,703
targets: 126,703
feature_target_ohlcv_joined_keys: 126,703
```

Current date ranges:

```text
processed_ohlcv: 2024-06-18 to 2026-06-25
cleaned_ohlcv: 2024-06-18 to 2026-06-25
features: 2024-06-18 to 2026-06-25
targets: 2024-06-18 to 2026-06-25
```

## Cleaned OHLCV

The raw processed Upstox OHLCV parquet currently contains one hard-invalid row:

```text
symbol: IDEA
instrument_key: NSE_EQ|INE669E01016
date: 2024-08-30
reason: negative_volume
```

The cleaned OHLCV artifact excludes this row:

```text
data/processed/validated/ohlcv_daily_validated.parquet
data/processed/validated/ohlcv_daily_validated_metadata.json
```

Use cleaned OHLCV for feature, target, factor research, and future baseline ML
dataset preparation.

## Stock-Level Coverage

The daily pipeline now writes per-stock fetched coverage:

```text
data/processed/validation/daily_pipeline_stock_coverage.parquet
data/processed/validation/daily_pipeline_stock_coverage_windows.parquet
```

Coverage is measured against the full observed cleaned OHLCV date set through
the latest expected trading date. Rolling coverage windows are measured over
6, 9, 12, 15, 18, and 24 months ending at the latest expected trading date.

Current full-history stock coverage:

```text
stocks: 261
expected_date_count: 502
pass_stocks: 243
warn_stocks: 7
fail_stocks: 11
stocks_missing_latest_expected_date: 0
min_coverage_pct: 0.2131
median_coverage_pct: 1.0
max_coverage_pct: 1.0
```

Current rolling-window coverage:

```text
window  stocks  100%  >=90%  70-90%  <70%
6m      261     260   260    1       0
9m      261     253   254    5       2
12m     261     253   253    0       8
15m     261     250   251    2       8
18m     261     250   250    2       9
24m     261     239   244    6       11
```

TimescaleDB tables:

```text
stock_coverage_runs
stock_coverage_by_window
```

The latest Dagster run inserted 1,566 coverage rows: 261 stocks multiplied by
6 rolling windows.

Low-coverage stocks are mostly newer listings and should be handled explicitly
in ML and backtests:

```text
BHARATCOAL  21.5%
ICICIAMC    25.4%
MEESHO      26.8%
PINELABS    30.4%
GROWW       30.8%
TMCV        30.8%
LENSKART    31.2%
LGEINDIA    34.6%
ENRIN       50.7%
BELRISE     53.9%
ATHERENERG  57.1%
```

Warn-level coverage stocks:

```text
VMM          75.7%
NTPCGREEN    78.7%
SWIGGY       80.3%
SAGILITY     80.5%
WAAREEENER   82.7%
HYUNDAI      83.5%
```

## Remaining Warnings

- Raw Upstox candle payloads are not persisted, so full replay validation is
  unavailable after fetch.
- The processed OHLCV file still contains the one invalid negative-volume row;
  cleaned OHLCV excludes it.
- Feature nulls are expected rolling-window warmup nulls.
- Target nulls are expected horizon-end nulls.
- Three liquid-universe symbols remain unmatched to Upstox:
  `STLTECH`, `KRN`, and `PFOCUS`.
- Some newer listings have short history and should be excluded, bucketed, or
  handled with listing-age features in ML workflows.

## Next Step

Proceed to baseline ML dataset preparation using:

```text
data/processed/validated/ohlcv_daily_validated.parquet
data/processed/features/daily_v1_ohlcv_technical.parquet
data/processed/targets/daily_v1_forward_returns.parquet
data/processed/validation/daily_pipeline_stock_coverage.parquet
```

The ML dataset builder should join only on validated `instrument_key + date`
keys and should record exclusions or special handling for low-coverage stocks.

Before building the ML dataset builder, review and finalize:

- daily Dagster schedule timing and whether it should remain stopped by default;
- incremental fetch behavior for symbols that are already current versus
  symbols with gaps;
- whether coverage warnings should be blocking for ML dataset materialization;
- how newer listings should be excluded, bucketed, or represented with
  listing-age features;
- whether TimescaleDB or Parquet should be the canonical input for the first
  ML dataset build.
