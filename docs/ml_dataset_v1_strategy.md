# ML Dataset v1 Strategy

This document freezes the first ML dataset and model-evaluation strategy. It is
the implementation contract for `ml_dataset_v1` and the first next-day stock
return models.

## Objective

Build a leakage-aware daily ML dataset for NSE equities that supports fast
LightGBM, XGBoost, and related model experiments.

The first model objective is:

```text
Predict next-day stock returns, rank stocks by expected upside, and avoid names
with elevated downside risk.
```

The first strategy is long-only. It should rank the eligible universe each day,
select the top candidates, and evaluate realized next-day returns with explicit
drawdown and loss metrics.

## Frozen v1 Scope

`ml_dataset_v1` depends on `processed_dataset_validation`.

The v1 universe is intentionally conservative:

```text
Only stocks with 100% coverage across the full available two-year history are
eligible for the ML dataset.
```

Current data horizon:

```text
2024-06-18 to 2026-06-25
```

This full-history coverage filter is useful for a clean first baseline. It is
not fully point-in-time realistic because early historical rows benefit from
knowing that a stock later maintained complete coverage. The dataset summary
must record this policy explicitly:

```text
universe_policy: static_full_history_100pct_coverage
leakage_note: static research universe; later replace with point-in-time
coverage eligibility for live-realistic backtests
```

Later UI-configurable versions can relax or change this policy by minimum
coverage, liquidity bucket, index membership, listing age, sector, or custom
stock selection.

## Pipeline Position

Dagster should materialize the dataset after processed validation:

```text
upstox_daily_ohlcv
  -> daily_features_v1
  -> daily_targets_v1
  -> processed_dataset_validation
  -> ml_dataset_v1
  -> factor_research_v1
  -> daily_pipeline_health
```

`ml_dataset_v1` must not materialize when `processed_dataset_validation` fails.

## Inputs

Use generated Parquet artifacts as the canonical v1 inputs:

```text
data/processed/validated/ohlcv_daily_validated.parquet
data/processed/features/daily_v1_ohlcv_technical.parquet
data/processed/targets/daily_v1_forward_returns.parquet
data/processed/validation/daily_pipeline_stock_coverage.parquet
data/processed/validation/daily_pipeline_stock_coverage_windows.parquet
```

Parquet is preferred for v1 because it is reproducible, inspectable, and already
validated by the daily pipeline. Timescale-backed materialization can be added
after the first dataset and model loop is proven.

## Output Artifacts

Write all outputs under:

```text
data/processed/ml/
```

Required artifacts:

```text
ml_dataset_v1.parquet
ml_dataset_v1_summary.json
ml_dataset_v1_exclusions.csv
ml_dataset_v1_feature_columns.json
ml_dataset_v1_leakage_checks.json
```

The main dataset should be one row per:

```text
instrument_key + date
```

Required metadata columns:

```text
instrument_key
symbol
exchange
source
date
ml_dataset_version
feature_version
target_version
coverage_policy
coverage_pct_full_history
is_trainable
split
exclusion_reasons
```

Required target/risk columns:

```text
forward_ret_1d
next_day_positive
next_day_top_decile
next_day_bottom_decile
daily_forward_ret_1d_rank
```

The model feature list must be emitted separately in
`ml_dataset_v1_feature_columns.json`. Do not infer feature columns implicitly at
training time.

## Dataset Construction

Implementation should live in package code, not notebooks:

```text
src/trade_research/pipelines/ml_dataset.py
src/trade_research/modeling/ml_dataset_v1.py
tests/test_ml_dataset_v1.py
```

Recommended responsibilities:

- `modeling/ml_dataset_v1.py`: pure dataframe builder, eligibility rules,
  feature-column selection, split assignment, and leakage checks.
- `pipelines/ml_dataset.py`: artifact IO, summary writing, and
  `PipelineRunResult` wrapping.
- `dagster/daily_assets.py`: `ml_dataset_v1` asset wiring.

Construction steps:

1. Read validated OHLCV, feature, target, and coverage artifacts.
2. Validate no duplicate `instrument_key + date` rows in OHLCV/features/targets.
3. Select only stocks with 100% full-history coverage.
4. Join OHLCV, features, and targets on `instrument_key + date`.
5. Keep next-day target columns and create next-day classification/ranking
   helper labels.
6. Build a strict model feature whitelist from v1 technical feature columns.
7. Mark trainable rows with `is_trainable`; do not silently drop rejected rows
   before writing audits.
8. Assign chronological walk-forward eligibility metadata.
9. Run leakage checks and fail the pipeline on hard leakage violations.
10. Write dataset, summary, exclusions, feature list, and leakage report.

## Trainable Row Rules

A row is trainable only when all of these are true:

```text
stock is in the static full-history 100% coverage universe
feature row exists for instrument_key + date
target row exists for instrument_key + date
forward_ret_1d is non-null
all selected feature columns are non-null and finite
row date is inside the walk-forward evaluation horizon
```

Rows that fail a rule should carry one or more explicit exclusion reasons:

```text
not_full_history_coverage
missing_feature_row
missing_target_row
feature_null_or_inf
target_null
outside_walk_forward_horizon
```

The exclusion CSV should include both stock-level and row-level exclusion counts.

## Leakage Prevention

Leakage prevention is mandatory for v1.

Hard rules:

- Feature rows at date `T` may use only data known on or before `T`.
- `forward_ret_1d` and all derived labels are target-only columns.
- Target, rank, split, trainability, exclusion, and identifier columns must not
  enter the model feature list.
- Splits and walk-forward windows must be chronological only.
- Preprocessing must be fit on training data only inside each walk-forward fold.
- Validation and backtest/prediction windows must never influence training
  labels, feature selection, imputation, scaling, or hyperparameter fitting.
- The full-history coverage policy must be recorded as a static research
  universe assumption.

Required leakage checks:

```text
target_columns_not_in_feature_columns
identifier_columns_not_in_feature_columns
train_max_date_before_prediction_date
latest_train_label_date_before_prediction_date
no_duplicate_dataset_keys
no_null_targets_in_trainable_rows
no_null_or_inf_features_in_trainable_rows
coverage_policy_recorded
```

For next-day returns, a training row with feature date `D` is label-complete
only after `D + 1`. For a prediction generated after close on date `T`, training
labels can include feature dates up to `T - 1`, and prediction features can use
date `T`.

## Walk-Forward Evaluation

The first evaluation should support daily incremental walk-forward prediction.

For each prediction date `T`:

```text
train only on rows whose labels were known before T
optionally validate on a recent window before T
fit preprocessing on the training rows only
train or reuse the model according to retrain_frequency
predict all eligible stocks using features from T
rank stocks by predicted next-day return
evaluate realized forward_ret_1d after T + 1 is known
```

Recommended initial configuration:

```text
target: forward_ret_1d
min_train_days: 240
validation_days: 60
prediction_step_days: 1
retrain_frequency: daily, with weekly as a speed fallback
universe: static full-history 100% coverage stocks
```

Phase 2 adapter smoke tests against the real `ml_dataset_v1` artifacts showed
that the 200-day feature warmup leaves about 302 trainable dates. A
`300 train + 60 validation` configuration is therefore too large for the first
walk-forward run. Use `240 train + 60 validation` for v1, then revisit once more
history is available or shorter-warmup feature sets are added.

Phase 3 materializes this as:

```text
data/processed/ml/walk_forward_v1/walk_forward_folds.parquet
data/processed/ml/walk_forward_v1/walk_forward_summary.json
```

Command:

```bash
trade-research build-walk-forward-folds-v1
```

With the current real `ml_dataset_v1` artifacts and the default `240/60`
configuration, the first manifest produces 3 valid prediction folds:

```text
2026-06-23
2026-06-24
2026-06-25
```

This is enough to validate the mechanics, but early model phases may also use a
shorter training/validation configuration for exploratory baseline comparisons.

Phase 4 baseline artifacts:

```text
data/processed/ml/baselines_v1/baseline_predictions.parquet
data/processed/ml/baselines_v1/baseline_metrics.json
data/processed/ml/baselines_v1/baseline_summary.md
```

Command:

```bash
trade-research run-baseline-predictions-v1
```

The Phase 4 command defaults to `180 train + 40 validation` to create enough
folds for baseline comparison while the stricter `240/60` setup remains the
official conservative reference.

Phase 5 LightGBM artifacts:

```text
data/processed/ml/lightgbm_v1/lightgbm_predictions.parquet
data/processed/ml/lightgbm_v1/lightgbm_metrics.json
data/processed/ml/lightgbm_v1/lightgbm_summary.md
```

Command:

```bash
trade-research run-lightgbm-predictions-v1
```

The first implementation trains three models per fold:

```text
lgbm_regressor
lgbm_upside_classifier
lgbm_downside_classifier
```

It also emits momentum-blended variants of each LightGBM score:

```text
lgbm_regressor_momentum_blend
lgbm_upside_classifier_momentum_blend
lgbm_downside_classifier_momentum_blend
```

The default run is capped at 10 folds for local smoke testing. The first real
10-fold run completed successfully, but the LightGBM metrics were weaker than
the simple momentum baselines. Treat this as a functional model pipeline, not
yet as a better signal. Momentum blending improved rank IC versus raw LightGBM
in the first smoke run, but top-10 average returns remained negative over that
window.

Daily retraining is the cleanest evaluation. Weekly retraining is acceptable as
a speed optimization if every prediction still records which model snapshot was
used and what data that snapshot was allowed to see.

## Validation vs Final Backtest

Validation is for model and strategy selection:

```text
model family
hyperparameters
feature subset
top-N portfolio size
downside filter threshold
retrain frequency
transaction-cost assumptions
```

The final backtest is a locked simulation after those choices are fixed. It
answers:

```text
What would have happened if this strategy had been run without further tuning?
```

With roughly two years of data, use walk-forward validation on the earlier
evaluation period and preserve the latest block as the locked final assessment
where possible. A 75-100 trading-day final assessment is acceptable for v1, but
it is not enough to claim production robustness.

## Model Strategy

The first model should be a cross-sectional next-day ranking model.

Baseline model families:

```text
LightGBM
XGBoost
simple sklearn baselines
```

Initial prediction outputs:

```text
prediction_date
model_id
model_family
train_start_date
train_end_date
validation_start_date
validation_end_date
symbol
instrument_key
predicted_forward_ret_1d
predicted_downside_risk
rank
selected_for_trade
realized_forward_ret_1d
```

The first portfolio rule should be simple:

```text
long top N predicted stocks each day
equal weight
optional downside-risk filter
no leverage
```

Start with top 5, top 10, and top 20 comparisons, then choose one based on
validation results before final backtest reporting.

## Reporting Findings

Every model/backtest run should write machine-readable artifacts and a concise
human report.

Suggested artifact root:

```text
data/processed/ml/reports/
```

Required reports:

```text
walk_forward_predictions.parquet
daily_portfolio_returns.csv
portfolio_equity_curve.csv
model_metrics.json
backtest_summary.json
backtest_report.md
```

Report the model quality:

```text
rank_ic_mean
rank_ic_median
hit_rate_top_n
top_decile_average_return
bottom_decile_average_return
prediction_return_correlation
directional_accuracy
```

Report the trading quality:

```text
total_return
annualized_return
annualized_volatility
sharpe_ratio
max_drawdown
win_rate
average_daily_return
average_winning_day
average_losing_day
profit_factor
turnover
estimated_transaction_costs
net_return_after_costs
best_day
worst_day
```

Report robustness:

```text
monthly_returns
rolling_20d_return
rolling_20d_drawdown
performance_by_market_regime_if_available
performance_by_stock
top_feature_importance
```

Every report must include:

```text
dataset_version
feature_version
target_version
coverage_policy
walk_forward_config
model_config
transaction_cost_config
leakage_check_status
known_limitations
```

The first reports should be framed as research evidence, not trading approval.
The strategy becomes a paper-trading candidate only after repeated
walk-forward/backtest runs remain positive after costs and drawdowns are
acceptable.

First backtest artifacts:

```text
data/processed/ml/backtests_v1/baselines/daily_portfolio_returns.csv
data/processed/ml/backtests_v1/baselines/portfolio_equity_curve.csv
data/processed/ml/backtests_v1/baselines/backtest_metrics.json
data/processed/ml/backtests_v1/baselines/backtest_report.md

data/processed/ml/backtests_v1/lightgbm/daily_portfolio_returns.csv
data/processed/ml/backtests_v1/lightgbm/portfolio_equity_curve.csv
data/processed/ml/backtests_v1/lightgbm/backtest_metrics.json
data/processed/ml/backtests_v1/lightgbm/backtest_report.md
```

Command:

```bash
trade-research run-prediction-backtest-v1 \
  --predictions data/processed/ml/baselines_v1/baseline_predictions.parquet \
  --output-dir data/processed/ml/backtests_v1/baselines
```

The initial baseline backtest uses long-only, equal-weight, daily-rebalanced
top-N portfolios with transaction costs. The first result strongly favored
`momentum_1d` on the exploratory `180/40` baseline window:

```text
momentum_1d top 5
days: 82
total_return: 0.6792
sharpe_ratio: 4.8399
max_drawdown: -0.1270
win_rate: 0.6098
```

The first LightGBM backtest remained negative across raw and momentum-blended
variants. This reinforces that LightGBM needs more work before it is useful,
and the current simple momentum baseline is the bar to beat.

## Known v1 Limitations

- The static full-history 100% coverage universe is clean but not fully
  point-in-time realistic.
- Current `forward_ret_1d` is close-to-close. Live execution after close should
  eventually use next-open based targets.
- Two years of data is enough for an initial baseline, but not enough for a
  strong production claim.
- Feature set is technical/OHLCV-only. No fundamentals, news, sector, event, or
  regime features are included yet.
- Final model performance must be reported after transaction costs and slippage.

## Future UI Configuration

Later, the UI should let the user configure:

```text
universe selection
coverage threshold and window
liquidity filters
target horizon
model family
walk-forward windows
retrain frequency
top-N portfolio size
downside-risk filter
transaction costs
backtest date range
```

The v1 code should keep these settings as explicit config fields even if the
first values are hardcoded defaults.
