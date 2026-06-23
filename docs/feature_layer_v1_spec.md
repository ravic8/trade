# Feature Layer v1 Spec

This document is the implementation contract for Step 2 of the Trade Research
Agent. The educational companion is `docs/feature_field_guide_v1.md`.

## Feature Set

```text
feature_set_name: daily_v1_ohlcv_technical
initial_feature_version: daily_v1_ohlcv_technical_v1_0
grain: instrument_key + date + feature_version
primary_key: instrument_key, date, feature_version
```

Purpose:

```text
Create audited, deterministic, no-leakage daily technical features from the
liquid NSE equity OHLCV dataset.
```

## Inputs

Canonical input:

```text
ohlcv_daily
```

Required columns:

```text
instrument_key
symbol
exchange
source
date
open
high
low
close
volume
open_interest
quality_status
```

Required input validations:

```text
open > 0
high > 0
low > 0
close > 0
high >= low
high >= open
high >= close
low <= open
low <= close
volume >= 0
no duplicate instrument_key + date + source rows
dates monotonic increasing within instrument_key after sorting
```

Rows failing hard OHLCV validation must not silently produce trusted features.
The strict builder should fail on invalid rows by default. The CLI may exclude
invalid rows for canonical batch generation only when the exclusion count is
written to the feature summary.

## Output Metadata

Required metadata columns:

```text
instrument_key
symbol
exchange
source
date
feature_version
computed_at
quality_status
```

Allowed `quality_status` values:

```text
passed
warning
failed
```

Initial interpretation:

```text
passed  = valid input row and normal feature output
warning = valid input row with expected warmup nulls or non-critical missing rolling values
failed  = invalid input row or duplicate key condition
```

## Core Rules

1. Every feature for date `T` must use only data available on or before `T`.
2. Targets, labels, future returns, and target-hit flags must not be stored in
   the feature table.
3. Features must be deterministic and reproducible.
4. Rolling features must use trailing windows only.
5. Warmup nulls are allowed and must be audited.
6. Missing values must not be filled silently.
7. Cross-sectional ranks must be computed only within the same date and current
   feature universe.
8. Duplicate `instrument_key + date + feature_version` rows are a hard failure.
9. All feature runs must produce Parquet output plus audit CSV and summary JSON.
10. TimescaleDB storage should come after Parquet output and formula validation.

## v1.0 Feature Columns

`daily_v1_ohlcv_technical_v1_0` should include only the core feature families
needed to start trustworthy research.

### Base OHLCV

Include base OHLCV columns for convenience:

```text
open
high
low
close
volume
open_interest
```

### Returns / Momentum

```text
ret_1d
ret_2d
ret_3d
ret_5d
ret_10d
ret_20d
ret_60d
ret_120d
log_ret_1d
```

Formulas:

```text
ret_Nd = close[t] / close[t-N] - 1
log_ret_1d = ln(close[t] / close[t-1])
```

Warmup:

```text
ret_Nd is null until N previous rows exist for that instrument.
log_ret_1d is null for the first row per instrument.
```

### Moving Averages / Trend

```text
sma_10
sma_20
sma_50
sma_100
sma_200
ema_10
ema_20
ema_50
ema_100
ema_200
```

Formulas:

```text
sma_N = trailing N-session average close through date T
ema_N = trailing exponential moving average close through date T
```

Warmup rule:

```text
Require N observations for both SMA and EMA in v1.0.
```

### Price Vs Trend

```text
close_vs_sma_20
close_vs_sma_50
close_vs_sma_200
close_vs_ema_20
close_vs_ema_50
close_vs_ema_200
```

Formulas:

```text
close_vs_sma_N = close / sma_N - 1
close_vs_ema_N = close / ema_N - 1
```

### Moving Average Relationships

```text
sma_20_vs_sma_50
sma_50_vs_sma_200
```

Formulas:

```text
sma_A_vs_sma_B = sma_A / sma_B - 1
```

### Volatility / Risk

```text
volatility_10d
volatility_20d
volatility_60d
volatility_ratio_20d_60d
```

Formulas:

```text
volatility_Nd = rolling standard deviation of log_ret_1d over N sessions
volatility_ratio_20d_60d = volatility_20d / volatility_60d
```

v1.0 rule:

```text
Do not annualize volatility. Keep daily volatility.
```

### ATR / True Range

```text
true_range
atr_14
atr_pct_14
```

Formulas:

```text
true_range = max(
  high - low,
  abs(high - previous_close),
  abs(low - previous_close)
)

atr_14 = trailing 14-session average true_range
atr_pct_14 = atr_14 / close
```

Warmup:

```text
true_range is null for first row per instrument.
atr_14 is null until 14 true-range values exist.
```

### Volume / Liquidity

```text
volume_avg_20d
volume_avg_60d
volume_ratio_20d
volume_ratio_60d
turnover
turnover_avg_20d
turnover_avg_60d
turnover_ratio_20d
turnover_ratio_60d
```

Formulas:

```text
volume_avg_Nd = trailing N-session average volume
volume_ratio_Nd = volume / volume_avg_Nd
turnover = close * volume
turnover_avg_Nd = trailing N-session average turnover
turnover_ratio_Nd = turnover / turnover_avg_Nd
```

## v1.1 Feature Candidates

Add after v1.0 is built, tested, and audited.

### Candle Structure

```text
daily_range_pct
body_pct
upper_wick_pct
lower_wick_pct
close_location_in_range
gap_pct
```

### Rolling High / Low Position

```text
high_20d
low_20d
high_60d
low_60d
high_252d
low_252d
close_vs_20d_high
close_vs_60d_high
close_vs_52w_high
close_vs_20d_low
close_vs_60d_low
close_vs_52w_low
```

### RSI

```text
rsi_14
rsi_14_change_5d
rsi_14_above_70
rsi_14_below_30
```

### Bollinger Bands

```text
bb_middle_20
bb_upper_20_2
bb_lower_20_2
bb_width_20
bb_percent_b_20
bb_close_above_upper_20
bb_close_below_lower_20
```

### MACD

```text
macd_12_26
macd_signal_9
macd_hist_12_26_9
macd_hist_change_5d
macd_above_signal
macd_above_zero
```

### Selected Cross-Sectional Ranks

```text
rank_ret_20d
rank_ret_60d
rank_ret_120d
rank_volatility_20d
rank_volume_ratio_20d
rank_turnover_avg_20d
rank_close_vs_52w_high
rank_rsi_14
rank_atr_pct_14
```

Ranks must be percentile ranks from `0.0` to `1.0` within each date.

## v1.2 Feature Candidates

Add only after v1.0 and v1.1 are trusted.

### ADX / Directional Movement

```text
adx_14
plus_di_14
minus_di_14
di_spread_14
adx_trending_25
```

Reason for v1.2:

```text
ADX is useful but more complex. It should not block the first feature pipeline.
```

## Excluded From Feature Table

Do not include:

```text
future returns
labels
target hit flags
strategy signals
portfolio positions
model predictions
fundamental features
news/sentiment features
intraday features
options/F&O features
market depth features
random interaction features
```

## Target Dataset Later

Targets must be separate from features.

Proposed target dataset:

```text
labels_forward_returns_v1
```

Candidate columns:

```text
instrument_key
symbol
date
label_version
forward_ret_1d
forward_ret_5d
forward_ret_10d
forward_ret_20d
forward_ret_60d
forward_outperform_universe_20d
top_quantile_forward_return_20d
forward_max_ret_3d
forward_min_ret_3d
hit_1pct_3d
hit_1pct_before_minus_0_5pct_3d
```

## Signal Dataset Later

Signals must be separate from features.

Proposed signal dataset:

```text
research_signals_daily_v1
```

Candidate signals:

```text
top_20_momentum_60d
top_decile_momentum_120d
above_sma_200
near_52w_high
volume_expansion
low_volatility_filter
breakout_20d_high
momentum_trend_volume_combo
```

## Audits

Every feature run must write:

```text
data/processed/features/daily_v1_ohlcv_technical_audit.csv
data/processed/features/daily_v1_ohlcv_technical_summary.json
```

Audit fields:

```text
row_count
symbol_count
date_min
date_max
feature_version
duplicate_key_count
invalid_ohlcv_count
null_count_by_feature
null_pct_by_feature
inf_count_by_feature
extreme_value_count_by_feature
warmup_null_expected_count
quality_status_counts
latest_date_by_symbol
missing_dates_by_symbol
```

Hard failures in strict builder mode:

```text
duplicate instrument_key/date/feature_version
invalid OHLCV
missing required columns
non-monotonic dates within instrument after sorting
```

Warnings:

```text
expected warmup nulls
some missing dates
zero volume rows
small number of extreme returns
indicator nulls from rolling windows
```

## Storage Outputs

Initial Parquet output:

```text
data/processed/features/daily_v1_ohlcv_technical.parquet
```

Initial audit outputs:

```text
data/processed/features/daily_v1_ohlcv_technical_audit.csv
data/processed/features/daily_v1_ohlcv_technical_summary.json
```

TimescaleDB tables:

```text
features_daily
feature_runs
feature_audits
```

Implementation status:

```text
1. Build and validate Parquet output.
2. Add tests and audits.
3. Inspect feature values manually.
4. Store v1.0 feature rows, run metadata, and feature audits in TimescaleDB.
```

## CLI Contract

Command:

```bash
trade-research build-daily-features
```

Options:

```text
--input-source parquet|timescale
--input-name processed/equities/nse_daily_ohlcv_upstox
--output-name processed/features/daily_v1_ohlcv_technical
--feature-version daily_v1_ohlcv_technical_v1_0
--store-db / --no-store-db
--limit
--strict-invalid-rows
```

Default CLI behavior:

```text
Exclude invalid OHLCV rows before feature generation and report the exclusion
count in daily_v1_ohlcv_technical_summary.json.
Write canonical Parquet, audit CSV, and summary JSON. TimescaleDB storage is
explicit via --store-db.
```

Strict CLI behavior:

```text
Use --strict-invalid-rows to fail instead of excluding invalid OHLCV rows.
```

## Required Tests

Minimum tests for v1.0:

```text
returns use past closes only
rolling windows do not leak future values
EMA warmup follows the spec
volatility uses log returns
true_range handles gaps correctly
ATR warmup follows the spec
volume and turnover ratios are trailing only
duplicates fail audit
invalid OHLCV fails validation
warmup nulls are warnings, not hard failures
feature output has one row per instrument_key/date/feature_version
```

## Freeze Decision For First Development Batch

Freeze first implementation as:

```text
daily_v1_ohlcv_technical_v1_0
```

Frozen decision:

```text
The CLI may exclude invalid OHLCV rows only if the excluded count is written
into daily_v1_ohlcv_technical_summary.json. Strict builder behavior remains
available through --strict-invalid-rows and tests should continue to verify
that invalid OHLCV fails validation.
```

Included families:

```text
metadata
base OHLCV
returns / momentum
moving averages / trend
price vs trend
moving average relationships
volatility / risk
ATR / true range
volume / liquidity
audit
Parquet output
CLI
tests
```

Deferred:

```text
candle structure
rolling highs/lows
RSI
Bollinger Bands
MACD
cross-sectional ranks
ADX
labels
signals
backtests
models
```

Next implementation step:

```text
Store daily_v1_ohlcv_technical_v1_0 in TimescaleDB after this Parquet-first
feature contract is accepted.
```
