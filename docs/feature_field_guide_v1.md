# Feature Field Guide v1

This guide explains the daily OHLCV feature families planned for Step 2 of the
Trade Research Agent. It is educational, not an implementation contract. The
implementation contract lives in `docs/feature_layer_v1_spec.md`.

The goal is to understand what each feature family means, how it can be used in
research, what traps it creates, and how raw features become ranked features,
signals, and eventually strategies.

## Core Mental Model

```text
Raw OHLCV
    -> raw feature
    -> normalized or ranked feature
    -> signal
    -> strategy
    -> backtest / paper trading
```

Example:

```text
ret_60d = 0.18
rank_ret_60d = 0.92
top_20_momentum_60d = true
monthly rebalance top 20 momentum stocks = strategy
```

Definitions:

- **Raw feature**: A measured value for one stock on one date.
- **Normalized feature**: A transformed value, often z-scored or scaled.
- **Ranked feature**: A feature ranked against the same-date universe.
- **Signal**: A rule derived from one or more features.
- **Strategy**: A portfolio process using signals, sizing, exits, costs, and
  rebalance rules.

Features describe the present or past. Targets describe the future. Signals are
research rules. Strategies are simulated trading processes.

## No-Leakage Rule

Every feature on date `T` must use only data available on or before `T`.

Allowed:

```text
ret_20d on 2026-06-18 uses closes through 2026-06-18 only.
rank_ret_60d on 2026-06-18 ranks symbols using values from 2026-06-18 only.
```

Not allowed:

```text
Using future returns inside feature columns.
Using tomorrow's close to define today's signal.
Normalizing with full-sample future data.
Ranking against future universe membership.
```

## 1. Returns / Momentum

### Basic Concept

Returns measure how far the stock moved over a lookback window.

```text
ret_Nd = close[t] / close[t-N] - 1
```

Example:

```text
close[t-20] = 100
close[t]    = 115
ret_20d     = 15%
```

Visual:

```text
Price
  ^
  |                         *
  |                    *
  |               *
  |          *
  |     *
  +----------------------------> time
        t-20                 t
```

### Advanced Interpretation

Momentum can mean different things depending on horizon:

- `ret_1d`, `ret_2d`, `ret_3d`: short-term strength or possible reversal.
- `ret_20d`: one-month trend.
- `ret_60d`: quarterly momentum.
- `ret_120d`: longer trend persistence.

Strong momentum can indicate institutional buying, trend continuation, or
overextension. The feature alone does not decide which.

### Research Questions

- Do top-ranked `ret_60d` stocks outperform over the next 20 sessions?
- Do very strong 1-day moves reverse the next day?
- Does momentum work better when volume is expanding?
- Does momentum work better when volatility is low?

### Possible Signals

```text
top_20_momentum_60d
top_decile_momentum_120d
short_term_reversal_candidate
momentum_with_volume_expansion
```

### Common Traps

- Momentum can fail sharply during regime shifts.
- Strong return may just be a news spike.
- Multiple return windows are correlated, so models may overstate importance.
- Short-term momentum and long-term momentum can have different behavior.

## 2. Moving Averages / Trend

### Basic Concept

Moving averages smooth price to describe trend.

```text
sma_N = trailing N-session average close through date T
ema_N = trailing exponential moving average close through date T
```

Visual:

```text
Price:      /\      /\       /\
           /  \    /  \     /  \
SMA:   ___/----\__/----\___/----\___
```

### Advanced Interpretation

Short moving averages react quickly. Long moving averages define broad trend.

Useful comparisons:

```text
close_vs_sma_200 = close / sma_200 - 1
sma_20_vs_sma_50 = sma_20 / sma_50 - 1
sma_50_vs_sma_200 = sma_50 / sma_200 - 1
```

Trend alignment example:

```text
close > sma_20 > sma_50 > sma_200
```

This describes a strong upward trend structure.

### Research Questions

- Do stocks above SMA200 outperform those below SMA200?
- Does SMA20 > SMA50 > SMA200 improve momentum signals?
- Is a stock too stretched when `close_vs_sma_20` is very high?

### Possible Signals

```text
above_sma_200
trend_aligned_sma_20_50_200
close_extended_above_sma_20
trend_recovery_above_sma_50
```

### Common Traps

- Moving averages are lagging indicators.
- Crossovers can whipsaw in sideways markets.
- A stock far above moving averages may be strong or overextended.

## 3. Volatility / Risk

### Basic Concept

Volatility measures how noisy or risky recent returns have been.

```text
volatility_Nd = rolling stddev(log_ret_1d, N)
```

Visual:

```text
Low volatility:     ----__----__----
High volatility:    /\/\__/\/\/\__/\
```

### Advanced Interpretation

Volatility can be predictive, risk-controlling, or both.

- Low volatility may identify stable trends.
- High volatility may increase target-hit probability but also stop-hit risk.
- Rising volatility can indicate regime change.

### Research Questions

- Do low-volatility momentum stocks outperform high-volatility momentum stocks?
- Does high volatility increase `hit_1pct_3d` but worsen drawdown?
- Does volatility expansion after compression signal breakout potential?

### Possible Signals

```text
low_volatility_filter
volatility_expansion
momentum_low_volatility_combo
avoid_extreme_volatility
```

### Common Traps

- Volatility says movement size, not direction.
- High-volatility stocks can look attractive for 1% targets but may be hard to
  control with stops.
- Volatility estimates are unstable around sudden news events.

## 4. ATR / True Range

### Basic Concept

True range measures daily movement including gaps. ATR smooths true range.

```text
true_range = max(
  high - low,
  abs(high - previous_close),
  abs(low - previous_close)
)

atr_14 = trailing 14-session average true_range
atr_pct_14 = atr_14 / close
```

Visual:

```text
Previous close -----
                   \
Today high   ---------
Today low    ----
```

### Advanced Interpretation

ATR helps decide whether a target or stop is realistic.

Example:

```text
atr_pct_14 = 2.5%
```

A 1% target is inside normal recent movement.

```text
atr_pct_14 = 0.4%
```

A 1% target may be difficult over a short window.

### Research Questions

- Do stocks with ATR above 1% hit 1% targets more often?
- Does high ATR lead to poor stop behavior?
- Should stops be ATR-adjusted instead of fixed percentages?

### Possible Signals

```text
target_feasible_by_atr
avoid_low_atr_for_1pct_target
high_noise_filter
atr_adjusted_stop_candidate
```

### Common Traps

- ATR does not predict direction.
- ATR can spike after news and then mean-revert.
- ATR may make a stock look tradeable but live depth/spread still matters.

## 5. Volume / Liquidity

### Basic Concept

Volume measures shares traded. Volume ratios measure participation relative to
recent normal behavior.

```text
volume_avg_Nd = trailing average volume
volume_ratio_Nd = volume / volume_avg_Nd
```

Visual:

```text
Price:   ___/^^^^
Volume:  |||||||||||||||||||||||
```

### Advanced Interpretation

Volume can confirm or reject price movement.

- Price up with volume expansion may indicate broad participation.
- Price up with weak volume may indicate fragile movement.
- Price down with high volume may indicate distribution or panic.

### Research Questions

- Do breakouts with volume expansion continue more often?
- Do low-volume breakouts fail more often?
- Does volume expansion matter more near 20-day highs?

### Possible Signals

```text
volume_expansion_20d
breakout_with_volume
low_volume_move_warning
distribution_day_candidate
```

### Common Traps

- Volume spike can mean accumulation, distribution, news, or exhaustion.
- Volume is not comparable across stock prices, so turnover is also needed.
- Historical volume does not guarantee live order-book depth.

## 6. Turnover / Tradeability

### Basic Concept

Turnover measures rupee value traded.

```text
turnover = close * volume
turnover_ratio_Nd = turnover / turnover_avg_Nd
```

Example:

```text
Stock A: close = Rs 10, volume = 10,000,000, turnover = Rs 10 crore
Stock B: close = Rs 1,000, volume = 500,000, turnover = Rs 50 crore
```

Stock A has higher share volume, but Stock B has higher traded value.

### Advanced Interpretation

Turnover is central for this project because the intended trade size is around
Rs 1 lakh per stock.

Historical turnover helps screen tradeability. Live spread and depth still need
separate future checks before execution.

### Research Questions

- Does high turnover improve signal reliability?
- Do turnover spikes confirm breakout behavior?
- Does declining turnover increase false signals?

### Possible Signals

```text
high_turnover_universe_member
turnover_expansion
liquidity_stability_filter
avoid_declining_liquidity
```

### Common Traps

- Turnover can jump for one-time news and not persist.
- High historical turnover does not guarantee tight live spread.
- Penny stocks can have high volume but unsuitable trading quality.

## 7. Candle Structure

### Basic Concept

Candle features describe what happened inside the daily OHLC bar.

```text
daily_range_pct = (high - low) / close
body_pct = abs(close - open) / close
upper_wick_pct = (high - max(open, close)) / close
lower_wick_pct = (min(open, close) - low) / close
close_location_in_range = (close - low) / (high - low)
gap_pct = open / previous_close - 1
```

Visual:

```text
       High
        |
        | upper wick
     [ body ]
        |
        | lower wick
       Low
```

Close location:

```text
0.0 = close near low
0.5 = close near middle
1.0 = close near high
```

### Advanced Interpretation

Candle structure can describe pressure:

- Close near high may show buyers controlled the session.
- Long upper wick may show rejection at higher prices.
- Long lower wick may show buying after weakness.
- Gap up with strong close may show continuation potential.

### Research Questions

- Do stocks closing near the day high continue next day?
- Do long upper wicks predict weakness?
- Do gap-ups with volume expansion continue?
- Are lower wicks useful only in uptrends?

### Possible Signals

```text
strong_close_near_high
gap_up_strong_close
upper_wick_rejection
lower_wick_recovery
wide_range_breakout_day
```

### Common Traps

- Daily candles hide intraday sequence.
- A close near high can be buying pressure or late short covering.
- Gap features can be distorted by corporate actions if not adjusted.

## 8. Rolling High / Low Position

### Basic Concept

These features locate price relative to recent highs and lows.

```text
high_Nd = trailing N-session max high
low_Nd = trailing N-session min low
close_vs_Nd_high = close / high_Nd - 1
close_vs_Nd_low = close / low_Nd - 1
```

Visual:

```text
52w high  ----------------
Price                  *
                    *
                 *
52w low   ----------------
```

### Advanced Interpretation

Being near a high can mean strength, while being far below a high can indicate
drawdown or value/reversal context.

Important horizons:

- 20-day: short-term breakout/reversal.
- 60-day: medium-term range.
- 252-day: 52-week high/low context.

### Research Questions

- Do stocks near 52-week highs continue to outperform?
- Do stocks near 20-day lows bounce?
- Are breakouts more reliable when volume expands?
- Does `close_vs_52w_high` behave differently in bull and bear regimes?

### Possible Signals

```text
near_52w_high
breakout_20d_high
pullback_to_60d_low
recovery_from_20d_low
```

### Common Traps

- Near high can mean strength or exhaustion.
- Near low can mean opportunity or structural weakness.
- A breakout without liquidity/volume confirmation may fail.

## 9. RSI

### Basic Concept

RSI measures recent upward strength versus downward weakness.

Typical interpretation:

```text
RSI > 70 = strong / overbought
RSI < 30 = weak / oversold
RSI ~ 50 = neutral
```

Visual:

```text
RSI
100 |        overbought
 70 |-------------------
 50 |        neutral
 30 |-------------------
  0 |        oversold
```

### Advanced Interpretation

RSI is not automatically a contrarian indicator.

In strong uptrends, RSI can stay high for a long time. In strong downtrends,
RSI can stay low for a long time.

Useful states:

```text
rsi_14
rsi_14_change_5d
rsi_14_above_70
rsi_14_below_30
```

### Research Questions

- Does RSI below 30 bounce in liquid NSE stocks?
- Does RSI between 50 and 70 work better in uptrends?
- Does RSI above 70 indicate momentum rather than overbought weakness?
- Does RSI recovery above 50 after pullback improve signals?

### Possible Signals

```text
rsi_recovery_above_50
rsi_oversold_bounce_candidate
rsi_momentum_state
rsi_overextended_warning
```

### Common Traps

- RSI can stay extreme in trends.
- RSI thresholds should not be treated as buy/sell rules by default.
- RSI is derived from returns and may overlap with momentum features.

## 10. Bollinger Bands

### Basic Concept

Bollinger Bands compare price to a moving average and volatility band.

```text
middle = SMA20
upper = SMA20 + 2 * rolling_std_close_20
lower = SMA20 - 2 * rolling_std_close_20
bb_width = (upper - lower) / middle
bb_percent_b = (close - lower) / (upper - lower)
```

Visual:

```text
Upper band   ~~~~~~~~~~~~~
Price             /\  /\
Middle       --------------
Lower band   ~~~~~~~~~~~~~
```

### Advanced Interpretation

Useful ideas:

- Low band width can mean volatility compression.
- Expanding band width can mean volatility expansion.
- Price above upper band can mean breakout or exhaustion.
- Price near lower band can mean weakness or reversal opportunity.

### Research Questions

- Do Bollinger squeezes lead to breakouts?
- Does closing above the upper band continue or reverse?
- Do lower-band touches bounce only when price is above SMA200?
- Does `bb_percent_b` improve momentum signals?

### Possible Signals

```text
bollinger_squeeze
upper_band_breakout
lower_band_reversal_candidate
volatility_expansion_after_squeeze
```

### Common Traps

- Band touch is not automatically buy/sell.
- Band width changes with volatility, not direction.
- Bollinger features overlap with moving average and volatility features.

## 11. MACD

### Basic Concept

MACD measures trend acceleration using fast and slow EMAs.

```text
macd_12_26 = EMA12 - EMA26
macd_signal_9 = EMA9(macd_12_26)
macd_hist = macd_12_26 - macd_signal_9
```

Visual:

```text
MACD line:    /\/\____/\/\
Signal:      __/\/\____/\
Histogram:   + + - - + +
```

### Advanced Interpretation

MACD can show whether short-term trend is stronger than longer-term trend.

Useful states:

```text
macd_hist_change_5d
macd_above_signal
macd_above_zero
```

### Research Questions

- Does positive MACD histogram predict continuation?
- Does MACD above zero improve momentum filters?
- Does improving histogram lead price breakouts?
- Does MACD fail in low-ADX sideways regimes?

### Possible Signals

```text
macd_positive_momentum
macd_hist_improving
macd_cross_above_signal
macd_above_zero_filter
```

### Common Traps

- MACD is lagging.
- MACD can whipsaw in sideways markets.
- MACD overlaps heavily with EMA features.

## 12. ADX / Directional Movement

### Basic Concept

ADX measures trend strength, not direction.

```text
adx_14
plus_di_14
minus_di_14
di_spread_14 = plus_di_14 - minus_di_14
```

Interpretation:

```text
ADX high       = strong trend
+DI > -DI      = upward directional pressure
-DI > +DI      = downward directional pressure
```

Visual:

```text
ADX
50 |          strong trend
25 |----------------------
10 |          weak trend
```

### Advanced Interpretation

ADX can help decide which kind of signal should be trusted.

- Trend-following signals may work better when ADX is rising.
- Mean-reversion signals may work better when ADX is low.
- High ADX with `minus_di > plus_di` can describe a strong downtrend.

### Research Questions

- Do breakouts work better when ADX is rising?
- Do mean-reversion setups fail when ADX is high?
- Does `plus_di > minus_di` improve long-only signals?

### Possible Signals

```text
trend_strength_filter
adx_rising_breakout_filter
plus_di_dominance
avoid_high_adx_downtrend
```

### Common Traps

- ADX does not say direction.
- ADX is slower and more complex to compute.
- ADX should be a v1.1/v1.2 feature if the core pipeline is not yet trusted.

## 13. Cross-Sectional Ranks

### Basic Concept

Ranks compare stocks against the same-date liquid universe.

```text
rank_ret_60d = percentile rank of ret_60d across all valid stocks on date T
```

Visual:

```text
Weakest                                Strongest
0.0 -------- 0.25 -------- 0.5 -------- 0.75 -------- 1.0
```

### Advanced Interpretation

Cross-sectional ranks are central to factor research.

Examples:

```text
rank_ret_60d = 0.92
```

The stock is stronger than about 92% of the universe by 60-day return on that
date.

```text
rank_volatility_20d = 0.10
```

The stock is among the lower-volatility names on that date.

### Research Questions

- Do top momentum quantiles outperform bottom quantiles?
- Does top volatility rank hit 1% more often but draw down more?
- Do high turnover-ratio stocks have better breakout continuation?

### Possible Signals

```text
top_20_momentum_60d
top_decile_turnover_expansion
low_volatility_top_half
near_52w_high_top_quintile
```

### Common Traps

- Ranks must use same-date universe only.
- Ranks are unstable when many values are missing.
- A rank can hide absolute values: top-ranked momentum may still be negative
  during a bad market.

## Feature Influence Methods

The first research layer should measure whether features matter before model
training.

Useful methods:

- **Quantile analysis**: split stocks by feature rank and compare forward
  returns.
- **IC / rank IC**: correlation between feature values/ranks and future returns.
- **Hit-rate by bucket**: compare target success rates across feature buckets.
- **T-stat**: estimate whether mean return differences are meaningful.
- **Monthly stability**: check whether the feature works consistently over
  time.
- **Combination tests**: test whether a feature only works with another filter.
- **Ablation later**: train a model with and without a feature family.

Important: A feature can be useful in different ways.

```text
Predictive: higher value predicts higher future return.
Risk-controlling: improves drawdown or stop behavior.
Combination-only: useful only with other features.
Regime-dependent: works in some market states, fails in others.
```

## Good First Research Signals

Start with interpretable signals, not complex formulas:

```text
top_20_momentum_60d
top_decile_momentum_120d
above_sma_200
sma_20_above_sma_50
low_volatility_filter
volume_expansion_20d
near_52w_high
breakout_20d_high
strong_close_near_high
gap_up_with_volume
momentum_low_volatility_combo
momentum_trend_volume_combo
```

Signals should be stored separately from features. A signal is a rule; a
feature is a measurement.

## What This Guide Does Not Include

This guide does not define:

- target labels,
- model training,
- backtest implementation,
- portfolio sizing,
- live execution,
- fundamentals,
- news/sentiment,
- intraday features.

Those should come after the feature layer and label layer are trusted.
