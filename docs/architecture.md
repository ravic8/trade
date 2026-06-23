# Repository Architecture

This repo is a small modular monolith for market research, Lens chat, and
predictive trading experiments.

```text
apps/web/              React UI
src/trade_research/    Python package for API, data, storage, features, targets, chat, and jobs
src/trade_research/modeling/
                       Small home for reusable model code
experiments/           Exploratory model work
notebooks/             Original NSE/TSX exploration notebooks
data/                  Generated local datasets, gitignored
artifacts/             Generated model artifacts, gitignored
deploy/                Deployment scripts and proxy config
```

Keep production logic in `src/trade_research`. Use notebooks and `experiments/`
to explore, then move reusable code into the package when it becomes stable.

Current research data flow:

```text
ohlcv_daily
  -> daily_v1_ohlcv_technical_v1_0 features
  -> daily_v1_forward_returns_v1_0 targets
  -> daily_v1_factor_research outputs
  -> signals, models, and backtests
```

Features and targets are intentionally separate. Feature rows describe what was
known at date `T`; target rows describe outcomes after date `T`.
