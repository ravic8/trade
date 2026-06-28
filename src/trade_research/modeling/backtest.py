from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class BacktestConfig:
    top_n_values: tuple[int, ...] = (5, 10, 20)
    transaction_cost_bps: float = 10.0
    starting_equity: float = 1.0
    trading_days_per_year: int = 252


@dataclass(frozen=True)
class BacktestRun:
    daily_returns: pd.DataFrame
    equity_curve: pd.DataFrame
    metrics: dict[str, Any]
    summary_md: str


def run_prediction_backtest(
    predictions: pd.DataFrame,
    config: BacktestConfig | None = None,
) -> BacktestRun:
    cfg = config or BacktestConfig()
    prepared = predictions.copy()
    prepared["prediction_date"] = pd.to_datetime(
        prepared["prediction_date"],
        errors="coerce",
    ).dt.date
    prepared = prepared[prepared["realized_forward_ret_1d"].notna()].copy()

    daily_frames = []
    for top_n in cfg.top_n_values:
        daily_frames.append(_daily_returns_for_top_n(prepared, top_n, cfg))
    daily_returns = (
        pd.concat(daily_frames, ignore_index=True)
        if daily_frames
        else _empty_daily_returns()
    )
    equity_curve = _equity_curve(daily_returns, cfg)
    metrics = _metrics(daily_returns, equity_curve, cfg)
    return BacktestRun(
        daily_returns=daily_returns,
        equity_curve=equity_curve,
        metrics=metrics,
        summary_md=_summary_markdown(metrics),
    )


def _daily_returns_for_top_n(
    predictions: pd.DataFrame,
    top_n: int,
    config: BacktestConfig,
) -> pd.DataFrame:
    rows = []
    previous_holdings: dict[str, set[str]] = {}
    for (model_id, prediction_date), group in predictions.groupby(
        ["model_id", "prediction_date"],
        sort=True,
    ):
        selected = group.sort_values(["rank", "symbol"]).head(top_n).copy()
        holdings = set(selected["instrument_key"].astype(str))
        prior = previous_holdings.get(str(model_id), set())
        turnover = _turnover(prior, holdings, top_n)
        gross_return = (
            float(selected["realized_forward_ret_1d"].mean())
            if not selected.empty
            else 0.0
        )
        cost = turnover * (config.transaction_cost_bps / 10_000)
        rows.append(
            {
                "model_id": model_id,
                "prediction_date": prediction_date,
                "top_n": top_n,
                "selected_count": int(len(selected)),
                "gross_return": gross_return,
                "turnover": turnover,
                "transaction_cost": cost,
                "net_return": gross_return - cost,
            }
        )
        previous_holdings[str(model_id)] = holdings
    return pd.DataFrame(rows)


def _turnover(previous: set[str], current: set[str], top_n: int) -> float:
    if not current:
        return 0.0
    if not previous:
        return 1.0
    changed = len(current.symmetric_difference(previous))
    return min(1.0, changed / max(1, top_n * 2))


def _equity_curve(daily_returns: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    if daily_returns.empty:
        return pd.DataFrame(
            columns=["model_id", "top_n", "prediction_date", "equity", "drawdown"]
        )
    frames = []
    for _, group in daily_returns.groupby(["model_id", "top_n"], sort=True):
        ordered = group.sort_values("prediction_date").copy()
        ordered["equity"] = config.starting_equity * (1.0 + ordered["net_return"]).cumprod()
        peak = ordered["equity"].cummax()
        ordered["drawdown"] = ordered["equity"] / peak - 1.0
        frames.append(
            ordered[
                [
                    "model_id",
                    "top_n",
                    "prediction_date",
                    "equity",
                    "drawdown",
                ]
            ]
        )
    return pd.concat(frames, ignore_index=True)


def _metrics(
    daily_returns: pd.DataFrame,
    equity_curve: pd.DataFrame,
    config: BacktestConfig,
) -> dict[str, Any]:
    rows = []
    for (model_id, top_n), group in daily_returns.groupby(["model_id", "top_n"], sort=True):
        curve = equity_curve[
            equity_curve["model_id"].eq(model_id) & equity_curve["top_n"].eq(top_n)
        ]
        net = group["net_return"]
        gross = group["gross_return"]
        total_return = _last_or_none(curve["equity"]) - config.starting_equity
        volatility = float(net.std(ddof=0)) if len(net) else 0.0
        rows.append(
            {
                "model_id": model_id,
                "top_n": int(top_n),
                "day_count": int(len(group)),
                "total_return": _nullable_float(total_return),
                "average_daily_gross_return": _nullable_float(gross.mean()),
                "average_daily_net_return": _nullable_float(net.mean()),
                "annualized_return": _annualized_return(total_return, len(group), config),
                "annualized_volatility": volatility * np.sqrt(config.trading_days_per_year),
                "sharpe_ratio": _sharpe(net, config),
                "max_drawdown": _nullable_float(curve["drawdown"].min()),
                "win_rate": _nullable_float(net.gt(0).mean()),
                "average_turnover": _nullable_float(group["turnover"].mean()),
                "total_transaction_cost": _nullable_float(group["transaction_cost"].sum()),
                "best_day": _nullable_float(net.max()),
                "worst_day": _nullable_float(net.min()),
                "profit_factor": _profit_factor(net),
            }
        )
    return {
        "artifact_name": "prediction_backtest_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "config": asdict(config),
        "strategy": "long_top_n_equal_weight_daily_rebalanced",
        "model_count": int(daily_returns["model_id"].nunique()) if not daily_returns.empty else 0,
        "result_count": int(len(rows)),
        "results": rows,
    }


def _summary_markdown(metrics: dict[str, Any]) -> str:
    lines = [
        "# Prediction Backtest v1",
        "",
        f"Generated at: `{metrics['generated_at']}`",
        f"Strategy: `{metrics['strategy']}`",
        "",
        "| Model | Top N | Days | Total Return | Sharpe | Max Drawdown | Win Rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in metrics["results"]:
        lines.append(
            "| {model_id} | {top_n} | {day_count} | {total_return} | "
            "{sharpe_ratio} | {max_drawdown} | {win_rate} |".format(
                model_id=row["model_id"],
                top_n=row["top_n"],
                day_count=row["day_count"],
                total_return=_format_metric(row["total_return"]),
                sharpe_ratio=_format_metric(row["sharpe_ratio"]),
                max_drawdown=_format_metric(row["max_drawdown"]),
                win_rate=_format_metric(row["win_rate"]),
            )
        )
    lines.append("")
    return "\n".join(lines)


def _annualized_return(
    total_return: float | None,
    day_count: int,
    config: BacktestConfig,
) -> float | None:
    if total_return is None or day_count <= 0:
        return None
    ending = 1.0 + total_return
    if ending <= 0:
        return -1.0
    return float(ending ** (config.trading_days_per_year / day_count) - 1.0)


def _sharpe(returns: pd.Series, config: BacktestConfig) -> float | None:
    if returns.empty:
        return None
    std = returns.std(ddof=0)
    if pd.isna(std) or std == 0:
        return None
    return float((returns.mean() / std) * np.sqrt(config.trading_days_per_year))


def _profit_factor(returns: pd.Series) -> float | None:
    gains = returns[returns.gt(0)].sum()
    losses = returns[returns.lt(0)].sum()
    if losses == 0:
        return None
    return float(gains / abs(losses))


def _last_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return None
    return float(series.iloc[-1])


def _nullable_float(value: object) -> float | None:
    return None if value is None or pd.isna(value) else float(value)


def _format_metric(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.6f}"


def _empty_daily_returns() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "model_id",
            "prediction_date",
            "top_n",
            "selected_count",
            "gross_return",
            "turnover",
            "transaction_cost",
            "net_return",
        ]
    )
