from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.modeling.backtest import BacktestConfig, run_prediction_backtest
from trade_research.pipelines.base import PipelineRunResult


def run_prediction_backtest_v1_pipeline(
    predictions_path: Path,
    output_dir: Path | None = None,
    daily_returns_output: Path | None = None,
    equity_curve_output: Path | None = None,
    metrics_output: Path | None = None,
    summary_output: Path | None = None,
    config: BacktestConfig | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    root = output_dir or settings.data_dir / "processed/ml/backtests_v1"
    daily_path = daily_returns_output or root / "daily_portfolio_returns.csv"
    curve_path = equity_curve_output or root / "portfolio_equity_curve.csv"
    metrics_path = metrics_output or root / "backtest_metrics.json"
    summary_path = summary_output or root / "backtest_report.md"

    predictions = pd.read_parquet(predictions_path)
    result = run_prediction_backtest(predictions, config=config)

    daily_path.parent.mkdir(parents=True, exist_ok=True)
    curve_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    result.daily_returns.to_csv(daily_path, index=False)
    result.equity_curve.to_csv(curve_path, index=False)
    metrics_path.write_text(json.dumps(result.metrics, indent=2, default=_json_default) + "\n")
    summary_path.write_text(result.summary_md)

    status = "pass" if result.metrics["result_count"] > 0 else "warn"
    warnings = [] if status == "pass" else ["No backtest results generated."]
    return PipelineRunResult(
        name="prediction_backtest_v1",
        status=status,
        rows=len(result.daily_returns),
        artifacts={
            "daily_returns": daily_path,
            "equity_curve": curve_path,
            "metrics": metrics_path,
            "summary": summary_path,
        },
        metrics={
            "result_count": result.metrics["result_count"],
            "model_count": result.metrics["model_count"],
            "strategy": result.metrics["strategy"],
        },
        warnings=warnings,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
