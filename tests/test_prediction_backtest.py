from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from trade_research.modeling.backtest import BacktestConfig, run_prediction_backtest
from trade_research.pipelines.backtest import run_prediction_backtest_v1_pipeline


def test_prediction_backtest_computes_top_n_returns_and_costs() -> None:
    result = run_prediction_backtest(
        _predictions(days=3),
        config=BacktestConfig(top_n_values=(2,), transaction_cost_bps=10),
    )

    assert len(result.daily_returns) == 3
    first = result.daily_returns.iloc[0]
    assert first["selected_count"] == 2
    assert first["gross_return"] == 0.025
    assert first["turnover"] == 1.0
    assert first["transaction_cost"] == 0.001
    assert first["net_return"] == 0.024
    assert result.metrics["result_count"] == 1
    assert result.equity_curve["equity"].iloc[-1] > 1.0


def test_prediction_backtest_handles_multiple_models_and_top_n_values() -> None:
    predictions = pd.concat(
        [
            _predictions(days=3, model_id="model_a"),
            _predictions(days=3, model_id="model_b"),
        ],
        ignore_index=True,
    )

    result = run_prediction_backtest(
        predictions,
        config=BacktestConfig(top_n_values=(1, 2), transaction_cost_bps=0),
    )

    assert result.metrics["model_count"] == 2
    assert result.metrics["result_count"] == 4
    assert set(result.daily_returns["top_n"]) == {1, 2}
    assert "# Prediction Backtest v1" in result.summary_md


def test_prediction_backtest_pipeline_writes_artifacts(tmp_path: Path) -> None:
    predictions_path = tmp_path / "predictions.parquet"
    output_dir = tmp_path / "backtest"
    _predictions(days=3).to_parquet(predictions_path, index=False)

    result = run_prediction_backtest_v1_pipeline(
        predictions_path=predictions_path,
        output_dir=output_dir,
        config=BacktestConfig(top_n_values=(2,), transaction_cost_bps=10),
    )

    assert result.status == "pass"
    for path in result.artifacts.values():
        assert path.exists()
        assert output_dir in path.parents


def _predictions(days: int, model_id: str = "model_a") -> pd.DataFrame:
    rows = []
    for offset in range(days):
        current_date = date(2026, 1, 1) + timedelta(days=offset)
        for rank, symbol in enumerate(["AAA", "BBB", "CCC"], start=1):
            rows.append(
                {
                    "instrument_key": f"NSE_EQ|{symbol}",
                    "symbol": symbol,
                    "prediction_date": current_date,
                    "model_id": model_id,
                    "score": 10 - rank,
                    "rank": float(rank),
                    "realized_forward_ret_1d": {
                        "AAA": 0.03,
                        "BBB": 0.02,
                        "CCC": -0.01,
                    }[symbol],
                }
            )
    return pd.DataFrame(rows)
