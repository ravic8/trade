import json
from pathlib import Path

import pandas as pd

from trade_research.research.artifacts import ResearchArtifactReader
from trade_research.research.ml_artifacts import MLArtifactReader


def test_research_progress_reports_missing_artifacts(tmp_path: Path) -> None:
    payload = ResearchArtifactReader(tmp_path).progress()

    assert payload["overall_status"] == "warning"
    assert payload["missing_count"] > 0
    assert payload["steps"][0]["status"] == "missing"


def test_research_progress_reads_universe_and_instrument_metrics(tmp_path: Path) -> None:
    universe_path = tmp_path / "processed/universe"
    instrument_path = tmp_path / "processed/instruments"
    universe_path.mkdir(parents=True)
    instrument_path.mkdir(parents=True)
    (universe_path / "liquid_nse_stocks.csv").write_text("symbol\nABC\n")
    (universe_path / "liquid_nse_stock_audit.csv").write_text("symbol,status\nABC,ok\n")
    (universe_path / "liquid_nse_universe_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-06-18T18:50:24+00:00",
                "tickers_requested": 100,
                "tickers_with_data": 98,
                "output_rows": 42,
                "start_date": "2025-12-17",
                "end_date": "2026-06-18",
                "min_trading_days": 90,
                "min_avg_daily_turnover": 1000000000,
                "null_rows": 1,
                "duplicate_ticker_date_rows": 0,
            }
        )
    )
    (instrument_path / "upstox_instruments.parquet").write_text("")
    pd.DataFrame(
        [
            {
                "rows": 140865,
                "missing_instrument_key_rows": 0,
                "duplicate_instrument_key_rows": 0,
                "nse_equity_rows": 2424,
                "fetched_at": "2026-06-18T18:43:07+00:00",
            }
        ]
    ).to_csv(instrument_path / "upstox_instruments_audit.csv", index=False)

    payload = ResearchArtifactReader(tmp_path).progress()
    steps = {step["step_id"]: step for step in payload["steps"]}

    assert steps["step_0_universe"]["row_count"] == 42
    assert steps["step_0_universe"]["symbol_count"] == 42
    assert steps["step_0_universe"]["status"] == "warning"
    assert steps["step_1_0_instruments"]["row_count"] == 140865
    assert steps["step_1_0_instruments"]["symbol_count"] == 2424
    assert steps["step_1_0_instruments"]["detail_items"]


def test_research_factor_summary_reads_json(tmp_path: Path) -> None:
    summary_path = tmp_path / "processed/research/factors"
    summary_path.mkdir(parents=True)
    (summary_path / "daily_v1_factor_research_summary.json").write_text(
        json.dumps({"row_count": 10, "feature_count": 2})
    )

    payload = ResearchArtifactReader(tmp_path).factor_summary()

    assert payload["status"] == "done"
    assert payload["summary"]["row_count"] == 10


def test_research_factor_ic_filters_sorts_and_limits(tmp_path: Path) -> None:
    factor_path = tmp_path / "processed/research/factors"
    factor_path.mkdir(parents=True)
    pd.DataFrame(
        [
            {"feature": "ret_1d", "target": "forward_ret_20d", "mean_rank_ic": 0.1},
            {"feature": "ret_60d", "target": "forward_ret_20d", "mean_rank_ic": 0.3},
            {"feature": "atr_14", "target": "forward_ret_5d", "mean_rank_ic": 0.9},
        ]
    ).to_csv(factor_path / "daily_v1_factor_ic.csv", index=False)

    payload = ResearchArtifactReader(tmp_path).factor_ic(
        target="forward_ret_20d",
        sort="mean_rank_ic",
        direction="desc",
        limit=1,
    )

    assert payload["status"] == "done"
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["feature"] == "ret_60d"


def test_research_factor_ic_missing_response_keeps_query_metadata(tmp_path: Path) -> None:
    payload = ResearchArtifactReader(tmp_path).factor_ic(
        target="forward_ret_20d",
        sort="mean_ic",
        direction="asc",
        limit=10,
    )

    assert payload["status"] == "missing"
    assert payload["target"] == "forward_ret_20d"
    assert payload["sort"] == "mean_ic"
    assert payload["direction"] == "asc"
    assert payload["rows"] == []


def test_ml_artifact_reader_summarizes_models_and_backtests(tmp_path: Path) -> None:
    ml_path = tmp_path / "processed/ml"
    (ml_path / "baselines_v1").mkdir(parents=True)
    (ml_path / "backtests_v1/baselines").mkdir(parents=True)
    (ml_path / "ml_dataset_v1_summary.json").write_text(
        json.dumps(
            {
                "target_column": "forward_ret_1d",
                "coverage_policy": "static_full_history_100pct_coverage",
                "leakage_note": "static universe",
                "row_count": 100,
                "trainable_row_count": 80,
                "symbol_count": 5,
            }
        )
    )
    (ml_path / "walk_forward_v1").mkdir()
    (ml_path / "walk_forward_v1/walk_forward_summary.json").write_text(
        json.dumps({"fold_count": 3})
    )
    (ml_path / "baselines_v1/baseline_metrics.json").write_text(
        json.dumps(
            {
                "model_count": 1,
                "prediction_row_count": 10,
                "generated_at": "2026-06-28T00:00:00+00:00",
                "manifest": {"fold_count": 3},
                "models": [
                    {
                        "model_id": "momentum_1d",
                        "prediction_rows": 10,
                        "evaluated_rows": 10,
                        "top_10_average_return": 0.01,
                    }
                ],
            }
        )
    )
    (ml_path / "backtests_v1/baselines/backtest_metrics.json").write_text(
        json.dumps(
            {
                "model_count": 1,
                "result_count": 1,
                "results": [
                    {
                        "model_id": "momentum_1d",
                        "top_n": 5,
                        "day_count": 3,
                        "total_return": 0.2,
                    }
                ],
            }
        )
    )

    payload = MLArtifactReader(tmp_path).summary()
    metrics = MLArtifactReader(tmp_path).model_metrics()
    backtests = MLArtifactReader(tmp_path).backtests()

    assert payload["status"] == "done"
    assert payload["current_winner"]["model_id"] == "momentum_1d"
    assert metrics["rows"][0]["run_id"] == "baselines"
    assert backtests["rows"][0]["total_return"] == 0.2


def test_ml_artifact_reader_candidates_reads_prediction_parquet(tmp_path: Path) -> None:
    baseline_path = tmp_path / "processed/ml/baselines_v1"
    baseline_path.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "prediction_date": "2026-01-01",
                "instrument_key": "NSE_EQ|AAA",
                "symbol": "AAA",
                "model_id": "momentum_1d",
                "rank": 1.0,
                "score": 0.05,
                "realized_forward_ret_1d": 0.02,
            },
            {
                "prediction_date": "2026-01-01",
                "instrument_key": "NSE_EQ|BBB",
                "symbol": "BBB",
                "model_id": "momentum_1d",
                "rank": 6.0,
                "score": 0.01,
                "realized_forward_ret_1d": -0.01,
            },
        ]
    ).to_parquet(baseline_path / "baseline_predictions.parquet", index=False)

    payload = MLArtifactReader(tmp_path).candidates(model_id="momentum_1d", top_n=5)

    assert payload["status"] == "done"
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["symbol"] == "AAA"


def test_ml_artifact_reader_latest_candidates_groups_by_model(tmp_path: Path) -> None:
    baseline_path = tmp_path / "processed/ml/baselines_v1"
    baseline_path.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "prediction_date": "2026-01-01",
                "instrument_key": "NSE_EQ|OLD",
                "symbol": "OLD",
                "model_id": "momentum_1d",
                "rank": 1.0,
                "score": 0.9,
                "realized_forward_ret_1d": 0.01,
            },
            {
                "prediction_date": "2026-01-02",
                "instrument_key": "NSE_EQ|AAA",
                "symbol": "AAA",
                "model_id": "momentum_1d",
                "rank": 1.0,
                "score": 0.5,
                "realized_forward_ret_1d": None,
            },
            {
                "prediction_date": "2026-01-02",
                "instrument_key": "NSE_EQ|BBB",
                "symbol": "BBB",
                "model_id": "momentum_5d",
                "rank": 1.0,
                "score": 0.4,
                "realized_forward_ret_1d": None,
            },
        ]
    ).to_parquet(baseline_path / "baseline_predictions.parquet", index=False)

    payload = MLArtifactReader(tmp_path).latest_candidates(run="baselines", top_n=1)

    assert payload["status"] == "done"
    assert payload["prediction_date"] == "2026-01-02"
    assert payload["model_count"] == 2
    assert {model["model_id"] for model in payload["models"]} == {"momentum_1d", "momentum_5d"}


def test_ml_artifact_reader_robustness_computes_cost_sensitivity(tmp_path: Path) -> None:
    baseline_path = tmp_path / "processed/ml/baselines_v1"
    baseline_path.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "prediction_date": "2026-01-01",
                "instrument_key": "NSE_EQ|AAA",
                "symbol": "AAA",
                "model_id": "momentum_1d",
                "rank": 1.0,
                "score": 0.5,
                "realized_forward_ret_1d": 0.02,
            },
            {
                "prediction_date": "2026-01-02",
                "instrument_key": "NSE_EQ|AAA",
                "symbol": "AAA",
                "model_id": "momentum_1d",
                "rank": 1.0,
                "score": 0.4,
                "realized_forward_ret_1d": 0.01,
            },
        ]
    ).to_parquet(baseline_path / "baseline_predictions.parquet", index=False)

    payload = MLArtifactReader(tmp_path).robustness(
        group="baselines",
        model_id="momentum_1d",
        top_n=1,
        cost_bps_values=(0.0, 10.0),
    )

    assert payload["status"] == "done"
    assert [row["transaction_cost_bps"] for row in payload["cost_sensitivity"]] == [0.0, 10.0]
    zero_cost_return = payload["cost_sensitivity"][0]["total_return"]
    ten_bps_return = payload["cost_sensitivity"][1]["total_return"]
    assert zero_cost_return > ten_bps_return
