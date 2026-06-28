from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ArtifactRef:
    path: str
    kind: str
    required: bool = True


class ResearchArtifactReader:
    def __init__(self, data_dir: Path | str = "data") -> None:
        self.data_dir = Path(data_dir)

    def progress(self) -> dict[str, Any]:
        steps = [
            self._universe_step(),
            self._instrument_step(),
            self._mapping_step(),
            self._daily_ohlcv_step(),
            self._summary_step(
                step_id="step_2_0_features",
                title="Daily Technical Features",
                command="trade-research build-daily-features --store-db",
                summary_path="processed/features/daily_v1_ohlcv_technical_summary.json",
                artifacts=[
                    ArtifactRef("processed/features/daily_v1_ohlcv_technical.parquet", "parquet"),
                    ArtifactRef("processed/features/daily_v1_ohlcv_technical_audit.csv", "csv"),
                    ArtifactRef("processed/features/daily_v1_ohlcv_technical_summary.json", "json"),
                ],
                timescale_tables=["features_daily", "feature_runs", "feature_audits"],
                notes=["Features use only information available on or before date T."],
            ),
            self._summary_step(
                step_id="step_2_1_targets",
                title="Daily Forward Targets",
                command="trade-research build-daily-targets --store-db",
                summary_path="processed/targets/daily_v1_forward_returns_summary.json",
                artifacts=[
                    ArtifactRef("processed/targets/daily_v1_forward_returns.parquet", "parquet"),
                    ArtifactRef("processed/targets/daily_v1_forward_returns_audit.csv", "csv"),
                    ArtifactRef("processed/targets/daily_v1_forward_returns_summary.json", "json"),
                ],
                timescale_tables=["targets_daily", "target_runs", "target_audits"],
                notes=[
                    "Warnings are expected near latest dates when future windows are incomplete."
                ],
            ),
            self._summary_step(
                step_id="step_2_2_processed_validation",
                title="Processed Dataset Validation",
                command="trade-research validate-processed-datasets",
                summary_path="processed/validation/processed_dataset_validation_summary.json",
                artifacts=[
                    ArtifactRef("processed/validated/ohlcv_daily_validated.parquet", "parquet"),
                    ArtifactRef(
                        "processed/validation/processed_dataset_validation_summary.json",
                        "json",
                    ),
                    ArtifactRef(
                        "processed/validation/processed_dataset_validation_summary.md",
                        "md",
                    ),
                    ArtifactRef(
                        "processed/validation/daily_pipeline_stock_coverage.parquet",
                        "parquet",
                    ),
                    ArtifactRef(
                        "processed/validation/daily_pipeline_stock_coverage_windows.parquet",
                        "parquet",
                    ),
                ],
                timescale_tables=[],
                notes=["Validated parquet artifacts are the canonical inputs for ML v1."],
            ),
            self._ml_dataset_step(),
            self._walk_forward_step(),
            self._model_metrics_step(
                step_id="step_3_2_baseline_predictions",
                title="Baseline Predictions",
                command="trade-research run-baseline-predictions-v1",
                metrics_path="processed/ml/baselines_v1/baseline_metrics.json",
                artifacts=[
                    ArtifactRef(
                        "processed/ml/baselines_v1/baseline_predictions.parquet",
                        "parquet",
                    ),
                    ArtifactRef("processed/ml/baselines_v1/baseline_metrics.json", "json"),
                    ArtifactRef("processed/ml/baselines_v1/baseline_summary.md", "md"),
                ],
                notes=["Simple momentum baselines are the current bar to beat."],
            ),
            self._model_metrics_step(
                step_id="step_3_3_lightgbm_predictions",
                title="LightGBM Predictions",
                command="trade-research run-lightgbm-predictions-v1",
                metrics_path="processed/ml/lightgbm_v1/lightgbm_metrics.json",
                artifacts=[
                    ArtifactRef("processed/ml/lightgbm_v1/lightgbm_predictions.parquet", "parquet"),
                    ArtifactRef("processed/ml/lightgbm_v1/lightgbm_metrics.json", "json"),
                    ArtifactRef("processed/ml/lightgbm_v1/lightgbm_summary.md", "md"),
                ],
                notes=["Smoke-run LightGBM results are available for comparison."],
            ),
            self._backtest_step(
                step_id="step_3_4_baseline_backtests",
                title="Baseline Backtests",
                command=(
                    "trade-research run-prediction-backtest-v1 --predictions "
                    "data/processed/ml/baselines_v1/baseline_predictions.parquet"
                ),
                metrics_path="processed/ml/backtests_v1/baselines/backtest_metrics.json",
                artifact_dir="processed/ml/backtests_v1/baselines",
                notes=["Long-only top-N equal-weight daily backtests with transaction costs."],
            ),
            self._backtest_step(
                step_id="step_3_5_lightgbm_backtests",
                title="LightGBM Backtests",
                command=(
                    "trade-research run-prediction-backtest-v1 --predictions "
                    "data/processed/ml/lightgbm_v1/lightgbm_predictions.parquet"
                ),
                metrics_path="processed/ml/backtests_v1/lightgbm/backtest_metrics.json",
                artifact_dir="processed/ml/backtests_v1/lightgbm",
                notes=[
                    "LightGBM backtests are shown separately because the smoke window is shorter."
                ],
            ),
            self._latest_predictions_step(),
            self._summary_step(
                step_id="step_4_0_factor_research",
                title="Factor Research Outputs",
                command="trade-research build-factor-research",
                summary_path="processed/research/factors/daily_v1_factor_research_summary.json",
                artifacts=[
                    ArtifactRef("processed/research/factors/daily_v1_factor_ic.csv", "csv"),
                    ArtifactRef("processed/research/factors/daily_v1_factor_quantiles.csv", "csv"),
                    ArtifactRef("processed/research/factors/daily_v1_factor_hit_rates.csv", "csv"),
                    ArtifactRef(
                        "processed/research/factors/daily_v1_factor_monthly_stability.csv",
                        "csv",
                    ),
                    ArtifactRef(
                        "processed/research/factors/daily_v1_factor_research_summary.json",
                        "json",
                    ),
                ],
                timescale_tables=[],
                notes=["First-pass evidence for feature review before signals or backtests."],
            ),
        ]
        completed = sum(1 for step in steps if step["status"] in {"done", "warning"})
        warnings = sum(1 for step in steps if step["status"] == "warning")
        missing = sum(1 for step in steps if step["status"] == "missing")
        overall_status = "done" if missing == 0 else "warning"
        return {
            "overall_status": overall_status,
            "step_count": len(steps),
            "completed_count": completed,
            "warning_count": warnings,
            "missing_count": missing,
            "steps": steps,
        }

    def factor_summary(self) -> dict[str, Any]:
        path = self.data_dir / "processed/research/factors/daily_v1_factor_research_summary.json"
        summary = self._read_json(path)
        if summary is None:
            return {"status": "missing", "path": str(path), "summary": None}
        return {"status": "done", "path": str(path), "summary": summary}

    def factor_ic(
        self,
        target: str | None = None,
        sort: str = "mean_rank_ic",
        direction: str = "desc",
        limit: int = 100,
    ) -> dict[str, Any]:
        path = self.data_dir / "processed/research/factors/daily_v1_factor_ic.csv"
        if not path.exists():
            return {
                "status": "missing",
                "path": str(path),
                "target": target,
                "sort": sort,
                "direction": direction,
                "rows": [],
            }
        frame = pd.read_csv(path)
        if target:
            frame = frame[frame["target"].eq(target)]
        if sort in frame.columns:
            frame = frame.sort_values(
                sort,
                ascending=direction.lower() == "asc",
                na_position="last",
            )
        frame = frame.head(max(limit, 0))
        return {
            "status": "done",
            "path": str(path),
            "target": target,
            "sort": sort,
            "direction": direction,
            "rows": frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records"),
        }

    def _universe_step(self) -> dict[str, Any]:
        summary_path = self.data_dir / "processed/universe/liquid_nse_universe_summary.json"
        step = self._base_step(
            step_id="step_0_universe",
            title="Liquid NSE Universe",
            command=(
                "python scripts/select_liquid_nse_universe.py "
                "--min-avg-daily-turnover 1000000000 --top-n 1000"
            ),
            artifacts=[
                ArtifactRef("processed/universe/liquid_nse_stocks.csv", "csv"),
                ArtifactRef("processed/universe/liquid_nse_stock_audit.csv", "csv"),
                ArtifactRef("processed/universe/liquid_nse_universe_summary.json", "json"),
            ],
            timescale_tables=[],
            notes=["Core liquid universe uses six-month ADT >= Rs 100 crore/day."],
        )
        summary = self._read_json(summary_path)
        if summary is None:
            return step

        output_rows = summary.get("output_rows")
        step["row_count"] = output_rows
        step["symbol_count"] = output_rows
        step["date_min"] = summary.get("start_date")
        step["date_max"] = summary.get("end_date")
        step["warning_count"] = int(summary.get("null_rows", 0) or 0)
        step["failed_count"] = int(summary.get("duplicate_ticker_date_rows", 0) or 0)
        step["last_generated_at"] = summary.get("generated_at")
        step["warning_explanation"] = (
            "Input rows with null market data are tracked in the audit before selection."
            if step["warning_count"]
            else None
        )
        step["detail_items"] = [
            {"label": "Requested tickers", "value": summary.get("tickers_requested")},
            {"label": "Tickers with data", "value": summary.get("tickers_with_data")},
            {"label": "Output rows", "value": summary.get("output_rows")},
            {"label": "Min trading days", "value": summary.get("min_trading_days")},
            {
                "label": "Min avg daily turnover",
                "value": self._format_currency(summary.get("min_avg_daily_turnover")),
            },
        ]
        return self._refresh_step_status(step)

    def _instrument_step(self) -> dict[str, Any]:
        audit_path = self.data_dir / "processed/instruments/upstox_instruments_audit.csv"
        step = self._base_step(
            step_id="step_1_0_instruments",
            title="Upstox Instrument Master",
            command="trade-research fetch-upstox-instruments",
            artifacts=[
                ArtifactRef("processed/instruments/upstox_instruments.parquet", "parquet"),
                ArtifactRef("processed/instruments/upstox_instruments_audit.csv", "csv"),
            ],
            timescale_tables=["provider_instruments"],
            notes=["Instrument master comes from the public Upstox endpoint."],
        )
        if not audit_path.exists():
            return step
        audit = pd.read_csv(audit_path)
        if audit.empty:
            return self._refresh_step_status(step)
        row = audit.iloc[0]
        missing_keys = int(row.get("missing_instrument_key_rows", 0) or 0)
        duplicate_keys = int(row.get("duplicate_instrument_key_rows", 0) or 0)
        nse_equity_rows = int(row.get("nse_equity_rows", 0) or 0)
        fetched_at = str(row.get("fetched_at")) if row.get("fetched_at") is not None else None
        step["row_count"] = int(row.get("rows", 0) or 0)
        step["symbol_count"] = nse_equity_rows
        step["warning_count"] = missing_keys + duplicate_keys
        step["last_generated_at"] = fetched_at
        step["warning_explanation"] = (
            "Instrument-key gaps or duplicates need review before mapping."
            if step["warning_count"]
            else None
        )
        step["detail_items"] = [
            {"label": "NSE equity rows", "value": nse_equity_rows},
            {"label": "Missing keys", "value": missing_keys},
            {"label": "Duplicate keys", "value": duplicate_keys},
            {"label": "Fetched at", "value": fetched_at},
        ]
        return self._refresh_step_status(step)

    def _mapping_step(self) -> dict[str, Any]:
        mapping_path = self.data_dir / "processed/universe/liquid_nse_upstox_mapping.csv"
        unmatched_path = self.data_dir / "processed/universe/liquid_nse_upstox_unmatched.csv"
        artifacts = [
            ArtifactRef("processed/universe/liquid_nse_upstox_mapping.csv", "csv"),
            ArtifactRef("processed/universe/liquid_nse_upstox_unmatched.csv", "csv"),
        ]
        step = self._base_step(
            step_id="step_1_1_mapping",
            title="Liquid Universe To Upstox Mapping",
            command="trade-research map-liquid-nse-upstox",
            artifacts=artifacts,
            timescale_tables=["tradable_universes", "tradable_universe_members"],
            notes=["Unmatched symbols remain visible for manual review."],
        )
        if mapping_path.exists():
            mapping = pd.read_csv(mapping_path)
            step["row_count"] = int(len(mapping))
            step["symbol_count"] = int(mapping["symbol"].nunique()) if "symbol" in mapping else None
        if unmatched_path.exists():
            unmatched = pd.read_csv(unmatched_path)
            step["warning_count"] = int(len(unmatched))
            step["warning_explanation"] = (
                "Unmatched liquid-universe symbols are kept visible for manual review."
                if step["warning_count"]
                else None
            )
        step["detail_items"] = [
            {"label": "Mapped rows", "value": step["row_count"]},
            {"label": "Unmatched rows", "value": step["warning_count"]},
        ]
        return self._refresh_step_status(step)

    def _daily_ohlcv_step(self) -> dict[str, Any]:
        audit_path = self.data_dir / "processed/equities/nse_daily_ohlcv_upstox_audit.csv"
        parquet_path = self.data_dir / "processed/equities/nse_daily_ohlcv_upstox.parquet"
        step = self._base_step(
            step_id="step_1_2_daily_ohlcv",
            title="Daily OHLCV",
            command="trade-research fetch-upstox-nse-daily",
            artifacts=[
                ArtifactRef("processed/equities/nse_daily_ohlcv_upstox.parquet", "parquet"),
                ArtifactRef("processed/equities/nse_daily_ohlcv_upstox_audit.csv", "csv"),
                ArtifactRef("processed/equities/nse_daily_ohlcv_upstox_failures.csv", "csv"),
                ArtifactRef("processed/equities/nse_daily_ohlcv_upstox_skipped.csv", "csv"),
                ArtifactRef(
                    "processed/equities/nse_daily_ohlcv_upstox_fetch_coverage.csv",
                    "csv",
                ),
            ],
            timescale_tables=[
                "ohlcv_daily",
                "data_quality_audits",
                "daily_ohlcv_fetch_coverage",
            ],
            notes=[
                "Daily fetch is incremental by default with a settlement lag.",
                "Fetch coverage is run-scoped and will drive the future retry pipeline.",
            ],
        )
        if parquet_path.exists():
            frame = pd.read_parquet(parquet_path)
            frame = frame.rename(
                columns={
                    "instrument_key": "InstrumentKey",
                    "symbol": "Symbol",
                    "date": "Date",
                }
            )
            step["row_count"] = int(len(frame))
            step["symbol_count"] = int(frame["Symbol"].nunique()) if "Symbol" in frame else None
            if "Date" in frame and not frame.empty:
                dates = pd.to_datetime(frame["Date"]).dt.date
                step["date_min"] = str(dates.min())
                step["date_max"] = str(dates.max())
        if audit_path.exists():
            audit = pd.read_csv(audit_path)
            if "status" in audit:
                step["warning_count"] = int(audit["status"].eq("warning").sum())
                step["failed_count"] = int(audit["status"].eq("failed").sum())
                step["warning_explanation"] = (
                    "Instrument-level fetch warnings are retained in the OHLCV audit."
                    if step["warning_count"]
                    else None
                )
                step["detail_items"] = [
                    {"label": "Audit rows", "value": len(audit)},
                    {"label": "Warnings", "value": step["warning_count"]},
                    {"label": "Failures", "value": step["failed_count"]},
                ]
        return self._refresh_step_status(step)

    def _ml_dataset_step(self) -> dict[str, Any]:
        step = self._base_step(
            step_id="step_3_0_ml_dataset_v1",
            title="ML Dataset v1",
            command="trade-research build-ml-dataset-v1",
            artifacts=[
                ArtifactRef("processed/ml/ml_dataset_v1.parquet", "parquet"),
                ArtifactRef("processed/ml/ml_dataset_v1_summary.json", "json"),
                ArtifactRef("processed/ml/ml_dataset_v1_exclusions.csv", "csv"),
                ArtifactRef("processed/ml/ml_dataset_v1_feature_columns.json", "json"),
                ArtifactRef("processed/ml/ml_dataset_v1_leakage_checks.json", "json"),
            ],
            timescale_tables=[],
            notes=[
                "Target is forward_ret_1d.",
                "Universe is static full-history 100% coverage for research v1.",
            ],
        )
        summary = self._read_json(self.data_dir / "processed/ml/ml_dataset_v1_summary.json")
        if summary is None:
            return step
        step["row_count"] = summary.get("row_count")
        step["symbol_count"] = summary.get("symbol_count")
        step["date_min"] = summary.get("date_min")
        step["date_max"] = summary.get("date_max")
        step["warning_count"] = 0 if summary.get("leakage_checks_passed") else 1
        step["last_generated_at"] = summary.get("generated_at")
        step["warning_explanation"] = (
            None
            if summary.get("leakage_checks_passed")
            else "ML dataset leakage checks did not pass."
        )
        step["detail_items"] = [
            {"label": "Trainable rows", "value": summary.get("trainable_row_count")},
            {"label": "Eligible symbols", "value": summary.get("trainable_symbol_count")},
            {"label": "Excluded symbols", "value": summary.get("excluded_symbol_count")},
            {"label": "Feature columns", "value": summary.get("feature_column_count")},
            {"label": "Coverage policy", "value": summary.get("coverage_policy")},
            {"label": "Leakage checks passed", "value": summary.get("leakage_checks_passed")},
        ]
        return self._refresh_step_status(step)

    def _latest_predictions_step(self) -> dict[str, Any]:
        step = self._base_step(
            step_id="step_3_6_latest_predictions",
            title="Latest Prediction Layer",
            command="trade-research run-latest-predictions-v1",
            artifacts=[
                ArtifactRef(
                    "processed/ml/latest_predictions_v1/latest_predictions.parquet",
                    "parquet",
                ),
                ArtifactRef("processed/ml/latest_predictions_v1/latest_candidates.json", "json"),
                ArtifactRef(
                    "processed/ml/latest_predictions_v1/latest_predictions_summary.json",
                    "json",
                ),
                ArtifactRef(
                    "processed/ml/latest_predictions_v1/latest_predictions_report.md",
                    "md",
                ),
            ],
            timescale_tables=[],
            notes=[
                "Current inference artifact for the latest feature-complete date.",
                "This layer is separate from walk-forward backtest predictions.",
            ],
        )
        summary = self._read_json(
            self.data_dir / "processed/ml/latest_predictions_v1/latest_predictions_summary.json"
        )
        if summary is None:
            return step
        step["row_count"] = summary.get("prediction_row_count")
        step["symbol_count"] = summary.get("prediction_symbol_count")
        step["date_min"] = summary.get("prediction_date")
        step["date_max"] = summary.get("prediction_date")
        step["last_generated_at"] = summary.get("generated_at")
        step["detail_items"] = [
            {"label": "Prediction date", "value": summary.get("prediction_date")},
            {"label": "Runs", "value": summary.get("run_count")},
            {"label": "Models", "value": summary.get("model_count")},
            {"label": "Prediction symbols", "value": summary.get("prediction_symbol_count")},
            {"label": "Train end", "value": summary.get("train_end_date")},
            {"label": "Validation end", "value": summary.get("validation_end_date")},
        ]
        return self._refresh_step_status(step)

    def _walk_forward_step(self) -> dict[str, Any]:
        step = self._base_step(
            step_id="step_3_1_walk_forward_folds",
            title="Walk-Forward Folds",
            command="trade-research build-walk-forward-folds-v1",
            artifacts=[
                ArtifactRef("processed/ml/walk_forward_v1/walk_forward_folds.parquet", "parquet"),
                ArtifactRef("processed/ml/walk_forward_v1/walk_forward_summary.json", "json"),
            ],
            timescale_tables=[],
            notes=["Strict default config keeps train and validation windows before prediction."],
        )
        summary = self._read_json(
            self.data_dir / "processed/ml/walk_forward_v1/walk_forward_summary.json"
        )
        if summary is None:
            return step
        step["row_count"] = summary.get("trainable_row_count")
        step["symbol_count"] = None
        step["date_min"] = summary.get("first_prediction_date")
        step["date_max"] = summary.get("last_prediction_date")
        step["warning_count"] = 0 if summary.get("leakage_checks_passed") else 1
        step["last_generated_at"] = summary.get("generated_at")
        step["warning_explanation"] = (
            None
            if summary.get("leakage_checks_passed")
            else "Walk-forward leakage checks did not pass."
        )
        step["detail_items"] = [
            {"label": "Folds", "value": summary.get("fold_count")},
            {"label": "Candidate dates", "value": summary.get("candidate_date_count")},
            {"label": "Skipped candidates", "value": summary.get("skipped_candidate_count")},
            {"label": "Min train days", "value": summary.get("config", {}).get("min_train_days")},
            {"label": "Validation days", "value": summary.get("config", {}).get("validation_days")},
            {"label": "Leakage checks passed", "value": summary.get("leakage_checks_passed")},
        ]
        return self._refresh_step_status(step)

    def _model_metrics_step(
        self,
        step_id: str,
        title: str,
        command: str,
        metrics_path: str,
        artifacts: list[ArtifactRef],
        notes: list[str],
    ) -> dict[str, Any]:
        step = self._base_step(
            step_id=step_id,
            title=title,
            command=command,
            artifacts=artifacts,
            timescale_tables=[],
            notes=notes,
        )
        metrics = self._read_json(self.data_dir / metrics_path)
        if metrics is None:
            return step
        best = self._best_prediction_metric(metrics)
        step["row_count"] = metrics.get("prediction_row_count")
        step["symbol_count"] = metrics.get("model_count")
        step["date_min"] = metrics.get("manifest", {}).get("first_prediction_date")
        step["date_max"] = metrics.get("manifest", {}).get("last_prediction_date")
        step["last_generated_at"] = metrics.get("generated_at")
        step["detail_items"] = [
            {"label": "Models", "value": metrics.get("model_count")},
            {"label": "Prediction rows", "value": metrics.get("prediction_row_count")},
            {"label": "Folds", "value": metrics.get("manifest", {}).get("fold_count")},
            {
                "label": "Best top-10 model",
                "value": best.get("model_id") if best else None,
            },
            {
                "label": "Best top-10 avg return",
                "value": best.get("top_10_average_return") if best else None,
            },
        ]
        return self._refresh_step_status(step)

    def _backtest_step(
        self,
        step_id: str,
        title: str,
        command: str,
        metrics_path: str,
        artifact_dir: str,
        notes: list[str],
    ) -> dict[str, Any]:
        step = self._base_step(
            step_id=step_id,
            title=title,
            command=command,
            artifacts=[
                ArtifactRef(f"{artifact_dir}/daily_portfolio_returns.csv", "csv"),
                ArtifactRef(f"{artifact_dir}/portfolio_equity_curve.csv", "csv"),
                ArtifactRef(f"{artifact_dir}/backtest_metrics.json", "json"),
                ArtifactRef(f"{artifact_dir}/backtest_report.md", "md"),
            ],
            timescale_tables=[],
            notes=notes,
        )
        metrics = self._read_json(self.data_dir / metrics_path)
        if metrics is None:
            return step
        best = self._best_backtest_metric(metrics)
        step["row_count"] = metrics.get("result_count")
        step["symbol_count"] = metrics.get("model_count")
        step["last_generated_at"] = metrics.get("generated_at")
        step["detail_items"] = [
            {"label": "Strategy", "value": metrics.get("strategy")},
            {"label": "Models", "value": metrics.get("model_count")},
            {"label": "Results", "value": metrics.get("result_count")},
            {
                "label": "Transaction cost bps",
                "value": metrics.get("config", {}).get("transaction_cost_bps"),
            },
            {"label": "Best model", "value": best.get("model_id") if best else None},
            {"label": "Best top N", "value": best.get("top_n") if best else None},
            {"label": "Best total return", "value": best.get("total_return") if best else None},
            {"label": "Best Sharpe", "value": best.get("sharpe_ratio") if best else None},
        ]
        return self._refresh_step_status(step)

    def _summary_step(
        self,
        step_id: str,
        title: str,
        command: str,
        summary_path: str,
        artifacts: list[ArtifactRef],
        timescale_tables: list[str],
        notes: list[str],
    ) -> dict[str, Any]:
        step = self._base_step(
            step_id=step_id,
            title=title,
            command=command,
            artifacts=artifacts,
            timescale_tables=timescale_tables,
            notes=notes,
        )
        summary = self._read_json(self.data_dir / summary_path)
        if summary is None:
            return step
        step["row_count"] = summary.get("row_count")
        step["symbol_count"] = summary.get("symbol_count")
        step["date_min"] = summary.get("date_min")
        step["date_max"] = summary.get("date_max")
        step["warning_count"] = summary.get("warning_rows", 0)
        step["failed_count"] = summary.get("failed_rows", 0)
        step["last_generated_at"] = summary.get("generated_at")
        step["warning_explanation"] = self._summary_warning_explanation(step_id, step)
        step["detail_items"] = self._summary_detail_items(summary)
        return self._refresh_step_status(step)

    def _base_step(
        self,
        step_id: str,
        title: str,
        command: str,
        artifacts: list[ArtifactRef],
        timescale_tables: list[str],
        notes: list[str],
    ) -> dict[str, Any]:
        artifact_rows = [self._artifact_status(artifact) for artifact in artifacts]
        required_missing = any(
            row["required"] and row["status"] == "missing" for row in artifact_rows
        )
        return {
            "step_id": step_id,
            "title": title,
            "status": "missing" if required_missing else "done",
            "row_count": None,
            "symbol_count": None,
            "date_min": None,
            "date_max": None,
            "warning_count": 0,
            "failed_count": 0,
            "last_generated_at": None,
            "command": command,
            "timescale_tables": timescale_tables,
            "artifacts": artifact_rows,
            "notes": notes,
            "warning_explanation": None,
            "detail_items": [],
        }

    def _artifact_status(self, artifact: ArtifactRef) -> dict[str, Any]:
        path = self.data_dir / artifact.path
        return {
            "path": str(path),
            "kind": artifact.kind,
            "required": artifact.required,
            "status": "present" if path.exists() else "missing",
        }

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        return json.loads(path.read_text())

    @staticmethod
    def _refresh_step_status(step: dict[str, Any]) -> dict[str, Any]:
        if step["status"] == "missing":
            return step
        if int(step.get("failed_count") or 0) > 0 or int(step.get("warning_count") or 0) > 0:
            step["status"] = "warning"
        else:
            step["status"] = "done"
        return step

    @staticmethod
    def _format_currency(value: Any) -> str | None:
        if value is None:
            return None
        return f"Rs {float(value):,.0f}"

    @staticmethod
    def _summary_warning_explanation(step_id: str, step: dict[str, Any]) -> str | None:
        if int(step.get("warning_count") or 0) == 0:
            return None
        if step_id == "step_2_0_features":
            return "Feature warnings are mostly rolling-window warmup rows and review flags."
        if step_id == "step_2_1_targets":
            return "Target warnings are expected where future return windows are incomplete."
        return None

    @staticmethod
    def _summary_detail_items(summary: dict[str, Any]) -> list[dict[str, Any]]:
        labels = {
            "dataset_name": "Dataset",
            "feature_version": "Feature version",
            "target_version": "Target version",
            "feature_count": "Features",
            "return_target_count": "Return targets",
            "quantile_count": "Quantiles",
            "passed_rows": "Passed rows",
            "warning_rows": "Warning rows",
            "failed_rows": "Failed rows",
            "invalid_ohlcv_rows_excluded": "Invalid OHLCV excluded",
            "ic_rows": "IC rows",
            "quantile_rows": "Quantile rows",
            "hit_rate_rows": "Hit-rate rows",
            "monthly_stability_rows": "Monthly stability rows",
            "generated_at": "Generated at",
        }
        return [
            {"label": label, "value": summary[key]}
            for key, label in labels.items()
            if key in summary
        ]

    @staticmethod
    def _best_prediction_metric(metrics: dict[str, Any]) -> dict[str, Any] | None:
        rows = [
            row
            for row in metrics.get("models", [])
            if row.get("top_10_average_return") is not None
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: row.get("top_10_average_return") or float("-inf"))

    @staticmethod
    def _best_backtest_metric(metrics: dict[str, Any]) -> dict[str, Any] | None:
        rows = [
            row
            for row in metrics.get("results", [])
            if row.get("total_return") is not None
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: row.get("total_return") or float("-inf"))
