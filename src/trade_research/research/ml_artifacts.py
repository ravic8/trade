from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trade_research.modeling.backtest import BacktestConfig, run_prediction_backtest


class MLArtifactReader:
    def __init__(self, data_dir: Path | str = "data") -> None:
        self.data_dir = Path(data_dir)
        self.ml_dir = self.data_dir / "processed/ml"

    def summary(self) -> dict[str, Any]:
        dataset_path = self.ml_dir / "ml_dataset_v1_summary.json"
        walk_forward_path = self.ml_dir / "walk_forward_v1/walk_forward_summary.json"
        baseline_path = self.ml_dir / "baselines_v1/baseline_metrics.json"
        lightgbm_path = self.ml_dir / "lightgbm_v1/lightgbm_metrics.json"
        baseline_backtest_path = self.ml_dir / "backtests_v1/baselines/backtest_metrics.json"
        lightgbm_backtest_path = self.ml_dir / "backtests_v1/lightgbm/backtest_metrics.json"

        dataset = self._read_json(dataset_path)
        walk_forward = self._read_json(walk_forward_path)
        baseline_metrics = self._read_json(baseline_path)
        lightgbm_metrics = self._read_json(lightgbm_path)
        baseline_backtest = self._read_json(baseline_backtest_path)
        lightgbm_backtest = self._read_json(lightgbm_backtest_path)

        winner = self._best_backtest_result([baseline_backtest, lightgbm_backtest])
        status = "done" if dataset is not None else "missing"
        return {
            "status": status,
            "paths": {
                "dataset": str(dataset_path),
                "walk_forward": str(walk_forward_path),
                "baseline_metrics": str(baseline_path),
                "lightgbm_metrics": str(lightgbm_path),
                "baseline_backtest": str(baseline_backtest_path),
                "lightgbm_backtest": str(lightgbm_backtest_path),
            },
            "dataset": dataset,
            "walk_forward": walk_forward,
            "model_runs": [
                self._model_run_summary("baselines", baseline_metrics, baseline_backtest),
                self._model_run_summary("lightgbm", lightgbm_metrics, lightgbm_backtest),
            ],
            "current_winner": winner,
            "assumptions": {
                "target": dataset.get("target_column") if dataset else "forward_ret_1d",
                "universe": dataset.get("coverage_policy")
                if dataset
                else "static_full_history_100pct_coverage",
                "evaluation": "leakage-aware walk-forward",
                "strategy": "long_top_n_equal_weight_daily_rebalanced",
                "caveat": (
                    dataset.get("leakage_note")
                    if dataset
                    else "Static research universe; later replace with point-in-time coverage."
                ),
            },
        }

    def model_metrics(self, run: str = "all") -> dict[str, Any]:
        runs = self._selected_runs(run)
        rows = []
        for run_id, metrics_path in runs:
            metrics = self._read_json(metrics_path)
            if metrics is None:
                continue
            for row in metrics.get("models", []):
                rows.append(
                    {
                        "run_id": run_id,
                        "model_id": row.get("model_id"),
                        "prediction_rows": row.get("prediction_rows"),
                        "evaluated_rows": row.get("evaluated_rows"),
                        "prediction_date_count": row.get("prediction_date_count"),
                        "rank_ic_mean": row.get("rank_ic_mean"),
                        "average_realized_return": row.get("average_realized_return"),
                        "top_5_average_return": row.get("top_5_average_return"),
                        "top_5_hit_rate": row.get("top_5_hit_rate"),
                        "top_10_average_return": row.get("top_10_average_return"),
                        "top_10_hit_rate": row.get("top_10_hit_rate"),
                        "top_20_average_return": row.get("top_20_average_return"),
                        "top_20_hit_rate": row.get("top_20_hit_rate"),
                    }
                )
        return {
            "status": "done" if rows else "missing",
            "run": run,
            "rows": rows,
        }

    def backtests(self, group: str = "all") -> dict[str, Any]:
        groups = self._selected_backtest_groups(group)
        rows = []
        for group_id, metrics_path in groups:
            metrics = self._read_json(metrics_path)
            if metrics is None:
                continue
            for row in metrics.get("results", []):
                rows.append({"group": group_id, **row})
        rows = sorted(
            rows,
            key=lambda row: (
                row.get("total_return") is None,
                -(row.get("total_return") or 0),
            ),
        )
        return {
            "status": "done" if rows else "missing",
            "group": group,
            "rows": rows,
        }

    def candidates(
        self,
        model_id: str = "momentum_1d",
        top_n: int = 5,
        run: str = "baselines",
        limit: int = 200,
    ) -> dict[str, Any]:
        predictions_path = self._predictions_path(run)
        if predictions_path is None or not predictions_path.exists():
            return {
                "status": "missing",
                "path": str(predictions_path) if predictions_path else None,
                "model_id": model_id,
                "top_n": top_n,
                "rows": [],
            }
        frame = pd.read_parquet(predictions_path)
        if "prediction_date" in frame:
            frame["prediction_date"] = pd.to_datetime(
                frame["prediction_date"],
                errors="coerce",
            ).dt.date.astype(str)
        filtered = frame[frame["model_id"].eq(model_id)].copy()
        filtered = filtered[filtered["rank"].le(top_n)]
        filtered = filtered.sort_values(["prediction_date", "rank"]).tail(max(limit, 0))
        columns = [
            "prediction_date",
            "symbol",
            "instrument_key",
            "model_id",
            "rank",
            "score",
            "realized_forward_ret_1d",
        ]
        output = filtered[[column for column in columns if column in filtered.columns]]
        rows = output.astype(object).where(pd.notna(output), None).to_dict(orient="records")
        return {
            "status": "done",
            "path": str(predictions_path),
            "run": run,
            "model_id": model_id,
            "top_n": top_n,
            "rows": rows,
        }

    def latest_candidates(self, run: str = "baselines", top_n: int = 5) -> dict[str, Any]:
        latest_layer = self._latest_prediction_candidates(run, top_n)
        if latest_layer is not None:
            return latest_layer

        predictions_path = self._predictions_path(run)
        if predictions_path is None or not predictions_path.exists():
            return {
                "status": "missing",
                "path": str(predictions_path) if predictions_path else None,
                "run": run,
                "top_n": top_n,
                "prediction_date": None,
                "model_count": 0,
                "models": [],
            }

        frame = pd.read_parquet(predictions_path)
        frame["prediction_date"] = pd.to_datetime(
            frame["prediction_date"],
            errors="coerce",
        ).dt.date
        latest_date = frame["prediction_date"].max()
        latest = frame[frame["prediction_date"].eq(latest_date)].copy()
        latest = latest[latest["rank"].le(top_n)].sort_values(["model_id", "rank"])
        latest["prediction_date"] = latest["prediction_date"].astype(str)

        columns = [
            "prediction_date",
            "symbol",
            "instrument_key",
            "model_id",
            "rank",
            "score",
            "realized_forward_ret_1d",
        ]
        models = []
        for model_id, group in latest.groupby("model_id", sort=True):
            output = group[[column for column in columns if column in group.columns]]
            rows = output.astype(object).where(pd.notna(output), None).to_dict(orient="records")
            models.append({"model_id": model_id, "rows": rows})

        return {
            "status": "done",
            "path": str(predictions_path),
            "run": run,
            "top_n": top_n,
            "prediction_date": latest_date.isoformat() if pd.notna(latest_date) else None,
            "model_count": len(models),
            "models": models,
            "note": (
                "Latest prediction date available in the stored artifact; use as next-session "
                "candidates only if the pipeline data is current."
            ),
        }

    def _latest_prediction_candidates(self, run: str, top_n: int) -> dict[str, Any] | None:
        candidates_path = self.ml_dir / "latest_predictions_v1/latest_candidates.json"
        if not candidates_path.exists():
            return None
        payload = self._read_json(candidates_path)
        if payload is None:
            return None
        run_payload = next(
            (row for row in payload.get("runs", []) if row.get("run_id") == run),
            None,
        )
        if run_payload is None:
            return None
        top_n_key = str(top_n)
        models = [
            {
                "model_id": model.get("model_id"),
                "rows": model.get("top_n", {}).get(top_n_key, []),
            }
            for model in run_payload.get("models", [])
        ]
        return {
            "status": "done",
            "path": str(candidates_path),
            "run": run,
            "top_n": top_n,
            "prediction_date": payload.get("prediction_date"),
            "model_count": len(models),
            "models": models,
            "note": payload.get("note"),
        }

    def robustness(
        self,
        group: str = "baselines",
        model_id: str = "momentum_1d",
        top_n: int = 5,
        cost_bps_values: tuple[float, ...] = (0.0, 5.0, 10.0, 20.0, 30.0, 50.0),
    ) -> dict[str, Any]:
        predictions_path = self._predictions_path(group)
        if predictions_path is None or not predictions_path.exists():
            return {
                "status": "missing",
                "group": group,
                "model_id": model_id,
                "top_n": top_n,
                "cost_sensitivity": [],
                "top_n_comparison": [],
                "drawdown": None,
            }

        predictions = pd.read_parquet(predictions_path)
        predictions = predictions[predictions["model_id"].eq(model_id)].copy()
        if predictions.empty:
            return {
                "status": "missing",
                "group": group,
                "model_id": model_id,
                "top_n": top_n,
                "cost_sensitivity": [],
                "top_n_comparison": self._top_n_comparison(group, model_id),
                "drawdown": self._drawdown_summary(group, model_id, top_n),
            }
        cost_rows = []
        for cost_bps in cost_bps_values:
            result = run_prediction_backtest(
                predictions,
                config=BacktestConfig(
                    top_n_values=(top_n,),
                    transaction_cost_bps=cost_bps,
                ),
            )
            if result.metrics["results"]:
                row = result.metrics["results"][0]
                cost_rows.append({"transaction_cost_bps": cost_bps, **row})

        return {
            "status": "done" if cost_rows else "missing",
            "group": group,
            "model_id": model_id,
            "top_n": top_n,
            "cost_sensitivity": self._records(cost_rows),
            "top_n_comparison": self._top_n_comparison(group, model_id),
            "drawdown": self._drawdown_summary(group, model_id, top_n),
        }

    def equity_curve(
        self,
        group: str = "baselines",
        model_id: str = "momentum_1d",
        top_n: int = 5,
    ) -> dict[str, Any]:
        path = self.ml_dir / f"backtests_v1/{group}/portfolio_equity_curve.csv"
        if not path.exists():
            return {
                "status": "missing",
                "path": str(path),
                "group": group,
                "model_id": model_id,
                "top_n": top_n,
                "rows": [],
            }
        frame = pd.read_csv(path)
        filtered = frame[frame["model_id"].eq(model_id) & frame["top_n"].eq(top_n)].copy()
        if "prediction_date" in filtered:
            filtered["prediction_date"] = pd.to_datetime(
                filtered["prediction_date"],
                errors="coerce",
            ).dt.date.astype(str)
        rows = (
            filtered.astype(object)
            .where(pd.notna(filtered), None)
            .to_dict(orient="records")
        )
        return {
            "status": "done" if not filtered.empty else "missing",
            "path": str(path),
            "group": group,
            "model_id": model_id,
            "top_n": top_n,
            "rows": rows,
        }

    def _top_n_comparison(self, group: str, model_id: str) -> list[dict[str, Any]]:
        metrics_path = self.ml_dir / f"backtests_v1/{group}/backtest_metrics.json"
        metrics = self._read_json(metrics_path)
        if metrics is None:
            return []
        rows = [
            row
            for row in metrics.get("results", [])
            if row.get("model_id") == model_id
        ]
        return self._records(sorted(rows, key=lambda row: row.get("top_n") or 0))

    def _drawdown_summary(
        self,
        group: str,
        model_id: str,
        top_n: int,
    ) -> dict[str, Any] | None:
        curve_path = self.ml_dir / f"backtests_v1/{group}/portfolio_equity_curve.csv"
        daily_path = self.ml_dir / f"backtests_v1/{group}/daily_portfolio_returns.csv"
        if not curve_path.exists():
            return None

        curve = pd.read_csv(curve_path)
        curve = curve[curve["model_id"].eq(model_id) & curve["top_n"].eq(top_n)].copy()
        if curve.empty:
            return None
        curve["prediction_date"] = pd.to_datetime(curve["prediction_date"], errors="coerce").dt.date
        curve = curve.sort_values("prediction_date").reset_index(drop=True)
        peak_equity = curve["equity"].cummax()
        peak_index = peak_equity.idxmax()
        trough_index = curve["drawdown"].idxmin()
        trough = curve.loc[trough_index]
        peak_before_trough = curve.loc[:trough_index, "equity"].idxmax()
        peak = curve.loc[peak_before_trough]
        recovery = curve.loc[
            (curve.index > trough_index) & curve["equity"].ge(float(peak["equity"]))
        ]

        daily_rows: list[dict[str, Any]] = []
        if daily_path.exists():
            daily = pd.read_csv(daily_path)
            daily["prediction_date"] = pd.to_datetime(
                daily["prediction_date"],
                errors="coerce",
            ).dt.date
            daily = daily[
                daily["model_id"].eq(model_id)
                & daily["top_n"].eq(top_n)
                & daily["prediction_date"].ge(peak["prediction_date"])
                & daily["prediction_date"].le(trough["prediction_date"])
            ].copy()
            daily_rows = self._records(
                daily.sort_values("prediction_date").to_dict(orient="records")
            )

        return {
            "peak_date": peak["prediction_date"].isoformat(),
            "trough_date": trough["prediction_date"].isoformat(),
            "recovery_date": (
                recovery.iloc[0]["prediction_date"].isoformat()
                if not recovery.empty
                else None
            ),
            "max_drawdown": float(trough["drawdown"]),
            "peak_equity": float(peak["equity"]),
            "trough_equity": float(trough["equity"]),
            "days": int(trough_index - peak_before_trough + 1),
            "latest_peak_date": curve.loc[peak_index]["prediction_date"].isoformat(),
            "daily_returns": daily_rows,
        }

    def _model_run_summary(
        self,
        run_id: str,
        metrics: dict[str, Any] | None,
        backtest: dict[str, Any] | None,
    ) -> dict[str, Any]:
        best = self._best_backtest_result([backtest])
        return {
            "run_id": run_id,
            "status": "done" if metrics else "missing",
            "generated_at": metrics.get("generated_at") if metrics else None,
            "model_count": metrics.get("model_count") if metrics else 0,
            "prediction_row_count": metrics.get("prediction_row_count") if metrics else 0,
            "fold_count": metrics.get("manifest", {}).get("fold_count") if metrics else None,
            "best_backtest": best,
        }

    @staticmethod
    def _best_backtest_result(backtests: list[dict[str, Any] | None]) -> dict[str, Any] | None:
        rows = [
            row
            for backtest in backtests
            if backtest
            for row in backtest.get("results", [])
            if row.get("total_return") is not None
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: row.get("total_return") or float("-inf"))

    def _selected_runs(self, run: str) -> list[tuple[str, Path]]:
        paths = {
            "baselines": self.ml_dir / "baselines_v1/baseline_metrics.json",
            "lightgbm": self.ml_dir / "lightgbm_v1/lightgbm_metrics.json",
        }
        if run == "all":
            return list(paths.items())
        return [(run, paths[run])] if run in paths else []

    def _selected_backtest_groups(self, group: str) -> list[tuple[str, Path]]:
        paths = {
            "baselines": self.ml_dir / "backtests_v1/baselines/backtest_metrics.json",
            "lightgbm": self.ml_dir / "backtests_v1/lightgbm/backtest_metrics.json",
        }
        if group == "all":
            return list(paths.items())
        return [(group, paths[group])] if group in paths else []

    def _predictions_path(self, run: str) -> Path | None:
        paths = {
            "baselines": self.ml_dir / "baselines_v1/baseline_predictions.parquet",
            "lightgbm": self.ml_dir / "lightgbm_v1/lightgbm_predictions.parquet",
        }
        return paths.get(run)

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        return json.loads(path.read_text())

    @staticmethod
    def _records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        frame = pd.DataFrame(rows)
        return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")
