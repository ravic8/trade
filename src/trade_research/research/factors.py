from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trade_research.features.daily_technical import FEATURE_COLUMNS_V1_0, FEATURE_VERSION_V1_0
from trade_research.targets.daily_forward import DAILY_FORWARD_TARGET_VERSION_V1_0

DEFAULT_RETURN_TARGETS = [
    "forward_ret_1d",
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "forward_ret_60d",
]


@dataclass(frozen=True)
class FactorResearchSummary:
    dataset_name: str
    feature_version: str
    target_version: str
    generated_at: str
    row_count: int
    symbol_count: int
    date_min: str | None
    date_max: str | None
    feature_count: int
    return_target_count: int
    quantile_count: int
    ic_rows: int
    quantile_rows: int
    hit_rate_rows: int
    monthly_stability_rows: int


class DailyFactorResearchBuilder:
    def __init__(
        self,
        feature_version: str = FEATURE_VERSION_V1_0,
        target_version: str = DAILY_FORWARD_TARGET_VERSION_V1_0,
        quantiles: int = 5,
        min_date_rows: int = 5,
        min_month_rows: int = 20,
    ) -> None:
        self.feature_version = feature_version
        self.target_version = target_version
        self.quantiles = quantiles
        self.min_date_rows = min_date_rows
        self.min_month_rows = min_month_rows

    def build(
        self,
        features: pd.DataFrame,
        targets: pd.DataFrame,
        feature_columns: list[str] | None = None,
        return_targets: list[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, FactorResearchSummary]:
        joined = join_features_and_targets(
            features,
            targets,
            feature_version=self.feature_version,
            target_version=self.target_version,
        )
        selected_features = [
            column
            for column in (feature_columns or FEATURE_COLUMNS_V1_0)
            if column in joined.columns and pd.api.types.is_numeric_dtype(joined[column])
        ]
        selected_targets = [
            column
            for column in (return_targets or DEFAULT_RETURN_TARGETS)
            if column in joined.columns and pd.api.types.is_numeric_dtype(joined[column])
        ]

        ic = self._ic_table(joined, selected_features, selected_targets)
        quantiles = self._quantile_table(joined, selected_features, selected_targets)
        hit_rates = self._hit_rate_table(joined, selected_features)
        monthly = self._monthly_stability_table(joined, selected_features, selected_targets)
        summary = FactorResearchSummary(
            dataset_name="daily_v1_factor_research",
            feature_version=self.feature_version,
            target_version=self.target_version,
            generated_at=datetime.now(UTC).isoformat(),
            row_count=len(joined),
            symbol_count=int(joined["symbol"].nunique()) if "symbol" in joined else 0,
            date_min=_date_iso(joined["date"].min()) if not joined.empty else None,
            date_max=_date_iso(joined["date"].max()) if not joined.empty else None,
            feature_count=len(selected_features),
            return_target_count=len(selected_targets),
            quantile_count=self.quantiles,
            ic_rows=len(ic),
            quantile_rows=len(quantiles),
            hit_rate_rows=len(hit_rates),
            monthly_stability_rows=len(monthly),
        )
        return ic, quantiles, hit_rates, monthly, summary

    def _ic_table(
        self,
        joined: pd.DataFrame,
        feature_columns: list[str],
        return_targets: list[str],
    ) -> pd.DataFrame:
        rows = []
        for feature in feature_columns:
            for target in return_targets:
                daily = _daily_correlations(
                    joined,
                    feature=feature,
                    target=target,
                    min_date_rows=self.min_date_rows,
                )
                rows.append(_summarize_correlations(feature, target, daily))
        return pd.DataFrame(rows)

    def _quantile_table(
        self,
        joined: pd.DataFrame,
        feature_columns: list[str],
        return_targets: list[str],
    ) -> pd.DataFrame:
        rows = []
        for feature in feature_columns:
            quantile_frame = _with_feature_quantiles(joined, feature, self.quantiles)
            for target in return_targets:
                valid = quantile_frame.dropna(subset=["feature_quantile", target])
                grouped = valid.groupby("feature_quantile", observed=True)
                for quantile, group in grouped:
                    rows.append(
                        {
                            "feature": feature,
                            "target": target,
                            "feature_quantile": int(quantile),
                            "rows": len(group),
                            "mean_target": float(group[target].mean()),
                            "median_target": float(group[target].median()),
                            "positive_rate": float((group[target] > 0).mean()),
                        }
                    )
                top = valid[valid["feature_quantile"].eq(self.quantiles)]
                bottom = valid[valid["feature_quantile"].eq(1)]
                if not top.empty and not bottom.empty:
                    rows.append(
                        {
                            "feature": feature,
                            "target": target,
                            "feature_quantile": 0,
                            "rows": len(top) + len(bottom),
                            "mean_target": float(top[target].mean() - bottom[target].mean()),
                            "median_target": float(
                                top[target].median() - bottom[target].median()
                            ),
                            "positive_rate": float(
                                (top[target] > 0).mean() - (bottom[target] > 0).mean()
                            ),
                        }
                    )
        return pd.DataFrame(rows)

    def _hit_rate_table(self, joined: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
        if "top_quantile_forward_return_20d" not in joined.columns:
            return pd.DataFrame(
                columns=["feature", "feature_quantile", "rows", "top_quantile_hit_rate"]
            )
        rows = []
        label = "top_quantile_forward_return_20d"
        for feature in feature_columns:
            quantile_frame = _with_feature_quantiles(joined, feature, self.quantiles)
            valid = quantile_frame.dropna(subset=["feature_quantile", label]).copy()
            valid[label] = valid[label].astype(float)
            for quantile, group in valid.groupby("feature_quantile", observed=True):
                rows.append(
                    {
                        "feature": feature,
                        "feature_quantile": int(quantile),
                        "rows": len(group),
                        "top_quantile_hit_rate": float(group[label].mean()),
                    }
                )
        return pd.DataFrame(rows)

    def _monthly_stability_table(
        self,
        joined: pd.DataFrame,
        feature_columns: list[str],
        return_targets: list[str],
    ) -> pd.DataFrame:
        rows = []
        data = joined.copy()
        data["month"] = pd.to_datetime(data["date"]).dt.to_period("M").astype(str)
        for feature in feature_columns:
            for target in return_targets:
                for month, group in data.groupby("month", observed=True):
                    valid = group[[feature, target]].dropna()
                    if len(valid) < self.min_month_rows:
                        continue
                    rows.append(
                        {
                            "feature": feature,
                            "target": target,
                            "month": month,
                            "rows": len(valid),
                            "ic": _corr(valid[feature], valid[target], method="pearson"),
                            "rank_ic": _corr(valid[feature], valid[target], method="spearman"),
                        }
                    )
        return pd.DataFrame(rows)


def join_features_and_targets(
    features: pd.DataFrame,
    targets: pd.DataFrame,
    feature_version: str,
    target_version: str,
) -> pd.DataFrame:
    feature_rows = features[features["feature_version"].eq(feature_version)].copy()
    target_rows = targets[targets["target_version"].eq(target_version)].copy()
    keys = ["instrument_key", "date"]
    target_columns = [
        column
        for column in [
            "instrument_key",
            "date",
            "target_version",
            "forward_ret_1d",
            "forward_ret_5d",
            "forward_ret_10d",
            "forward_ret_20d",
            "forward_ret_60d",
            "forward_outperform_universe_20d",
            "top_quantile_forward_return_20d",
        ]
        if column in target_rows.columns
    ]
    joined = feature_rows.merge(target_rows[target_columns], on=keys, how="inner")
    return joined.sort_values(["date", "instrument_key"]).reset_index(drop=True)


def write_factor_research_outputs(
    ic: pd.DataFrame,
    quantiles: pd.DataFrame,
    hit_rates: pd.DataFrame,
    monthly: pd.DataFrame,
    summary: FactorResearchSummary,
    output_dir: Path,
) -> dict[str, Path]:
    import json

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "ic": output_dir / "daily_v1_factor_ic.csv",
        "quantiles": output_dir / "daily_v1_factor_quantiles.csv",
        "hit_rates": output_dir / "daily_v1_factor_hit_rates.csv",
        "monthly_stability": output_dir / "daily_v1_factor_monthly_stability.csv",
        "summary": output_dir / "daily_v1_factor_research_summary.json",
    }
    ic.to_csv(paths["ic"], index=False)
    quantiles.to_csv(paths["quantiles"], index=False)
    hit_rates.to_csv(paths["hit_rates"], index=False)
    monthly.to_csv(paths["monthly_stability"], index=False)
    paths["summary"].write_text(json.dumps(asdict(summary), indent=2) + "\n")
    return paths


def _daily_correlations(
    frame: pd.DataFrame,
    feature: str,
    target: str,
    min_date_rows: int,
) -> pd.DataFrame:
    rows = []
    for value_date, group in frame.groupby("date", observed=True):
        valid = group[[feature, target]].dropna()
        if len(valid) < min_date_rows:
            continue
        rows.append(
            {
                "date": value_date,
                "rows": len(valid),
                "ic": _corr(valid[feature], valid[target], method="pearson"),
                "rank_ic": _corr(valid[feature], valid[target], method="spearman"),
            }
        )
    return pd.DataFrame(rows)


def _summarize_correlations(feature: str, target: str, daily: pd.DataFrame) -> dict[str, Any]:
    if daily.empty:
        return {
            "feature": feature,
            "target": target,
            "dates": 0,
            "rows": 0,
            "mean_ic": np.nan,
            "mean_rank_ic": np.nan,
            "ic_t_stat": np.nan,
            "rank_ic_t_stat": np.nan,
            "positive_ic_pct": np.nan,
            "positive_rank_ic_pct": np.nan,
        }
    return {
        "feature": feature,
        "target": target,
        "dates": len(daily),
        "rows": int(daily["rows"].sum()),
        "mean_ic": float(daily["ic"].mean()),
        "mean_rank_ic": float(daily["rank_ic"].mean()),
        "ic_t_stat": _t_stat(daily["ic"]),
        "rank_ic_t_stat": _t_stat(daily["rank_ic"]),
        "positive_ic_pct": float((daily["ic"] > 0).mean()),
        "positive_rank_ic_pct": float((daily["rank_ic"] > 0).mean()),
    }


def _with_feature_quantiles(frame: pd.DataFrame, feature: str, quantiles: int) -> pd.DataFrame:
    out = frame[["date", "instrument_key", feature, *DEFAULT_RETURN_TARGETS]].copy()
    if "top_quantile_forward_return_20d" in frame.columns:
        out["top_quantile_forward_return_20d"] = frame["top_quantile_forward_return_20d"]
    percentile = out.groupby("date")[feature].rank(pct=True, method="first")
    out["feature_quantile"] = np.ceil(percentile * quantiles)
    out["feature_quantile"] = out["feature_quantile"].clip(lower=1, upper=quantiles)
    return out


def _corr(left: pd.Series, right: pd.Series, method: str) -> float:
    if method == "spearman":
        left = left.rank(method="average")
        right = right.rank(method="average")
        method = "pearson"
    value = left.corr(right, method=method)
    return float(value) if not pd.isna(value) else np.nan


def _t_stat(values: pd.Series) -> float:
    clean = values.dropna()
    if len(clean) < 2:
        return np.nan
    std = clean.std(ddof=1)
    if pd.isna(std) or std == 0:
        return np.nan
    return float(clean.mean() / (std / np.sqrt(len(clean))))


def _date_iso(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)
