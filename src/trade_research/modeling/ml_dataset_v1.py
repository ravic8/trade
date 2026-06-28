from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

from trade_research.features.daily_technical import FEATURE_COLUMNS_V1_0

ML_DATASET_VERSION_V1 = "ml_dataset_v1_0"
STATIC_FULL_HISTORY_COVERAGE_POLICY = "static_full_history_100pct_coverage"
TARGET_COLUMN_V1 = "forward_ret_1d"

METADATA_COLUMNS = {
    "instrument_key",
    "symbol",
    "exchange",
    "source",
    "date",
    "ml_dataset_version",
    "feature_version",
    "target_version",
    "coverage_policy",
    "coverage_pct_full_history",
    "is_trainable",
    "split",
    "exclusion_reasons",
}

TARGET_COLUMNS_V1 = [
    TARGET_COLUMN_V1,
    "next_day_positive",
    "next_day_top_decile",
    "next_day_bottom_decile",
    "daily_forward_ret_1d_rank",
]

BLOCKED_FEATURE_COLUMNS = {
    *METADATA_COLUMNS,
    *TARGET_COLUMNS_V1,
    "computed_at",
    "quality_status",
    "feature_quality_status",
    "target_quality_status",
    "close_target",
    "open_target",
    "high_target",
    "low_target",
    "volume_target",
}


@dataclass(frozen=True)
class MLDatasetV1Config:
    ml_dataset_version: str = ML_DATASET_VERSION_V1
    coverage_policy: str = STATIC_FULL_HISTORY_COVERAGE_POLICY
    required_full_history_coverage_pct: float = 1.0
    train_fraction: float = 0.70
    validation_fraction: float = 0.15


@dataclass(frozen=True)
class MLDatasetV1Build:
    dataset: pd.DataFrame
    feature_columns: list[str]
    exclusions: pd.DataFrame
    leakage_checks: dict[str, Any]
    summary: dict[str, Any]


def build_ml_dataset_v1(
    ohlcv: pd.DataFrame,
    features: pd.DataFrame,
    targets: pd.DataFrame,
    stock_coverage: pd.DataFrame,
    config: MLDatasetV1Config | None = None,
) -> MLDatasetV1Build:
    cfg = config or MLDatasetV1Config()
    prepared_ohlcv = _prepare_frame(ohlcv)
    prepared_features = _prepare_frame(features)
    prepared_targets = _prepare_frame(targets)
    prepared_coverage = _prepare_coverage(stock_coverage)

    duplicate_summary = {
        "ohlcv": _duplicate_key_count(prepared_ohlcv),
        "features": _duplicate_key_count(prepared_features),
        "targets": _duplicate_key_count(prepared_targets),
    }
    if any(duplicate_summary.values()):
        raise ValueError(f"Duplicate instrument/date keys found: {duplicate_summary}")

    eligible_keys = _eligible_full_history_coverage_keys(prepared_coverage, cfg)
    exclusions = _build_stock_exclusions(prepared_coverage, eligible_keys, cfg)

    ohlcv_eligible = prepared_ohlcv[
        prepared_ohlcv["instrument_key"].isin(eligible_keys)
    ].copy()
    merged = ohlcv_eligible.merge(
        _feature_columns_for_join(prepared_features),
        on=["instrument_key", "date"],
        how="left",
        suffixes=("", "_feature"),
    )
    merged = merged.merge(
        _target_columns_for_join(prepared_targets),
        on=["instrument_key", "date"],
        how="left",
        suffixes=("", "_target"),
    )

    merged = _coalesce_identity_columns(merged)
    merged = merged.merge(
        prepared_coverage[["instrument_key", "coverage_pct", "coverage_status"]],
        on="instrument_key",
        how="left",
    )
    merged = merged.rename(columns={"coverage_pct": "coverage_pct_full_history"})

    feature_columns = _model_feature_columns(merged)
    merged = _add_target_labels(merged)
    merged["ml_dataset_version"] = cfg.ml_dataset_version
    merged["coverage_policy"] = cfg.coverage_policy
    merged["split"] = _chronological_splits(merged["date"], cfg)
    merged["exclusion_reasons"] = _row_exclusion_reasons(merged, feature_columns)
    merged["is_trainable"] = merged["exclusion_reasons"].eq("")

    dataset = _ordered_dataset(merged, feature_columns)
    leakage_checks = _leakage_checks(dataset, feature_columns, cfg)
    if not leakage_checks["passed"]:
        failures = [
            name for name, check in leakage_checks["checks"].items() if not check["passed"]
        ]
        raise ValueError(f"ML dataset leakage checks failed: {failures}")

    summary = _summary(dataset, exclusions, feature_columns, leakage_checks, cfg)
    return MLDatasetV1Build(
        dataset=dataset,
        feature_columns=feature_columns,
        exclusions=exclusions,
        leakage_checks=leakage_checks,
        summary=summary,
    )


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "Date" in out.columns and "date" not in out.columns:
        out = out.rename(columns={"Date": "date"})
    if "InstrumentKey" in out.columns and "instrument_key" not in out.columns:
        out = out.rename(columns={"InstrumentKey": "instrument_key"})
    if "Symbol" in out.columns and "symbol" not in out.columns:
        out = out.rename(columns={"Symbol": "symbol"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    return out


def _prepare_coverage(frame: pd.DataFrame) -> pd.DataFrame:
    out = (
        _prepare_frame(frame)
        if "date" in frame.columns or "Date" in frame.columns
        else frame.copy()
    )
    if "InstrumentKey" in out.columns and "instrument_key" not in out.columns:
        out = out.rename(columns={"InstrumentKey": "instrument_key"})
    if "Symbol" in out.columns and "symbol" not in out.columns:
        out = out.rename(columns={"Symbol": "symbol"})
    if "coverage_pct" not in out.columns:
        raise ValueError("Stock coverage must include coverage_pct.")
    return out


def _duplicate_key_count(frame: pd.DataFrame) -> int:
    return int(frame.duplicated(["instrument_key", "date"]).sum())


def _eligible_full_history_coverage_keys(
    coverage: pd.DataFrame,
    config: MLDatasetV1Config,
) -> set[str]:
    eligible = coverage[
        np.isclose(
            coverage["coverage_pct"].astype(float),
            config.required_full_history_coverage_pct,
        )
    ].copy()
    if "coverage_status" in eligible.columns:
        eligible = eligible[eligible["coverage_status"].eq("pass")]
    return set(eligible["instrument_key"].astype(str))


def _build_stock_exclusions(
    coverage: pd.DataFrame,
    eligible_keys: set[str],
    config: MLDatasetV1Config,
) -> pd.DataFrame:
    excluded = coverage[~coverage["instrument_key"].astype(str).isin(eligible_keys)].copy()
    if excluded.empty:
        return pd.DataFrame(
            columns=[
                "instrument_key",
                "symbol",
                "coverage_pct_full_history",
                "coverage_status",
                "exclusion_reason",
                "coverage_policy",
            ]
        )
    excluded = excluded.rename(columns={"coverage_pct": "coverage_pct_full_history"})
    excluded["exclusion_reason"] = "not_full_history_coverage"
    excluded["coverage_policy"] = config.coverage_policy
    keep = [
        column
        for column in [
            "instrument_key",
            "symbol",
            "coverage_pct_full_history",
            "coverage_status",
            "first_date",
            "last_date",
            "observed_date_count",
            "expected_date_count",
            "exclusion_reason",
            "coverage_policy",
        ]
        if column in excluded.columns
    ]
    return excluded[keep].sort_values(["coverage_pct_full_history", "symbol"])


def _feature_columns_for_join(features: pd.DataFrame) -> pd.DataFrame:
    columns = [
        column
        for column in [
            "instrument_key",
            "date",
            "symbol",
            "exchange",
            "source",
            "feature_version",
            "quality_status",
            *FEATURE_COLUMNS_V1_0,
        ]
        if column in features.columns
    ]
    out = features[columns].copy()
    if "quality_status" in out.columns:
        out = out.rename(columns={"quality_status": "feature_quality_status"})
    return out


def _target_columns_for_join(targets: pd.DataFrame) -> pd.DataFrame:
    columns = [
        column
        for column in [
            "instrument_key",
            "date",
            "symbol",
            "exchange",
            "source",
            "target_version",
            "quality_status",
            TARGET_COLUMN_V1,
        ]
        if column in targets.columns
    ]
    out = targets[columns].copy()
    if "quality_status" in out.columns:
        out = out.rename(columns={"quality_status": "target_quality_status"})
    return out


def _coalesce_identity_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in ["symbol", "exchange", "source"]:
        candidates = [
            name for name in [column, f"{column}_feature", f"{column}_target"] if name in out
        ]
        if not candidates:
            continue
        series = out[candidates[0]]
        for candidate in candidates[1:]:
            series = series.combine_first(out[candidate])
        out[column] = series
        drop_columns = [candidate for candidate in candidates[1:] if candidate in out]
        out = out.drop(columns=drop_columns)
    return out


def _model_feature_columns(frame: pd.DataFrame) -> list[str]:
    return [
        column
        for column in FEATURE_COLUMNS_V1_0
        if column in frame.columns and column not in BLOCKED_FEATURE_COLUMNS
    ]


def _add_target_labels(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    target = out[TARGET_COLUMN_V1]
    out["next_day_positive"] = target.gt(0).where(target.notna(), pd.NA).astype("boolean")
    pct_rank = out.groupby("date")[TARGET_COLUMN_V1].rank(pct=True, method="average")
    descending_rank = out.groupby("date")[TARGET_COLUMN_V1].rank(
        ascending=False,
        method="first",
    )
    complete = target.notna()
    out["next_day_top_decile"] = (pct_rank >= 0.90).where(complete, pd.NA).astype("boolean")
    out["next_day_bottom_decile"] = (pct_rank <= 0.10).where(complete, pd.NA).astype("boolean")
    out["daily_forward_ret_1d_rank"] = descending_rank.where(complete)
    return out


def _chronological_splits(dates: pd.Series, config: MLDatasetV1Config) -> pd.Series:
    unique_dates = sorted(date_value for date_value in dates.dropna().unique())
    if not unique_dates:
        return pd.Series("unknown", index=dates.index)
    train_end_idx = max(1, int(len(unique_dates) * config.train_fraction))
    validation_end_idx = max(
        train_end_idx + 1,
        int(len(unique_dates) * (config.train_fraction + config.validation_fraction)),
    )
    train_dates = set(unique_dates[:train_end_idx])
    validation_dates = set(unique_dates[train_end_idx:validation_end_idx])

    def split_for(value: object) -> str:
        if value in train_dates:
            return "train_seed"
        if value in validation_dates:
            return "validation_seed"
        return "walk_forward_eval"

    return dates.map(split_for)


def _row_exclusion_reasons(frame: pd.DataFrame, feature_columns: list[str]) -> pd.Series:
    reasons: list[list[str]] = [[] for _ in range(len(frame))]

    feature_missing = (
        frame["feature_version"].isna()
        if "feature_version" in frame
        else pd.Series(True, index=frame.index)
    )
    target_missing = (
        frame["target_version"].isna()
        if "target_version" in frame
        else pd.Series(True, index=frame.index)
    )
    target_null = frame[TARGET_COLUMN_V1].isna()
    feature_invalid = _feature_null_or_inf_mask(frame, feature_columns)

    for idx, has_reason in enumerate(feature_missing):
        if bool(has_reason):
            reasons[idx].append("missing_feature_row")
    for idx, has_reason in enumerate(target_missing):
        if bool(has_reason):
            reasons[idx].append("missing_target_row")
    for idx, has_reason in enumerate(feature_invalid):
        if bool(has_reason):
            reasons[idx].append("feature_null_or_inf")
    for idx, has_reason in enumerate(target_null):
        if bool(has_reason):
            reasons[idx].append("target_null")

    return pd.Series([";".join(row_reasons) for row_reasons in reasons], index=frame.index)


def _feature_null_or_inf_mask(frame: pd.DataFrame, feature_columns: list[str]) -> pd.Series:
    if not feature_columns:
        return pd.Series(True, index=frame.index)
    feature_frame = frame[feature_columns]
    nulls = feature_frame.isna().any(axis=1)
    numeric = feature_frame.select_dtypes(include=[np.number])
    infs = (
        pd.Series(np.isinf(numeric).any(axis=1), index=frame.index)
        if not numeric.empty
        else pd.Series(False, index=frame.index)
    )
    return nulls | infs


def _ordered_dataset(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    ordered = [
        "instrument_key",
        "symbol",
        "exchange",
        "source",
        "date",
        "ml_dataset_version",
        "feature_version",
        "target_version",
        "coverage_policy",
        "coverage_pct_full_history",
        "coverage_status",
        "split",
        "is_trainable",
        "exclusion_reasons",
        "feature_quality_status",
        "target_quality_status",
        *feature_columns,
        *TARGET_COLUMNS_V1,
    ]
    columns = [column for column in ordered if column in frame.columns]
    return frame[columns].sort_values(["date", "instrument_key"]).reset_index(drop=True)


def _leakage_checks(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    config: MLDatasetV1Config,
) -> dict[str, Any]:
    trainable = dataset[dataset["is_trainable"]].copy()
    checks = {
        "target_columns_not_in_feature_columns": not bool(
            set(TARGET_COLUMNS_V1).intersection(feature_columns)
        ),
        "identifier_columns_not_in_feature_columns": not bool(
            {
                "instrument_key",
                "symbol",
                "date",
                "exchange",
                "source",
            }.intersection(feature_columns)
        ),
        "no_duplicate_dataset_keys": int(dataset.duplicated(["instrument_key", "date"]).sum())
        == 0,
        "no_null_targets_in_trainable_rows": bool(
            trainable.empty or trainable[TARGET_COLUMN_V1].notna().all()
        ),
        "no_null_or_inf_features_in_trainable_rows": bool(
            trainable.empty or not _feature_null_or_inf_mask(trainable, feature_columns).any()
        ),
        "coverage_policy_recorded": bool(
            dataset.empty
            or dataset["coverage_policy"].eq(config.coverage_policy).all()
        ),
    }
    formatted = {
        name: {"passed": bool(passed)}
        for name, passed in checks.items()
    }
    return {
        "passed": all(check["passed"] for check in formatted.values()),
        "checks": formatted,
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _summary(
    dataset: pd.DataFrame,
    exclusions: pd.DataFrame,
    feature_columns: list[str],
    leakage_checks: dict[str, Any],
    config: MLDatasetV1Config,
) -> dict[str, Any]:
    trainable = dataset[dataset["is_trainable"]]
    split_counts = (
        dataset["split"].value_counts().sort_index().to_dict()
        if not dataset.empty
        else {}
    )
    exclusion_counts = (
        dataset.loc[dataset["exclusion_reasons"].ne(""), "exclusion_reasons"]
        .str.get_dummies(sep=";")
        .sum()
        .astype(int)
        .to_dict()
        if not dataset.empty
        else {}
    )
    return {
        "dataset_name": "ml_dataset_v1",
        "ml_dataset_version": config.ml_dataset_version,
        "generated_at": datetime.now(UTC).isoformat(),
        "target_column": TARGET_COLUMN_V1,
        "coverage_policy": config.coverage_policy,
        "leakage_note": (
            "Static research universe; later replace with point-in-time coverage "
            "eligibility for live-realistic backtests."
        ),
        "row_count": int(len(dataset)),
        "trainable_row_count": int(len(trainable)),
        "symbol_count": int(dataset["symbol"].nunique()) if not dataset.empty else 0,
        "trainable_symbol_count": int(trainable["symbol"].nunique()) if not trainable.empty else 0,
        "excluded_symbol_count": int(exclusions["instrument_key"].nunique())
        if not exclusions.empty
        else 0,
        "feature_column_count": int(len(feature_columns)),
        "feature_columns": feature_columns,
        "date_min": _date_iso(dataset["date"].min()) if not dataset.empty else None,
        "date_max": _date_iso(dataset["date"].max()) if not dataset.empty else None,
        "split_counts": split_counts,
        "row_exclusion_counts": exclusion_counts,
        "stock_exclusion_count": int(len(exclusions)),
        "leakage_checks_passed": bool(leakage_checks["passed"]),
        "leakage_checks": leakage_checks,
        "config": asdict(config),
    }


def _date_iso(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)
