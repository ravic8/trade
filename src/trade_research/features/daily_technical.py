from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

FEATURE_VERSION_V1_0 = "daily_v1_ohlcv_technical_v1_0"

REQUIRED_COLUMNS = {
    "instrument_key",
    "symbol",
    "exchange",
    "source",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "quality_status",
}

FEATURE_COLUMNS_V1_0 = [
    "ret_1d",
    "ret_2d",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "ret_60d",
    "ret_120d",
    "log_ret_1d",
    "sma_10",
    "sma_20",
    "sma_50",
    "sma_100",
    "sma_200",
    "ema_10",
    "ema_20",
    "ema_50",
    "ema_100",
    "ema_200",
    "close_vs_sma_20",
    "close_vs_sma_50",
    "close_vs_sma_200",
    "close_vs_ema_20",
    "close_vs_ema_50",
    "close_vs_ema_200",
    "sma_20_vs_sma_50",
    "sma_50_vs_sma_200",
    "volatility_10d",
    "volatility_20d",
    "volatility_60d",
    "volatility_ratio_20d_60d",
    "true_range",
    "atr_14",
    "atr_pct_14",
    "volume_avg_20d",
    "volume_avg_60d",
    "volume_ratio_20d",
    "volume_ratio_60d",
    "turnover",
    "turnover_avg_20d",
    "turnover_avg_60d",
    "turnover_ratio_20d",
    "turnover_ratio_60d",
]


@dataclass(frozen=True)
class FeatureAuditSummary:
    dataset_name: str
    feature_version: str
    generated_at: str
    row_count: int
    symbol_count: int
    date_min: str | None
    date_max: str | None
    duplicate_key_count: int
    invalid_ohlcv_count: int
    inf_value_count: int
    extreme_value_count: int
    passed_rows: int
    warning_rows: int
    failed_rows: int


class DailyTechnicalFeatureBuilder:
    def __init__(
        self,
        feature_version: str = FEATURE_VERSION_V1_0,
        computed_at: datetime | None = None,
        drop_invalid_rows: bool = False,
    ) -> None:
        self.feature_version = feature_version
        self.computed_at = computed_at or datetime.now(UTC)
        self.drop_invalid_rows = drop_invalid_rows

    def build(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        frame = normalize_daily_ohlcv(ohlcv)
        if self.drop_invalid_rows:
            frame = frame[~invalid_daily_ohlcv_mask(frame)].copy()
        validate_daily_ohlcv(frame)
        frame = frame.sort_values(["instrument_key", "date"]).reset_index(drop=True)

        groups = frame.groupby("instrument_key", group_keys=False, sort=False)
        out = frame.copy()
        out["feature_version"] = self.feature_version
        out["computed_at"] = self.computed_at

        previous_close = groups["close"].shift(1)
        for window in [1, 2, 3, 5, 10, 20, 60, 120]:
            out[f"ret_{window}d"] = groups["close"].pct_change(periods=window)
        out["log_ret_1d"] = np.log(out["close"] / previous_close)

        for window in [10, 20, 50, 100, 200]:
            out[f"sma_{window}"] = groups["close"].transform(
                lambda series, w=window: series.rolling(w, min_periods=w).mean()
            )
            out[f"ema_{window}"] = groups["close"].transform(
                lambda series, w=window: series.ewm(span=w, adjust=False, min_periods=w).mean()
            )

        for window in [20, 50, 200]:
            out[f"close_vs_sma_{window}"] = _safe_ratio(out["close"], out[f"sma_{window}"])
            out[f"close_vs_ema_{window}"] = _safe_ratio(out["close"], out[f"ema_{window}"])

        out["sma_20_vs_sma_50"] = _safe_ratio(out["sma_20"], out["sma_50"])
        out["sma_50_vs_sma_200"] = _safe_ratio(out["sma_50"], out["sma_200"])

        groups = out.groupby("instrument_key", group_keys=False, sort=False)
        for window in [10, 20, 60]:
            out[f"volatility_{window}d"] = groups["log_ret_1d"].transform(
                lambda series, w=window: series.rolling(w, min_periods=w).std()
            )
        out["volatility_ratio_20d_60d"] = _safe_plain_ratio(
            out["volatility_20d"], out["volatility_60d"]
        )

        range_intraday = out["high"] - out["low"]
        range_gap_high = (out["high"] - previous_close).abs()
        range_gap_low = (out["low"] - previous_close).abs()
        out["true_range"] = pd.concat(
            [range_intraday, range_gap_high, range_gap_low], axis=1
        ).max(axis=1)
        out.loc[previous_close.isna(), "true_range"] = np.nan
        groups = out.groupby("instrument_key", group_keys=False, sort=False)
        out["atr_14"] = groups["true_range"].transform(
            lambda series: series.rolling(14, min_periods=14).mean()
        )
        out["atr_pct_14"] = _safe_plain_ratio(out["atr_14"], out["close"])

        out["turnover"] = out["close"] * out["volume"]
        for window in [20, 60]:
            out[f"volume_avg_{window}d"] = groups["volume"].transform(
                lambda series, w=window: series.rolling(w, min_periods=w).mean()
            )
            out[f"volume_ratio_{window}d"] = _safe_plain_ratio(
                out["volume"], out[f"volume_avg_{window}d"]
            )
            out[f"turnover_avg_{window}d"] = groups["turnover"].transform(
                lambda series, w=window: series.rolling(w, min_periods=w).mean()
            )
            out[f"turnover_ratio_{window}d"] = _safe_plain_ratio(
                out["turnover"], out[f"turnover_avg_{window}d"]
            )

        out["quality_status"] = np.where(
            out[FEATURE_COLUMNS_V1_0].isna().any(axis=1),
            "warning",
            "passed",
        )

        ordered_columns = [
            "instrument_key",
            "symbol",
            "exchange",
            "source",
            "date",
            "feature_version",
            "computed_at",
            "quality_status",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
            *FEATURE_COLUMNS_V1_0,
        ]
        return out[ordered_columns]


def normalize_daily_ohlcv(frame: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "InstrumentKey": "instrument_key",
        "Symbol": "symbol",
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "OpenInterest": "open_interest",
        "Source": "source",
        "Exchange": "exchange",
        "QualityStatus": "quality_status",
    }
    out = frame.rename(columns=rename_map).copy()
    if "exchange" not in out.columns:
        out["exchange"] = "NSE"
    if "source" not in out.columns:
        out["source"] = "upstox"
    if "open_interest" not in out.columns:
        out["open_interest"] = np.nan
    if "quality_status" not in out.columns:
        out["quality_status"] = "passed"

    missing = REQUIRED_COLUMNS - set(out.columns)
    if missing:
        raise ValueError(f"Daily OHLCV is missing required columns: {sorted(missing)}")

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    for column in ["open", "high", "low", "close", "volume", "open_interest"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in ["instrument_key", "symbol", "exchange", "source", "quality_status"]:
        out[column] = out[column].astype(str)
    return out[list(REQUIRED_COLUMNS)].copy()


def validate_daily_ohlcv(frame: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"Daily OHLCV is missing required columns: {sorted(missing)}")
    duplicate_count = int(frame.duplicated(["instrument_key", "source", "date"]).sum())
    if duplicate_count:
        raise ValueError(f"Daily OHLCV contains {duplicate_count} duplicate instrument/date rows")

    invalid_count = int(invalid_daily_ohlcv_mask(frame).sum())
    if invalid_count:
        raise ValueError(f"Daily OHLCV contains {invalid_count} invalid rows")


def invalid_daily_ohlcv_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["date"].isna()
        | frame["open"].isna()
        | frame["high"].isna()
        | frame["low"].isna()
        | frame["close"].isna()
        | frame["volume"].isna()
        | (frame["open"] <= 0)
        | (frame["high"] <= 0)
        | (frame["low"] <= 0)
        | (frame["close"] <= 0)
        | (frame["volume"] < 0)
        | (frame["high"] < frame["low"])
        | (frame["high"] < frame["open"])
        | (frame["high"] < frame["close"])
        | (frame["low"] > frame["open"])
        | (frame["low"] > frame["close"])
    )


def audit_daily_features(
    features: pd.DataFrame,
    feature_version: str = FEATURE_VERSION_V1_0,
    dataset_name: str = "daily_v1_ohlcv_technical",
    extreme_return_threshold: float = 0.50,
    invalid_ohlcv_count: int | None = None,
) -> tuple[pd.DataFrame, FeatureAuditSummary]:
    duplicate_key_count = int(
        features.duplicated(["instrument_key", "date", "feature_version"]).sum()
    )
    numeric_features = [column for column in FEATURE_COLUMNS_V1_0 if column in features.columns]
    inf_counts = (
        np.isinf(features[numeric_features].select_dtypes(include=[np.number])).sum().astype(int)
        if numeric_features
        else pd.Series(dtype=int)
    )
    null_counts = features[numeric_features].isna().sum().astype(int)
    null_pct = (features[numeric_features].isna().mean() * 100).round(4)

    return_columns = [column for column in numeric_features if column.startswith("ret_")]
    extreme_count = 0
    if return_columns:
        extreme_count = int((features[return_columns].abs() > extreme_return_threshold).sum().sum())

    audit = pd.DataFrame(
        [
            {
                "feature": column,
                "null_count": int(null_counts.get(column, 0)),
                "null_pct": float(null_pct.get(column, 0.0)),
                "inf_count": int(inf_counts.get(column, 0)),
            }
            for column in numeric_features
        ]
    )

    status_counts = features["quality_status"].value_counts().to_dict()
    observed_invalid_ohlcv_count = (
        _invalid_ohlcv_count(features) if invalid_ohlcv_count is None else invalid_ohlcv_count
    )
    summary = FeatureAuditSummary(
        dataset_name=dataset_name,
        feature_version=feature_version,
        generated_at=datetime.now(UTC).isoformat(),
        row_count=len(features),
        symbol_count=int(features["symbol"].nunique()) if "symbol" in features else 0,
        date_min=_date_iso(features["date"].min()) if not features.empty else None,
        date_max=_date_iso(features["date"].max()) if not features.empty else None,
        duplicate_key_count=duplicate_key_count,
        invalid_ohlcv_count=observed_invalid_ohlcv_count,
        inf_value_count=int(inf_counts.sum()) if not inf_counts.empty else 0,
        extreme_value_count=extreme_count,
        passed_rows=int(status_counts.get("passed", 0)),
        warning_rows=int(status_counts.get("warning", 0)),
        failed_rows=int(status_counts.get("failed", 0)),
    )
    return audit, summary


def write_feature_audit_outputs(
    audit: pd.DataFrame,
    summary: FeatureAuditSummary,
    audit_output: Path,
    summary_output: Path,
) -> None:
    import json

    audit_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(audit_output, index=False)
    summary_output.write_text(json.dumps(asdict(summary), indent=2) + "\n")


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return _safe_plain_ratio(numerator, denominator) - 1


def _safe_plain_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.div(denominator.where(denominator.ne(0)))


def _invalid_ohlcv_count(frame: pd.DataFrame) -> int:
    required = ["open", "high", "low", "close", "volume"]
    if any(column not in frame.columns for column in required):
        return 0
    invalid_mask = (
        frame[required].isna().any(axis=1)
        | (frame["open"] <= 0)
        | (frame["high"] <= 0)
        | (frame["low"] <= 0)
        | (frame["close"] <= 0)
        | (frame["volume"] < 0)
        | (frame["high"] < frame["low"])
        | (frame["high"] < frame["open"])
        | (frame["high"] < frame["close"])
        | (frame["low"] > frame["open"])
        | (frame["low"] > frame["close"])
    )
    return int(invalid_mask.sum())


def _date_iso(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)
