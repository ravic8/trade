from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trade_research.features.daily_technical import (
    invalid_daily_ohlcv_mask,
    normalize_daily_ohlcv,
    validate_daily_ohlcv,
)

DAILY_FORWARD_TARGET_VERSION_V1_0 = "daily_v1_forward_returns_v1_0"

DAILY_FORWARD_TARGET_COLUMNS_V1_0 = [
    "forward_ret_1d",
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "forward_ret_60d",
    "forward_outperform_universe_20d",
    "top_quantile_forward_return_20d",
]


@dataclass(frozen=True)
class TargetAuditSummary:
    dataset_name: str
    target_version: str
    generated_at: str
    row_count: int
    symbol_count: int
    date_min: str | None
    date_max: str | None
    duplicate_key_count: int
    invalid_ohlcv_count: int
    inf_value_count: int
    passed_rows: int
    warning_rows: int
    failed_rows: int


class DailyForwardTargetBuilder:
    def __init__(
        self,
        target_version: str = DAILY_FORWARD_TARGET_VERSION_V1_0,
        computed_at: datetime | None = None,
        drop_invalid_rows: bool = False,
        top_quantile: float = 0.80,
    ) -> None:
        self.target_version = target_version
        self.computed_at = computed_at or datetime.now(UTC)
        self.drop_invalid_rows = drop_invalid_rows
        self.top_quantile = top_quantile

    def build(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        frame = normalize_daily_ohlcv(ohlcv)
        if self.drop_invalid_rows:
            frame = frame[~invalid_daily_ohlcv_mask(frame)].copy()
        validate_daily_ohlcv(frame)
        frame = frame.sort_values(["instrument_key", "date"]).reset_index(drop=True)

        groups = frame.groupby("instrument_key", group_keys=False, sort=False)
        out = frame[
            ["instrument_key", "symbol", "exchange", "source", "date", "close"]
        ].copy()
        out["target_version"] = self.target_version
        out["computed_at"] = self.computed_at

        for horizon in [1, 5, 10, 20, 60]:
            future_close = groups["close"].shift(-horizon)
            out[f"forward_ret_{horizon}d"] = future_close / frame["close"] - 1

        universe_mean_20d = out.groupby("date")["forward_ret_20d"].transform("mean")
        out["forward_outperform_universe_20d"] = out["forward_ret_20d"] - universe_mean_20d

        percentile_rank = out.groupby("date")["forward_ret_20d"].rank(pct=True, method="average")
        top_quantile = pd.Series(pd.NA, index=out.index, dtype="boolean")
        complete_20d = out["forward_ret_20d"].notna()
        top_quantile.loc[complete_20d] = percentile_rank.loc[complete_20d] >= self.top_quantile
        out["top_quantile_forward_return_20d"] = top_quantile

        out["quality_status"] = np.where(
            out[DAILY_FORWARD_TARGET_COLUMNS_V1_0].isna().any(axis=1),
            "warning",
            "passed",
        )

        ordered_columns = [
            "instrument_key",
            "symbol",
            "exchange",
            "source",
            "date",
            "target_version",
            "computed_at",
            "quality_status",
            *DAILY_FORWARD_TARGET_COLUMNS_V1_0,
        ]
        return out[ordered_columns]


def audit_daily_forward_targets(
    targets: pd.DataFrame,
    target_version: str = DAILY_FORWARD_TARGET_VERSION_V1_0,
    dataset_name: str = "daily_v1_forward_returns",
    invalid_ohlcv_count: int | None = None,
) -> tuple[pd.DataFrame, TargetAuditSummary]:
    duplicate_key_count = int(
        targets.duplicated(["instrument_key", "date", "target_version"]).sum()
    )
    target_columns = [
        column for column in DAILY_FORWARD_TARGET_COLUMNS_V1_0 if column in targets.columns
    ]
    numeric_targets = targets[target_columns].select_dtypes(include=[np.number])
    inf_counts = (
        np.isinf(numeric_targets).sum().astype(int)
        if not numeric_targets.empty
        else pd.Series(dtype=int)
    )
    null_counts = targets[target_columns].isna().sum().astype(int)
    null_pct = (targets[target_columns].isna().mean() * 100).round(4)
    audit = pd.DataFrame(
        [
            {
                "target": column,
                "null_count": int(null_counts.get(column, 0)),
                "null_pct": float(null_pct.get(column, 0.0)),
                "inf_count": int(inf_counts.get(column, 0)),
            }
            for column in target_columns
        ]
    )

    status_counts = targets["quality_status"].value_counts().to_dict()
    summary = TargetAuditSummary(
        dataset_name=dataset_name,
        target_version=target_version,
        generated_at=datetime.now(UTC).isoformat(),
        row_count=len(targets),
        symbol_count=int(targets["symbol"].nunique()) if "symbol" in targets else 0,
        date_min=_date_iso(targets["date"].min()) if not targets.empty else None,
        date_max=_date_iso(targets["date"].max()) if not targets.empty else None,
        duplicate_key_count=duplicate_key_count,
        invalid_ohlcv_count=int(invalid_ohlcv_count or 0),
        inf_value_count=int(inf_counts.sum()) if not inf_counts.empty else 0,
        passed_rows=int(status_counts.get("passed", 0)),
        warning_rows=int(status_counts.get("warning", 0)),
        failed_rows=int(status_counts.get("failed", 0)),
    )
    return audit, summary


def write_target_audit_outputs(
    audit: pd.DataFrame,
    summary: TargetAuditSummary,
    audit_output: Path,
    summary_output: Path,
) -> None:
    import json

    audit_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(audit_output, index=False)
    summary_output.write_text(json.dumps(asdict(summary), indent=2) + "\n")


def _date_iso(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)
