from __future__ import annotations

import json
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

DAILY_OPPORTUNITY_TARGET_VERSION_V1_0 = "daily_opportunity_outcomes_v1_0"

DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0 = [
    "session_return",
    "gap",
    "true_return",
    "upside",
    "downside",
    "giveback",
    "recovery",
    "session_range",
    "true_upside",
    "true_downside",
    "true_range",
]


@dataclass(frozen=True)
class OpportunityTargetAuditSummary:
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


class DailyOpportunityTargetBuilder:
    """Build completed-session outcome variables from OHLC and previous close.

    The formulas intentionally use Open as their denominator and implement the
    project-specific additive True Range definition. They are not Wilder ATR
    inputs and they must not be treated as information available before close.
    """

    def __init__(
        self,
        target_version: str = DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        computed_at: datetime | None = None,
        drop_invalid_rows: bool = False,
    ) -> None:
        self.target_version = target_version
        self.computed_at = computed_at or datetime.now(UTC)
        self.drop_invalid_rows = drop_invalid_rows

    def build(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        frame = normalize_daily_ohlcv(ohlcv)
        if self.drop_invalid_rows:
            frame = frame[~invalid_daily_ohlcv_mask(frame)].copy()
        validate_daily_ohlcv(frame)
        frame = frame.sort_values(["source", "instrument_key", "date"]).reset_index(
            drop=True
        )

        groups = frame.groupby(["source", "instrument_key"], sort=False)
        previous_close = groups["close"].shift(1)
        denominator = frame["open"].replace(0, np.nan)

        out = frame[
            [
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
            ]
        ].copy()
        out["previous_close"] = previous_close
        out["target_version"] = self.target_version
        out["computed_at"] = self.computed_at

        out["session_return"] = (frame["close"] - frame["open"]) / denominator
        out["gap"] = (frame["open"] - previous_close) / denominator
        out["true_return"] = (frame["close"] - previous_close) / denominator
        out["upside"] = (frame["high"] - frame["open"]) / denominator
        out["downside"] = (frame["open"] - frame["low"]) / denominator
        out["giveback"] = (frame["high"] - frame["close"]) / denominator
        out["recovery"] = (frame["close"] - frame["low"]) / denominator
        out["session_range"] = (frame["high"] - frame["low"]) / denominator
        out["true_upside"] = (
            frame["high"] - pd.concat([frame["open"], previous_close], axis=1).min(axis=1)
        ) / denominator
        out["true_downside"] = (
            pd.concat([frame["open"], previous_close], axis=1).max(axis=1) - frame["low"]
        ) / denominator
        out["true_range"] = out["true_upside"] + out["true_downside"]

        previous_close_targets = [
            "gap",
            "true_return",
            "true_upside",
            "true_downside",
            "true_range",
        ]
        out.loc[previous_close.isna(), previous_close_targets] = np.nan
        out["quality_status"] = np.where(
            out[DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0].isna().any(axis=1),
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
            "open",
            "high",
            "low",
            "close",
            "previous_close",
            "volume",
            "open_interest",
            *DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0,
        ]
        return out[ordered_columns]


def audit_daily_opportunity_targets(
    targets: pd.DataFrame,
    *,
    target_version: str = DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
    dataset_name: str = "daily_opportunity_outcomes",
    invalid_ohlcv_count: int = 0,
) -> tuple[pd.DataFrame, OpportunityTargetAuditSummary]:
    duplicate_key_count = int(
        targets.duplicated(["instrument_key", "source", "date", "target_version"]).sum()
    )
    numeric = targets[DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0]
    inf_counts = np.isinf(numeric).sum().astype(int)
    null_counts = numeric.isna().sum().astype(int)
    null_pct = (numeric.isna().mean() * 100).round(4)
    audit = pd.DataFrame(
        [
            {
                "target": column,
                "null_count": int(null_counts[column]),
                "null_pct": float(null_pct[column]),
                "inf_count": int(inf_counts[column]),
            }
            for column in DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0
        ]
    )
    status_counts = targets["quality_status"].value_counts().to_dict()
    summary = OpportunityTargetAuditSummary(
        dataset_name=dataset_name,
        target_version=target_version,
        generated_at=datetime.now(UTC).isoformat(),
        row_count=len(targets),
        symbol_count=int(targets["instrument_key"].nunique()),
        date_min=_date_iso(targets["date"].min()) if not targets.empty else None,
        date_max=_date_iso(targets["date"].max()) if not targets.empty else None,
        duplicate_key_count=duplicate_key_count,
        invalid_ohlcv_count=invalid_ohlcv_count,
        inf_value_count=int(inf_counts.sum()),
        passed_rows=int(status_counts.get("passed", 0)),
        warning_rows=int(status_counts.get("warning", 0)),
        failed_rows=int(status_counts.get("failed", 0)),
    )
    return audit, summary


def _date_iso(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def write_opportunity_target_audit_outputs(
    audit: pd.DataFrame,
    summary: OpportunityTargetAuditSummary,
    audit_output: Path,
    summary_output: Path,
) -> None:
    audit_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(audit_output, index=False)
    summary_output.write_text(json.dumps(asdict(summary), indent=2) + "\n")
