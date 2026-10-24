"""Replay independently verified corrections from the existing quality ledger.

Raw snapshots remain provider originals. A correction applies only to its known
bad or known corrected basis; an unexpected revision requires re-verification.
"""

from __future__ import annotations

from collections.abc import Mapping
from math import isclose
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine

from trade_data.control_plane.tables import market_data_quality_outcomes_table as quality
from trade_data.market_data.price_quality import daily_price_errors

CORRECTION_REASON = "source_verified_correction"
CORRECTION_RUN_ID = "nse-daily-verified-corrections-v1"


def apply_verified_daily_corrections(
    engine: Engine,
    frame: pd.DataFrame,
    *,
    exchange: str,
    provider: str,
) -> pd.DataFrame:
    if frame.empty or exchange.upper() != "NSE" or provider != "yfinance":
        return frame
    normalized = "instrument_key" in frame.columns
    key_col, date_col = ("instrument_key", "date") if normalized else ("InstrumentKey", "Date")
    symbols = frame[key_col].astype(str).str.removeprefix("YF|").unique().tolist()
    query = (
        select(quality.c.session_date, quality.c.details)
        .where(
            quality.c.source_run_id == CORRECTION_RUN_ID,
            quality.c.workspace_id == "default",
            quality.c.exchange == exchange.upper(),
            quality.c.provider == provider,
            quality.c.interval == "1d",
            quality.c.reason_code == CORRECTION_REASON,
            quality.c.status == "valid",
            quality.c.provider_symbol.in_(symbols),
            quality.c.session_date.between(frame[date_col].min(), frame[date_col].max()),
        )
        .order_by(quality.c.observed_at, quality.c.quality_outcome_id)
    )
    with engine.connect() as c:
        corrections = {
            (row.details["instrument_key"], row.session_date): row.details
            for row in c.execute(query)
        }
    if not corrections:
        return frame
    result = frame.copy()
    price_cols = ["open", "high", "low", "close", "volume"]
    names = {k: k if normalized else k.title() for k in price_cols}
    for index, row in result.iterrows():
        correction = corrections.get((row[key_col], row[date_col]))
        if correction is None:
            continue
        current = {k: row[names[k]] for k in price_cols}
        values = corrected_values(current, correction)
        for key, value in values.items():
            result.at[index, names[key]] = value
        if not normalized and "AdjClose" in result and pd.notna(row.get("AdjClose")):
            result.at[index, "AdjClose"] = (
                row["AdjClose"] * values["close"] / current["close"]
                if current["close"] > 0
                else float("nan")
            )
        if "quality_status" in result:
            result.at[index, "quality_status"] = "ok"
    return result


def corrected_values(current: Mapping[str, Any], correction: Mapping[str, Any]) -> dict[str, Any]:
    before, after = correction["before_prices"], correction["after_prices"]
    if not correction.get("evidence_sha256") or not correction.get("evidence_url"):
        raise ValueError("Verified correction is missing source evidence")
    if daily_price_errors(after):
        raise ValueError("Verified correction violates OHLCV invariants")

    def matches(expected: Mapping[str, Any]) -> bool:
        return (
            all(
                isclose(float(current[k]), float(expected[k]), rel_tol=1e-7, abs_tol=1e-5)
                for k in ("open", "high", "low", "close")
            )
            and current["volume"] == expected["volume"]
        )

    prior_inputs = correction.get("additional_before_prices", [])
    if not isinstance(prior_inputs, list) or any(
        not isinstance(value, Mapping)
        or not all(key in value for key in ("open", "high", "low", "close", "volume"))
        for value in prior_inputs
    ):
        raise ValueError("Verified correction has malformed prior input evidence")
    # A later volume correction may follow a previously verified price repair.
    # Accept only complete observations explicitly preserved in the evidence;
    # never infer combinations of independently observed field values.
    if not any(matches(value) for value in (before, after, *prior_inputs)):
        raise ValueError("Verified correction basis changed; source re-verification required")
    return dict(after)
