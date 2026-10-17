from __future__ import annotations

import pandas as pd


def limit_daily_symbols(frame: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    if limit is None:
        return frame
    key_column = "InstrumentKey" if "InstrumentKey" in frame.columns else "instrument_key"
    if key_column not in frame.columns:
        return frame.head(0)
    keys = sorted(frame[key_column].dropna().astype(str).unique())[:limit]
    return frame[frame[key_column].astype(str).isin(keys)].copy()

