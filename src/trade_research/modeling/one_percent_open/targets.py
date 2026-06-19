"""Target definitions for the 1% open model."""

import pandas as pd


def reached_one_percent_from_open(frame: pd.DataFrame) -> pd.Series:
    """Return whether each row's high traded at least 1% above its open."""
    return frame["High"] >= frame["Open"] * 1.01
