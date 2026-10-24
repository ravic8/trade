"""Shared hard OHLCV invariants, independent of provider and request metadata."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from math import isfinite
from typing import Any

VALIDATION_VERSION = "daily-ohlcv-v2"


def daily_price_errors(row: Mapping[str, Any]) -> tuple[str, ...]:
    prices = [row.get(key) for key in ("open", "high", "low", "close")]
    errors = []
    if any(value is None or not _finite(value) for value in prices):
        errors.append("non_finite_price")
    else:
        opening, high, low, close = prices
        if any(value <= 0 for value in prices):
            errors.append("non_positive_price")
        if high < max(opening, low, close) or low > min(opening, high, close):
            errors.append("invalid_ohlc_range")
    volume = row.get("volume")
    if volume is None or not _finite(volume) or volume < 0 or int(volume) != volume:
        errors.append("invalid_volume")
    return tuple(errors)


def _finite(value: Any) -> bool:
    if isinstance(value, Decimal):
        return value.is_finite()
    try:
        return isfinite(value)
    except (TypeError, ValueError, OverflowError):
        return False
