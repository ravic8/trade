"""Market-wide daily stats for the Candles page (Groww/Kite-style blocks)."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any


def compute_market_stats(rows: Iterable[dict[str, Any]], limit: int = 20) -> dict[str, Any]:
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol[str(row["symbol"])].append(row)
    if not by_symbol:
        return {
            "session_date": None,
            "near_52w_low": [],
            "near_52w_high": [],
            "top_gainers": [],
            "top_losers": [],
            "most_traded": [],
            "breadth": {"advances": 0, "declines": 0, "unchanged": 0},
        }

    session_date = max(row["date"] for group in by_symbol.values() for row in group)
    entries = []
    advances = declines = unchanged = 0
    for symbol, group in by_symbol.items():
        group.sort(key=lambda r: r["date"])
        latest = group[-1]
        if str(latest["date"]) != str(session_date):
            continue
        prev_close = group[-2]["close"] if len(group) > 1 else None
        close = float(latest["close"])
        high_52 = max(float(r["high"]) for r in group)
        low_52 = min(float(r["low"]) for r in group)
        change_pct = ((close / prev_close) - 1) * 100 if prev_close else None
        if change_pct is None or abs(change_pct) < 0.005:
            unchanged += 1
        elif change_pct > 0:
            advances += 1
        else:
            declines += 1
        entries.append(
            {
                "symbol": symbol,
                "instrument_key": latest["instrument_key"],
                "close": close,
                "prev_close": prev_close,
                "change_pct": round(change_pct, 2) if change_pct is not None else None,
                "volume": int(latest["volume"]),
                "high_52w": high_52,
                "low_52w": low_52,
                "pct_from_52w_low": round((close / low_52 - 1) * 100, 2) if low_52 else None,
                "pct_below_52w_high": round((1 - close / high_52) * 100, 2) if high_52 else None,
            }
        )

    def top(key: str, reverse: bool = False) -> list[dict[str, Any]]:
        valid = [e for e in entries if e[key] is not None]
        return sorted(valid, key=lambda e: e[key], reverse=reverse)[:limit]

    return {
        "session_date": str(session_date),
        "near_52w_low": top("pct_from_52w_low"),
        "near_52w_high": top("pct_below_52w_high"),
        "top_gainers": top("change_pct", reverse=True),
        "top_losers": top("change_pct"),
        "most_traded": sorted(entries, key=lambda e: e["volume"], reverse=True)[:limit],
        "breadth": {"advances": advances, "declines": declines, "unchanged": unchanged},
    }
