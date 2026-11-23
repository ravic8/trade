from datetime import date, timedelta

from trade_data.api.market_stats import compute_market_stats


def rows_for(symbol, closes, start=date(2025, 1, 1)):
    return [
        {
            "symbol": symbol,
            "instrument_key": f"NSE:{symbol}",
            "date": start + timedelta(days=i * 30),
            "close": c,
            "high": c * 1.02,
            "low": c * 0.98,
            "volume": 1000 + i,
        }
        for i, c in enumerate(closes)
    ]


def test_market_stats_rankings():
    rows = []
    rows += rows_for("NEARLOW", [100, 52, 51])  # close just above 52w low (~50)
    rows += rows_for("NEARHIGH", [50, 95, 99])  # close just below 52w high (~101)
    rows += rows_for("GAINER", [100, 100, 110])
    rows += rows_for("LOSER", [100, 100, 90])
    stats = compute_market_stats(rows, limit=5)
    assert stats["near_52w_low"][0]["symbol"] == "NEARLOW"
    assert stats["near_52w_high"][0]["symbol"] == "NEARHIGH"
    assert stats["top_gainers"][0]["symbol"] == "GAINER"
    assert stats["top_gainers"][0]["change_pct"] == 10.0
    assert stats["top_losers"][0]["symbol"] == "LOSER"
    assert stats["breadth"] == {"advances": 2, "declines": 2, "unchanged": 0}


def test_market_stats_empty():
    stats = compute_market_stats([])
    assert stats["session_date"] is None
    assert stats["top_gainers"] == []
