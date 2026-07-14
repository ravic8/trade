import lzma
import struct
from datetime import UTC, datetime

from trade_research.data.dukascopy_provider import (
    aggregate_ticks_to_ohlcv,
    dukascopy_hour_url,
    normalize_dukascopy_ticks,
)
from trade_research.universe import dukascopy_intraday_universe


def test_dukascopy_universe_uses_usdcnh_proxy_and_btcusd() -> None:
    instruments = dukascopy_intraday_universe()
    by_symbol = {item.symbol: item for item in instruments}

    assert by_symbol["USD/CNH"].dukascopy_id == "usdcnh"
    assert by_symbol["BTC/USD"].asset_class == "crypto"
    assert "USD/CNY" not in by_symbol


def test_dukascopy_hour_url_uses_zero_based_month() -> None:
    instrument = dukascopy_intraday_universe()[0]

    url = dukascopy_hour_url(
        "https://datafeed.dukascopy.com/datafeed",
        instrument,
        datetime(2026, 7, 1, 3, tzinfo=UTC),
    )

    assert url.endswith("/EURUSD/2026/06/01/03h_ticks.bi5")


def test_normalize_dukascopy_ticks_and_aggregate_to_5m() -> None:
    instrument = dukascopy_intraday_universe()[0]
    payload = lzma.compress(
        b"".join(
            [
                struct.pack(">iiiff", 0, 110010, 110000, 1.0, 2.0),
                struct.pack(">iiiff", 60_000, 110030, 110020, 1.0, 1.0),
                struct.pack(">iiiff", 300_000, 110050, 110040, 2.0, 3.0),
            ]
        )
    )

    ticks = normalize_dukascopy_ticks(
        payload,
        instrument=instrument,
        hour_start=datetime(2026, 7, 1, tzinfo=UTC),
    )
    candles = aggregate_ticks_to_ohlcv(ticks, instrument=instrument)

    assert len(ticks) == 3
    assert len(candles) == 2
    first = candles.iloc[0]
    assert first["Symbol"] == "EUR/USD"
    assert first["InstrumentKey"] == "DUKAS|EURUSD"
    assert first["Exchange"] == "FX"
    assert first["Interval"] == "5m"
    assert first["Open"] == 1.10005
    assert first["High"] == 1.10025
    assert first["Low"] == 1.10005
    assert first["Close"] == 1.10025
    assert first["Volume"] == 5.0
