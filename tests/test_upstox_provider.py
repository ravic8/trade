import asyncio
from datetime import date

import httpx
import pandas as pd

from trade_data.data.upstox import (
    AsyncUpstoxHistoricalDataProvider,
    UpstoxHistoricalDataProvider,
    UpstoxInstrumentMasterProvider,
    audit_daily_ohlcv,
    instrument_master_audit,
    map_liquid_universe_to_upstox,
)


def test_fetch_instrument_master_normalizes_complete_list() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {
                    "instrument_key": "NSE_EQ|INE040A01034",
                    "exchange": "NSE",
                    "segment": "NSE_EQ",
                    "instrument_type": "EQ",
                    "trading_symbol": "HDFCBANK",
                    "name": "HDFC Bank Limited",
                    "isin": "INE040A01034",
                    "lot_size": 1,
                    "tick_size": 0.05,
                }
            ],
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    provider = UpstoxInstrumentMasterProvider(client=client)

    frame = provider.fetch()
    audit = instrument_master_audit(frame)

    assert frame.loc[0, "instrument_key"] == "NSE_EQ|INE040A01034"
    assert frame.loc[0, "trading_symbol"] == "HDFCBANK"
    assert audit.rows == 1
    assert audit.nse_equity_rows == 1


def test_map_liquid_universe_to_upstox_matches_nse_equities() -> None:
    universe = pd.DataFrame(
        {
            "rank": [1, 2],
            "symbol": ["HDFCBANK", "MISSING"],
            "ticker": ["HDFCBANK.NS", "MISSING.NS"],
            "avg_daily_volume": [10_000_000, 1_000_000],
            "avg_daily_turnover": [12_000_000_000, 2_000_000_000],
            "trading_days": [124, 124],
            "zero_volume_ratio": [0.0, 0.0],
            "first_date": ["2026-01-01", "2026-01-01"],
            "last_date": ["2026-06-01", "2026-06-01"],
        }
    )
    instruments = pd.DataFrame(
        {
            "instrument_key": ["NSE_EQ|INE040A01034"],
            "exchange": ["NSE"],
            "segment": ["NSE_EQ"],
            "asset_type": ["EQ"],
            "trading_symbol": ["HDFCBANK"],
            "name": ["HDFC Bank Limited"],
            "isin": ["INE040A01034"],
            "lot_size": [1],
            "tick_size": [0.05],
        }
    )

    matched, unmatched = map_liquid_universe_to_upstox(universe, instruments)

    assert matched["symbol"].tolist() == ["HDFCBANK"]
    assert matched["instrument_key"].tolist() == ["NSE_EQ|INE040A01034"]
    assert unmatched["symbol"].tolist() == ["MISSING"]


def test_fetch_daily_candles_uses_v3_days_endpoint() -> None:
    requested_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_paths.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "candles": [
                        ["2026-06-17T00:00:00+05:30", 100.0, 110.0, 95.0, 105.0, 12345, 0]
                    ]
                },
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    provider = UpstoxHistoricalDataProvider("token", client=client)

    frame = provider.fetch_daily_candles(
        "NSE_EQ|INE040A01034",
        start=date(2026, 6, 1),
        end=date(2026, 6, 17),
        symbol="HDFCBANK",
    )

    assert requested_paths == [
        "/v3/historical-candle/NSE_EQ|INE040A01034/days/1/2026-06-17/2026-06-01"
    ]
    assert frame["Symbol"].tolist() == ["HDFCBANK"]
    assert frame["Volume"].tolist() == [12345]


def test_async_fetch_daily_candles_uses_v3_days_endpoint() -> None:
    requested_paths: list[str] = []

    async def run() -> pd.DataFrame:
        def handler(request: httpx.Request) -> httpx.Response:
            requested_paths.append(request.url.path)
            return httpx.Response(
                200,
                json={
                    "status": "success",
                    "data": {
                        "candles": [
                            [
                                "2026-06-17T00:00:00+05:30",
                                100.0,
                                110.0,
                                95.0,
                                105.0,
                                12345,
                                0,
                            ]
                        ]
                    },
                },
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        async with AsyncUpstoxHistoricalDataProvider("token", client=client) as provider:
            return await provider.fetch_daily_candles(
                "NSE_EQ|INE040A01034",
                start=date(2026, 6, 1),
                end=date(2026, 6, 17),
                symbol="HDFCBANK",
            )

    frame = asyncio.run(run())

    assert requested_paths == [
        "/v3/historical-candle/NSE_EQ|INE040A01034/days/1/2026-06-17/2026-06-01"
    ]
    assert frame["Symbol"].tolist() == ["HDFCBANK"]
    assert frame["Volume"].tolist() == [12345]


def test_audit_daily_ohlcv_flags_missing_and_null_rows() -> None:
    frame = pd.DataFrame(
        {
            "Date": [date(2026, 6, 16), date(2026, 6, 17), date(2026, 6, 17)],
            "Open": [100.0, 101.0, 101.0],
            "High": [110.0, 111.0, 111.0],
            "Low": [95.0, 96.0, 96.0],
            "Close": [105.0, None, None],
            "Volume": [1000, 0, 0],
            "InstrumentKey": ["A", "A", "A"],
            "Symbol": ["AAA", "AAA", "AAA"],
        }
    )
    expected = pd.DataFrame({"symbol": ["AAA", "BBB"], "instrument_key": ["A", "B"]})

    audit = audit_daily_ohlcv(frame, expected)

    aaa = audit[audit["symbol"].eq("AAA")].iloc[0]
    bbb = audit[audit["symbol"].eq("BBB")].iloc[0]
    assert aaa["duplicate_date_rows"] == 2
    assert aaa["null_ohlcv_rows"] == 2
    assert aaa["status"] == "failed"
    assert bbb["rows"] == 0
    assert bbb["status"] == "failed"
