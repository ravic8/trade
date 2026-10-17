from datetime import UTC, date, datetime, timedelta

from sqlalchemy import create_engine

from trade_data.storage.timescale import (
    TimescaleStore,
    ingestion_runs_table,
    ohlcv_daily_table,
    symbols_table,
    universe_snapshot_members_table,
    universe_snapshots_table,
)

NOW = datetime(2026, 10, 16, 13, tzinfo=UTC)


def test_status_uses_latest_accepted_universe_and_daily_source_only():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        symbols_table,
        universe_snapshots_table,
        universe_snapshot_members_table,
        ohlcv_daily_table,
        ingestion_runs_table,
    ):
        table.create(engine)
    store = TimescaleStore.__new__(TimescaleStore)
    store.engine = engine
    with engine.begin() as connection:
        connection.execute(
            symbols_table.insert(),
            [
                {
                    "symbol": symbol,
                    "exchange": "NSE",
                    "source": "nse",
                    "fetched_at": NOW,
                    "canonical_instrument_id": symbol,
                    "provider_instrument_key": "YF|" + symbol,
                    "is_active": symbol != "INACTIVE",
                }
                for symbol in ("AAA", "BBB", "OLD", "INACTIVE")
            ],
        )
        connection.execute(
            universe_snapshots_table.insert(),
            [
                {
                    "snapshot_id": identity,
                    "exchange": "NSE",
                    "source": "nse",
                    "status": status,
                    "fetched_at": NOW + timedelta(days=offset),
                    "created_at": NOW,
                }
                for identity, status, offset in (
                    ("old", "accepted", -1),
                    ("current", "accepted", 0),
                    ("rejected-newer", "rejected", 1),
                )
            ],
        )
        connection.execute(
            universe_snapshot_members_table.insert(),
            [
                {
                    "snapshot_id": identity,
                    "canonical_instrument_id": symbol,
                    "exchange_symbol": symbol,
                }
                for identity, symbol in (
                    ("old", "OLD"),
                    ("current", "AAA"),
                    ("current", "BBB"),
                    ("current", "INACTIVE"),
                    ("rejected-newer", "OLD"),
                )
            ],
        )

        def candle(symbol, day, *, source="yfinance", quality="ok"):
            return {
                "instrument_key": "YF|" + symbol,
                "symbol": symbol,
                "exchange": "NSE",
                "source": source,
                "date": day,
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 100,
                "fetched_at": NOW,
                "quality_status": quality,
            }

        connection.execute(
            ohlcv_daily_table.insert(),
            [
                candle("AAA", date(2026, 10, 16)),
                candle("BBB", date(2026, 10, 15)),
                candle("AAA", date(2026, 10, 17), quality="invalid"),
                candle("BBB", date(2026, 10, 18), source="upstox"),
                candle("OLD", date(2026, 10, 19)),
                candle("INACTIVE", date(2026, 10, 19)),
            ],
        )
        connection.execute(
            ingestion_runs_table.insert(),
            [
                {
                    "run_id": identity,
                    "job_name": job,
                    "source": source,
                    "exchange": "NSE",
                    "status": status,
                    "started_at": NOW + timedelta(hours=offset),
                }
                for identity, job, source, status, offset in (
                    ("daily", "yfinance_nse_eq_daily_ohlcv", "yfinance", "success", 0),
                    ("hourly", "nse_hourly_ohlcv", "yfinance", "failed", 1),
                    ("worker", "yfinance_daily_work_queue", "yfinance", "success", 1.5),
                    ("other-source", "nse_daily_ohlcv", "upstox", "failed", 2),
                )
            ],
        )
    rows = store.market_status()
    assert len(rows) == 1
    assert rows[0]["exchange"] == "NSE"
    assert rows[0]["universe_size"] == 2
    assert rows[0]["latest_candle"] == date(2026, 10, 16)
    assert rows[0]["candle_symbols"] == 1
    assert rows[0]["last_ohlcv_status"] == "success"
    assert rows[0]["last_ohlcv_run"] == (NOW + timedelta(hours=1.5)).replace(tzinfo=None)
    # Existing observations alone cannot invent an accepted universe.
    with engine.begin() as connection:
        connection.execute(universe_snapshots_table.update().values(status="rejected"))
    assert store.market_status() == []


def test_market_status_api_reports_latest_session_coverage(monkeypatch):
    from types import SimpleNamespace

    from trade_data.api import app as api_module

    monkeypatch.setattr(
        api_module,
        "_store",
        lambda: SimpleNamespace(
            market_status=lambda: [
                {
                    "exchange": "NSE",
                    "universe_size": 2,
                    "latest_candle": date(2026, 10, 16),
                    "candle_symbols": 1,
                    "last_ohlcv_run": NOW,
                    "last_ohlcv_status": "success",
                }
            ]
        ),
    )
    row = api_module.market_status()[0]
    assert row["latestCandle"] == "2026-10-16"
    assert row["staleSymbols"] == 1
    assert row["dataQualityScore"] == 50.0
