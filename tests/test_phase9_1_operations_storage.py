from datetime import UTC, date, datetime

from trade_research.storage.timescale import (
    TimescaleStore,
    adaptive_rate_state_table,
    metadata,
    ohlcv_daily_table,
    pipeline_work_items_table,
    symbol_lifecycle_events_table,
    symbols_table,
    universe_snapshots_table,
)

NOW = datetime(2026, 7, 19, 6, 30, tzinfo=UTC)


def test_operations_storage_queries_durable_pipeline_state() -> None:
    store = TimescaleStore("sqlite://")
    metadata.create_all(store.engine)
    with store.engine.begin() as connection:
        connection.execute(
            symbols_table.insert(),
            {
                "symbol": "RY",
                "exchange": "TSX",
                "yahoo_symbol": "RY.TO",
                "name": "Royal Bank of Canada",
                "source": "tmx_symbol_directory",
                "is_active": True,
                "canonical_instrument_id": "eq-ry",
                "provider_instrument_key": "YF|RY.TO",
                "fetched_at": NOW,
            },
        )
        connection.execute(
            pipeline_work_items_table.insert(),
            {
                "work_item_id": "work-1",
                "idempotency_key": "phase9-work-1",
                "work_type": "daily_incremental",
                "provider": "yfinance",
                "exchange": "TSX",
                "canonical_instrument_id": "eq-ry",
                "provider_symbol": "RY.TO",
                "interval": "1d",
                "window_start": date(2026, 7, 17),
                "window_end": date(2026, 7, 17),
                "priority": 10,
                "status": "retry_wait",
                "attempt_count": 2,
                "max_attempts": 9,
                "next_attempt_at": NOW,
                "created_at": NOW,
                "updated_at": NOW,
            },
        )
        connection.execute(
            symbol_lifecycle_events_table.insert(),
            {
                "event_id": "event-1",
                "canonical_instrument_id": "eq-ry",
                "exchange": "TSX",
                "event_type": "added",
                "new_value": {"is_active": True},
                "snapshot_id": "snapshot-new",
                "created_at": NOW,
            },
        )
        connection.execute(
            adaptive_rate_state_table.insert(),
            {
                "provider": "yfinance",
                "current_rpm": 300,
                "last_safe_rpm": 300,
                "minimum_rpm": 30,
                "maximum_rpm": 600,
                "current_concurrency": 4,
                "consecutive_healthy_windows": 5,
                "circuit_state": "closed",
                "recent_error_rate": 0.0,
                "latency_baseline_ms": 7500.0,
                "updated_at": NOW,
            },
        )
        connection.execute(
            ohlcv_daily_table.insert(),
            [
                {
                    "instrument_key": "YF|RY.TO",
                    "source": "yfinance",
                    "date": date(2026, 7, 16),
                    "symbol": "RY.TO",
                    "exchange": "TSX",
                    "open": 200.0,
                    "high": 202.0,
                    "low": 199.0,
                    "close": 201.0,
                    "volume": 1_000,
                    "fetched_at": NOW,
                    "quality_status": "ok",
                },
                {
                    "instrument_key": "YF|RY.TO",
                    "source": "yfinance",
                    "date": date(2026, 7, 17),
                    "symbol": "RY.TO",
                    "exchange": "TSX",
                    "open": 201.0,
                    "high": 203.0,
                    "low": 200.0,
                    "close": 202.0,
                    "volume": 1_100,
                    "fetched_at": NOW,
                    "quality_status": "suspicious",
                },
            ],
        )
        connection.execute(
            universe_snapshots_table.insert(),
            [
                {
                    "snapshot_id": "snapshot-old",
                    "exchange": "TSX",
                    "source": "tmx_symbol_directory",
                    "status": "accepted",
                    "fetched_at": datetime(2026, 7, 18, tzinfo=UTC),
                    "symbol_count": 640,
                    "validation_json": {},
                    "created_at": datetime(2026, 7, 18, tzinfo=UTC),
                },
                {
                    "snapshot_id": "snapshot-new",
                    "exchange": "TSX",
                    "source": "tmx_symbol_directory",
                    "status": "accepted",
                    "fetched_at": NOW,
                    "symbol_count": 645,
                    "validation_json": {"accepted": True},
                    "created_at": NOW,
                },
            ],
        )

    queue = store.pipeline_work_queue_groups(provider="yfinance", exchange="TSX")
    assert queue[0]["items"] == 1
    assert queue[0]["symbols"] == 1
    assert queue[0]["maximum_attempts"] == 2

    work = store.pipeline_work_items_page(
        provider="yfinance",
        exchange="TSX",
        status="retry_wait",
        work_type="daily_incremental",
        symbol="ry",
    )
    assert work["total"] == 1
    assert work["rows"][0]["provider_symbol"] == "RY.TO"

    lifecycle = store.symbol_lifecycle_events_page(
        exchange="TSX",
        event_type="added",
        symbol="ry",
    )
    assert lifecycle["total"] == 1
    assert lifecycle["rows"][0]["symbol"] == "RY"

    rates = store.adaptive_rate_states(provider="yfinance")
    assert rates[0]["current_concurrency"] == 4

    freshness = store.provider_data_freshness(provider="yfinance", exchange="TSX")
    assert freshness[0]["rows"] == 2
    assert freshness[0]["symbols"] == 1
    assert freshness[0]["suspicious_rows"] == 1
    assert freshness[0]["latest_date"] == date(2026, 7, 17)

    snapshots = store.latest_accepted_universe_snapshots(exchange="TSX")
    assert [row["snapshot_id"] for row in snapshots] == ["snapshot-new"]

    exchange_symbol = store.resolve_provider_instruments(
        ["RY"], source="yfinance", exchange="TSX"
    )
    yahoo_symbol = store.resolve_provider_instruments(
        ["RY.TO"], source="yfinance", exchange="TSX"
    )
    assert exchange_symbol[0]["instrument_key"] == "YF|RY.TO"
    assert yahoo_symbol[0]["instrument_key"] == "YF|RY.TO"
