from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select

from trade_research.control_plane.tables import market_data_replication_checkpoints_table
from trade_research.market_data.partition_reconciliation import (
    DailyPartitionReconciler,
    DailyReplicaRow,
    compare_daily_partitions,
    month_window,
    run_nse_daily_partition_reconciliation,
)
from trade_research.storage.clickhouse import ClickHouseMarketDataRepository
from trade_research.storage.timescale import ohlcv_daily_table, symbols_table

NOW = datetime(2026, 8, 31, 13, 0, tzinfo=UTC)


def _source_engine(*, mapped: bool = True):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    symbols_table.create(engine)
    ohlcv_daily_table.create(engine)
    market_data_replication_checkpoints_table.create(engine)
    with engine.begin() as connection:
        if mapped:
            connection.execute(
                symbols_table.insert().values(
                    symbol="RELIANCE",
                    exchange="NSE",
                    yahoo_symbol="RELIANCE.NS",
                    canonical_instrument_id="NSE_EQ|RELIANCE",
                    provider_instrument_key="YF|RELIANCE.NS",
                    source="nse",
                    is_active=True,
                    pipeline_eligibility="incremental",
                    provider_status="available",
                    instrument_type="equity",
                    reconciliation_status="not_required",
                    listing_status="active",
                    fetched_at=NOW,
                )
            )
        connection.execute(
            ohlcv_daily_table.insert().values(
                instrument_key="YF|RELIANCE.NS",
                source="yfinance",
                date=date(2026, 8, 31),
                symbol="RELIANCE",
                exchange="NSE",
                open=100.0,
                high=105.0,
                low=99.0,
                close=104.0,
                volume=1000,
                open_interest=None,
                fetched_at=NOW,
                quality_status="ok",
            )
        )
    return engine


def _replica_row(*, close: str = "104", instrument_id: str = "NSE_EQ|RELIANCE"):
    return {
        "instrument_id": instrument_id,
        "provider_symbol": "RELIANCE.NS",
        "symbol": "RELIANCE",
        "exchange": "NSE",
        "session_date": date(2026, 8, 31),
        "open": Decimal("100"),
        "high": Decimal("105"),
        "low": Decimal("99"),
        "close": Decimal(close),
        "volume": 1000,
        "source": "yfinance",
        "provider_timestamp": NOW,
    }


class _ClickHousePartitionRepository:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = {
            (row["instrument_id"], row["source"], row["session_date"]): row
            for row in (rows or [])
        }
        self.insert_calls: list[list] = []

    def read_daily_partition(self, **_filters):
        return list(self.rows.values())

    def insert_validated(
        self,
        candles,
        *,
        source_run_id: str,
        workspace_id: str,
        version: int,
    ) -> int:
        materialized = list(candles)
        self.insert_calls.append(materialized)
        assert source_run_id == "repair-run"
        assert workspace_id == "default"
        assert version == int(NOW.timestamp() * 1_000_000)
        for candle in materialized:
            row = {
                "instrument_id": candle.instrument_id,
                "provider_symbol": candle.provider_symbol,
                "symbol": candle.symbol,
                "exchange": candle.exchange,
                "session_date": candle.session_date,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
                "source": candle.provider,
                "provider_timestamp": candle.provider_timestamp,
            }
            self.rows[(candle.instrument_id, candle.provider, candle.session_date)] = row
        return len(materialized)


def test_partition_repair_upserts_missing_or_divergent_rows_and_reconciles() -> None:
    engine = _source_engine()
    clickhouse = _ClickHousePartitionRepository([_replica_row(close="103")])

    result = DailyPartitionReconciler(
        engine=engine,
        clickhouse=clickhouse,
    ).reconcile(
        month="2026-08",
        repair=True,
        source_run_id="repair-run",
        at=NOW,
    )

    assert result.status == "reconciled"
    assert len(result.initial.divergent_identities) == 1
    assert result.repaired_rows == 1
    assert result.final.reconciled is True
    repaired = clickhouse.insert_calls[0][0]
    assert repaired.adapter_version == "postgresql-authority-repair-v1"
    assert repaired.close == Decimal("104.00000000")
    with engine.connect() as connection:
        checkpoint = connection.execute(
            select(market_data_replication_checkpoints_table)
        ).mappings().one()
    assert checkpoint["status"] == "reconciled"
    assert checkpoint["dataset_key"] == "ohlcv_daily:2026-08"
    assert checkpoint["source_row_count"] == 1
    assert checkpoint["destination_row_count"] == 1
    assert checkpoint["details"]["initial"]["divergent_rows"] == 1
    assert checkpoint["details"]["repaired_rows"] == 1


def test_audit_records_mismatch_without_writing() -> None:
    engine = _source_engine()
    clickhouse = _ClickHousePartitionRepository()

    result = DailyPartitionReconciler(
        engine=engine,
        clickhouse=clickhouse,
    ).reconcile(
        month="2026-08",
        repair=False,
        source_run_id="audit-run",
        at=NOW,
    )

    assert result.status == "mismatch"
    assert len(result.final.missing_identities) == 1
    assert result.repaired_rows == 0
    assert clickhouse.insert_calls == []
    with engine.connect() as connection:
        checkpoint = connection.execute(
            select(market_data_replication_checkpoints_table)
        ).mappings().one()
    assert checkpoint["status"] == "mismatch"


def test_clickhouse_read_failure_records_failed_checkpoint() -> None:
    engine = _source_engine()

    class _UnavailableClickHouse(_ClickHousePartitionRepository):
        def read_daily_partition(self, **_filters):
            raise RuntimeError("ClickHouse unavailable")

    with pytest.raises(RuntimeError, match="ClickHouse unavailable"):
        DailyPartitionReconciler(
            engine=engine,
            clickhouse=_UnavailableClickHouse(),
        ).reconcile(
            month="2026-08",
            source_run_id="failed-run",
            at=NOW,
        )

    with engine.connect() as connection:
        checkpoint = connection.execute(
            select(market_data_replication_checkpoints_table)
        ).mappings().one()
    assert checkpoint["status"] == "failed"
    assert checkpoint["error_message"] == "ClickHouse unavailable"
    assert checkpoint["details"]["initial"]["missing_rows"] == 1


def test_repair_never_deletes_unexpected_replica_rows() -> None:
    engine = _source_engine()
    clickhouse = _ClickHousePartitionRepository(
        [
            _replica_row(),
            _replica_row(instrument_id="NSE_EQ|UNEXPECTED"),
        ]
    )

    result = DailyPartitionReconciler(
        engine=engine,
        clickhouse=clickhouse,
    ).reconcile(
        month="2026-08",
        repair=True,
        source_run_id="repair-run",
        at=NOW,
    )

    assert result.status == "mismatch"
    assert len(result.final.unexpected_identities) == 1
    assert result.repaired_rows == 0
    assert clickhouse.insert_calls == []
    with engine.connect() as connection:
        checkpoint = connection.execute(
            select(market_data_replication_checkpoints_table)
        ).mappings().one()
    assert checkpoint["details"]["unexpected_rows_require_manual_review"] is True


def test_partition_reconciliation_rejects_unmapped_and_unbounded_sources() -> None:
    with pytest.raises(ValueError, match="unmapped canonical instruments"):
        DailyPartitionReconciler(
            engine=_source_engine(mapped=False),
            clickhouse=_ClickHousePartitionRepository(),
        ).reconcile(month="2026-08")

    bounded_engine = _source_engine()
    with bounded_engine.begin() as connection:
        connection.execute(
            symbols_table.insert().values(
                symbol="INFY",
                exchange="NSE",
                yahoo_symbol="INFY.NS",
                canonical_instrument_id="NSE_EQ|INFY",
                provider_instrument_key="YF|INFY.NS",
                source="nse",
                is_active=True,
                pipeline_eligibility="incremental",
                provider_status="available",
                instrument_type="equity",
                reconciliation_status="not_required",
                listing_status="active",
                fetched_at=NOW,
            )
        )
        connection.execute(
            ohlcv_daily_table.insert().values(
                instrument_key="YF|INFY.NS",
                source="yfinance",
                date=date(2026, 8, 31),
                symbol="INFY",
                exchange="NSE",
                open=50.0,
                high=52.0,
                low=49.0,
                close=51.0,
                volume=500,
                fetched_at=NOW,
                quality_status="ok",
            )
        )
    with pytest.raises(ValueError, match="exceeds max_rows"):
        DailyPartitionReconciler(
            engine=bounded_engine,
            clickhouse=_ClickHousePartitionRepository(),
            max_rows=1,
        ).reconcile(month="2026-08")


def test_partition_comparison_normalizes_clickhouse_decimal_scale() -> None:
    row = DailyReplicaRow(
        instrument_id="NSE_EQ|RELIANCE",
        provider_symbol="RELIANCE.NS",
        symbol="RELIANCE",
        exchange="NSE",
        session_date=date(2026, 8, 31),
        open=Decimal("100"),
        high=Decimal("105"),
        low=Decimal("99"),
        close=Decimal("104"),
        volume=1000,
        provider="yfinance",
        provider_timestamp=NOW,
    )
    scaled = DailyReplicaRow(
        **{**vars(row), "close": Decimal("104.00000000")}
    )

    assert compare_daily_partitions([row], [scaled]).reconciled is True


@pytest.mark.parametrize(
    ("month", "expected"),
    [
        ("2026-02", (date(2026, 2, 1), date(2026, 2, 28))),
        ("2024-02", (date(2024, 2, 1), date(2024, 2, 29))),
        ("2025-12", (date(2025, 12, 1), date(2025, 12, 31))),
    ],
)
def test_month_window_is_clickhouse_partition_aligned(month, expected) -> None:
    assert month_window(month) == expected


@pytest.mark.parametrize("month", ["2026-8", "2026-13", "not-a-month"])
def test_month_window_rejects_noncanonical_values(month) -> None:
    with pytest.raises(ValueError, match="YYYY-MM"):
        month_window(month)


def test_configured_reconciliation_fails_closed_on_storage_flags() -> None:
    with pytest.raises(RuntimeError, match="ClickHouse must be enabled"):
        run_nse_daily_partition_reconciliation(
            month="2026-08",
            settings=SimpleNamespace(clickhouse_enabled=False),
        )
    with pytest.raises(RuntimeError, match="writes must be enabled"):
        run_nse_daily_partition_reconciliation(
            month="2026-08",
            repair=True,
            settings=SimpleNamespace(
                clickhouse_enabled=True,
                clickhouse_write_enabled=False,
            ),
        )


class _QueryResult:
    column_names = (
        "instrument_id",
        "provider_symbol",
        "symbol",
        "exchange",
        "session_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "provider_timestamp",
    )
    result_rows = [tuple(_replica_row()[name] for name in column_names)]


class _QueryClient:
    def __init__(self) -> None:
        self.query_text = ""
        self.parameters = {}

    def query(self, query, parameters=None):
        self.query_text = query
        self.parameters = parameters
        return _QueryResult()


def test_clickhouse_partition_read_is_scoped_and_final() -> None:
    client = _QueryClient()
    repository = ClickHouseMarketDataRepository(client)

    rows = repository.read_daily_partition(
        workspace_id="default",
        provider="yfinance",
        exchange="NSE",
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 31),
    )

    assert rows[0]["instrument_id"] == "NSE_EQ|RELIANCE"
    assert "ohlcv_daily FINAL" in client.query_text
    assert "workspace_id = {workspace_id:String}" in client.query_text
    assert "source = {provider:String}" in client.query_text
    assert client.parameters["window_start"] == date(2026, 8, 1)
