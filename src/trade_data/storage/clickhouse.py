from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Any, Protocol

from trade_data.config import Settings
from trade_data.market_data.contracts import CandleInterval, MarketCandle, candle_content_sha256

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ClickHouseClient(Protocol):
    def command(
        self,
        command: str,
        parameters: Mapping[str, Any] | None = None,
    ) -> Any: ...

    def insert(
        self,
        table: str,
        data: Sequence[Sequence[Any]],
        column_names: Sequence[str],
    ) -> Any: ...

    def query(self, query: str, parameters: Mapping[str, Any] | None = None) -> Any: ...


class StorageReadOnlyError(PermissionError):
    """Raised when a write is attempted through a read-only repository."""


def create_clickhouse_client(settings: Settings) -> ClickHouseClient:
    if not settings.clickhouse_enabled:
        raise RuntimeError("ClickHouse is disabled")
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure,
        connect_timeout=settings.clickhouse_connect_timeout_seconds,
        send_receive_timeout=settings.clickhouse_query_timeout_seconds,
    )


class _Repository:
    def __init__(
        self,
        client: ClickHouseClient,
        *,
        database: str = "research",
        write_enabled: bool = False,
    ) -> None:
        if not _IDENTIFIER.fullmatch(database):
            raise ValueError("invalid ClickHouse database")
        self._client = client
        self._database = database
        self._write_enabled = write_enabled

    def _insert(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        columns: Sequence[str],
    ) -> int:
        if not self._write_enabled:
            raise StorageReadOnlyError("ClickHouse repository is read-only")
        materialized = list(rows)
        if not materialized:
            return 0
        data = [[row.get(column) for column in columns] for row in materialized]
        self._client.insert(
            f"{self._database}.{table}",
            data,
            column_names=list(columns),
        )
        return len(data)


class ClickHouseMarketDataRepository(_Repository):
    """Append validated canonical candles to the analytical replica."""

    MAX_PARTITIONS_PER_INSERT = 50

    DAILY_COLUMNS = (
        "workspace_id",
        "instrument_id",
        "exchange",
        "symbol",
        "provider_symbol",
        "currency",
        "session_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "provider_timestamp",
        "request_id",
        "raw_artifact_id",
        "adapter_version",
        "source_run_id",
        "content_sha256",
        "version",
    )

    def insert_validated(
        self,
        candles: Iterable[MarketCandle],
        *,
        source_run_id: str,
        workspace_id: str = "default",
        version: int,
    ) -> int:
        materialized = list(candles)
        if any(candle.interval is not CandleInterval.ONE_DAY for candle in materialized):
            raise ValueError("Only daily candles are supported")
        return self._insert_partition_batches(
            "ohlcv_daily",
            materialized,
            self.DAILY_COLUMNS,
            source_run_id=source_run_id,
            workspace_id=workspace_id,
            version=version,
        )

    def _insert_partition_batches(
        self,
        table: str,
        candles: Sequence[MarketCandle],
        columns: Sequence[str],
        *,
        source_run_id: str,
        workspace_id: str,
        version: int,
    ) -> int:
        """Bound each insert below ClickHouse's distinct-partition safety limit."""

        by_partition: dict[tuple[int, int], list[MarketCandle]] = {}
        for candle in candles:
            key = (candle.session_date.year, candle.session_date.month)
            by_partition.setdefault(key, []).append(candle)
        ordered = [by_partition[key] for key in sorted(by_partition)]
        inserted = 0
        for offset in range(0, len(ordered), self.MAX_PARTITIONS_PER_INSERT):
            batch = [
                candle
                for partition in ordered[offset : offset + self.MAX_PARTITIONS_PER_INSERT]
                for candle in partition
            ]
            inserted += self._insert(
                table,
                (
                    self._row(
                        candle,
                        source_run_id=source_run_id,
                        workspace_id=workspace_id,
                        version=version,
                    )
                    for candle in batch
                ),
                columns,
            )
        return inserted

    @staticmethod
    def _row(
        candle: MarketCandle,
        *,
        source_run_id: str,
        workspace_id: str,
        version: int,
    ) -> dict[str, Any]:
        row = {
            "workspace_id": workspace_id,
            "instrument_id": candle.instrument_id,
            "exchange": candle.exchange,
            "symbol": candle.symbol or candle.provider_symbol,
            "provider_symbol": candle.provider_symbol,
            "currency": candle.currency,
            "session_date": candle.session_date,
            # Decimal(20, 8) inserts truncate extra digits in the driver. Round
            # explicitly to the same scale used by authoritative replica audits.
            "open": candle.open.quantize(Decimal("0.00000001")),
            "high": candle.high.quantize(Decimal("0.00000001")),
            "low": candle.low.quantize(Decimal("0.00000001")),
            "close": candle.close.quantize(Decimal("0.00000001")),
            "volume": candle.volume,
            "source": candle.provider,
            "provider_timestamp": candle.provider_timestamp,
            "request_id": candle.request_id,
            "raw_artifact_id": candle.raw_artifact_id or "",
            "adapter_version": candle.adapter_version,
            "source_run_id": source_run_id,
            "content_sha256": candle_content_sha256(candle),
            "version": version,
        }
        return row

    def count_rows(
        self,
        *,
        exchange: str,
        interval: str,
        source_run_id: str,
        workspace_id: str = "default",
    ) -> int:
        if interval != "1d":
            raise ValueError("Only daily replicas are supported")
        table = "ohlcv_daily"

        result = self._client.query(
            f"""
            SELECT count()
            FROM {self._database}.{table} FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND exchange = {{exchange:String}}
              AND source_run_id = {{source_run_id:String}}
            """,
            parameters={
                "workspace_id": workspace_id,
                "exchange": exchange,
                "source_run_id": source_run_id,
                "interval": interval,
            },
        )
        return int(result.result_rows[0][0]) if result.result_rows else 0

    def batch_summary(
        self,
        *,
        exchange: str,
        interval: str,
        source_run_id: str,
        workspace_id: str = "default",
        start_date: date | None = None,
        end_date: date | None = None,
        instrument_ids: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        if interval != "1d":
            raise ValueError("Only daily replicas are supported")
        table = "ohlcv_daily"
        watermark_column = "session_date"

        scope = ""
        if start_date is not None and end_date is not None and instrument_ids:
            if start_date > end_date:
                raise ValueError("Invalid daily verification date range")
            # These are immutable parts of the replacement key, so prefiltering
            # them preserves FINAL's latest-version semantics.
            scope = (
                "PREWHERE session_date BETWEEN {start_date:Date} AND {end_date:Date} "
                "AND instrument_id IN {instrument_ids:Array(String)} "
                "AND workspace_id = {workspace_id:String} AND exchange = {exchange:String}"
            )
        result = self._client.query(
            f"""
            SELECT content_sha256, {watermark_column}
            FROM {self._database}.{table} FINAL
            {scope}
            WHERE workspace_id = {{workspace_id:String}}
              AND exchange = {{exchange:String}}
              AND source_run_id = {{source_run_id:String}}
            ORDER BY content_sha256
            SETTINGS max_threads=2, max_memory_usage=536870912,
                max_bytes_before_external_sort=67108864
            """,
            parameters={
                "workspace_id": workspace_id,
                "exchange": exchange,
                "source_run_id": source_run_id,
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date,
                "instrument_ids": list(instrument_ids or []),
            },
        )
        digests = [
            value.decode("ascii") if isinstance(value, bytes) else str(value)
            for value, _watermark in result.result_rows
        ]
        watermarks = [row[1] for row in result.result_rows if row[1] is not None]
        return {
            "row_count": len(result.result_rows),
            "digest": hashlib.sha256("\n".join(sorted(digests)).encode()).hexdigest(),
            "watermark": max(watermarks) if watermarks else None,
        }

    def read_daily_partition(
        self,
        *,
        workspace_id: str,
        provider: str,
        exchange: str,
        window_start: date,
        window_end: date,
    ) -> list[dict[str, Any]]:
        """Read one bounded daily partition using common source/replica fields."""

        result = self._client.query(
            f"""
            SELECT
                instrument_id,
                provider_symbol,
                symbol,
                exchange,
                session_date,
                open,
                high,
                low,
                close,
                volume,
                source,
                provider_timestamp
            FROM {self._database}.ohlcv_daily FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND exchange = {{exchange:String}}
              AND source = {{provider:String}}
              AND session_date BETWEEN {{window_start:Date}} AND {{window_end:Date}}
            ORDER BY session_date, instrument_id
            """,
            parameters={
                "workspace_id": workspace_id,
                "provider": provider,
                "exchange": exchange,
                "window_start": window_start,
                "window_end": window_end,
            },
        )
        return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]


class ClickHouseQualityRepository(_Repository):
    RESULT_COLUMNS = (
        "workspace_id",
        "validation_run_id",
        "check_key",
        "check_version",
        "subject_type",
        "subject_id",
        "status",
        "observed_value",
        "expected_value",
        "details_json",
        "measured_at",
        "source_run_id",
        "version",
    )

    def insert_results(self, rows: Iterable[Mapping[str, Any]]) -> int:
        return self._insert("data_quality_results", rows, self.RESULT_COLUMNS)

    def read_results(self, validation_run_id: str) -> list[dict[str, Any]]:
        result = self._client.query(
            f"""
            SELECT * FROM {self._database}.data_quality_results FINAL
            WHERE validation_run_id = {{validation_run_id:String}}
            ORDER BY measured_at, check_key, subject_id
            """,
            parameters={"validation_run_id": validation_run_id},
        )
        return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]
