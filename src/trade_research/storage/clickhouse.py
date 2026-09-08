from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Protocol

from trade_research.config import Settings
from trade_research.market_data.aggregation import (
    AggregatedMarketCandle,
    IntradayAggregationRequest,
)
from trade_research.market_data.contracts import MarketCandle, candle_content_sha256

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


class ResearchStorageReadOnlyError(PermissionError):
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
            raise ResearchStorageReadOnlyError("ClickHouse repository is read-only")
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


class ClickHouseFeatureRepository(_Repository):
    FEATURE_COLUMNS = (
        "workspace_id",
        "feature_key",
        "feature_version",
        "instrument_id",
        "session_date",
        "value",
        "dataset_snapshot_id",
        "source_run_id",
        "content_sha256",
        "version",
    )

    def insert_observations(self, rows: Iterable[Mapping[str, Any]]) -> int:
        return self._insert("feature_observations_daily", rows, self.FEATURE_COLUMNS)

    def read_observations(
        self,
        *,
        feature_key: str,
        feature_version: str,
        start_date: str,
        end_date: str,
        workspace_id: str = "default",
    ) -> list[dict[str, Any]]:
        result = self._client.query(
            f"""
            SELECT
                workspace_id, feature_key, feature_version, instrument_id,
                session_date, value, dataset_snapshot_id, source_run_id,
                content_sha256, version
            FROM {self._database}.feature_observations_daily FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND feature_key = {{feature_key:String}}
              AND feature_version = {{feature_version:String}}
              AND session_date BETWEEN {{start_date:Date}} AND {{end_date:Date}}
            ORDER BY session_date, instrument_id
            """,
            parameters={
                "workspace_id": workspace_id,
                "feature_key": feature_key,
                "feature_version": feature_version,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]


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
    INTRADAY_COLUMNS = (
        "workspace_id",
        "instrument_id",
        "exchange",
        "symbol",
        "provider_symbol",
        "currency",
        "candle_timestamp",
        "session_date",
        "interval",
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
        daily = [candle for candle in materialized if not candle.interval.is_intraday]
        intraday = [candle for candle in materialized if candle.interval.is_intraday]
        inserted = self._insert_partition_batches(
            "ohlcv_daily",
            daily,
            self.DAILY_COLUMNS,
            source_run_id=source_run_id,
            workspace_id=workspace_id,
            version=version,
        )
        inserted += self._insert_partition_batches(
            "ohlcv_intraday",
            intraday,
            self.INTRADAY_COLUMNS,
            source_run_id=source_run_id,
            workspace_id=workspace_id,
            version=version,
        )
        return inserted

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
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
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
        if candle.interval.is_intraday:
            row["candle_timestamp"] = candle.timestamp
            row["interval"] = candle.interval.value
        return row

    def count_rows(
        self,
        *,
        exchange: str,
        interval: str,
        source_run_id: str,
        workspace_id: str = "default",
    ) -> int:
        table = "ohlcv_daily" if interval == "1d" else "ohlcv_intraday"
        interval_filter = "" if interval == "1d" else " AND interval = {interval:String}"
        result = self._client.query(
            f"""
            SELECT count()
            FROM {self._database}.{table} FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND exchange = {{exchange:String}}
              AND source_run_id = {{source_run_id:String}}
              {interval_filter}
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
    ) -> dict[str, Any]:
        table = "ohlcv_daily" if interval == "1d" else "ohlcv_intraday"
        watermark_column = "session_date" if interval == "1d" else "candle_timestamp"
        interval_filter = "" if interval == "1d" else " AND interval = {interval:String}"
        result = self._client.query(
            f"""
            SELECT content_sha256, {watermark_column}
            FROM {self._database}.{table} FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND exchange = {{exchange:String}}
              AND source_run_id = {{source_run_id:String}}
              {interval_filter}
            ORDER BY content_sha256
            """,
            parameters={
                "workspace_id": workspace_id,
                "exchange": exchange,
                "source_run_id": source_run_id,
                "interval": interval,
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

    def aggregate_nse_intraday(
        self,
        request: IntradayAggregationRequest,
    ) -> list[AggregatedMarketCandle]:
        """Derive session-anchored OHLCV aggregates from validated NSE 1m rows."""

        result = self._client.query(
            f"""
            WITH {{bucket_minutes:UInt16}} AS bucket_minutes
            SELECT
                workspace_id,
                instrument_id,
                exchange,
                argMin(symbol, source_candle_timestamp) AS symbol,
                argMin(provider_symbol, source_candle_timestamp) AS provider_symbol,
                argMin(currency, source_candle_timestamp) AS currency,
                toTimeZone(bucket_timestamp_local, 'UTC') AS candle_timestamp,
                session_date,
                {{target_interval:String}} AS interval,
                argMin(open, source_candle_timestamp) AS open,
                max(high) AS high,
                min(low) AS low,
                argMax(close, source_candle_timestamp) AS close,
                sum(volume) AS volume,
                source AS provider,
                max(provider_timestamp) AS provider_timestamp,
                count() AS source_rows,
                least(
                    bucket_minutes,
                    375 - dateDiff('minute', session_open, bucket_timestamp_local)
                ) AS expected_source_rows,
                source_rows = expected_source_rows AS complete,
                arraySort(groupArray(toString(content_sha256))) AS source_content_digests,
                arraySort(groupUniqArray(source_run_id)) AS source_run_ids,
                arraySort(groupUniqArrayIf(raw_artifact_id, raw_artifact_id != ''))
                    AS raw_artifact_ids
            FROM
            (
                SELECT
                    *,
                    candle_timestamp AS source_candle_timestamp,
                    toDateTime(
                        concat(toString(session_date), ' 09:15:00'),
                        'Asia/Kolkata'
                    ) AS session_open,
                    addMinutes(
                        session_open,
                        intDiv(
                            dateDiff(
                                'minute',
                                session_open,
                                toTimeZone(candle_timestamp, 'Asia/Kolkata')
                            ),
                            bucket_minutes
                        ) * bucket_minutes
                    ) AS bucket_timestamp_local
                FROM {self._database}.ohlcv_intraday FINAL
                WHERE workspace_id = {{workspace_id:String}}
                  AND exchange = 'NSE'
                  AND instrument_id = {{instrument_id:String}}
                  AND source = {{provider:String}}
                  AND interval = '1m'
                  AND candle_timestamp >= {{window_start:DateTime64(6, 'UTC')}}
                  AND candle_timestamp < {{window_end:DateTime64(6, 'UTC')}}
            )
            GROUP BY
                workspace_id,
                instrument_id,
                exchange,
                session_date,
                source,
                session_open,
                bucket_timestamp_local
            HAVING {{complete_only:UInt8}} = 0 OR complete
            ORDER BY candle_timestamp, instrument_id
            LIMIT {{limit:UInt32}}
            """,
            parameters={
                "workspace_id": request.workspace_id,
                "instrument_id": request.instrument_id,
                "provider": request.provider,
                "window_start": request.window_start,
                "window_end": request.window_end,
                "bucket_minutes": request.interval_minutes,
                "target_interval": request.interval.value,
                "complete_only": int(request.complete_only),
                "limit": request.limit,
            },
        )
        candles: list[AggregatedMarketCandle] = []
        for row in result.result_rows:
            mapped = dict(zip(result.column_names, row, strict=True))
            # clickhouse-connect returns DateTime64 values without tzinfo even when
            # the ClickHouse column is explicitly declared in UTC. Restore that
            # schema guarantee at the storage boundary so domain constructors can
            # continue rejecting ambiguous naive datetimes from other sources.
            for column in ("candle_timestamp", "provider_timestamp"):
                value = mapped[column]
                if isinstance(value, datetime) and value.tzinfo is None:
                    mapped[column] = value.replace(tzinfo=UTC)
            candles.append(AggregatedMarketCandle.from_mapping(mapped))
        return candles


class ClickHouseExperimentRepository(_Repository):
    METRIC_COLUMNS = (
        "workspace_id",
        "experiment_run_id",
        "metric_key",
        "metric_value",
        "metric_text",
        "split",
        "step",
        "measured_at",
        "source_run_id",
        "version",
    )
    RETURN_COLUMNS = (
        "workspace_id",
        "experiment_run_id",
        "session_date",
        "gross_return",
        "net_return",
        "turnover",
        "transaction_cost",
        "equity",
        "source_run_id",
        "version",
    )
    POSITION_COLUMNS = (
        "workspace_id",
        "experiment_run_id",
        "session_date",
        "instrument_id",
        "quantity",
        "price",
        "market_value",
        "weight",
        "source_run_id",
        "version",
    )

    def insert_metrics(self, rows: Iterable[Mapping[str, Any]]) -> int:
        return self._insert("experiment_metrics", rows, self.METRIC_COLUMNS)

    def insert_returns(self, rows: Iterable[Mapping[str, Any]]) -> int:
        return self._insert("backtest_returns_daily", rows, self.RETURN_COLUMNS)

    def insert_positions(self, rows: Iterable[Mapping[str, Any]]) -> int:
        return self._insert("backtest_positions_daily", rows, self.POSITION_COLUMNS)

    def read_metrics(self, experiment_run_id: str) -> list[dict[str, Any]]:
        result = self._client.query(
            f"""
            SELECT * FROM {self._database}.experiment_metrics FINAL
            WHERE experiment_run_id = {{experiment_run_id:String}}
            ORDER BY measured_at, metric_key
            """,
            parameters={"experiment_run_id": experiment_run_id},
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
