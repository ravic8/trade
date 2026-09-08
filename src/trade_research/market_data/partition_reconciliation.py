from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from time import perf_counter
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, select

from trade_research.config import Settings, get_settings
from trade_research.market_data.contracts import CandleInterval, MarketCandle
from trade_research.market_data.replication import (
    MarketDataReplicationRepository,
    ReplicationCheckpoint,
    checkpoint_id,
    watermark_lag_seconds,
)
from trade_research.storage.clickhouse import (
    ClickHouseMarketDataRepository,
    create_clickhouse_client,
)
from trade_research.storage.timescale import TimescaleStore, ohlcv_daily_table, symbols_table

_PRICE_SCALE = Decimal("0.00000001")


@dataclass(frozen=True)
class DailyReplicaRow:
    instrument_id: str
    provider_symbol: str
    symbol: str
    exchange: str
    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    provider: str
    provider_timestamp: datetime

    @property
    def identity(self) -> tuple[str, str, date]:
        return (self.instrument_id, self.provider, self.session_date)


@dataclass(frozen=True)
class DailyPartitionDifference:
    source_row_count: int
    destination_row_count: int
    source_digest: str
    destination_digest: str
    source_watermark: date | None
    destination_watermark: date | None
    missing_identities: tuple[tuple[str, str, date], ...]
    divergent_identities: tuple[tuple[str, str, date], ...]
    unexpected_identities: tuple[tuple[str, str, date], ...]

    @property
    def reconciled(self) -> bool:
        return not (
            self.missing_identities
            or self.divergent_identities
            or self.unexpected_identities
        )


@dataclass(frozen=True)
class DailyPartitionReconciliationResult:
    partition: str
    window_start: date
    window_end: date
    source_run_id: str
    replication_checkpoint_id: str
    repair_requested: bool
    repaired_rows: int
    initial: DailyPartitionDifference
    final: DailyPartitionDifference

    @property
    def status(self) -> str:
        return "reconciled" if self.final.reconciled else "mismatch"


class PostgreSQLDailyPartitionRepository:
    """Read validated daily authority rows with canonical NSE identities."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def read(
        self,
        *,
        window_start: date,
        window_end: date,
        provider: str,
        exchange: str,
    ) -> list[DailyReplicaRow]:
        candles = ohlcv_daily_table
        symbols = symbols_table
        statement = (
            select(
                candles.c.instrument_key,
                candles.c.symbol,
                candles.c.exchange,
                candles.c.date,
                candles.c.open,
                candles.c.high,
                candles.c.low,
                candles.c.close,
                candles.c.volume,
                candles.c.source,
                candles.c.fetched_at,
                symbols.c.canonical_instrument_id,
                symbols.c.yahoo_symbol,
            )
            .select_from(
                candles.outerjoin(
                    symbols,
                    (candles.c.instrument_key == symbols.c.provider_instrument_key)
                    & (symbols.c.exchange == exchange),
                )
            )
            .where(candles.c.source == provider)
            .where(candles.c.exchange == exchange)
            .where(candles.c.quality_status == "ok")
            .where(candles.c.date >= window_start)
            .where(candles.c.date <= window_end)
            .order_by(candles.c.date, candles.c.instrument_key)
        )
        with self._engine.connect() as connection:
            records = connection.execute(statement).mappings().all()
        unmapped = sorted(
            {str(row["instrument_key"]) for row in records if not row["canonical_instrument_id"]}
        )
        if unmapped:
            preview = ", ".join(unmapped[:5])
            raise ValueError(
                "Cannot reconcile daily partition with unmapped canonical instruments: "
                f"{preview}{'…' if len(unmapped) > 5 else ''}"
            )
        rows: dict[tuple[str, str, date], DailyReplicaRow] = {}
        for record in records:
            fetched_at = _as_utc(record["fetched_at"])
            row = DailyReplicaRow(
                instrument_id=str(record["canonical_instrument_id"]),
                provider_symbol=str(
                    record["yahoo_symbol"] or _provider_symbol(record["instrument_key"])
                ),
                symbol=str(record["symbol"]),
                exchange=str(record["exchange"]),
                session_date=record["date"],
                open=_decimal(record["open"]),
                high=_decimal(record["high"]),
                low=_decimal(record["low"]),
                close=_decimal(record["close"]),
                volume=int(record["volume"]),
                provider=str(record["source"]),
                provider_timestamp=fetched_at,
            )
            if row.identity in rows:
                raise ValueError(
                    "Canonical identity collision in PostgreSQL daily partition: "
                    f"{row.identity}"
                )
            rows[row.identity] = row
        return list(rows.values())


class DailyPartitionReconciler:
    """Audit and safely upsert one monthly NSE daily replica partition."""

    def __init__(
        self,
        *,
        engine: Engine,
        clickhouse: ClickHouseMarketDataRepository,
        max_rows: int = 100_000,
        repair_chunk_size: int = 5_000,
    ) -> None:
        if max_rows < 1 or repair_chunk_size < 1:
            raise ValueError("reconciliation row bounds must be positive")
        self._source = PostgreSQLDailyPartitionRepository(engine)
        self._checkpoints = MarketDataReplicationRepository(engine)
        self._clickhouse = clickhouse
        self._max_rows = max_rows
        self._repair_chunk_size = repair_chunk_size

    def reconcile(
        self,
        *,
        month: str,
        repair: bool = False,
        provider: str = "yfinance",
        exchange: str = "NSE",
        workspace_id: str = "default",
        source_run_id: str | None = None,
        at: datetime | None = None,
    ) -> DailyPartitionReconciliationResult:
        window_start, window_end = month_window(month)
        if provider != "yfinance" or exchange != "NSE" or workspace_id != "default":
            raise ValueError(
                "daily partition reconciliation currently supports default/NSE/yfinance"
            )
        observed_at = _as_utc(at or datetime.now(UTC))
        run_id = source_run_id or f"phase3-daily-{month}-{uuid4()}"
        dataset_key = f"ohlcv_daily:{month}"
        source = self._source.read(
            window_start=window_start,
            window_end=window_end,
            provider=provider,
            exchange=exchange,
        )
        if not source:
            raise ValueError(f"PostgreSQL source partition {month} is empty")
        if len(source) > self._max_rows:
            raise ValueError(
                f"PostgreSQL source partition exceeds max_rows: {len(source)}>{self._max_rows}"
            )
        initial = compare_daily_partitions(source, [])
        replication_id = checkpoint_id(
            workspace_id=workspace_id,
            source_run_id=run_id,
            dataset_key=dataset_key,
            exchange=exchange,
            interval=CandleInterval.ONE_DAY.value,
        )
        pending = ReplicationCheckpoint(
            replication_checkpoint_id=replication_id,
            source_run_id=run_id,
            source_store="postgresql",
            destination_store="clickhouse",
            dataset_key=dataset_key,
            exchange=exchange,
            interval=CandleInterval.ONE_DAY.value,
            status="pending",
            source_row_count=initial.source_row_count,
            source_digest=initial.source_digest,
            source_watermark=_watermark_datetime(initial.source_watermark),
            started_at=observed_at,
            workspace_id=workspace_id,
            details={
                "partition": month,
                "digest_version": "daily-business-v1",
                "repair_policy": "bounded_upsert_only",
                "repair_requested": repair,
            },
        )
        self._checkpoints.record(pending)
        timer = perf_counter()
        repaired_rows = 0
        final = initial
        try:
            destination = self._read_destination(
                workspace_id=workspace_id,
                provider=provider,
                exchange=exchange,
                window_start=window_start,
                window_end=window_end,
            )
            initial = compare_daily_partitions(source, destination)
            final = initial
            if repair and not initial.reconciled:
                source_by_identity = {row.identity: row for row in source}
                repair_identities = (
                    *initial.missing_identities,
                    *initial.divergent_identities,
                )
                repair_rows = [source_by_identity[identity] for identity in repair_identities]
                for offset in range(0, len(repair_rows), self._repair_chunk_size):
                    chunk = repair_rows[offset : offset + self._repair_chunk_size]
                    repaired_rows += self._clickhouse.insert_validated(
                        [_repair_candle(row, month=month, run_id=run_id) for row in chunk],
                        source_run_id=run_id,
                        workspace_id=workspace_id,
                        version=int(observed_at.timestamp() * 1_000_000),
                    )
                destination = self._read_destination(
                    workspace_id=workspace_id,
                    provider=provider,
                    exchange=exchange,
                    window_start=window_start,
                    window_end=window_end,
                )
                final = compare_daily_partitions(source, destination)
        except Exception as exc:
            completed_at = datetime.now(UTC)
            self._checkpoints.record(
                replace(
                    pending,
                    status="failed",
                    destination_row_count=final.destination_row_count,
                    destination_digest=final.destination_digest,
                    destination_watermark=_watermark_datetime(
                        final.destination_watermark
                    ),
                    watermark_lag_seconds=watermark_lag_seconds(
                        _watermark_datetime(final.source_watermark),
                        _watermark_datetime(final.destination_watermark),
                    ),
                    replication_latency_ms=(perf_counter() - timer) * 1_000,
                    error_message=str(exc),
                    details={
                        **(pending.details or {}),
                        "initial": _difference_details(initial),
                        "repaired_rows": repaired_rows,
                    },
                    completed_at=completed_at,
                )
            )
            raise
        completed_at = datetime.now(UTC)
        self._checkpoints.record(
            replace(
                pending,
                status="reconciled" if final.reconciled else "mismatch",
                destination_row_count=final.destination_row_count,
                destination_digest=final.destination_digest,
                destination_watermark=_watermark_datetime(final.destination_watermark),
                watermark_lag_seconds=watermark_lag_seconds(
                    _watermark_datetime(final.source_watermark),
                    _watermark_datetime(final.destination_watermark),
                ),
                replication_latency_ms=(perf_counter() - timer) * 1_000,
                details={
                    **(pending.details or {}),
                    "initial": _difference_details(initial),
                    "repaired_rows": repaired_rows,
                    "final": _difference_details(final),
                    "unexpected_rows_require_manual_review": bool(
                        final.unexpected_identities
                    ),
                },
                completed_at=completed_at,
            )
        )
        return DailyPartitionReconciliationResult(
            partition=month,
            window_start=window_start,
            window_end=window_end,
            source_run_id=run_id,
            replication_checkpoint_id=replication_id,
            repair_requested=repair,
            repaired_rows=repaired_rows,
            initial=initial,
            final=final,
        )

    def _read_destination(
        self,
        *,
        workspace_id: str,
        provider: str,
        exchange: str,
        window_start: date,
        window_end: date,
    ) -> list[DailyReplicaRow]:
        return [
            DailyReplicaRow(
                instrument_id=str(row["instrument_id"]),
                provider_symbol=str(row["provider_symbol"]),
                symbol=str(row["symbol"]),
                exchange=str(row["exchange"]),
                session_date=row["session_date"],
                open=_decimal(row["open"]),
                high=_decimal(row["high"]),
                low=_decimal(row["low"]),
                close=_decimal(row["close"]),
                volume=int(row["volume"]),
                provider=str(row["source"]),
                provider_timestamp=_as_utc(row["provider_timestamp"]),
            )
            for row in self._clickhouse.read_daily_partition(
                workspace_id=workspace_id,
                provider=provider,
                exchange=exchange,
                window_start=window_start,
                window_end=window_end,
            )
        ]


def run_nse_daily_partition_reconciliation(
    *,
    month: str,
    repair: bool = False,
    source_run_id: str | None = None,
    max_rows: int = 100_000,
    at: datetime | None = None,
    settings: Settings | None = None,
) -> DailyPartitionReconciliationResult:
    """Build configured repositories and reconcile one NSE daily month."""

    current_settings = settings or get_settings()
    if not current_settings.clickhouse_enabled:
        raise RuntimeError("ClickHouse must be enabled for partition reconciliation")
    if repair and not current_settings.clickhouse_write_enabled:
        raise RuntimeError("ClickHouse writes must be enabled for partition repair")
    engine = TimescaleStore(current_settings.database_url).engine
    repository = ClickHouseMarketDataRepository(
        create_clickhouse_client(current_settings),
        database=current_settings.clickhouse_database,
        write_enabled=repair,
    )
    return DailyPartitionReconciler(
        engine=engine,
        clickhouse=repository,
        max_rows=max_rows,
    ).reconcile(
        month=month,
        repair=repair,
        source_run_id=source_run_id,
        at=at,
    )


def month_window(month: str) -> tuple[date, date]:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError("month must use YYYY-MM")
    try:
        start = datetime.strptime(month, "%Y-%m").date().replace(day=1)
    except ValueError as exc:
        raise ValueError("month must use YYYY-MM") from exc
    if start.year < 2000 or start > datetime.now(UTC).date().replace(day=1):
        raise ValueError("month must be between 2000-01 and the current month")
    next_month = (
        date(start.year + 1, 1, 1)
        if start.month == 12
        else date(start.year, start.month + 1, 1)
    )
    return start, date.fromordinal(next_month.toordinal() - 1)


def compare_daily_partitions(
    source: list[DailyReplicaRow],
    destination: list[DailyReplicaRow],
) -> DailyPartitionDifference:
    source_rows = _indexed_rows(source, label="source")
    destination_rows = _indexed_rows(destination, label="destination")
    source_keys = set(source_rows)
    destination_keys = set(destination_rows)
    shared = source_keys & destination_keys
    divergent = sorted(
        identity
        for identity in shared
        if daily_replica_content_sha256(source_rows[identity])
        != daily_replica_content_sha256(destination_rows[identity])
    )
    return DailyPartitionDifference(
        source_row_count=len(source),
        destination_row_count=len(destination),
        source_digest=_partition_digest(source),
        destination_digest=_partition_digest(destination),
        source_watermark=max((row.session_date for row in source), default=None),
        destination_watermark=max(
            (row.session_date for row in destination), default=None
        ),
        missing_identities=tuple(sorted(source_keys - destination_keys)),
        divergent_identities=tuple(divergent),
        unexpected_identities=tuple(sorted(destination_keys - source_keys)),
    )


def daily_replica_content_sha256(row: DailyReplicaRow) -> str:
    payload = {
        "instrument_id": row.instrument_id,
        "provider": row.provider,
        "exchange": row.exchange,
        "session_date": row.session_date.isoformat(),
        "symbol": row.symbol,
        "open": format(_decimal(row.open), "f"),
        "high": format(_decimal(row.high), "f"),
        "low": format(_decimal(row.low), "f"),
        "close": format(_decimal(row.close), "f"),
        "volume": row.volume,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _partition_digest(rows: list[DailyReplicaRow]) -> str:
    digests = sorted(daily_replica_content_sha256(row) for row in rows)
    return hashlib.sha256("\n".join(digests).encode()).hexdigest()


def _indexed_rows(
    rows: list[DailyReplicaRow], *, label: str
) -> dict[tuple[str, str, date], DailyReplicaRow]:
    indexed: dict[tuple[str, str, date], DailyReplicaRow] = {}
    for row in rows:
        if row.identity in indexed:
            raise ValueError(f"Duplicate {label} daily identity: {row.identity}")
        indexed[row.identity] = row
    return indexed


def _repair_candle(row: DailyReplicaRow, *, month: str, run_id: str) -> MarketCandle:
    return MarketCandle(
        instrument_id=row.instrument_id,
        provider_symbol=row.provider_symbol,
        symbol=row.symbol,
        exchange=row.exchange,
        session_date=row.session_date,
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        volume=row.volume,
        currency="INR",
        provider=row.provider,
        provider_timestamp=row.provider_timestamp,
        request_id=f"{run_id}-{month}",
        adapter_version="postgresql-authority-repair-v1",
        interval=CandleInterval.ONE_DAY,
    )


def _difference_details(difference: DailyPartitionDifference) -> dict[str, Any]:
    return {
        "source_row_count": difference.source_row_count,
        "destination_row_count": difference.destination_row_count,
        "missing_rows": len(difference.missing_identities),
        "divergent_rows": len(difference.divergent_identities),
        "unexpected_rows": len(difference.unexpected_identities),
        "reconciled": difference.reconciled,
    }


def _provider_symbol(instrument_key: Any) -> str:
    value = str(instrument_key)
    return value[3:] if value.startswith("YF|") else value


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(_PRICE_SCALE)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _watermark_datetime(value: date | None) -> datetime | None:
    return datetime.combine(value, time.min, tzinfo=UTC) if value is not None else None
