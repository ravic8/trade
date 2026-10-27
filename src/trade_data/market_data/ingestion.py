from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from time import perf_counter
from typing import Any

import pandas as pd

from trade_data.config import Settings
from trade_data.control_plane.artifacts import ArtifactManifestRepository
from trade_data.market_data.contracts import MarketCandle, ProviderRequest
from trade_data.market_data.raw_snapshots import RawSnapshotWriter, StoredRawSnapshot
from trade_data.market_data.replication import (
    MarketDataReplicationError,
    MarketDataReplicationRepository,
    ReplicationCheckpoint,
    assert_replication_matches,
    candle_batch_summary,
    candle_business_batch_summary,
    checkpoint_id,
    watermark_lag_seconds,
)
from trade_data.market_data.validation import (
    CandleValidationResult,
)
from trade_data.storage.clickhouse import (
    ClickHouseMarketDataRepository,
    create_clickhouse_client,
)
from trade_data.storage.object_store import ObjectArtifactStore


@dataclass(frozen=True)
class ValidatedMarketDataBatch:
    request: ProviderRequest
    frame: pd.DataFrame
    candles: tuple[MarketCandle, ...]
    validation: CandleValidationResult
    raw_snapshot: StoredRawSnapshot | None


def replicate_validated_batch(
    settings: Settings,
    batch: ValidatedMarketDataBatch,
    *,
    database_engine: Any | None = None,
    source_run_id: str,
    source_store: str = "validated_batch",
    version: int,
) -> int:
    """Bound ClickHouse parameter size as well as insert payload size."""
    if not settings.clickhouse_write_enabled or not batch.candles:
        return 0
    repository = ClickHouseMarketDataRepository(
        create_clickhouse_client(settings),
        database=settings.clickhouse_database,
        write_enabled=True,
    )
    total = 0
    for offset in range(0, len(batch.candles), 200):
        chunk = replace(batch, candles=batch.candles[offset : offset + 200])
        total += _replicate_chunk(
            settings,
            chunk,
            repository=repository,
            database_engine=database_engine,
            source_run_id=f"{source_run_id}-{offset // 200:04d}",
            source_store=source_store,
            version=version,
        )
    return total


def _replicate_chunk(
    settings: Settings,
    batch: ValidatedMarketDataBatch,
    *,
    repository: ClickHouseMarketDataRepository,
    database_engine: Any | None = None,
    source_run_id: str,
    source_store: str = "validated_batch",
    version: int,
) -> int:
    if not settings.clickhouse_write_enabled or not batch.candles:
        return 0
    candles = list(batch.candles)
    source = candle_batch_summary(candles)
    business = candle_business_batch_summary(candles)
    dataset_key = "ohlcv_daily"
    started_at = datetime.now(UTC)
    replication_id = checkpoint_id(
        workspace_id="default",
        source_run_id=source_run_id,
        dataset_key=dataset_key,
        exchange=batch.request.exchange,
        interval=batch.request.interval.value,
    )
    checkpoint_repository = (
        MarketDataReplicationRepository(database_engine) if database_engine is not None else None
    )
    pending = ReplicationCheckpoint(
        replication_checkpoint_id=replication_id,
        source_run_id=source_run_id,
        source_store=source_store,
        destination_store="clickhouse",
        dataset_key=dataset_key,
        exchange=batch.request.exchange,
        interval=batch.request.interval.value,
        status="pending",
        source_row_count=source["row_count"],
        source_digest=source["digest"],
        source_watermark=source["watermark"],
        details={
            "business_row_count": business["row_count"],
            "business_digest": business["digest"],
        },
        started_at=started_at,
    )
    if checkpoint_repository is not None:
        checkpoint_repository.record(pending)
    timer = perf_counter()
    try:
        destination: dict[str, Any] | None = None
        inserted = repository.insert_validated(
            candles,
            source_run_id=source_run_id,
            version=version,
        )
        destination = repository.batch_summary(
            exchange=batch.request.exchange,
            interval=batch.request.interval.value,
            source_run_id=source_run_id,
            start_date=min(candle.session_date for candle in candles),
            end_date=max(candle.session_date for candle in candles),
            instrument_ids=sorted({candle.instrument_id for candle in candles}),
        )
        assert_replication_matches(source, destination)
    except Exception as exc:
        completed_at = datetime.now(UTC)
        if checkpoint_repository is not None:
            checkpoint_repository.record(
                replace(
                    pending,
                    status=(
                        "mismatch" if isinstance(exc, MarketDataReplicationError) else "failed"
                    ),
                    destination_row_count=(
                        destination["row_count"] if destination is not None else None
                    ),
                    destination_digest=(destination["digest"] if destination is not None else None),
                    destination_watermark=(
                        destination["watermark"] if destination is not None else None
                    ),
                    watermark_lag_seconds=(
                        watermark_lag_seconds(source["watermark"], destination["watermark"])
                        if destination is not None
                        else None
                    ),
                    replication_latency_ms=(perf_counter() - timer) * 1000,
                    error_message=str(exc),
                    completed_at=completed_at,
                )
            )
        raise
    completed_at = datetime.now(UTC)
    if checkpoint_repository is not None:
        checkpoint_repository.record(
            replace(
                pending,
                status="reconciled",
                destination_row_count=destination["row_count"],
                destination_digest=destination["digest"],
                destination_watermark=destination["watermark"],
                watermark_lag_seconds=watermark_lag_seconds(
                    source["watermark"], destination["watermark"]
                ),
                replication_latency_ms=(perf_counter() - timer) * 1000,
                completed_at=completed_at,
            )
        )
    return inserted


def _write_raw_snapshot_if_enabled(
    *,
    settings: Settings,
    database_engine: Any,
    request: ProviderRequest,
    frame: pd.DataFrame,
) -> StoredRawSnapshot | None:
    if not settings.object_store_write_enabled:
        return None
    return RawSnapshotWriter(
        ObjectArtifactStore.from_settings(settings),
        registrar=ArtifactManifestRepository(database_engine),
    ).write(request, frame)


def _as_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    return datetime.combine(value, time.min, tzinfo=UTC)


def _exclusive_end(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    return datetime.combine(value + timedelta(days=1), time.min, tzinfo=UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
