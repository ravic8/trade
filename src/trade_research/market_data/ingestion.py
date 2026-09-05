from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from time import perf_counter
from typing import Any

import pandas as pd

from trade_research.config import Settings
from trade_research.control_plane.artifacts import ArtifactManifestRepository
from trade_research.market_data.adapters import yfinance_frame_to_candles
from trade_research.market_data.contracts import CandleInterval, MarketCandle, ProviderRequest
from trade_research.market_data.quality import (
    MarketDataQualityRepository,
    validation_quality_outcomes,
)
from trade_research.market_data.raw_snapshots import RawSnapshotWriter, StoredRawSnapshot
from trade_research.market_data.replication import (
    MarketDataReplicationError,
    MarketDataReplicationRepository,
    ReplicationCheckpoint,
    assert_replication_matches,
    candle_batch_summary,
    checkpoint_id,
    watermark_lag_seconds,
)
from trade_research.market_data.validation import (
    CandleValidationResult,
    MarketDataValidationError,
    validate_candle_batch,
)
from trade_research.storage.clickhouse import (
    ClickHouseMarketDataRepository,
    create_clickhouse_client,
)
from trade_research.storage.object_store import ObjectArtifactStore


@dataclass(frozen=True)
class ValidatedMarketDataBatch:
    request: ProviderRequest
    frame: pd.DataFrame
    candles: tuple[MarketCandle, ...]
    validation: CandleValidationResult
    raw_snapshot: StoredRawSnapshot | None


def prepare_yfinance_batch(
    *,
    settings: Settings,
    database_engine: Any,
    frame: pd.DataFrame,
    raw_frame: pd.DataFrame | None = None,
    exchange: str,
    interval: CandleInterval,
    run_id: str,
    window_start: date | datetime,
    window_end: date | datetime,
    provider_symbols: Sequence[str],
    canonical_instrument_ids: Mapping[str, str],
    eligible_sessions: set[date] | None,
    retrieved_at: datetime,
    adapter_version: str = "yfinance-v1",
) -> ValidatedMarketDataBatch:
    """Snapshot, translate, validate, and deduplicate one yfinance response."""

    request = ProviderRequest(
        request_id=f"{run_id}-{exchange.lower()}-{interval.value}",
        provider="yfinance",
        exchange=exchange.upper(),
        interval=interval,
        window_start=_as_datetime(window_start),
        window_end=_exclusive_end(window_end),
        provider_symbols=tuple(sorted(set(provider_symbols))),
        retrieved_at=_as_utc(retrieved_at),
        adapter_version=adapter_version,
        parameters={"auto_adjust": False},
    )
    snapshot = _write_raw_snapshot_if_enabled(
        settings=settings,
        database_engine=database_engine,
        request=request,
        frame=raw_frame if raw_frame is not None else frame,
    )
    currency = {"NSE": "INR", "TSX": "CAD", "US": "USD"}.get(exchange.upper(), "USD")
    candles = yfinance_frame_to_candles(
        frame,
        request,
        currency=currency,
        raw_artifact_id=(snapshot.artifact_manifest_id if snapshot is not None else None),
        canonical_instrument_ids=canonical_instrument_ids,
    )
    validation = validate_candle_batch(
        request,
        candles,
        eligible_sessions=eligible_sessions,
        observed_at=retrieved_at,
    )
    MarketDataQualityRepository(database_engine).record(
        validation_quality_outcomes(
            request=request,
            source_run_id=run_id,
            result=validation,
        )
    )
    if not validation.valid:
        raise MarketDataValidationError(validation)
    identity_columns = (
        ["InstrumentKey", "Timestamp"]
        if interval.is_intraday
        else ["InstrumentKey", "Date"]
    )
    accepted_frame = frame.drop_duplicates(subset=identity_columns, keep="first").reset_index(
        drop=True
    )
    return ValidatedMarketDataBatch(
        request=request,
        frame=accepted_frame,
        candles=validation.accepted,
        validation=validation,
        raw_snapshot=snapshot,
    )


def replicate_validated_batch(
    settings: Settings,
    batch: ValidatedMarketDataBatch,
    *,
    database_engine: Any | None = None,
    source_run_id: str,
    source_store: str = "validated_batch",
    version: int,
) -> int:
    if not settings.clickhouse_write_enabled or not batch.candles:
        return 0
    repository = ClickHouseMarketDataRepository(
        create_clickhouse_client(settings),
        database=settings.clickhouse_database,
        write_enabled=True,
    )
    candles = list(batch.candles)
    source = candle_batch_summary(candles)
    dataset_key = "ohlcv_intraday" if batch.request.interval.is_intraday else "ohlcv_daily"
    started_at = datetime.now(UTC)
    replication_id = checkpoint_id(
        workspace_id="default",
        source_run_id=source_run_id,
        dataset_key=dataset_key,
        exchange=batch.request.exchange,
        interval=batch.request.interval.value,
    )
    checkpoint_repository = (
        MarketDataReplicationRepository(database_engine)
        if database_engine is not None
        else None
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
        )
        assert_replication_matches(source, destination)
    except Exception as exc:
        completed_at = datetime.now(UTC)
        if checkpoint_repository is not None:
            checkpoint_repository.record(
                replace(
                    pending,
                    status=(
                        "mismatch"
                        if isinstance(exc, MarketDataReplicationError)
                        else "failed"
                    ),
                    destination_row_count=(
                        destination["row_count"] if destination is not None else None
                    ),
                    destination_digest=(
                        destination["digest"] if destination is not None else None
                    ),
                    destination_watermark=(
                        destination["watermark"] if destination is not None else None
                    ),
                    watermark_lag_seconds=(
                        watermark_lag_seconds(
                            source["watermark"], destination["watermark"]
                        )
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
