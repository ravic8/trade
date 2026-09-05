from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import pandas as pd

from trade_research.config import Settings
from trade_research.control_plane.artifacts import ArtifactManifestRepository
from trade_research.market_data.adapters import yfinance_frame_to_candles
from trade_research.market_data.contracts import CandleInterval, MarketCandle, ProviderRequest
from trade_research.market_data.raw_snapshots import RawSnapshotWriter, StoredRawSnapshot
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
        frame=accepted_frame,
        candles=validation.accepted,
        validation=validation,
        raw_snapshot=snapshot,
    )


def replicate_validated_batch(
    settings: Settings,
    batch: ValidatedMarketDataBatch,
    *,
    source_run_id: str,
    version: int,
) -> int:
    if not settings.clickhouse_write_enabled or not batch.candles:
        return 0
    repository = ClickHouseMarketDataRepository(
        create_clickhouse_client(settings),
        database=settings.clickhouse_database,
        write_enabled=True,
    )
    return repository.insert_validated(
        batch.candles,
        source_run_id=source_run_id,
        version=version,
    )


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
