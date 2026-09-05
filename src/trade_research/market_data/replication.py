from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time
from typing import Any

from sqlalchemy import Engine, insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trade_research.control_plane.tables import market_data_replication_checkpoints_table
from trade_research.market_data.contracts import MarketCandle, candle_content_sha256


class MarketDataReplicationError(RuntimeError):
    """Raised when a ClickHouse replica cannot prove equality to its source batch."""


@dataclass(frozen=True)
class ReplicationCheckpoint:
    replication_checkpoint_id: str
    source_run_id: str
    source_store: str
    destination_store: str
    dataset_key: str
    exchange: str
    interval: str
    status: str
    source_row_count: int
    source_digest: str
    started_at: datetime
    destination_row_count: int | None = None
    destination_digest: str | None = None
    source_watermark: datetime | None = None
    destination_watermark: datetime | None = None
    watermark_lag_seconds: float | None = None
    replication_latency_ms: float | None = None
    error_message: str | None = None
    details: dict[str, Any] | None = None
    completed_at: datetime | None = None
    workspace_id: str = "default"

    def row(self, at: datetime) -> dict[str, Any]:
        row = asdict(self)
        row["details"] = self.details or {}
        row["created_at"] = at
        row["updated_at"] = at
        return row


class MarketDataReplicationRepository:
    """Stores the latest idempotent state of each source-run replication."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def record(self, checkpoint: ReplicationCheckpoint) -> None:
        at = datetime.now(UTC)
        values = checkpoint.row(at)
        dialect = self._engine.dialect.name
        if dialect == "postgresql":
            statement: Any = postgresql_insert(
                market_data_replication_checkpoints_table
            ).values(**values)
            statement = statement.on_conflict_do_update(
                index_elements=["replication_checkpoint_id"],
                set_={
                    column: getattr(statement.excluded, column)
                    for column in (
                        "status",
                        "destination_row_count",
                        "destination_digest",
                        "source_watermark",
                        "destination_watermark",
                        "watermark_lag_seconds",
                        "replication_latency_ms",
                        "error_message",
                        "details",
                        "completed_at",
                        "updated_at",
                    )
                },
            )
        elif dialect == "sqlite":
            statement = sqlite_insert(market_data_replication_checkpoints_table).values(
                **values
            )
            statement = statement.on_conflict_do_update(
                index_elements=["replication_checkpoint_id"],
                set_={
                    column: getattr(statement.excluded, column)
                    for column in (
                        "status",
                        "destination_row_count",
                        "destination_digest",
                        "source_watermark",
                        "destination_watermark",
                        "watermark_lag_seconds",
                        "replication_latency_ms",
                        "error_message",
                        "details",
                        "completed_at",
                        "updated_at",
                    )
                },
            )
        else:
            statement = insert(market_data_replication_checkpoints_table).values(**values)
        with self._engine.begin() as connection:
            connection.execute(statement)


def checkpoint_id(
    *,
    workspace_id: str,
    source_run_id: str,
    dataset_key: str,
    exchange: str,
    interval: str,
) -> str:
    identity = "|".join(
        (workspace_id, source_run_id, dataset_key, exchange.upper(), interval)
    )
    return hashlib.sha256(identity.encode()).hexdigest()


def candle_batch_summary(candles: list[MarketCandle]) -> dict[str, Any]:
    digests = sorted(candle_content_sha256(candle) for candle in candles)
    watermarks = [_candle_watermark(candle) for candle in candles]
    return {
        "row_count": len(candles),
        "digest": hashlib.sha256("\n".join(digests).encode()).hexdigest(),
        "watermark": max(watermarks) if watermarks else None,
    }


def assert_replication_matches(
    source: dict[str, Any], destination: dict[str, Any]
) -> None:
    mismatches: list[str] = []
    if source["row_count"] != destination["row_count"]:
        mismatches.append(
            f"row_count={source['row_count']}:{destination['row_count']}"
        )
    if source["digest"] != destination["digest"]:
        mismatches.append(f"digest={source['digest']}:{destination['digest']}")
    if _as_utc(source["watermark"]) != _as_utc(destination["watermark"]):
        mismatches.append(
            f"watermark={source['watermark']}:{destination['watermark']}"
        )
    if mismatches:
        raise MarketDataReplicationError(
            "ClickHouse replication mismatch: " + ", ".join(mismatches)
        )


def watermark_lag_seconds(source: Any, destination: Any) -> float | None:
    source_timestamp = _as_utc(source)
    destination_timestamp = _as_utc(destination)
    if source_timestamp is None or destination_timestamp is None:
        return None
    return abs((source_timestamp - destination_timestamp).total_seconds())


def _candle_watermark(candle: MarketCandle) -> datetime:
    return candle.timestamp or datetime.combine(candle.session_date, time.min, tzinfo=UTC)


def _as_utc(value: date | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min, tzinfo=UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
