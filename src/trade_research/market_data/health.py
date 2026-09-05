from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import Engine, case, distinct, func, select

from trade_research.control_plane.tables import (
    artifact_manifests_table,
    market_data_quality_outcomes_table,
    market_data_replication_checkpoints_table,
)

_QUALITY_STATUSES = (
    "valid",
    "missing",
    "provider_unavailable",
    "duplicate",
    "invalid",
    "outside_session",
    "stale",
)
_QUARANTINE_STATUSES = {"duplicate", "invalid", "outside_session", "stale"}


@dataclass(frozen=True)
class MarketDataQualityHealth:
    interval: str
    source_run_id: str
    request_count: int
    total_outcomes: int
    expected_outcomes: int
    affected_instruments: int
    latest_session_date: date | None
    latest_candle_timestamp: datetime | None
    observed_at: datetime
    completeness_ratio: float | None
    unexplained_gap_count: int
    quarantined_count: int
    raw_artifact_count: int
    status_counts: dict[str, int]
    health_status: str


@dataclass(frozen=True)
class MarketDataQualityIssue:
    interval: str
    source_run_id: str
    status: str
    reason_code: str
    severity: str
    retryable: bool
    occurrences: int
    affected_instruments: int
    first_session_date: date
    latest_session_date: date
    observed_at: datetime


@dataclass(frozen=True)
class MarketDataRawLineage:
    artifact_manifest_id: str
    artifact_type: str
    sha256: str
    size_bytes: int
    media_type: str
    object_versioned: bool
    created_at: datetime


@dataclass(frozen=True)
class MarketDataReplicationHealth:
    interval: str
    source_run_id: str
    dataset_key: str
    source_store: str
    destination_store: str
    status: str
    source_row_count: int
    destination_row_count: int | None
    counts_match: bool | None
    digests_match: bool | None
    source_watermark: datetime | None
    destination_watermark: datetime | None
    watermark_lag_seconds: float | None
    replication_latency_ms: float | None
    error_message: str | None
    started_at: datetime
    completed_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True)
class MarketDataHealthSnapshot:
    workspace_id: str
    provider: str
    exchange: str
    health_status: str
    quality: tuple[MarketDataQualityHealth, ...]
    issues: tuple[MarketDataQualityIssue, ...]
    raw_lineage: tuple[MarketDataRawLineage, ...]
    replication: tuple[MarketDataReplicationHealth, ...]


class MarketDataHealthRepository:
    """Read-only operational view over the Phase 3 quality and replica ledgers."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def snapshot(
        self,
        *,
        workspace_id: str,
        provider: str = "yfinance",
        exchange: str = "NSE",
        issue_limit: int = 50,
        lineage_limit: int = 20,
    ) -> MarketDataHealthSnapshot:
        quality: list[MarketDataQualityHealth] = []
        issues: list[MarketDataQualityIssue] = []
        raw_lineage: list[MarketDataRawLineage] = []
        with self._engine.connect() as connection:
            latest_runs = self._latest_quality_runs(
                connection,
                workspace_id=workspace_id,
                provider=provider,
                exchange=exchange,
            )
            for interval, source_run_id in latest_runs.items():
                quality.append(
                    self._quality_summary(
                        connection,
                        workspace_id=workspace_id,
                        provider=provider,
                        exchange=exchange,
                        interval=interval,
                        source_run_id=source_run_id,
                    )
                )
            issues.extend(
                self._quality_issues(
                    connection,
                    workspace_id=workspace_id,
                    provider=provider,
                    exchange=exchange,
                    latest_runs=latest_runs,
                    limit=issue_limit,
                )
            )
            raw_lineage.extend(
                self._raw_lineage(
                    connection,
                    workspace_id=workspace_id,
                    provider=provider,
                    exchange=exchange,
                    latest_runs=latest_runs,
                    limit=lineage_limit,
                )
            )
            replication = self._latest_replication(
                connection,
                workspace_id=workspace_id,
                exchange=exchange,
            )
        quality.sort(key=lambda row: row.interval)
        overall = _overall_health(quality, replication)
        return MarketDataHealthSnapshot(
            workspace_id=workspace_id,
            provider=provider,
            exchange=exchange,
            health_status=overall,
            quality=tuple(quality),
            issues=tuple(issues),
            raw_lineage=tuple(raw_lineage),
            replication=tuple(replication),
        )

    @staticmethod
    def _latest_quality_runs(
        connection: Any,
        *,
        workspace_id: str,
        provider: str,
        exchange: str,
    ) -> dict[str, str]:
        table = market_data_quality_outcomes_table
        rows = connection.execute(
            select(
                table.c.interval,
                table.c.source_run_id,
                func.max(table.c.observed_at).label("observed_at"),
            )
            .where(table.c.workspace_id == workspace_id)
            .where(table.c.provider == provider)
            .where(table.c.exchange == exchange)
            .group_by(table.c.interval, table.c.source_run_id)
            .order_by(func.max(table.c.observed_at).desc(), table.c.source_run_id.desc())
        ).mappings()
        latest: dict[str, str] = {}
        for row in rows:
            latest.setdefault(str(row["interval"]), str(row["source_run_id"]))
        return latest

    @staticmethod
    def _quality_summary(
        connection: Any,
        *,
        workspace_id: str,
        provider: str,
        exchange: str,
        interval: str,
        source_run_id: str,
    ) -> MarketDataQualityHealth:
        table = market_data_quality_outcomes_table
        status_columns = [
            func.sum(case((table.c.status == status, 1), else_=0)).label(status)
            for status in _QUALITY_STATUSES
        ]
        row = connection.execute(
            select(
                func.count().label("total_outcomes"),
                func.sum(case((table.c.expected.is_(True), 1), else_=0)).label(
                    "expected_outcomes"
                ),
                func.count(distinct(table.c.request_id)).label("request_count"),
                func.count(distinct(table.c.instrument_id)).label("affected_instruments"),
                func.count(distinct(table.c.raw_artifact_id)).label("raw_artifact_count"),
                func.max(table.c.session_date).label("latest_session_date"),
                func.max(table.c.candle_timestamp).label("latest_candle_timestamp"),
                func.max(table.c.observed_at).label("observed_at"),
                *status_columns,
            )
            .where(table.c.workspace_id == workspace_id)
            .where(table.c.provider == provider)
            .where(table.c.exchange == exchange)
            .where(table.c.interval == interval)
            .where(table.c.source_run_id == source_run_id)
        ).mappings().one()
        counts = {status: int(row[status] or 0) for status in _QUALITY_STATUSES}
        completeness = _completeness_ratio(counts)
        quarantined = sum(counts[status] for status in _QUARANTINE_STATUSES)
        return MarketDataQualityHealth(
            interval=interval,
            source_run_id=source_run_id,
            request_count=int(row["request_count"] or 0),
            total_outcomes=int(row["total_outcomes"] or 0),
            expected_outcomes=int(row["expected_outcomes"] or 0),
            affected_instruments=int(row["affected_instruments"] or 0),
            latest_session_date=row["latest_session_date"],
            latest_candle_timestamp=row["latest_candle_timestamp"],
            observed_at=row["observed_at"],
            completeness_ratio=completeness,
            unexplained_gap_count=counts["missing"],
            quarantined_count=quarantined,
            raw_artifact_count=int(row["raw_artifact_count"] or 0),
            status_counts=counts,
            health_status=_quality_health(counts, completeness),
        )

    @staticmethod
    def _quality_issues(
        connection: Any,
        *,
        workspace_id: str,
        provider: str,
        exchange: str,
        latest_runs: dict[str, str],
        limit: int,
    ) -> list[MarketDataQualityIssue]:
        table = market_data_quality_outcomes_table
        rows: list[MarketDataQualityIssue] = []
        for interval, source_run_id in latest_runs.items():
            result = connection.execute(
                select(
                    table.c.status,
                    table.c.reason_code,
                    table.c.severity,
                    table.c.retryable,
                    func.count().label("occurrences"),
                    func.count(distinct(table.c.instrument_id)).label(
                        "affected_instruments"
                    ),
                    func.min(table.c.session_date).label("first_session_date"),
                    func.max(table.c.session_date).label("latest_session_date"),
                    func.max(table.c.observed_at).label("observed_at"),
                )
                .where(table.c.workspace_id == workspace_id)
                .where(table.c.provider == provider)
                .where(table.c.exchange == exchange)
                .where(table.c.interval == interval)
                .where(table.c.source_run_id == source_run_id)
                .where(table.c.status != "valid")
                .group_by(
                    table.c.status,
                    table.c.reason_code,
                    table.c.severity,
                    table.c.retryable,
                )
                .order_by(func.count().desc(), table.c.reason_code)
                .limit(limit)
            ).mappings()
            rows.extend(
                MarketDataQualityIssue(
                    interval=interval,
                    source_run_id=source_run_id,
                    status=str(row["status"]),
                    reason_code=str(row["reason_code"]),
                    severity=str(row["severity"]),
                    retryable=bool(row["retryable"]),
                    occurrences=int(row["occurrences"]),
                    affected_instruments=int(row["affected_instruments"]),
                    first_session_date=row["first_session_date"],
                    latest_session_date=row["latest_session_date"],
                    observed_at=row["observed_at"],
                )
                for row in result
            )
        rows.sort(key=lambda row: (-row.occurrences, row.interval, row.reason_code))
        return rows[:limit]

    @staticmethod
    def _raw_lineage(
        connection: Any,
        *,
        workspace_id: str,
        provider: str,
        exchange: str,
        latest_runs: dict[str, str],
        limit: int,
    ) -> list[MarketDataRawLineage]:
        quality = market_data_quality_outcomes_table
        artifacts = artifact_manifests_table
        rows: dict[str, MarketDataRawLineage] = {}
        for interval, source_run_id in latest_runs.items():
            result = connection.execute(
                select(
                    artifacts.c.artifact_manifest_id,
                    artifacts.c.artifact_type,
                    artifacts.c.sha256,
                    artifacts.c.size_bytes,
                    artifacts.c.media_type,
                    artifacts.c.object_version_id,
                    artifacts.c.created_at,
                )
                .select_from(
                    quality.join(
                        artifacts,
                        quality.c.raw_artifact_id == artifacts.c.artifact_manifest_id,
                    )
                )
                .where(quality.c.workspace_id == workspace_id)
                .where(quality.c.provider == provider)
                .where(quality.c.exchange == exchange)
                .where(quality.c.interval == interval)
                .where(quality.c.source_run_id == source_run_id)
                .distinct()
                .order_by(artifacts.c.created_at.desc())
                .limit(limit)
            ).mappings()
            for row in result:
                artifact_id = str(row["artifact_manifest_id"])
                rows[artifact_id] = MarketDataRawLineage(
                    artifact_manifest_id=artifact_id,
                    artifact_type=str(row["artifact_type"]),
                    sha256=str(row["sha256"]),
                    size_bytes=int(row["size_bytes"]),
                    media_type=str(row["media_type"]),
                    object_versioned=bool(row["object_version_id"]),
                    created_at=row["created_at"],
                )
        return sorted(rows.values(), key=lambda row: row.created_at, reverse=True)[:limit]

    @staticmethod
    def _latest_replication(
        connection: Any,
        *,
        workspace_id: str,
        exchange: str,
    ) -> list[MarketDataReplicationHealth]:
        table = market_data_replication_checkpoints_table
        result = connection.execute(
            select(table)
            .where(table.c.workspace_id == workspace_id)
            .where(table.c.exchange == exchange)
            .order_by(table.c.updated_at.desc(), table.c.replication_checkpoint_id.desc())
            .limit(100)
        ).mappings()
        latest: dict[tuple[str, str], MarketDataReplicationHealth] = {}
        for row in result:
            key = (str(row["interval"]), str(row["dataset_key"]))
            if key in latest:
                continue
            destination_count = row["destination_row_count"]
            destination_digest = row["destination_digest"]
            latest[key] = MarketDataReplicationHealth(
                interval=str(row["interval"]),
                source_run_id=str(row["source_run_id"]),
                dataset_key=str(row["dataset_key"]),
                source_store=str(row["source_store"]),
                destination_store=str(row["destination_store"]),
                status=str(row["status"]),
                source_row_count=int(row["source_row_count"]),
                destination_row_count=(
                    int(destination_count) if destination_count is not None else None
                ),
                counts_match=(
                    int(row["source_row_count"]) == int(destination_count)
                    if destination_count is not None
                    else None
                ),
                digests_match=(
                    str(row["source_digest"]) == str(destination_digest)
                    if destination_digest is not None
                    else None
                ),
                source_watermark=row["source_watermark"],
                destination_watermark=row["destination_watermark"],
                watermark_lag_seconds=row["watermark_lag_seconds"],
                replication_latency_ms=row["replication_latency_ms"],
                error_message=row["error_message"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                updated_at=row["updated_at"],
            )
        return sorted(latest.values(), key=lambda row: (row.interval, row.dataset_key))


def _completeness_ratio(counts: dict[str, int]) -> float | None:
    expected = sum(
        counts[status]
        for status in ("valid", "missing", "provider_unavailable", "invalid", "stale")
    )
    if expected == 0:
        return None
    return counts["valid"] / expected


def _quality_health(counts: dict[str, int], completeness: float | None) -> str:
    if completeness is None:
        return "unknown"
    if counts["missing"] or counts["invalid"]:
        return "failed"
    if (
        completeness < 0.995
        or counts["provider_unavailable"]
        or any(counts[status] for status in _QUARANTINE_STATUSES)
    ):
        return "degraded"
    return "healthy"


def _overall_health(
    quality: list[MarketDataQualityHealth],
    replication: list[MarketDataReplicationHealth],
) -> str:
    if any(row.status in {"failed", "mismatch"} for row in replication):
        return "failed"
    if not quality:
        return "unknown"
    if any(row.health_status == "failed" for row in quality):
        return "failed"
    if any(row.health_status in {"degraded", "unknown"} for row in quality):
        return "degraded"
    replicated_intervals = {row.interval for row in replication}
    if any(row.interval not in replicated_intervals for row in quality):
        return "degraded"
    if any(row.status != "reconciled" for row in replication):
        return "degraded"
    return "healthy"
