from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Engine, case, desc, distinct, func, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trade_research.control_plane.tables import (
    market_data_availability_observations_table,
    market_data_quality_outcomes_table,
    market_data_replication_checkpoints_table,
    nse_provider_cutover_decisions_table,
    phase3_readiness_evidence_table,
)
from trade_research.market_data.cutover import NseProviderCutoverRepository

_QUALITY_STATUSES = (
    "valid",
    "missing",
    "provider_unavailable",
    "duplicate",
    "invalid",
    "outside_session",
    "stale",
)
_DAILY_EXPECTED_STATUSES = (
    "valid",
    "missing",
    "provider_unavailable",
    "invalid",
    "stale",
)
_MINUTE_OBSERVED_STATUSES = ("valid", "missing", "invalid", "stale")
_REQUIRED_DRILL_CHECKS = (
    "rollback_effective_provider_upstox",
    "upstox_pipeline_healthy",
    "restore_required_explicit_approval",
    "post_restore_yfinance_pipeline_healthy",
)


@dataclass(frozen=True)
class Phase3Gate:
    name: str
    passed: bool
    reason: str
    evidence_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class Phase3Readiness:
    ready_for_production: bool
    checked_at: datetime
    gates: tuple[Phase3Gate, ...]
    blocking_issues: tuple[str, ...]
    evidence: tuple[dict[str, Any], ...]


class Phase3ReadinessRepository:
    """Build a fail-closed production gate from durable Phase 3 evidence."""

    def __init__(self, engine: Engine, *, workspace_id: str = "default") -> None:
        self._engine = engine
        self._workspace_id = workspace_id

    def assess_canary(
        self,
        *,
        daily_run_id: str,
        minute_run_id: str,
        minute_rerun_id: str,
        max_instruments: int,
        minimum_completeness: float = 0.995,
        required_observed_sessions: int = 5,
        observed_at: datetime | None = None,
    ) -> dict[str, Any]:
        run_ids = {
            "daily": _required_run_id(daily_run_id, "daily_run_id"),
            "minute": _required_run_id(minute_run_id, "minute_run_id"),
            "minute_rerun": _required_run_id(minute_rerun_id, "minute_rerun_id"),
        }
        if run_ids["minute"] == run_ids["minute_rerun"]:
            raise ValueError("minute_run_id and minute_rerun_id must be distinct")
        if max_instruments < 1:
            raise ValueError("max_instruments must be positive")
        if not 0.995 <= minimum_completeness <= 1:
            raise ValueError("minimum_completeness must be between 0.995 and 1")
        if required_observed_sessions < 2:
            raise ValueError("required_observed_sessions must be at least 2")

        blocking: list[str] = []
        metrics: dict[str, Any] = {
            "scope": {
                "provider": "yfinance",
                "exchange": "NSE",
                "max_instruments": max_instruments,
                "minimum_completeness": minimum_completeness,
                "required_observed_sessions": required_observed_sessions,
            },
            "runs": {},
        }
        evidence_refs: dict[str, Any] = {"replication_checkpoint_ids": []}
        all_sessions: set[str] = set()
        with self._engine.connect() as connection:
            for label, interval in (
                ("daily", "1d"),
                ("minute", "1m"),
                ("minute_rerun", "1m"),
            ):
                run_id = run_ids[label]
                quality = self._quality_metrics(connection, run_id=run_id, interval=interval)
                replication = self._replication_metrics(
                    connection, run_id=run_id, interval=interval
                )
                availability = (
                    self._availability_metrics(connection, run_id=run_id)
                    if interval == "1m"
                    else None
                )
                metrics["runs"][label] = {
                    "source_run_id": run_id,
                    "interval": interval,
                    "quality": quality,
                    "replication": replication,
                    "availability": availability,
                }
                if replication.get("replication_checkpoint_id"):
                    evidence_refs["replication_checkpoint_ids"].append(
                        replication["replication_checkpoint_id"]
                    )
                self._validate_run(
                    label=label,
                    quality=quality,
                    replication=replication,
                    availability=availability,
                    max_instruments=max_instruments,
                    minimum_completeness=minimum_completeness,
                    blocking=blocking,
                )
                if availability is not None:
                    all_sessions.update(availability["observed_sessions"])

        if len(all_sessions) < required_observed_sessions:
            blocking.append(
                "Minute canary evidence covers "
                f"{len(all_sessions)} distinct observed sessions; "
                f"{required_observed_sessions} are required."
            )

        first_replica = metrics["runs"]["minute"]["replication"]
        rerun_replica = metrics["runs"]["minute_rerun"]["replication"]
        first_business = first_replica.get("business_digest")
        rerun_business = rerun_replica.get("business_digest")
        first_business_rows = first_replica.get("business_row_count")
        rerun_business_rows = rerun_replica.get("business_row_count")
        equivalent = bool(first_business) and (
            first_business == rerun_business and first_business_rows == rerun_business_rows
        )
        metrics["idempotence"] = {
            "equivalent": equivalent,
            "first_business_digest": first_business,
            "rerun_business_digest": rerun_business,
            "first_business_row_count": first_business_rows,
            "rerun_business_row_count": rerun_business_rows,
        }
        if not equivalent:
            blocking.append(
                "Independent minute reruns do not have equal business row counts and digests."
            )

        evidence_refs["replication_checkpoint_ids"] = sorted(
            set(evidence_refs["replication_checkpoint_ids"])
        )
        return self._record(
            evidence_type="bounded_canary",
            status="pass" if not blocking else "fail",
            source_run_ids=run_ids,
            session_dates=sorted(all_sessions),
            metrics=metrics,
            blocking_issues=blocking,
            evidence_refs=evidence_refs,
            actor_email=None,
            reason=None,
            observed_at=observed_at,
        )

    def record_rollback_restore_drill(
        self,
        *,
        actor_email: str,
        reason: str,
        rollback_decision_sha256: str,
        restored_decision_sha256: str,
        checks: Mapping[str, bool],
        observed_at: datetime | None = None,
    ) -> dict[str, Any]:
        actor = actor_email.strip().lower()
        if not actor:
            raise ValueError("actor_email is required")
        normalized_reason = reason.strip()
        if len(normalized_reason) < 10:
            raise ValueError("reason must contain at least 10 characters")
        normalized_checks = {name: bool(checks.get(name)) for name in _REQUIRED_DRILL_CHECKS}
        missing = [name for name in _REQUIRED_DRILL_CHECKS if name not in checks]
        if missing:
            raise ValueError("missing drill checks: " + ", ".join(missing))

        decisions = nse_provider_cutover_decisions_table
        with self._engine.connect() as connection:
            rows = (
                connection.execute(
                    select(decisions).where(
                        decisions.c.workspace_id == self._workspace_id,
                        decisions.c.decision_sha256.in_(
                            [rollback_decision_sha256, restored_decision_sha256]
                        ),
                    )
                )
                .mappings()
                .all()
            )
        by_digest = {str(row["decision_sha256"]): row for row in rows}
        rollback = by_digest.get(rollback_decision_sha256)
        restored = by_digest.get(restored_decision_sha256)
        if rollback is None or rollback["action"] != "rollback_to_upstox":
            raise ValueError("rollback decision evidence was not found")
        if restored is None or restored["action"] != "approve_yfinance_primary":
            raise ValueError("restored approval decision evidence was not found")
        if restored["created_at"] <= rollback["created_at"]:
            raise ValueError("restored approval must occur after rollback")

        blocking = [
            f"Rollback drill check failed: {name}."
            for name, ok in normalized_checks.items()
            if not ok
        ]
        return self._record(
            evidence_type="rollback_restore_drill",
            status="pass" if not blocking else "fail",
            source_run_ids={},
            session_dates=[],
            metrics={"checks": normalized_checks},
            blocking_issues=blocking,
            evidence_refs={
                "rollback_decision_sha256": rollback_decision_sha256,
                "restored_decision_sha256": restored_decision_sha256,
            },
            actor_email=actor,
            reason=normalized_reason,
            observed_at=observed_at,
        )

    def readiness(
        self,
        *,
        required_passing_windows: int,
        evidence_limit: int = 20,
    ) -> Phase3Readiness:
        evidence = self.recent_evidence(limit=evidence_limit)
        latest = {row["evidence_type"]: row for row in reversed(evidence)}
        canary = latest.get("bounded_canary")
        drill = latest.get("rollback_restore_drill")
        cutover = NseProviderCutoverRepository(
            self._engine, workspace_id=self._workspace_id
        ).eligibility(required_passing_windows=required_passing_windows)
        gates = (
            _evidence_gate(
                "bounded_canary",
                canary,
                "No bounded daily/minute canary assessment has been recorded.",
            ),
            _evidence_gate(
                "rollback_restore_drill",
                drill,
                "No reviewed rollback/restore drill has been recorded.",
            ),
            Phase3Gate(
                name="provider_comparison",
                passed=cutover.eligible,
                reason=(
                    f"{cutover.passing_windows} consecutive provider windows passed."
                    if cutover.eligible
                    else cutover.blocking_issues[0]
                ),
                evidence_ids=tuple(cutover.evidence_ids),
            ),
        )
        blocking = tuple(gate.reason for gate in gates if not gate.passed)
        return Phase3Readiness(
            ready_for_production=not blocking,
            checked_at=datetime.now(UTC),
            gates=gates,
            blocking_issues=blocking,
            evidence=tuple(evidence),
        )

    def recent_evidence(self, *, limit: int = 20) -> list[dict[str, Any]]:
        table = phase3_readiness_evidence_table
        with self._engine.connect() as connection:
            rows = (
                connection.execute(
                    select(table)
                    .where(table.c.workspace_id == self._workspace_id)
                    .order_by(desc(table.c.observed_at), desc(table.c.evidence_id))
                    .limit(limit)
                )
                .mappings()
                .all()
            )
        return [_evidence_dict(row) for row in rows]

    def _quality_metrics(self, connection: Any, *, run_id: str, interval: str) -> dict[str, Any]:
        table = market_data_quality_outcomes_table
        row = (
            connection.execute(
                select(
                    func.count().label("total"),
                    func.count(distinct(table.c.instrument_id)).label("instruments"),
                    func.count(distinct(table.c.raw_artifact_id)).label("raw_artifacts"),
                    *[
                        func.sum(case((table.c.status == status, 1), else_=0)).label(status)
                        for status in _QUALITY_STATUSES
                    ],
                ).where(
                    table.c.workspace_id == self._workspace_id,
                    table.c.provider == "yfinance",
                    table.c.exchange == "NSE",
                    table.c.interval == interval,
                    table.c.source_run_id == run_id,
                )
            )
            .mappings()
            .one()
        )
        counts = {status: int(row[status] or 0) for status in _QUALITY_STATUSES}
        expected_statuses = (
            _MINUTE_OBSERVED_STATUSES if interval == "1m" else _DAILY_EXPECTED_STATUSES
        )
        expected = sum(counts[name] for name in expected_statuses)
        return {
            "total_outcomes": int(row["total"] or 0),
            "affected_instruments": int(row["instruments"] or 0),
            "raw_artifact_count": int(row["raw_artifacts"] or 0),
            "status_counts": counts,
            "completeness_ratio": counts["valid"] / expected if expected else None,
        }

    def _replication_metrics(
        self, connection: Any, *, run_id: str, interval: str
    ) -> dict[str, Any]:
        table = market_data_replication_checkpoints_table
        row = (
            connection.execute(
                select(table)
                .where(
                    table.c.workspace_id == self._workspace_id,
                    table.c.exchange == "NSE",
                    table.c.interval == interval,
                    table.c.source_run_id == run_id,
                )
                .order_by(desc(table.c.updated_at))
                .limit(1)
            )
            .mappings()
            .first()
        )
        if row is None:
            return {"status": "missing"}
        details = dict(row["details"] or {})
        return {
            "replication_checkpoint_id": str(row["replication_checkpoint_id"]),
            "status": str(row["status"]),
            "source_row_count": int(row["source_row_count"]),
            "destination_row_count": (
                int(row["destination_row_count"])
                if row["destination_row_count"] is not None
                else None
            ),
            "counts_match": row["destination_row_count"] == row["source_row_count"],
            "digests_match": row["destination_digest"] == row["source_digest"],
            "watermarks_match": row["destination_watermark"] == row["source_watermark"],
            "business_row_count": details.get("business_row_count"),
            "business_digest": details.get("business_digest"),
        }

    def _availability_metrics(self, connection: Any, *, run_id: str) -> dict[str, Any]:
        table = market_data_availability_observations_table
        rows = (
            connection.execute(
                select(table).where(
                    table.c.workspace_id == self._workspace_id,
                    table.c.provider == "yfinance",
                    table.c.exchange == "NSE",
                    table.c.interval == "1m",
                    table.c.source_run_id == run_id,
                )
            )
            .mappings()
            .all()
        )
        sessions = sorted(
            {str(value) for row in rows for value in list(row["observed_sessions"] or [])}
        )
        return {
            "instruments_total": len(rows),
            "instruments_observed": sum(row["status"] == "observed" for row in rows),
            "instruments_empty": sum(row["status"] == "empty" for row in rows),
            "instruments_failed": sum(row["status"] == "request_failed" for row in rows),
            "observed_sessions": sessions,
            "observed_row_count": sum(int(row["observed_row_count"]) for row in rows),
        }

    @staticmethod
    def _validate_run(
        *,
        label: str,
        quality: Mapping[str, Any],
        replication: Mapping[str, Any],
        availability: Mapping[str, Any] | None,
        max_instruments: int,
        minimum_completeness: float,
        blocking: list[str],
    ) -> None:
        instruments = int(quality["affected_instruments"])
        if instruments < 1:
            blocking.append(f"{label} run has no durable quality outcomes.")
        elif instruments > max_instruments:
            blocking.append(
                f"{label} run affected {instruments} instruments; "
                f"bounded canary maximum is {max_instruments}."
            )
        completeness = quality["completeness_ratio"]
        if completeness is None or completeness < minimum_completeness:
            rendered = "unavailable" if completeness is None else f"{completeness:.4%}"
            blocking.append(
                f"{label} completeness is {rendered}; "
                f"at least {minimum_completeness:.2%} is required."
            )
        counts = quality["status_counts"]
        if counts["missing"]:
            blocking.append(f"{label} has {counts['missing']} unexplained missing candles.")
        quarantined = sum(
            counts[name] for name in ("duplicate", "invalid", "outside_session", "stale")
        )
        if quarantined:
            blocking.append(f"{label} has {quarantined} quarantined candles.")
        if quality["raw_artifact_count"] < 1:
            blocking.append(f"{label} has no immutable raw-artifact lineage.")
        if not (
            replication.get("status") == "reconciled"
            and replication.get("counts_match")
            and replication.get("digests_match")
            and replication.get("watermarks_match")
        ):
            blocking.append(f"{label} ClickHouse count, digest, and watermark are not reconciled.")
        if availability is not None:
            if availability["instruments_total"] < 1:
                blocking.append(f"{label} has no observed provider-availability evidence.")
            if availability["instruments_failed"]:
                blocking.append(
                    f"{label} has {availability['instruments_failed']} provider request failures."
                )

    def _record(
        self,
        *,
        evidence_type: str,
        status: str,
        source_run_ids: Mapping[str, str],
        session_dates: list[str],
        metrics: Mapping[str, Any],
        blocking_issues: list[str],
        evidence_refs: Mapping[str, Any],
        actor_email: str | None,
        reason: str | None,
        observed_at: datetime | None,
    ) -> dict[str, Any]:
        at = _as_utc(observed_at or datetime.now(UTC))
        payload = {
            "workspace_id": self._workspace_id,
            "evidence_type": evidence_type,
            "status": status,
            "source_run_ids": dict(source_run_ids),
            "session_dates": list(session_dates),
            "metrics": _json_value(metrics),
            "blocking_issues": list(blocking_issues),
            "evidence_refs": _json_value(evidence_refs),
            "actor_email": actor_email,
            "reason": reason,
        }
        digest = _digest(payload)
        values = {
            **payload,
            "evidence_id": digest,
            "evidence_sha256": digest,
            "observed_at": at,
            "created_at": at,
        }
        dialect = self._engine.dialect.name
        if dialect == "postgresql":
            statement: Any = postgresql_insert(phase3_readiness_evidence_table).values(**values)
            statement = statement.on_conflict_do_nothing(index_elements=["evidence_id"])
        elif dialect == "sqlite":
            statement = sqlite_insert(phase3_readiness_evidence_table).values(**values)
            statement = statement.on_conflict_do_nothing(index_elements=["evidence_id"])
        else:
            statement = phase3_readiness_evidence_table.insert().values(**values)
        with self._engine.begin() as connection:
            connection.execute(statement)
            row = (
                connection.execute(
                    select(phase3_readiness_evidence_table).where(
                        phase3_readiness_evidence_table.c.evidence_id == digest
                    )
                )
                .mappings()
                .one()
            )
        return _evidence_dict(row)


def _evidence_gate(name: str, evidence: dict[str, Any] | None, missing_reason: str) -> Phase3Gate:
    if evidence is None:
        return Phase3Gate(name=name, passed=False, reason=missing_reason)
    passed = evidence["status"] == "pass"
    return Phase3Gate(
        name=name,
        passed=passed,
        reason=(
            "Latest evidence passed."
            if passed
            else "; ".join(evidence["blocking_issues"]) or "Latest evidence failed."
        ),
        evidence_ids=(str(evidence["evidence_id"]),),
    )


def _required_run_id(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized or len(normalized) > 255:
        raise ValueError(f"{name} is invalid")
    return normalized


def _evidence_dict(row: Any) -> dict[str, Any]:
    return {
        "evidence_id": str(row["evidence_id"]),
        "evidence_type": str(row["evidence_type"]),
        "status": str(row["status"]),
        "source_run_ids": dict(row["source_run_ids"] or {}),
        "session_dates": list(row["session_dates"] or []),
        "metrics": dict(row["metrics"] or {}),
        "blocking_issues": list(row["blocking_issues"] or []),
        "evidence_refs": dict(row["evidence_refs"] or {}),
        "actor_email": row["actor_email"],
        "reason": row["reason"],
        "evidence_sha256": str(row["evidence_sha256"]),
        "observed_at": _as_utc(row["observed_at"]),
    }


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(_json_value(payload), sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
