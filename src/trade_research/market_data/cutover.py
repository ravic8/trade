from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import Engine, desc, insert, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trade_research.control_plane.tables import (
    audit_events_table,
    nse_provider_comparison_evidence_table,
    nse_provider_cutover_decisions_table,
)

CutoverAction = Literal["approve_yfinance_primary", "rollback_to_upstox"]


@dataclass(frozen=True)
class CutoverEligibility:
    eligible: bool
    required_passing_windows: int
    passing_windows: int
    evidence_ids: list[str]
    evidence_bundle_sha256: str
    blocking_issues: list[str]


@dataclass(frozen=True)
class CutoverDecision:
    decision_id: str
    action: CutoverAction
    from_provider: str
    to_provider: str
    evidence_ids: list[str]
    evidence_bundle_sha256: str
    actor_email: str
    reason: str
    decision_sha256: str
    created_at: datetime

    @property
    def yfinance_approved(self) -> bool:
        return self.action == "approve_yfinance_primary"


@dataclass(frozen=True)
class CutoverStatus:
    configured_primary: str
    effective_primary: str
    eligibility: CutoverEligibility
    active_decision: CutoverDecision | None
    evidence: list[dict[str, Any]]

    @property
    def yfinance_approved(self) -> bool:
        return self.active_decision is not None and self.active_decision.yfinance_approved


class NseProviderCutoverRepository:
    """Append-only NSE provider evidence and authenticated decision ledger."""

    def __init__(self, engine: Engine, *, workspace_id: str = "default") -> None:
        self._engine = engine
        self._workspace_id = workspace_id

    def record_comparison(
        self,
        *,
        status: str,
        metrics: dict[str, Any],
        blocking_issues: list[str],
        observed_at: datetime,
    ) -> dict[str, Any]:
        evidence_payload = {
            "workspace_id": self._workspace_id,
            "window_start": metrics["window_start"],
            "window_end": metrics["window_end"],
            "status": status,
            "comparison_state": metrics["comparison_state"],
            "ready": bool(metrics["ready"]),
            "metrics": _json_value(metrics),
            "blocking_issues": list(blocking_issues),
        }
        evidence_sha256 = _digest(evidence_payload)
        evidence_id = evidence_sha256
        created_at = _as_utc(observed_at)
        values = {
            "evidence_id": evidence_id,
            "workspace_id": self._workspace_id,
            "window_start": date.fromisoformat(str(metrics["window_start"])),
            "window_end": date.fromisoformat(str(metrics["window_end"])),
            "status": status,
            "comparison_state": str(metrics["comparison_state"]),
            "ready": bool(metrics["ready"]),
            "metrics": _json_value(metrics),
            "blocking_issues": list(blocking_issues),
            "evidence_sha256": evidence_sha256,
            "observed_at": created_at,
            "created_at": created_at,
        }
        statement = _insert_do_nothing(
            self._engine,
            nse_provider_comparison_evidence_table,
            values,
            ["evidence_id"],
        )
        with self._engine.begin() as connection:
            connection.execute(statement)
            row = (
                connection.execute(
                    select(nse_provider_comparison_evidence_table).where(
                        nse_provider_comparison_evidence_table.c.evidence_id == evidence_id
                    )
                )
                .mappings()
                .one()
            )
        return _evidence_dict(row)

    def recent_evidence(self, *, limit: int = 30) -> list[dict[str, Any]]:
        query = (
            select(nse_provider_comparison_evidence_table)
            .where(nse_provider_comparison_evidence_table.c.workspace_id == self._workspace_id)
            .order_by(
                desc(nse_provider_comparison_evidence_table.c.window_end),
                desc(nse_provider_comparison_evidence_table.c.observed_at),
            )
            .limit(limit)
        )
        with self._engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [_evidence_dict(row) for row in rows]

    def eligibility(self, *, required_passing_windows: int) -> CutoverEligibility:
        rows = self.recent_evidence(limit=max(required_passing_windows * 5, 30))
        latest_by_window: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            window_end = str(row["window_end"])
            if window_end not in seen:
                seen.add(window_end)
                latest_by_window.append(row)
        selected = latest_by_window[:required_passing_windows]
        passing = 0
        evidence_ids: list[str] = []
        for row in selected:
            if row["status"] != "pass" or not row["ready"]:
                break
            passing += 1
            evidence_ids.append(str(row["evidence_id"]))
        evidence_ids.reverse()
        issues: list[str] = []
        if passing < required_passing_windows:
            issues.append(
                "Provider comparison needs "
                f"{required_passing_windows} consecutive passing session windows; "
                f"{passing} are currently available."
            )
        bundle = self._evidence_bundle_digest(evidence_ids)
        return CutoverEligibility(
            eligible=passing >= required_passing_windows,
            required_passing_windows=required_passing_windows,
            passing_windows=passing,
            evidence_ids=evidence_ids,
            evidence_bundle_sha256=bundle,
            blocking_issues=issues,
        )

    def latest_decision(self) -> CutoverDecision | None:
        query = (
            select(nse_provider_cutover_decisions_table)
            .where(nse_provider_cutover_decisions_table.c.workspace_id == self._workspace_id)
            .order_by(desc(nse_provider_cutover_decisions_table.c.created_at))
            .limit(1)
        )
        with self._engine.connect() as connection:
            row = connection.execute(query).mappings().first()
        return _decision(row) if row is not None else None

    def approve(
        self,
        *,
        actor_email: str,
        reason: str,
        idempotency_key: str,
        expected_evidence_bundle_sha256: str,
        required_passing_windows: int,
        at: datetime | None = None,
    ) -> CutoverDecision:
        existing = self._decision_for_idempotency_key(idempotency_key)
        if existing is not None:
            self._assert_replay_matches(
                existing,
                action="approve_yfinance_primary",
                actor_email=actor_email,
                reason=reason,
                evidence_bundle_sha256=expected_evidence_bundle_sha256,
            )
            return existing
        eligibility = self.eligibility(required_passing_windows=required_passing_windows)
        if not eligibility.eligible:
            raise ValueError(eligibility.blocking_issues[0])
        if eligibility.evidence_bundle_sha256 != expected_evidence_bundle_sha256:
            raise ValueError("Cutover evidence changed; refresh and review it again.")
        return self._record_decision(
            action="approve_yfinance_primary",
            from_provider="upstox",
            to_provider="yfinance",
            evidence_ids=eligibility.evidence_ids,
            evidence_bundle_sha256=eligibility.evidence_bundle_sha256,
            actor_email=actor_email,
            reason=reason,
            idempotency_key=idempotency_key,
            at=at,
        )

    def rollback(
        self,
        *,
        actor_email: str,
        reason: str,
        idempotency_key: str,
        expected_current_decision_sha256: str,
        at: datetime | None = None,
    ) -> CutoverDecision:
        existing = self._decision_for_idempotency_key(idempotency_key)
        if existing is not None:
            self._assert_replay_matches(
                existing,
                action="rollback_to_upstox",
                actor_email=actor_email,
                reason=reason,
                evidence_bundle_sha256=existing.evidence_bundle_sha256,
            )
            return existing
        current = self.latest_decision()
        if current is None or not current.yfinance_approved:
            raise ValueError("No active yfinance cutover approval exists.")
        if current.decision_sha256 != expected_current_decision_sha256:
            raise ValueError("Cutover decision changed; refresh before rolling back.")
        return self._record_decision(
            action="rollback_to_upstox",
            from_provider="yfinance",
            to_provider="upstox",
            evidence_ids=current.evidence_ids,
            evidence_bundle_sha256=current.evidence_bundle_sha256,
            actor_email=actor_email,
            reason=reason,
            idempotency_key=idempotency_key,
            at=at,
        )

    def status(
        self,
        *,
        configured_primary: str,
        required_passing_windows: int,
        evidence_limit: int = 20,
    ) -> CutoverStatus:
        decision = self.latest_decision()
        approved = decision is not None and decision.yfinance_approved
        effective = "yfinance" if configured_primary == "yfinance" and approved else "upstox"
        return CutoverStatus(
            configured_primary=configured_primary,
            effective_primary=effective,
            eligibility=self.eligibility(required_passing_windows=required_passing_windows),
            active_decision=decision,
            evidence=self.recent_evidence(limit=evidence_limit),
        )

    def _record_decision(
        self,
        *,
        action: CutoverAction,
        from_provider: str,
        to_provider: str,
        evidence_ids: list[str],
        evidence_bundle_sha256: str,
        actor_email: str,
        reason: str,
        idempotency_key: str,
        at: datetime | None,
    ) -> CutoverDecision:
        existing = self._decision_for_idempotency_key(idempotency_key)
        if existing is not None:
            if (
                existing.action != action
                or existing.actor_email != actor_email
                or existing.reason != reason
                or existing.evidence_bundle_sha256 != evidence_bundle_sha256
            ):
                raise ValueError("Idempotency key is already bound to another decision.")
            return existing
        created_at = _as_utc(at or datetime.now(UTC))
        decision_payload = {
            "workspace_id": self._workspace_id,
            "action": action,
            "from_provider": from_provider,
            "to_provider": to_provider,
            "evidence_ids": evidence_ids,
            "evidence_bundle_sha256": evidence_bundle_sha256,
            "actor_email": actor_email.strip().lower(),
            "reason": reason.strip(),
            "created_at": created_at.isoformat(),
        }
        decision_sha256 = _digest(decision_payload)
        decision_id = _digest(
            {
                "workspace_id": self._workspace_id,
                "idempotency_key": idempotency_key,
            }
        )
        values = {
            **decision_payload,
            "decision_id": decision_id,
            "idempotency_key": idempotency_key,
            "decision_sha256": decision_sha256,
            "created_at": created_at,
        }
        audit_values = {
            "audit_event_id": str(uuid5(NAMESPACE_URL, f"nse-cutover:{decision_id}")),
            "actor": decision_payload["actor_email"],
            "action": action,
            "entity_type": "nse_provider_cutover",
            "entity_id": decision_id,
            "request_id": idempotency_key,
            "event_metadata": {
                "from_provider": from_provider,
                "to_provider": to_provider,
                "evidence_bundle_sha256": evidence_bundle_sha256,
                "decision_sha256": decision_sha256,
            },
            "created_at": created_at,
        }
        with self._engine.begin() as connection:
            connection.execute(insert(nse_provider_cutover_decisions_table).values(**values))
            connection.execute(insert(audit_events_table).values(**audit_values))
        return CutoverDecision(
            decision_id=decision_id,
            action=action,
            from_provider=from_provider,
            to_provider=to_provider,
            evidence_ids=evidence_ids,
            evidence_bundle_sha256=evidence_bundle_sha256,
            actor_email=str(decision_payload["actor_email"]),
            reason=str(decision_payload["reason"]),
            decision_sha256=decision_sha256,
            created_at=created_at,
        )

    def _decision_for_idempotency_key(self, key: str) -> CutoverDecision | None:
        query = select(nse_provider_cutover_decisions_table).where(
            nse_provider_cutover_decisions_table.c.workspace_id == self._workspace_id,
            nse_provider_cutover_decisions_table.c.idempotency_key == key,
        )
        with self._engine.connect() as connection:
            row = connection.execute(query).mappings().first()
        return _decision(row) if row is not None else None

    @staticmethod
    def _assert_replay_matches(
        existing: CutoverDecision,
        *,
        action: CutoverAction,
        actor_email: str,
        reason: str,
        evidence_bundle_sha256: str,
    ) -> None:
        if (
            existing.action != action
            or existing.actor_email != actor_email.strip().lower()
            or existing.reason != reason.strip()
            or existing.evidence_bundle_sha256 != evidence_bundle_sha256
        ):
            raise ValueError("Idempotency key is already bound to another decision.")

    def _evidence_bundle_digest(self, evidence_ids: list[str]) -> str:
        if not evidence_ids:
            return _digest({"workspace_id": self._workspace_id, "evidence": []})
        query = select(
            nse_provider_comparison_evidence_table.c.evidence_id,
            nse_provider_comparison_evidence_table.c.evidence_sha256,
        ).where(
            nse_provider_comparison_evidence_table.c.workspace_id == self._workspace_id,
            nse_provider_comparison_evidence_table.c.evidence_id.in_(evidence_ids),
        )
        with self._engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        digests = {str(row["evidence_id"]): str(row["evidence_sha256"]) for row in rows}
        return _digest(
            {
                "workspace_id": self._workspace_id,
                "evidence": [digests[item] for item in evidence_ids if item in digests],
            }
        )


def _insert_do_nothing(
    engine: Engine, table: Any, values: dict[str, Any], index_elements: list[str]
) -> Any:
    if engine.dialect.name == "postgresql":
        return (
            postgresql_insert(table)
            .values(**values)
            .on_conflict_do_nothing(index_elements=index_elements)
        )
    if engine.dialect.name == "sqlite":
        return (
            sqlite_insert(table)
            .values(**values)
            .on_conflict_do_nothing(index_elements=index_elements)
        )
    return insert(table).values(**values)


def _evidence_dict(row: Any) -> dict[str, Any]:
    return {
        "evidence_id": str(row["evidence_id"]),
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "status": str(row["status"]),
        "comparison_state": str(row["comparison_state"]),
        "ready": bool(row["ready"]),
        "metrics": dict(row["metrics"]),
        "blocking_issues": list(row["blocking_issues"]),
        "evidence_sha256": str(row["evidence_sha256"]),
        "observed_at": row["observed_at"],
    }


def _decision(row: Any) -> CutoverDecision:
    return CutoverDecision(
        decision_id=str(row["decision_id"]),
        action=str(row["action"]),  # type: ignore[arg-type]
        from_provider=str(row["from_provider"]),
        to_provider=str(row["to_provider"]),
        evidence_ids=list(row["evidence_ids"]),
        evidence_bundle_sha256=str(row["evidence_bundle_sha256"]),
        actor_email=str(row["actor_email"]),
        reason=str(row["reason"]),
        decision_sha256=str(row["decision_sha256"]),
        created_at=row["created_at"],
    )


def _digest(value: Any) -> str:
    encoded = json.dumps(_json_value(value), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
