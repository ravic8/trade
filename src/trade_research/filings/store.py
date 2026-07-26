from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import and_, create_engine, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine

from trade_research.filings.models import (
    CandidateStatus,
    EvidenceReference,
    FilingDocument,
    FilingDocumentStatus,
    FilingRun,
    FilingRunStatus,
    FilingUniverseSnapshot,
    FinancialFact,
    IntelligenceObject,
    InvestigationEvent,
    InvestigationRun,
    InvestigationStatus,
    ReviewDecision,
    ReviewItemAction,
    ReviewRequest,
    ReviewStatus,
    ValidationDefect,
    ValidationStatus,
)
from trade_research.filings.tables import (
    filing_analysis_runs_table,
    filing_approved_facts_table,
    filing_audit_events_table,
    filing_candidate_facts_table,
    filing_documents_table,
    filing_evidence_table,
    filing_index_runs_table,
    filing_intelligence_objects_table,
    filing_investigation_events_table,
    filing_investigation_runs_table,
    filing_metadata,
    filing_review_requests_table,
    filing_runs_table,
    filing_universe_snapshots_table,
    filing_validation_defects_table,
)


def stable_id(kind: str, *parts: object) -> str:
    payload = "|".join([kind, *(str(part) for part in parts)])
    return str(uuid5(NAMESPACE_URL, payload))


def utc_now() -> datetime:
    return datetime.now(UTC)


class FilingStore:
    """Authoritative filing-intelligence business store.

    LangGraph checkpoints deliberately live outside this metadata collection.
    """

    def __init__(self, database_url: str) -> None:
        connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
        self.engine: Engine = create_engine(
            database_url,
            pool_pre_ping=True,
            hide_parameters=True,
            connect_args=connect_args,
        )

    def initialize(self) -> None:
        filing_metadata.create_all(self.engine)

    @contextmanager
    def begin(self):
        with self.engine.begin() as connection:
            yield connection

    def register_document(self, document: FilingDocument) -> tuple[FilingDocument, bool, bool]:
        now = utc_now()
        with self.engine.begin() as connection:
            existing_row = (
                connection.execute(
                    select(filing_documents_table).where(
                        filing_documents_table.c.workspace_id == document.workspace_id,
                        filing_documents_table.c.company_id == document.company_id,
                        filing_documents_table.c.sha256 == document.sha256,
                    )
                )
                .mappings()
                .first()
            )
            if existing_row:
                return self._document_model(existing_row), False, False

            latest = (
                connection.execute(
                    select(filing_documents_table)
                    .where(
                        filing_documents_table.c.workspace_id == document.workspace_id,
                        filing_documents_table.c.document_key == document.document_key,
                    )
                    .order_by(filing_documents_table.c.version.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            version = int(latest["version"]) + 1 if latest else 1
            supersedes = str(latest["filing_id"]) if latest else None
            if latest:
                connection.execute(
                    update(filing_documents_table)
                    .where(filing_documents_table.c.filing_id == latest["filing_id"])
                    .values(
                        is_current=False,
                        status=FilingDocumentStatus.SUPERSEDED.value,
                        updated_at=now,
                    )
                )

            row = document.model_dump(mode="python")
            row.update(
                {
                    "version": version,
                    "supersedes_filing_id": supersedes,
                    "is_current": True,
                    "status": FilingDocumentStatus.REGISTERED.value,
                    "consolidation_scope": document.consolidation_scope.value,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            connection.execute(filing_documents_table.insert(), row)
            inserted = (
                connection.execute(
                    select(filing_documents_table).where(
                        filing_documents_table.c.filing_id == document.filing_id
                    )
                )
                .mappings()
                .one()
            )
            return self._document_model(inserted), True, latest is not None

    def document(self, filing_id: str, workspace_id: str) -> FilingDocument | None:
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(filing_documents_table).where(
                        filing_documents_table.c.filing_id == filing_id,
                        filing_documents_table.c.workspace_id == workspace_id,
                    )
                )
                .mappings()
                .first()
            )
        return self._document_model(row) if row else None

    def documents(
        self,
        *,
        workspace_id: str,
        company_id: str | None = None,
        category: str | None = None,
        current_only: bool = False,
        limit: int = 200,
    ) -> list[FilingDocument]:
        query = select(filing_documents_table).where(
            filing_documents_table.c.workspace_id == workspace_id
        )
        if company_id:
            query = query.where(filing_documents_table.c.company_id == company_id)
        if current_only:
            query = query.where(filing_documents_table.c.is_current.is_(True))
        query = query.order_by(
            filing_documents_table.c.period_end.desc(),
            filing_documents_table.c.filing_date.desc(),
        ).limit(limit)
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        documents = [self._document_model(row) for row in rows]
        if category:
            documents = [item for item in documents if category in item.categories]
        return documents

    def update_document_parse(
        self,
        filing_id: str,
        *,
        status: FilingDocumentStatus,
        parse_quality: float | None,
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                update(filing_documents_table)
                .where(filing_documents_table.c.filing_id == filing_id)
                .values(
                    status=status.value,
                    parse_quality=parse_quality,
                    updated_at=utc_now(),
                )
            )

    def create_run(
        self,
        *,
        workspace_id: str,
        company_id: str,
        filing_id: str,
        idempotency_key: str,
        max_attempts: int,
        input_payload: Mapping[str, Any] | None = None,
    ) -> tuple[FilingRun, bool]:
        now = utc_now()
        with self.engine.begin() as connection:
            existing = (
                connection.execute(
                    select(filing_runs_table).where(
                        filing_runs_table.c.workspace_id == workspace_id,
                        filing_runs_table.c.idempotency_key == idempotency_key,
                    )
                )
                .mappings()
                .first()
            )
            if existing:
                if existing["filing_id"] != filing_id:
                    raise ValueError("idempotency key is already bound to another filing")
                return self._run_model(existing), False

            run_id = stable_id("filing-run", workspace_id, idempotency_key)
            row = {
                "run_id": run_id,
                "thread_id": stable_id("filing-thread", workspace_id, run_id),
                "workspace_id": workspace_id,
                "company_id": company_id,
                "filing_id": filing_id,
                "workflow_type": "filing.document.intelligence",
                "idempotency_key": idempotency_key,
                "status": FilingRunStatus.ACCEPTED.value,
                "current_node": "accepted",
                "progress": 0.0,
                "attempt_count": 0,
                "max_attempts": max_attempts,
                "cancel_requested": False,
                "input_payload": dict(input_payload or {}),
                "output_payload": {},
                "error_code": None,
                "error_message": None,
                "worker_id": None,
                "trace_id": None,
                "queued_at": None,
                "started_at": None,
                "heartbeat_at": None,
                "lease_expires_at": None,
                "waiting_review_at": None,
                "finished_at": None,
                "created_at": now,
                "updated_at": now,
            }
            connection.execute(filing_runs_table.insert(), row)
        return FilingRun.model_validate(row), True

    def mark_run_queued(self, run_id: str) -> None:
        now = utc_now()
        with self.engine.begin() as connection:
            connection.execute(
                update(filing_runs_table)
                .where(
                    filing_runs_table.c.run_id == run_id,
                    filing_runs_table.c.status.in_(
                        [
                            FilingRunStatus.ACCEPTED.value,
                            FilingRunStatus.RETRYING.value,
                            FilingRunStatus.WAITING_REVIEW.value,
                        ]
                    ),
                )
                .values(
                    status=FilingRunStatus.QUEUED.value,
                    current_node="queued",
                    queued_at=now,
                    updated_at=now,
                )
            )

    def claim_run(self, run_id: str, *, worker_id: str, lease_seconds: int) -> bool:
        now = utc_now()
        eligible = or_(
            filing_runs_table.c.status.in_(
                [
                    FilingRunStatus.ACCEPTED.value,
                    FilingRunStatus.QUEUED.value,
                    FilingRunStatus.RETRYING.value,
                ]
            ),
            and_(
                filing_runs_table.c.status == FilingRunStatus.RUNNING.value,
                filing_runs_table.c.lease_expires_at < now,
            ),
        )
        with self.engine.begin() as connection:
            result = connection.execute(
                update(filing_runs_table)
                .where(
                    filing_runs_table.c.run_id == run_id,
                    eligible,
                    filing_runs_table.c.cancel_requested.is_(False),
                )
                .values(
                    status=FilingRunStatus.RUNNING.value,
                    current_node="worker_claimed",
                    worker_id=worker_id,
                    started_at=func.coalesce(filing_runs_table.c.started_at, now),
                    heartbeat_at=now,
                    lease_expires_at=now + timedelta(seconds=lease_seconds),
                    attempt_count=filing_runs_table.c.attempt_count + 1,
                    updated_at=now,
                )
            )
        return bool(result.rowcount)

    def heartbeat_run(
        self,
        run_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
        current_node: str | None = None,
        progress: float | None = None,
    ) -> bool:
        now = utc_now()
        values: dict[str, Any] = {
            "heartbeat_at": now,
            "lease_expires_at": now + timedelta(seconds=lease_seconds),
            "updated_at": now,
        }
        if current_node is not None:
            values["current_node"] = current_node
        if progress is not None:
            values["progress"] = min(max(progress, 0.0), 1.0)
        with self.engine.begin() as connection:
            result = connection.execute(
                update(filing_runs_table)
                .where(
                    filing_runs_table.c.run_id == run_id,
                    filing_runs_table.c.worker_id == worker_id,
                    filing_runs_table.c.status == FilingRunStatus.RUNNING.value,
                )
                .values(**values)
            )
        return bool(result.rowcount)

    def run(self, run_id: str, workspace_id: str | None = None) -> FilingRun | None:
        query = select(filing_runs_table).where(filing_runs_table.c.run_id == run_id)
        if workspace_id:
            query = query.where(filing_runs_table.c.workspace_id == workspace_id)
        with self.engine.connect() as connection:
            row = connection.execute(query).mappings().first()
        return self._run_model(row) if row else None

    def runs(
        self,
        *,
        workspace_id: str,
        status: FilingRunStatus | None = None,
        limit: int = 100,
    ) -> list[FilingRun]:
        query = select(filing_runs_table).where(filing_runs_table.c.workspace_id == workspace_id)
        if status:
            query = query.where(filing_runs_table.c.status == status.value)
        query = query.order_by(filing_runs_table.c.created_at.desc()).limit(limit)
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [self._run_model(row) for row in rows]

    def request_cancel(self, run_id: str, workspace_id: str) -> bool:
        with self.engine.begin() as connection:
            result = connection.execute(
                update(filing_runs_table)
                .where(
                    filing_runs_table.c.run_id == run_id,
                    filing_runs_table.c.workspace_id == workspace_id,
                    filing_runs_table.c.status.not_in(
                        [
                            FilingRunStatus.COMPLETED.value,
                            FilingRunStatus.FAILED.value,
                            FilingRunStatus.CANCELLED.value,
                        ]
                    ),
                )
                .values(cancel_requested=True, updated_at=utc_now())
            )
        return bool(result.rowcount)

    def is_cancel_requested(self, run_id: str) -> bool:
        with self.engine.connect() as connection:
            value = connection.execute(
                select(filing_runs_table.c.cancel_requested).where(
                    filing_runs_table.c.run_id == run_id
                )
            ).scalar_one_or_none()
        return bool(value)

    def transition_run(
        self,
        run_id: str,
        *,
        status: FilingRunStatus,
        current_node: str,
        progress: float | None = None,
        output_payload: Mapping[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        trace_id: str | None = None,
    ) -> None:
        now = utc_now()
        values: dict[str, Any] = {
            "status": status.value,
            "current_node": current_node,
            "updated_at": now,
            "error_code": error_code,
            "error_message": error_message,
        }
        if progress is not None:
            values["progress"] = min(max(progress, 0.0), 1.0)
        if output_payload is not None:
            values["output_payload"] = dict(output_payload)
        if trace_id is not None:
            values["trace_id"] = trace_id
        if status == FilingRunStatus.WAITING_REVIEW:
            values["waiting_review_at"] = now
            values["worker_id"] = None
            values["lease_expires_at"] = None
        if status in {
            FilingRunStatus.COMPLETED,
            FilingRunStatus.FAILED,
            FilingRunStatus.CANCELLED,
        }:
            values["finished_at"] = now
            values["worker_id"] = None
            values["lease_expires_at"] = None
        with self.engine.begin() as connection:
            connection.execute(
                update(filing_runs_table)
                .where(filing_runs_table.c.run_id == run_id)
                .values(**values)
            )

    def recover_stale_runs(
        self,
        *,
        workspace_id: str | None = None,
        limit: int = 100,
    ) -> list[str]:
        now = utc_now()
        conditions = [
            filing_runs_table.c.status == FilingRunStatus.RUNNING.value,
            filing_runs_table.c.lease_expires_at < now,
            filing_runs_table.c.cancel_requested.is_(False),
            filing_runs_table.c.attempt_count < filing_runs_table.c.max_attempts,
        ]
        if workspace_id is not None:
            conditions.append(filing_runs_table.c.workspace_id == workspace_id)
        with self.engine.begin() as connection:
            eligible = (
                select(filing_runs_table.c.run_id)
                .where(*conditions)
                .order_by(filing_runs_table.c.lease_expires_at)
                .limit(limit)
            )
            rows = (
                connection.execute(
                    update(filing_runs_table)
                    .where(
                        filing_runs_table.c.run_id.in_(eligible),
                        *conditions,
                    )
                    .values(
                        status=FilingRunStatus.RETRYING.value,
                        current_node="lease_expired",
                        worker_id=None,
                        lease_expires_at=None,
                        updated_at=now,
                    )
                    .returning(filing_runs_table.c.run_id)
                )
                .scalars()
                .all()
            )
        return list(rows)

    def upsert_evidence(self, evidence: Iterable[EvidenceReference]) -> list[str]:
        now = utc_now()
        rows = [item.model_dump(mode="python") | {"created_at": now} for item in evidence]
        if not rows:
            return []
        with self.engine.begin() as connection:
            for row in rows:
                self._execute_upsert(
                    connection,
                    filing_evidence_table,
                    row,
                    index_elements=["evidence_id"],
                    update_columns=[
                        "page",
                        "section_path",
                        "table_name",
                        "row_label",
                        "column_label",
                        "xbrl_concept",
                        "context_ref",
                        "chunk_id",
                        "snippet",
                        "effective_date",
                    ],
                )
        return [str(row["evidence_id"]) for row in rows]

    def evidence(
        self,
        *,
        workspace_id: str,
        evidence_ids: Sequence[str] | None = None,
        filing_id: str | None = None,
        limit: int = 500,
    ) -> list[EvidenceReference]:
        query = select(filing_evidence_table).where(
            filing_evidence_table.c.workspace_id == workspace_id
        )
        if evidence_ids:
            query = query.where(filing_evidence_table.c.evidence_id.in_(evidence_ids))
        if filing_id:
            query = query.where(filing_evidence_table.c.filing_id == filing_id)
        query = query.limit(limit)
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [EvidenceReference.model_validate(dict(row)) for row in rows]

    def upsert_candidate_facts(self, candidates: Iterable[Mapping[str, Any]]) -> list[str]:
        now = utc_now()
        rows = []
        for candidate in candidates:
            row = dict(candidate)
            row.setdefault("validation_status", ValidationStatus.PENDING.value)
            row.setdefault("status", CandidateStatus.CANDIDATE.value)
            row["created_at"] = row.get("created_at") or now
            row["updated_at"] = now
            rows.append(row)
        with self.engine.begin() as connection:
            for row in rows:
                self._execute_upsert(
                    connection,
                    filing_candidate_facts_table,
                    row,
                    index_elements=["candidate_id"],
                    update_columns=[
                        "canonical_metric",
                        "reported_label",
                        "value_decimal",
                        "currency",
                        "unit_scale",
                        "period_start",
                        "period_end",
                        "period_type",
                        "consolidation_scope",
                        "evidence_ids",
                        "confidence",
                        "extractor_version",
                        "updated_at",
                    ],
                )
        return [str(row["candidate_id"]) for row in rows]

    def candidate_facts(self, run_id: str) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    select(filing_candidate_facts_table)
                    .where(filing_candidate_facts_table.c.run_id == run_id)
                    .order_by(
                        filing_candidate_facts_table.c.period_end,
                        filing_candidate_facts_table.c.canonical_metric,
                    )
                ).mappings()
            ]

    def upsert_intelligence_objects(self, objects: Iterable[IntelligenceObject]) -> list[str]:
        now = utc_now()
        rows = []
        for item in objects:
            row = item.model_dump(mode="python")
            row["object_type"] = item.object_type.value
            row["review_status"] = item.review_status.value
            row["value_decimal"] = (
                str(item.value_decimal) if item.value_decimal is not None else None
            )
            row["created_at"] = now
            row["updated_at"] = now
            rows.append(row)
        with self.engine.begin() as connection:
            for row in rows:
                self._execute_upsert(
                    connection,
                    filing_intelligence_objects_table,
                    row,
                    index_elements=["object_id"],
                    update_columns=[
                        "canonical_name",
                        "reported_label",
                        "value_decimal",
                        "value_text",
                        "currency",
                        "unit",
                        "period_start",
                        "period_end",
                        "evidence_ids",
                        "confidence",
                        "review_status",
                        "extractor_version",
                        "updated_at",
                    ],
                )
        return [str(row["object_id"]) for row in rows]

    def intelligence_objects(
        self, *, run_id: str | None = None, workspace_id: str | None = None
    ) -> list[dict[str, Any]]:
        query = select(filing_intelligence_objects_table)
        if run_id:
            query = query.where(filing_intelligence_objects_table.c.run_id == run_id)
        if workspace_id:
            query = query.where(filing_intelligence_objects_table.c.workspace_id == workspace_id)
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def set_intelligence_object_review_status(
        self,
        run_id: str,
        status: ReviewStatus,
    ) -> int:
        with self.engine.begin() as connection:
            result = connection.execute(
                update(filing_intelligence_objects_table)
                .where(filing_intelligence_objects_table.c.run_id == run_id)
                .values(review_status=status.value, updated_at=utc_now())
            )
        return int(result.rowcount or 0)

    def apply_intelligence_object_review_decisions(
        self,
        *,
        run_id: str,
        decisions: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, int]:
        allowed_edits = {
            "canonical_name",
            "reported_label",
            "value_decimal",
            "value_text",
            "currency",
            "unit",
            "period_start",
            "period_end",
        }
        counts = {"approved": 0, "edited": 0, "rejected": 0}
        now = utc_now()
        with self.engine.begin() as connection:
            objects = (
                connection.execute(
                    select(filing_intelligence_objects_table).where(
                        filing_intelligence_objects_table.c.run_id == run_id
                    )
                )
                .mappings()
                .all()
            )
            for item in objects:
                object_id = str(item["object_id"])
                decision = decisions.get(object_id)
                if decision is None:
                    raise ValueError(f"missing intelligence object decision: {object_id}")
                action = ReviewItemAction(str(decision["action"]))
                values: dict[str, Any] = {"updated_at": now}
                if action == ReviewItemAction.REJECT:
                    values["review_status"] = ReviewStatus.REJECTED.value
                    counts["rejected"] += 1
                elif action == ReviewItemAction.APPROVE:
                    values["review_status"] = ReviewStatus.APPROVED.value
                    counts["approved"] += 1
                else:
                    edits = dict(decision.get("edits") or {})
                    unsupported = sorted(set(edits) - allowed_edits)
                    if unsupported:
                        raise ValueError(
                            "unsupported intelligence object edit fields: " + ", ".join(unsupported)
                        )
                    for key in ("period_start", "period_end"):
                        if isinstance(edits.get(key), str):
                            edits[key] = date.fromisoformat(edits[key])
                    if edits.get("value_decimal") is not None:
                        Decimal(str(edits["value_decimal"]))
                        edits["value_decimal"] = str(edits["value_decimal"])
                    values.update(edits)
                    values["review_status"] = ReviewStatus.EDITED.value
                    counts["edited"] += 1
                connection.execute(
                    update(filing_intelligence_objects_table)
                    .where(
                        filing_intelligence_objects_table.c.object_id == object_id,
                        filing_intelligence_objects_table.c.run_id == run_id,
                    )
                    .values(**values)
                )
        return counts

    def replace_validation_defects(
        self,
        run_id: str,
        defects: Iterable[ValidationDefect],
        candidate_statuses: Mapping[str, ValidationStatus],
    ) -> None:
        now = utc_now()
        rows = [
            item.model_dump(mode="python") | {"severity": item.severity.value, "created_at": now}
            for item in defects
        ]
        with self.engine.begin() as connection:
            connection.execute(
                delete(filing_validation_defects_table).where(
                    filing_validation_defects_table.c.run_id == run_id
                )
            )
            if rows:
                connection.execute(filing_validation_defects_table.insert(), rows)
            for candidate_id, status in candidate_statuses.items():
                connection.execute(
                    update(filing_candidate_facts_table)
                    .where(filing_candidate_facts_table.c.candidate_id == candidate_id)
                    .values(validation_status=status.value, updated_at=now)
                )

    def validation_defects(self, run_id: str) -> list[ValidationDefect]:
        with self.engine.connect() as connection:
            rows = (
                connection.execute(
                    select(filing_validation_defects_table).where(
                        filing_validation_defects_table.c.run_id == run_id
                    )
                )
                .mappings()
                .all()
            )
        return [ValidationDefect.model_validate(dict(row)) for row in rows]

    def create_review_request(
        self,
        *,
        run_id: str,
        workspace_id: str,
        payload: Mapping[str, Any],
    ) -> ReviewRequest:
        now = utc_now()
        with self.engine.begin() as connection:
            existing = (
                connection.execute(
                    select(filing_review_requests_table)
                    .where(filing_review_requests_table.c.run_id == run_id)
                    .order_by(filing_review_requests_table.c.created_at.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            if existing:
                return self._review_model(existing)
            row = {
                "review_id": stable_id("filing-review", run_id, "pending"),
                "run_id": run_id,
                "workspace_id": workspace_id,
                "status": ReviewStatus.PENDING.value,
                "payload": dict(payload),
                "decision_payload": {},
                "reviewer_id": None,
                "reason": None,
                "created_at": now,
                "decided_at": None,
            }
            connection.execute(filing_review_requests_table.insert(), row)
        return ReviewRequest.model_validate(row)

    def pending_review_for_run(self, run_id: str) -> ReviewRequest | None:
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(filing_review_requests_table).where(
                        filing_review_requests_table.c.run_id == run_id,
                        filing_review_requests_table.c.status == ReviewStatus.PENDING.value,
                    )
                )
                .mappings()
                .first()
            )
        return self._review_model(row) if row else None

    def review(self, review_id: str, workspace_id: str) -> ReviewRequest | None:
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(filing_review_requests_table).where(
                        filing_review_requests_table.c.review_id == review_id,
                        filing_review_requests_table.c.workspace_id == workspace_id,
                    )
                )
                .mappings()
                .first()
            )
        return self._review_model(row) if row else None

    def reviews(
        self,
        *,
        workspace_id: str,
        status: ReviewStatus | None = ReviewStatus.PENDING,
        limit: int = 100,
    ) -> list[ReviewRequest]:
        query = select(filing_review_requests_table).where(
            filing_review_requests_table.c.workspace_id == workspace_id
        )
        if status:
            query = query.where(filing_review_requests_table.c.status == status.value)
        query = query.order_by(filing_review_requests_table.c.created_at.desc()).limit(limit)
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [self._review_model(row) for row in rows]

    def decide_review(
        self,
        *,
        review_id: str,
        workspace_id: str,
        decision: ReviewDecision,
        reviewer_id: str,
        reason: str,
        candidate_decisions: Mapping[str, Mapping[str, Any]] | None = None,
        object_decisions: Mapping[str, Mapping[str, Any]] | None = None,
        trace_id: str | None = None,
    ) -> ReviewRequest:
        now = utc_now()
        decision_status = {
            ReviewDecision.APPROVE: ReviewStatus.APPROVED,
            ReviewDecision.EDIT: ReviewStatus.EDITED,
            ReviewDecision.REJECT: ReviewStatus.REJECTED,
        }[decision]
        with self.engine.begin() as connection:
            before = (
                connection.execute(
                    select(filing_review_requests_table).where(
                        filing_review_requests_table.c.review_id == review_id,
                        filing_review_requests_table.c.workspace_id == workspace_id,
                    )
                )
                .mappings()
                .first()
            )
            if not before:
                raise KeyError("review not found")
            if before["status"] != ReviewStatus.PENDING.value:
                raise ValueError("review is no longer pending")
            decision_payload = {
                "decision": decision.value,
                "candidate_decisions": dict(candidate_decisions or {}),
                "object_decisions": dict(object_decisions or {}),
            }
            result = connection.execute(
                update(filing_review_requests_table)
                .where(
                    filing_review_requests_table.c.review_id == review_id,
                    filing_review_requests_table.c.status == ReviewStatus.PENDING.value,
                )
                .values(
                    status=decision_status.value,
                    decision_payload=decision_payload,
                    reviewer_id=reviewer_id,
                    reason=reason,
                    decided_at=now,
                )
            )
            if not result.rowcount:
                raise ValueError("review decision lost an optimistic-concurrency race")
            after = (
                connection.execute(
                    select(filing_review_requests_table).where(
                        filing_review_requests_table.c.review_id == review_id
                    )
                )
                .mappings()
                .one()
            )
            self._insert_audit_event(
                connection,
                workspace_id=workspace_id,
                actor_id=reviewer_id,
                action=f"review.{decision.value}",
                target_type="filing_review",
                target_id=review_id,
                before_payload=dict(before),
                after_payload=dict(after),
                reason=reason,
                trace_id=trace_id,
            )
        return self._review_model(after)

    def approve_run_candidates(
        self,
        *,
        run_id: str,
        approved_by: str,
        decisions: Mapping[str, Mapping[str, Any]] | None = None,
        review_status: ReviewStatus = ReviewStatus.APPROVED,
    ) -> list[str]:
        now = utc_now()
        facts: list[str] = []
        with self.engine.begin() as connection:
            candidates = (
                connection.execute(
                    select(filing_candidate_facts_table).where(
                        filing_candidate_facts_table.c.run_id == run_id,
                        filing_candidate_facts_table.c.status == CandidateStatus.CANDIDATE.value,
                        filing_candidate_facts_table.c.validation_status.in_(
                            [ValidationStatus.PASSED.value, ValidationStatus.REVIEW.value]
                        ),
                    )
                )
                .mappings()
                .all()
            )
            for candidate in candidates:
                row = dict(candidate)
                item_decision = (
                    decisions.get(str(candidate["candidate_id"])) if decisions is not None else None
                )
                if decisions is not None and item_decision is None:
                    raise ValueError(f"missing candidate decision: {candidate['candidate_id']}")
                action = (
                    ReviewItemAction(str(item_decision["action"]))
                    if item_decision is not None
                    else ReviewItemAction.APPROVE
                )
                if action == ReviewItemAction.REJECT:
                    connection.execute(
                        update(filing_candidate_facts_table)
                        .where(filing_candidate_facts_table.c.candidate_id == row["candidate_id"])
                        .values(
                            status=CandidateStatus.REJECTED.value,
                            updated_at=now,
                        )
                    )
                    continue
                allowed_edits = {
                    "canonical_metric",
                    "reported_label",
                    "value_decimal",
                    "currency",
                    "unit_scale",
                    "period_start",
                    "period_end",
                    "period_type",
                    "consolidation_scope",
                }
                item_edits = (
                    dict(item_decision.get("edits") or {})
                    if action == ReviewItemAction.EDIT and item_decision is not None
                    else {}
                )
                for key, value in item_edits.items():
                    if key not in allowed_edits:
                        raise ValueError(f"unsupported candidate edit field: {key}")
                    if key in {"period_start", "period_end"} and isinstance(value, str):
                        value = date.fromisoformat(value)
                    row[key] = value
                Decimal(str(row["value_decimal"]))
                existing_fact_id = connection.execute(
                    select(filing_approved_facts_table.c.fact_id).where(
                        filing_approved_facts_table.c.workspace_id == row["workspace_id"],
                        filing_approved_facts_table.c.company_id == row["company_id"],
                        filing_approved_facts_table.c.canonical_metric == row["canonical_metric"],
                        filing_approved_facts_table.c.period_end == row["period_end"],
                        filing_approved_facts_table.c.period_type == row["period_type"],
                        filing_approved_facts_table.c.consolidation_scope
                        == row["consolidation_scope"],
                        filing_approved_facts_table.c.source_filing_id == row["source_filing_id"],
                        filing_approved_facts_table.c.source_filing_version
                        == row["source_filing_version"],
                    )
                ).scalar_one_or_none()
                fact_id = (
                    str(existing_fact_id)
                    if existing_fact_id is not None
                    else stable_id(
                        "approved-fact",
                        row["workspace_id"],
                        row["company_id"],
                        row["canonical_metric"],
                        row["period_end"],
                        row["period_type"],
                        row["consolidation_scope"],
                        row["source_filing_id"],
                        row["source_filing_version"],
                    )
                )
                connection.execute(
                    update(filing_approved_facts_table)
                    .where(
                        filing_approved_facts_table.c.workspace_id == row["workspace_id"],
                        filing_approved_facts_table.c.company_id == row["company_id"],
                        filing_approved_facts_table.c.canonical_metric == row["canonical_metric"],
                        filing_approved_facts_table.c.period_end == row["period_end"],
                        filing_approved_facts_table.c.period_type == row["period_type"],
                        filing_approved_facts_table.c.consolidation_scope
                        == row["consolidation_scope"],
                        filing_approved_facts_table.c.is_current.is_(True),
                        filing_approved_facts_table.c.source_filing_id != row["source_filing_id"],
                    )
                    .values(is_current=False)
                )
                approved = {
                    "fact_id": fact_id,
                    "candidate_id": row["candidate_id"],
                    "run_id": row["run_id"],
                    "workspace_id": row["workspace_id"],
                    "company_id": row["company_id"],
                    "canonical_metric": row["canonical_metric"],
                    "reported_label": row["reported_label"],
                    "value_decimal": str(row["value_decimal"]),
                    "currency": row["currency"],
                    "unit_scale": str(row["unit_scale"]),
                    "period_start": row["period_start"],
                    "period_end": row["period_end"],
                    "period_type": row["period_type"],
                    "consolidation_scope": row["consolidation_scope"],
                    "source_filing_id": row["source_filing_id"],
                    "source_filing_version": row["source_filing_version"],
                    "evidence_ids": row["evidence_ids"],
                    "confidence": row["confidence"],
                    "validation_status": row["validation_status"],
                    "review_status": (
                        ReviewStatus.EDITED.value
                        if action == ReviewItemAction.EDIT
                        else review_status.value
                    ),
                    "extractor_version": row["extractor_version"],
                    "prompt_version": row["prompt_version"],
                    "approved_at": now,
                    "approved_by": approved_by,
                    "supersedes_fact_id": None,
                    "is_current": True,
                }
                self._execute_upsert(
                    connection,
                    filing_approved_facts_table,
                    approved,
                    index_elements=["fact_id"],
                    update_columns=[
                        "candidate_id",
                        "run_id",
                        "reported_label",
                        "value_decimal",
                        "currency",
                        "unit_scale",
                        "period_start",
                        "evidence_ids",
                        "confidence",
                        "validation_status",
                        "review_status",
                        "extractor_version",
                        "prompt_version",
                        "approved_at",
                        "approved_by",
                        "is_current",
                    ],
                )
                connection.execute(
                    update(filing_candidate_facts_table)
                    .where(filing_candidate_facts_table.c.candidate_id == row["candidate_id"])
                    .values(status=CandidateStatus.APPROVED.value, updated_at=now)
                )
                facts.append(fact_id)
            persisted = (
                connection.execute(
                    select(filing_approved_facts_table.c.fact_id).where(
                        filing_approved_facts_table.c.run_id == run_id
                    )
                )
                .scalars()
                .all()
            )
            facts = list(dict.fromkeys([*facts, *(str(value) for value in persisted)]))
        return facts

    def reject_run_candidates(self, run_id: str) -> int:
        with self.engine.begin() as connection:
            result = connection.execute(
                update(filing_candidate_facts_table)
                .where(
                    filing_candidate_facts_table.c.run_id == run_id,
                    filing_candidate_facts_table.c.status == CandidateStatus.CANDIDATE.value,
                )
                .values(status=CandidateStatus.REJECTED.value, updated_at=utc_now())
            )
        return int(result.rowcount or 0)

    def approved_facts(
        self,
        *,
        workspace_id: str,
        company_id: str,
        metrics: Sequence[str] | None = None,
        current_only: bool = True,
        limit: int = 500,
    ) -> list[FinancialFact]:
        query = select(filing_approved_facts_table).where(
            filing_approved_facts_table.c.workspace_id == workspace_id,
            filing_approved_facts_table.c.company_id == company_id,
        )
        if metrics:
            query = query.where(filing_approved_facts_table.c.canonical_metric.in_(list(metrics)))
        if current_only:
            query = query.where(filing_approved_facts_table.c.is_current.is_(True))
        query = query.order_by(
            filing_approved_facts_table.c.period_end.desc(),
            filing_approved_facts_table.c.canonical_metric,
        ).limit(limit)
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [self._fact_model(row) for row in rows]

    def approved_facts_for_companies(
        self,
        *,
        workspace_id: str,
        company_ids: Sequence[str],
        metrics: Sequence[str],
        current_only: bool = True,
        limit: int = 10_000,
    ) -> list[FinancialFact]:
        if not company_ids or not metrics:
            return []
        query = select(filing_approved_facts_table).where(
            filing_approved_facts_table.c.workspace_id == workspace_id,
            filing_approved_facts_table.c.company_id.in_(list(company_ids)),
            filing_approved_facts_table.c.canonical_metric.in_(list(metrics)),
        )
        if current_only:
            query = query.where(filing_approved_facts_table.c.is_current.is_(True))
        query = query.order_by(
            filing_approved_facts_table.c.company_id,
            filing_approved_facts_table.c.period_end.desc(),
            filing_approved_facts_table.c.canonical_metric,
        ).limit(limit)
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [self._fact_model(row) for row in rows]

    def filing_company_directory(self, *, workspace_id: str) -> dict[str, dict[str, str]]:
        query = (
            select(
                filing_documents_table.c.company_id,
                filing_documents_table.c.symbol,
                filing_documents_table.c.company_name,
            )
            .where(filing_documents_table.c.workspace_id == workspace_id)
            .distinct()
        )
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return {
            str(row["company_id"]): {
                "company_id": str(row["company_id"]),
                "symbol": str(row["symbol"]),
                "name": str(row["company_name"]),
            }
            for row in rows
        }

    def record_universe_snapshot(
        self,
        *,
        workspace_id: str,
        universe_id: str,
        effective_date: date,
        source_url: str,
        source_hash: str,
        members: Sequence[Mapping[str, str]],
    ) -> FilingUniverseSnapshot:
        normalized_members = [
            {
                "company_id": str(item["company_id"]),
                "symbol": str(item["symbol"]).upper(),
                "name": str(item["name"]),
            }
            for item in members
        ]
        snapshot_id = stable_id(
            "filing-universe-snapshot",
            workspace_id,
            universe_id,
            source_hash,
        )
        row = {
            "snapshot_id": snapshot_id,
            "workspace_id": workspace_id,
            "universe_id": universe_id,
            "effective_date": effective_date,
            "source_url": source_url,
            "source_hash": source_hash,
            "members": normalized_members,
            "member_count": len(normalized_members),
            "created_at": utc_now(),
        }
        with self.engine.begin() as connection:
            self._execute_upsert(
                connection,
                filing_universe_snapshots_table,
                row,
                index_elements=["snapshot_id"],
                update_columns=[
                    "effective_date",
                    "source_url",
                    "members",
                    "member_count",
                ],
            )
        return FilingUniverseSnapshot.model_validate(row)

    def latest_universe_snapshot(
        self,
        *,
        workspace_id: str,
        universe_id: str,
    ) -> FilingUniverseSnapshot | None:
        query = (
            select(filing_universe_snapshots_table)
            .where(
                filing_universe_snapshots_table.c.workspace_id == workspace_id,
                filing_universe_snapshots_table.c.universe_id == universe_id,
            )
            .order_by(
                filing_universe_snapshots_table.c.effective_date.desc(),
                filing_universe_snapshots_table.c.created_at.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as connection:
            row = connection.execute(query).mappings().first()
        return FilingUniverseSnapshot.model_validate(dict(row)) if row else None

    def create_investigation(
        self,
        *,
        workspace_id: str,
        universe_id: str,
        question: str,
        request_payload: Mapping[str, Any],
        idempotency_key: str,
    ) -> tuple[InvestigationRun, bool]:
        analysis_id = stable_id(
            "filing-investigation",
            workspace_id,
            idempotency_key,
        )
        with self.engine.begin() as connection:
            existing = (
                connection.execute(
                    select(filing_investigation_runs_table).where(
                        filing_investigation_runs_table.c.analysis_id == analysis_id,
                        filing_investigation_runs_table.c.workspace_id == workspace_id,
                    )
                )
                .mappings()
                .first()
            )
            if existing:
                semantic_keys = ("strict_evidence", "comparison", "max_tool_calls")
                existing_request = dict(existing["request_payload"] or {})
                incoming_request = dict(request_payload)
                semantic_request_changed = any(
                    existing_request.get(key) != incoming_request.get(key)
                    for key in semantic_keys
                )
                if (
                    existing["question"] != question
                    or existing["universe_id"] != universe_id
                    or semantic_request_changed
                ):
                    raise ValueError(
                        "investigation idempotency key is bound to another request"
                    )
                return InvestigationRun.model_validate(dict(existing)), False
            now = utc_now()
            row = {
                "analysis_id": analysis_id,
                "thread_id": stable_id("filing-investigation-thread", workspace_id, analysis_id),
                "workspace_id": workspace_id,
                "universe_id": universe_id,
                "universe_snapshot_id": None,
                "question": question,
                "status": InvestigationStatus.ACCEPTED.value,
                "current_node": "accepted",
                "progress": 0.0,
                "request_payload": self._json_safe(dict(request_payload)),
                "plan_payload": {},
                "result_payload": {},
                "error_code": None,
                "error_message": None,
                "trace_id": None,
                "created_at": now,
                "updated_at": now,
                "finished_at": None,
            }
            connection.execute(filing_investigation_runs_table.insert(), row)
            self._append_investigation_event(
                connection,
                analysis_id=analysis_id,
                workspace_id=workspace_id,
                node="accepted",
                status="completed",
                detail={"universe_id": universe_id},
            )
        return InvestigationRun.model_validate(row), True

    def investigation(
        self,
        analysis_id: str,
        workspace_id: str | None = None,
    ) -> InvestigationRun | None:
        query = select(filing_investigation_runs_table).where(
            filing_investigation_runs_table.c.analysis_id == analysis_id
        )
        if workspace_id:
            query = query.where(filing_investigation_runs_table.c.workspace_id == workspace_id)
        with self.engine.connect() as connection:
            row = connection.execute(query).mappings().first()
        return InvestigationRun.model_validate(dict(row)) if row else None

    def transition_investigation(
        self,
        analysis_id: str,
        *,
        status: InvestigationStatus | None = None,
        current_node: str,
        progress: float,
        detail: Mapping[str, Any] | None = None,
        universe_snapshot_id: str | None = None,
        plan_payload: Mapping[str, Any] | None = None,
        result_payload: Mapping[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        trace_id: str | None = None,
    ) -> None:
        values: dict[str, Any] = {
            "current_node": current_node,
            "progress": min(max(progress, 0.0), 1.0),
            "updated_at": utc_now(),
        }
        if status is not None:
            values["status"] = status.value
            if status in {
                InvestigationStatus.COMPLETED,
                InvestigationStatus.PARTIAL,
                InvestigationStatus.ABSTAINED,
                InvestigationStatus.FAILED,
            }:
                values["finished_at"] = utc_now()
        if universe_snapshot_id is not None:
            values["universe_snapshot_id"] = universe_snapshot_id
        if plan_payload is not None:
            values["plan_payload"] = self._json_safe(dict(plan_payload))
        if result_payload is not None:
            values["result_payload"] = self._json_safe(dict(result_payload))
        if error_code is not None:
            values["error_code"] = error_code
        if error_message is not None:
            values["error_message"] = error_message[:2_000]
        if trace_id is not None:
            values["trace_id"] = trace_id
        with self.engine.begin() as connection:
            run = (
                connection.execute(
                    select(
                        filing_investigation_runs_table.c.workspace_id,
                    ).where(filing_investigation_runs_table.c.analysis_id == analysis_id)
                )
                .mappings()
                .first()
            )
            if not run:
                raise KeyError(f"filing investigation not found: {analysis_id}")
            connection.execute(
                update(filing_investigation_runs_table)
                .where(filing_investigation_runs_table.c.analysis_id == analysis_id)
                .values(**values)
            )
            self._append_investigation_event(
                connection,
                analysis_id=analysis_id,
                workspace_id=str(run["workspace_id"]),
                node=current_node,
                status=(status.value if status is not None else "completed"),
                detail=detail or {},
            )

    def investigation_events(
        self,
        *,
        analysis_id: str,
        workspace_id: str,
    ) -> list[InvestigationEvent]:
        query = (
            select(filing_investigation_events_table)
            .where(
                filing_investigation_events_table.c.analysis_id == analysis_id,
                filing_investigation_events_table.c.workspace_id == workspace_id,
            )
            .order_by(filing_investigation_events_table.c.sequence)
        )
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [InvestigationEvent.model_validate(dict(row)) for row in rows]

    def _append_investigation_event(
        self,
        connection: Connection,
        *,
        analysis_id: str,
        workspace_id: str,
        node: str,
        status: str,
        detail: Mapping[str, Any],
    ) -> None:
        latest = connection.execute(
            select(func.max(filing_investigation_events_table.c.sequence)).where(
                filing_investigation_events_table.c.analysis_id == analysis_id
            )
        ).scalar_one_or_none()
        connection.execute(
            filing_investigation_events_table.insert(),
            {
                "event_id": str(uuid4()),
                "analysis_id": analysis_id,
                "workspace_id": workspace_id,
                "sequence": int(latest or 0) + 1,
                "node": node,
                "status": status,
                "detail": self._json_safe(dict(detail)),
                "created_at": utc_now(),
            },
        )

    def fact(self, fact_id: str, workspace_id: str) -> FinancialFact | None:
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(filing_approved_facts_table).where(
                        filing_approved_facts_table.c.fact_id == fact_id,
                        filing_approved_facts_table.c.workspace_id == workspace_id,
                    )
                )
                .mappings()
                .first()
            )
        return self._fact_model(row) if row else None

    def record_analysis(
        self,
        *,
        analysis_id: str,
        workspace_id: str,
        company_id: str,
        question: str,
        status: str,
        answer: str,
        citations: Sequence[Mapping[str, Any]],
        tool_calls: Sequence[Mapping[str, Any]],
        warnings: Sequence[str],
        trace_id: str,
    ) -> None:
        row = {
            "analysis_id": analysis_id,
            "workspace_id": workspace_id,
            "company_id": company_id,
            "question": question,
            "status": status,
            "answer": answer,
            "citations": list(citations),
            "tool_calls": list(tool_calls),
            "warnings": list(warnings),
            "trace_id": trace_id,
            "created_at": utc_now(),
        }
        with self.engine.begin() as connection:
            connection.execute(filing_analysis_runs_table.insert(), row)

    def upsert_index_run(
        self,
        *,
        run_id: str,
        workspace_id: str,
        company_id: str,
        filing_id: str,
        filing_version: int,
        index_version: str,
        embedding_model: str,
        collection_name: str,
        status: str,
        chunk_count: int,
        error_message: str | None = None,
    ) -> str:
        now = utc_now()
        index_run_id = stable_id(
            "filing-index-run",
            workspace_id,
            filing_id,
            filing_version,
            index_version,
        )
        row = {
            "index_run_id": index_run_id,
            "run_id": run_id,
            "workspace_id": workspace_id,
            "company_id": company_id,
            "filing_id": filing_id,
            "filing_version": filing_version,
            "index_version": index_version,
            "embedding_model": embedding_model,
            "collection_name": collection_name,
            "status": status,
            "chunk_count": chunk_count,
            "error_message": error_message,
            "created_at": now,
            "updated_at": now,
        }
        with self.engine.begin() as connection:
            self._execute_upsert(
                connection,
                filing_index_runs_table,
                row,
                index_elements=["index_run_id"],
                update_columns=[
                    "run_id",
                    "status",
                    "chunk_count",
                    "error_message",
                    "updated_at",
                ],
            )
        return index_run_id

    def record_audit_event(
        self,
        *,
        workspace_id: str,
        actor_id: str,
        action: str,
        target_type: str,
        target_id: str,
        before_payload: Mapping[str, Any] | None = None,
        after_payload: Mapping[str, Any] | None = None,
        reason: str | None = None,
        trace_id: str | None = None,
    ) -> str:
        with self.engine.begin() as connection:
            return self._insert_audit_event(
                connection,
                workspace_id=workspace_id,
                actor_id=actor_id,
                action=action,
                target_type=target_type,
                target_id=target_id,
                before_payload=dict(before_payload or {}),
                after_payload=dict(after_payload or {}),
                reason=reason,
                trace_id=trace_id,
            )

    def audit_events(
        self,
        *,
        workspace_id: str,
        action: str | None = None,
        target_type: str | None = None,
        target_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        query = select(filing_audit_events_table).where(
            filing_audit_events_table.c.workspace_id == workspace_id
        )
        if action is not None:
            query = query.where(filing_audit_events_table.c.action == action)
        if target_type is not None:
            query = query.where(filing_audit_events_table.c.target_type == target_type)
        if target_id is not None:
            query = query.where(filing_audit_events_table.c.target_id == target_id)
        query = query.order_by(filing_audit_events_table.c.created_at.desc()).limit(limit)
        with self.engine.connect() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def _insert_audit_event(
        self,
        connection: Connection,
        *,
        workspace_id: str,
        actor_id: str,
        action: str,
        target_type: str,
        target_id: str,
        before_payload: Mapping[str, Any],
        after_payload: Mapping[str, Any],
        reason: str | None,
        trace_id: str | None,
    ) -> str:
        event_id = str(uuid4())
        connection.execute(
            filing_audit_events_table.insert(),
            {
                "event_id": event_id,
                "workspace_id": workspace_id,
                "actor_id": actor_id,
                "action": action,
                "target_type": target_type,
                "target_id": target_id,
                "before_payload": self._json_safe(dict(before_payload)),
                "after_payload": self._json_safe(dict(after_payload)),
                "reason": reason,
                "trace_id": trace_id,
                "created_at": utc_now(),
            },
        )
        return event_id

    def _execute_upsert(
        self,
        connection: Connection,
        table,
        row: Mapping[str, Any],
        *,
        index_elements: Sequence[str],
        update_columns: Sequence[str],
    ) -> None:
        dialect = connection.dialect.name
        if dialect == "postgresql":
            statement = postgresql_insert(table).values(**dict(row))
            statement = statement.on_conflict_do_update(
                index_elements=list(index_elements),
                set_={name: statement.excluded[name] for name in update_columns},
            )
            connection.execute(statement)
            return
        if dialect == "sqlite":
            statement = sqlite_insert(table).values(**dict(row))
            statement = statement.on_conflict_do_update(
                index_elements=list(index_elements),
                set_={name: statement.excluded[name] for name in update_columns},
            )
            connection.execute(statement)
            return
        existing = connection.execute(
            select(table).where(*[table.c[name] == row[name] for name in index_elements])
        ).first()
        if existing:
            connection.execute(
                update(table)
                .where(*[table.c[name] == row[name] for name in index_elements])
                .values(**{name: row[name] for name in update_columns})
            )
        else:
            connection.execute(table.insert(), dict(row))

    @staticmethod
    def _document_model(row: Mapping[str, Any]) -> FilingDocument:
        payload = dict(row)
        return FilingDocument.model_validate(payload)

    @staticmethod
    def _run_model(row: Mapping[str, Any]) -> FilingRun:
        return FilingRun.model_validate(dict(row))

    @staticmethod
    def _review_model(row: Mapping[str, Any]) -> ReviewRequest:
        return ReviewRequest.model_validate(dict(row))

    @staticmethod
    def _fact_model(row: Mapping[str, Any]) -> FinancialFact:
        payload = dict(row)
        payload["value"] = Decimal(str(payload.pop("value_decimal")))
        payload["unit_scale"] = Decimal(str(payload["unit_scale"]))
        payload.pop("candidate_id", None)
        payload.pop("supersedes_fact_id", None)
        return FinancialFact.model_validate(payload)

    @classmethod
    def _json_safe(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, dict):
            return {str(key): cls._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._json_safe(item) for item in value]
        return value
