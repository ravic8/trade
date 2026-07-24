from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Annotated, Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, Send, interrupt
from sqlalchemy import text
from sqlalchemy.engine import make_url

from trade_research.config import Settings
from trade_research.filings.extractors import (
    extract_pdf_intelligence,
    extract_xbrl_financial_candidates,
    planned_sections,
)
from trade_research.filings.indexing import FilingEvidenceIndexer
from trade_research.filings.models import (
    FilingDocumentStatus,
    FilingRunStatus,
    ReviewStatus,
    ValidationDefect,
    ValidationSeverity,
)
from trade_research.filings.parsers import FilingArtifactStore, FilingParser
from trade_research.filings.review import materialize_candidate_decisions
from trade_research.filings.store import FilingStore, stable_id
from trade_research.filings.telemetry import (
    current_trace_id,
    filing_metrics,
    operation_span,
)
from trade_research.filings.validators import FilingFactValidator


def merge_unique(left: list[str] | None, right: list[str] | None) -> list[str]:
    return list(dict.fromkeys([*(left or []), *(right or [])]))


class FilingGraphState(TypedDict, total=False):
    run_id: str
    thread_id: str
    workspace_id: str
    company_id: str
    filing_id: str
    filing_version: int
    source_hash: str
    content_type: str
    force_review: bool
    artifact_uri: str
    parse_quality: float
    parse_warnings: list[str]
    sections: list[str]
    section_job: str
    candidate_ids: Annotated[list[str], merge_unique]
    intelligence_object_ids: Annotated[list[str], merge_unique]
    evidence_ids: Annotated[list[str], merge_unique]
    validation_summary: dict[str, Any]
    blocking: bool
    requires_review: bool
    review_id: str
    approved_fact_ids: Annotated[list[str], merge_unique]
    index_summary: dict[str, Any]
    final_status: str


class FilingCancelled(RuntimeError):
    pass


class FilingWorkflowError(RuntimeError):
    pass


@dataclass(frozen=True)
class WorkflowServices:
    settings: Settings
    store: FilingStore
    parser: FilingParser
    artifact_store: FilingArtifactStore
    validator: FilingFactValidator
    indexer: FilingEvidenceIndexer | None
    worker_id: str


class FilingWorkflow:
    def __init__(
        self,
        services: WorkflowServices,
        *,
        checkpointer: Any,
    ) -> None:
        self.services = services
        self.checkpointer = checkpointer
        self.graph = self._build_graph().compile(
            checkpointer=checkpointer,
            name="filing.document.intelligence",
        )

    def invoke(
        self,
        run_id: str,
        *,
        resume_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        run = self.services.store.run(run_id)
        if not run:
            raise KeyError(f"filing run not found: {run_id}")
        config = {
            "configurable": {"thread_id": run.thread_id},
            "metadata": {
                "run_id": run.run_id,
                "workspace_id": run.workspace_id,
                "company_id": run.company_id,
                "filing_id": run.filing_id,
                "workflow": "filing.document.intelligence",
            },
        }
        if resume_payload is not None:
            filing_metrics().resume_count.add(
                1, {"graph": "filing.document.intelligence"}
            )
            result = self.graph.invoke(Command(resume=resume_payload), config=config)
        else:
            document = self.services.store.document(run.filing_id, run.workspace_id)
            if not document:
                raise KeyError(f"filing document not found: {run.filing_id}")
            result = self.graph.invoke(
                {
                    "run_id": run.run_id,
                    "thread_id": run.thread_id,
                    "workspace_id": run.workspace_id,
                    "company_id": run.company_id,
                    "filing_id": run.filing_id,
                    "filing_version": document.version,
                    "source_hash": document.sha256,
                    "content_type": document.content_type,
                    "force_review": bool(run.input_payload.get("force_review")),
                    "candidate_ids": [],
                    "intelligence_object_ids": [],
                    "evidence_ids": [],
                    "approved_fact_ids": [],
                },
                config=config,
            )
        return dict(result)

    def _build_graph(self) -> StateGraph:
        builder = StateGraph(FilingGraphState)
        builder.add_node("authorize", self._authorize)
        builder.add_node("parse", self._parse)
        builder.add_node("plan_sections", self._plan_sections)
        builder.add_node("extract_section", self._extract_section)
        builder.add_node("index_evidence", self._index_evidence)
        builder.add_node("validate", self._validate)
        builder.add_node("persist", self._persist)
        builder.add_node("human_review", self._human_review)
        builder.add_node("block", self._block)

        builder.add_edge(START, "authorize")
        builder.add_edge("authorize", "parse")
        builder.add_edge("parse", "plan_sections")
        builder.add_conditional_edges("plan_sections", self._dispatch_sections)
        builder.add_edge("extract_section", "index_evidence")
        builder.add_edge("index_evidence", "validate")
        builder.add_conditional_edges(
            "validate",
            self._route_validation,
            {
                "persist": "persist",
                "human_review": "human_review",
                "block": "block",
            },
        )
        builder.add_edge("persist", END)
        builder.add_edge("human_review", END)
        builder.add_edge("block", END)
        return builder

    def _authorize(self, state: FilingGraphState) -> dict[str, Any]:
        with self._node(state, "authorize", 0.08):
            run = self.services.store.run(state["run_id"], state["workspace_id"])
            document = self.services.store.document(
                state["filing_id"], state["workspace_id"]
            )
            if not run or not document:
                raise FilingWorkflowError("run or filing is outside the authorized workspace")
            if run.company_id != document.company_id:
                raise FilingWorkflowError("run company does not match filing company")
            if document.sha256 != state["source_hash"]:
                raise FilingWorkflowError("filing hash changed after run registration")
            self.services.store.update_document_parse(
                document.filing_id,
                status=FilingDocumentStatus.PROCESSING,
                parse_quality=document.parse_quality,
            )
            return {}

    def _parse(self, state: FilingGraphState) -> dict[str, Any]:
        with self._node(state, "parse", 0.22):
            document = self._document(state)
            if self.services.artifact_store.has_parsed_document(document.filing_id):
                artifact_uri = self.services.artifact_store.parsed_document_uri(
                    document.filing_id
                )
                parsed = self.services.artifact_store.read_parsed_document(artifact_uri)
            else:
                parsed = self.services.parser.parse(document)
                artifact_uri = parsed.artifact_uri
            self.services.store.update_document_parse(
                document.filing_id,
                status=FilingDocumentStatus.PROCESSING,
                parse_quality=parsed.parse_quality,
            )
            return {
                "artifact_uri": artifact_uri,
                "parse_quality": parsed.parse_quality,
                "parse_warnings": parsed.warnings,
            }

    def _plan_sections(self, state: FilingGraphState) -> dict[str, Any]:
        with self._node(state, "plan_sections", 0.30):
            parsed = self.services.artifact_store.read_parsed_document(
                state["artifact_uri"]
            )
            return {"sections": planned_sections(parsed)}

    @staticmethod
    def _dispatch_sections(state: FilingGraphState):
        return [
            Send(
                "extract_section",
                {
                    "run_id": state["run_id"],
                    "thread_id": state["thread_id"],
                    "workspace_id": state["workspace_id"],
                    "company_id": state["company_id"],
                    "filing_id": state["filing_id"],
                    "filing_version": state["filing_version"],
                    "source_hash": state["source_hash"],
                    "content_type": state["content_type"],
                    "artifact_uri": state["artifact_uri"],
                    "parse_quality": state["parse_quality"],
                    "parse_warnings": state.get("parse_warnings", []),
                    "force_review": state.get("force_review", False),
                    "section_job": section,
                    "candidate_ids": [],
                    "intelligence_object_ids": [],
                    "evidence_ids": [],
                    "approved_fact_ids": [],
                },
            )
            for section in state["sections"]
        ]

    def _extract_section(self, state: FilingGraphState) -> dict[str, Any]:
        section = state["section_job"]
        with self._node(state, f"extract.{section}", 0.58):
            document = self._document(state)
            parsed = self.services.artifact_store.read_parsed_document(
                state["artifact_uri"]
            )
            if parsed.xbrl_facts:
                evidence, candidates = extract_xbrl_financial_candidates(
                    parsed=parsed,
                    run_id=state["run_id"],
                    workspace_id=state["workspace_id"],
                    company_id=state["company_id"],
                    filing_id=state["filing_id"],
                    filing_version=state["filing_version"],
                    source_hash=state["source_hash"],
                    default_scope=document.consolidation_scope,
                    section=section,
                    extractor_version=self.services.settings.filing_extractor_version,
                )
                evidence_ids = self.services.store.upsert_evidence(evidence)
                candidate_ids = self.services.store.upsert_candidate_facts(candidates)
                filing_metrics().extraction_candidates.add(
                    len(candidate_ids),
                    {"object_type": "financial_fact", "section": section},
                )
                return {
                    "evidence_ids": evidence_ids,
                    "candidate_ids": candidate_ids,
                }
            evidence, objects = extract_pdf_intelligence(
                parsed=parsed,
                run_id=state["run_id"],
                workspace_id=state["workspace_id"],
                company_id=state["company_id"],
                filing_id=state["filing_id"],
                filing_version=state["filing_version"],
                source_hash=state["source_hash"],
                period_end=document.period_end,
                section=section,
                claim_limit=self.services.settings.filing_pdf_claim_limit,
                extractor_version=self.services.settings.filing_extractor_version,
            )
            evidence_ids = self.services.store.upsert_evidence(evidence)
            object_ids = self.services.store.upsert_intelligence_objects(objects)
            filing_metrics().extraction_candidates.add(
                len(object_ids),
                {"object_type": "intelligence_object", "section": section},
            )
            return {
                "evidence_ids": evidence_ids,
                "intelligence_object_ids": object_ids,
            }

    def _index_evidence(self, state: FilingGraphState) -> dict[str, Any]:
        with self._node(state, "index_evidence", 0.68):
            if self.services.indexer is None:
                return {
                    "index_summary": {
                        "status": "disabled",
                        "index_version": self.services.settings.filing_index_version,
                    }
                }
            document = self._document(state)
            parsed = self.services.artifact_store.read_parsed_document(
                state["artifact_uri"]
            )
            chunk_count = self.services.indexer.index(
                run_id=state["run_id"],
                document=document,
                parsed=parsed,
            )
            return {
                "index_summary": {
                    "status": "completed",
                    "index_version": self.services.settings.filing_index_version,
                    "embedding_model": self.services.settings.openai_embedding_model,
                    "chunk_count": chunk_count,
                }
            }

    def _validate(self, state: FilingGraphState) -> dict[str, Any]:
        with self._node(state, "validate", 0.76):
            candidates = self.services.store.candidate_facts(state["run_id"])
            objects = self.services.store.intelligence_objects(run_id=state["run_id"])
            result = self.services.validator.validate(state["run_id"], candidates)
            defects = list(result.defects)
            blocking = result.blocking
            requires_review = result.requires_review

            if state.get("parse_quality", 0) < self.services.settings.filing_parse_min_quality:
                defects.append(
                    ValidationDefect(
                        defect_id=stable_id(
                            "filing-validation-defect",
                            state["run_id"],
                            "parse.quality_low",
                        ),
                        run_id=state["run_id"],
                        rule_code="parse.quality_low",
                        severity=ValidationSeverity.WARNING,
                        message="document parse quality is below the automatic threshold",
                        context={
                            "parse_quality": state.get("parse_quality", 0),
                            "threshold": self.services.settings.filing_parse_min_quality,
                            "warnings": state.get("parse_warnings", []),
                        },
                    )
                )
                requires_review = True
            if not candidates and not objects:
                defects.append(
                    ValidationDefect(
                        defect_id=stable_id(
                            "filing-validation-defect",
                            state["run_id"],
                            "extraction.no_candidates",
                        ),
                        run_id=state["run_id"],
                        rule_code="extraction.no_candidates",
                        severity=ValidationSeverity.BLOCKING,
                        message="supported extraction produced no candidate objects",
                    )
                )
                blocking = True
            if objects:
                requires_review = True
            if state.get("force_review") or self.services.settings.filing_force_human_review:
                requires_review = True
            if (
                candidates
                and not self.services.settings.filing_auto_approve_xbrl
            ):
                requires_review = True

            self.services.store.replace_validation_defects(
                state["run_id"], defects, result.candidate_statuses
            )
            for defect in defects:
                filing_metrics().validation_defects.add(
                    1,
                    {
                        "rule": defect.rule_code,
                        "severity": defect.severity.value,
                    },
                )
            return {
                "blocking": blocking,
                "requires_review": requires_review,
                "validation_summary": {
                    "candidate_count": len(candidates),
                    "intelligence_object_count": len(objects),
                    "defect_count": len(defects),
                    "blocking": blocking,
                    "requires_review": requires_review,
                },
            }

    @staticmethod
    def _route_validation(state: FilingGraphState) -> str:
        if state.get("blocking"):
            return "block"
        if state.get("requires_review"):
            return "human_review"
        return "persist"

    def _persist(self, state: FilingGraphState) -> dict[str, Any]:
        with self._node(state, "persist", 0.94):
            fact_ids = self.services.store.approve_run_candidates(
                run_id=state["run_id"],
                approved_by="system:xbrl-validator",
                review_status=ReviewStatus.APPROVED,
            )
            self.services.store.set_intelligence_object_review_status(
                state["run_id"], ReviewStatus.APPROVED
            )
            self.services.store.update_document_parse(
                state["filing_id"],
                status=FilingDocumentStatus.PROCESSED,
                parse_quality=state.get("parse_quality"),
            )
            output = {
                "approved_fact_ids": fact_ids,
                "validation": state.get("validation_summary", {}),
                "index": state.get("index_summary", {}),
                "review": "automatic",
            }
            self.services.store.transition_run(
                state["run_id"],
                status=FilingRunStatus.COMPLETED,
                current_node="completed",
                progress=1.0,
                output_payload=output,
                trace_id=current_trace_id(),
            )
            self.services.store.record_audit_event(
                workspace_id=state["workspace_id"],
                actor_id="system:xbrl-validator",
                action="facts.auto_approved",
                target_type="filing_run",
                target_id=state["run_id"],
                after_payload=output,
                reason="all deterministic validation gates passed",
                trace_id=current_trace_id(),
            )
            filing_metrics().workflow_runs.add(
                1, {"graph": "filing.document.intelligence", "status": "completed"}
            )
            return {
                "approved_fact_ids": fact_ids,
                "final_status": FilingRunStatus.COMPLETED.value,
            }

    def _human_review(self, state: FilingGraphState) -> dict[str, Any]:
        with self._node(state, "human_review", 0.86):
            candidates = self.services.store.candidate_facts(state["run_id"])
            objects = self.services.store.intelligence_objects(run_id=state["run_id"])
            defects = self.services.store.validation_defects(state["run_id"])
            evidence_ids = list(
                dict.fromkeys(
                    evidence_id
                    for item in [*candidates, *objects]
                    for evidence_id in item["evidence_ids"]
                )
            )
            evidence = self.services.store.evidence(
                workspace_id=state["workspace_id"],
                evidence_ids=evidence_ids,
            )
            review = self.services.store.create_review_request(
                run_id=state["run_id"],
                workspace_id=state["workspace_id"],
                payload={
                    "filing_id": state["filing_id"],
                    "candidate_facts": [
                        {
                            "candidate_id": item["candidate_id"],
                            "metric": item["canonical_metric"],
                            "reported_label": item["reported_label"],
                            "value": item["value_decimal"],
                            "currency": item["currency"],
                            "unit_scale": item["unit_scale"],
                            "period_start": str(item["period_start"])
                            if item["period_start"]
                            else None,
                            "period_end": str(item["period_end"]),
                            "period_type": item["period_type"],
                            "scope": item["consolidation_scope"],
                            "evidence_ids": item["evidence_ids"],
                            "confidence": item["confidence"],
                            "validation_status": item["validation_status"],
                        }
                        for item in candidates
                    ],
                    "intelligence_objects": [
                        {
                            "object_id": item["object_id"],
                            "object_type": item["object_type"],
                            "canonical_name": item["canonical_name"],
                            "value_decimal": item["value_decimal"],
                            "value_text": item["value_text"],
                            "evidence_ids": item["evidence_ids"],
                        }
                        for item in objects
                    ],
                    "evidence": [
                        item.model_dump(mode="json") for item in evidence
                    ],
                    "defects": [item.model_dump(mode="json") for item in defects],
                },
            )
            self.services.store.transition_run(
                state["run_id"],
                status=FilingRunStatus.WAITING_REVIEW,
                current_node="human_review",
                progress=0.86,
                output_payload={"review_id": review.review_id},
                trace_id=current_trace_id(),
            )
            interrupt(
                {
                    "review_id": review.review_id,
                    "run_id": state["run_id"],
                    "candidate_count": len(candidates),
                    "intelligence_object_count": len(objects),
                    "defect_count": len(defects),
                }
            )

            decided = self.services.store.review(
                review.review_id, state["workspace_id"]
            )
            if not decided or decided.status == ReviewStatus.PENDING:
                raise FilingWorkflowError("workflow resumed without a persisted review decision")
            if decided.status == ReviewStatus.REJECTED:
                self.services.store.reject_run_candidates(state["run_id"])
                self.services.store.set_intelligence_object_review_status(
                    state["run_id"], ReviewStatus.REJECTED
                )
                fact_ids: list[str] = []
            else:
                candidate_decisions = (
                    decided.decision_payload.get("candidate_decisions") or {}
                )
                object_decisions = (
                    decided.decision_payload.get("object_decisions") or {}
                )
                if decided.status == ReviewStatus.EDITED:
                    reviewed_candidates = materialize_candidate_decisions(
                        candidates,
                        candidate_decisions,
                    )
                    validation = self.services.validator.validate(
                        state["run_id"],
                        reviewed_candidates,
                    )
                    if validation.blocking:
                        codes = sorted(
                            {
                                item.rule_code
                                for item in validation.defects
                                if item.severity
                                in {
                                    ValidationSeverity.BLOCKING,
                                    ValidationSeverity.ERROR,
                                }
                            }
                        )
                        raise FilingWorkflowError(
                            "reviewed candidates failed validation: "
                            + ", ".join(codes)
                        )
                    self.services.store.replace_validation_defects(
                        state["run_id"],
                        validation.defects,
                        validation.candidate_statuses,
                    )
                fact_ids = self.services.store.approve_run_candidates(
                    run_id=state["run_id"],
                    approved_by=decided.reviewer_id or "unknown-reviewer",
                    decisions=(
                        candidate_decisions
                        if decided.status == ReviewStatus.EDITED
                        else None
                    ),
                    review_status=decided.status,
                )
                if decided.status == ReviewStatus.EDITED:
                    self.services.store.apply_intelligence_object_review_decisions(
                        run_id=state["run_id"],
                        decisions=object_decisions,
                    )
                else:
                    self.services.store.set_intelligence_object_review_status(
                        state["run_id"], decided.status
                    )
            self.services.store.update_document_parse(
                state["filing_id"],
                status=FilingDocumentStatus.PROCESSED,
                parse_quality=state.get("parse_quality"),
            )
            output = {
                "review_id": review.review_id,
                "review_status": decided.status.value,
                "approved_fact_ids": fact_ids,
                "validation": state.get("validation_summary", {}),
                "index": state.get("index_summary", {}),
            }
            self.services.store.transition_run(
                state["run_id"],
                status=FilingRunStatus.COMPLETED,
                current_node="completed",
                progress=1.0,
                output_payload=output,
                trace_id=current_trace_id(),
            )
            filing_metrics().workflow_runs.add(
                1, {"graph": "filing.document.intelligence", "status": "completed"}
            )
            return {
                "review_id": review.review_id,
                "approved_fact_ids": fact_ids,
                "final_status": FilingRunStatus.COMPLETED.value,
            }

    def _block(self, state: FilingGraphState) -> dict[str, Any]:
        with self._node(state, "block", 1.0):
            output = {
                "validation": state.get("validation_summary", {}),
                "defects": [
                    item.model_dump(mode="json")
                    for item in self.services.store.validation_defects(state["run_id"])
                ],
            }
            self.services.store.update_document_parse(
                state["filing_id"],
                status=FilingDocumentStatus.FAILED,
                parse_quality=state.get("parse_quality"),
            )
            self.services.store.transition_run(
                state["run_id"],
                status=FilingRunStatus.FAILED,
                current_node="blocked",
                progress=1.0,
                output_payload=output,
                error_code="validation_blocked",
                error_message="filing failed deterministic validation",
                trace_id=current_trace_id(),
            )
            filing_metrics().workflow_runs.add(
                1, {"graph": "filing.document.intelligence", "status": "failed"}
            )
            return {"final_status": FilingRunStatus.FAILED.value}

    @contextmanager
    def _node(
        self,
        state: FilingGraphState,
        node_name: str,
        progress: float,
    ) -> Iterator[None]:
        if self.services.store.is_cancel_requested(state["run_id"]):
            self.services.store.transition_run(
                state["run_id"],
                status=FilingRunStatus.CANCELLED,
                current_node="cancelled",
                progress=progress,
                error_code="cancelled",
                error_message="run cancelled by user",
                trace_id=current_trace_id(),
            )
            raise FilingCancelled(state["run_id"])
        self.services.store.heartbeat_run(
            state["run_id"],
            worker_id=self.services.worker_id,
            lease_seconds=self.services.settings.filing_worker_lease_seconds,
            current_node=node_name,
            progress=progress,
        )
        started = time.monotonic()
        with operation_span(
            self.services.settings,
            f"filing.{node_name}",
            metadata={
                "run_id": state["run_id"],
                "workspace_id": state["workspace_id"],
                "company_id": state["company_id"],
                "filing_id": state["filing_id"],
                "node": node_name,
                "graph_version": "filing-document-v1",
                "extractor_version": self.services.settings.filing_extractor_version,
            },
        ):
            try:
                yield
            finally:
                filing_metrics().node_duration.record(
                    max(time.monotonic() - started, 0),
                    {
                        "graph": "filing.document.intelligence",
                        "node": node_name,
                    },
                )

    def _document(self, state: FilingGraphState):
        document = self.services.store.document(
            state["filing_id"], state["workspace_id"]
        )
        if not document:
            raise FilingWorkflowError("filing document disappeared during workflow")
        return document


@contextmanager
def workflow_checkpointer(
    settings: Settings,
    store: FilingStore,
    *,
    memory_saver: InMemorySaver | None = None,
):
    checkpoint_url = settings.filing_checkpoint_database_url or settings.database_url
    if checkpoint_url.startswith("sqlite"):
        yield memory_saver or InMemorySaver()
        return
    schema = settings.filing_checkpoint_schema
    with store.engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    dsn = _checkpoint_dsn(checkpoint_url, schema)
    with PostgresSaver.from_conn_string(dsn) as saver:
        saver.setup()
        yield saver


def _checkpoint_dsn(database_url: str, schema: str) -> str:
    url = make_url(database_url)
    driver = url.drivername.split("+", maxsplit=1)[0]
    query = dict(url.query)
    query["options"] = f"-csearch_path={schema},public"
    return url.set(drivername=driver, query=query).render_as_string(
        hide_password=False
    )
