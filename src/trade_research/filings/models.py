from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class FilingRunStatus(StrEnum):
    ACCEPTED = "accepted"
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    WAITING_REVIEW = "waiting_review"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_RUN_STATUSES = {
    FilingRunStatus.COMPLETED,
    FilingRunStatus.FAILED,
    FilingRunStatus.CANCELLED,
}


class FilingDocumentStatus(StrEnum):
    REGISTERED = "registered"
    PROCESSING = "processing"
    PROCESSED = "processed"
    SUPERSEDED = "superseded"
    FAILED = "failed"


class ReviewDecision(StrEnum):
    APPROVE = "approve"
    EDIT = "edit"
    REJECT = "reject"


class ReviewItemAction(StrEnum):
    APPROVE = "approve"
    EDIT = "edit"
    REJECT = "reject"


class ReviewStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    EDITED = "edited"
    REJECTED = "rejected"


class CandidateStatus(StrEnum):
    CANDIDATE = "candidate"
    APPROVED = "approved"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


class ValidationSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    BLOCKING = "blocking"


class ValidationStatus(StrEnum):
    PENDING = "pending"
    PASSED = "passed"
    REVIEW = "review"
    FAILED = "failed"


class ConsolidationScope(StrEnum):
    CONSOLIDATED = "consolidated"
    STANDALONE = "standalone"
    UNKNOWN = "unknown"


class PeriodType(StrEnum):
    INSTANT = "instant"
    QUARTER = "quarter"
    YEAR_TO_DATE = "year_to_date"
    ANNUAL = "annual"
    DURATION = "duration"
    UNKNOWN = "unknown"


class IntelligenceObjectType(StrEnum):
    FINANCIAL_FACT = "financial_fact"
    OPERATIONAL_METRIC = "operational_metric"
    GUIDANCE = "guidance"
    ADJUSTMENT = "adjustment"
    CORPORATE_EVENT = "corporate_event"
    GOVERNANCE_RESOLUTION = "governance_resolution"
    RISK_DISCLOSURE = "risk_disclosure"
    MANAGEMENT_CLAIM = "management_claim"


class FilingDocument(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    filing_id: str
    workspace_id: str
    company_id: str
    symbol: str
    exchange: str = "NSE"
    company_name: str
    categories: list[str]
    title: str | None = None
    source_url: str
    source_apis: list[str] = Field(default_factory=list)
    filing_date: datetime | None = None
    period_end: date | None = None
    consolidation_scope: ConsolidationScope = ConsolidationScope.UNKNOWN
    audited: bool | None = None
    submission_type: str | None = None
    relative_path: str
    object_uri: str
    filename: str
    byte_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_type: str
    document_key: str
    version: int = Field(default=1, ge=1)
    supersedes_filing_id: str | None = None
    is_current: bool = True
    status: FilingDocumentStatus = FilingDocumentStatus.REGISTERED
    parse_quality: float | None = Field(default=None, ge=0, le=1)
    source_metadata: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class EvidenceReference(BaseModel):
    evidence_id: str
    workspace_id: str
    company_id: str
    filing_id: str
    filing_version: int = Field(ge=1)
    page: int | None = Field(default=None, ge=1)
    section_path: str | None = None
    table_name: str | None = None
    row_label: str | None = None
    column_label: str | None = None
    xbrl_concept: str | None = None
    context_ref: str | None = None
    chunk_id: str | None = None
    source_hash: str
    snippet: str | None = None
    effective_date: date | None = None


class FinancialFact(BaseModel):
    fact_id: str
    workspace_id: str
    company_id: str
    run_id: str
    canonical_metric: str
    reported_label: str
    value: Decimal
    currency: str | None = None
    unit_scale: Decimal = Decimal("1")
    period_start: date | None = None
    period_end: date
    period_type: PeriodType
    consolidation_scope: ConsolidationScope
    source_filing_id: str
    source_filing_version: int = Field(ge=1)
    evidence_ids: list[str] = Field(min_length=1)
    confidence: float = Field(ge=0, le=1)
    validation_status: ValidationStatus
    review_status: ReviewStatus
    extractor_version: str
    prompt_version: str | None = None
    approved_at: datetime | None = None
    approved_by: str | None = None
    is_current: bool = True


class IntelligenceObject(BaseModel):
    object_id: str
    workspace_id: str
    company_id: str
    run_id: str
    object_type: IntelligenceObjectType
    canonical_name: str
    reported_label: str | None = None
    value_decimal: Decimal | None = None
    value_text: str | None = None
    currency: str | None = None
    unit: str | None = None
    period_start: date | None = None
    period_end: date | None = None
    source_filing_id: str
    source_filing_version: int = Field(ge=1)
    evidence_ids: list[str] = Field(min_length=1)
    confidence: float = Field(ge=0, le=1)
    review_status: ReviewStatus = ReviewStatus.PENDING
    extractor_version: str


class OperationalMetric(IntelligenceObject):
    object_type: Literal[IntelligenceObjectType.OPERATIONAL_METRIC] = (
        IntelligenceObjectType.OPERATIONAL_METRIC
    )


class Guidance(IntelligenceObject):
    object_type: Literal[IntelligenceObjectType.GUIDANCE] = IntelligenceObjectType.GUIDANCE


class Adjustment(IntelligenceObject):
    object_type: Literal[IntelligenceObjectType.ADJUSTMENT] = IntelligenceObjectType.ADJUSTMENT


class CorporateEvent(IntelligenceObject):
    object_type: Literal[IntelligenceObjectType.CORPORATE_EVENT] = (
        IntelligenceObjectType.CORPORATE_EVENT
    )


class GovernanceResolution(IntelligenceObject):
    object_type: Literal[IntelligenceObjectType.GOVERNANCE_RESOLUTION] = (
        IntelligenceObjectType.GOVERNANCE_RESOLUTION
    )


class RiskDisclosure(IntelligenceObject):
    object_type: Literal[IntelligenceObjectType.RISK_DISCLOSURE] = (
        IntelligenceObjectType.RISK_DISCLOSURE
    )


class ManagementClaim(IntelligenceObject):
    object_type: Literal[IntelligenceObjectType.MANAGEMENT_CLAIM] = (
        IntelligenceObjectType.MANAGEMENT_CLAIM
    )


class ValidationDefect(BaseModel):
    defect_id: str
    run_id: str
    candidate_id: str | None = None
    rule_code: str
    severity: ValidationSeverity
    message: str
    context: dict[str, Any] = Field(default_factory=dict)


class ParsedXbrlContext(BaseModel):
    context_ref: str
    entity_identifier: str | None = None
    period_start: date | None = None
    period_end: date | None = None
    instant: date | None = None
    dimensions: dict[str, str] = Field(default_factory=dict)


class ParsedXbrlFact(BaseModel):
    concept: str
    namespace: str | None = None
    context_ref: str
    unit_ref: str | None = None
    decimals: str | None = None
    value_text: str


class ParsedPage(BaseModel):
    page: int = Field(ge=1)
    text: str
    character_count: int = Field(ge=0)


class ParsedDocument(BaseModel):
    filing_id: str
    content_type: str
    parser_name: str
    parser_version: str
    parse_quality: float = Field(ge=0, le=1)
    artifact_uri: str
    pages: list[ParsedPage] = Field(default_factory=list)
    xbrl_contexts: dict[str, ParsedXbrlContext] = Field(default_factory=dict)
    xbrl_facts: list[ParsedXbrlFact] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class FilingRun(BaseModel):
    run_id: str
    thread_id: str
    workspace_id: str
    company_id: str
    filing_id: str
    workflow_type: str = "filing.document.intelligence"
    idempotency_key: str
    status: FilingRunStatus
    current_node: str | None = None
    progress: float = Field(default=0, ge=0, le=1)
    attempt_count: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, ge=1)
    cancel_requested: bool = False
    input_payload: dict[str, Any] = Field(default_factory=dict)
    output_payload: dict[str, Any] = Field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None
    worker_id: str | None = None
    trace_id: str | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    heartbeat_at: datetime | None = None
    lease_expires_at: datetime | None = None
    waiting_review_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ReviewRequest(BaseModel):
    review_id: str
    run_id: str
    workspace_id: str
    status: ReviewStatus
    payload: dict[str, Any]
    decision_payload: dict[str, Any] = Field(default_factory=dict)
    reviewer_id: str | None = None
    reason: str | None = None
    created_at: datetime
    decided_at: datetime | None = None


class ManifestImportRequest(BaseModel):
    manifest_path: str = "data/filings/nse/INFY/manifest.json"
    workspace_id: str | None = None


class ManifestImportResponse(BaseModel):
    workspace_id: str
    company_id: str
    registered: int
    existing: int
    skipped_failed: int
    superseded: int
    filing_ids: list[str]


class FilingRunRequest(BaseModel):
    filing_id: str
    idempotency_key: str = Field(min_length=8, max_length=200)
    force_review: bool = False
    max_attempts: int = Field(default=3, ge=1, le=10)


class FilingRunResponse(BaseModel):
    run: FilingRun
    accepted: bool
    status_url: str
    review_url: str | None = None


class ReviewItemDecision(BaseModel):
    action: ReviewItemAction
    edits: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_edits(self) -> ReviewItemDecision:
        if self.action == ReviewItemAction.EDIT and not self.edits:
            raise ValueError("an item edit requires at least one edited field")
        if self.action != ReviewItemAction.EDIT and self.edits:
            raise ValueError("edits are only accepted when the item action is edit")
        return self


class ReviewDecisionRequest(BaseModel):
    decision: ReviewDecision
    reason: str = Field(min_length=3, max_length=2_000)
    candidate_decisions: dict[str, ReviewItemDecision] = Field(default_factory=dict)
    object_decisions: dict[str, ReviewItemDecision] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_decision_shape(self) -> ReviewDecisionRequest:
        has_item_decisions = bool(self.candidate_decisions or self.object_decisions)
        if self.decision == ReviewDecision.EDIT and not has_item_decisions:
            raise ValueError("edit decisions require candidate or object decisions")
        if self.decision != ReviewDecision.EDIT and has_item_decisions:
            raise ValueError("item decisions are only accepted with decision=edit")
        return self


class AnalysisQueryRequest(BaseModel):
    question: str = Field(min_length=3, max_length=4_000)
    company_id: str = "NSE:INFY"
    max_tool_calls: int = Field(default=4, ge=1, le=8)
    strict_evidence: bool = True

    @field_validator("question")
    @classmethod
    def reject_prompt_injection_commands(cls, value: str) -> str:
        lowered = value.lower()
        forbidden = (
            "ignore previous instructions",
            "reveal system prompt",
            "show hidden prompt",
        )
        if any(item in lowered for item in forbidden):
            raise ValueError("question contains a disallowed prompt-control instruction")
        return value.strip()


class AnalysisCitation(BaseModel):
    citation_id: str
    fact_id: str
    evidence_ids: list[str]
    label: str
    filing_id: str
    filing_version: int
    period_end: date


class AnalysisQueryResponse(BaseModel):
    analysis_id: str
    answer: str
    status: Literal["answered", "partial", "abstained"]
    citations: list[AnalysisCitation] = Field(default_factory=list)
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    trace_id: str


class InvestigationStatus(StrEnum):
    ACCEPTED = "accepted"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    ABSTAINED = "abstained"
    FAILED = "failed"


class InvestigationRequest(BaseModel):
    question: str = Field(min_length=8, max_length=4_000)
    universe_id: str = Field(default="NIFTY50", pattern=r"^[A-Z0-9_.:-]{2,64}$")
    strict_evidence: bool = True
    max_tool_calls: int = Field(default=8, ge=3, le=12)
    comparison: Literal["auto", "yoy", "qoq"] = "auto"
    idempotency_key: str | None = Field(default=None, min_length=8, max_length=200)

    @field_validator("question")
    @classmethod
    def reject_prompt_control(cls, value: str) -> str:
        normalized = value.strip()
        lowered = normalized.lower()
        forbidden = (
            "ignore previous instructions",
            "reveal system prompt",
            "show hidden prompt",
            "exfiltrate",
        )
        if any(item in lowered for item in forbidden):
            raise ValueError("question contains a disallowed prompt-control instruction")
        return normalized


class InvestigationEvent(BaseModel):
    event_id: str
    analysis_id: str
    sequence: int = Field(ge=1)
    node: str
    status: str
    detail: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class InvestigationRun(BaseModel):
    analysis_id: str
    thread_id: str
    workspace_id: str
    universe_id: str
    universe_snapshot_id: str | None = None
    question: str
    status: InvestigationStatus
    current_node: str
    progress: float = Field(ge=0, le=1)
    request_payload: dict[str, Any] = Field(default_factory=dict)
    plan_payload: dict[str, Any] = Field(default_factory=dict)
    result_payload: dict[str, Any] = Field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None
    trace_id: str | None = None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None


class InvestigationSubmission(BaseModel):
    run: InvestigationRun
    accepted: bool
    status_url: str
    events_url: str


class FilingUniverseSnapshot(BaseModel):
    snapshot_id: str
    workspace_id: str
    universe_id: str
    effective_date: date
    source_url: str
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    members: list[dict[str, str]]
    member_count: int = Field(ge=1)
    created_at: datetime


class FilingUniverseSnapshotRequest(BaseModel):
    universe_id: str = Field(default="NIFTY50", pattern=r"^[A-Z0-9_.:-]{2,64}$")
    effective_date: date
    source_url: str = Field(min_length=8, max_length=2_000)
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    members: list[dict[str, str]] = Field(min_length=1, max_length=500)

    @field_validator("members")
    @classmethod
    def validate_members(cls, value: list[dict[str, str]]) -> list[dict[str, str]]:
        normalized: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in value:
            symbol = str(item.get("symbol") or "").strip().upper()
            name = str(item.get("name") or "").strip()
            company_id = str(item.get("company_id") or f"NSE:{symbol}").strip()
            if not symbol or not name or not company_id.startswith("NSE:"):
                raise ValueError("each member requires an NSE company_id, symbol, and name")
            if company_id in seen:
                raise ValueError(f"duplicate universe member: {company_id}")
            seen.add(company_id)
            normalized.append({"company_id": company_id, "symbol": symbol, "name": name})
        return normalized


class FilingCoverageCompany(BaseModel):
    company_id: str
    symbol: str
    name: str
    status: Literal["eligible", "insufficient_history", "no_approved_facts"]
    approved_fact_count: int = Field(ge=0)
    available_periods: list[date] = Field(default_factory=list)
    available_metrics: list[str] = Field(default_factory=list)
    reason_codes: list[str] = Field(default_factory=list)


class FilingUniverseCoverage(BaseModel):
    universe_id: str
    snapshot_id: str | None = None
    member_count: int = Field(ge=0)
    represented_company_count: int = Field(ge=0)
    eligible_company_count: int = Field(ge=0)
    excluded_company_count: int = Field(ge=0)
    companies: list[FilingCoverageCompany] = Field(default_factory=list)
