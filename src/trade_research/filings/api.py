from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.exc import SQLAlchemyError

from trade_research.api.security import authenticated_email, require_admin_request
from trade_research.config import Settings, get_settings
from trade_research.filings.analysis import FinancialAnalysisService
from trade_research.filings.models import (
    AnalysisQueryRequest,
    AnalysisQueryResponse,
    EvidenceReference,
    FilingDocument,
    FilingRun,
    FilingRunRequest,
    FilingRunResponse,
    FilingRunStatus,
    FinancialFact,
    ManifestImportRequest,
    ManifestImportResponse,
    ReviewDecisionRequest,
    ReviewRequest,
    ReviewStatus,
)
from trade_research.filings.registry import import_manifest
from trade_research.filings.review import (
    serialized_item_decisions,
    validate_review_decision,
)
from trade_research.filings.runtime import FilingRuntime, get_filing_runtime
from trade_research.filings.tasks import dispatch_filing_run
from trade_research.filings.telemetry import filing_metrics

router = APIRouter(prefix="/api/filings", tags=["filing-intelligence"])
_WORKSPACE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def filing_runtime_dependency() -> FilingRuntime:
    return get_filing_runtime()


def workspace_dependency(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> str:
    workspace_id = request.headers.get("x-workspace-id")
    if settings.filing_require_workspace_header and not workspace_id:
        raise HTTPException(status_code=401, detail="X-Workspace-ID header is required")
    workspace_id = workspace_id or settings.filing_default_workspace_id
    if not _WORKSPACE_PATTERN.fullmatch(workspace_id):
        raise HTTPException(status_code=400, detail="invalid workspace identifier")
    return workspace_id


def actor_dependency(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> str:
    email = authenticated_email(request, settings.admin_email_headers)
    actor_id = email or request.headers.get("x-actor-id")
    if actor_id:
        return actor_id.strip().lower()
    if settings.filing_require_workspace_header:
        raise HTTPException(status_code=401, detail="authenticated actor is required")
    return "local-user"


def admin_or_local_dependency(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> str:
    if settings.admin_emails.strip():
        return require_admin_request(request, settings)
    if settings.app_env in {"local", "test"}:
        return "local-admin"
    raise HTTPException(
        status_code=403,
        detail="admin access must be configured outside the local environment",
    )


@router.get("/health")
def filing_health(
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> dict[str, Any]:
    return {
        "status": "ok",
        "queue_mode": runtime.settings.filing_queue_mode,
        "checkpoint_backend": (
            "memory"
            if runtime.settings.database_url.startswith("sqlite")
            else "postgresql"
        ),
        "artifact_backend": runtime.settings.filing_artifact_backend,
        "workspace_header_required": (
            runtime.settings.filing_require_workspace_header
        ),
        "index_enabled": runtime.settings.filing_index_enabled,
        "langfuse_enabled": runtime.settings.langfuse_enabled,
        "otel_enabled": runtime.settings.otel_enabled,
        "extractor_version": runtime.settings.filing_extractor_version,
    }


@router.post("/manifests/import", response_model=ManifestImportResponse)
def import_filing_manifest(
    body: ManifestImportRequest,
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    _admin: Annotated[str, Depends(admin_or_local_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> ManifestImportResponse:
    manifest_path = Path(body.manifest_path)
    if not manifest_path.is_absolute():
        manifest_path = Path.cwd() / manifest_path
    manifest_path = manifest_path.resolve()
    data_root = runtime.settings.data_dir.expanduser().resolve()
    if not manifest_path.is_relative_to(data_root):
        raise HTTPException(
            status_code=400,
            detail="manifest must be located under the configured DATA_DIR",
        )
    try:
        result = import_manifest(
            runtime.store,
            manifest_path=manifest_path,
            workspace_id=workspace_id,
            verify_hashes=True,
        )
    except (ValueError, FileNotFoundError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="filing database unavailable") from exc
    runtime.store.record_audit_event(
        workspace_id=workspace_id,
        actor_id=_admin,
        action="filing_manifest.imported",
        target_type="filing_manifest",
        target_id=str(manifest_path),
        after_payload=result.model_dump(mode="json"),
        reason="admin manifest import",
    )
    return result


@router.get("/documents", response_model=list[FilingDocument])
def list_filing_documents(
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
    company_id: str | None = "NSE:INFY",
    category: str | None = None,
    current_only: bool = False,
    limit: Annotated[int, Query(ge=1, le=1_000)] = 200,
) -> list[FilingDocument]:
    return runtime.store.documents(
        workspace_id=workspace_id,
        company_id=company_id,
        category=category,
        current_only=current_only,
        limit=limit,
    )


@router.post(
    "/runs",
    response_model=FilingRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def submit_filing_run(
    body: FilingRunRequest,
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    actor_id: Annotated[str, Depends(actor_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> FilingRunResponse:
    document = runtime.store.document(body.filing_id, workspace_id)
    if not document:
        raise HTTPException(status_code=404, detail="filing document not found")
    try:
        run, created = runtime.store.create_run(
            workspace_id=workspace_id,
            company_id=document.company_id,
            filing_id=document.filing_id,
            idempotency_key=body.idempotency_key,
            max_attempts=body.max_attempts,
            input_payload={"force_review": body.force_review, "submitted_by": actor_id},
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if created:
        runtime.store.record_audit_event(
            workspace_id=workspace_id,
            actor_id=actor_id,
            action="filing_run.submitted",
            target_type="filing_run",
            target_id=run.run_id,
            after_payload=run.model_dump(mode="json"),
            reason="filing processing requested",
        )
        dispatch_filing_run(run.run_id, runtime=runtime)
        run = runtime.store.run(run.run_id, workspace_id) or run
    review = runtime.store.pending_review_for_run(run.run_id)
    return FilingRunResponse(
        run=run,
        accepted=created,
        status_url=f"/api/filings/runs/{run.run_id}",
        review_url=(
            f"/api/filings/reviews/{review.review_id}" if review is not None else None
        ),
    )


@router.get("/runs", response_model=list[FilingRun])
def list_filing_runs(
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
    run_status: Annotated[FilingRunStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[FilingRun]:
    return runtime.store.runs(
        workspace_id=workspace_id,
        status=run_status,
        limit=limit,
    )


@router.get("/runs/{run_id}", response_model=FilingRun)
def filing_run_status(
    run_id: str,
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> FilingRun:
    run = runtime.store.run(run_id, workspace_id)
    if not run:
        raise HTTPException(status_code=404, detail="filing run not found")
    return run


@router.post("/runs/{run_id}/cancel", response_model=FilingRun)
def cancel_filing_run(
    run_id: str,
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    actor_id: Annotated[str, Depends(actor_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> FilingRun:
    if not runtime.store.request_cancel(run_id, workspace_id):
        raise HTTPException(
            status_code=409,
            detail="run is missing or already terminal",
        )
    runtime.store.record_audit_event(
        workspace_id=workspace_id,
        actor_id=actor_id,
        action="filing_run.cancel_requested",
        target_type="filing_run",
        target_id=run_id,
        reason="user cancellation",
    )
    run = runtime.store.run(run_id, workspace_id)
    assert run is not None
    return run


@router.get("/reviews", response_model=list[ReviewRequest])
def list_filing_reviews(
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
    review_status: Annotated[
        ReviewStatus | None, Query(alias="status")
    ] = ReviewStatus.PENDING,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[ReviewRequest]:
    return runtime.store.reviews(
        workspace_id=workspace_id,
        status=review_status,
        limit=limit,
    )


@router.get("/reviews/{review_id}", response_model=ReviewRequest)
def filing_review(
    review_id: str,
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> ReviewRequest:
    review = runtime.store.review(review_id, workspace_id)
    if not review:
        raise HTTPException(status_code=404, detail="filing review not found")
    return review


@router.post("/reviews/{review_id}/decision", response_model=FilingRun)
def decide_filing_review(
    review_id: str,
    body: ReviewDecisionRequest,
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    actor_id: Annotated[str, Depends(actor_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> FilingRun:
    review = runtime.store.review(review_id, workspace_id)
    if not review:
        raise HTTPException(status_code=404, detail="filing review not found")
    try:
        validate_review_decision(
            review=review,
            request=body,
            candidates=runtime.store.candidate_facts(review.run_id),
            objects=runtime.store.intelligence_objects(run_id=review.run_id),
            validator=runtime.validator,
        )
        decided = runtime.store.decide_review(
            review_id=review_id,
            workspace_id=workspace_id,
            decision=body.decision,
            reviewer_id=actor_id,
            reason=body.reason,
            candidate_decisions=serialized_item_decisions(
                body.candidate_decisions
            ),
            object_decisions=serialized_item_decisions(body.object_decisions),
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    filing_metrics().review_decisions.add(1, {"decision": body.decision.value})
    dispatch_filing_run(
        decided.run_id,
        resume_payload={"review_id": decided.review_id},
        runtime=runtime,
    )
    run = runtime.store.run(decided.run_id, workspace_id)
    assert run is not None
    return run


@router.get("/facts", response_model=list[FinancialFact])
def list_approved_facts(
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
    company_id: str = "NSE:INFY",
    metric: Annotated[list[str] | None, Query()] = None,
    current_only: bool = True,
    limit: Annotated[int, Query(ge=1, le=2_000)] = 500,
) -> list[FinancialFact]:
    return runtime.store.approved_facts(
        workspace_id=workspace_id,
        company_id=company_id,
        metrics=metric,
        current_only=current_only,
        limit=limit,
    )


@router.get("/facts/{fact_id}/evidence", response_model=list[EvidenceReference])
def approved_fact_evidence(
    fact_id: str,
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> list[EvidenceReference]:
    fact = runtime.store.fact(fact_id, workspace_id)
    if not fact:
        raise HTTPException(status_code=404, detail="approved fact not found")
    evidence = runtime.store.evidence(
        workspace_id=workspace_id,
        evidence_ids=fact.evidence_ids,
    )
    if len(evidence) != len(set(fact.evidence_ids)):
        raise HTTPException(
            status_code=500,
            detail="approved fact has unresolved evidence",
        )
    return evidence


@router.post("/analysis/query", response_model=AnalysisQueryResponse)
def analyze_filing_question(
    body: AnalysisQueryRequest,
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
) -> AnalysisQueryResponse:
    service = FinancialAnalysisService(settings=runtime.settings, store=runtime.store)
    return service.answer(workspace_id=workspace_id, request=body)


@router.post("/operations/recover-stale", response_model=list[str])
def recover_stale_filing_runs(
    _admin: Annotated[str, Depends(admin_or_local_dependency)],
    workspace_id: Annotated[str, Depends(workspace_dependency)],
    runtime: Annotated[FilingRuntime, Depends(filing_runtime_dependency)],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[str]:
    recovered = runtime.store.recover_stale_runs(
        workspace_id=workspace_id,
        limit=limit,
    )
    for run_id in recovered:
        run = runtime.store.run(run_id, workspace_id)
        if run:
            dispatch_filing_run(run_id, runtime=runtime)
    return recovered
