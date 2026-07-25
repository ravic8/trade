from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from typing import Any

from trade_research.config import Settings, get_settings
from trade_research.filings.runtime import FilingRuntime, get_filing_runtime

_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+$")
_HEADER_PATTERN = re.compile(r"^[A-Za-z0-9-]+$")


def verify_reviewer_identity(
    settings: Settings,
    *,
    reviewer_email: str,
    actor_header: str,
) -> dict[str, Any]:
    normalized_email = reviewer_email.strip().lower()
    normalized_header = actor_header.strip().lower()
    if not _EMAIL_PATTERN.fullmatch(normalized_email):
        raise ValueError("reviewer identity must be a valid email address")
    if not _HEADER_PATTERN.fullmatch(actor_header):
        raise ValueError("actor header contains unsupported characters")
    configured_headers = {
        value.strip().lower() for value in settings.admin_email_headers.split(",") if value.strip()
    }
    configured_admins = {
        value.strip().lower() for value in settings.admin_emails.split(",") if value.strip()
    }
    if normalized_header not in configured_headers:
        raise ValueError("actor header is not configured for authenticated identity")
    if normalized_email not in configured_admins:
        raise ValueError("reviewer identity is not in the production admin allowlist")
    return {
        "reviewer_email": normalized_email,
        "actor_header": normalized_header,
        "header_configured": True,
        "reviewer_allowlisted": True,
    }


def human_review_acceptance_status(
    runtime: FilingRuntime,
    *,
    review_id: str,
    workspace_id: str,
) -> dict[str, Any]:
    review = runtime.store.review(review_id, workspace_id)
    if review is None:
        raise ValueError("review request was not found")
    run = runtime.store.run(review.run_id, workspace_id)
    if run is None:
        raise ValueError("review run was not found")
    candidates = runtime.store.candidate_facts(run.run_id)
    defects = runtime.store.validation_defects(run.run_id)
    facts = runtime.store.approved_facts(
        workspace_id=workspace_id,
        company_id=run.company_id,
        current_only=False,
        limit=2_000,
    )
    run_facts = [
        fact
        for fact in facts
        if fact.source_filing_id == run.filing_id and fact.run_id == run.run_id
    ]
    decision = str(review.decision_payload.get("decision") or "")
    audit_events = runtime.store.audit_events(
        workspace_id=workspace_id,
        action=f"review.{decision}" if decision else None,
        target_type="filing_review",
        target_id=review.review_id,
        limit=10,
    )
    review_status_counts = Counter(fact.review_status.value for fact in run_facts)
    reviewer_id = (review.reviewer_id or "").lower()
    return {
        "run_id": run.run_id,
        "review_id": review.review_id,
        "workspace_id": run.workspace_id,
        "company_id": run.company_id,
        "filing_id": run.filing_id,
        "run_status": run.status.value,
        "current_node": run.current_node,
        "attempt_count": run.attempt_count,
        "worker_id": run.worker_id,
        "lease_expires_at": (run.lease_expires_at.isoformat() if run.lease_expires_at else None),
        "waiting_review_at": (run.waiting_review_at.isoformat() if run.waiting_review_at else None),
        "trace_id": run.trace_id,
        "output_review_id": run.output_payload.get("review_id"),
        "output_review_status": run.output_payload.get("review_status"),
        "review_status": review.status.value,
        "reviewer_id": reviewer_id or None,
        "reason": review.reason,
        "decision": decision or None,
        "decided_at": (review.decided_at.isoformat() if review.decided_at else None),
        "packet_candidate_count": len(review.payload.get("candidate_facts", [])),
        "packet_object_count": len(review.payload.get("intelligence_objects", [])),
        "packet_evidence_count": len(review.payload.get("evidence", [])),
        "packet_defect_count": len(review.payload.get("defects", [])),
        "candidate_count": len(candidates),
        "validation_defect_count": len(defects),
        "approved_fact_count": len(run_facts),
        "unique_approved_fact_ids": len({fact.fact_id for fact in run_facts}),
        "review_status_counts": dict(review_status_counts),
        "reviewer_approved_fact_count": sum(
            1 for fact in run_facts if (fact.approved_by or "").lower() == reviewer_id
        ),
        "audit_event_count": len(audit_events),
        "audit_actor_ids": sorted({str(event["actor_id"]).lower() for event in audit_events}),
        "audit_actions": sorted({str(event["action"]) for event in audit_events}),
        "audit_reason_matches": bool(audit_events)
        and all(event.get("reason") == review.reason for event in audit_events),
    }


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, separators=(",", ":"), default=str))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Internal verification for the filing human-review drill."
    )
    subparsers = parser.add_subparsers(dest="operation", required=True)

    identity = subparsers.add_parser("identity")
    identity.add_argument("--reviewer-email", required=True)
    identity.add_argument("--actor-header", required=True)

    status = subparsers.add_parser("status")
    status.add_argument("--review-id", required=True)
    status.add_argument("--workspace-id", default="default")

    arguments = parser.parse_args()
    if arguments.operation == "identity":
        _print_json(
            verify_reviewer_identity(
                get_settings(),
                reviewer_email=arguments.reviewer_email,
                actor_header=arguments.actor_header,
            )
        )
    elif arguments.operation == "status":
        _print_json(
            human_review_acceptance_status(
                get_filing_runtime(),
                review_id=arguments.review_id,
                workspace_id=arguments.workspace_id,
            )
        )


if __name__ == "__main__":
    main()
