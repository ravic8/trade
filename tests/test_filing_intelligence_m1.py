from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from trade_research.api.app import app
from trade_research.config import Settings, get_settings
from trade_research.filings.api import filing_runtime_dependency
from trade_research.filings.extractors import _normalize_legacy_nse_duration
from trade_research.filings.models import (
    FilingRunStatus,
    ReviewDecision,
    ReviewStatus,
)
from trade_research.filings.registry import import_manifest
from trade_research.filings.runtime import FilingRuntime
from trade_research.filings.store import FilingStore


def _xbrl(revenue: str = "1000", *, include_eps: bool = False) -> bytes:
    filler = "\n".join(
        f'<in-gaap:UnmappedMetric{index} contextRef="FY">{index}</in-gaap:UnmappedMetric{index}>'
        for index in range(1, 9)
    )
    eps = (
        '<in-gaap:BasicEarningsLossPerShareFromContinuingOperations '
        'contextRef="FY" unitRef="INR" decimals="2">10.50'
        "</in-gaap:BasicEarningsLossPerShareFromContinuingOperations>"
        if include_eps
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl
    xmlns:xbrli="http://www.xbrl.org/2003/instance"
    xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
    xmlns:in-gaap="https://example.test/in-gaap">
  <xbrli:context id="FY">
    <xbrli:entity>
      <xbrli:identifier scheme="https://www.mca.gov.in/CIN">INFY</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2024-04-01</xbrli:startDate>
      <xbrli:endDate>2025-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="INR"><xbrli:measure>iso4217:INR</xbrli:measure></xbrli:unit>
  <in-gaap:NatureOfReportStandaloneConsolidated contextRef="FY">
    Consolidated
  </in-gaap:NatureOfReportStandaloneConsolidated>
  <in-gaap:RevenueFromOperations contextRef="FY" unitRef="INR" decimals="0">
    {revenue}
  </in-gaap:RevenueFromOperations>
  {eps}
  {filler}
</xbrli:xbrl>
""".encode()


def _manifest_document(path: Path, *, title: str = "Financial results") -> dict[str, object]:
    payload = path.read_bytes()
    return {
        "categories": ["xbrl financial"],
        "titles": [title],
        "url": f"https://nsearchives.nseindia.com/{path.name}",
        "source_apis": ["NSE corporate-announcements"],
        "filing_date": "2025-04-17T16:00:00+05:30",
        "period_end": "2025-03-31",
        "scope": "consolidated",
        "audited": "audited",
        "submission_type": "financial-results",
        "relative_path": path.name,
        "filename": path.name,
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "detected_content_type": "application/xml",
        "source_metadata": [{"exchange": "NSE", "symbol": "INFY"}],
    }


def _write_manifest(
    root: Path,
    documents: list[dict[str, object]],
    *,
    filename: str = "manifest.json",
) -> Path:
    manifest_path = root / filename
    manifest_path.write_text(
        json.dumps(
            {
                "company": "Infosys Limited",
                "symbol": "INFY",
                "documents": documents,
            }
        ),
        encoding="utf-8",
    )
    return manifest_path


def _runtime(tmp_path: Path, *, force_review: bool = False) -> FilingRuntime:
    settings = Settings(
        app_env="test",
        data_dir=tmp_path,
        database_url=f"sqlite:///{tmp_path / 'filings.sqlite3'}",
        filing_artifact_dir=tmp_path / "artifacts",
        filing_force_human_review=force_review,
        filing_parse_min_quality=0.60,
        filing_worker_heartbeat_seconds=5,
        filing_worker_lease_seconds=30,
        filing_index_enabled=False,
        langfuse_enabled=False,
        otel_enabled=False,
    )
    return FilingRuntime(settings)


def test_manifest_import_is_integrity_checked_idempotent_and_versioned(
    tmp_path: Path,
) -> None:
    store = FilingStore(f"sqlite:///{tmp_path / 'registry.sqlite3'}")
    store.initialize()
    first_path = tmp_path / "financial-results-original.xml"
    first_path.write_bytes(_xbrl("1000"))
    first_manifest = _write_manifest(
        tmp_path,
        [_manifest_document(first_path)],
        filename="manifest-first.json",
    )

    first = import_manifest(store, manifest_path=first_manifest, workspace_id="alpha")
    repeated = import_manifest(store, manifest_path=first_manifest, workspace_id="alpha")

    assert first.registered == 1
    assert repeated.existing == 1
    assert first.filing_ids == repeated.filing_ids

    revised_path = tmp_path / "financial-results-revised.xml"
    revised_path.write_bytes(_xbrl("1100"))
    revised_manifest = _write_manifest(
        tmp_path,
        [_manifest_document(revised_path)],
        filename="manifest-revised.json",
    )
    revised = import_manifest(store, manifest_path=revised_manifest, workspace_id="alpha")
    documents = store.documents(
        workspace_id="alpha",
        company_id="NSE:INFY",
        current_only=False,
    )

    assert revised.superseded == 1
    assert {document.version for document in documents} == {1, 2}
    assert sum(document.is_current for document in documents) == 1
    assert store.documents(workspace_id="beta", company_id="NSE:INFY") == []

    revised_path.write_bytes(_xbrl("tampered"))
    try:
        import_manifest(store, manifest_path=revised_manifest, workspace_id="alpha")
    except ValueError as exc:
        assert "source hash mismatch" in str(exc)
    else:
        raise AssertionError("tampered source should fail manifest registration")


def test_manifest_import_accepts_nse_display_dates(tmp_path: Path) -> None:
    store = FilingStore(f"sqlite:///{tmp_path / 'registry.sqlite3'}")
    store.initialize()
    source = tmp_path / "financial-results.xml"
    source.write_bytes(_xbrl())
    document = _manifest_document(source)
    document["period_end"] = "31-MAR-2025"
    manifest = _write_manifest(tmp_path, [document])

    result = import_manifest(store, manifest_path=manifest, workspace_id="alpha")
    registered = store.document(result.filing_ids[0], "alpha")

    assert registered is not None
    assert registered.period_end is not None
    assert registered.period_end.isoformat() == "2025-03-31"


def test_legacy_nse_four_d_context_is_normalized_to_fiscal_ytd() -> None:
    from datetime import date

    assert _normalize_legacy_nse_duration(
        context_ref="FourD",
        period_start=date(2024, 10, 1),
        period_end=date(2024, 12, 31),
    ) == date(2024, 4, 1)
    assert _normalize_legacy_nse_duration(
        context_ref="OneD",
        period_start=date(2024, 10, 1),
        period_end=date(2024, 12, 31),
    ) == date(2024, 10, 1)


def test_xbrl_workflow_auto_approves_with_exact_evidence_and_replays_safely(
    tmp_path: Path,
) -> None:
    source = tmp_path / "financial-results.xml"
    source.write_bytes(_xbrl("1000"))
    manifest = _write_manifest(tmp_path, [_manifest_document(source)])
    runtime = _runtime(tmp_path)
    imported = import_manifest(
        runtime.store,
        manifest_path=manifest,
        workspace_id="alpha",
    )
    filing_id = imported.filing_ids[0]
    run, created = runtime.store.create_run(
        workspace_id="alpha",
        company_id="NSE:INFY",
        filing_id=filing_id,
        idempotency_key="fy25-results",
        max_attempts=3,
    )
    repeated, repeated_created = runtime.store.create_run(
        workspace_id="alpha",
        company_id="NSE:INFY",
        filing_id=filing_id,
        idempotency_key="fy25-results",
        max_attempts=3,
    )

    assert created is True
    assert repeated_created is False
    assert repeated.run_id == run.run_id

    runtime.store.mark_run_queued(run.run_id)
    completed = runtime.run_once(run.run_id, worker_id="test-worker")
    facts = runtime.store.approved_facts(
        workspace_id="alpha",
        company_id="NSE:INFY",
    )

    assert completed.status == FilingRunStatus.COMPLETED
    assert len(facts) == 1
    assert facts[0].canonical_metric == "revenue"
    assert facts[0].value == Decimal("1000")
    assert facts[0].review_status == ReviewStatus.APPROVED

    evidence_rows = runtime.store.evidence(
        workspace_id="alpha",
        evidence_ids=[facts[0].evidence_ids[0]],
    )
    evidence = evidence_rows[0]
    assert evidence is not None
    assert evidence.xbrl_concept == "RevenueFromOperations"
    assert evidence.context_ref == "FY"
    assert evidence.snippet == "RevenueFromOperations=1000"
    assert runtime.store.evidence(
        workspace_id="beta",
        evidence_ids=[facts[0].evidence_ids[0]],
    ) == []

    replayed = runtime.run_once(run.run_id, worker_id="test-worker-replay")
    replayed_facts = runtime.store.approved_facts(
        workspace_id="alpha",
        company_id="NSE:INFY",
    )
    assert replayed.status == FilingRunStatus.COMPLETED
    assert [fact.fact_id for fact in replayed_facts] == [facts[0].fact_id]


def test_langgraph_human_review_interrupt_and_resume(tmp_path: Path) -> None:
    source = tmp_path / "financial-results.xml"
    source.write_bytes(_xbrl("1000"))
    manifest = _write_manifest(tmp_path, [_manifest_document(source)])
    runtime = _runtime(tmp_path, force_review=True)
    imported = import_manifest(
        runtime.store,
        manifest_path=manifest,
        workspace_id="alpha",
    )
    run, _ = runtime.store.create_run(
        workspace_id="alpha",
        company_id="NSE:INFY",
        filing_id=imported.filing_ids[0],
        idempotency_key="forced-review",
        max_attempts=3,
        input_payload={"force_review": True},
    )

    runtime.store.mark_run_queued(run.run_id)
    waiting = runtime.run_once(run.run_id, worker_id="review-worker")
    assert waiting.status == FilingRunStatus.WAITING_REVIEW
    review = runtime.store.pending_review_for_run(run.run_id)
    assert review is not None
    assert len(review.payload["candidate_facts"]) == 1

    runtime.store.decide_review(
        review_id=review.review_id,
        workspace_id="alpha",
        decision=ReviewDecision.APPROVE,
        reviewer_id="analyst@example.test",
        reason="Matched the audited NSE XBRL value and context.",
    )
    runtime.store.mark_run_queued(run.run_id)
    completed = runtime.run_once(
        run.run_id,
        resume_payload={"review_id": review.review_id},
        worker_id="review-worker-resume",
    )
    facts = runtime.store.approved_facts(
        workspace_id="alpha",
        company_id="NSE:INFY",
    )

    assert completed.status == FilingRunStatus.COMPLETED
    assert completed.output_payload["review_id"] == review.review_id
    assert len(facts) == 1
    assert facts[0].approved_by == "analyst@example.test"


def test_filing_api_enforces_workspace_boundary_and_idempotency(tmp_path: Path) -> None:
    source = tmp_path / "financial-results.xml"
    source.write_bytes(_xbrl("1000"))
    manifest = _write_manifest(tmp_path, [_manifest_document(source)])
    runtime = _runtime(tmp_path)
    app.dependency_overrides[get_settings] = lambda: runtime.settings
    app.dependency_overrides[filing_runtime_dependency] = lambda: runtime
    try:
        with TestClient(app) as client:
            imported = client.post(
                "/api/filings/manifests/import",
                headers={"X-Workspace-ID": "alpha"},
                json={"manifest_path": str(manifest)},
            )
            assert imported.status_code == 200
            filing_id = imported.json()["filing_ids"][0]

            first = client.post(
                "/api/filings/runs",
                headers={
                    "X-Workspace-ID": "alpha",
                    "X-Actor-ID": "analyst@example.test",
                },
                json={
                    "filing_id": filing_id,
                    "idempotency_key": "api-fy25-results",
                    "max_attempts": 3,
                },
            )
            repeated = client.post(
                "/api/filings/runs",
                headers={
                    "X-Workspace-ID": "alpha",
                    "X-Actor-ID": "analyst@example.test",
                },
                json={
                    "filing_id": filing_id,
                    "idempotency_key": "api-fy25-results",
                    "max_attempts": 3,
                },
            )
            outside_workspace = client.get(
                "/api/filings/documents",
                headers={"X-Workspace-ID": "beta"},
            )

        assert first.status_code == 202
        assert first.json()["run"]["status"] == FilingRunStatus.COMPLETED.value
        assert first.json()["accepted"] is True
        assert repeated.status_code == 202
        assert repeated.json()["accepted"] is False
        assert repeated.json()["run"]["run_id"] == first.json()["run"]["run_id"]
        assert outside_workspace.status_code == 200
        assert outside_workspace.json() == []
    finally:
        app.dependency_overrides.clear()


def test_candidate_level_review_requires_complete_inventory_and_applies_actions(
    tmp_path: Path,
) -> None:
    source = tmp_path / "financial-results.xml"
    source.write_bytes(_xbrl("1000", include_eps=True))
    manifest = _write_manifest(tmp_path, [_manifest_document(source)])
    runtime = _runtime(tmp_path, force_review=True)
    app.dependency_overrides[get_settings] = lambda: runtime.settings
    app.dependency_overrides[filing_runtime_dependency] = lambda: runtime
    try:
        with TestClient(app) as client:
            imported = client.post(
                "/api/filings/manifests/import",
                headers={"X-Workspace-ID": "alpha"},
                json={"manifest_path": str(manifest)},
            )
            filing_id = imported.json()["filing_ids"][0]
            submitted = client.post(
                "/api/filings/runs",
                headers={
                    "X-Workspace-ID": "alpha",
                    "X-Actor-ID": "analyst@example.test",
                },
                json={
                    "filing_id": filing_id,
                    "idempotency_key": "candidate-review-actions",
                    "force_review": True,
                },
            )
            assert submitted.status_code == 202
            assert submitted.json()["run"]["status"] == "waiting_review"
            review_id = submitted.json()["review_url"].rsplit("/", 1)[-1]
            review = client.get(
                f"/api/filings/reviews/{review_id}",
                headers={"X-Workspace-ID": "alpha"},
            ).json()
            candidates = {
                item["metric"]: item["candidate_id"]
                for item in review["payload"]["candidate_facts"]
            }
            assert set(candidates) == {"revenue", "basic_eps"}
            assert review["payload"]["evidence"]

            incomplete = client.post(
                f"/api/filings/reviews/{review_id}/decision",
                headers={
                    "X-Workspace-ID": "alpha",
                    "X-Actor-ID": "reviewer@example.test",
                },
                json={
                    "decision": "edit",
                    "reason": "Reviewed against the filing.",
                    "candidate_decisions": {
                        candidates["revenue"]: {
                            "action": "edit",
                            "edits": {"value_decimal": "1001"},
                        }
                    },
                },
            )
            assert incomplete.status_code == 409
            assert "complete review packet" in incomplete.json()["detail"]

            decided = client.post(
                f"/api/filings/reviews/{review_id}/decision",
                headers={
                    "X-Workspace-ID": "alpha",
                    "X-Actor-ID": "reviewer@example.test",
                },
                json={
                    "decision": "edit",
                    "reason": "Revenue corrected; EPS rejected as out of scope.",
                    "candidate_decisions": {
                        candidates["revenue"]: {
                            "action": "edit",
                            "edits": {"value_decimal": "1001"},
                        },
                        candidates["basic_eps"]: {"action": "reject"},
                    },
                },
            )
            facts = client.get(
                "/api/filings/facts",
                headers={"X-Workspace-ID": "alpha"},
            ).json()

        assert decided.status_code == 200
        assert decided.json()["status"] == "completed"
        assert len(facts) == 1
        assert facts[0]["canonical_metric"] == "revenue"
        assert Decimal(facts[0]["value"]) == Decimal("1001")
        assert facts[0]["review_status"] == "edited"
        candidate_rows = runtime.store.candidate_facts(
            decided.json()["run_id"]
        )
        statuses = {
            item["canonical_metric"]: item["status"] for item in candidate_rows
        }
        assert statuses == {"revenue": "approved", "basic_eps": "rejected"}
    finally:
        app.dependency_overrides.clear()
