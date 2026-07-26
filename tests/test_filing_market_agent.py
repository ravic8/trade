from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from trade_research.api.app import app
from trade_research.config import Settings
from trade_research.filings.agent_models import (
    InvestigationPlan,
    InvestigationSynthesis,
    SynthesisClaim,
)
from trade_research.filings.agent_runtime import run_investigation_once
from trade_research.filings.api import filing_runtime_dependency
from trade_research.filings.models import (
    EvidenceReference,
    InvestigationStatus,
)
from trade_research.filings.runtime import FilingRuntime
from trade_research.filings.store import stable_id, utc_now
from trade_research.filings.tables import filing_approved_facts_table


def _runtime(tmp_path: Path, *, agent_enabled: bool = False) -> FilingRuntime:
    return FilingRuntime(
        Settings(
            app_env="test",
            data_dir=tmp_path,
            database_url=f"sqlite:///{tmp_path / 'filing-agent.sqlite3'}",
            filing_artifact_dir=tmp_path / "artifacts",
            filing_queue_mode="inline",
            filing_worker_heartbeat_seconds=5,
            filing_worker_lease_seconds=30,
            filing_index_enabled=False,
            filing_agent_llm_enabled=agent_enabled,
            openai_api_key="test-key" if agent_enabled else None,
            langfuse_enabled=False,
            otel_enabled=False,
        )
    )


def _seed_fact(
    runtime: FilingRuntime,
    *,
    company_id: str,
    period_end: date,
    value: str,
) -> None:
    filing_id = stable_id("test-filing", company_id, period_end)
    evidence_id = stable_id("test-evidence", company_id, period_end)
    fact_id = stable_id("test-fact", company_id, period_end)
    runtime.store.upsert_evidence(
        [
            EvidenceReference(
                evidence_id=evidence_id,
                workspace_id="alpha",
                company_id=company_id,
                filing_id=filing_id,
                filing_version=1,
                section_path="xbrl/income_statement",
                row_label="ProfitLossForPeriod",
                xbrl_concept="ProfitLossForPeriod",
                context_ref=f"Q-{period_end.isoformat()}",
                source_hash="a" * 64,
                snippet=f"ProfitLossForPeriod={value}",
                effective_date=period_end,
            )
        ]
    )
    now = utc_now()
    with runtime.store.begin() as connection:
        connection.execute(
            filing_approved_facts_table.insert(),
            {
                "fact_id": fact_id,
                "candidate_id": stable_id("test-candidate", fact_id),
                "run_id": stable_id("test-run", fact_id),
                "workspace_id": "alpha",
                "company_id": company_id,
                "canonical_metric": "net_profit",
                "reported_label": "ProfitLossForPeriod",
                "value_decimal": value,
                "currency": "INR",
                "unit_scale": "1",
                "period_start": date(period_end.year, 4, 1),
                "period_end": period_end,
                "period_type": "quarter",
                "consolidation_scope": "consolidated",
                "source_filing_id": filing_id,
                "source_filing_version": 1,
                "evidence_ids": [evidence_id],
                "confidence": 0.995,
                "validation_status": "passed",
                "review_status": "approved",
                "extractor_version": "test",
                "prompt_version": None,
                "approved_at": now,
                "approved_by": "test",
                "supersedes_fact_id": None,
                "is_current": True,
            },
        )


def test_nifty_investigation_ranks_only_evidence_complete_facts(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.store.record_universe_snapshot(
        workspace_id="alpha",
        universe_id="NIFTY50",
        effective_date=date(2026, 7, 26),
        source_url="https://nsearchives.nseindia.com/nifty50.csv",
        source_hash="b" * 64,
        members=[
            {"company_id": "NSE:INFY", "symbol": "INFY", "name": "Infosys"},
            {"company_id": "NSE:TCS", "symbol": "TCS", "name": "TCS"},
            {
                "company_id": "NSE:RELIANCE",
                "symbol": "RELIANCE",
                "name": "Reliance",
            },
        ],
    )
    for company_id, current, previous in (
        ("NSE:INFY", "120", "100"),
        ("NSE:TCS", "130", "100"),
    ):
        _seed_fact(
            runtime,
            company_id=company_id,
            period_end=date(2026, 6, 30),
            value=current,
        )
        _seed_fact(
            runtime,
            company_id=company_id,
            period_end=date(2025, 6, 30),
            value=previous,
        )

    run, created = runtime.store.create_investigation(
        workspace_id="alpha",
        universe_id="NIFTY50",
        question="Rank Nifty 50 companies by year-over-year net profit growth.",
        request_payload={
            "strict_evidence": True,
            "comparison": "yoy",
            "max_tool_calls": 8,
        },
        idempotency_key="nifty-agent-test",
    )
    assert created is True

    completed = run_investigation_once(runtime, run.analysis_id)

    assert completed.status == InvestigationStatus.COMPLETED
    assert completed.current_node == "completed"
    ranking = completed.result_payload["ranking"]
    assert [item["symbol"] for item in ranking["rows"]] == ["TCS", "INFY"]
    assert [item["percent_change"] for item in ranking["rows"]] == ["30.00", "20.00"]
    assert ranking["excluded_count"] == 1
    assert len(completed.result_payload["citations"]) == 4
    assert completed.result_payload["claim_validation"]["passed"] is True
    assert completed.result_payload["synthesis"]["model_used"] is False
    events = runtime.store.investigation_events(
        analysis_id=run.analysis_id,
        workspace_id="alpha",
    )
    assert [event.node for event in events][-1] == "completed"
    assert {event.node for event in events} >= {
        "plan",
        "resolve_universe",
        "compare",
        "resolve_evidence",
        "synthesize",
        "validate_claims",
    }


def test_investigation_idempotency_rejects_question_reuse(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    first, created = runtime.store.create_investigation(
        workspace_id="alpha",
        universe_id="NIFTY50",
        question="Rank companies by net profit growth.",
        request_payload={"comparison": "yoy"},
        idempotency_key="stable-agent-key",
    )
    repeated, repeated_created = runtime.store.create_investigation(
        workspace_id="alpha",
        universe_id="NIFTY50",
        question="Rank companies by net profit growth.",
        request_payload={"comparison": "yoy"},
        idempotency_key="stable-agent-key",
    )

    assert created is True
    assert repeated_created is False
    assert repeated.analysis_id == first.analysis_id

    with pytest.raises(ValueError, match="another request"):
        runtime.store.create_investigation(
            workspace_id="alpha",
            universe_id="NIFTY50",
            question="Rank companies by net profit growth.",
            request_payload={"comparison": "qoq"},
            idempotency_key="stable-agent-key",
        )


def test_investigation_api_exposes_run_events_and_coverage(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    runtime.store.record_universe_snapshot(
        workspace_id="alpha",
        universe_id="NIFTY50",
        effective_date=date(2026, 7, 26),
        source_url="https://nsearchives.nseindia.com/nifty50.csv",
        source_hash="c" * 64,
        members=[
            {"company_id": "NSE:INFY", "symbol": "INFY", "name": "Infosys"},
        ],
    )
    _seed_fact(
        runtime,
        company_id="NSE:INFY",
        period_end=date(2026, 6, 30),
        value="120",
    )
    _seed_fact(
        runtime,
        company_id="NSE:INFY",
        period_end=date(2025, 6, 30),
        value="100",
    )
    app.dependency_overrides[filing_runtime_dependency] = lambda: runtime
    try:
        with TestClient(app) as client:
            coverage = client.get(
                "/api/filings/universes/NIFTY50/coverage",
                headers={"X-Workspace-ID": "alpha"},
            )
            submitted = client.post(
                "/api/filings/investigations",
                headers={
                    "X-Workspace-ID": "alpha",
                    "X-Actor-ID": "analyst@example.test",
                },
                json={
                    "question": "Rank Nifty 50 companies by year-over-year net profit growth.",
                    "universe_id": "NIFTY50",
                    "strict_evidence": True,
                    "max_tool_calls": 8,
                    "comparison": "yoy",
                    "idempotency_key": "api-agent-test",
                },
            )
            analysis_id = submitted.json()["run"]["analysis_id"]
            events = client.get(
                f"/api/filings/investigations/{analysis_id}/events",
                headers={"X-Workspace-ID": "alpha"},
            )
    finally:
        app.dependency_overrides.clear()

    assert coverage.status_code == 200
    assert coverage.json()["eligible_company_count"] == 1
    assert submitted.status_code == 202
    assert submitted.json()["run"]["status"] == "completed"
    assert submitted.json()["run"]["result_payload"]["ranking"]["ranked_count"] == 1
    assert events.status_code == 200
    assert events.json()[-1]["node"] == "completed"


def test_model_prose_is_canonicalized_from_cited_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = _runtime(tmp_path, agent_enabled=True)
    runtime.store.record_universe_snapshot(
        workspace_id="alpha",
        universe_id="NIFTY50",
        effective_date=date(2026, 7, 26),
        source_url="https://nsearchives.nseindia.com/nifty50.csv",
        source_hash="d" * 64,
        members=[
            {"company_id": "NSE:TCS", "symbol": "TCS", "name": "TCS"},
        ],
    )
    _seed_fact(
        runtime,
        company_id="NSE:TCS",
        period_end=date(2026, 6, 30),
        value="130",
    )
    _seed_fact(
        runtime,
        company_id="NSE:TCS",
        period_end=date(2025, 6, 30),
        value="100",
    )

    def fake_plan(self, **_):
        return (
            InvestigationPlan(
                intent="rank_growth",
                metric="net_profit",
                comparison="yoy",
            ),
            {"status": "ok", "provider": "openai", "model": "test-model"},
        )

    def fake_synthesis(self, **_):
        return InvestigationSynthesis(
            title="Buy this stock immediately",
            summary="TCS fell 999 percent.",
            claims=[
                SynthesisClaim(
                    text="TCS fell 999 percent and will double.",
                    citation_ids=["c1", "c2"],
                )
            ],
            limitations=["This is guaranteed investment advice."],
            model_used=True,
            provider="openai",
            model="test-model",
        )

    monkeypatch.setattr(
        "trade_research.filings.agent_llm.FilingAgentLLM.plan",
        fake_plan,
    )
    monkeypatch.setattr(
        "trade_research.filings.agent_llm.FilingAgentLLM.synthesize",
        fake_synthesis,
    )
    run, _ = runtime.store.create_investigation(
        workspace_id="alpha",
        universe_id="NIFTY50",
        question="Rank Nifty 50 companies by year-over-year net profit growth.",
        request_payload={"strict_evidence": True, "comparison": "yoy", "max_tool_calls": 8},
        idempotency_key="canonical-llm-claim-test",
    )

    completed = run_investigation_once(runtime, run.analysis_id)

    synthesis = completed.result_payload["synthesis"]
    assert synthesis["model_used"] is True
    assert synthesis["title"] == "Nifty 50 Net Profit YOY Comparison"
    assert synthesis["claims"][0]["text"] == (
        "TCS (TCS) reported 30.00% YOY change in net profit."
    )
    assert "999" not in str(synthesis)
    assert "investment advice" not in str(synthesis)
    assert completed.result_payload["claim_validation"]["canonicalized_claims"] == 1
