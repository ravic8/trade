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
    classify_investigation_intent,
)
from trade_research.filings.agent_runtime import run_investigation_once
from trade_research.filings.api import filing_runtime_dependency
from trade_research.filings.models import (
    EvidenceReference,
    InvestigationStatus,
)
from trade_research.filings.quality import (
    build_validation_report,
    evaluate_investigation,
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
            validation = client.get(
                f"/api/filings/investigations/{analysis_id}/validation",
                headers={"X-Workspace-ID": "alpha"},
            )
            evaluation = client.post(
                f"/api/filings/investigations/{analysis_id}/evaluations",
                headers={
                    "X-Workspace-ID": "alpha",
                    "X-Actor-ID": "analyst@example.test",
                },
            )
            latest_evaluation = client.get(
                f"/api/filings/investigations/{analysis_id}/evaluations/latest",
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
    assert validation.status_code == 200
    assert validation.json()["status"] == "passed"
    assert validation.json()["retrieval_mode"] == "structured_financial_retrieval"
    assert {item["check_id"] for item in validation.json()["checks"]} == {
        "execution",
        "plan_schema",
        "intent_alignment",
        "answer_relevance",
        "universe_coverage",
        "tool_policy",
        "structured_retrieval",
        "evidence_resolution",
        "claim_validation",
    }
    assert evaluation.status_code == 201
    assert evaluation.json()["dataset_id"] == "nifty50-investigation-v2"
    assert evaluation.json()["evaluator_version"] == (
        "filing-investigation-evaluator-v2"
    )
    assert latest_evaluation.status_code == 200
    assert latest_evaluation.json()["evaluation_id"] == (
        evaluation.json()["evaluation_id"]
    )
    assert runtime.store.audit_events(
        workspace_id="alpha",
        action="filing_investigation.evaluated",
    )


def test_validation_report_detects_tool_policy_tampering(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    runtime.store.record_universe_snapshot(
        workspace_id="alpha",
        universe_id="NIFTY50",
        effective_date=date(2026, 7, 26),
        source_url="https://nsearchives.nseindia.com/nifty50.csv",
        source_hash="9" * 64,
        members=[
            {"company_id": "NSE:INFY", "symbol": "INFY", "name": "Infosys"},
        ],
    )
    for period_end, value in (
        (date(2026, 6, 30), "120"),
        (date(2025, 6, 30), "100"),
    ):
        _seed_fact(
            runtime,
            company_id="NSE:INFY",
            period_end=period_end,
            value=value,
        )
    run, _ = runtime.store.create_investigation(
        workspace_id="alpha",
        universe_id="NIFTY50",
        question="Rank Nifty 50 companies by year-over-year net profit growth.",
        request_payload={
            "strict_evidence": True,
            "comparison": "yoy",
            "max_tool_calls": 8,
        },
        idempotency_key="quality-tamper-test",
    )
    completed = run_investigation_once(runtime, run.analysis_id)
    tampered = completed.model_copy(deep=True)
    tampered.result_payload["tool_calls"][1]["tool"] = "unapproved.web_search"

    report = build_validation_report(tampered)

    assert report.status == "failed"
    tool_check = next(
        item for item in report.checks if item.check_id == "tool_policy"
    )
    assert tool_check.status == "failed"
    assert "unapproved.web_search" in tool_check.metrics["called_tools"]


def test_investigation_evaluation_passes_all_available_hard_gates(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.settings.filing_golden_dataset_path = tmp_path / "not-installed.json"
    runtime.store.record_universe_snapshot(
        workspace_id="alpha",
        universe_id="NIFTY50",
        effective_date=date(2026, 7, 26),
        source_url="https://nsearchives.nseindia.com/nifty50.csv",
        source_hash="8" * 64,
        members=[
            {"company_id": "NSE:INFY", "symbol": "INFY", "name": "Infosys"},
        ],
    )
    for period_end, value in (
        (date(2026, 6, 30), "120"),
        (date(2025, 6, 30), "100"),
    ):
        _seed_fact(
            runtime,
            company_id="NSE:INFY",
            period_end=period_end,
            value=value,
        )
    run, _ = runtime.store.create_investigation(
        workspace_id="alpha",
        universe_id="NIFTY50",
        question="Rank Nifty 50 companies by year-over-year net profit growth.",
        request_payload={
            "strict_evidence": True,
            "comparison": "yoy",
            "max_tool_calls": 8,
        },
        idempotency_key="quality-pass-test",
    )
    completed = run_investigation_once(runtime, run.analysis_id).model_copy(deep=True)
    completed.result_payload["planner_telemetry"] = {
        "status": "ok",
        "provider": "openai",
        "model": "test-model",
        "fallback": False,
    }

    report = evaluate_investigation(runtime, run=completed)

    assert report.status == "passed"
    assert report.score == 100
    assert all(
        suite.status == "passed"
        for suite in report.suites
        if suite.hard_gate
    )
    golden = next(
        suite for suite in report.suites if suite.suite_id == "extraction_golden"
    )
    assert golden.status == "not_evaluated"
    assert golden.hard_gate is False


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
    assert synthesis["claims"][0]["text"] == ("TCS (TCS) reported 30.00% YOY change in net profit.")
    assert "999" not in str(synthesis)
    assert "investment advice" not in str(synthesis)
    assert completed.result_payload["claim_validation"]["canonicalized_claims"] == 1


def test_top_ten_model_claims_are_validated_against_all_ranked_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = _runtime(tmp_path, agent_enabled=True)
    members = [
        {
            "company_id": f"NSE:COMPANY{index}",
            "symbol": f"COMPANY{index}",
            "name": f"Company {index}",
        }
        for index in range(1, 11)
    ]
    runtime.store.record_universe_snapshot(
        workspace_id="alpha",
        universe_id="NIFTY50",
        effective_date=date(2026, 7, 26),
        source_url="https://nsearchives.nseindia.com/nifty50.csv",
        source_hash="e" * 64,
        members=members,
    )
    for index, member in enumerate(members, start=1):
        _seed_fact(
            runtime,
            company_id=member["company_id"],
            period_end=date(2026, 6, 30),
            value=str(100 + index),
        )
        _seed_fact(
            runtime,
            company_id=member["company_id"],
            period_end=date(2025, 6, 30),
            value="100",
        )

    def fake_plan(self, **_):
        return (
            InvestigationPlan(
                intent="rank_growth",
                metric="net_profit",
                comparison="yoy",
                limit=10,
            ),
            {"status": "ok", "provider": "openai", "model": "test-model"},
        )

    def fake_synthesis(self, **kwargs):
        rows = kwargs["comparison"]["rows"]
        return InvestigationSynthesis(
            title="Model title",
            summary="Model summary",
            claims=[
                SynthesisClaim(
                    text=f"Model-authored claim {index}",
                    citation_ids=list(row["citation_ids"]),
                )
                for index, row in enumerate(rows, start=1)
            ],
            limitations=[],
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
        idempotency_key="top-ten-canonical-claims-test",
    )

    completed = run_investigation_once(runtime, run.analysis_id)

    validation = completed.result_payload["claim_validation"]
    synthesis = completed.result_payload["synthesis"]
    assert completed.status == InvestigationStatus.COMPLETED
    assert validation["passed"] is True
    assert validation["valid_claim_count"] == 10
    assert validation["rejected_claim_count"] == 0
    assert validation["canonicalized_claims"] == 10
    assert len(synthesis["claims"]) == 10


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("For which stocks do you have data?", "coverage"),
        ("What universe stocks do you have data for?", "coverage"),
        ("What are your capabilities and limitations?", "capabilities"),
        ("What are your capabilites?", "capabilities"),
        ("What can you not do?", "limitations"),
        ("Compare INFY and TCS revenue", "compare_companies"),
    ],
)
def test_question_intent_classifier(question: str, expected: str) -> None:
    assert classify_investigation_intent(question) == expected


@pytest.mark.parametrize(
    ("question", "answer_type", "expected_tools"),
    [
        (
            "For which stocks do you have data?",
            "coverage",
            ["filings.get_coverage"],
        ),
        (
            "What are your capabilities and limitations?",
            "capabilities",
            ["filings.get_coverage", "agent.describe_capabilities"],
        ),
    ],
)
def test_system_questions_take_non_financial_graph_route(
    tmp_path: Path,
    question: str,
    answer_type: str,
    expected_tools: list[str],
) -> None:
    runtime = _runtime(tmp_path)
    runtime.store.record_universe_snapshot(
        workspace_id="alpha",
        universe_id="NIFTY50",
        effective_date=date(2026, 7, 26),
        source_url="https://nsearchives.nseindia.com/nifty50.csv",
        source_hash="f" * 64,
        members=[
            {"company_id": "NSE:INFY", "symbol": "INFY", "name": "Infosys"},
            {"company_id": "NSE:TCS", "symbol": "TCS", "name": "TCS"},
        ],
    )
    _seed_fact(
        runtime,
        company_id="NSE:INFY",
        period_end=date(2026, 6, 30),
        value="120",
    )
    run, _ = runtime.store.create_investigation(
        workspace_id="alpha",
        universe_id="NIFTY50",
        question=question,
        request_payload={"strict_evidence": True, "comparison": "auto", "max_tool_calls": 8},
        idempotency_key=f"system-route-{answer_type}",
    )

    completed = run_investigation_once(runtime, run.analysis_id)

    result = completed.result_payload
    assert completed.status == InvestigationStatus.COMPLETED
    assert result["answer_type"] == answer_type
    assert "ranking" not in result
    assert "synthesis" not in result
    assert result["answer_validation"]["passed"] is True
    assert [item["tool"] for item in result["tool_calls"]] == expected_tools
    if answer_type == "coverage":
        assert [
            item["symbol"] for item in result["system_answer"]["available_companies"]
        ] == ["INFY"]
        assert [
            item["symbol"] for item in result["system_answer"]["unavailable_companies"]
        ] == ["TCS"]
    else:
        assert result["system_answer"]["capabilities"]
        assert result["system_answer"]["limitations"]

    validation = build_validation_report(completed)
    assert validation.status == "passed"
    assert {item.check_id: item.status for item in validation.checks}[
        "answer_relevance"
    ] == "passed"


def test_semantically_wrong_legacy_answer_fails_quality_gates(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    runtime.store.record_universe_snapshot(
        workspace_id="alpha",
        universe_id="NIFTY50",
        effective_date=date(2026, 7, 26),
        source_url="https://nsearchives.nseindia.com/nifty50.csv",
        source_hash="1" * 64,
        members=[{"company_id": "NSE:INFY", "symbol": "INFY", "name": "Infosys"}],
    )
    for period_end, value in ((date(2026, 6, 30), "120"), (date(2025, 6, 30), "100")):
        _seed_fact(runtime, company_id="NSE:INFY", period_end=period_end, value=value)
    run, _ = runtime.store.create_investigation(
        workspace_id="alpha",
        universe_id="NIFTY50",
        question="Rank companies by year-over-year net profit growth.",
        request_payload={"strict_evidence": True, "comparison": "yoy", "max_tool_calls": 8},
        idempotency_key="wrong-answer-quality",
    )
    completed = run_investigation_once(runtime, run.analysis_id)
    wrong_result = {
        **completed.result_payload,
        "plan": {**completed.result_payload["plan"], "intent": "coverage"},
    }
    legacy_wrong = completed.model_copy(
        update={
            "question": "What are your capabilities and limitations?",
            "result_payload": wrong_result,
        }
    )

    report = build_validation_report(legacy_wrong)
    checks = {item.check_id: item.status for item in report.checks}
    assert report.status == "failed"
    assert checks["intent_alignment"] == "failed"
    assert checks["answer_relevance"] == "failed"
    assert checks["tool_policy"] == "failed"

    evaluation = evaluate_investigation(runtime, run=legacy_wrong)
    suites = {item.suite_id: item for item in evaluation.suites}
    assert evaluation.status == "failed"
    assert suites["plan_quality"].status == "failed"
    assert suites["answer_relevance"].status == "failed"
