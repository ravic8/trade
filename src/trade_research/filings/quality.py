from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from trade_research.filings.agent_models import (
    SYSTEM_INTENTS,
    InvestigationPlan,
    decide_investigation_intent,
)
from trade_research.filings.evaluation import (
    evaluate_golden_dataset,
    load_golden_dataset,
)
from trade_research.filings.intent_evaluation import (
    DEFAULT_INTENT_DATASET_PATH,
    IntentEvaluationExpectation,
    evaluate_question_intent,
    load_intent_evaluation_dataset,
)
from trade_research.filings.models import (
    InvestigationEvaluationReport,
    InvestigationEvaluationSuite,
    InvestigationQualityCheck,
    InvestigationRun,
    InvestigationStatus,
    InvestigationValidationReport,
)
from trade_research.filings.runtime import FilingRuntime
from trade_research.filings.store import utc_now

EVALUATOR_VERSION = "filing-investigation-evaluator-v3"
DATASET_ID = "nifty50-investigation-v3"
FINANCIAL_TOOL_SEQUENCE = (
    "filings.get_coverage",
    "financial_facts.compare",
    "filing_evidence.resolve",
)


def build_validation_report(
    run: InvestigationRun,
    *,
    intent_dataset_path: Path = DEFAULT_INTENT_DATASET_PATH,
) -> InvestigationValidationReport:
    result = run.result_payload
    plan_payload = result.get("plan") or run.plan_payload
    plan_valid, plan_detail = _validate_plan(plan_payload)
    expectation = _intent_expectation(run.question, intent_dataset_path)
    planned_intent = str(plan_payload.get("intent") or "") if isinstance(plan_payload, dict) else ""
    route_intent = expectation.intent or planned_intent
    intent_valid, intent_detail, intent_metrics, intent_status = _validate_intent(
        expectation,
        plan_payload,
    )
    answer_valid, answer_detail, answer_metrics, answer_status = _validate_answer_relevance(
        expectation.intent,
        result,
    )
    coverage_valid, coverage_detail, coverage_metrics = _validate_coverage(result.get("coverage"))
    tools_valid, tools_detail, tools_metrics = _validate_tools(
        run,
        route_intent=route_intent,
    )
    retrieval_valid, retrieval_detail, retrieval_metrics = _validate_retrieval(result)
    evidence_valid, evidence_detail, evidence_metrics = _validate_evidence(result)
    claims_valid, claims_detail, claims_metrics = _validate_claims(result)
    terminal = run.status in {
        InvestigationStatus.COMPLETED,
        InvestigationStatus.PARTIAL,
        InvestigationStatus.ABSTAINED,
        InvestigationStatus.FAILED,
    }
    execution_passed = terminal and run.status != InvestigationStatus.FAILED
    checks = [
        _check(
            "execution",
            "Durable graph execution",
            execution_passed,
            (
                f"Terminal status {run.status.value} at node {run.current_node}."
                if terminal
                else f"Run is still {run.status.value} at node {run.current_node}."
            ),
            status="warning" if not terminal else None,
            metrics={"progress": run.progress, "trace_id": run.trace_id},
        ),
        _check("plan_schema", "Bounded plan schema", plan_valid, plan_detail),
        _check(
            "intent_alignment",
            "Question-to-intent alignment",
            intent_valid,
            intent_detail,
            status=intent_status,
            metrics=intent_metrics,
        ),
        _check(
            "answer_relevance",
            "Answer relevance and route contract",
            answer_valid,
            answer_detail,
            status=answer_status,
            metrics=answer_metrics,
        ),
        _check(
            "universe_coverage",
            "Universe and exclusion accounting",
            coverage_valid,
            coverage_detail,
            metrics=coverage_metrics,
        ),
        _check(
            "tool_policy",
            "Tool allowlist and budget",
            tools_valid,
            tools_detail,
            metrics=tools_metrics,
        ),
    ]
    if route_intent not in SYSTEM_INTENTS:
        checks.extend(
            [
                _check(
                    "structured_retrieval",
                    "Structured retrieval and deterministic math",
                    retrieval_valid,
                    retrieval_detail,
                    metrics=retrieval_metrics,
                ),
                _check(
                    "evidence_resolution",
                    "Exact filing evidence resolution",
                    evidence_valid,
                    evidence_detail,
                    metrics=evidence_metrics,
                ),
                _check(
                    "claim_validation",
                    "Citation-bound claim validation",
                    claims_valid,
                    claims_detail,
                    metrics=claims_metrics,
                ),
            ]
        )
    failed = any(item.status == "failed" for item in checks)
    warning = any(item.status in {"warning", "not_evaluated"} for item in checks)
    return InvestigationValidationReport(
        analysis_id=run.analysis_id,
        status="failed" if failed else "partial" if warning else "passed",
        checks=checks,
        generated_at=utc_now(),
    )


def evaluate_investigation(
    runtime: FilingRuntime,
    *,
    run: InvestigationRun,
) -> InvestigationEvaluationReport:
    intent_dataset_path = runtime.settings.filing_intent_evaluation_dataset_path
    validation = build_validation_report(
        run,
        intent_dataset_path=intent_dataset_path,
    )
    checks = {item.check_id: item for item in validation.checks}
    planner = dict(run.result_payload.get("planner_telemetry") or {})

    plan_checks = [
        checks["plan_schema"],
        checks["intent_alignment"],
        _check(
            "planner_provider",
            "LLM planner completed",
            planner.get("status") == "ok",
            (
                f"Planner status {planner.get('status', 'missing')}; "
                f"provider {planner.get('provider', 'unknown')}."
            ),
            metrics={
                "provider": planner.get("provider"),
                "model": planner.get("model"),
                "latency_ms": planner.get("latency_ms"),
                "usage": planner.get("usage") or {},
            },
        ),
        _check(
            "planner_fallback",
            "No planner fallback",
            planner.get("fallback") is False,
            (
                "The provider plan passed schema validation."
                if planner.get("fallback") is False
                else "The deterministic safety fallback was used."
            ),
        ),
        _check(
            "planner_repair",
            "Bounded planner repair",
            planner.get("repair_attempted") is not True or planner.get("repair_succeeded") is True,
            (
                "The initial provider plan passed without repair."
                if planner.get("repair_attempted") is not True
                else "The provider repaired its structured plan within one revision."
                if planner.get("repair_succeeded") is True
                else "The bounded provider repair failed; deterministic fallback was used."
            ),
            metrics={
                "repair_attempted": planner.get("repair_attempted", False),
                "repair_succeeded": planner.get("repair_succeeded", False),
                "initial_failure": planner.get("initial_failure"),
                "primary_status": planner.get("primary_status"),
                "repair_status": planner.get("repair_status"),
            },
        ),
    ]
    expectation = _intent_expectation(run.question, intent_dataset_path)
    plan_payload = run.result_payload.get("plan") or run.plan_payload
    planned_intent = str(plan_payload.get("intent") or "") if isinstance(plan_payload, dict) else ""
    route_intent = expectation.intent or planned_intent
    suites = [
        _suite("plan_quality", "LLM intent and plan quality", plan_checks),
        _suite(
            "answer_relevance",
            "Question-answer relevance",
            [checks["answer_relevance"]],
        ),
        _suite("tool_selection", "Tool selection and arguments", [checks["tool_policy"]]),
    ]
    if route_intent in SYSTEM_INTENTS:
        suites.append(
            _suite(
                "system_contract",
                "System capability and coverage contract",
                [checks["universe_coverage"], checks["answer_relevance"]],
            )
        )
    else:
        suites.extend(
            [
                _suite(
                    "structured_retrieval",
                    "Structured retrieval quality",
                    [checks["universe_coverage"], checks["structured_retrieval"]],
                    metrics={"retrieval_mode": "structured_financial_retrieval"},
                ),
                _suite(
                    "evidence_and_claims",
                    "Evidence and grounded claims",
                    [checks["evidence_resolution"], checks["claim_validation"]],
                ),
            ]
        )
    suites.append(_intent_routing_suite(intent_dataset_path))
    suites.append(_golden_suite(runtime, workspace_id=run.workspace_id))
    evaluated = [suite for suite in suites if suite.status != "not_evaluated"]
    score = round(sum(suite.score for suite in evaluated) / len(evaluated), 2) if evaluated else 0.0
    hard_failure = any(suite.hard_gate and suite.status != "passed" for suite in suites)
    return InvestigationEvaluationReport(
        evaluation_id=str(uuid4()),
        analysis_id=run.analysis_id,
        workspace_id=run.workspace_id,
        dataset_id=DATASET_ID,
        evaluator_version=EVALUATOR_VERSION,
        status="failed" if hard_failure else "passed",
        score=score,
        suites=suites,
        trace_id=run.trace_id,
        created_at=utc_now(),
    )


def _validate_plan(payload: Any) -> tuple[bool, str]:
    try:
        plan = InvestigationPlan.model_validate(payload)
    except ValidationError as exc:
        return False, f"Plan failed the bounded schema: {exc.errors()[0]['msg']}."
    return (
        True,
        f"{plan.intent} · {plan.metric} · {plan.comparison} · top {plan.limit} · {plan.scope}.",
    )


def _validate_intent(
    expectation: IntentEvaluationExpectation,
    payload: Any,
) -> tuple[bool, str, dict[str, Any], str | None]:
    planned = payload.get("intent") if isinstance(payload, dict) else None
    if not expectation.evaluated or expectation.intent is None:
        return (
            False,
            (
                "No sufficiently similar labeled intent case exists; semantic alignment "
                "was not inferred from the runtime router."
            ),
            {
                "expected_intent": None,
                "planned_intent": planned,
                "oracle_confidence": expectation.confidence,
                "oracle_similarity": expectation.similarity,
                "intent_dataset_id": expectation.dataset_id,
            },
            "not_evaluated",
        )
    valid = planned == expectation.intent
    return (
        valid,
        (
            f"Independent labeled oracle expected {expectation.intent}; planner selected "
            f"{planned or 'no intent'}."
        ),
        {
            "expected_intent": expectation.intent,
            "planned_intent": planned,
            "oracle_confidence": expectation.confidence,
            "oracle_case_id": expectation.case_id,
            "oracle_similarity": expectation.similarity,
            "intent_dataset_id": expectation.dataset_id,
        },
        None,
    )


def _validate_answer_relevance(
    expected_intent: str | None,
    result: dict[str, Any],
) -> tuple[bool, str, dict[str, Any], str | None]:
    answer_type = str(result.get("answer_type") or "")
    system_answer = (
        result.get("system_answer") if isinstance(result.get("system_answer"), dict) else {}
    )
    has_ranking = isinstance(result.get("ranking"), dict)
    answer_validation = (
        result.get("answer_validation") if isinstance(result.get("answer_validation"), dict) else {}
    )
    if expected_intent is None:
        return (
            False,
            (
                "Question-answer semantic relevance was not scored because the question "
                "has no sufficiently similar labeled intent case."
            ),
            {
                "expected_intent": None,
                "answer_type": answer_type or "legacy_financial",
                "financial_ranking_present": has_ranking,
            },
            "not_evaluated",
        )
    if expected_intent == "coverage":
        available = system_answer.get("available_companies")
        coverage = result.get("coverage") if isinstance(result.get("coverage"), dict) else {}
        represented = _integer(coverage.get("represented_company_count"))
        valid = (
            answer_type == "coverage"
            and isinstance(available, list)
            and len(available) == represented
            and not has_ranking
            and answer_validation.get("passed") is True
        )
        detail = (
            f"Coverage question returned {len(available) if isinstance(available, list) else 0}/"
            f"{represented} represented companies without executing a ranking."
        )
    elif expected_intent in {"capabilities", "limitations"}:
        capabilities = system_answer.get("capabilities")
        limitations = system_answer.get("limitations")
        valid = (
            answer_type == "capabilities"
            and isinstance(capabilities, list)
            and bool(capabilities)
            and isinstance(limitations, list)
            and bool(limitations)
            and not has_ranking
            and answer_validation.get("passed") is True
        )
        detail = (
            "Capability question returned the versioned supported-operation and "
            "limitation contract."
            if valid
            else "Capability question did not return the required system contract."
        )
    else:
        ranking = result.get("ranking") if has_ranking else {}
        synthesis = result.get("synthesis")
        valid = (
            answer_type in {"", "financial_analysis"}
            and isinstance(ranking.get("rows"), list)
            and isinstance(synthesis, dict)
        )
        detail = (
            "Financial objective returned a structured ranking and synthesis."
            if valid
            else "Financial objective did not return a relevant ranking and synthesis."
        )
    return (
        valid,
        detail,
        {
            "expected_intent": expected_intent,
            "answer_type": answer_type or "legacy_financial",
            "financial_ranking_present": has_ranking,
        },
        None,
    )


def _validate_coverage(payload: Any) -> tuple[bool, str, dict[str, Any]]:
    coverage = payload if isinstance(payload, dict) else {}
    member_count = _integer(coverage.get("member_count"))
    represented = _integer(coverage.get("represented_company_count"))
    eligible = _integer(coverage.get("eligible_company_count"))
    excluded = _integer(coverage.get("excluded_company_count"))
    companies = coverage.get("companies") if isinstance(coverage.get("companies"), list) else []
    valid = (
        member_count > 0
        and len(companies) == member_count
        and 0 <= eligible <= represented <= member_count
        and excluded == member_count - eligible
    )
    metrics = {
        "member_count": member_count,
        "represented_company_count": represented,
        "eligible_company_count": eligible,
        "excluded_company_count": excluded,
    }
    return (
        valid,
        (
            f"{represented}/{member_count} members represented; {eligible} coverage-eligible; "
            f"{excluded} explicitly excluded."
        ),
        metrics,
    )


def _validate_tools(
    run: InvestigationRun,
    *,
    route_intent: str,
) -> tuple[bool, str, dict[str, Any]]:
    calls = run.result_payload.get("tool_calls")
    calls = calls if isinstance(calls, list) else []
    names = tuple(str(item.get("tool")) for item in calls if isinstance(item, dict))
    budget = _integer(run.request_payload.get("max_tool_calls"), default=8)
    plan_payload = run.result_payload.get("plan")
    plan = plan_payload if isinstance(plan_payload, dict) else {}
    if route_intent == "coverage":
        expected_tools = ("filings.get_coverage",)
    elif route_intent in {"capabilities", "limitations"}:
        expected_tools = ("filings.get_coverage", "agent.describe_capabilities")
    else:
        expected_tools = FINANCIAL_TOOL_SEQUENCE
    compare = calls[1] if len(calls) > 1 and isinstance(calls[1], dict) else {}
    arguments = compare.get("arguments") if isinstance(compare.get("arguments"), dict) else {}
    arguments_valid = route_intent in SYSTEM_INTENTS or all(
        arguments.get(key) == plan.get(key) for key in ("metric", "comparison", "scope")
    )
    valid = names == expected_tools and len(calls) <= budget and arguments_valid
    return (
        valid,
        (
            f"Called {' → '.join(names) if names else 'no tools'}; "
            f"{len(calls)}/{budget} budget used."
        ),
        {
            "called_tools": list(names),
            "expected_tools": list(expected_tools),
            "tool_call_count": len(calls),
            "max_tool_calls": budget,
            "arguments_match_plan": arguments_valid,
        },
    )


def _validate_retrieval(result: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    ranking = result.get("ranking") if isinstance(result.get("ranking"), dict) else {}
    rows = ranking.get("rows") if isinstance(ranking.get("rows"), list) else []
    plan = result.get("plan") if isinstance(result.get("plan"), dict) else {}
    comparison = str(plan.get("comparison") or "")
    math_valid = True
    periods_valid = True
    changes: list[Decimal] = []
    for row in rows:
        if not isinstance(row, dict):
            math_valid = periods_valid = False
            continue
        try:
            current = Decimal(str(row["current_value"]))
            previous = Decimal(str(row["comparison_value"]))
            observed = Decimal(str(row["percent_change"]))
            expected = ((current - previous) / abs(previous) * Decimal("100")).quantize(
                Decimal("0.01")
            )
            math_valid = math_valid and previous != 0 and observed == expected
            changes.append(observed)
            current_period = date.fromisoformat(str(row["current_period"]))
            previous_period = date.fromisoformat(str(row["comparison_period"]))
            if comparison == "yoy":
                periods_valid = (
                    periods_valid
                    and (
                        current_period.month,
                        current_period.day,
                    )
                    == (previous_period.month, previous_period.day)
                    and (current_period.year - previous_period.year == 1)
                )
            elif comparison == "qoq":
                periods_valid = (
                    periods_valid and 75 <= (current_period - previous_period).days <= 100
                )
            else:
                periods_valid = False
        except (KeyError, ValueError, InvalidOperation, ZeroDivisionError):
            math_valid = periods_valid = False
    sorted_valid = changes == sorted(changes, reverse=True)
    ranked_count = _integer(ranking.get("ranked_count"))
    eligible_count = _integer(ranking.get("eligible_count"))
    excluded_count = _integer(ranking.get("excluded_count"))
    exclusions = ranking.get("exclusions") if isinstance(ranking.get("exclusions"), list) else []
    valid = (
        bool(rows)
        and len(rows) == ranked_count
        and ranked_count <= eligible_count
        and len(exclusions) == excluded_count
        and math_valid
        and periods_valid
        and sorted_valid
    )
    return (
        valid,
        (
            f"Recomputed {len(rows)} ranked rows; sorting, {comparison.upper()} period pairing, "
            f"and percentage arithmetic {'match' if valid else 'contain defects'}."
        ),
        {
            "ranked_count": ranked_count,
            "eligible_count": eligible_count,
            "excluded_count": excluded_count,
            "math_valid": math_valid,
            "period_pairs_valid": periods_valid,
            "ranking_order_valid": sorted_valid,
        },
    )


def _validate_evidence(result: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    ranking = result.get("ranking") if isinstance(result.get("ranking"), dict) else {}
    rows = ranking.get("rows") if isinstance(ranking.get("rows"), list) else []
    citations = result.get("citations") if isinstance(result.get("citations"), list) else []
    by_id = {
        item.get("citation_id"): item
        for item in citations
        if isinstance(item, dict) and item.get("citation_id")
    }
    expected_ids = [
        citation_id
        for row in rows
        if isinstance(row, dict)
        for citation_id in row.get("citation_ids", [])
    ]
    resolved = all(citation_id in by_id for citation_id in expected_ids)
    exact = all(
        isinstance(citation.get("evidence"), list)
        and bool(citation["evidence"])
        and all(
            isinstance(item, dict)
            and len(str(item.get("source_hash") or "")) == 64
            and bool(item.get("evidence_id"))
            for item in citation["evidence"]
        )
        for citation in by_id.values()
    )
    fact_links = all(
        set(row.get("fact_ids", []))
        == {by_id[item].get("fact_id") for item in row.get("citation_ids", []) if item in by_id}
        for row in rows
        if isinstance(row, dict)
    )
    valid = (
        bool(expected_ids)
        and len(expected_ids) == len(set(expected_ids))
        and resolved
        and exact
        and fact_links
    )
    return (
        valid,
        (
            f"Resolved {len(by_id)}/{len(expected_ids)} citation references to versioned facts "
            f"and source-hash evidence."
        ),
        {
            "expected_citation_count": len(expected_ids),
            "resolved_citation_count": len(by_id),
            "unique_citation_ids": len(set(expected_ids)),
            "fact_links_valid": fact_links,
            "source_hashes_valid": exact,
        },
    )


def _validate_claims(result: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    validation = (
        result.get("claim_validation") if isinstance(result.get("claim_validation"), dict) else {}
    )
    ranking = result.get("ranking") if isinstance(result.get("ranking"), dict) else {}
    ranked = _integer(ranking.get("ranked_count"))
    valid_claims = _integer(validation.get("valid_claim_count"))
    rejected = _integer(validation.get("rejected_claim_count"))
    complete = _integer(validation.get("complete_row_count"))
    valid = (
        validation.get("passed") is True
        and valid_claims == ranked
        and complete == ranked
        and rejected == 0
    )
    return (
        valid,
        (
            f"{valid_claims}/{ranked} ranked claims passed deterministic citation validation; "
            f"{rejected} rejected."
        ),
        {
            "ranked_count": ranked,
            "valid_claim_count": valid_claims,
            "complete_row_count": complete,
            "rejected_claim_count": rejected,
        },
    )


def _intent_expectation(
    question: str,
    path: Path,
) -> IntentEvaluationExpectation:
    try:
        return evaluate_question_intent(question, path=path)
    except (FileNotFoundError, ValueError, ValidationError, OSError):
        return IntentEvaluationExpectation(
            intent=None,
            evaluated=False,
            confidence="none",
            dataset_id="intent-dataset-unavailable",
        )


def _intent_routing_suite(path: Path) -> InvestigationEvaluationSuite:
    try:
        dataset = load_intent_evaluation_dataset(path)
    except (FileNotFoundError, ValueError, ValidationError, OSError) as exc:
        return InvestigationEvaluationSuite(
            suite_id="intent_routing_regression",
            label="Locked intent-routing regression set",
            status="failed",
            score=0,
            summary=f"Intent-routing dataset could not be loaded: {exc}",
            metrics={"dataset_path": str(path)},
        )

    defects = []
    for case in dataset.cases:
        decision = decide_investigation_intent(case.utterance)
        if decision.intent != case.expected_intent or not decision.enforce:
            defects.append(
                {
                    "case_id": case.case_id,
                    "expected_intent": case.expected_intent,
                    "observed_intent": decision.intent,
                    "policy_rule_id": decision.rule_id,
                    "policy_confidence": decision.confidence,
                }
            )
    passed = len(dataset.cases) - len(defects)
    score = round(passed / len(dataset.cases) * 100, 2)
    check = _check(
        "intent_dataset_accuracy",
        "Labeled intent routing accuracy",
        not defects,
        f"{passed}/{len(dataset.cases)} locked utterances routed correctly.",
        metrics={
            "dataset_id": dataset.dataset_id,
            "case_count": len(dataset.cases),
            "defect_count": len(defects),
            "defects": defects[:20],
        },
    )
    return InvestigationEvaluationSuite(
        suite_id="intent_routing_regression",
        label="Locked intent-routing regression set",
        status="passed" if not defects else "failed",
        score=score,
        summary=check.detail,
        checks=[check],
        metrics={
            "dataset_id": dataset.dataset_id,
            "case_count": len(dataset.cases),
            "defect_count": len(defects),
        },
    )


def _golden_suite(runtime: FilingRuntime, *, workspace_id: str) -> InvestigationEvaluationSuite:
    path = runtime.settings.filing_golden_dataset_path.expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.is_file():
        return InvestigationEvaluationSuite(
            suite_id="extraction_golden",
            label="Locked extraction golden set",
            status="not_evaluated",
            score=0,
            hard_gate=False,
            summary=f"Golden dataset is unavailable at {path}.",
        )
    try:
        dataset = load_golden_dataset(path)
        report = evaluate_golden_dataset(
            runtime.store,
            workspace_id=workspace_id,
            dataset=dataset,
        )
    except (FileNotFoundError, ValueError) as exc:
        return InvestigationEvaluationSuite(
            suite_id="extraction_golden",
            label="Locked extraction golden set",
            status="failed",
            score=0,
            summary=f"Golden evaluation could not run: {exc}",
        )
    expected = report.expected_fact_count
    score = round(report.evidence_correct_count / expected * 100, 2) if expected else 0.0
    check = _check(
        "locked_facts",
        "Locked values and evidence",
        report.passed,
        (
            f"{report.value_correct_count}/{expected} values and "
            f"{report.evidence_correct_count}/{expected} evidence references match."
        ),
        metrics=report.model_dump(mode="json", exclude={"defects"}),
    )
    return InvestigationEvaluationSuite(
        suite_id="extraction_golden",
        label="Locked extraction golden set",
        status="passed" if report.passed else "failed",
        score=score,
        summary=check.detail,
        checks=[check],
        metrics={"defect_count": len(report.defects), "dataset_id": report.dataset_id},
    )


def _suite(
    suite_id: str,
    label: str,
    checks: Iterable[InvestigationQualityCheck],
    *,
    metrics: dict[str, Any] | None = None,
) -> InvestigationEvaluationSuite:
    items = list(checks)
    passed = sum(item.status == "passed" for item in items)
    score = round(passed / len(items) * 100, 2) if items else 0.0
    status = "passed" if items and passed == len(items) else "failed"
    return InvestigationEvaluationSuite(
        suite_id=suite_id,
        label=label,
        status=status,
        score=score,
        summary=f"{passed}/{len(items)} checks passed.",
        checks=items,
        metrics=metrics or {},
    )


def _check(
    check_id: str,
    label: str,
    passed: bool,
    detail: str,
    *,
    status: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> InvestigationQualityCheck:
    return InvestigationQualityCheck(
        check_id=check_id,
        label=label,
        status=status or ("passed" if passed else "failed"),
        detail=detail,
        metrics=metrics or {},
    )


def _integer(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
