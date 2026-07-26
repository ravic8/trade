from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from trade_research.config import Settings
from trade_research.filings.agent_llm import FilingAgentLLM, deterministic_synthesis
from trade_research.filings.agent_models import (
    SYSTEM_INTENTS,
    InvestigationPlan,
    InvestigationSynthesis,
)
from trade_research.filings.agent_tools import InvestigationToolGateway
from trade_research.filings.capabilities import (
    build_system_answer,
    validate_system_answer,
)
from trade_research.filings.models import InvestigationStatus
from trade_research.filings.store import FilingStore
from trade_research.filings.telemetry import current_trace_id, operation_span


class InvestigationGraphState(TypedDict, total=False):
    schema_version: int
    analysis_id: str
    thread_id: str
    workspace_id: str
    universe_id: str
    question: str
    strict_evidence: bool
    requested_comparison: str
    max_tool_calls: int
    tool_call_count: int
    plan: dict[str, Any]
    planner_telemetry: dict[str, Any]
    coverage: dict[str, Any]
    comparison: dict[str, Any]
    evidence: dict[str, Any]
    synthesis: dict[str, Any]
    claim_validation: dict[str, Any]
    system_answer: dict[str, Any]
    answer_validation: dict[str, Any]
    tool_calls: list[dict[str, Any]]
    final_status: str
    result: dict[str, Any]


@dataclass(frozen=True)
class InvestigationServices:
    settings: Settings
    store: FilingStore
    tools: InvestigationToolGateway
    llm: FilingAgentLLM


class InvestigationWorkflow:
    def __init__(self, services: InvestigationServices, *, checkpointer: Any) -> None:
        self.services = services
        self.graph = self._build().compile(
            checkpointer=checkpointer,
            name="filing.market.investigation",
        )

    def invoke(self, analysis_id: str) -> dict[str, Any]:
        run = self.services.store.investigation(analysis_id)
        if not run:
            raise KeyError(f"filing investigation not found: {analysis_id}")
        request = run.request_payload
        result = self.graph.invoke(
            {
                "schema_version": 1,
                "analysis_id": run.analysis_id,
                "thread_id": run.thread_id,
                "workspace_id": run.workspace_id,
                "universe_id": run.universe_id,
                "question": run.question,
                "strict_evidence": bool(request.get("strict_evidence", True)),
                "requested_comparison": str(request.get("comparison", "auto")),
                "max_tool_calls": int(request.get("max_tool_calls", 8)),
                "tool_call_count": 0,
                "tool_calls": [],
            },
            config={
                "configurable": {"thread_id": run.thread_id},
                "metadata": {
                    "analysis_id": run.analysis_id,
                    "workspace_id": run.workspace_id,
                    "universe_id": run.universe_id,
                    "workflow": "filing.market.investigation",
                },
            },
        )
        return dict(result)

    def _build(self) -> StateGraph:
        builder = StateGraph(InvestigationGraphState)
        builder.add_node("plan", self._plan)
        builder.add_node("resolve_universe", self._resolve_universe)
        builder.add_node("compare", self._compare)
        builder.add_node("resolve_evidence", self._resolve_evidence)
        builder.add_node("synthesize", self._synthesize)
        builder.add_node("validate_claims", self._validate_claims)
        builder.add_node("finalize", self._finalize)
        builder.add_node("answer_system_question", self._answer_system_question)
        builder.add_node("validate_system_answer", self._validate_system_answer)
        builder.add_node("finalize_system_answer", self._finalize_system_answer)
        builder.add_edge(START, "plan")
        builder.add_edge("plan", "resolve_universe")
        builder.add_conditional_edges(
            "resolve_universe",
            self._route_after_universe,
            {
                "financial": "compare",
                "system": "answer_system_question",
            },
        )
        builder.add_edge("compare", "resolve_evidence")
        builder.add_edge("resolve_evidence", "synthesize")
        builder.add_edge("synthesize", "validate_claims")
        builder.add_edge("validate_claims", "finalize")
        builder.add_edge("finalize", END)
        builder.add_edge("answer_system_question", "validate_system_answer")
        builder.add_edge("validate_system_answer", "finalize_system_answer")
        builder.add_edge("finalize_system_answer", END)
        return builder

    @staticmethod
    def _route_after_universe(state: InvestigationGraphState) -> str:
        intent = str(state.get("plan", {}).get("intent") or "")
        return "system" if intent in SYSTEM_INTENTS else "financial"

    def _plan(self, state: InvestigationGraphState) -> dict[str, Any]:
        self._progress(state, "plan", 0.12, {"status": "running"})
        with operation_span(
            self.services.settings,
            "filing.investigation.plan",
            observation_type="generation",
            metadata={
                "analysis_id": state["analysis_id"],
                "prompt_version": self.services.settings.filing_agent_prompt_version,
                "model": self.services.settings.filing_agent_llm_model,
            },
        ):
            plan, telemetry = self.services.llm.plan(
                question=state["question"],
                requested_comparison=state["requested_comparison"],
            )
        self._progress(
            state,
            "plan",
            0.18,
            {
                "status": "completed",
                "metric": plan.metric,
                "comparison": plan.comparison,
                "model_status": telemetry.get("status"),
            },
            plan_payload=plan.model_dump(mode="json"),
        )
        return {
            "plan": plan.model_dump(mode="json"),
            "planner_telemetry": telemetry,
        }

    def _resolve_universe(self, state: InvestigationGraphState) -> dict[str, Any]:
        self._assert_tool_budget(state)
        coverage = self.services.tools.coverage(
            workspace_id=state["workspace_id"],
            universe_id=state["universe_id"],
        )
        tool_calls = [
            *state.get("tool_calls", []),
            {
                "tool": "filings.get_coverage",
                "arguments": {"universe_id": state["universe_id"]},
                "result_count": coverage.member_count,
            },
        ]
        self._progress(
            state,
            "resolve_universe",
            0.32,
            {
                "member_count": coverage.member_count,
                "represented_company_count": coverage.represented_company_count,
                "eligible_company_count": coverage.eligible_company_count,
            },
            universe_snapshot_id=coverage.snapshot_id,
        )
        return {
            "coverage": coverage.model_dump(mode="json"),
            "tool_call_count": state.get("tool_call_count", 0) + 1,
            "tool_calls": tool_calls,
        }

    def _answer_system_question(
        self,
        state: InvestigationGraphState,
    ) -> dict[str, Any]:
        plan = InvestigationPlan.model_validate(state["plan"])
        tool_calls = list(state["tool_calls"])
        tool_call_count = state["tool_call_count"]
        if plan.intent != "coverage":
            self._assert_tool_budget(state)
            tool_calls.append(
                {
                    "tool": "agent.describe_capabilities",
                    "arguments": {
                        "contract": "live",
                        "universe_id": state["universe_id"],
                    },
                    "result_count": 1,
                }
            )
            tool_call_count += 1
        answer = build_system_answer(
            intent=plan.intent,
            coverage=state["coverage"],
            settings=self.services.settings,
        )
        self._progress(
            state,
            "answer_system_question",
            0.76,
            {
                "intent": plan.intent,
                "answer_type": answer["answer_type"],
                "represented_company_count": state["coverage"].get(
                    "represented_company_count", 0
                ),
            },
        )
        return {
            "system_answer": answer,
            "tool_calls": tool_calls,
            "tool_call_count": tool_call_count,
        }

    def _validate_system_answer(
        self,
        state: InvestigationGraphState,
    ) -> dict[str, Any]:
        validation = validate_system_answer(
            intent=str(state["plan"]["intent"]),
            answer=state["system_answer"],
            coverage=state["coverage"],
        )
        self._progress(state, "validate_system_answer", 0.92, validation)
        return {"answer_validation": validation}

    def _finalize_system_answer(
        self,
        state: InvestigationGraphState,
    ) -> dict[str, Any]:
        passed = state["answer_validation"]["passed"] is True
        status = InvestigationStatus.COMPLETED if passed else InvestigationStatus.FAILED
        result = {
            "answer_type": state["system_answer"]["answer_type"],
            "plan": state["plan"],
            "planner_telemetry": state["planner_telemetry"],
            "coverage": state["coverage"],
            "system_answer": state["system_answer"],
            "answer_validation": state["answer_validation"],
            "tool_calls": state["tool_calls"],
            "prompt_version": self.services.settings.filing_agent_prompt_version,
        }
        self.services.store.transition_investigation(
            state["analysis_id"],
            status=status,
            current_node="completed",
            progress=1.0,
            detail={
                "status": status.value,
                "answer_type": result["answer_type"],
                "answer_valid": passed,
            },
            result_payload=result,
            trace_id=current_trace_id(),
        )
        return {"final_status": status.value, "result": result}

    def _compare(self, state: InvestigationGraphState) -> dict[str, Any]:
        self._assert_tool_budget(state)
        plan = InvestigationPlan.model_validate(state["plan"])
        from trade_research.filings.models import FilingUniverseCoverage

        coverage = FilingUniverseCoverage.model_validate(state["coverage"])
        comparison = self.services.tools.compare(
            workspace_id=state["workspace_id"],
            coverage=coverage,
            plan=plan,
        )
        tool_calls = [
            *state["tool_calls"],
            {
                "tool": "financial_facts.compare",
                "arguments": {
                    "metric": plan.metric,
                    "comparison": plan.comparison,
                    "scope": plan.scope,
                },
                "result_count": comparison["eligible_count"],
            },
        ]
        self._progress(
            state,
            "compare",
            0.52,
            {
                "eligible_count": comparison["eligible_count"],
                "excluded_count": comparison["excluded_count"],
            },
        )
        return {
            "comparison": comparison,
            "tool_call_count": state["tool_call_count"] + 1,
            "tool_calls": tool_calls,
        }

    def _resolve_evidence(self, state: InvestigationGraphState) -> dict[str, Any]:
        self._assert_tool_budget(state)
        evidence = self.services.tools.resolve_evidence(
            workspace_id=state["workspace_id"],
            comparison=state["comparison"],
        )
        tool_calls = [
            *state["tool_calls"],
            {
                "tool": "filing_evidence.resolve",
                "arguments": {"citation_ids": state["comparison"]["allowed_citation_ids"]},
                "result_count": len(evidence["citations"]),
            },
        ]
        self._progress(
            state,
            "resolve_evidence",
            0.68,
            {
                "citation_count": len(evidence["citations"]),
                "missing_count": len(evidence["missing_citation_ids"]),
            },
        )
        return {
            "evidence": evidence,
            "tool_call_count": state["tool_call_count"] + 1,
            "tool_calls": tool_calls,
        }

    def _synthesize(self, state: InvestigationGraphState) -> dict[str, Any]:
        plan = InvestigationPlan.model_validate(state["plan"])
        with operation_span(
            self.services.settings,
            "filing.investigation.synthesize",
            observation_type="generation",
            metadata={
                "analysis_id": state["analysis_id"],
                "prompt_version": self.services.settings.filing_agent_prompt_version,
                "model": self.services.settings.filing_agent_llm_model,
                "ranked_count": state["comparison"]["ranked_count"],
            },
        ):
            synthesis = self.services.llm.synthesize(
                question=state["question"],
                plan=plan,
                comparison=state["comparison"],
                coverage={
                    key: state["coverage"][key]
                    for key in (
                        "member_count",
                        "represented_company_count",
                        "eligible_company_count",
                        "excluded_company_count",
                    )
                },
            )
        self._progress(
            state,
            "synthesize",
            0.82,
            {
                "claim_count": len(synthesis.claims),
                "model_used": synthesis.model_used,
            },
        )
        return {"synthesis": synthesis.model_dump(mode="json")}

    def _validate_claims(self, state: InvestigationGraphState) -> dict[str, Any]:
        synthesis = InvestigationSynthesis.model_validate(state["synthesis"])
        plan = InvestigationPlan.model_validate(state["plan"])
        canonical = deterministic_synthesis(
            plan=plan,
            comparison=state["comparison"],
            coverage=state["coverage"],
        )
        canonical_by_citations = {
            frozenset(claim.citation_ids): claim
            for claim in canonical.claims
        }
        resolved_ids = {item["citation_id"] for item in state["evidence"]["citations"]}
        valid_claims = []
        rejected_claims = []
        candidate_claims = synthesis.claims or canonical.claims
        for claim in candidate_claims:
            citation_ids = set(claim.citation_ids)
            canonical_claim = canonical_by_citations.get(frozenset(citation_ids))
            if (
                canonical_claim is not None
                and citation_ids
                and citation_ids.issubset(resolved_ids)
            ):
                # Model prose is not authoritative. Re-render the statement
                # from the deterministic row selected by its citation pair.
                valid_claims.append(canonical_claim.model_dump(mode="json"))
            else:
                rejected_claims.append(
                    {
                        **claim.model_dump(mode="json"),
                        "reason_code": "unresolved_or_missing_citation",
                    }
                )
        complete_rows = [
            row
            for row in state["comparison"]["rows"]
            if set(row["citation_ids"]).issubset(resolved_ids)
        ]
        validation = {
            "passed": not rejected_claims
            and (state["evidence"]["complete"] or not state.get("strict_evidence", True)),
            "valid_claim_count": len(valid_claims),
            "rejected_claim_count": len(rejected_claims),
            "rejected_claims": rejected_claims,
            "complete_row_count": len(complete_rows),
            "canonicalized_claims": len(valid_claims),
            "fallback_claims_used": not synthesis.claims,
        }
        synthesis_payload = {
            **state["synthesis"],
            "title": canonical.title,
            "summary": canonical.summary,
            "claims": valid_claims,
            "limitations": canonical.limitations,
        }
        comparison_payload = {
            **state["comparison"],
            "rows": complete_rows,
            "ranked_count": len(complete_rows),
        }
        self._progress(state, "validate_claims", 0.92, validation)
        return {
            "claim_validation": validation,
            "synthesis": synthesis_payload,
            "comparison": comparison_payload,
        }

    def _finalize(self, state: InvestigationGraphState) -> dict[str, Any]:
        rows = state["comparison"]["rows"]
        if not rows:
            status = InvestigationStatus.ABSTAINED
        elif state["claim_validation"]["passed"]:
            status = InvestigationStatus.COMPLETED
        else:
            status = InvestigationStatus.PARTIAL
        result = {
            "answer_type": "financial_analysis",
            "plan": state["plan"],
            "planner_telemetry": state["planner_telemetry"],
            "coverage": state["coverage"],
            "ranking": {
                key: state["comparison"][key]
                for key in (
                    "rows",
                    "eligible_count",
                    "ranked_count",
                    "excluded_count",
                    "exclusions",
                )
            },
            "synthesis": state["synthesis"],
            "citations": state["evidence"]["citations"],
            "claim_validation": state["claim_validation"],
            "tool_calls": state["tool_calls"],
            "prompt_version": self.services.settings.filing_agent_prompt_version,
        }
        self.services.store.transition_investigation(
            state["analysis_id"],
            status=status,
            current_node="completed",
            progress=1.0,
            detail={
                "status": status.value,
                "ranked_count": len(rows),
                "citation_count": len(state["evidence"]["citations"]),
            },
            result_payload=result,
            trace_id=current_trace_id(),
        )
        return {
            "final_status": status.value,
            "result": result,
        }

    def _assert_tool_budget(self, state: InvestigationGraphState) -> None:
        if state.get("tool_call_count", 0) >= state["max_tool_calls"]:
            raise RuntimeError("filing investigation tool budget exhausted")

    def _progress(
        self,
        state: InvestigationGraphState,
        node: str,
        progress: float,
        detail: dict[str, Any],
        *,
        universe_snapshot_id: str | None = None,
        plan_payload: dict[str, Any] | None = None,
    ) -> None:
        self.services.store.transition_investigation(
            state["analysis_id"],
            status=InvestigationStatus.RUNNING,
            current_node=node,
            progress=progress,
            detail=detail,
            universe_snapshot_id=universe_snapshot_id,
            plan_payload=plan_payload,
            trace_id=current_trace_id(),
        )
