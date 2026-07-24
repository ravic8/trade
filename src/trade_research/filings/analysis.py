from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import uuid4

from trade_research.config import Settings
from trade_research.filings.models import (
    AnalysisCitation,
    AnalysisQueryRequest,
    AnalysisQueryResponse,
    FinancialFact,
    PeriodType,
)
from trade_research.filings.store import FilingStore
from trade_research.filings.telemetry import current_trace_id, operation_span

METRIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "revenue": ("revenue", "sales", "top line"),
    "net_profit": ("net profit", "profit after tax", "pat"),
    "profit_before_tax": ("profit before tax", "pbt"),
    "basic_eps": ("basic eps", "earnings per share", "eps"),
    "diluted_eps": ("diluted eps",),
    "employee_benefit_expense": ("employee cost", "employee expense"),
    "total_expenses": ("total expenses", "expenses"),
    "tax_expense": ("tax expense", "tax"),
    "total_assets": ("total assets", "assets"),
    "total_equity": ("total equity", "equity"),
    "cash_and_cash_equivalents": ("cash and cash equivalents", "cash"),
    "cash_flow_from_operations": ("operating cash flow", "cash flow from operations"),
}


class FinancialAnalysisService:
    def __init__(self, *, settings: Settings, store: FilingStore) -> None:
        self.settings = settings
        self.store = store

    def answer(
        self,
        *,
        workspace_id: str,
        request: AnalysisQueryRequest,
    ) -> AnalysisQueryResponse:
        analysis_id = str(uuid4())
        tool_calls: list[dict[str, Any]] = []
        warnings: list[str] = []
        citations: list[AnalysisCitation] = []
        with operation_span(
            self.settings,
            "financial.analysis.question",
            observation_type="agent",
            metadata={
                "analysis_id": analysis_id,
                "workspace_id": workspace_id,
                "company_id": request.company_id,
                "strict_evidence": request.strict_evidence,
                "max_tool_calls": request.max_tool_calls,
            },
        ):
            metrics = _classify_metrics(request.question)
            if not metrics:
                response = self._abstention(
                    analysis_id,
                    "The question does not map to an approved M1 financial metric.",
                    tool_calls,
                    warnings,
                )
                self._record(workspace_id, request, response)
                return response

            with operation_span(
                self.settings,
                "financial.analysis.approved_facts",
                observation_type="tool",
                metadata={"company_id": request.company_id, "metrics": metrics},
            ):
                facts = self.store.approved_facts(
                    workspace_id=workspace_id,
                    company_id=request.company_id,
                    metrics=metrics,
                    current_only=True,
                    limit=100,
                )
            tool_calls.append(
                {
                    "tool": "approved_financial_facts_sql",
                    "arguments": {
                        "company_id": request.company_id,
                        "metrics": metrics,
                        "current_only": True,
                    },
                    "result_count": len(facts),
                }
            )
            if len(tool_calls) > request.max_tool_calls:
                raise ValueError("analysis tool budget exhausted")
            facts = _select_facts(facts, request.question)
            if not facts:
                response = self._abstention(
                    analysis_id,
                    "No approved facts matched the requested metric and period.",
                    tool_calls,
                    warnings,
                )
                self._record(workspace_id, request, response)
                return response

            valid_facts: list[FinancialFact] = []
            with operation_span(
                self.settings,
                "financial.analysis.evidence",
                observation_type="retriever",
                metadata={"fact_count": len(facts)},
            ):
                for fact in facts:
                    evidence = self.store.evidence(
                        workspace_id=workspace_id,
                        evidence_ids=fact.evidence_ids,
                    )
                    if len(evidence) != len(set(fact.evidence_ids)):
                        warnings.append(f"Evidence is incomplete for fact {fact.fact_id}.")
                        if request.strict_evidence:
                            continue
                    valid_facts.append(fact)
                    citations.append(
                        AnalysisCitation(
                            citation_id=f"c{len(citations) + 1}",
                            fact_id=fact.fact_id,
                            evidence_ids=fact.evidence_ids,
                            label=(
                                f"{fact.canonical_metric} - "
                                f"{fact.period_end.isoformat()} "
                                f"({fact.consolidation_scope.value})"
                            ),
                            filing_id=fact.source_filing_id,
                            filing_version=fact.source_filing_version,
                            period_end=fact.period_end,
                        )
                    )
            tool_calls.append(
                {
                    "tool": "filing_evidence_resolver",
                    "arguments": {"fact_ids": [fact.fact_id for fact in facts]},
                    "result_count": len(valid_facts),
                }
            )
            if len(tool_calls) > request.max_tool_calls:
                raise ValueError("analysis tool budget exhausted")
            if not valid_facts:
                response = self._abstention(
                    analysis_id,
                    "Approved values exist, but their evidence could not be resolved.",
                    tool_calls,
                    warnings,
                )
                self._record(workspace_id, request, response)
                return response

            answer_lines = [_format_fact(fact, index + 1) for index, fact in enumerate(valid_facts)]
            if _needs_change(request.question) and len(valid_facts) >= 2:
                first, second = valid_facts[0], valid_facts[1]
                if first.canonical_metric == second.canonical_metric and second.value != 0:
                    change = (first.value - second.value) / abs(second.value) * Decimal("100")
                    answer_lines.append(
                        f"Latest sequential change: {change.quantize(Decimal('0.01'))}% "
                        f"from {second.period_end.isoformat()} to {first.period_end.isoformat()}."
                    )
                    tool_calls.append(
                        {
                            "tool": "deterministic_variance_calculator",
                            "arguments": {
                                "latest_fact_id": first.fact_id,
                                "comparison_fact_id": second.fact_id,
                            },
                            "result": {"percent_change": str(change)},
                        }
                    )
            if "why" in request.question.lower():
                warnings.append(
                    "M1 numerical facts are available, but causal management commentary "
                    "requires an approved management claim."
                )
            status = "partial" if warnings else "answered"
            response = AnalysisQueryResponse(
                analysis_id=analysis_id,
                answer="\n".join(answer_lines),
                status=status,
                citations=citations,
                tool_calls=tool_calls,
                warnings=list(dict.fromkeys(warnings)),
                trace_id=current_trace_id(),
            )
            self._record(workspace_id, request, response)
            return response

    def _abstention(
        self,
        analysis_id: str,
        message: str,
        tool_calls: list[dict[str, Any]],
        warnings: list[str],
    ) -> AnalysisQueryResponse:
        return AnalysisQueryResponse(
            analysis_id=analysis_id,
            answer=f"I cannot answer this from approved filing evidence. {message}",
            status="abstained",
            citations=[],
            tool_calls=tool_calls,
            warnings=warnings,
            trace_id=current_trace_id(),
        )

    def _record(
        self,
        workspace_id: str,
        request: AnalysisQueryRequest,
        response: AnalysisQueryResponse,
    ) -> None:
        self.store.record_analysis(
            analysis_id=response.analysis_id,
            workspace_id=workspace_id,
            company_id=request.company_id,
            question=request.question,
            status=response.status,
            answer=response.answer,
            citations=[item.model_dump(mode="json") for item in response.citations],
            tool_calls=response.tool_calls,
            warnings=response.warnings,
            trace_id=response.trace_id,
        )


def _classify_metrics(question: str) -> list[str]:
    lowered = question.lower()
    matches = [
        metric
        for metric, keywords in METRIC_KEYWORDS.items()
        if any(keyword in lowered for keyword in keywords)
    ]
    return list(dict.fromkeys(matches))


def _select_facts(facts: list[FinancialFact], question: str) -> list[FinancialFact]:
    lowered = question.lower()
    consolidated = [
        fact for fact in facts if fact.consolidation_scope.value == "consolidated"
    ]
    selected = consolidated or facts
    if any(term in lowered for term in ("quarter", "qoq", "last four")):
        quarters = [fact for fact in selected if fact.period_type == PeriodType.QUARTER]
        if quarters:
            selected = quarters
    limit = 4 if "four" in lowered or "trend" in lowered else 2
    return selected[:limit]


def _needs_change(question: str) -> bool:
    lowered = question.lower()
    return any(term in lowered for term in ("change", "growth", "trend", "compare", "qoq"))


def _format_fact(fact: FinancialFact, citation_number: int) -> str:
    if fact.currency == "INR" and fact.canonical_metric not in {"basic_eps", "diluted_eps"}:
        crores = fact.value / Decimal("10000000")
        value = f"INR {crores.quantize(Decimal('0.01'))} crore"
    elif fact.currency:
        value = f"{fact.currency} {fact.value}"
    else:
        value = str(fact.value)
    return (
        f"{fact.canonical_metric} for {fact.period_end.isoformat()} "
        f"({fact.consolidation_scope.value}): {value} [c{citation_number}]"
    )
