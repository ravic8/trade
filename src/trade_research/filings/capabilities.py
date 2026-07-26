from __future__ import annotations

from typing import Any

from trade_research.config import Settings

CAPABILITY_CONTRACT_VERSION = "lens-filing-capabilities-v2"

SUPPORTED_METRICS = [
    "revenue",
    "net_profit",
    "profit_before_tax",
    "basic_eps",
    "diluted_eps",
]


def build_system_answer(
    *,
    intent: str,
    coverage: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    companies = list(coverage.get("companies") or [])
    represented = [
        company for company in companies if int(company.get("approved_fact_count") or 0) > 0
    ]
    eligible = [company for company in companies if company.get("status") == "eligible"]
    unavailable = [
        company for company in companies if int(company.get("approved_fact_count") or 0) == 0
    ]
    insufficient = [
        company
        for company in companies
        if int(company.get("approved_fact_count") or 0) > 0 and company.get("status") != "eligible"
    ]
    coverage_summary = {
        "universe_id": coverage.get("universe_id"),
        "snapshot_id": coverage.get("snapshot_id"),
        "member_count": len(companies),
        "represented_company_count": len(represented),
        "eligible_company_count": len(eligible),
        "unavailable_company_count": len(unavailable),
        "insufficient_history_count": len(insufficient),
    }
    if intent == "coverage":
        return {
            "contract_version": CAPABILITY_CONTRACT_VERSION,
            "answer_type": "coverage",
            "title": "Nifty 50 filing-data coverage",
            "summary": (
                f"Approved filing facts are available for {len(represented)} of "
                f"{len(companies)} Nifty 50 companies; {len(eligible)} currently have "
                "enough history for comparative analysis."
            ),
            "coverage": coverage_summary,
            "available_companies": represented,
            "analysis_eligible_companies": eligible,
            "insufficient_history_companies": insufficient,
            "unavailable_companies": unavailable,
        }

    limitations = [
        "The active universe is NIFTY50 on NSE; arbitrary companies and other "
        "exchanges are not yet supported by this workflow.",
        (
            f"Approved core facts are represented for {len(represented)} of "
            f"{len(companies)} current universe members, and comparative analysis is "
            f"eligible for {len(eligible)}."
        ),
        "Analysis is limited to consolidated quarterly approved core metrics; it "
        "does not yet answer qualitative annual-report, notes, proxy, or "
        "earnings-call narrative questions.",
        "It does not predict prices, recommend trades, or provide investment advice.",
    ]
    if not settings.filing_index_enabled:
        limitations.append(
            "Semantic/vector document retrieval is disabled; this workflow uses "
            "typed structured fact retrieval."
        )
    return {
        "contract_version": CAPABILITY_CONTRACT_VERSION,
        "answer_type": "capabilities",
        "title": "Current capabilities and limitations",
        "summary": (
            "Lens is a bounded filing-investigation agent: the model plans and "
            "summarizes, while allowlisted tools retrieve approved facts, perform "
            "deterministic comparisons, and resolve exact filing evidence."
        ),
        "coverage": coverage_summary,
        "supported_universes": [
            {
                "universe_id": "NIFTY50",
                "exchange": "NSE",
                "member_count": len(companies),
            }
        ],
        "supported_analysis": {
            "metrics": SUPPORTED_METRICS,
            "comparisons": ["yoy", "qoq"],
            "scope": ["consolidated"],
            "periodicity": ["quarterly"],
        },
        "capabilities": [
            {
                "id": "coverage_discovery",
                "label": "Live universe coverage",
                "detail": (
                    "Lists represented, comparison-eligible, and unavailable "
                    "companies without hiding exclusions."
                ),
            },
            {
                "id": "bounded_financial_comparison",
                "label": "Bounded financial comparison",
                "detail": (
                    "Ranks or compares approved core financial facts using "
                    "deterministic YoY or QoQ arithmetic."
                ),
            },
            {
                "id": "exact_evidence",
                "label": "Exact filing evidence",
                "detail": (
                    "Links every financial result to versioned facts and "
                    "source-hash evidence."
                ),
            },
            {
                "id": "durable_execution",
                "label": "Durable agent execution",
                "detail": (
                    "Uses checkpointed LangGraph execution, bounded tools, review "
                    "interrupts, telemetry, and production drills."
                ),
            },
            {
                "id": "quality_gates",
                "label": "Runtime validation and evaluation",
                "detail": (
                    "Checks intent alignment, answer relevance, tool policy, "
                    "retrieval arithmetic, evidence, and locked extraction facts."
                ),
            },
        ],
        "limitations": limitations,
        "runtime": {
            "llm_enabled": settings.filing_agent_llm_enabled,
            "llm_provider": settings.filing_agent_llm_provider,
            "llm_model": settings.filing_agent_llm_model,
            "semantic_index_enabled": settings.filing_index_enabled,
            "langfuse_enabled": settings.langfuse_enabled,
            "otel_enabled": settings.otel_enabled,
        },
    }


def validate_system_answer(
    *,
    intent: str,
    answer: dict[str, Any],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    companies = list(coverage.get("companies") or [])
    represented = sum(int(item.get("approved_fact_count") or 0) > 0 for item in companies)
    if intent == "coverage":
        returned = list(answer.get("available_companies") or [])
        unavailable = list(answer.get("unavailable_companies") or [])
        expected_ids = {
            str(item.get("company_id")) for item in companies if isinstance(item, dict)
        }
        returned_ids = {
            str(item.get("company_id"))
            for item in [*returned, *unavailable]
            if isinstance(item, dict)
        }
        valid = (
            answer.get("answer_type") == "coverage"
            and len(returned) == represented
            and len(unavailable) + represented == len(companies)
            and returned_ids == expected_ids
        )
        detail = (
            f"Returned all {represented} represented companies and accounted for "
            f"all {len(companies)} universe members."
        )
    else:
        valid = (
            answer.get("answer_type") == "capabilities"
            and bool(answer.get("capabilities"))
            and bool(answer.get("limitations"))
            and answer.get("contract_version") == CAPABILITY_CONTRACT_VERSION
        )
        detail = (
            "Returned the versioned capability contract with supported operations "
            "and current limitations."
        )
    return {"passed": valid, "intent": intent, "detail": detail}
