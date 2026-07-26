from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Literal

from trade_research.filings.agent_models import InvestigationPlan
from trade_research.filings.models import (
    ConsolidationScope,
    FilingCoverageCompany,
    FilingUniverseCoverage,
    FinancialFact,
    PeriodType,
)
from trade_research.filings.store import FilingStore

CORE_COVERAGE_METRICS = (
    "net_profit",
    "revenue",
    "basic_eps",
    "diluted_eps",
    "profit_before_tax",
)


class InvestigationToolGateway:
    """Allowlisted, read-only tools for the Nifty filing investigation graph."""

    def __init__(self, *, store: FilingStore) -> None:
        self.store = store

    def coverage(
        self,
        *,
        workspace_id: str,
        universe_id: str,
    ) -> FilingUniverseCoverage:
        snapshot = self.store.latest_universe_snapshot(
            workspace_id=workspace_id,
            universe_id=universe_id,
        )
        directory = self.store.filing_company_directory(workspace_id=workspace_id)
        members = (
            snapshot.members
            if snapshot is not None
            else sorted(directory.values(), key=lambda item: item["symbol"])
        )
        company_ids = [str(item["company_id"]) for item in members]
        facts = self.store.approved_facts_for_companies(
            workspace_id=workspace_id,
            company_ids=company_ids,
            metrics=CORE_COVERAGE_METRICS,
            limit=max(len(company_ids) * 100, 100),
        )
        by_company: dict[str, list[FinancialFact]] = defaultdict(list)
        for fact in facts:
            by_company[fact.company_id].append(fact)

        companies: list[FilingCoverageCompany] = []
        for member in members:
            company_id = str(member["company_id"])
            company_facts = by_company.get(company_id, [])
            periods = sorted({fact.period_end for fact in company_facts}, reverse=True)
            metrics = sorted({fact.canonical_metric for fact in company_facts})
            if not company_facts:
                company_status: Literal["eligible", "insufficient_history", "no_approved_facts"] = (
                    "no_approved_facts"
                )
                reasons = ["no_approved_facts"]
            elif len(periods) < 2:
                company_status = "insufficient_history"
                reasons = ["fewer_than_two_periods"]
            else:
                company_status = "eligible"
                reasons = []
            companies.append(
                FilingCoverageCompany(
                    company_id=company_id,
                    symbol=str(member["symbol"]),
                    name=str(member["name"]),
                    status=company_status,
                    approved_fact_count=len(company_facts),
                    available_periods=periods[:8],
                    available_metrics=metrics,
                    reason_codes=reasons,
                )
            )
        eligible = sum(item.status == "eligible" for item in companies)
        represented = sum(item.approved_fact_count > 0 for item in companies)
        return FilingUniverseCoverage(
            universe_id=universe_id,
            snapshot_id=snapshot.snapshot_id if snapshot else None,
            member_count=len(members),
            represented_company_count=represented,
            eligible_company_count=eligible,
            excluded_company_count=len(companies) - eligible,
            companies=companies,
        )

    def compare(
        self,
        *,
        workspace_id: str,
        coverage: FilingUniverseCoverage,
        plan: InvestigationPlan,
    ) -> dict[str, Any]:
        company_ids = [item.company_id for item in coverage.companies]
        facts = self.store.approved_facts_for_companies(
            workspace_id=workspace_id,
            company_ids=company_ids,
            metrics=[plan.metric],
            limit=max(len(company_ids) * 40, 100),
        )
        member_map = {item.company_id: item for item in coverage.companies}
        by_company: dict[str, list[FinancialFact]] = defaultdict(list)
        for fact in facts:
            if (
                fact.period_type == PeriodType.QUARTER
                and fact.consolidation_scope == ConsolidationScope.CONSOLIDATED
            ):
                by_company[fact.company_id].append(fact)

        rows: list[dict[str, Any]] = []
        exclusions: list[dict[str, Any]] = []
        citations: list[dict[str, Any]] = []
        for company_id in company_ids:
            company_facts = sorted(
                by_company.get(company_id, []),
                key=lambda fact: fact.period_end,
                reverse=True,
            )
            latest = _dedupe_periods(company_facts)
            if len(latest) < 2:
                exclusions.append(_exclusion(member_map[company_id], "insufficient_metric_history"))
                continue
            current = latest[0]
            comparison = _comparison_fact(current, latest[1:], plan.comparison)
            if comparison is None:
                exclusions.append(
                    _exclusion(
                        member_map[company_id],
                        f"no_{plan.comparison}_comparison_period",
                    )
                )
                continue
            if current.currency != comparison.currency:
                exclusions.append(_exclusion(member_map[company_id], "currency_mismatch"))
                continue
            current_value = current.value * current.unit_scale
            comparison_value = comparison.value * comparison.unit_scale
            if comparison_value == 0:
                exclusions.append(_exclusion(member_map[company_id], "zero_comparison_value"))
                continue
            percent_change = (
                (current_value - comparison_value) / abs(comparison_value) * Decimal("100")
            )
            current_citation = f"c{len(citations) + 1}"
            comparison_citation = f"c{len(citations) + 2}"
            citations.extend(
                [
                    _citation(current_citation, current),
                    _citation(comparison_citation, comparison),
                ]
            )
            member = member_map[company_id]
            rows.append(
                {
                    "company_id": company_id,
                    "symbol": member.symbol,
                    "name": member.name,
                    "metric": plan.metric,
                    "comparison": plan.comparison,
                    "current_period": current.period_end.isoformat(),
                    "comparison_period": comparison.period_end.isoformat(),
                    "current_value": str(current_value),
                    "comparison_value": str(comparison_value),
                    "currency": current.currency,
                    "percent_change": str(percent_change.quantize(Decimal("0.01"))),
                    "fact_ids": [current.fact_id, comparison.fact_id],
                    "citation_ids": [current_citation, comparison_citation],
                }
            )

        rows.sort(
            key=lambda item: Decimal(str(item["percent_change"])),
            reverse=True,
        )
        selected = rows[: plan.limit]
        selected_citations = {
            citation_id for row in selected for citation_id in row["citation_ids"]
        }
        citations = [item for item in citations if item["citation_id"] in selected_citations]
        return {
            "rows": selected,
            "eligible_count": len(rows),
            "ranked_count": len(selected),
            "excluded_count": len(exclusions),
            "exclusions": exclusions,
            "citations": citations,
            "allowed_citation_ids": sorted(selected_citations),
        }

    def resolve_evidence(
        self,
        *,
        workspace_id: str,
        comparison: dict[str, Any],
    ) -> dict[str, Any]:
        resolved: list[dict[str, Any]] = []
        missing: list[str] = []
        for citation in comparison["citations"]:
            evidence = self.store.evidence(
                workspace_id=workspace_id,
                evidence_ids=citation["evidence_ids"],
            )
            if len(evidence) != len(set(citation["evidence_ids"])):
                missing.append(citation["citation_id"])
                continue
            resolved.append(
                {
                    **citation,
                    "evidence": [item.model_dump(mode="json") for item in evidence],
                }
            )
        return {
            "citations": resolved,
            "missing_citation_ids": missing,
            "complete": not missing,
        }


def _dedupe_periods(facts: list[FinancialFact]) -> list[FinancialFact]:
    selected: dict[date, FinancialFact] = {}
    for fact in facts:
        selected.setdefault(fact.period_end, fact)
    return list(selected.values())


def _comparison_fact(
    current: FinancialFact,
    candidates: list[FinancialFact],
    comparison: str,
) -> FinancialFact | None:
    target_days = 365 if comparison == "yoy" else 91
    tolerance = 45 if comparison == "yoy" else 25
    matches = [
        (abs((current.period_end - fact.period_end).days - target_days), fact)
        for fact in candidates
        if abs((current.period_end - fact.period_end).days - target_days) <= tolerance
    ]
    return min(matches, key=lambda item: item[0])[1] if matches else None


def _citation(citation_id: str, fact: FinancialFact) -> dict[str, Any]:
    return {
        "citation_id": citation_id,
        "fact_id": fact.fact_id,
        "evidence_ids": list(fact.evidence_ids),
        "label": (f"{fact.company_id} {fact.canonical_metric} {fact.period_end.isoformat()}"),
        "company_id": fact.company_id,
        "filing_id": fact.source_filing_id,
        "filing_version": fact.source_filing_version,
        "period_end": fact.period_end.isoformat(),
    }


def _exclusion(company: FilingCoverageCompany, reason: str) -> dict[str, str]:
    return {
        "company_id": company.company_id,
        "symbol": company.symbol,
        "name": company.name,
        "reason_code": reason,
    }
