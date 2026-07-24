from __future__ import annotations

import re
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from trade_research.filings.models import (
    ConsolidationScope,
    EvidenceReference,
    IntelligenceObject,
    IntelligenceObjectType,
    ParsedDocument,
    ParsedXbrlContext,
    PeriodType,
    ReviewStatus,
)
from trade_research.filings.store import stable_id

EXTRACTOR_VERSION = "nse-filing-extractor-v1"


CONCEPT_TO_METRIC: dict[str, str] = {
    "RevenueFromOperations": "revenue",
    "Revenue": "revenue",
    "OtherIncome": "other_income",
    "Income": "total_income",
    "CostOfMaterialsConsumed": "cost_of_materials",
    "PurchasesOfStockInTrade": "purchases_stock_in_trade",
    "ChangesInInventoriesOfFinishedGoodsWorkInProgressAndStockInTrade": (
        "inventory_change"
    ),
    "EmployeeBenefitExpense": "employee_benefit_expense",
    "FinanceCosts": "finance_costs",
    "DepreciationDepletionAndAmortisationExpense": "depreciation_amortization",
    "OtherExpenses": "other_expenses",
    "Expenses": "total_expenses",
    "ProfitBeforeExceptionalItemsAndTax": "profit_before_exceptional_items_and_tax",
    "ExceptionalItemsBeforeTax": "exceptional_items",
    "ProfitBeforeTax": "profit_before_tax",
    "TaxExpense": "tax_expense",
    "ProfitLossForPeriodFromContinuingOperations": "net_profit_continuing",
    "ProfitLossForPeriod": "net_profit",
    "ProfitOrLossAttributableToOwnersOfParent": "net_profit_parent",
    "OtherComprehensiveIncomeNetOfTaxes": "other_comprehensive_income",
    "ComprehensiveIncomeForThePeriod": "comprehensive_income",
    "PaidUpValueOfEquityShareCapital": "equity_share_capital",
    "BasicEarningsLossPerShareFromContinuingOperations": "basic_eps",
    "DilutedEarningsLossPerShareFromContinuingOperations": "diluted_eps",
    "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "basic_eps_total",
    "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations": (
        "diluted_eps_total"
    ),
    "Assets": "total_assets",
    "TotalAssets": "total_assets",
    "Liabilities": "total_liabilities",
    "TotalLiabilities": "total_liabilities",
    "Equity": "total_equity",
    "EquityAttributableToOwnersOfParent": "parent_equity",
    "CashAndCashEquivalents": "cash_and_cash_equivalents",
    "CurrentAssets": "current_assets",
    "CurrentLiabilities": "current_liabilities",
    "NoncurrentAssets": "noncurrent_assets",
    "NoncurrentLiabilities": "noncurrent_liabilities",
    "NetCashFlowsFromUsedInOperatingActivities": "cash_flow_from_operations",
    "NetCashFlowsFromUsedInInvestingActivities": "cash_flow_from_investing",
    "NetCashFlowsFromUsedInFinancingActivities": "cash_flow_from_financing",
    "PaymentsToAcquirePropertyPlantAndEquipment": "capital_expenditure",
}


METRIC_SECTION: dict[str, str] = {
    "revenue": "income_statement",
    "other_income": "income_statement",
    "total_income": "income_statement",
    "cost_of_materials": "income_statement",
    "purchases_stock_in_trade": "income_statement",
    "inventory_change": "income_statement",
    "employee_benefit_expense": "income_statement",
    "finance_costs": "income_statement",
    "depreciation_amortization": "income_statement",
    "other_expenses": "income_statement",
    "total_expenses": "income_statement",
    "profit_before_exceptional_items_and_tax": "income_statement",
    "exceptional_items": "income_statement",
    "profit_before_tax": "income_statement",
    "tax_expense": "income_statement",
    "net_profit_continuing": "income_statement",
    "net_profit": "income_statement",
    "net_profit_parent": "income_statement",
    "other_comprehensive_income": "income_statement",
    "comprehensive_income": "income_statement",
    "basic_eps": "income_statement",
    "diluted_eps": "income_statement",
    "basic_eps_total": "income_statement",
    "diluted_eps_total": "income_statement",
    "equity_share_capital": "balance_sheet",
    "total_assets": "balance_sheet",
    "total_liabilities": "balance_sheet",
    "total_equity": "balance_sheet",
    "parent_equity": "balance_sheet",
    "cash_and_cash_equivalents": "balance_sheet",
    "current_assets": "balance_sheet",
    "current_liabilities": "balance_sheet",
    "noncurrent_assets": "balance_sheet",
    "noncurrent_liabilities": "balance_sheet",
    "cash_flow_from_operations": "cash_flow",
    "cash_flow_from_investing": "cash_flow",
    "cash_flow_from_financing": "cash_flow",
    "capital_expenditure": "cash_flow",
}


METADATA_CONCEPTS = {
    "NatureOfReportStandaloneConsolidated",
    "DateOfStartOfReportingPeriod",
    "DateOfEndOfReportingPeriod",
    "DescriptionOfPresentationCurrency",
    "LevelOfRoundingUsedInFinancialStatements",
}


def planned_sections(parsed: ParsedDocument) -> list[str]:
    if parsed.xbrl_facts:
        available = {
            METRIC_SECTION[metric]
            for fact in parsed.xbrl_facts
            if (metric := CONCEPT_TO_METRIC.get(fact.concept)) in METRIC_SECTION
        }
        return sorted(available) or ["financial"]
    return ["operational_guidance", "management_commentary"]


def extract_xbrl_financial_candidates(
    *,
    parsed: ParsedDocument,
    run_id: str,
    workspace_id: str,
    company_id: str,
    filing_id: str,
    filing_version: int,
    source_hash: str,
    default_scope: ConsolidationScope,
    section: str,
    extractor_version: str = EXTRACTOR_VERSION,
) -> tuple[list[EvidenceReference], list[dict[str, Any]]]:
    facts_by_context: dict[str, dict[str, str]] = defaultdict(dict)
    for fact in parsed.xbrl_facts:
        if fact.concept in METADATA_CONCEPTS:
            facts_by_context[fact.context_ref][fact.concept] = fact.value_text

    global_scope = default_scope
    for metadata in facts_by_context.values():
        if value := metadata.get("NatureOfReportStandaloneConsolidated"):
            global_scope = _scope(value)
            if global_scope != ConsolidationScope.UNKNOWN:
                break

    evidence: list[EvidenceReference] = []
    candidates: list[dict[str, Any]] = []
    for fact in parsed.xbrl_facts:
        metric = CONCEPT_TO_METRIC.get(fact.concept)
        if not metric:
            continue
        if section not in {"financial", METRIC_SECTION.get(metric)}:
            continue
        context = parsed.xbrl_contexts.get(fact.context_ref)
        if not context or context.dimensions:
            continue
        try:
            value = _decimal(fact.value_text)
        except InvalidOperation:
            continue
        period_start, period_end = _context_dates(context)
        if period_end is None:
            continue
        period_start = _normalize_legacy_nse_duration(
            context_ref=fact.context_ref,
            period_start=period_start,
            period_end=period_end,
        )
        scope = global_scope
        context_scope = facts_by_context.get(fact.context_ref, {}).get(
            "NatureOfReportStandaloneConsolidated"
        )
        if context_scope:
            scope = _scope(context_scope)
        currency = _currency(fact.unit_ref)
        evidence_id = stable_id(
            "filing-evidence",
            filing_id,
            filing_version,
            fact.concept,
            fact.context_ref,
            fact.value_text,
        )
        evidence.append(
            EvidenceReference(
                evidence_id=evidence_id,
                workspace_id=workspace_id,
                company_id=company_id,
                filing_id=filing_id,
                filing_version=filing_version,
                section_path=f"xbrl/{METRIC_SECTION.get(metric, 'financial')}",
                row_label=fact.concept,
                xbrl_concept=fact.concept,
                context_ref=fact.context_ref,
                source_hash=source_hash,
                snippet=f"{fact.concept}={fact.value_text}",
                effective_date=period_end,
            )
        )
        candidate_id = stable_id(
            "filing-candidate",
            filing_id,
            filing_version,
            metric,
            fact.context_ref,
            fact.value_text,
        )
        candidates.append(
            {
                "candidate_id": candidate_id,
                "run_id": run_id,
                "workspace_id": workspace_id,
                "company_id": company_id,
                "canonical_metric": metric,
                "reported_label": fact.concept,
                "value_decimal": str(value),
                "currency": currency,
                "unit_scale": "1",
                "period_start": period_start,
                "period_end": period_end,
                "period_type": infer_period_type(period_start, period_end, context.instant).value,
                "consolidation_scope": scope.value,
                "source_filing_id": filing_id,
                "source_filing_version": filing_version,
                "evidence_ids": [evidence_id],
                "confidence": 0.995
                if scope != ConsolidationScope.UNKNOWN
                else 0.98,
                "extractor_version": extractor_version,
                "prompt_version": None,
            }
        )
    return evidence, candidates


_PDF_PATTERNS: tuple[
    tuple[str, IntelligenceObjectType, re.Pattern[str], str | None, str | None],
    ...,
] = (
    (
        "revenue_growth_guidance",
        IntelligenceObjectType.GUIDANCE,
        re.compile(
            r"(?:revenue|sales)\s+(?:growth\s+)?guidance.{0,90}?"
            r"(-?\d+(?:\.\d+)?)\s*%\s*(?:to|-)\s*(-?\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE | re.DOTALL,
        ),
        None,
        "percent_range",
    ),
    (
        "operating_margin_guidance",
        IntelligenceObjectType.GUIDANCE,
        re.compile(
            r"(?:operating\s+)?margin\s+guidance.{0,80}?"
            r"(\d+(?:\.\d+)?)\s*%\s*(?:to|-)\s*(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE | re.DOTALL,
        ),
        None,
        "percent_range",
    ),
    (
        "attrition_rate",
        IntelligenceObjectType.OPERATIONAL_METRIC,
        re.compile(
            r"(?:voluntary\s+)?attrition(?:\s+rate)?.{0,45}?(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE,
        ),
        None,
        "percent",
    ),
    (
        "employee_count",
        IntelligenceObjectType.OPERATIONAL_METRIC,
        re.compile(
            r"(?:total\s+)?(?:employees|headcount).{0,45}?(\d{2,3}(?:,\d{3})+|\d{5,7})",
            re.IGNORECASE,
        ),
        None,
        "count",
    ),
    (
        "utilization",
        IntelligenceObjectType.OPERATIONAL_METRIC,
        re.compile(
            r"utili[sz]ation.{0,45}?(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE,
        ),
        None,
        "percent",
    ),
    (
        "large_deal_tcv",
        IntelligenceObjectType.OPERATIONAL_METRIC,
        re.compile(
            r"large\s+deal.{0,100}?(?:TCV|total\s+contract\s+value).{0,40}?"
            r"(?:US\s*)?\$\s*(\d+(?:\.\d+)?)\s*(billion|million|bn|mn)",
            re.IGNORECASE | re.DOTALL,
        ),
        "USD",
        None,
    ),
)


def extract_pdf_intelligence(
    *,
    parsed: ParsedDocument,
    run_id: str,
    workspace_id: str,
    company_id: str,
    filing_id: str,
    filing_version: int,
    source_hash: str,
    period_end: date | None,
    section: str,
    claim_limit: int,
    extractor_version: str = EXTRACTOR_VERSION,
) -> tuple[list[EvidenceReference], list[IntelligenceObject]]:
    evidence: list[EvidenceReference] = []
    objects: list[IntelligenceObject] = []
    seen: set[tuple[str, str]] = set()
    if section == "operational_guidance":
        for page in parsed.pages:
            normalized = " ".join(page.text.split())
            for name, object_type, pattern, currency, unit in _PDF_PATTERNS:
                for match in pattern.finditer(normalized):
                    matched = match.group(0)
                    key = (name, matched.lower())
                    if key in seen:
                        continue
                    seen.add(key)
                    value_decimal, value_text, resolved_unit = _pdf_match_value(
                        name, match, unit
                    )
                    evidence_id = stable_id(
                        "filing-evidence",
                        filing_id,
                        filing_version,
                        page.page,
                        name,
                        matched,
                    )
                    evidence.append(
                        EvidenceReference(
                            evidence_id=evidence_id,
                            workspace_id=workspace_id,
                            company_id=company_id,
                            filing_id=filing_id,
                            filing_version=filing_version,
                            page=page.page,
                            section_path="pdf/operational_guidance",
                            row_label=name,
                            source_hash=source_hash,
                            snippet=matched[:500],
                            effective_date=period_end,
                        )
                    )
                    object_id = stable_id(
                        "filing-intelligence-object",
                        filing_id,
                        filing_version,
                        page.page,
                        name,
                        matched,
                    )
                    objects.append(
                        IntelligenceObject(
                            object_id=object_id,
                            workspace_id=workspace_id,
                            company_id=company_id,
                            run_id=run_id,
                            object_type=object_type,
                            canonical_name=name,
                            reported_label=matched[:300],
                            value_decimal=value_decimal,
                            value_text=value_text,
                            currency=currency,
                            unit=resolved_unit,
                            period_end=period_end,
                            source_filing_id=filing_id,
                            source_filing_version=filing_version,
                            evidence_ids=[evidence_id],
                            confidence=0.78,
                            review_status=ReviewStatus.PENDING,
                            extractor_version=extractor_version,
                        )
                    )
    if section == "management_commentary":
        claim_count = 0
        for page in parsed.pages:
            for sentence in _sentences(page.text):
                lowered = sentence.lower()
                if not any(
                    keyword in lowered
                    for keyword in (
                        "guidance",
                        "large deal",
                        "attrition",
                        "artificial intelligence",
                        "generative ai",
                        "operating margin",
                    )
                ):
                    continue
                if len(sentence) < 35 or len(sentence) > 700:
                    continue
                key = ("management_claim", sentence.lower())
                if key in seen:
                    continue
                seen.add(key)
                evidence_id = stable_id(
                    "filing-evidence",
                    filing_id,
                    filing_version,
                    page.page,
                    "management_claim",
                    sentence,
                )
                evidence.append(
                    EvidenceReference(
                        evidence_id=evidence_id,
                        workspace_id=workspace_id,
                        company_id=company_id,
                        filing_id=filing_id,
                        filing_version=filing_version,
                        page=page.page,
                        section_path="pdf/management_commentary",
                        source_hash=source_hash,
                        snippet=sentence,
                        effective_date=period_end,
                    )
                )
                objects.append(
                    IntelligenceObject(
                        object_id=stable_id(
                            "filing-intelligence-object",
                            filing_id,
                            filing_version,
                            page.page,
                            "management_claim",
                            sentence,
                        ),
                        workspace_id=workspace_id,
                        company_id=company_id,
                        run_id=run_id,
                        object_type=IntelligenceObjectType.MANAGEMENT_CLAIM,
                        canonical_name="management_claim",
                        value_text=sentence,
                        period_end=period_end,
                        source_filing_id=filing_id,
                        source_filing_version=filing_version,
                        evidence_ids=[evidence_id],
                        confidence=0.70,
                        review_status=ReviewStatus.PENDING,
                        extractor_version=extractor_version,
                    )
                )
                claim_count += 1
                if claim_count >= claim_limit:
                    return evidence, objects
    return evidence, objects


def infer_period_type(
    period_start: date | None,
    period_end: date,
    instant: date | None = None,
) -> PeriodType:
    if instant is not None or period_start is None:
        return PeriodType.INSTANT
    days = (period_end - period_start).days + 1
    if 75 <= days <= 110:
        return PeriodType.QUARTER
    if 330 <= days <= 380:
        return PeriodType.ANNUAL
    if 111 <= days <= 329:
        return PeriodType.YEAR_TO_DATE
    return PeriodType.DURATION


def _context_dates(context: ParsedXbrlContext) -> tuple[date | None, date | None]:
    if context.instant:
        return None, context.instant
    return context.period_start, context.period_end


def _normalize_legacy_nse_duration(
    *,
    context_ref: str,
    period_start: date | None,
    period_end: date,
) -> date | None:
    """Correct a known legacy NSE Ind-AS template context defect.

    Older result instances publish both ``OneD`` (the quarter) and ``FourD``
    (fiscal year-to-date) with the quarter's start date in the XML context.
    The values and stable context convention identify ``FourD`` as YTD. Keep
    the original context in evidence while normalizing the candidate period.
    """

    if context_ref != "FourD" or period_start is None:
        return period_start
    fiscal_year_start = date(
        period_end.year if period_end.month >= 4 else period_end.year - 1,
        4,
        1,
    )
    if period_start != fiscal_year_start:
        return fiscal_year_start
    return period_start


def _scope(value: str) -> ConsolidationScope:
    normalized = value.strip().lower()
    if "non-consolidated" in normalized or "standalone" in normalized:
        return ConsolidationScope.STANDALONE
    if "consolidated" in normalized:
        return ConsolidationScope.CONSOLIDATED
    return ConsolidationScope.UNKNOWN


def _currency(unit_ref: str | None) -> str | None:
    if not unit_ref:
        return None
    normalized = unit_ref.upper()
    if "INR" in normalized:
        return "INR"
    if "USD" in normalized:
        return "USD"
    return unit_ref


def _decimal(value: str) -> Decimal:
    normalized = value.strip().replace(",", "").replace("(", "-").replace(")", "")
    return Decimal(normalized)


def _pdf_match_value(
    name: str,
    match: re.Match[str],
    unit: str | None,
) -> tuple[Decimal | None, str | None, str | None]:
    groups = match.groups()
    if name.endswith("_guidance"):
        return None, f"{groups[0]}%-{groups[1]}%", unit
    value = Decimal(groups[0].replace(",", ""))
    if name == "large_deal_tcv":
        magnitude = groups[1].lower()
        scale = Decimal("1000000000") if magnitude in {"billion", "bn"} else Decimal("1000000")
        return value * scale, None, "currency"
    return value, None, unit


def _sentences(text: str) -> list[str]:
    normalized = " ".join(text.split())
    return [
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", normalized)
        if sentence.strip()
    ]
