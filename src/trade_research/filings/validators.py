from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from trade_research.filings.models import (
    ConsolidationScope,
    ValidationDefect,
    ValidationSeverity,
    ValidationStatus,
)
from trade_research.filings.store import stable_id

MONETARY_METRICS = {
    "revenue",
    "other_income",
    "total_income",
    "employee_benefit_expense",
    "finance_costs",
    "depreciation_amortization",
    "other_expenses",
    "total_expenses",
    "profit_before_exceptional_items_and_tax",
    "exceptional_items",
    "profit_before_tax",
    "tax_expense",
    "net_profit_continuing",
    "net_profit",
    "net_profit_parent",
    "comprehensive_income",
    "equity_share_capital",
    "total_assets",
    "total_liabilities",
    "total_equity",
    "cash_and_cash_equivalents",
    "cash_flow_from_operations",
    "cash_flow_from_investing",
    "cash_flow_from_financing",
    "capital_expenditure",
}


@dataclass(frozen=True)
class ValidationResult:
    defects: list[ValidationDefect]
    candidate_statuses: dict[str, ValidationStatus]
    blocking: bool
    requires_review: bool


class FilingFactValidator:
    def __init__(self, *, auto_approve_confidence: float = 0.98) -> None:
        self.auto_approve_confidence = auto_approve_confidence

    def validate(self, run_id: str, candidates: list[dict[str, Any]]) -> ValidationResult:
        defects: list[ValidationDefect] = []
        statuses: dict[str, ValidationStatus] = {}
        defects_by_candidate: dict[str, list[ValidationDefect]] = defaultdict(list)

        for candidate in candidates:
            candidate_id = str(candidate["candidate_id"])
            candidate_defects = self._candidate_defects(run_id, candidate)
            defects.extend(candidate_defects)
            defects_by_candidate[candidate_id].extend(candidate_defects)

        equation_defects = self._accounting_defects(run_id, candidates)
        defects.extend(equation_defects)
        for defect in equation_defects:
            if defect.candidate_id:
                defects_by_candidate[defect.candidate_id].append(defect)

        for candidate in candidates:
            candidate_id = str(candidate["candidate_id"])
            item_defects = defects_by_candidate.get(candidate_id, [])
            severities = {item.severity for item in item_defects}
            if ValidationSeverity.BLOCKING in severities or ValidationSeverity.ERROR in severities:
                statuses[candidate_id] = ValidationStatus.FAILED
            elif ValidationSeverity.WARNING in severities:
                statuses[candidate_id] = ValidationStatus.REVIEW
            else:
                statuses[candidate_id] = ValidationStatus.PASSED

        blocking = any(
            defect.severity in {ValidationSeverity.BLOCKING, ValidationSeverity.ERROR}
            for defect in defects
        )
        requires_review = any(
            status == ValidationStatus.REVIEW for status in statuses.values()
        )
        return ValidationResult(
            defects=defects,
            candidate_statuses=statuses,
            blocking=blocking,
            requires_review=requires_review,
        )

    def _candidate_defects(
        self, run_id: str, candidate: dict[str, Any]
    ) -> list[ValidationDefect]:
        candidate_id = str(candidate["candidate_id"])
        defects: list[ValidationDefect] = []

        try:
            value = Decimal(str(candidate["value_decimal"]))
            if not value.is_finite():
                raise InvalidOperation
        except (InvalidOperation, KeyError):
            defects.append(
                self._defect(
                    run_id,
                    candidate_id,
                    "numeric.invalid",
                    ValidationSeverity.BLOCKING,
                    "candidate value is not a finite decimal",
                )
            )

        period_end = candidate.get("period_end")
        period_start = candidate.get("period_start")
        if not isinstance(period_end, date):
            defects.append(
                self._defect(
                    run_id,
                    candidate_id,
                    "period.end_missing",
                    ValidationSeverity.BLOCKING,
                    "candidate has no valid period end",
                )
            )
        elif period_start and period_start > period_end:
            defects.append(
                self._defect(
                    run_id,
                    candidate_id,
                    "period.invalid_range",
                    ValidationSeverity.BLOCKING,
                    "candidate period start occurs after period end",
                )
            )

        scope = candidate.get("consolidation_scope")
        if scope == ConsolidationScope.UNKNOWN.value:
            defects.append(
                self._defect(
                    run_id,
                    candidate_id,
                    "scope.unknown",
                    ValidationSeverity.WARNING,
                    "standalone versus consolidated scope is unresolved",
                )
            )

        metric = str(candidate.get("canonical_metric") or "")
        if metric in MONETARY_METRICS and not candidate.get("currency"):
            defects.append(
                self._defect(
                    run_id,
                    candidate_id,
                    "unit.currency_missing",
                    ValidationSeverity.WARNING,
                    "monetary candidate has no currency",
                )
            )
        if not candidate.get("evidence_ids"):
            defects.append(
                self._defect(
                    run_id,
                    candidate_id,
                    "evidence.missing",
                    ValidationSeverity.BLOCKING,
                    "candidate has no evidence reference",
                )
            )
        if float(candidate.get("confidence") or 0) < self.auto_approve_confidence:
            defects.append(
                self._defect(
                    run_id,
                    candidate_id,
                    "confidence.review_required",
                    ValidationSeverity.WARNING,
                    "candidate confidence is below the automatic-approval threshold",
                    {
                        "confidence": candidate.get("confidence"),
                        "threshold": self.auto_approve_confidence,
                    },
                )
            )
        return defects

    def _accounting_defects(
        self, run_id: str, candidates: list[dict[str, Any]]
    ) -> list[ValidationDefect]:
        grouped: dict[tuple[Any, ...], dict[str, dict[str, Any]]] = defaultdict(dict)
        duplicate_values: dict[
            tuple[Any, ...], dict[str, list[dict[str, Any]]]
        ] = defaultdict(lambda: defaultdict(list))
        for candidate in candidates:
            key = (
                candidate.get("period_end"),
                candidate.get("period_type"),
                candidate.get("consolidation_scope"),
            )
            metric = str(candidate["canonical_metric"])
            duplicate_values[key][metric].append(candidate)
            grouped[key].setdefault(metric, candidate)

        defects: list[ValidationDefect] = []
        for key, metrics in duplicate_values.items():
            for metric, rows in metrics.items():
                values = {str(row["value_decimal"]) for row in rows}
                if len(values) > 1:
                    for row in rows:
                        defects.append(
                            self._defect(
                                run_id,
                                str(row["candidate_id"]),
                                "metric.conflicting_duplicate",
                                ValidationSeverity.BLOCKING,
                                (
                                    f"conflicting values found for {metric} in "
                                    "the same period and scope"
                                ),
                                {"values": sorted(values), "group": [str(value) for value in key]},
                            )
                        )

        for metrics in grouped.values():
            self._equation(
                run_id,
                metrics,
                defects,
                code="accounting.balance_sheet",
                left="total_assets",
                right_positive=("total_liabilities", "total_equity"),
                tolerance_ratio=Decimal("0.005"),
            )
            self._equation(
                run_id,
                metrics,
                defects,
                code="accounting.income_less_expense",
                left="total_income",
                right_positive=("total_expenses", "profit_before_exceptional_items_and_tax"),
                tolerance_ratio=Decimal("0.005"),
            )
            if all(name in metrics for name in ("profit_before_tax", "tax_expense", "net_profit")):
                pbt = _value(metrics["profit_before_tax"])
                tax = _value(metrics["tax_expense"])
                net = _value(metrics["net_profit"])
                expected = pbt - tax
                if not _close(expected, net, Decimal("0.01")):
                    defects.append(
                        self._defect(
                            run_id,
                            str(metrics["net_profit"]["candidate_id"]),
                            "accounting.profit_after_tax",
                            ValidationSeverity.WARNING,
                            "net profit does not reconcile to profit before tax less tax expense",
                            {
                                "profit_before_tax": str(pbt),
                                "tax_expense": str(tax),
                                "net_profit": str(net),
                                "expected": str(expected),
                            },
                        )
                    )
        return defects

    def _equation(
        self,
        run_id: str,
        metrics: dict[str, dict[str, Any]],
        defects: list[ValidationDefect],
        *,
        code: str,
        left: str,
        right_positive: tuple[str, ...],
        tolerance_ratio: Decimal,
    ) -> None:
        if left not in metrics or not all(name in metrics for name in right_positive):
            return
        left_value = _value(metrics[left])
        right_value = sum((_value(metrics[name]) for name in right_positive), Decimal("0"))
        if _close(left_value, right_value, tolerance_ratio):
            return
        defects.append(
            self._defect(
                run_id,
                str(metrics[left]["candidate_id"]),
                code,
                ValidationSeverity.WARNING,
                f"{left} does not reconcile to {' + '.join(right_positive)}",
                {
                    left: str(left_value),
                    "expected": str(right_value),
                    "difference": str(left_value - right_value),
                },
            )
        )

    @staticmethod
    def _defect(
        run_id: str,
        candidate_id: str | None,
        code: str,
        severity: ValidationSeverity,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> ValidationDefect:
        return ValidationDefect(
            defect_id=stable_id(
                "filing-validation-defect",
                run_id,
                candidate_id or "run",
                code,
                message,
            ),
            run_id=run_id,
            candidate_id=candidate_id,
            rule_code=code,
            severity=severity,
            message=message,
            context=context or {},
        )


def _value(candidate: dict[str, Any]) -> Decimal:
    return Decimal(str(candidate["value_decimal"]))


def _close(left: Decimal, right: Decimal, tolerance_ratio: Decimal) -> bool:
    scale = max(abs(left), abs(right), Decimal("1"))
    return abs(left - right) <= scale * tolerance_ratio
