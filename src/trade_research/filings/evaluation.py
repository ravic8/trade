from __future__ import annotations

import hashlib
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator

from trade_research.filings.models import ConsolidationScope, PeriodType
from trade_research.filings.store import FilingStore


class GoldenExpectedFact(BaseModel):
    canonical_metric: str
    value: Decimal
    currency: str | None
    period_type: PeriodType
    xbrl_concept: str
    context_ref: str


class GoldenFilingCase(BaseModel):
    case_id: str
    period_end: date
    consolidation_scope: ConsolidationScope
    source_filename: str
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_facts: list[GoldenExpectedFact] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_fact_keys(self) -> GoldenFilingCase:
        keys = [
            (fact.canonical_metric, fact.period_type)
            for fact in self.expected_facts
        ]
        if len(keys) != len(set(keys)):
            raise ValueError(f"duplicate golden fact key in {self.case_id}")
        return self


class FilingGoldenDataset(BaseModel):
    schema_version: int = 1
    dataset_id: str
    company_id: str
    source_manifest: str
    source_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    locked_at: datetime
    review_status: str
    cases: list[GoldenFilingCase] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_cases(self) -> FilingGoldenDataset:
        case_ids = [case.case_id for case in self.cases]
        periods = [
            (case.period_end, case.consolidation_scope)
            for case in self.cases
        ]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("golden dataset contains duplicate case IDs")
        if len(periods) != len(set(periods)):
            raise ValueError("golden dataset contains duplicate period/scope cases")
        return self


class FilingGoldenDefect(BaseModel):
    case_id: str
    canonical_metric: str
    rule_code: str
    message: str
    expected: dict[str, Any] = Field(default_factory=dict)
    observed: dict[str, Any] = Field(default_factory=dict)


class FilingGoldenReport(BaseModel):
    dataset_id: str
    company_id: str
    case_count: int
    expected_fact_count: int
    matched_fact_count: int
    value_correct_count: int
    evidence_correct_count: int
    passed: bool
    defects: list[FilingGoldenDefect] = Field(default_factory=list)


def load_golden_dataset(
    path: Path,
    *,
    verify_manifest: bool = True,
    repository_root: Path | None = None,
) -> FilingGoldenDataset:
    path = path.expanduser().resolve()
    dataset = FilingGoldenDataset.model_validate_json(path.read_text(encoding="utf-8"))
    if verify_manifest:
        root = (repository_root or Path.cwd()).resolve()
        manifest = (root / dataset.source_manifest).resolve()
        if not manifest.is_relative_to(root):
            raise ValueError("golden dataset manifest escapes the repository root")
        if not manifest.is_file():
            raise FileNotFoundError(manifest)
        actual_hash = _sha256(manifest)
        if actual_hash != dataset.source_manifest_sha256:
            raise ValueError(
                "golden dataset source manifest changed: "
                f"expected {dataset.source_manifest_sha256}, got {actual_hash}"
            )
    return dataset


def evaluate_golden_dataset(
    store: FilingStore,
    *,
    workspace_id: str,
    dataset: FilingGoldenDataset,
) -> FilingGoldenReport:
    documents = store.documents(
        workspace_id=workspace_id,
        company_id=dataset.company_id,
        current_only=False,
        limit=5_000,
    )
    documents_by_hash = {document.sha256: document for document in documents}
    facts = store.approved_facts(
        workspace_id=workspace_id,
        company_id=dataset.company_id,
        current_only=False,
        limit=20_000,
    )
    facts_by_filing: dict[str, list[Any]] = {}
    for fact in facts:
        facts_by_filing.setdefault(fact.source_filing_id, []).append(fact)

    defects: list[FilingGoldenDefect] = []
    matched = 0
    values_correct = 0
    evidence_correct = 0
    expected_count = 0
    for case in dataset.cases:
        document = documents_by_hash.get(case.source_sha256)
        if document is None:
            for expected in case.expected_facts:
                expected_count += 1
                defects.append(
                    _defect(
                        case,
                        expected,
                        "filing.missing",
                        "the locked source filing is not registered",
                    )
                )
            continue
        if document.filename != case.source_filename:
            defects.append(
                FilingGoldenDefect(
                    case_id=case.case_id,
                    canonical_metric="__filing__",
                    rule_code="filing.filename_mismatch",
                    message="registered filing filename differs from the locked case",
                    expected={"source_filename": case.source_filename},
                    observed={"source_filename": document.filename},
                )
            )

        filing_facts = facts_by_filing.get(document.filing_id, [])
        for expected in case.expected_facts:
            expected_count += 1
            matches = [
                fact
                for fact in filing_facts
                if fact.canonical_metric == expected.canonical_metric
                and fact.period_end == case.period_end
                and fact.period_type == expected.period_type
                and fact.consolidation_scope == case.consolidation_scope
            ]
            if len(matches) != 1:
                defects.append(
                    _defect(
                        case,
                        expected,
                        "fact.cardinality",
                        "expected exactly one approved fact",
                        observed={"match_count": len(matches)},
                    )
                )
                continue
            matched += 1
            fact = matches[0]
            if fact.value != expected.value or fact.currency != expected.currency:
                defects.append(
                    _defect(
                        case,
                        expected,
                        "fact.value_mismatch",
                        "approved value or currency differs from the locked value",
                        observed={
                            "value": str(fact.value),
                            "currency": fact.currency,
                            "fact_id": fact.fact_id,
                        },
                    )
                )
                continue
            values_correct += 1
            evidence = store.evidence(
                workspace_id=workspace_id,
                evidence_ids=fact.evidence_ids,
            )
            exact_evidence = [
                item
                for item in evidence
                if item.source_hash == case.source_sha256
                and item.xbrl_concept == expected.xbrl_concept
                and item.context_ref == expected.context_ref
            ]
            if not exact_evidence:
                defects.append(
                    _defect(
                        case,
                        expected,
                        "evidence.mismatch",
                        "approved fact does not resolve to the locked concept/context",
                        observed={
                            "evidence": [
                                {
                                    "evidence_id": item.evidence_id,
                                    "source_hash": item.source_hash,
                                    "xbrl_concept": item.xbrl_concept,
                                    "context_ref": item.context_ref,
                                }
                                for item in evidence
                            ]
                        },
                    )
                )
                continue
            evidence_correct += 1

    return FilingGoldenReport(
        dataset_id=dataset.dataset_id,
        company_id=dataset.company_id,
        case_count=len(dataset.cases),
        expected_fact_count=expected_count,
        matched_fact_count=matched,
        value_correct_count=values_correct,
        evidence_correct_count=evidence_correct,
        passed=not defects and expected_count > 0,
        defects=defects,
    )


def _defect(
    case: GoldenFilingCase,
    fact: GoldenExpectedFact,
    rule_code: str,
    message: str,
    *,
    observed: dict[str, Any] | None = None,
) -> FilingGoldenDefect:
    return FilingGoldenDefect(
        case_id=case.case_id,
        canonical_metric=fact.canonical_metric,
        rule_code=rule_code,
        message=message,
        expected={
            "value": str(fact.value),
            "currency": fact.currency,
            "period_type": fact.period_type.value,
            "xbrl_concept": fact.xbrl_concept,
            "context_ref": fact.context_ref,
            "source_sha256": case.source_sha256,
        },
        observed=observed or {},
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
