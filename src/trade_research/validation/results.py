from __future__ import annotations

from collections.abc import Collection
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

VALIDATION_RESULT_CONTRACT_VERSION: Literal["validation_result.v1"] = (
    "validation_result.v1"
)
VALIDATION_REPORT_CONTRACT_VERSION: Literal["validation_report.v1"] = (
    "validation_report.v1"
)

ValidationSeverity = Literal["info", "warning", "error"]
ValidationStatus = Literal["passed", "warning", "failed", "skipped_with_reason"]


class ValidationContractError(RuntimeError):
    """Raised when validation evidence does not satisfy a downstream contract."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ValidationResult(BaseModel):
    """Versioned, JSON-safe evidence emitted by one validation check."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    contract_version: Literal["validation_result.v1"] = VALIDATION_RESULT_CONTRACT_VERSION
    check_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9_.-]*$")
    dataset_id: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.:/-]*$")
    run_id: str = Field(min_length=1)
    scope: dict[str, JsonValue] = Field(default_factory=dict)
    severity: ValidationSeverity
    status: ValidationStatus
    observed_value: JsonValue | None = None
    expected_value: JsonValue | None = None
    message: str = Field(min_length=1)
    evidence: dict[str, JsonValue] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utc_now)

    @field_validator("created_at")
    @classmethod
    def created_at_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value.astimezone(UTC)


class ValidationReport(BaseModel):
    """A coherent set of validation checks for one dataset run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    contract_version: Literal["validation_report.v1"] = VALIDATION_REPORT_CONTRACT_VERSION
    dataset_id: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.:/-]*$")
    run_id: str = Field(min_length=1)
    results: tuple[ValidationResult, ...] = Field(min_length=1)
    created_at: datetime = Field(default_factory=_utc_now)

    @field_validator("created_at")
    @classmethod
    def created_at_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def results_must_share_identity_and_have_unique_checks(self) -> ValidationReport:
        seen: set[str] = set()
        for result in self.results:
            if result.dataset_id != self.dataset_id:
                raise ValueError(
                    f"Validation result {result.check_id!r} has dataset_id "
                    f"{result.dataset_id!r}; expected {self.dataset_id!r}."
                )
            if result.run_id != self.run_id:
                raise ValueError(
                    f"Validation result {result.check_id!r} has run_id "
                    f"{result.run_id!r}; expected {self.run_id!r}."
                )
            if result.check_id in seen:
                raise ValueError(f"Duplicate validation check_id: {result.check_id}")
            seen.add(result.check_id)
        return self

    @property
    def status(self) -> ValidationStatus:
        statuses = {result.status for result in self.results}
        if "failed" in statuses:
            return "failed"
        if "warning" in statuses:
            return "warning"
        if "skipped_with_reason" in statuses:
            return "skipped_with_reason"
        return "passed"

    def downstream_blockers(
        self,
        *,
        accepted_warning_check_ids: Collection[str] = (),
    ) -> tuple[ValidationResult, ...]:
        accepted_warnings = set(accepted_warning_check_ids)
        return tuple(
            result
            for result in self.results
            if result.status in {"failed", "skipped_with_reason"}
            or (
                result.status == "warning"
                and result.check_id not in accepted_warnings
            )
        )

    def require_downstream_ready(
        self,
        *,
        accepted_warning_check_ids: Collection[str] = (),
    ) -> None:
        blockers = self.downstream_blockers(
            accepted_warning_check_ids=accepted_warning_check_ids
        )
        if not blockers:
            return
        details = "; ".join(
            f"{result.check_id}={result.status}: {result.message}" for result in blockers
        )
        raise ValidationContractError(
            f"Validation report {self.run_id!r} blocks downstream use: {details}"
        )
