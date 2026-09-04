from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from trade_research.validation.results import (
    ValidationContractError,
    ValidationReport,
    ValidationResult,
)


def _result(
    check_id: str,
    status: str = "passed",
    *,
    dataset_id: str = "dataset.v1",
    run_id: str = "run-1",
) -> ValidationResult:
    return ValidationResult(
        check_id=check_id,
        dataset_id=dataset_id,
        run_id=run_id,
        scope={"exchange": "NSE"},
        severity="error",
        status=status,
        observed_value=1,
        expected_value=1,
        message=f"{check_id} is {status}.",
        evidence={"source": "test"},
        created_at=datetime(2026, 8, 25, tzinfo=UTC),
    )


def test_validation_result_is_strict_versioned_and_json_safe() -> None:
    result = _result("ohlcv.unique_keys")

    payload = result.model_dump(mode="json")

    assert payload["contract_version"] == "validation_result.v1"
    assert payload["check_id"] == "ohlcv.unique_keys"
    assert payload["created_at"] == "2026-08-25T00:00:00Z"


def test_validation_result_rejects_naive_timestamp_and_unknown_fields() -> None:
    values = _result("ohlcv.unique_keys").model_dump()
    values["created_at"] = datetime(2026, 8, 25)

    with pytest.raises(ValidationError, match="timezone-aware"):
        ValidationResult.model_validate(values)

    values["created_at"] = datetime(2026, 8, 25, tzinfo=UTC)
    values["unexpected"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ValidationResult.model_validate(values)


def test_validation_report_rejects_mixed_identity_and_duplicate_checks() -> None:
    with pytest.raises(ValidationError, match="at least 1 item"):
        ValidationReport(dataset_id="dataset.v1", run_id="run-1", results=())

    with pytest.raises(ValidationError, match="expected 'dataset.v1'"):
        ValidationReport(
            dataset_id="dataset.v1",
            run_id="run-1",
            results=(_result("check.one", dataset_id="other.v1"),),
        )

    with pytest.raises(ValidationError, match="Duplicate validation check_id"):
        ValidationReport(
            dataset_id="dataset.v1",
            run_id="run-1",
            results=(_result("check.one"), _result("check.one")),
        )


def test_downstream_policy_accepts_only_named_safe_warnings() -> None:
    report = ValidationReport(
        dataset_id="dataset.v1",
        run_id="run-1",
        results=(
            _result("check.passed"),
            _result("check.safe_warning", "warning"),
        ),
    )

    assert report.status == "warning"
    with pytest.raises(ValidationContractError, match="check.safe_warning"):
        report.require_downstream_ready()

    report.require_downstream_ready(
        accepted_warning_check_ids={"check.safe_warning"}
    )


@pytest.mark.parametrize("status", ["failed", "skipped_with_reason"])
def test_failed_or_skipped_check_cannot_be_accepted_as_safe_warning(status: str) -> None:
    report = ValidationReport(
        dataset_id="dataset.v1",
        run_id="run-1",
        results=(_result("check.blocking", status),),
    )

    with pytest.raises(ValidationContractError, match="check.blocking"):
        report.require_downstream_ready(
            accepted_warning_check_ids={"check.blocking"}
        )
