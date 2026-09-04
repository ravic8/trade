from datetime import UTC, date, datetime

import pandas as pd
import pytest
from pydantic import ValidationError

from trade_research.contracts import (
    ColumnContract,
    ContractEvaluationContext,
    DataContract,
    FreshnessContract,
    evaluate_frame_contract,
    get_data_contract,
)
from trade_research.validation.results import ValidationContractError


def _daily_ohlcv_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_key": "NSE_EQ|AAA",
                "source": "yfinance",
                "date": date(2026, 8, 24),
                "symbol": "AAA",
                "exchange": "NSE",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 100_000,
                "open_interest": None,
                "fetched_at": datetime(2026, 8, 25, 1, tzinfo=UTC),
                "quality_status": "ok",
            }
        ]
    )


def _context(*sessions: date) -> ContractEvaluationContext:
    return ContractEvaluationContext(
        evaluated_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
        eligible_session_dates=tuple(sessions),
    )


def test_valid_frame_passes_all_generic_contract_checks() -> None:
    contract = get_data_contract("market_data.ohlcv_daily.v1")

    report = evaluate_frame_contract(
        _daily_ohlcv_frame(),
        contract,
        run_id="run-1",
        scope={"exchange": "NSE"},
        context=_context(date(2026, 8, 24)),
    )

    assert report.dataset_id == contract.contract_id
    assert report.status == "passed"
    assert len(report.results) == 8
    assert all(result.status == "passed" for result in report.results)
    assert report.model_dump(mode="json")["contract_version"] == "validation_report.v1"


def test_invalid_frame_reports_type_null_key_enum_and_range_failures() -> None:
    contract = get_data_contract("market_data.ohlcv_daily.v1")
    first = _daily_ohlcv_frame().iloc[0].to_dict()
    invalid = dict(first)
    invalid.update(
        {
            "symbol": None,
            "open": "not-a-number",
            "volume": -1,
            "quality_status": "unknown",
        }
    )
    frame = pd.DataFrame([first, invalid])

    report = evaluate_frame_contract(
        frame,
        contract,
        run_id="run-invalid",
        context=_context(date(2026, 8, 24)),
    )
    statuses = {result.check_id: result.status for result in report.results}

    assert report.status == "failed"
    assert statuses[f"{contract.contract_id}.schema.logical_types"] == "failed"
    assert statuses[f"{contract.contract_id}.schema.nullability"] == "failed"
    assert statuses[f"{contract.contract_id}.keys.unique"] == "failed"
    assert statuses[f"{contract.contract_id}.values.allowed"] == "failed"
    assert statuses[f"{contract.contract_id}.values.ranges"] == "failed"


def test_exclusive_price_minimum_rejects_zero() -> None:
    contract = get_data_contract("market_data.ohlcv_daily.v1")
    frame = _daily_ohlcv_frame()
    frame.loc[0, "open"] = 0.0

    report = evaluate_frame_contract(
        frame,
        contract,
        run_id="run-zero",
        context=_context(date(2026, 8, 24)),
    )

    check = next(result for result in report.results if result.check_id.endswith("values.ranges"))
    assert check.status == "failed"


def test_missing_key_column_skips_uniqueness_and_blocks_downstream() -> None:
    contract = get_data_contract("market_data.ohlcv_daily.v1")
    frame = _daily_ohlcv_frame().drop(columns=["source"])

    report = evaluate_frame_contract(
        frame,
        contract,
        run_id="run-missing",
        context=_context(date(2026, 8, 24)),
    )
    key_check = next(result for result in report.results if result.check_id.endswith("keys.unique"))

    assert key_check.status == "skipped_with_reason"
    with pytest.raises(ValidationContractError, match="keys.unique"):
        report.require_downstream_ready()


def test_unregistered_columns_are_warning_and_require_explicit_acceptance() -> None:
    contract = get_data_contract("market_data.ohlcv_daily.v1")
    frame = _daily_ohlcv_frame()
    frame["provider_payload"] = "opaque"
    report = evaluate_frame_contract(
        frame,
        contract,
        run_id="run-extra",
        context=_context(date(2026, 8, 24)),
    )
    warning_id = f"{contract.contract_id}.schema.unregistered_columns"

    assert report.status == "warning"
    with pytest.raises(ValidationContractError, match="unregistered_columns"):
        report.require_downstream_ready()
    report.require_downstream_ready(accepted_warning_check_ids={warning_id})


def test_session_freshness_is_fail_closed_without_calendar_context() -> None:
    contract = get_data_contract("market_data.ohlcv_daily.v1")

    missing_context = evaluate_frame_contract(
        _daily_ohlcv_frame(),
        contract,
        run_id="run-no-calendar",
    )
    stale = evaluate_frame_contract(
        _daily_ohlcv_frame(),
        contract,
        run_id="run-stale",
        context=_context(
            date(2026, 8, 24),
            date(2026, 8, 25),
            date(2026, 8, 26),
        ),
    )

    missing_check = next(
        result for result in missing_context.results if result.check_id.endswith("freshness")
    )
    stale_check = next(result for result in stale.results if result.check_id.endswith("freshness"))
    assert missing_check.status == "skipped_with_reason"
    assert stale_check.status == "failed"
    assert stale_check.observed_value["lag_sessions"] == 2


def test_wall_clock_freshness_detects_stale_and_future_timestamps() -> None:
    contract = DataContract(
        contract_id="universe.example_refresh.v1",
        dataset_version="example_v1",
        domain="universe",
        owner="test-owner",
        description="Test wall-clock freshness.",
        lifecycle="current",
        authoritative_store="local_parquet",
        storage_name="example.parquet",
        primary_key=("id",),
        columns=(
            ColumnContract(
                name="id",
                logical_type="string",
                nullable=False,
                description="Identifier.",
            ),
            ColumnContract(
                name="generated_at",
                logical_type="datetime",
                nullable=False,
                description="Generation timestamp.",
            ),
        ),
        freshness=FreshnessContract(
            mode="wall_clock",
            basis_column="generated_at",
            max_age_minutes=60,
            description="Fresh for one hour.",
        ),
        invariants=("id is unique",),
    )
    context = ContractEvaluationContext(evaluated_at=datetime(2026, 8, 25, 12, tzinfo=UTC))

    stale = evaluate_frame_contract(
        pd.DataFrame([{"id": "one", "generated_at": datetime(2026, 8, 25, 10, tzinfo=UTC)}]),
        contract,
        run_id="stale",
        context=context,
    )
    future = evaluate_frame_contract(
        pd.DataFrame([{"id": "one", "generated_at": datetime(2026, 8, 25, 13, tzinfo=UTC)}]),
        contract,
        run_id="future",
        context=context,
    )

    assert (
        next(item for item in stale.results if item.check_id.endswith("freshness")).status
        == "failed"
    )
    assert (
        next(item for item in future.results if item.check_id.endswith("freshness")).status
        == "failed"
    )


def test_context_rejects_unsorted_or_duplicate_session_dates() -> None:
    with pytest.raises(ValidationError, match="sorted and unique"):
        ContractEvaluationContext(eligible_session_dates=(date(2026, 8, 25), date(2026, 8, 24)))
