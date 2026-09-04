import json
from datetime import UTC, date, datetime

import pandas as pd
import pytest

import trade_research.pipelines.daily_features as daily_features_module
import trade_research.pipelines.daily_targets as daily_targets_module
from trade_research.config import Settings
from trade_research.contracts import get_data_contract
from trade_research.pipelines.contract_gate import (
    daily_ohlcv_invariant_result,
    enforce_publication_contract,
    expected_publication_sessions,
    failed_quality_rows_result,
    feature_target_leakage_result,
    normalize_daily_ohlcv_publication_frame,
)
from trade_research.pipelines.daily_features import run_daily_feature_pipeline
from trade_research.pipelines.daily_targets import run_daily_target_pipeline
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.validation import ValidationContractError


def _provider_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "InstrumentKey": "NSE_EQ|AAA",
                "Symbol": "aaa",
                "Date": date(2026, 8, 24),
                "Open": 100.0,
                "High": 102.0,
                "Low": 99.0,
                "Close": 101.0,
                "Volume": 10_000,
                "OpenInterest": None,
            }
        ]
    )


def test_daily_ohlcv_publication_normalization_matches_registered_schema() -> None:
    contract = get_data_contract("market_data.ohlcv_daily.v1")

    frame = normalize_daily_ohlcv_publication_frame(
        _provider_frame(),
        exchange="NSE",
        source="upstox",
        fetched_at=datetime(2026, 8, 25, 1, tzinfo=UTC),
    )

    assert tuple(frame.columns) == tuple(column.name for column in contract.columns)
    assert frame.iloc[0]["symbol"] == "AAA"
    assert frame.iloc[0]["source"] == "upstox"
    assert frame.iloc[0]["quality_status"] == "ok"
    stored = TimescaleStore._daily_ohlcv_rows(frame, exchange="NSE", source="upstox")
    assert stored[0]["fetched_at"] == datetime(2026, 8, 25, 1, tzinfo=UTC)
    assert stored[0]["quality_status"] == "ok"


def test_publication_gate_writes_passed_generic_and_ohlc_evidence(tmp_path) -> None:
    frame = normalize_daily_ohlcv_publication_frame(
        _provider_frame(),
        exchange="NSE",
        source="upstox",
        fetched_at=datetime(2026, 8, 25, 1, tzinfo=UTC),
    )
    run_id = "ingestion-run-1"
    evaluated_at = datetime(2026, 8, 25, 2, tzinfo=UTC)
    report_path = tmp_path / "daily-contract.json"

    evidence = enforce_publication_contract(
        frame,
        contract_id="market_data.ohlcv_daily.v1",
        report_path=report_path,
        run_id=run_id,
        scope={"exchange": "NSE", "source": "upstox"},
        eligible_session_dates=expected_publication_sessions(frame, exchange="NSE"),
        additional_results=(
            daily_ohlcv_invariant_result(
                frame,
                run_id=run_id,
                created_at=evaluated_at,
            ),
        ),
        evaluated_at=evaluated_at,
    )

    payload = json.loads(report_path.read_text())
    assert evidence.report.status == "passed"
    assert len(evidence.report.results) == 9
    assert payload["run_id"] == run_id
    assert payload["results"][-1]["check_id"].endswith("invariants.ohlc_ordering")


def test_publication_gate_preserves_duplicates_and_writes_failure_before_raise(
    tmp_path,
) -> None:
    invalid = _provider_frame()
    invalid.loc[0, "High"] = 98.0
    invalid = pd.concat([invalid, invalid], ignore_index=True)
    frame = normalize_daily_ohlcv_publication_frame(
        invalid,
        exchange="NSE",
        source="upstox",
    )
    run_id = "ingestion-run-invalid"
    report_path = tmp_path / "failed-contract.json"

    with pytest.raises(ValidationContractError, match="keys.unique"):
        enforce_publication_contract(
            frame,
            contract_id="market_data.ohlcv_daily.v1",
            report_path=report_path,
            run_id=run_id,
            eligible_session_dates=expected_publication_sessions(frame, exchange="NSE"),
            additional_results=(
                daily_ohlcv_invariant_result(frame, run_id=run_id),
            ),
        )

    payload = json.loads(report_path.read_text())
    statuses = {result["check_id"]: result["status"] for result in payload["results"]}
    assert len(frame) == 2
    assert statuses["market_data.ohlcv_daily.v1.keys.unique"] == "failed"
    assert (
        statuses["market_data.ohlcv_daily.v1.invariants.ohlc_ordering"]
        == "failed"
    )


def test_feature_specific_checks_reject_target_columns_and_failed_rows() -> None:
    frame = pd.DataFrame(
        {
            "quality_status": ["passed", "failed"],
            "forward_ret_1d": [0.01, 0.02],
        }
    )

    leakage = feature_target_leakage_result(frame, run_id="feature-run")
    quality = failed_quality_rows_result(
        frame,
        contract_id="feature.daily_technical.v1",
        run_id="feature-run",
    )

    assert leakage.status == "failed"
    assert leakage.observed_value == {"target_columns": ["forward_ret_1d"]}
    assert quality.status == "failed"
    assert quality.observed_value == {"failed_row_count": 1}


def test_feature_and_target_pipelines_publish_contract_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(data_dir=tmp_path)
    monkeypatch.setattr(daily_features_module, "get_settings", lambda: settings)
    monkeypatch.setattr(daily_targets_module, "get_settings", lambda: settings)
    source = _daily_source_frame()
    ParquetStore(tmp_path).write_frame("processed/equities/input", source)

    features = run_daily_feature_pipeline(
        input_name="processed/equities/input",
        audit_output=tmp_path / "feature-audit.csv",
        summary_output=tmp_path / "feature-summary.json",
    )
    targets = run_daily_target_pipeline(
        input_name="processed/equities/input",
        audit_output=tmp_path / "target-audit.csv",
        summary_output=tmp_path / "target-summary.json",
    )

    assert features.metrics["contract_validation_status"] == "passed"
    assert features.metrics["contract_validation_checks"] == 10
    assert features.artifacts["contract_validation"].exists()
    assert targets.metrics["contract_validation_status"] == "passed"
    assert targets.metrics["contract_validation_checks"] == 9
    assert targets.artifacts["contract_validation"].exists()


def _daily_source_frame() -> pd.DataFrame:
    sessions = pd.bdate_range("2025-01-02", periods=80)
    return pd.DataFrame(
        [
            {
                "instrument_key": "NSE_EQ|AAA",
                "symbol": "AAA",
                "exchange": "NSE",
                "source": "upstox",
                "date": session.date(),
                "open": 100.0 + index,
                "high": 102.0 + index,
                "low": 99.0 + index,
                "close": 101.0 + index,
                "volume": 10_000 + index,
                "open_interest": None,
                "quality_status": "ok",
            }
            for index, session in enumerate(sessions)
        ]
    )
