from importlib import import_module

import pytest

dagster = pytest.importorskip("dagster")
market_data_assets = import_module("trade_research.dagster.market_data_assets")


def _comparison_result(status: str = "pass") -> object:
    return market_data_assets.PipelineRunResult(
        name="nse_yfinance_cutover_readiness",
        status=status,
        rows=42,
        metrics={
            "comparison_state": "overlap_available",
            "window_start": "2026-09-01",
            "window_end": "2026-09-08",
            "overlapping_symbols": 25,
            "row_overlap_ratio": 1.0,
            "close_match_ratio": 1.0,
            "evidence_id": "e" * 64,
        },
        blocking_issues=[] if status == "pass" else ["comparison failed"],
    )


def test_provider_comparison_asset_records_evidence_metadata(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_comparison(**kwargs) -> object:
        captured.update(kwargs)
        return _comparison_result()

    monkeypatch.setattr(
        market_data_assets,
        "run_nse_yfinance_cutover_readiness",
        fake_comparison,
    )

    result = market_data_assets.nse_yfinance_provider_comparison(
        dagster.build_op_context()
    )

    assert result.status == "pass"
    assert captured == {
        "trigger": "dagster",
        "at": None,
    }


def test_provider_comparison_asset_fails_dagster_run_on_blocked_window(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        market_data_assets,
        "run_nse_yfinance_cutover_readiness",
        lambda **_kwargs: _comparison_result(status="fail"),
    )

    with pytest.raises(RuntimeError, match="NSE provider comparison did not pass"):
        market_data_assets.nse_yfinance_provider_comparison(dagster.build_op_context())
