from __future__ import annotations

from pathlib import Path

import pandas as pd

from trade_research.pipelines import processed_validation
from trade_research.validation.processed_datasets import (
    build_date_coverage,
    find_invalid_ohlcv_rows,
    normalize_ohlcv,
    validate_alignment,
    validate_processed_datasets,
)


def _ohlcv_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "InstrumentKey": "A",
                "Symbol": "AAA",
                "Date": "2026-01-01",
                "Open": 10,
                "High": 11,
                "Low": 9,
                "Close": 10,
                "Volume": 100,
                "OpenInterest": 0,
                "Source": "upstox",
            },
            {
                "InstrumentKey": "B",
                "Symbol": "BBB",
                "Date": "2026-01-01",
                "Open": 20,
                "High": 21,
                "Low": 19,
                "Close": 20,
                "Volume": -5,
                "OpenInterest": 0,
                "Source": "upstox",
            },
            {
                "InstrumentKey": "A",
                "Symbol": "AAA",
                "Date": "2026-01-02",
                "Open": 11,
                "High": 12,
                "Low": 10,
                "Close": 11,
                "Volume": 120,
                "OpenInterest": 0,
                "Source": "upstox",
            },
        ]
    )


def test_negative_volume_is_invalid() -> None:
    frame = normalize_ohlcv(_ohlcv_rows())

    invalid = find_invalid_ohlcv_rows(frame)

    assert len(invalid) == 1
    assert invalid.iloc[0]["instrument_key"] == "B"
    assert invalid.iloc[0]["invalid_reasons"] == "negative_volume"


def test_duplicate_keys_fail_validation(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    path = data_dir / "processed/equities/nse_daily_ohlcv_upstox.parquet"
    path.parent.mkdir(parents=True)
    duplicate = pd.concat([_ohlcv_rows().iloc[[0]], _ohlcv_rows().iloc[[0]]], ignore_index=True)
    duplicate.to_parquet(path, index=False)

    result = validate_processed_datasets(data_dir=data_dir)

    assert result.summary["overall_status"] == "fail"
    assert (
        "Processed OHLCV has duplicate instrument/date keys." in result.summary["blocking_issues"]
    )


def test_cleaned_ohlcv_excludes_invalid_rows(tmp_path: Path) -> None:
    data_dir = _write_minimal_validatable_data(tmp_path)

    result = validate_processed_datasets(data_dir=data_dir)
    cleaned = pd.read_parquet(data_dir / "processed/validated/ohlcv_daily_validated.parquet")

    assert len(cleaned) == 2
    assert result.summary["cleaned_ohlcv"]["dropped_row_count"] == 1
    assert result.summary["cleaned_ohlcv"]["invalid_row_count"] == 0


def test_low_coverage_dates_are_flagged() -> None:
    frame = normalize_ohlcv(_ohlcv_rows())
    frame = frame[frame["volume"].ge(0)].copy()

    coverage = build_date_coverage(
        frame,
        expected_count=2,
        pass_threshold=0.90,
        warn_threshold=0.70,
    )

    date_two = coverage[coverage["date"].astype(str).eq("2026-01-02")].iloc[0]
    assert date_two["coverage_status"] == "fail"
    assert bool(date_two["exclude_from_ml_by_default"]) is True


def test_feature_target_key_alignment_works() -> None:
    cleaned = normalize_ohlcv(_ohlcv_rows())
    cleaned = cleaned[cleaned["volume"].ge(0)].copy()
    features = cleaned[["instrument_key", "symbol", "date"]].copy()
    features["feature_x"] = [1.0, 2.0]
    targets = cleaned[["instrument_key", "symbol", "date"]].copy()
    targets["forward_ret_1d"] = [0.01, 0.02]

    joined, summary = validate_alignment(cleaned, features, targets)

    assert len(joined) == 2
    assert summary["status"] == "pass"
    assert summary["feature_target_joined_key_count"] == 2


def test_summary_json_contains_required_fields(tmp_path: Path) -> None:
    data_dir = _write_minimal_validatable_data(tmp_path)

    result = validate_processed_datasets(data_dir=data_dir)

    for field in [
        "overall_status",
        "baseline_ml_ready",
        "serious_research_ready",
        "production_ready",
        "blocking_issues",
        "warnings",
        "files_generated",
        "recommended_exclusions",
        "source_paths",
        "row_counts",
        "date_ranges",
        "symbol_counts",
    ]:
        assert field in result.summary
    assert result.summary["baseline_ml_ready"] is True
    assert result.summary["overall_status"] == "warn"


def test_pipeline_materializes_stock_coverage_before_ml_dataset(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = _write_minimal_validatable_data(tmp_path)
    monkeypatch.setattr(
        processed_validation,
        "get_settings",
        lambda: type(
            "Settings",
            (),
            {
                "data_dir": data_dir,
                "nse_daily_primary_source": "upstox",
            },
        )(),
    )

    result = processed_validation.run_processed_dataset_validation_pipeline(
        data_dir=data_dir,
        coverage_run_id="dagster-run-1",
    )

    coverage_path = data_dir / "processed/validation/daily_pipeline_stock_coverage.parquet"
    windows_path = (
        data_dir / "processed/validation/daily_pipeline_stock_coverage_windows.parquet"
    )
    assert coverage_path.exists()
    assert windows_path.exists()
    assert result.artifacts["stock_coverage"] == coverage_path
    assert result.metrics["stock_coverage"]["coverage_run_id"] == "dagster-run-1"
    coverage = pd.read_parquet(coverage_path)
    assert coverage["coverage_pct"].eq(1.0).all()


def _write_minimal_validatable_data(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    processed_path = data_dir / "processed/equities/nse_daily_ohlcv_upstox.parquet"
    feature_path = data_dir / "processed/features/daily_v1_ohlcv_technical.parquet"
    target_path = data_dir / "processed/targets/daily_v1_forward_returns.parquet"
    for path in [processed_path, feature_path, target_path]:
        path.parent.mkdir(parents=True, exist_ok=True)

    raw = _ohlcv_rows()
    raw.to_parquet(processed_path, index=False)

    cleaned_keys = normalize_ohlcv(raw)
    cleaned_keys = cleaned_keys[cleaned_keys["volume"].ge(0)][
        ["instrument_key", "symbol", "date"]
    ].copy()
    features = cleaned_keys.copy()
    features["feature_version"] = "v1"
    features["quality_status"] = "passed"
    features["feature_x"] = [1.0, 2.0]
    features.to_parquet(feature_path, index=False)

    targets = cleaned_keys.copy()
    targets["target_version"] = "v1"
    targets["quality_status"] = "passed"
    targets["forward_ret_1d"] = [0.01, 0.02]
    targets["forward_ret_5d"] = [0.01, 0.02]
    targets["forward_ret_10d"] = [0.01, 0.02]
    targets["forward_ret_20d"] = [0.01, 0.02]
    targets["forward_ret_60d"] = [0.01, 0.02]
    targets["top_quantile_forward_return_20d"] = [True, False]
    targets.to_parquet(target_path, index=False)

    validation_dir = data_dir / "processed/validation"
    validation_dir.mkdir(parents=True)
    (validation_dir / "raw_to_processed_validation_report.md").write_text("ok\n")
    (validation_dir / "raw_to_processed_metadata.json").write_text("{}\n")
    pd.DataFrame().to_parquet(validation_dir / "processed_ohlcv_invalid_rows.parquet")
    pd.DataFrame().to_parquet(validation_dir / "processed_ohlcv_symbol_health.parquet")
    pd.DataFrame().to_parquet(validation_dir / "processed_ohlcv_date_coverage.parquet")
    return data_dir
