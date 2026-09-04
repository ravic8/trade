from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
from pydantic import JsonValue

from trade_research.contracts import ML_INPUTS_CONTRACT_ID, get_data_contract
from trade_research.validation.results import (
    VALIDATION_REPORT_CONTRACT_VERSION,
    ValidationReport,
    ValidationResult,
    ValidationStatus,
)

PROCESSED_OHLCV = "processed/equities/nse_daily_ohlcv_upstox.parquet"
CLEANED_OHLCV = "processed/validated/ohlcv_daily_validated.parquet"
CLEANED_METADATA = "processed/validated/ohlcv_daily_validated_metadata.json"
FEATURES = "processed/features/daily_v1_ohlcv_technical.parquet"
TARGETS = "processed/targets/daily_v1_forward_returns.parquet"
VALIDATION_DIR = "processed/validation"
VALIDATION_RESULTS = "processed/validation/processed_dataset_validation_results_v1.json"
PROCESSED_DATASET_ID = ML_INPUTS_CONTRACT_ID

RAW_VALIDATION_FILES = [
    "processed/validation/raw_to_processed_validation_report.md",
    "processed/validation/raw_to_processed_metadata.json",
    "processed/validation/processed_ohlcv_invalid_rows.parquet",
    "processed/validation/processed_ohlcv_symbol_health.parquet",
    "processed/validation/processed_ohlcv_date_coverage.parquet",
]

TARGET_HORIZONS = {
    "forward_ret_1d": 1,
    "forward_ret_5d": 5,
    "forward_ret_10d": 10,
    "forward_ret_20d": 20,
    "forward_ret_60d": 60,
}


@dataclass(frozen=True)
class ProcessedDatasetValidationResult:
    summary: dict[str, Any]
    files_generated: list[str]
    validation_report: ValidationReport


def validate_processed_datasets(
    data_dir: Path | str = "data",
    pass_coverage_threshold: float = 0.90,
    warn_coverage_threshold: float = 0.70,
    processed_ohlcv: str = PROCESSED_OHLCV,
    run_id: str | None = None,
) -> ProcessedDatasetValidationResult:
    data_root = Path(data_dir)
    validation_run_id = run_id or f"processed-validation-{uuid4()}"
    created_at = datetime.now(UTC)
    data_contract = get_data_contract(PROCESSED_DATASET_ID)
    validation_dir = data_root / VALIDATION_DIR
    validation_dir.mkdir(parents=True, exist_ok=True)

    generated: list[Path] = []
    blocking: list[str] = []
    warnings: list[str] = []
    source_paths = _source_paths(data_root, processed_ohlcv=processed_ohlcv)
    files_inspected: dict[str, dict[str, Any]] = {}

    processed_path = data_root / processed_ohlcv
    cleaned_path = data_root / CLEANED_OHLCV
    feature_path = data_root / FEATURES
    target_path = data_root / TARGETS

    for name, path in source_paths.items():
        files_inspected[name] = {
            "path": str(path),
            "exists": path.exists(),
            "blocking_if_missing": name == "processed_ohlcv",
        }
        if name == "processed_ohlcv" and not path.exists():
            blocking.append(f"Missing processed OHLCV file: {path}")
        elif not path.exists():
            warnings.append(f"Missing optional input: {path}")

    if any(not (data_root / raw_file).exists() for raw_file in RAW_VALIDATION_FILES):
        warnings.append("Raw-to-processed validation outputs are incomplete or missing.")
    warnings.append("Raw Upstox API response files are not available for replay validation.")

    processed = pd.DataFrame()
    cleaned = pd.DataFrame()
    processed_summary: dict[str, Any] = {"status": "missing"}
    cleaned_summary: dict[str, Any] = {"status": "missing"}
    invalid_rows = pd.DataFrame()
    date_coverage = pd.DataFrame()
    symbol_health = pd.DataFrame()

    if processed_path.exists():
        processed = normalize_ohlcv(pd.read_parquet(processed_path))
        invalid_rows = find_invalid_ohlcv_rows(processed)
        processed_summary = summarize_ohlcv(processed, invalid_rows)
        if processed_summary["missing_key_rows"]:
            blocking.append("Processed OHLCV has missing key values.")
        if processed_summary["duplicate_key_count"]:
            blocking.append("Processed OHLCV has duplicate instrument/date keys.")
        if processed_summary["invalid_row_count"]:
            warnings.append(
                f"Processed OHLCV has {processed_summary['invalid_row_count']} invalid rows; "
                "they must be excluded before ML."
            )

        expected_cleaned = processed.loc[
            ~processed.index.isin(invalid_rows["_source_index"])
        ].copy()
        cleaned_created = False
        cleaned_refreshed = False
        if cleaned_path.exists():
            cleaned = normalize_ohlcv(pd.read_parquet(cleaned_path))
            if _key_set(cleaned) != _key_set(expected_cleaned):
                cleaned = expected_cleaned
                cleaned_refreshed = True
        else:
            cleaned = expected_cleaned
            cleaned_created = True

        if cleaned_created or cleaned_refreshed:
            cleaned_path.parent.mkdir(parents=True, exist_ok=True)
            cleaned.to_parquet(cleaned_path, index=False)
            generated.append(cleaned_path)
            cleaned_missing_warning = f"Missing optional input: {cleaned_path}"
            warnings = [warning for warning in warnings if warning != cleaned_missing_warning]
            files_inspected["cleaned_ohlcv"]["exists"] = True
            files_inspected["cleaned_ohlcv"]["created_this_run"] = True

        cleaned_invalid = find_invalid_ohlcv_rows(cleaned)
        cleaned_summary = summarize_ohlcv(cleaned, cleaned_invalid)
        cleaned_summary["created_this_run"] = cleaned_created
        cleaned_summary["refreshed_this_run"] = cleaned_refreshed
        cleaned_summary["preserves_all_valid_processed_rows"] = _key_set(cleaned) == _key_set(
            expected_cleaned
        )
        cleaned_summary["dropped_row_count"] = int(len(processed) - len(cleaned))
        cleaned_summary["expected_invalid_drop_count"] = int(len(invalid_rows))

        if cleaned_summary["duplicate_key_count"]:
            blocking.append("Cleaned OHLCV has duplicate instrument/date keys.")
        if cleaned_summary["invalid_row_count"]:
            blocking.append("Invalid OHLCV rows remain in cleaned OHLCV.")
        if not cleaned_summary["preserves_all_valid_processed_rows"]:
            blocking.append("Cleaned OHLCV does not preserve all valid processed rows.")

        cleaned_metadata = _cleaned_metadata(
            processed_path, processed, cleaned, invalid_rows, cleaned_summary
        )
        metadata_path = data_root / CLEANED_METADATA
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(cleaned_metadata, indent=2) + "\n", encoding="utf-8")
        generated.append(metadata_path)

        expected_count = max(int(cleaned["instrument_key"].nunique()), 1)
        date_coverage = build_date_coverage(
            cleaned,
            expected_count=expected_count,
            pass_threshold=pass_coverage_threshold,
            warn_threshold=warn_coverage_threshold,
        )
        date_coverage_path = validation_dir / "processed_dataset_date_coverage.parquet"
        date_coverage.to_parquet(date_coverage_path, index=False)
        generated.append(date_coverage_path)

        symbol_health = build_symbol_health(cleaned, latest_date=cleaned["date"].max())
        symbol_health_path = validation_dir / "processed_dataset_symbol_health.parquet"
        symbol_health.to_parquet(symbol_health_path, index=False)
        generated.append(symbol_health_path)

        latest = date_coverage[date_coverage["date"].eq(date_coverage["date"].max())]
        if not latest.empty and latest.iloc[0]["coverage_status"] != "pass":
            warnings.append(
                "Latest dataset date has low coverage: "
                f"{latest.iloc[0]['instrument_count']}/{latest.iloc[0]['expected_instrument_count']}."
            )
        low_dates = date_coverage[date_coverage["coverage_status"].isin(["warn", "fail"])]
        if not low_dates.empty:
            warnings.append(f"{len(low_dates)} dates have warning/failing stock coverage.")
        lagged_symbols = symbol_health[symbol_health["coverage_status"].isin(["warn", "fail"])]
        if not lagged_symbols.empty:
            warnings.append(f"{len(lagged_symbols)} symbols have health warnings/failures.")

    feature_summary, feature_missing = _missing_dataset_result("features")
    target_summary, target_missing, class_balance = _missing_target_result()
    alignment, alignment_summary = _missing_alignment_result()

    if feature_path.exists():
        features = normalize_keyed_frame(pd.read_parquet(feature_path))
        feature_summary, feature_missing = validate_feature_dataset(features, cleaned)
        feature_summary_path = validation_dir / "feature_dataset_validation.parquet"
        feature_missing_path = validation_dir / "feature_missing_values.parquet"
        pd.DataFrame([feature_summary]).to_parquet(feature_summary_path, index=False)
        feature_missing.to_parquet(feature_missing_path, index=False)
        generated.extend([feature_summary_path, feature_missing_path])
        if feature_summary["duplicate_key_count"]:
            blocking.append("Feature dataset has duplicate instrument/date keys.")
        if feature_summary["extra_keys_not_in_ohlcv"] or feature_summary["missing_keys_from_ohlcv"]:
            blocking.append("Feature keys do not materially align with cleaned OHLCV keys.")
        if feature_summary["inf_value_count"]:
            blocking.append("Feature dataset contains infinite numeric values.")
        if feature_summary["missing_value_count"]:
            warnings.append("Feature dataset contains rolling-window or other missing values.")
    else:
        warnings.append("Feature dataset is missing; feature alignment cannot be validated.")

    if target_path.exists():
        targets = normalize_keyed_frame(pd.read_parquet(target_path))
        target_summary, target_missing, class_balance = validate_target_dataset(targets, cleaned)
        target_summary_path = validation_dir / "target_dataset_validation.parquet"
        target_missing_path = validation_dir / "target_missing_values.parquet"
        class_balance_path = validation_dir / "target_class_balance.parquet"
        pd.DataFrame([target_summary]).to_parquet(target_summary_path, index=False)
        target_missing.to_parquet(target_missing_path, index=False)
        class_balance.to_parquet(class_balance_path, index=False)
        generated.extend([target_summary_path, target_missing_path, class_balance_path])
        if target_summary["duplicate_key_count"]:
            blocking.append("Target dataset has duplicate instrument/date keys.")
        if target_summary["extra_keys_not_in_ohlcv"] or target_summary["missing_keys_from_ohlcv"]:
            blocking.append("Target keys do not materially align with cleaned OHLCV keys.")
        if target_summary["inf_value_count"]:
            blocking.append("Target dataset contains infinite numeric values.")
        if target_summary["missing_value_count"]:
            warnings.append("Target dataset contains expected horizon-end missing values.")
    else:
        warnings.append("Target dataset is missing; target alignment cannot be validated.")

    if cleaned_path.exists() and feature_path.exists() and target_path.exists():
        features = normalize_keyed_frame(pd.read_parquet(feature_path))
        targets = normalize_keyed_frame(pd.read_parquet(target_path))
        cleaned_for_alignment = normalize_ohlcv(pd.read_parquet(cleaned_path))
        alignment, alignment_summary = validate_alignment(cleaned_for_alignment, features, targets)
        alignment_path = validation_dir / "feature_target_alignment.parquet"
        alignment_summary_path = validation_dir / "feature_target_alignment_summary.json"
        alignment.to_parquet(alignment_path, index=False)
        alignment_summary_path.write_text(
            json.dumps(alignment_summary, indent=2) + "\n",
            encoding="utf-8",
        )
        generated.extend([alignment_path, alignment_summary_path])
        if alignment_summary["duplicate_joined_keys"]:
            blocking.append("Feature-target join produces duplicate keys.")
        for key in [
            "feature_keys_missing_from_ohlcv",
            "target_keys_missing_from_ohlcv",
            "ohlcv_keys_missing_from_features",
            "ohlcv_keys_missing_from_targets",
        ]:
            if alignment_summary[key]:
                blocking.append(f"Alignment issue: {key}={alignment_summary[key]}")

    recommended_exclusions = {
        "invalid_rows": _invalid_exclusions(invalid_rows),
        "low_coverage_dates": _date_exclusions(date_coverage),
        "failed_symbols": _symbol_exclusions(symbol_health),
    }

    row_counts = {
        "processed_ohlcv": int(len(processed)),
        "cleaned_ohlcv": int(len(cleaned)),
        "features": int(feature_summary.get("row_count", 0) or 0),
        "targets": int(target_summary.get("row_count", 0) or 0),
    }
    date_ranges = {
        "processed_ohlcv": _date_range(processed),
        "cleaned_ohlcv": _date_range(cleaned),
        "features": feature_summary.get("date_range"),
        "targets": target_summary.get("date_range"),
        "feature_target_join": alignment_summary.get("date_range"),
    }
    symbol_counts = {
        "processed_ohlcv": int(processed["instrument_key"].nunique()) if not processed.empty else 0,
        "cleaned_ohlcv": int(cleaned["instrument_key"].nunique()) if not cleaned.empty else 0,
        "features": int(feature_summary.get("instrument_count", 0) or 0),
        "targets": int(target_summary.get("instrument_count", 0) or 0),
        "feature_target_join": int(alignment_summary.get("instrument_count", 0) or 0),
    }

    summary_json = validation_dir / "processed_dataset_validation_summary.json"
    summary_md = validation_dir / "processed_dataset_validation_summary.md"
    validation_results_path = data_root / VALIDATION_RESULTS
    generated.extend([summary_json, summary_md, validation_results_path])

    overall_status = "fail" if blocking else ("warn" if warnings else "pass")
    baseline_ready = (
        not blocking and bool(len(cleaned)) and feature_path.exists() and target_path.exists()
    )
    validation_report = _build_processed_dataset_validation_report(
        run_id=validation_run_id,
        created_at=created_at,
        scope={
            "exchange": "NSE",
            "processed_ohlcv": processed_ohlcv,
            "data_contract_id": data_contract.contract_id,
            "dataset_version": data_contract.dataset_version,
        },
        processed_exists=processed_path.exists(),
        processed_summary=processed_summary,
        cleaned=cleaned,
        cleaned_summary=cleaned_summary,
        feature_exists=feature_path.exists(),
        feature_summary=feature_summary,
        target_exists=target_path.exists(),
        target_summary=target_summary,
        alignment_summary=alignment_summary,
        baseline_ready=bool(baseline_ready),
        warnings=sorted(set(warnings)),
    )
    summary = {
        "generated_at": created_at.isoformat(),
        "overall_status": overall_status,
        "validation_contract_version": VALIDATION_REPORT_CONTRACT_VERSION,
        "validation_run_id": validation_run_id,
        "validation_status": validation_report.status,
        "validation_results_path": str(validation_results_path),
        "data_contract_id": data_contract.contract_id,
        "data_contract_schema_version": data_contract.schema_version,
        "dataset_version": data_contract.dataset_version,
        "baseline_ml_ready": bool(baseline_ready),
        "serious_research_ready": bool(baseline_ready and not warnings),
        "production_ready": False,
        "blocking_issues": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "files_inspected": files_inspected,
        "files_generated": [str(path) for path in generated],
        "recommended_exclusions": recommended_exclusions,
        "source_paths": {key: str(value) for key, value in source_paths.items()},
        "row_counts": row_counts,
        "date_ranges": date_ranges,
        "symbol_counts": symbol_counts,
        "processed_ohlcv": processed_summary,
        "cleaned_ohlcv": cleaned_summary,
        "date_coverage": _coverage_summary(date_coverage),
        "symbol_health": _symbol_health_summary(symbol_health),
        "features": feature_summary,
        "targets": target_summary,
        "alignment": alignment_summary,
    }

    validation_results_path.write_text(
        validation_report.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    summary_md.write_text(_summary_markdown(summary), encoding="utf-8")

    return ProcessedDatasetValidationResult(
        summary=summary,
        files_generated=[str(path) for path in generated],
        validation_report=validation_report,
    )


def _build_processed_dataset_validation_report(
    *,
    run_id: str,
    created_at: datetime,
    scope: dict[str, JsonValue],
    processed_exists: bool,
    processed_summary: dict[str, Any],
    cleaned: pd.DataFrame,
    cleaned_summary: dict[str, Any],
    feature_exists: bool,
    feature_summary: dict[str, Any],
    target_exists: bool,
    target_summary: dict[str, Any],
    alignment_summary: dict[str, Any],
    baseline_ready: bool,
    warnings: list[str],
) -> ValidationReport:
    dataset_id = PROCESSED_DATASET_ID
    processed_row_count = int(processed_summary.get("row_count", 0) or 0)
    processed_key_issues: dict[str, JsonValue] = {
        "missing_key_rows": int(processed_summary.get("missing_key_rows", 0) or 0),
        "duplicate_key_count": int(
            processed_summary.get("duplicate_key_count", 0) or 0
        ),
    }
    cleaned_row_count = int(len(cleaned))
    cleaned_invalid_row_count = int(
        cleaned_summary.get("invalid_row_count", 0) or 0
    )
    cleaned_duplicate_key_count = int(
        cleaned_summary.get("duplicate_key_count", 0) or 0
    )
    cleaned_preserves_rows = bool(
        cleaned_summary.get("preserves_all_valid_processed_rows", False)
    )
    cleaned_ready = (
        cleaned_row_count > 0
        and cleaned_invalid_row_count == 0
        and cleaned_duplicate_key_count == 0
        and cleaned_preserves_rows
    )
    cleaned_invariants: dict[str, JsonValue] = {
        "row_count": cleaned_row_count,
        "invalid_row_count": cleaned_invalid_row_count,
        "duplicate_key_count": cleaned_duplicate_key_count,
        "preserves_all_valid_processed_rows": cleaned_preserves_rows,
    }
    feature_invariants = _dataset_invariant_counts(feature_summary)
    target_invariants = _dataset_invariant_counts(target_summary)
    alignment_invariants: dict[str, JsonValue] = {
        key: int(alignment_summary.get(key, 0) or 0)
        for key in [
            "duplicate_joined_keys",
            "feature_keys_missing_from_ohlcv",
            "target_keys_missing_from_ohlcv",
            "ohlcv_keys_missing_from_features",
            "ohlcv_keys_missing_from_targets",
        ]
    }

    warning_evidence: list[JsonValue] = list(warnings)
    results = [
        ValidationResult(
            check_id="processed_ohlcv.required_input",
            dataset_id=dataset_id,
            run_id=run_id,
            scope=scope,
            severity="error",
            status="passed" if processed_exists else "failed",
            observed_value=processed_exists,
            expected_value=True,
            message=(
                "Processed OHLCV input exists."
                if processed_exists
                else "Processed OHLCV input is missing."
            ),
            evidence={"row_count": processed_row_count},
            created_at=created_at,
        ),
        ValidationResult(
            check_id="processed_ohlcv.unique_complete_keys",
            dataset_id=dataset_id,
            run_id=run_id,
            scope=scope,
            severity="error",
            status=(
                "skipped_with_reason"
                if not processed_exists
                else (
                    "passed"
                    if processed_row_count > 0
                    and not any(processed_key_issues.values())
                    else "failed"
                )
            ),
            observed_value=processed_key_issues,
            expected_value={"missing_key_rows": 0, "duplicate_key_count": 0},
            message=(
                "Key validation skipped because processed OHLCV is missing."
                if not processed_exists
                else (
                    "Processed OHLCV keys are populated and unique."
                    if processed_row_count > 0 and not any(processed_key_issues.values())
                    else "Processed OHLCV keys are empty, missing, or duplicated."
                )
            ),
            evidence={"row_count": processed_row_count},
            created_at=created_at,
        ),
        ValidationResult(
            check_id="cleaned_ohlcv.valid_preserved_rows",
            dataset_id=dataset_id,
            run_id=run_id,
            scope=scope,
            severity="error",
            status=(
                "skipped_with_reason"
                if not processed_exists
                else (
                    "passed"
                    if cleaned_ready
                    else "failed"
                )
            ),
            observed_value=cleaned_invariants,
            expected_value={
                "minimum_row_count": 1,
                "invalid_row_count": 0,
                "duplicate_key_count": 0,
                "preserves_all_valid_processed_rows": True,
            },
            message=(
                "Cleaned OHLCV validation skipped because processed OHLCV is missing."
                if not processed_exists
                else (
                    "Cleaned OHLCV contains valid, unique, preserved rows."
                    if cleaned_ready
                    else "Cleaned OHLCV is empty or violates row-preservation invariants."
                )
            ),
            evidence={},
            created_at=created_at,
        ),
        _dataset_contract_result(
            check_id="features.dataset_contract",
            dataset_id=dataset_id,
            run_id=run_id,
            scope=scope,
            exists=feature_exists,
            invariants=feature_invariants,
            missing_value_count=int(feature_summary.get("missing_value_count", 0) or 0),
            created_at=created_at,
        ),
        _dataset_contract_result(
            check_id="targets.dataset_contract",
            dataset_id=dataset_id,
            run_id=run_id,
            scope=scope,
            exists=target_exists,
            invariants=target_invariants,
            missing_value_count=int(target_summary.get("missing_value_count", 0) or 0),
            created_at=created_at,
        ),
        ValidationResult(
            check_id="features_targets.key_alignment",
            dataset_id=dataset_id,
            run_id=run_id,
            scope=scope,
            severity="error",
            status=(
                "skipped_with_reason"
                if not (len(cleaned) and feature_exists and target_exists)
                else ("passed" if not any(alignment_invariants.values()) else "failed")
            ),
            observed_value=alignment_invariants,
            expected_value={key: 0 for key in alignment_invariants},
            message=(
                "Feature-target alignment skipped because a required dataset is unavailable."
                if not (len(cleaned) and feature_exists and target_exists)
                else (
                    "OHLCV, feature, and target keys align."
                    if not any(alignment_invariants.values())
                    else "OHLCV, feature, and target keys do not align."
                )
            ),
            evidence={
                "joined_key_count": int(
                    alignment_summary.get("feature_target_joined_key_count", 0) or 0
                )
            },
            created_at=created_at,
        ),
        ValidationResult(
            check_id="processed_datasets.baseline_ml_ready",
            dataset_id=dataset_id,
            run_id=run_id,
            scope=scope,
            severity="error",
            status="passed" if baseline_ready else "failed",
            observed_value=baseline_ready,
            expected_value=True,
            message=(
                "Processed datasets satisfy the baseline ML prerequisites."
                if baseline_ready
                else "Processed datasets do not satisfy the baseline ML prerequisites."
            ),
            evidence={},
            created_at=created_at,
        ),
        ValidationResult(
            check_id="processed_datasets.compatibility_advisories",
            dataset_id=dataset_id,
            run_id=run_id,
            scope=scope,
            severity="warning",
            status="warning" if warnings else "passed",
            observed_value=len(warnings),
            expected_value=0,
            message=(
                f"Processed validation emitted {len(warnings)} compatibility advisories."
                if warnings
                else "Processed validation emitted no compatibility advisories."
            ),
            evidence={"warnings": warning_evidence},
            created_at=created_at,
        ),
    ]
    return ValidationReport(
        dataset_id=dataset_id,
        run_id=run_id,
        results=tuple(results),
        created_at=created_at,
    )


def _dataset_invariant_counts(summary: dict[str, Any]) -> dict[str, JsonValue]:
    return {
        key: int(summary.get(key, 0) or 0)
        for key in [
            "duplicate_key_count",
            "missing_keys_from_ohlcv",
            "extra_keys_not_in_ohlcv",
            "inf_value_count",
        ]
    }


def _dataset_contract_result(
    *,
    check_id: str,
    dataset_id: str,
    run_id: str,
    scope: dict[str, JsonValue],
    exists: bool,
    invariants: dict[str, JsonValue],
    missing_value_count: int,
    created_at: datetime,
) -> ValidationResult:
    status: ValidationStatus
    if not exists:
        status = "skipped_with_reason"
        message = f"{check_id} skipped because its dataset is missing."
    elif any(invariants.values()):
        status = "failed"
        message = f"{check_id} failed key, alignment, or finite-value invariants."
    elif missing_value_count:
        status = "warning"
        message = f"{check_id} passed blocking invariants with expected missing values."
    else:
        status = "passed"
        message = f"{check_id} passed."
    return ValidationResult(
        check_id=check_id,
        dataset_id=dataset_id,
        run_id=run_id,
        scope=scope,
        severity="error",
        status=status,
        observed_value={**invariants, "missing_value_count": missing_value_count},
        expected_value={
            **{key: 0 for key in invariants},
            "missing_value_count": "contract-dependent",
        },
        message=message,
        evidence={},
        created_at=created_at,
    )


def normalize_ohlcv(frame: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "InstrumentKey": "instrument_key",
        "Date": "date",
        "Symbol": "symbol",
        "TradingSymbol": "trading_symbol",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "OpenInterest": "open_interest",
        "Source": "source",
        "Exchange": "exchange",
    }
    out = frame.rename(columns=rename).copy()
    required = {"instrument_key", "date", "symbol", "open", "high", "low", "close", "volume"}
    missing = sorted(required - set(out.columns))
    if missing:
        out["_schema_error"] = f"missing required columns: {missing}"
        return out
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    for column in ["open", "high", "low", "close", "volume", "open_interest"]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in ["instrument_key", "symbol", "trading_symbol", "source", "exchange"]:
        if column in out.columns:
            out[column] = out[column].astype("string")
    return out


def normalize_keyed_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.rename(columns={"InstrumentKey": "instrument_key", "Date": "date"}).copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    if "instrument_key" in out.columns:
        out["instrument_key"] = out["instrument_key"].astype("string")
    if "symbol" in out.columns:
        out["symbol"] = out["symbol"].astype("string")
    return out


def find_invalid_ohlcv_rows(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"instrument_key", "date", "symbol", "open", "high", "low", "close", "volume"}
    if missing := sorted(required - set(frame.columns)):
        return pd.DataFrame(
            [{"_source_index": -1, "invalid_reasons": f"missing required columns: {missing}"}]
        )

    checks = {
        "missing_instrument_key": frame["instrument_key"].isna() | frame["instrument_key"].eq(""),
        "missing_symbol": frame["symbol"].isna() | frame["symbol"].eq(""),
        "missing_date": frame["date"].isna(),
        "null_ohlcv": frame[["open", "high", "low", "close", "volume"]].isna().any(axis=1),
        "open_not_positive": frame["open"] <= 0,
        "high_not_positive": frame["high"] <= 0,
        "low_not_positive": frame["low"] <= 0,
        "close_not_positive": frame["close"] <= 0,
        "negative_volume": frame["volume"] < 0,
        "high_below_low": frame["high"] < frame["low"],
        "high_below_open": frame["high"] < frame["open"],
        "high_below_close": frame["high"] < frame["close"],
        "low_above_open": frame["low"] > frame["open"],
        "low_above_close": frame["low"] > frame["close"],
    }
    invalid = pd.Series(False, index=frame.index)
    reasons = pd.Series("", index=frame.index, dtype="string")
    for reason, mask in checks.items():
        mask = mask.fillna(False)
        invalid |= mask
        reasons.loc[mask] = (
            reasons.loc[mask].where(
                reasons.loc[mask].eq(""),
                reasons.loc[mask] + ";",
            )
            + reason
        )
    out = frame.loc[invalid].copy()
    out["_source_index"] = out.index
    out["invalid_reasons"] = reasons.loc[invalid].to_numpy()
    return out.reset_index(drop=True)


def summarize_ohlcv(frame: pd.DataFrame, invalid_rows: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"status": "empty", "row_count": 0}
    duplicate_count = int(frame.duplicated(["instrument_key", "date"]).sum())
    jumps = _suspicious_jump_count(frame)
    stale = _stale_close_count(frame)
    return {
        "status": "fail" if len(invalid_rows) or duplicate_count else "pass",
        "row_count": int(len(frame)),
        "symbol_count": int(frame["symbol"].nunique(dropna=True)),
        "instrument_count": int(frame["instrument_key"].nunique(dropna=True)),
        "date_range": _date_range(frame),
        "duplicate_key_count": duplicate_count,
        "missing_key_rows": int(frame[["instrument_key", "date"]].isna().any(axis=1).sum()),
        "invalid_row_count": int(len(invalid_rows)),
        "invalid_ohlc_rows": int(
            (
                (frame["open"] <= 0)
                | (frame["high"] <= 0)
                | (frame["low"] <= 0)
                | (frame["close"] <= 0)
                | (frame["high"] < frame["low"])
                | (frame["high"] < frame["open"])
                | (frame["high"] < frame["close"])
                | (frame["low"] > frame["open"])
                | (frame["low"] > frame["close"])
            ).sum()
        ),
        "negative_volume_rows": int((frame["volume"] < 0).sum()),
        "zero_volume_rows": int((frame["volume"] == 0).sum()),
        "null_ohlcv_rows": int(
            frame[["open", "high", "low", "close", "volume"]].isna().any(axis=1).sum()
        ),
        "suspicious_large_price_jump_rows": jumps,
        "stale_close_sequence_rows": stale,
    }


def build_date_coverage(
    frame: pd.DataFrame,
    expected_count: int,
    pass_threshold: float,
    warn_threshold: float,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    out = (
        frame.groupby("date")
        .agg(
            row_count=("instrument_key", "size"),
            instrument_count=("instrument_key", "nunique"),
        )
        .reset_index()
        .sort_values("date")
    )
    out["expected_instrument_count"] = expected_count
    out["missing_instrument_count"] = expected_count - out["instrument_count"]
    out["coverage_pct"] = out["instrument_count"] / expected_count
    out["coverage_status"] = np.select(
        [out["coverage_pct"] >= pass_threshold, out["coverage_pct"] >= warn_threshold],
        ["pass", "warn"],
        default="fail",
    )
    out["exclude_from_ml_by_default"] = out["coverage_status"].eq("fail")
    out["warn_for_ml"] = out["coverage_status"].eq("warn")
    out["is_latest_date"] = out["date"].eq(out["date"].max())
    return out.reset_index(drop=True)


def build_symbol_health(frame: pd.DataFrame, latest_date: Any) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    invalid = find_invalid_ohlcv_rows(frame)
    invalid_counts = invalid.groupby("instrument_key").size().rename("invalid_ohlc_row_count")
    out = (
        frame.groupby(["instrument_key", "symbol"], dropna=False)
        .agg(
            row_count=("date", "size"),
            min_date=("date", "min"),
            max_date=("date", "max"),
            unique_date_count=("date", "nunique"),
            zero_volume_row_count=("volume", lambda s: int((s == 0).sum())),
            negative_volume_row_count=("volume", lambda s: int((s < 0).sum())),
        )
        .reset_index()
    )
    out = out.merge(invalid_counts.reset_index(), on="instrument_key", how="left")
    out["invalid_ohlc_row_count"] = out["invalid_ohlc_row_count"].fillna(0).astype(int)
    latest_ts = pd.Timestamp(latest_date)
    out["latest_date_lag_days"] = (
        (latest_ts - pd.to_datetime(out["max_date"], errors="coerce")).dt.days.fillna(0).astype(int)
    )
    out["missing_date_count"] = out["row_count"].max() - out["row_count"]
    out["coverage_status"] = "pass"
    out.loc[out["latest_date_lag_days"].gt(0), "coverage_status"] = "warn"
    out.loc[
        out["invalid_ohlc_row_count"].gt(0) | out["negative_volume_row_count"].gt(0),
        "coverage_status",
    ] = "fail"
    return out.sort_values(["coverage_status", "symbol"]).reset_index(drop=True)


def validate_feature_dataset(
    features: pd.DataFrame, cleaned: pd.DataFrame
) -> tuple[dict[str, Any], pd.DataFrame]:
    numeric = features.select_dtypes(include=[np.number])
    missing = _missing_values(features)
    ohlcv_keys = _key_set(cleaned)
    feature_keys = _key_set(features)
    quality_counts = (
        features["quality_status"].value_counts(dropna=False).to_dict()
        if "quality_status" in features
        else {}
    )
    summary = {
        "status": "pass",
        "row_count": int(len(features)),
        "instrument_count": int(features["instrument_key"].nunique())
        if "instrument_key" in features
        else 0,
        "date_range": _date_range(features),
        "duplicate_key_count": int(features.duplicated(["instrument_key", "date"]).sum()),
        "missing_keys_from_ohlcv": int(len(ohlcv_keys - feature_keys)),
        "extra_keys_not_in_ohlcv": int(len(feature_keys - ohlcv_keys)),
        "inf_value_count": int(np.isinf(numeric).sum().sum()) if not numeric.empty else 0,
        "missing_value_count": int(features.isna().sum().sum()),
        "expected_rolling_window_nulls": int(_expected_feature_nulls(features)),
        "unexpected_nulls_after_warmup": int(_unexpected_feature_nulls(features)),
        "quality_status_counts": {str(k): int(v) for k, v in quality_counts.items()},
    }
    if (
        summary["duplicate_key_count"]
        or summary["missing_keys_from_ohlcv"]
        or summary["extra_keys_not_in_ohlcv"]
        or summary["inf_value_count"]
    ):
        summary["status"] = "fail"
    elif summary["missing_value_count"]:
        summary["status"] = "warn"
    return summary, missing


def validate_target_dataset(
    targets: pd.DataFrame, cleaned: pd.DataFrame
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    numeric = targets.select_dtypes(include=[np.number])
    missing = _missing_values(targets)
    ohlcv_keys = _key_set(cleaned)
    target_keys = _key_set(targets)
    horizon_nulls = {
        column: int(targets[column].isna().sum())
        for column in TARGET_HORIZONS
        if column in targets.columns
    }
    class_balance = _class_balance(targets)
    summary = {
        "status": "pass",
        "row_count": int(len(targets)),
        "instrument_count": int(targets["instrument_key"].nunique())
        if "instrument_key" in targets
        else 0,
        "date_range": _date_range(targets),
        "duplicate_key_count": int(targets.duplicated(["instrument_key", "date"]).sum()),
        "missing_keys_from_ohlcv": int(len(ohlcv_keys - target_keys)),
        "extra_keys_not_in_ohlcv": int(len(target_keys - ohlcv_keys)),
        "inf_value_count": int(np.isinf(numeric).sum().sum()) if not numeric.empty else 0,
        "missing_value_count": int(targets.isna().sum().sum()),
        "expected_horizon_end_nulls": horizon_nulls,
    }
    if (
        summary["duplicate_key_count"]
        or summary["missing_keys_from_ohlcv"]
        or summary["extra_keys_not_in_ohlcv"]
        or summary["inf_value_count"]
    ):
        summary["status"] = "fail"
    elif summary["missing_value_count"]:
        summary["status"] = "warn"
    return summary, missing, class_balance


def validate_alignment(
    cleaned: pd.DataFrame,
    features: pd.DataFrame,
    targets: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    ohlcv_keys = _key_set(cleaned)
    feature_keys = _key_set(features)
    target_keys = _key_set(targets)
    joined_keys = feature_keys & target_keys & ohlcv_keys
    joined = pd.DataFrame(sorted(joined_keys), columns=["instrument_key", "date"])
    duplicate_joined = (
        int(joined.duplicated(["instrument_key", "date"]).sum()) if not joined.empty else 0
    )
    summary = {
        "status": "pass",
        "cleaned_ohlcv_key_count": int(len(ohlcv_keys)),
        "feature_key_count": int(len(feature_keys)),
        "target_key_count": int(len(target_keys)),
        "feature_target_joined_key_count": int(len(joined_keys)),
        "feature_keys_missing_from_ohlcv": int(len(feature_keys - ohlcv_keys)),
        "target_keys_missing_from_ohlcv": int(len(target_keys - ohlcv_keys)),
        "ohlcv_keys_missing_from_features": int(len(ohlcv_keys - feature_keys)),
        "ohlcv_keys_missing_from_targets": int(len(ohlcv_keys - target_keys)),
        "duplicate_joined_keys": duplicate_joined,
        "date_range": _date_range(joined),
        "instrument_count": int(joined["instrument_key"].nunique()) if not joined.empty else 0,
    }
    if any(
        summary[key]
        for key in [
            "feature_keys_missing_from_ohlcv",
            "target_keys_missing_from_ohlcv",
            "ohlcv_keys_missing_from_features",
            "ohlcv_keys_missing_from_targets",
            "duplicate_joined_keys",
        ]
    ):
        summary["status"] = "fail"
    return joined, summary


def _source_paths(data_root: Path, *, processed_ohlcv: str = PROCESSED_OHLCV) -> dict[str, Path]:
    paths = {
        "processed_ohlcv": data_root / processed_ohlcv,
        "cleaned_ohlcv": data_root / CLEANED_OHLCV,
        "features": data_root / FEATURES,
        "targets": data_root / TARGETS,
    }
    paths.update(
        {
            f"raw_validation_{i}": data_root / file
            for i, file in enumerate(RAW_VALIDATION_FILES, start=1)
        }
    )
    return paths


def _key_set(frame: pd.DataFrame) -> set[tuple[str, Any]]:
    if frame.empty or "instrument_key" not in frame or "date" not in frame:
        return set()
    keys = frame[["instrument_key", "date"]].dropna().drop_duplicates()
    return set(map(tuple, keys.astype({"instrument_key": "string"}).to_numpy().tolist()))


def _date_range(frame: pd.DataFrame) -> dict[str, str | None]:
    if frame.empty or "date" not in frame:
        return {"min": None, "max": None}
    dates = pd.to_datetime(frame["date"], errors="coerce")
    return {
        "min": None if dates.isna().all() else str(dates.min().date()),
        "max": None if dates.isna().all() else str(dates.max().date()),
    }


def _missing_values(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    numeric = frame.select_dtypes(include=[np.number])
    inf_counts = np.isinf(numeric).sum().to_dict() if not numeric.empty else {}
    for column in frame.columns:
        rows.append(
            {
                "column": column,
                "missing_count": int(frame[column].isna().sum()),
                "missing_pct": float(frame[column].isna().mean()),
                "inf_count": int(inf_counts.get(column, 0)),
            }
        )
    return pd.DataFrame(rows)


def _class_balance(targets: pd.DataFrame) -> pd.DataFrame:
    labels = []
    for column in targets.columns:
        if column == "top_quantile_forward_return_20d":
            labels.append(column)
            continue
        if "outperform" not in column:
            continue
        unique_count = targets[column].nunique(dropna=True)
        if pd.api.types.is_bool_dtype(targets[column]) or unique_count <= 20:
            labels.append(column)
    rows = []
    for label in labels:
        counts = targets[label].value_counts(dropna=False)
        total = int(counts.sum())
        for value, count in counts.items():
            rows.append(
                {
                    "label": label,
                    "value": str(value),
                    "count": int(count),
                    "pct": float(count / total) if total else 0.0,
                }
            )
    return pd.DataFrame(rows, columns=["label", "value", "count", "pct"])


def _expected_feature_nulls(features: pd.DataFrame) -> int:
    if "quality_status" not in features:
        return 0
    return int(features.loc[features["quality_status"].eq("warning")].isna().sum().sum())


def _unexpected_feature_nulls(features: pd.DataFrame) -> int:
    if "quality_status" not in features:
        return int(features.isna().sum().sum())
    return int(features.loc[features["quality_status"].ne("warning")].isna().sum().sum())


def _suspicious_jump_count(frame: pd.DataFrame, threshold: float = 0.75) -> int:
    if frame.empty:
        return 0
    ordered = frame.sort_values(["instrument_key", "date"])
    returns = ordered.groupby("instrument_key")["close"].pct_change()
    return int(returns.abs().gt(threshold).sum())


def _stale_close_count(frame: pd.DataFrame, min_run: int = 5) -> int:
    if frame.empty:
        return 0
    stale_rows = 0
    for _, group in frame.sort_values(["instrument_key", "date"]).groupby("instrument_key"):
        same = group["close"].eq(group["close"].shift())
        run_id = same.ne(same.shift()).cumsum()
        stale_rows += int(same.groupby(run_id).transform("sum").ge(min_run).sum())
    return stale_rows


def _cleaned_metadata(
    source_path: Path,
    processed: pd.DataFrame,
    cleaned: pd.DataFrame,
    invalid_rows: pd.DataFrame,
    cleaned_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source_file": str(source_path),
        "generated_at": datetime.now(UTC).isoformat(),
        "original_row_count": int(len(processed)),
        "cleaned_row_count": int(len(cleaned)),
        "dropped_row_count": int(len(processed) - len(cleaned)),
        "invalid_row_count": int(len(invalid_rows)),
        "duplicate_count": int(cleaned_summary.get("duplicate_key_count", 0)),
        "date_range": cleaned_summary.get("date_range"),
        "instrument_count": int(cleaned_summary.get("instrument_count", 0)),
        "validation_status": cleaned_summary.get("status"),
    }


def _coverage_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"status": "missing"}
    latest = frame.loc[frame["is_latest_date"]].iloc[0].to_dict()
    return {
        "date_count": int(len(frame)),
        "pass_dates": int(frame["coverage_status"].eq("pass").sum()),
        "warn_dates": int(frame["coverage_status"].eq("warn").sum()),
        "fail_dates": int(frame["coverage_status"].eq("fail").sum()),
        "latest_date": str(latest["date"]),
        "latest_date_coverage_pct": float(latest["coverage_pct"]),
        "latest_date_status": str(latest["coverage_status"]),
        "exclude_from_ml_date_count": int(frame["exclude_from_ml_by_default"].sum()),
        "warn_for_ml_date_count": int(frame["warn_for_ml"].sum()),
    }


def _symbol_health_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"status": "missing"}
    return {
        "symbol_count": int(len(frame)),
        "pass_symbols": int(frame["coverage_status"].eq("pass").sum()),
        "warn_symbols": int(frame["coverage_status"].eq("warn").sum()),
        "fail_symbols": int(frame["coverage_status"].eq("fail").sum()),
    }


def _invalid_exclusions(invalid_rows: pd.DataFrame) -> list[dict[str, Any]]:
    if invalid_rows.empty or "instrument_key" not in invalid_rows:
        return []
    columns = ["instrument_key", "symbol", "date", "invalid_reasons"]
    available = [column for column in columns if column in invalid_rows]
    rows = invalid_rows[available].copy()
    rows["date"] = rows["date"].astype(str)
    return rows.to_dict(orient="records")


def _date_exclusions(date_coverage: pd.DataFrame) -> list[dict[str, Any]]:
    if date_coverage.empty:
        return []
    rows = date_coverage[date_coverage["coverage_status"].isin(["warn", "fail"])][
        ["date", "coverage_pct", "coverage_status", "exclude_from_ml_by_default"]
    ].copy()
    rows["date"] = rows["date"].astype(str)
    return rows.to_dict(orient="records")


def _symbol_exclusions(symbol_health: pd.DataFrame) -> list[dict[str, Any]]:
    if symbol_health.empty:
        return []
    rows = symbol_health[symbol_health["coverage_status"].eq("fail")][
        ["instrument_key", "symbol", "coverage_status"]
    ].copy()
    return rows.to_dict(orient="records")


def _missing_dataset_result(name: str) -> tuple[dict[str, Any], pd.DataFrame]:
    return {"status": "missing", "dataset": name, "row_count": 0}, pd.DataFrame()


def _missing_target_result() -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    return (
        {"status": "missing", "dataset": "targets", "row_count": 0},
        pd.DataFrame(),
        pd.DataFrame(),
    )


def _missing_alignment_result() -> tuple[pd.DataFrame, dict[str, Any]]:
    return pd.DataFrame(), {"status": "missing"}


def _summary_markdown(summary: dict[str, Any]) -> str:
    blocking = summary["blocking_issues"] or ["None"]
    warnings = summary["warnings"] or ["None"]
    lines = [
        "# Processed Dataset Validation Summary",
        "",
        "## Executive Summary",
        "",
        f"- Overall status: {summary['overall_status']}",
        f"- Baseline ML ready: {summary['baseline_ml_ready']}",
        f"- Serious research ready: {summary['serious_research_ready']}",
        f"- Production ready: {summary['production_ready']}",
        "",
        "## Files Inspected",
        "",
    ]
    for name, meta in summary["files_inspected"].items():
        status = "found" if meta["exists"] else "missing"
        blocking_text = "blocking" if meta["blocking_if_missing"] else "non-blocking"
        lines.append(f"- {name}: {status} ({blocking_text}) `{meta['path']}`")
    lines.extend(
        [
            "",
            "## Processed OHLCV Status",
            "",
            f"- Rows: {summary['row_counts']['processed_ohlcv']}",
            f"- Status: {summary['processed_ohlcv'].get('status')}",
            f"- Invalid rows: {summary['processed_ohlcv'].get('invalid_row_count', 0)}",
            f"- Duplicate keys: {summary['processed_ohlcv'].get('duplicate_key_count', 0)}",
            "",
            "## Cleaned OHLCV Status",
            "",
            f"- Rows: {summary['row_counts']['cleaned_ohlcv']}",
            f"- Status: {summary['cleaned_ohlcv'].get('status')}",
            f"- Created this run: {summary['cleaned_ohlcv'].get('created_this_run')}",
            f"- Dropped rows: {summary['cleaned_ohlcv'].get('dropped_row_count', 0)}",
            "",
            "## Date-Level Coverage",
            "",
            f"- Pass dates: {summary['date_coverage'].get('pass_dates', 0)}",
            f"- Warn dates: {summary['date_coverage'].get('warn_dates', 0)}",
            f"- Fail dates: {summary['date_coverage'].get('fail_dates', 0)}",
            f"- Latest date status: {summary['date_coverage'].get('latest_date_status')}",
            "",
            "## Symbol-Level Health",
            "",
            f"- Pass symbols: {summary['symbol_health'].get('pass_symbols', 0)}",
            f"- Warn symbols: {summary['symbol_health'].get('warn_symbols', 0)}",
            f"- Fail symbols: {summary['symbol_health'].get('fail_symbols', 0)}",
            "",
            "## Feature Dataset Status",
            "",
            f"- Status: {summary['features'].get('status')}",
            f"- Rows: {summary['row_counts']['features']}",
            f"- Missing values: {summary['features'].get('missing_value_count', 0)}",
            "",
            "## Target Dataset Status",
            "",
            f"- Status: {summary['targets'].get('status')}",
            f"- Rows: {summary['row_counts']['targets']}",
            f"- Missing values: {summary['targets'].get('missing_value_count', 0)}",
            "",
            "## Feature-Target-OHLCV Alignment",
            "",
            f"- Status: {summary['alignment'].get('status')}",
            f"- Joined keys: {summary['alignment'].get('feature_target_joined_key_count', 0)}",
            f"- Duplicate joined keys: {summary['alignment'].get('duplicate_joined_keys', 0)}",
            "",
            "## Blocking Issues",
            "",
        ]
    )
    lines.extend(f"- {issue}" for issue in blocking)
    lines.extend(["", "## Non-Blocking Warnings", ""])
    lines.extend(f"- {warning}" for warning in warnings)
    lines.extend(
        [
            "",
            "## ML Dataset Builder Next Step",
            "",
            "Use `data/processed/validated/ohlcv_daily_validated.parquet` as the OHLCV source, "
            "exclude failed low-coverage dates by default, keep warn dates explicit in metadata, "
            "and join features/targets only on the validated `instrument_key + date` key set.",
            "",
        ]
    )
    return "\n".join(lines)
