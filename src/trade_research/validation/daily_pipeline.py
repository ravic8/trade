from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from trade_research.config import get_settings
from trade_research.features import (
    FEATURE_VERSION_V1_0,
    DailyTechnicalFeatureBuilder,
)
from trade_research.market_calendar import (
    EXCHANGE_CONFIGS,
    ExchangeHolidays,
    fetch_exchange_holidays,
)
from trade_research.research import DailyFactorResearchBuilder, write_factor_research_outputs
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.targets import (
    DAILY_FORWARD_TARGET_VERSION_V1_0,
    DailyForwardTargetBuilder,
)
from trade_research.validation.processed_datasets import (
    find_invalid_ohlcv_rows,
    normalize_ohlcv,
    validate_processed_datasets,
)


@dataclass(frozen=True)
class LatestTradingDate:
    current_local_time: datetime
    latest_expected_trading_date: date
    reason: str
    calendar_source: str


@dataclass(frozen=True)
class DailyPipelineHealthResult:
    summary: dict[str, Any]
    report_path: Path
    json_path: Path


def validate_daily_pipeline_health(
    data_dir: Path | str = "data",
    run_live_fetch: bool = False,
    run_factor_research: bool = True,
    rebuild_artifacts: bool = True,
    coverage_run_id: str | None = None,
    store_coverage_db: bool = False,
    coverage_windows_months: list[int] | None = None,
    at: datetime | None = None,
) -> DailyPipelineHealthResult:
    data_root = Path(data_dir)
    validation_dir = data_root / "processed/validation"
    validation_dir.mkdir(parents=True, exist_ok=True)

    settings = get_settings()
    latest = resolve_latest_expected_trading_date(at=at)
    commands_run: list[dict[str, Any]] = []
    blocking: list[str] = []
    warnings: list[str] = []
    stages: dict[str, dict[str, Any]] = {}

    stages["environment_config"] = _environment_stage(data_root, settings.upstox_access_token)
    if stages["environment_config"]["status"] == "fail":
        blocking.extend(stages["environment_config"]["blocking_issues"])
    warnings.extend(stages["environment_config"].get("warnings", []))

    stages["instrument_master"] = _instrument_master_stage(data_root)
    stages["universe_mapping"] = _mapping_stage(data_root)
    for stage in ["instrument_master", "universe_mapping"]:
        if stages[stage]["status"] == "fail":
            blocking.extend(stages[stage]["blocking_issues"])
        warnings.extend(stages[stage].get("warnings", []))

    fetch_stage = _fetch_stage(
        data_root=data_root,
        latest_date=latest.latest_expected_trading_date,
        run_live_fetch=run_live_fetch,
        has_token=bool(settings.upstox_access_token),
        commands_run=commands_run,
    )
    stages["upstox_daily_ohlcv_fetch"] = fetch_stage
    if fetch_stage["status"] == "fail":
        blocking.extend(fetch_stage["blocking_issues"])
    warnings.extend(fetch_stage.get("warnings", []))

    raw_stage = (
        _run_raw_to_processed_validation(data_root, commands_run)
        if rebuild_artifacts
        else _read_raw_to_processed_validation(data_root)
    )
    stages["raw_to_processed_validation"] = raw_stage
    if raw_stage["status"] == "fail":
        blocking.extend(raw_stage["blocking_issues"])
    warnings.extend(raw_stage.get("warnings", []))

    cleaned_stage = (
        _ensure_cleaned_validation(data_root, commands_run)
        if rebuild_artifacts
        else _read_processed_dataset_validation(data_root)
    )
    stages["cleaned_ohlcv_validation"] = cleaned_stage
    if cleaned_stage["status"] == "fail":
        blocking.extend(cleaned_stage["blocking_issues"])
    warnings.extend(cleaned_stage.get("warnings", []))

    feature_stage = (
        _rebuild_features(data_root, commands_run)
        if rebuild_artifacts
        else _read_feature_artifact_stage(data_root)
    )
    stages["feature_rebuild"] = feature_stage
    if feature_stage["status"] == "fail":
        blocking.extend(feature_stage["blocking_issues"])
    warnings.extend(feature_stage.get("warnings", []))

    target_stage = (
        _rebuild_targets(data_root, commands_run)
        if rebuild_artifacts
        else _read_target_artifact_stage(data_root)
    )
    stages["target_rebuild"] = target_stage
    if target_stage["status"] == "fail":
        blocking.extend(target_stage["blocking_issues"])
    warnings.extend(target_stage.get("warnings", []))

    post_validation = (
        _ensure_cleaned_validation(data_root, commands_run) if rebuild_artifacts else cleaned_stage
    )
    stages["feature_target_alignment"] = {
        "status": post_validation["validation_summary"]
        .get("alignment", {})
        .get("status", "missing"),
        "validation_summary": post_validation["validation_summary"].get("alignment", {}),
        "blocking_issues": [],
        "warnings": [],
    }
    if stages["feature_target_alignment"]["status"] == "fail":
        blocking.append("Feature-target-OHLCV alignment failed after rebuild.")

    if run_factor_research and rebuild_artifacts:
        factor_stage = _rebuild_factor_research(data_root, commands_run)
    elif run_factor_research:
        factor_stage = _read_factor_research_stage(data_root)
    else:
        factor_stage = {
            "status": "warn",
            "warnings": ["Factor research rebuild skipped by option."],
            "blocking_issues": [],
        }
    stages["factor_research"] = factor_stage
    if factor_stage["status"] == "fail":
        blocking.extend(factor_stage["blocking_issues"])
    warnings.extend(factor_stage.get("warnings", []))

    local_artifact = _local_artifact_summary(
        data_root,
        latest.latest_expected_trading_date,
        coverage_run_id=coverage_run_id,
        store_coverage_db=store_coverage_db,
        coverage_windows_months=coverage_windows_months,
    )
    warnings.extend(local_artifact.get("warnings", []))
    blocking.extend(local_artifact.get("blocking_issues", []))
    stages["stock_coverage"] = {
        "status": local_artifact.get("stock_coverage_summary", {}).get("status", "missing"),
        "summary": local_artifact.get("stock_coverage_summary", {}),
        "path": local_artifact.get("stock_coverage_path"),
        "blocking_issues": [],
        "warnings": local_artifact.get("stock_coverage_warnings", []),
    }
    warnings.extend(local_artifact.get("stock_coverage_warnings", []))

    validation_summary_path = validation_dir / "processed_dataset_validation_summary.json"
    validation_summary = _read_json(validation_summary_path)
    if validation_summary.get("baseline_ml_ready") is False:
        blocking.append("Processed dataset validation reports baseline_ml_ready=false.")

    overall_status = "fail" if blocking else ("warn" if warnings else "pass")
    baseline_ready = not blocking and bool(validation_summary.get("baseline_ml_ready"))
    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "current_local_date": latest.current_local_time.date().isoformat(),
        "current_local_time": latest.current_local_time.isoformat(),
        "latest_expected_trading_date": latest.latest_expected_trading_date.isoformat(),
        "latest_expected_trading_date_reason": latest.reason,
        "calendar_source": latest.calendar_source,
        "rebuild_artifacts": bool(rebuild_artifacts),
        "coverage_run_id": local_artifact.get("coverage_run_id"),
        "commands_run": commands_run,
        "files_inspected_generated": _health_files(data_root),
        "stages": stages,
        "row_counts": local_artifact.get("row_counts", {}),
        "date_ranges": local_artifact.get("date_ranges", {}),
        "instrument_counts": local_artifact.get("instrument_counts", {}),
        "invalid_rows": local_artifact.get("invalid_rows", {}),
        "duplicate_key_counts": local_artifact.get("duplicate_key_counts", {}),
        "low_coverage_dates": validation_summary.get("recommended_exclusions", {}).get(
            "low_coverage_dates", []
        ),
        "symbols_lagging_latest_expected_date": local_artifact.get(
            "symbols_lagging_latest_expected_date", 0
        ),
        "stock_coverage": local_artifact.get("stock_coverage_summary", {}),
        "stock_coverage_windows": local_artifact.get("stock_coverage_windows_summary", {}),
        "lowest_stock_coverage": local_artifact.get("lowest_stock_coverage", []),
        "feature_null_summary": validation_summary.get("features", {}),
        "target_null_summary": validation_summary.get("targets", {}),
        "blocking_issues": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "overall_status": overall_status,
        "baseline_ml_ready": bool(baseline_ready),
        "serious_research_ready": bool(baseline_ready and not warnings),
        "production_ready": False,
        "next_recommended_action": _next_action(baseline_ready, blocking, warnings),
    }

    report_path = validation_dir / "daily_pipeline_health_report.md"
    json_path = validation_dir / "daily_pipeline_health_report.json"
    summary["files_inspected_generated"]["daily_pipeline_health_report_md"] = str(report_path)
    summary["files_inspected_generated"]["daily_pipeline_health_report_json"] = str(json_path)
    json_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(_health_markdown(summary), encoding="utf-8")
    return DailyPipelineHealthResult(summary=summary, report_path=report_path, json_path=json_path)


def resolve_latest_expected_trading_date(
    at: datetime | None = None,
    holidays: ExchangeHolidays | None = None,
) -> LatestTradingDate:
    config = EXCHANGE_CONFIGS["NSE"]
    now = at or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    local_time = now.astimezone(ZoneInfo(config.timezone))
    if holidays is None:
        try:
            holidays = fetch_exchange_holidays("NSE", local_time.year)
            source = holidays.source_url
        except Exception as exc:
            holidays = ExchangeHolidays(frozenset(), frozenset(), config.holiday_source_url)
            source = f"calendar_unavailable:{exc};weekend_only_fallback"
    else:
        source = holidays.source_url

    candidate = local_time.date()
    market_close = datetime.combine(candidate, config.close_time, tzinfo=local_time.tzinfo)
    if not _is_trading_day(candidate, holidays) or local_time <= market_close:
        candidate -= timedelta(days=1)

    while not _is_trading_day(candidate, holidays):
        candidate -= timedelta(days=1)

    if local_time.date() == candidate:
        reason = "current trading day is completed"
    elif local_time.weekday() >= 5:
        reason = "today is weekend; using previous NSE trading session"
    elif local_time.date() in holidays.closed_dates:
        reason = "today is NSE holiday; using previous NSE trading session"
    else:
        reason = "current NSE trading session is not closed; using previous completed session"
    return LatestTradingDate(
        current_local_time=local_time,
        latest_expected_trading_date=candidate,
        reason=reason,
        calendar_source=source,
    )


def _is_trading_day(value: date, holidays: ExchangeHolidays) -> bool:
    return value.weekday() < 5 and value not in holidays.closed_dates


def _environment_stage(data_root: Path, upstox_token: str | None) -> dict[str, Any]:
    blocking = []
    warnings = []
    if not data_root.exists():
        blocking.append(f"Data directory missing: {data_root}")
    if not upstox_token:
        warnings.append("UPSTOX_ACCESS_TOKEN is not configured; live Upstox fetch will be skipped.")
    dependencies = {}
    for package in ["pandas", "numpy", "pyarrow"]:
        try:
            __import__(package)
            dependencies[package] = True
        except Exception:
            dependencies[package] = False
            blocking.append(f"Missing Python dependency: {package}")
    return {
        "status": "fail" if blocking else ("warn" if warnings else "pass"),
        "has_upstox_token": bool(upstox_token),
        "dependencies": dependencies,
        "blocking_issues": blocking,
        "warnings": warnings,
    }


def _instrument_master_stage(data_root: Path) -> dict[str, Any]:
    path = data_root / "processed/instruments/upstox_instruments.parquet"
    if not path.exists():
        return {
            "status": "fail",
            "blocking_issues": [f"Missing instrument master: {path}"],
            "warnings": [],
        }
    frame = pd.read_parquet(path)
    missing = int(frame["instrument_key"].isna().sum()) if "instrument_key" in frame else len(frame)
    duplicates = int(frame.duplicated("instrument_key").sum()) if "instrument_key" in frame else 0
    blocking = []
    if missing:
        blocking.append("Instrument master contains missing instrument keys.")
    if duplicates:
        blocking.append("Instrument master contains duplicate instrument keys.")
    return {
        "status": "fail" if blocking else "pass",
        "path": str(path),
        "row_count": int(len(frame)),
        "missing_instrument_keys": missing,
        "duplicate_instrument_keys": duplicates,
        "blocking_issues": blocking,
        "warnings": [],
    }


def _mapping_stage(data_root: Path) -> dict[str, Any]:
    path = data_root / "processed/universe/liquid_nse_upstox_mapping.csv"
    unmatched_path = data_root / "processed/universe/liquid_nse_upstox_unmatched.csv"
    if not path.exists():
        return {
            "status": "fail",
            "blocking_issues": [f"Missing Upstox mapping: {path}"],
            "warnings": [],
        }
    frame = pd.read_csv(path)
    unmatched = pd.read_csv(unmatched_path) if unmatched_path.exists() else pd.DataFrame()
    missing = int(frame["instrument_key"].isna().sum()) if "instrument_key" in frame else len(frame)
    duplicates = int(frame.duplicated("instrument_key").sum()) if "instrument_key" in frame else 0
    blocking = []
    warnings = []
    if missing:
        blocking.append("Liquid mapping contains missing instrument keys.")
    if duplicates:
        blocking.append("Liquid mapping contains duplicate instrument keys.")
    if not unmatched.empty:
        warnings.append(f"{len(unmatched)} liquid universe symbols are unmatched to Upstox.")
    return {
        "status": "fail" if blocking else ("warn" if warnings else "pass"),
        "path": str(path),
        "row_count": int(len(frame)),
        "mapped_instruments": int(frame["instrument_key"].nunique())
        if "instrument_key" in frame
        else 0,
        "unmatched_symbols": int(len(unmatched)),
        "missing_instrument_keys": missing,
        "duplicate_instrument_keys": duplicates,
        "blocking_issues": blocking,
        "warnings": warnings,
    }


def _fetch_stage(
    data_root: Path,
    latest_date: date,
    run_live_fetch: bool,
    has_token: bool,
    commands_run: list[dict[str, Any]],
) -> dict[str, Any]:
    if not run_live_fetch:
        return {
            "status": "warn",
            "live_fetch_run": False,
            "blocking_issues": [],
            "warnings": ["Live Upstox fetch not run; validating existing local artifacts."],
        }
    if not has_token:
        return {
            "status": "fail",
            "live_fetch_run": False,
            "blocking_issues": ["UPSTOX_ACCESS_TOKEN is required for live Upstox fetch."],
            "warnings": [],
        }
    cmd = [
        sys.executable,
        "-m",
        "trade_research.cli",
        "fetch-upstox-nse-daily",
        "--to-date",
        latest_date.isoformat(),
        "--full-refresh",
        "--no-store-db",
    ]
    result = _run_command(cmd, commands_run)
    post_fetch = _fetch_output_status(data_root, latest_date)
    blocking = []
    warnings = []
    if result["returncode"] != 0:
        blocking.append("Live Upstox daily fetch failed.")
    if not post_fetch["reaches_latest_expected_date"]:
        blocking.append(
            "Processed OHLCV does not reach latest expected trading date after live fetch."
        )
    if post_fetch["failure_rows"]:
        warnings.append(f"Live Upstox fetch recorded {post_fetch['failure_rows']} failures.")
    return {
        "status": "fail" if blocking else ("warn" if warnings else "pass"),
        "live_fetch_run": True,
        "returncode": result["returncode"],
        **post_fetch,
        "blocking_issues": blocking,
        "warnings": warnings,
    }


def _fetch_output_status(data_root: Path, latest_date: date) -> dict[str, Any]:
    processed_path = data_root / "processed/equities/nse_daily_ohlcv_upstox.parquet"
    failures_path = data_root / "processed/equities/nse_daily_ohlcv_upstox_failures.csv"
    frame = _read_parquet(processed_path)
    failures = pd.read_csv(failures_path) if failures_path.exists() else pd.DataFrame()
    max_date = None
    if not frame.empty:
        normalized = normalize_ohlcv(frame)
        max_value = pd.to_datetime(normalized["date"], errors="coerce").max()
        max_date = None if pd.isna(max_value) else max_value.date()
    return {
        "processed_max_date": max_date.isoformat() if max_date else None,
        "latest_expected_trading_date": latest_date.isoformat(),
        "reaches_latest_expected_date": bool(max_date and max_date >= latest_date),
        "failure_rows": int(len(failures)),
    }


def _run_raw_to_processed_validation(
    data_root: Path,
    commands_run: list[dict[str, Any]],
) -> dict[str, Any]:
    if data_root != Path("data"):
        metadata = _read_json(data_root / "processed/validation/raw_to_processed_metadata.json")
        warnings = ["Raw-to-processed script skipped for non-default data_dir."]
        if not metadata.get("full_raw_replay_possible", False):
            warnings.append("Raw API payload replay remains unavailable.")
        return {
            "status": "warn",
            "returncode": None,
            "metadata": metadata,
            "blocking_issues": [],
            "warnings": warnings,
        }
    result = _run_command(
        [sys.executable, "scripts/validate_upstox_raw_to_processed.py"],
        commands_run,
    )
    metadata = _read_json(data_root / "processed/validation/raw_to_processed_metadata.json")
    warnings = []
    if not metadata.get("full_raw_replay_possible", False):
        warnings.append("Raw API payload replay remains unavailable.")
    if metadata.get("invalid_rows", 0):
        warnings.append(f"Processed OHLCV has {metadata.get('invalid_rows')} invalid rows.")
    return {
        "status": "pass" if result["returncode"] == 0 else "fail",
        "returncode": result["returncode"],
        "metadata": metadata,
        "blocking_issues": []
        if result["returncode"] == 0
        else ["Raw-to-processed validation failed."],
        "warnings": warnings,
    }


def _read_raw_to_processed_validation(data_root: Path) -> dict[str, Any]:
    metadata = _read_json(data_root / "processed/validation/raw_to_processed_metadata.json")
    warnings = ["Raw-to-processed validation read in read-only mode."]
    blocking = []
    if not metadata:
        blocking.append("Raw-to-processed validation metadata is missing.")
    elif not metadata.get("full_raw_replay_possible", False):
        warnings.append("Raw API payload replay remains unavailable.")
    if metadata.get("invalid_rows", 0):
        warnings.append(f"Processed OHLCV has {metadata.get('invalid_rows')} invalid rows.")
    return {
        "status": "fail" if blocking else ("warn" if warnings else "pass"),
        "mode": "read_only",
        "metadata": metadata,
        "blocking_issues": blocking,
        "warnings": warnings,
    }


def _ensure_cleaned_validation(
    data_root: Path,
    commands_run: list[dict[str, Any]],
) -> dict[str, Any]:
    result = validate_processed_datasets(data_dir=data_root)
    commands_run.append({"command": "trade-research validate-processed-datasets", "returncode": 0})
    summary = result.summary
    return {
        "status": "fail"
        if summary.get("blocking_issues")
        else ("warn" if summary.get("warnings") else "pass"),
        "validation_summary": summary,
        "blocking_issues": list(summary.get("blocking_issues", [])),
        "warnings": list(summary.get("warnings", [])),
    }


def _read_processed_dataset_validation(data_root: Path) -> dict[str, Any]:
    summary = _read_json(
        data_root / "processed/validation/processed_dataset_validation_summary.json"
    )
    blocking = []
    warnings = []
    if not summary:
        blocking.append("Processed dataset validation summary is missing.")
    else:
        blocking.extend(summary.get("blocking_issues", []))
        warnings.extend(summary.get("warnings", []))
        warnings.append("Processed dataset validation read in read-only mode.")
    return {
        "status": "fail" if blocking else ("warn" if warnings else "pass"),
        "mode": "read_only",
        "validation_summary": summary,
        "blocking_issues": list(blocking),
        "warnings": list(warnings),
    }


def _rebuild_features(data_root: Path, commands_run: list[dict[str, Any]]) -> dict[str, Any]:
    source_path = data_root / "processed/validated/ohlcv_daily_validated.parquet"
    output_path = data_root / "processed/features/daily_v1_ohlcv_technical.parquet"
    if not source_path.exists():
        return {
            "status": "fail",
            "blocking_issues": [f"Missing cleaned OHLCV: {source_path}"],
            "warnings": [],
        }
    try:
        source = pd.read_parquet(source_path)
        features = DailyTechnicalFeatureBuilder(drop_invalid_rows=False).build(source)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        features.to_parquet(output_path, index=False)
        commands_run.append(
            {
                "command": (
                    "trade-research build-daily-features "
                    "--input-name processed/validated/ohlcv_daily_validated --no-store-db"
                ),
                "returncode": 0,
            }
        )
        invalid_in_features = int(find_invalid_ohlcv_rows(normalize_ohlcv(features)).shape[0])
        return {
            "status": "pass" if invalid_in_features == 0 else "fail",
            "path": str(output_path),
            "row_count": int(len(features)),
            "instrument_count": int(features["instrument_key"].nunique()),
            "date_range": _date_range(features),
            "duplicate_key_count": int(features.duplicated(["instrument_key", "date"]).sum()),
            "inf_value_count": int(
                np.isinf(features.select_dtypes(include=[np.number])).sum().sum()
            ),
            "quality_status_counts": {
                str(k): int(v)
                for k, v in features["quality_status"].value_counts().to_dict().items()
            },
            "invalid_ohlcv_rows_in_features": invalid_in_features,
            "blocking_issues": []
            if invalid_in_features == 0
            else ["Invalid OHLCV row appears in features."],
            "warnings": [
                "Default build-daily-features input is uncleaned OHLCV; "
                "health check rebuilt from cleaned OHLCV explicitly."
            ],
        }
    except Exception as exc:
        commands_run.append(
            {"command": "feature rebuild from cleaned OHLCV", "returncode": 1, "error": str(exc)}
        )
        return {
            "status": "fail",
            "blocking_issues": [f"Feature rebuild failed: {exc}"],
            "warnings": [],
        }


def _read_feature_artifact_stage(data_root: Path) -> dict[str, Any]:
    path = data_root / "processed/features/daily_v1_ohlcv_technical.parquet"
    features = _read_parquet(path)
    if features.empty:
        return {
            "status": "fail",
            "mode": "read_only",
            "path": str(path),
            "blocking_issues": [f"Missing or empty feature artifact: {path}"],
            "warnings": [],
        }

    invalid_in_features = 0
    try:
        invalid_in_features = int(find_invalid_ohlcv_rows(normalize_ohlcv(features)).shape[0])
    except Exception:
        invalid_in_features = 0
    blocking = [] if invalid_in_features == 0 else ["Invalid OHLCV row appears in features."]
    return {
        "status": "fail" if blocking else "pass",
        "mode": "read_only",
        "path": str(path),
        "row_count": int(len(features)),
        "instrument_count": int(features["instrument_key"].nunique()),
        "date_range": _date_range(features),
        "duplicate_key_count": int(features.duplicated(["instrument_key", "date"]).sum()),
        "inf_value_count": int(np.isinf(features.select_dtypes(include=[np.number])).sum().sum()),
        "quality_status_counts": {
            str(k): int(v) for k, v in features["quality_status"].value_counts().to_dict().items()
        },
        "invalid_ohlcv_rows_in_features": invalid_in_features,
        "blocking_issues": blocking,
        "warnings": [],
    }


def _rebuild_targets(data_root: Path, commands_run: list[dict[str, Any]]) -> dict[str, Any]:
    source_path = data_root / "processed/validated/ohlcv_daily_validated.parquet"
    output_path = data_root / "processed/targets/daily_v1_forward_returns.parquet"
    if not source_path.exists():
        return {
            "status": "fail",
            "blocking_issues": [f"Missing cleaned OHLCV: {source_path}"],
            "warnings": [],
        }
    try:
        source = pd.read_parquet(source_path)
        targets = DailyForwardTargetBuilder(drop_invalid_rows=False).build(source)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        targets.to_parquet(output_path, index=False)
        commands_run.append(
            {
                "command": (
                    "trade-research build-daily-targets "
                    "--input-name processed/validated/ohlcv_daily_validated --no-store-db"
                ),
                "returncode": 0,
            }
        )
        return {
            "status": "pass",
            "path": str(output_path),
            "row_count": int(len(targets)),
            "instrument_count": int(targets["instrument_key"].nunique()),
            "date_range": _date_range(targets),
            "duplicate_key_count": int(targets.duplicated(["instrument_key", "date"]).sum()),
            "inf_value_count": int(
                np.isinf(targets.select_dtypes(include=[np.number])).sum().sum()
            ),
            "target_null_counts": {
                column: int(targets[column].isna().sum())
                for column in targets.columns
                if column.startswith("forward_ret_")
            },
            "blocking_issues": [],
            "warnings": [
                "Default build-daily-targets input is uncleaned OHLCV; "
                "health check rebuilt from cleaned OHLCV explicitly."
            ],
        }
    except Exception as exc:
        commands_run.append(
            {"command": "target rebuild from cleaned OHLCV", "returncode": 1, "error": str(exc)}
        )
        return {
            "status": "fail",
            "blocking_issues": [f"Target rebuild failed: {exc}"],
            "warnings": [],
        }


def _read_target_artifact_stage(data_root: Path) -> dict[str, Any]:
    path = data_root / "processed/targets/daily_v1_forward_returns.parquet"
    targets = _read_parquet(path)
    if targets.empty:
        return {
            "status": "fail",
            "mode": "read_only",
            "path": str(path),
            "blocking_issues": [f"Missing or empty target artifact: {path}"],
            "warnings": [],
        }

    return {
        "status": "pass",
        "mode": "read_only",
        "path": str(path),
        "row_count": int(len(targets)),
        "instrument_count": int(targets["instrument_key"].nunique()),
        "date_range": _date_range(targets),
        "duplicate_key_count": int(targets.duplicated(["instrument_key", "date"]).sum()),
        "inf_value_count": int(np.isinf(targets.select_dtypes(include=[np.number])).sum().sum()),
        "target_null_counts": {
            column: int(targets[column].isna().sum())
            for column in targets.columns
            if column.startswith("forward_ret_")
        },
        "blocking_issues": [],
        "warnings": [],
    }


def _rebuild_factor_research(data_root: Path, commands_run: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        store = ParquetStore(data_root)
        features = store.read_frame("processed/features/daily_v1_ohlcv_technical")
        targets = store.read_frame("processed/targets/daily_v1_forward_returns")
        builder = DailyFactorResearchBuilder(
            feature_version=FEATURE_VERSION_V1_0,
            target_version=DAILY_FORWARD_TARGET_VERSION_V1_0,
        )
        ic, quantiles, hit_rates, monthly, summary = builder.build(features, targets)
        paths = write_factor_research_outputs(
            ic,
            quantiles,
            hit_rates,
            monthly,
            summary,
            data_root / "processed/research/factors",
        )
        commands_run.append({"command": "trade-research build-factor-research", "returncode": 0})
        return {
            "status": "pass",
            "summary": summary.__dict__,
            "paths": {key: str(value) for key, value in paths.items()},
            "row_counts": {
                "ic": int(len(ic)),
                "quantiles": int(len(quantiles)),
                "hit_rates": int(len(hit_rates)),
                "monthly_stability": int(len(monthly)),
            },
            "blocking_issues": [],
            "warnings": [],
        }
    except Exception as exc:
        commands_run.append(
            {"command": "trade-research build-factor-research", "returncode": 1, "error": str(exc)}
        )
        return {
            "status": "fail",
            "blocking_issues": [f"Factor research rebuild failed: {exc}"],
            "warnings": [],
        }


def _read_factor_research_stage(data_root: Path) -> dict[str, Any]:
    summary_path = data_root / "processed/research/factors/daily_v1_factor_research_summary.json"
    summary = _read_json(summary_path)
    if not summary:
        return {
            "status": "fail",
            "mode": "read_only",
            "path": str(summary_path),
            "blocking_issues": [f"Missing factor research summary: {summary_path}"],
            "warnings": [],
        }
    return {
        "status": "pass",
        "mode": "read_only",
        "summary": summary,
        "paths": {
            "summary": str(summary_path),
            "ic": str(data_root / "processed/research/factors/daily_v1_factor_ic.csv"),
            "quantiles": str(
                data_root / "processed/research/factors/daily_v1_factor_quantiles.csv"
            ),
            "hit_rates": str(
                data_root / "processed/research/factors/daily_v1_factor_hit_rates.csv"
            ),
            "monthly_stability": str(
                data_root / "processed/research/factors/daily_v1_factor_monthly_stability.csv"
            ),
        },
        "row_counts": {
            "ic": int(summary.get("ic_rows", 0) or 0),
            "quantiles": int(summary.get("quantile_rows", 0) or 0),
            "hit_rates": int(summary.get("hit_rate_rows", 0) or 0),
            "monthly_stability": int(summary.get("monthly_stability_rows", 0) or 0),
        },
        "blocking_issues": [],
        "warnings": ["Factor research summary read in read-only mode."],
    }


def _local_artifact_summary(
    data_root: Path,
    latest_date: date,
    coverage_run_id: str | None = None,
    store_coverage_db: bool = False,
    coverage_windows_months: list[int] | None = None,
) -> dict[str, Any]:
    paths = {
        "processed_ohlcv": data_root / "processed/equities/nse_daily_ohlcv_upstox.parquet",
        "cleaned_ohlcv": data_root / "processed/validated/ohlcv_daily_validated.parquet",
        "features": data_root / "processed/features/daily_v1_ohlcv_technical.parquet",
        "targets": data_root / "processed/targets/daily_v1_forward_returns.parquet",
    }
    frames = {name: _read_parquet(path) for name, path in paths.items()}
    summary: dict[str, Any] = {
        "row_counts": {},
        "date_ranges": {},
        "instrument_counts": {},
        "invalid_rows": {},
        "duplicate_key_counts": {},
        "warnings": [],
        "blocking_issues": [],
    }
    for name, frame in frames.items():
        if frame.empty:
            summary["blocking_issues"].append(f"Missing or empty artifact: {paths[name]}")
            continue
        normalized = (
            normalize_ohlcv(frame) if name in {"processed_ohlcv", "cleaned_ohlcv"} else frame
        )
        key_frame = normalized.rename(columns={"InstrumentKey": "instrument_key", "Date": "date"})
        if "date" in key_frame:
            key_frame["date"] = pd.to_datetime(key_frame["date"], errors="coerce").dt.date
        summary["row_counts"][name] = int(len(key_frame))
        summary["date_ranges"][name] = _date_range(key_frame)
        summary["instrument_counts"][name] = (
            int(key_frame["instrument_key"].nunique()) if "instrument_key" in key_frame else 0
        )
        summary["duplicate_key_counts"][name] = (
            int(key_frame.duplicated(["instrument_key", "date"]).sum())
            if {"instrument_key", "date"}.issubset(key_frame.columns)
            else None
        )
        if name in {"processed_ohlcv", "cleaned_ohlcv"}:
            summary["invalid_rows"][name] = int(len(find_invalid_ohlcv_rows(normalized)))
        max_date = pd.to_datetime(key_frame["date"], errors="coerce").max().date()
        if max_date < latest_date:
            summary["warnings"].append(
                f"{name} max date {max_date} is before expected {latest_date}."
            )
    cleaned = frames["cleaned_ohlcv"]
    if not cleaned.empty:
        cleaned = normalize_ohlcv(cleaned)
        stock_coverage = write_stock_coverage(
            data_root,
            cleaned,
            latest_date,
            coverage_run_id=coverage_run_id,
            store_coverage_db=store_coverage_db,
            coverage_windows_months=coverage_windows_months,
        )
        summary["coverage_run_id"] = stock_coverage.get("coverage_run_id")
        summary["stock_coverage_path"] = stock_coverage["path"]
        summary["stock_coverage_summary"] = stock_coverage["summary"]
        summary["stock_coverage_windows_path"] = stock_coverage.get("windows_path")
        summary["stock_coverage_windows_summary"] = stock_coverage.get("windows_summary", {})
        summary["stock_coverage_warnings"] = stock_coverage["warnings"]
        summary["lowest_stock_coverage"] = stock_coverage["lowest_coverage"]
        latest_rows = cleaned[cleaned["date"].eq(latest_date)]
        expected = int(cleaned["instrument_key"].nunique())
        summary["latest_expected_date_coverage"] = {
            "date": latest_date.isoformat(),
            "rows": int(len(latest_rows)),
            "expected_instruments": expected,
            "coverage_pct": float(len(latest_rows) / expected) if expected else 0.0,
        }
        summary["symbols_lagging_latest_expected_date"] = int(
            (cleaned.groupby("instrument_key")["date"].max() < latest_date).sum()
        )
        if latest_rows.empty:
            summary["blocking_issues"].append(
                f"Cleaned OHLCV does not reach latest expected trading date {latest_date}."
            )
    return summary


def write_stock_coverage(
    data_root: Path,
    cleaned: pd.DataFrame,
    latest_date: date,
    coverage_run_id: str | None = None,
    store_coverage_db: bool = False,
    coverage_windows_months: list[int] | None = None,
) -> dict[str, Any]:
    validation_dir = data_root / "processed/validation"
    validation_dir.mkdir(parents=True, exist_ok=True)
    output_path = validation_dir / "daily_pipeline_stock_coverage.parquet"
    windows_output_path = validation_dir / "daily_pipeline_stock_coverage_windows.parquet"
    windows = coverage_windows_months or [6, 9, 12, 15, 18, 24]
    run_id = coverage_run_id or f"manual-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"

    if cleaned.empty:
        empty = pd.DataFrame(
            columns=[
                "instrument_key",
                "symbol",
                "row_count",
                "first_date",
                "last_date",
                "expected_date_count",
                "observed_date_count",
                "missing_date_count",
                "coverage_pct",
                "latest_expected_date",
                "has_latest_expected_date",
                "latest_date_lag_days",
                "coverage_status",
            ]
        )
        empty.to_parquet(output_path, index=False)
        empty.assign(window_months=pd.Series(dtype="int64")).to_parquet(
            windows_output_path,
            index=False,
        )
        return {
            "coverage_run_id": run_id,
            "path": str(output_path),
            "windows_path": str(windows_output_path),
            "summary": {"status": "missing", "stock_count": 0},
            "windows_summary": {},
            "warnings": ["Stock coverage could not be computed because cleaned OHLCV is empty."],
            "lowest_coverage": [],
        }

    frame = cleaned.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    coverage = _stock_coverage_for_window(
        frame,
        latest_date=latest_date,
        window_months=None,
        window_start=frame["date"].min(),
    )
    coverage_for_file = coverage.drop(columns=["window_months", "window_start", "window_end"])
    coverage_for_file.to_parquet(output_path, index=False)

    window_frames = [
        _stock_coverage_for_window(
            frame,
            latest_date=latest_date,
            window_months=months,
            window_start=_subtract_months(latest_date, months),
        )
        for months in windows
    ]
    windows_coverage = pd.concat(window_frames, ignore_index=True)
    windows_coverage.to_parquet(windows_output_path, index=False)

    windows_summary = _coverage_windows_summary(windows_coverage)
    db_rows = 0
    if store_coverage_db:
        db = TimescaleStore(get_settings().database_url)
        db.initialize()
        db_rows = db.insert_stock_coverage_run(
            run_id=run_id,
            dagster_run_id=coverage_run_id,
            coverage=windows_coverage,
            summary=windows_summary,
            as_of_date=latest_date,
        )

    status_counts = coverage["coverage_status"].value_counts().to_dict()
    missing_latest = int((~coverage["has_latest_expected_date"]).sum())
    low_coverage = coverage[coverage["coverage_status"].isin(["warn", "fail"])]
    warnings = []
    if missing_latest:
        warnings.append(f"{missing_latest} fetched stocks are missing the latest expected date.")
    if not low_coverage.empty:
        warnings.append(f"{len(low_coverage)} fetched stocks have warning/failing coverage.")
    summary = {
        "status": "fail" if missing_latest else ("warn" if not low_coverage.empty else "pass"),
        "path": str(output_path),
        "windows_path": str(windows_output_path),
        "coverage_run_id": run_id,
        "coverage_db_rows": int(db_rows),
        "stock_count": int(len(coverage)),
        "expected_date_count": int(coverage["expected_date_count"].iloc[0])
        if not coverage.empty
        else 0,
        "pass_stocks": int(status_counts.get("pass", 0)),
        "warn_stocks": int(status_counts.get("warn", 0)),
        "fail_stocks": int(status_counts.get("fail", 0)),
        "stocks_missing_latest_expected_date": missing_latest,
        "min_coverage_pct": float(coverage["coverage_pct"].min()) if not coverage.empty else 0.0,
        "median_coverage_pct": float(coverage["coverage_pct"].median())
        if not coverage.empty
        else 0.0,
        "max_coverage_pct": float(coverage["coverage_pct"].max()) if not coverage.empty else 0.0,
    }
    lowest = (
        coverage[
            [
                "instrument_key",
                "symbol",
                "row_count",
                "first_date",
                "last_date",
                "missing_date_count",
                "coverage_pct",
                "has_latest_expected_date",
                "coverage_status",
            ]
        ]
        .head(20)
        .to_dict(orient="records")
    )
    return {
        "coverage_run_id": run_id,
        "path": str(output_path),
        "windows_path": str(windows_output_path),
        "summary": summary,
        "windows_summary": windows_summary,
        "warnings": warnings,
        "lowest_coverage": lowest,
    }


def _stock_coverage_for_window(
    frame: pd.DataFrame,
    latest_date: date,
    window_months: int | None,
    window_start: date,
) -> pd.DataFrame:
    expected_dates = pd.Index(
        sorted(
            value
            for value in frame["date"].dropna().unique()
            if window_start <= value <= latest_date
        )
    )
    expected_count = int(len(expected_dates))
    rows: list[dict[str, Any]] = []
    for (instrument_key, symbol), group in frame.groupby(
        ["instrument_key", "symbol"], dropna=False
    ):
        dates = pd.Index(sorted(group["date"].dropna().unique()))
        eligible_dates = dates[(dates >= window_start) & (dates <= latest_date)]
        missing_dates = expected_dates.difference(eligible_dates)
        last_date = eligible_dates.max() if len(eligible_dates) else None
        latest_lag = None
        if last_date is not None:
            latest_lag = int((pd.Timestamp(latest_date) - pd.Timestamp(last_date)).days)
        coverage_pct = float(len(eligible_dates) / expected_count) if expected_count else 0.0
        has_latest = bool(latest_date in set(eligible_dates))
        if not has_latest:
            status = "fail"
        elif coverage_pct < 0.70:
            status = "fail"
        elif coverage_pct < 0.90:
            status = "warn"
        else:
            status = "pass"
        rows.append(
            {
                "window_months": int(window_months or 0),
                "window_start": window_start.isoformat(),
                "window_end": latest_date.isoformat(),
                "instrument_key": str(instrument_key),
                "symbol": str(symbol),
                "row_count": int(len(group)),
                "first_date": str(eligible_dates.min()) if len(eligible_dates) else None,
                "last_date": str(last_date) if last_date is not None else None,
                "expected_date_count": expected_count,
                "observed_date_count": int(len(eligible_dates)),
                "missing_date_count": int(len(missing_dates)),
                "coverage_pct": coverage_pct,
                "latest_expected_date": latest_date.isoformat(),
                "has_latest_expected_date": has_latest,
                "latest_date_lag_days": latest_lag,
                "coverage_status": status,
            }
        )
    return (
        pd.DataFrame(rows)
        .sort_values(["window_months", "coverage_status", "coverage_pct", "symbol"])
        .reset_index(drop=True)
    )


def _coverage_windows_summary(coverage: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for window_months, group in coverage.groupby("window_months", sort=True):
        status_counts = group["coverage_status"].value_counts().to_dict()
        key = f"{int(window_months)}m"
        summary[key] = {
            "window_months": int(window_months),
            "window_start": str(group["window_start"].iloc[0]),
            "window_end": str(group["window_end"].iloc[0]),
            "stock_count": int(len(group)),
            "expected_date_count": int(group["expected_date_count"].iloc[0]),
            "full_coverage_stocks": int((group["coverage_pct"].eq(1.0)).sum()),
            "pass_stocks": int(status_counts.get("pass", 0)),
            "warn_stocks": int(status_counts.get("warn", 0)),
            "fail_stocks": int(status_counts.get("fail", 0)),
            "stocks_missing_latest_expected_date": int((~group["has_latest_expected_date"]).sum()),
            "min_coverage_pct": float(group["coverage_pct"].min()),
            "median_coverage_pct": float(group["coverage_pct"].median()),
            "max_coverage_pct": float(group["coverage_pct"].max()),
        }
    return summary


def _subtract_months(value: date, months: int) -> date:
    import calendar

    year = value.year
    month = value.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _run_command(cmd: list[str], commands_run: list[dict[str, Any]]) -> dict[str, Any]:
    proc = subprocess.run(cmd, cwd=Path.cwd(), capture_output=True, text=True, check=False)
    item = {
        "command": " ".join(cmd),
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-1000:],
        "stderr_tail": proc.stderr[-1000:],
    }
    commands_run.append(item)
    return item


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _date_range(frame: pd.DataFrame) -> dict[str, str | None]:
    if frame.empty or "date" not in frame:
        return {"min": None, "max": None}
    values = pd.to_datetime(frame["date"], errors="coerce")
    return {
        "min": None if values.isna().all() else values.min().date().isoformat(),
        "max": None if values.isna().all() else values.max().date().isoformat(),
    }


def _health_files(data_root: Path) -> dict[str, str]:
    paths = {
        "processed_ohlcv": "processed/equities/nse_daily_ohlcv_upstox.parquet",
        "cleaned_ohlcv": "processed/validated/ohlcv_daily_validated.parquet",
        "features": "processed/features/daily_v1_ohlcv_technical.parquet",
        "targets": "processed/targets/daily_v1_forward_returns.parquet",
        "validation_summary_json": "processed/validation/processed_dataset_validation_summary.json",
        "validation_summary_md": "processed/validation/processed_dataset_validation_summary.md",
        "factor_summary": "processed/research/factors/daily_v1_factor_research_summary.json",
        "stock_coverage": "processed/validation/daily_pipeline_stock_coverage.parquet",
        "stock_coverage_windows": (
            "processed/validation/daily_pipeline_stock_coverage_windows.parquet"
        ),
    }
    return {key: str(data_root / value) for key, value in paths.items()}


def _next_action(baseline_ready: bool, blocking: list[str], warnings: list[str]) -> str:
    if blocking:
        return "Resolve blocking pipeline issues before ML dataset preparation."
    if baseline_ready:
        return (
            "Proceed to baseline ML dataset preparation using cleaned OHLCV, excluding "
            "documented low-coverage dates by default."
        )
    if warnings:
        return "Review warnings, then rerun pipeline health before ML dataset preparation."
    return "Rerun validation after the next daily fetch."


def _health_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Daily Pipeline Health Report",
        "",
        "## Executive Summary",
        "",
        f"- Overall status: {summary['overall_status']}",
        f"- Baseline ML ready: {summary['baseline_ml_ready']}",
        f"- Serious research ready: {summary['serious_research_ready']}",
        f"- Production ready: {summary['production_ready']}",
        f"- Current local time: {summary['current_local_time']}",
        f"- Latest expected trading date: {summary['latest_expected_trading_date']}",
        f"- Date reason: {summary['latest_expected_trading_date_reason']}",
        "",
        "## Commands Run",
        "",
    ]
    lines.extend(
        f"- `{item['command']}` -> {item['returncode']}" for item in summary["commands_run"]
    )
    lines.extend(["", "## Stage Status", ""])
    for name, stage in summary["stages"].items():
        lines.append(f"- {name}: {stage.get('status')}")
    lines.extend(
        [
            "",
            "## Row Counts",
            "",
            *[f"- {key}: {value}" for key, value in summary["row_counts"].items()],
            "",
            "## Date Ranges",
            "",
            *[f"- {key}: {value}" for key, value in summary["date_ranges"].items()],
            "",
            "## Invalid Rows",
            "",
            *[f"- {key}: {value}" for key, value in summary["invalid_rows"].items()],
            "",
            "## Duplicate Keys",
            "",
            *[f"- {key}: {value}" for key, value in summary["duplicate_key_counts"].items()],
            "",
            "## Low Coverage Dates",
            "",
        ]
    )
    if summary["low_coverage_dates"]:
        lines.extend(f"- {item}" for item in summary["low_coverage_dates"])
    else:
        lines.append("- None")
    lines.extend(["", "## Stock Coverage", ""])
    stock = summary.get("stock_coverage", {})
    if stock:
        lines.extend(
            [
                f"- Stocks: {stock.get('stock_count')}",
                f"- Expected dates per full-history stock: {stock.get('expected_date_count')}",
                (
                    f"- Pass/warn/fail: {stock.get('pass_stocks')}/"
                    f"{stock.get('warn_stocks')}/{stock.get('fail_stocks')}"
                ),
                (
                    "- Missing latest expected date: "
                    f"{stock.get('stocks_missing_latest_expected_date')}"
                ),
                (
                    f"- Coverage pct min/median/max: {stock.get('min_coverage_pct')}/"
                    f"{stock.get('median_coverage_pct')}/{stock.get('max_coverage_pct')}"
                ),
                f"- Detail file: {stock.get('path')}",
            ]
        )
    else:
        lines.append("- Missing")
    coverage_windows = summary.get("stock_coverage_windows", {})
    if coverage_windows:
        lines.extend(["", "### Rolling Window Stock Coverage", ""])
        for key, item in coverage_windows.items():
            lines.append(
                "- "
                f"{key}: full={item.get('full_coverage_stocks')}, "
                f"pass/warn/fail={item.get('pass_stocks')}/"
                f"{item.get('warn_stocks')}/{item.get('fail_stocks')}, "
                f"expected_dates={item.get('expected_date_count')}, "
                f"missing_latest={item.get('stocks_missing_latest_expected_date')}"
            )
    if summary.get("lowest_stock_coverage"):
        lines.extend(["", "### Lowest Stock Coverage Sample", ""])
        for item in summary["lowest_stock_coverage"][:10]:
            lines.append(
                "- "
                f"{item['symbol']} {item['instrument_key']}: "
                f"{item['coverage_pct']:.4f}, rows={item['row_count']}, "
                f"first={item['first_date']}, last={item['last_date']}, "
                f"status={item['coverage_status']}"
            )
    lines.extend(["", "## Blocking Issues", ""])
    lines.extend(f"- {item}" for item in (summary["blocking_issues"] or ["None"]))
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- {item}" for item in (summary["warnings"] or ["None"]))
    lines.extend(["", "## Next Recommended Action", "", summary["next_recommended_action"], ""])
    return "\n".join(lines)
