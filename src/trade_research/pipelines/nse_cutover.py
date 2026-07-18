from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.daily_ohlcv import run_upstox_daily_ohlcv_pipeline
from trade_research.storage import ParquetStore, TimescaleStore


def compare_nse_provider_frames(
    upstox: pd.DataFrame,
    yfinance: pd.DataFrame,
    *,
    sessions: list[date],
    close_tolerance: float,
) -> dict[str, Any]:
    """Compare raw daily candles on exchange symbol/date over shared symbols."""
    if not sessions:
        raise ValueError("At least one NSE comparison session is required.")
    session_set = set(sessions)
    left = _comparison_rows(upstox, session_set, "upstox")
    right = _comparison_rows(yfinance, session_set, "yfinance")
    upstox_latest_date = _latest_date(left)
    yfinance_latest_date = _latest_date(right)
    upstox_window_rows = len(left)
    yfinance_window_rows = len(right)
    upstox_window_symbols = int(left["symbol"].nunique())
    yfinance_window_symbols = int(right["symbol"].nunique())
    shared_symbols = sorted(set(left["symbol"]) & set(right["symbol"]))
    left = left[left["symbol"].isin(shared_symbols)]
    right = right[right["symbol"].isin(shared_symbols)]
    overlap = left.merge(
        right,
        on=["symbol", "date"],
        how="inner",
        suffixes=("_upstox", "_yfinance"),
    )
    denominator = max(len(left), len(right), 1)
    row_overlap_ratio = len(overlap) / denominator
    if overlap.empty:
        close_match_ratio = 0.0
        close_mismatches = 0
        maximum_close_difference = None
    else:
        denominator_close = overlap[["close_upstox", "close_yfinance"]].abs().max(axis=1)
        denominator_close = denominator_close.where(denominator_close > 0, 1.0)
        differences = (
            (overlap["close_upstox"] - overlap["close_yfinance"]).abs()
            / denominator_close
        )
        close_mismatches = int((differences > close_tolerance).sum())
        close_match_ratio = 1 - (close_mismatches / len(overlap))
        maximum_close_difference = float(differences.max())

    return {
        "window_start": sessions[0].isoformat(),
        "window_end": sessions[-1].isoformat(),
        "comparison_sessions": len(sessions),
        "upstox_window_rows": upstox_window_rows,
        "yfinance_window_rows": yfinance_window_rows,
        "upstox_window_symbols": upstox_window_symbols,
        "yfinance_window_symbols": yfinance_window_symbols,
        "upstox_rows": int(len(left)),
        "yfinance_rows": int(len(right)),
        "overlap_rows": int(len(overlap)),
        "row_overlap_ratio": float(row_overlap_ratio),
        "overlapping_symbols": len(shared_symbols),
        "close_tolerance": float(close_tolerance),
        "close_match_ratio": float(close_match_ratio),
        "close_mismatches": close_mismatches,
        "maximum_close_difference": maximum_close_difference,
        "upstox_latest_date": upstox_latest_date,
        "yfinance_latest_date": yfinance_latest_date,
        "overlap_state": (
            "provider_data_missing"
            if not upstox_window_rows or not yfinance_window_rows
            else "no_symbol_overlap"
            if not shared_symbols
            else "overlap_available"
        ),
    }


def run_nse_yfinance_cutover_readiness(
    *,
    trigger: str = "pipeline",
    at: datetime | None = None,
    store: TimescaleStore | None = None,
) -> PipelineRunResult:
    """Fail-closed overlap and freshness gate for an NSE primary-provider switch."""
    settings = get_settings()
    db = store or TimescaleStore(settings.database_url)
    if store is None:
        db.initialize()
    observed_at = at or datetime.now(UTC)
    eligible = db.latest_provider_eligible_exchange_session(
        "NSE",
        at=observed_at,
        provider_grace_minutes=settings.yfinance_provider_grace_minutes,
    )
    if eligible is None:
        return PipelineRunResult(
            name="nse_yfinance_cutover_readiness",
            status="fail",
            blocking_issues=["No provider-eligible NSE exchange session is available."],
            metrics={"trigger": trigger, "ready": False},
        )
    end = eligible["session_date"]
    calendar_rows = db.exchange_sessions("NSE", end - timedelta(days=400), end)
    sessions = [
        row["session_date"]
        for row in calendar_rows
        if row["is_trading_day"] and str(row["validation_status"]).startswith("valid")
    ][-settings.nse_provider_comparison_sessions :]
    if len(sessions) < settings.nse_provider_comparison_sessions:
        return PipelineRunResult(
            name="nse_yfinance_cutover_readiness",
            status="fail",
            blocking_issues=[
                "Insufficient validated NSE sessions for the provider comparison."
            ],
            metrics={"trigger": trigger, "ready": False, "sessions": len(sessions)},
        )
    upstox = db.daily_ohlcv_frame(
        exchange="NSE", source="upstox", start_date=sessions[0], end_date=sessions[-1]
    )
    yfinance = db.daily_ohlcv_frame(
        exchange="NSE", source="yfinance", start_date=sessions[0], end_date=sessions[-1]
    )
    metrics = compare_nse_provider_frames(
        upstox,
        yfinance,
        sessions=sessions,
        close_tolerance=settings.nse_provider_comparison_close_tolerance,
    )
    blocking: list[str] = []
    missing_providers = [
        provider
        for provider in ("upstox", "yfinance")
        if not metrics[f"{provider}_window_rows"]
    ]
    if missing_providers:
        blocking.append(
            "NSE comparison window has no data for: " + ", ".join(missing_providers) + "."
        )
    if not missing_providers:
        if metrics["overlapping_symbols"] == 0:
            blocking.append(
                "NSE providers have no comparable symbols after canonical normalization."
            )
        elif metrics["overlapping_symbols"] < settings.nse_provider_comparison_minimum_symbols:
            blocking.append(
                "NSE provider overlap has too few symbols: "
                f"{metrics['overlapping_symbols']}<"
                f"{settings.nse_provider_comparison_minimum_symbols}."
            )
    has_overlap = metrics["overlapping_symbols"] > 0
    if (
        has_overlap
        and metrics["row_overlap_ratio"]
        < settings.nse_provider_comparison_minimum_row_overlap
    ):
        blocking.append(
            "NSE provider row overlap is below threshold: "
            f"{metrics['row_overlap_ratio']:.4f}<"
            f"{settings.nse_provider_comparison_minimum_row_overlap:.4f}."
        )
    if (
        has_overlap
        and metrics["close_match_ratio"]
        < settings.nse_provider_comparison_minimum_close_match
    ):
        blocking.append(
            "NSE provider close-price match is below threshold: "
            f"{metrics['close_match_ratio']:.4f}<"
            f"{settings.nse_provider_comparison_minimum_close_match:.4f}."
        )
    stale_providers: list[str] = []
    for provider in ("upstox", "yfinance"):
        latest = metrics[f"{provider}_latest_date"]
        lag = _session_lag(latest, sessions)
        metrics[f"{provider}_session_lag"] = lag
        if (
            provider not in missing_providers
            and lag > settings.nse_provider_comparison_maximum_session_lag
        ):
            stale_providers.append(provider)
            blocking.append(
                f"{provider} NSE data is {lag} sessions behind; maximum is "
                f"{settings.nse_provider_comparison_maximum_session_lag}."
            )
    comparison_state = _readiness_state(
        metrics,
        missing_providers=missing_providers,
        stale_providers=stale_providers,
        minimum_symbols=settings.nse_provider_comparison_minimum_symbols,
        minimum_row_overlap=settings.nse_provider_comparison_minimum_row_overlap,
        minimum_close_match=settings.nse_provider_comparison_minimum_close_match,
    )
    metrics.update(
        {
            "trigger": trigger,
            "ready": not blocking,
            "minimum_symbols": settings.nse_provider_comparison_minimum_symbols,
            "minimum_row_overlap": settings.nse_provider_comparison_minimum_row_overlap,
            "minimum_close_match": settings.nse_provider_comparison_minimum_close_match,
            "maximum_session_lag": (
                settings.nse_provider_comparison_maximum_session_lag
            ),
            "missing_providers": missing_providers,
            "stale_providers": stale_providers,
            "comparison_state": comparison_state,
        }
    )
    return PipelineRunResult(
        name="nse_yfinance_cutover_readiness",
        status="pass" if not blocking else "fail",
        rows=int(metrics["overlap_rows"]),
        metrics=metrics,
        blocking_issues=blocking,
    )


def run_nse_daily_ohlcv_primary_pipeline(
    *,
    to_date: str | None = None,
    trigger: str = "pipeline",
) -> PipelineRunResult:
    """Provider-neutral NSE daily entry point used by Dagster research assets."""
    settings = get_settings()
    if settings.nse_daily_primary_source == "upstox":
        if not settings.legacy_upstox_nse_enabled:
            return PipelineRunResult(
                name="nse_daily_ohlcv",
                status="fail",
                blocking_issues=[
                    "Upstox is selected as NSE primary but LEGACY_UPSTOX_NSE_ENABLED=false."
                ],
                metrics={"primary_source": "upstox", "trigger": trigger},
            )
        result = run_upstox_daily_ohlcv_pipeline(
            to_date=to_date,
            store_db=True,
            export_db_snapshot=True,
            trigger=trigger,
            max_concurrent_fetches=settings.upstox_historical_concurrency,
        )
        result.metrics["primary_source"] = "upstox"
        return PipelineRunResult(
            name="nse_daily_ohlcv",
            status=result.status,
            rows=result.rows,
            artifacts=result.artifacts,
            metrics=result.metrics,
            warnings=result.warnings,
            blocking_issues=result.blocking_issues,
        )

    readiness = run_nse_yfinance_cutover_readiness(trigger=trigger)
    artifacts = {}
    snapshot_rows = 0
    if readiness.status == "pass":
        db = TimescaleStore(settings.database_url)
        snapshot = db.daily_ohlcv_frame(exchange="NSE", source="yfinance")
        snapshot_rows = len(snapshot)
        if not snapshot.empty:
            artifacts["ohlcv"] = ParquetStore(settings.data_dir).write_frame(
                "processed/equities/nse_daily_ohlcv_yfinance",
                snapshot,
            )
    return PipelineRunResult(
        name="nse_daily_ohlcv",
        status=readiness.status,
        rows=snapshot_rows,
        artifacts=artifacts,
        metrics={
            **readiness.metrics,
            "primary_source": "yfinance",
            "snapshot_rows": snapshot_rows,
        },
        warnings=[
            "Yahoo NSE ingestion is executed by the durable planner/worker; "
            "this asset validates the primary dataset instead of downloading inline."
        ],
        blocking_issues=readiness.blocking_issues,
    )


def _comparison_rows(
    frame: pd.DataFrame,
    sessions: set[date],
    provider: str,
) -> pd.DataFrame:
    required = {"symbol", "date", "close"}
    if frame.empty:
        return pd.DataFrame(columns=["symbol", "date", "close"])
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{provider} comparison frame is missing: {sorted(missing)}")
    result = frame.loc[:, ["symbol", "date", "close"]].copy()
    result["symbol"] = result["symbol"].astype(str).str.strip().str.upper()
    result["symbol"] = result["symbol"].str.replace(r"\.NS$", "", regex=True)
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.date
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result[
        result["date"].isin(sessions)
        & result["symbol"].ne("")
        & result["close"].notna()
    ]
    return result.drop_duplicates(["symbol", "date"], keep="last")


def _latest_date(frame: pd.DataFrame) -> str | None:
    if frame.empty:
        return None
    value = frame["date"].max()
    return value.isoformat() if value is not None else None


def _session_lag(latest: str | None, sessions: list[date]) -> int:
    if latest is None:
        return len(sessions)
    latest_date = date.fromisoformat(latest)
    return sum(session > latest_date for session in sessions)


def _readiness_state(
    metrics: dict[str, Any],
    *,
    missing_providers: list[str],
    stale_providers: list[str],
    minimum_symbols: int,
    minimum_row_overlap: float,
    minimum_close_match: float,
) -> str:
    if missing_providers:
        return "provider_data_missing"
    if metrics["overlapping_symbols"] == 0:
        return "no_symbol_overlap"
    if stale_providers:
        return "provider_stale"
    if metrics["overlapping_symbols"] < minimum_symbols:
        return "insufficient_symbol_overlap"
    if metrics["row_overlap_ratio"] < minimum_row_overlap:
        return "insufficient_row_overlap"
    if metrics["close_match_ratio"] < minimum_close_match:
        return "close_mismatch"
    return "ready"
