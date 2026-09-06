from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

import pandas as pd

from trade_research.config import get_settings
from trade_research.data.rate_limits import build_provider_rate_limiter
from trade_research.data.yfinance_provider import YFinanceIntradayProvider
from trade_research.market_data.availability import (
    MarketDataAvailabilityRepository,
    availability_session_maps,
    observe_nse_minute_availability,
)
from trade_research.market_data.contracts import CandleInterval
from trade_research.market_data.ingestion import (
    prepare_yfinance_batch,
    replicate_validated_batch,
)
from trade_research.market_data.quality import (
    MarketDataQualityRepository,
    nse_minute_missing_quality_outcomes,
)
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.yfinance_intraday import _fetch_yfinance_intraday_with_controls
from trade_research.storage import TimescaleStore
from trade_research.universe import YFinanceIntradayInstrument

_NSE_TIMEZONE = ZoneInfo("Asia/Kolkata")


class NSEMinuteProvider(Protocol):
    def fetch_intraday_ohlcv(
        self,
        instruments: list[YFinanceIntradayInstrument],
        start: datetime,
        end: datetime,
        interval: str = "1m",
    ) -> pd.DataFrame: ...


def run_yfinance_nse_minute_pipeline(
    *,
    from_datetime: str | None = None,
    to_datetime: str | None = None,
    symbol_limit: int | None = None,
    trigger: str = "pipeline",
    provider: NSEMinuteProvider | None = None,
    at: datetime | None = None,
) -> PipelineRunResult:
    """Load bounded, completed-session NSE 1m candles into ClickHouse."""

    settings = get_settings()
    if not settings.yfinance_nse_minute_enabled:
        raise ValueError("NSE minute ingestion is disabled by configuration.")
    observed_at = _as_utc(at or datetime.now(UTC))
    end = _parse_datetime(to_datetime, "to_datetime") if to_datetime else observed_at
    request_floor = end - timedelta(days=settings.yfinance_nse_minute_lookback_days)
    start = (
        _parse_datetime(from_datetime, "from_datetime")
        if from_datetime
        else request_floor
    )
    if start >= end:
        raise ValueError("from_datetime must be before to_datetime.")
    if start < request_floor:
        raise ValueError(
            "Requested NSE 1m window exceeds the configured request safety limit: "
            f"{settings.yfinance_nse_minute_lookback_days} days."
        )
    configured_limit = settings.yfinance_nse_minute_max_symbols_per_run
    if symbol_limit is not None and not 1 <= symbol_limit <= configured_limit:
        raise ValueError(
            f"symbol_limit must be between 1 and {configured_limit} for NSE minute ingestion."
        )

    db = TimescaleStore(settings.database_url)
    db.initialize()
    session_rows = db.exchange_sessions(
        "NSE",
        start.astimezone(_NSE_TIMEZONE).date(),
        end.astimezone(_NSE_TIMEZONE).date(),
    )
    eligible_sessions = {
        row["session_date"]
        for row in session_rows
        if row["is_trading_day"]
        and str(row["validation_status"]).startswith("valid")
        and row.get("market_close_utc") is not None
        and _as_utc(row["market_close_utc"])
        <= observed_at - timedelta(minutes=settings.yfinance_provider_grace_minutes)
    }
    if not eligible_sessions:
        raise ValueError("No completed NSE sessions are eligible in the requested minute window.")

    instrument_rows = db.active_yfinance_daily_instruments("NSE")
    selected_limit = symbol_limit or configured_limit
    selected_rows = instrument_rows[:selected_limit]
    instruments = [_instrument(row) for row in selected_rows]
    if not instruments:
        raise ValueError("No active NSE yfinance instruments are available.")

    run_id = db.start_ingestion_run(
        job_name="yfinance_nse_1m_ohlcv",
        exchange="NSE",
        source="yfinance",
        items_requested=len(instruments),
        run_metadata={
            "trigger": trigger,
            "interval": "1m",
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "request_lookback_limit_days": settings.yfinance_nse_minute_lookback_days,
            "adapter_version": settings.phase3_yfinance_adapter_version,
            "selected_symbols": len(instruments),
        },
    )
    failures: list[dict[str, str]] = []
    raw_frame = _fetch_yfinance_intraday_with_controls(
        provider=provider or YFinanceIntradayProvider(auto_adjust=False),
        instruments=instruments,
        start=start,
        end=end,
        interval="1m",
        limiter=build_provider_rate_limiter(settings),
        db=db,
        run_id=str(run_id),
        failures=failures,
    )
    frame = _completed_session_rows(raw_frame, eligible_sessions)
    canonical_instrument_ids = {
        instrument.yahoo_symbol: str(row["canonical_instrument_id"])
        for instrument, row in zip(instruments, selected_rows, strict=True)
    }
    validated = prepare_yfinance_batch(
        settings=settings,
        database_engine=db.engine,
        frame=frame,
        raw_frame=raw_frame,
        exchange="NSE",
        interval=CandleInterval.ONE_MINUTE,
        run_id=str(run_id),
        window_start=start,
        window_end=end,
        provider_symbols=[instrument.yahoo_symbol for instrument in instruments],
        canonical_instrument_ids=canonical_instrument_ids,
        eligible_sessions=eligible_sessions,
        retrieved_at=observed_at,
        adapter_version=settings.phase3_yfinance_adapter_version,
    )
    failed_instrument_keys = {str(failure.get("instrument_key") or "") for failure in failures}
    unavailable_provider_symbols = {
        instrument.yahoo_symbol
        for instrument in instruments
        if instrument.instrument_key in failed_instrument_keys
    }
    availability_observations = observe_nse_minute_availability(
        request=validated.request,
        source_run_id=str(run_id),
        raw_frame=raw_frame,
        canonical_instrument_ids=canonical_instrument_ids,
        eligible_sessions=eligible_sessions,
        unavailable_provider_symbols=unavailable_provider_symbols,
        raw_artifact_id=(
            validated.raw_snapshot.artifact_manifest_id
            if validated.raw_snapshot is not None
            else None
        ),
    )
    MarketDataAvailabilityRepository(db.engine).record(availability_observations)
    observed_availability_sessions, unavailable_reason_codes = (
        availability_session_maps(availability_observations)
    )
    MarketDataQualityRepository(db.engine).record(
        nse_minute_missing_quality_outcomes(
            request=validated.request,
            source_run_id=str(run_id),
            canonical_instrument_ids=canonical_instrument_ids,
            eligible_sessions=eligible_sessions,
            accepted=validated.candles,
            unavailable_provider_symbols=unavailable_provider_symbols,
            observed_availability_sessions=observed_availability_sessions,
            unavailable_reason_codes=unavailable_reason_codes,
        )
    )
    clickhouse_rows = replicate_validated_batch(
        settings,
        validated,
        database_engine=db.engine,
        source_run_id=str(run_id),
        source_store="validated_batch",
        version=int(observed_at.timestamp() * 1_000_000),
    )
    status = "completed_with_failures" if failures else (
        "completed" if clickhouse_rows else "completed_empty"
    )
    db.finish_ingestion_run(
        run_id,
        status=status,
        items_processed=len(instruments),
        items_succeeded=max(len(instruments) - len(failures), 0),
        items_failed=len(failures),
        run_metadata_patch={
            "raw_rows": len(raw_frame),
            "completed_session_rows": len(frame),
            "clickhouse_rows": clickhouse_rows,
            "raw_snapshot_uri": (
                validated.raw_snapshot.storage_uri if validated.raw_snapshot else None
            ),
            "eligible_sessions": len(eligible_sessions),
            "availability_observations": len(availability_observations),
        },
    )
    warnings = [
        *(
            [f"yfinance NSE minute fetch recorded {len(failures)} failures."]
            if failures
            else []
        ),
        *(["No completed-session NSE minute rows were returned."] if frame.empty else []),
    ]
    return PipelineRunResult(
        name="yfinance_nse_1m_ohlcv",
        status="warn" if warnings else "pass",
        rows=clickhouse_rows,
        metrics={
            "trigger": trigger,
            "run_id": run_id,
            "interval": "1m",
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "request_lookback_limit_days": settings.yfinance_nse_minute_lookback_days,
            "selected_symbols": len(instruments),
            "eligible_sessions": len(eligible_sessions),
            "raw_rows": len(raw_frame),
            "validated_rows": len(validated.candles),
            "clickhouse_rows": clickhouse_rows,
            "failure_rows": len(failures),
            "availability_observations": len(availability_observations),
            "raw_snapshot_uri": (
                validated.raw_snapshot.storage_uri if validated.raw_snapshot else None
            ),
        },
        warnings=warnings,
    )


def _instrument(row: dict) -> YFinanceIntradayInstrument:
    provider_symbol = str(row["provider_symbol"])
    return YFinanceIntradayInstrument(
        symbol=str(row.get("exchange_symbol") or provider_symbol.removesuffix(".NS")),
        yahoo_symbol=provider_symbol,
        instrument_key=str(row.get("provider_instrument_key") or f"YF|{provider_symbol}"),
        name=str(row.get("name") or provider_symbol),
        exchange="NSE",
        asset_class="equity",
        currency="INR",
    )


def _completed_session_rows(frame: pd.DataFrame, eligible_sessions: set[date]) -> pd.DataFrame:
    if frame.empty:
        return frame
    if "Timestamp" not in frame.columns:
        raise ValueError("NSE minute frame does not contain a Timestamp column.")
    timestamps = pd.to_datetime(frame["Timestamp"], errors="coerce", utc=True)
    local_dates = timestamps.dt.tz_convert(_NSE_TIMEZONE).dt.date
    completed = timestamps.notna() & local_dates.isin(eligible_sessions)
    return frame.loc[completed].reset_index(drop=True)


def _parse_datetime(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO datetime: {value}") from exc
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
