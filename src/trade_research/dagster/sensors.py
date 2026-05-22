from datetime import timedelta
from typing import Any

from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor

from trade_research.backlog import expected_hourly_candle_windows
from trade_research.config import get_settings
from trade_research.dagster.assets import _cached_or_fetch_holidays


def _backlog_run_requests(
    context,
    exchange: str,
    asset_name: str,
) -> list[RunRequest] | SkipReason:
    settings = get_settings()
    if not settings.hourly_backlog_enabled:
        return SkipReason("Hourly backlog recovery is disabled")

    store = context.resources.timescale_store
    try:
        holidays = _cached_or_fetch_holidays(
            store,
            exchange=exchange,
            max_age_days=settings.calendar_refresh_days,
        )
    except Exception as exc:
        return SkipReason(f"{exchange} backlog scan skipped because calendar is unavailable: {exc}")

    windows = expected_hourly_candle_windows(
        exchange=exchange,
        holidays=holidays,
        scan_days=settings.hourly_backlog_scan_days,
        min_candle_lag=timedelta(minutes=settings.hourly_backlog_min_candle_lag_minutes),
    )
    expected_symbols = store.fetchable_symbol_count(exchange)
    if expected_symbols <= 0:
        return SkipReason(f"{exchange} backlog scan has no fetchable cached symbols")

    store.scan_hourly_backlog_windows(
        exchange=exchange,
        windows=[(item.window_start, item.window_end) for item in windows],
        expected_symbol_count=expected_symbols,
        coverage_threshold=settings.hourly_backlog_coverage_threshold,
    )
    candidates = store.hourly_backlog_candidates(
        exchange=exchange,
        limit=settings.hourly_backlog_max_windows_per_tick,
        max_attempts=settings.hourly_backlog_max_attempts,
        stale_recovery_minutes=settings.hourly_backlog_stale_recovery_minutes,
    )
    requests: list[RunRequest] = []
    for item in candidates:
        store.mark_hourly_backlog_queued(exchange, item["window_start"])
        requests.append(_run_request(exchange, asset_name, item))
    if not requests:
        return SkipReason(f"{exchange} hourly backlog scan found no recovery candidates")
    return requests


def _run_request(exchange: str, asset_name: str, item: dict[str, Any]) -> RunRequest:
    window_start = item["window_start"].isoformat()
    window_end = item["window_end"].isoformat()
    return RunRequest(
        run_key=f"hourly-backlog:{exchange}:{window_start}:attempt-{item['attempt_count'] + 1}",
        run_config={
            "ops": {
                asset_name: {
                    "config": {
                        "window_start": window_start,
                        "window_end": window_end,
                    }
                }
            }
        },
        tags={
            "exchange": exchange,
            "fetch_mode": "backlog_recovery",
            "backlog_window_start": window_start,
        },
    )


@sensor(
    job_name="nse_hourly_backlog_recovery_job",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"timescale_store"},
)
def nse_hourly_backlog_sensor(context):
    result = _backlog_run_requests(context, "NSE", "nse_hourly_backlog_ohlcv")
    if isinstance(result, SkipReason):
        yield result
        return
    yield from result


@sensor(
    job_name="tsx_hourly_backlog_recovery_job",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"timescale_store"},
)
def tsx_hourly_backlog_sensor(context):
    result = _backlog_run_requests(context, "TSX", "tsx_hourly_backlog_ohlcv")
    if isinstance(result, SkipReason):
        yield result
        return
    yield from result
