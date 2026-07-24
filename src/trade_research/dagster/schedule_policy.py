from __future__ import annotations

from dataclasses import dataclass

from trade_research.config import Settings


@dataclass(frozen=True)
class SchedulePolicy:
    schedule_name: str
    job_name: str
    cron_schedule: str
    execution_timezone: str
    desired_status: str
    notes: str


def schedule_policy(settings: Settings) -> list[SchedulePolicy]:
    any_daily_exchange = settings.yfinance_daily_enabled and any(
        (
            settings.yfinance_nse_enabled,
            settings.yfinance_full_tsx_enabled,
            settings.yfinance_full_us_enabled,
        )
    )
    nse_yfinance = settings.yfinance_daily_enabled and settings.yfinance_nse_enabled
    tsx_yfinance = settings.yfinance_daily_enabled and settings.yfinance_full_tsx_enabled
    us_yfinance = settings.yfinance_daily_enabled and settings.yfinance_full_us_enabled
    daily_research = (
        settings.nse_daily_primary_source == "yfinance" and nse_yfinance
    ) or (
        settings.nse_daily_primary_source == "upstox"
        and settings.legacy_upstox_nse_enabled
    )
    calendar_status = _status(settings.materialized_exchange_sessions_enabled)
    return [
        SchedulePolicy(
            "daily_research_schedule",
            "daily_research_pipeline_job",
            "30 19 * * 1-5",
            "Asia/Kolkata",
            _status(daily_research),
            "Daily research for the configured NSE primary provider.",
        ),
        SchedulePolicy(
            "yfinance_daily_work_planner_schedule",
            "yfinance_daily_work_planner_job",
            "0 6 * * *",
            "UTC",
            _status(any_daily_exchange),
            "Plans bounded incremental work for enabled yfinance exchanges.",
        ),
        SchedulePolicy(
            "yfinance_nse_completed_session_work_planner_schedule",
            "yfinance_nse_completed_session_work_planner_job",
            "15 12 * * 1-5",
            "UTC",
            _status(nse_yfinance),
            "Plans NSE work after close and the Yahoo provider grace period.",
        ),
        SchedulePolicy(
            "yfinance_daily_work_worker_schedule",
            "yfinance_daily_work_worker_job",
            "*/5 * * * *",
            "UTC",
            _status(any_daily_exchange),
            "Executes one bounded durable yfinance work batch.",
        ),
        SchedulePolicy(
            "nse_completed_session_opportunity_targets_schedule",
            "nse_completed_session_opportunity_targets_job",
            "15 13-18 * * 1-5",
            "UTC",
            _status(nse_yfinance),
            "Coverage-gated NSE Opportunity refresh; current sessions become no-ops.",
        ),
        SchedulePolicy(
            "tsx_completed_session_opportunity_targets_schedule",
            "tsx_completed_session_opportunity_targets_job",
            "15 22-23 * * 1-5; 15 0-3 * * 2-6",
            "UTC",
            _status(tsx_yfinance),
            "Coverage-gated TSX Opportunity refresh across the post-close window.",
        ),
        SchedulePolicy(
            "us_completed_session_opportunity_targets_schedule",
            "us_completed_session_opportunity_targets_job",
            "15 22-23 * * 1-5; 15 0-3 * * 2-6",
            "UTC",
            _status(us_yfinance),
            "Coverage-gated US Opportunity refresh across the post-close window.",
        ),
        SchedulePolicy(
            "nse_universe_refresh_schedule",
            "nse_universe_refresh_job",
            "0 8 * * 1-5",
            "Asia/Kolkata",
            _status(nse_yfinance),
            "Refreshes the accepted NSE universe.",
        ),
        SchedulePolicy(
            "tsx_universe_refresh_schedule",
            "tsx_universe_refresh_job",
            "0 8 * * 1-5",
            "America/Toronto",
            _status(tsx_yfinance),
            "Refreshes the accepted TSX universe.",
        ),
        SchedulePolicy(
            "us_universe_refresh_schedule",
            "us_universe_refresh_job",
            "0 8 * * 1-5",
            "America/New_York",
            _status(us_yfinance),
            "Refreshes the accepted US universe.",
        ),
        SchedulePolicy(
            "nse_exchange_sessions_schedule",
            "nse_exchange_sessions_job",
            "0 6 1 * *",
            "Asia/Kolkata",
            calendar_status,
            "Materializes NSE exchange sessions.",
        ),
        SchedulePolicy(
            "tsx_exchange_sessions_schedule",
            "tsx_exchange_sessions_job",
            "0 6 1 * *",
            "America/Toronto",
            calendar_status,
            "Materializes TSX exchange sessions.",
        ),
        SchedulePolicy(
            "us_exchange_sessions_schedule",
            "us_exchange_sessions_job",
            "0 6 1 * *",
            "America/New_York",
            calendar_status,
            "Materializes US exchange sessions.",
        ),
        SchedulePolicy(
            "bigquery_daily_sync_schedule",
            "bigquery_export_sync_job",
            "30 8 * * *",
            "UTC",
            _status(
                settings.bigquery_enabled
                and settings.bigquery_production_sync_enabled
            ),
            "Synchronizes the enabled production reporting replica.",
        ),
        SchedulePolicy(
            "north_america_daily_yfinance_schedule",
            "north_america_daily_yfinance_job",
            "30 3 * * 2-6",
            "Asia/Kolkata",
            "stopped",
            "Legacy direct path remains stopped; durable work owns daily candles.",
        ),
        SchedulePolicy(
            "fx_intraday_dukascopy_schedule",
            "fx_intraday_dukascopy_job",
            "15 * * * 1-5",
            "UTC",
            "stopped",
            "Stopped because the Dukascopy datafeed is not production-ready.",
        ),
        SchedulePolicy(
            "yfinance_fx_intraday_schedule",
            "yfinance_fx_intraday_job",
            "20 * * * *",
            "UTC",
            "stopped",
            "Stopped until an intraday cadence is explicitly approved.",
        ),
    ]


def desired_schedule_statuses(settings: Settings) -> dict[str, str]:
    return {row.schedule_name: row.desired_status for row in schedule_policy(settings)}


def _status(enabled: bool) -> str:
    return "running" if enabled else "stopped"
