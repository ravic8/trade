from importlib import import_module

import pytest

dagster = pytest.importorskip("dagster")
definitions = import_module("trade_research.dagster.definitions")


def test_fx_intraday_dukascopy_job_and_schedule_are_registered() -> None:
    assert definitions.fx_intraday_dukascopy_job.name == "fx_intraday_dukascopy_job"
    assert (
        definitions.fx_intraday_dukascopy_schedule.name
        == "fx_intraday_dukascopy_schedule"
    )
    assert (
        definitions.fx_intraday_dukascopy_schedule.default_status
        == dagster.DefaultScheduleStatus.STOPPED
    )


def test_yfinance_fx_intraday_job_and_schedule_are_registered() -> None:
    assert definitions.yfinance_fx_intraday_job.name == "yfinance_fx_intraday_job"
    assert (
        definitions.yfinance_fx_intraday_schedule.name
        == "yfinance_fx_intraday_schedule"
    )
    assert (
        definitions.yfinance_fx_intraday_schedule.default_status
        == dagster.DefaultScheduleStatus.STOPPED
    )


def test_phase2_universe_refresh_jobs_and_schedules_are_stopped_by_default() -> None:
    for exchange in ("nse", "tsx", "us"):
        job = getattr(definitions, f"{exchange}_universe_refresh_job")
        schedule = getattr(definitions, f"{exchange}_universe_refresh_schedule")
        assert job.name == f"{exchange}_universe_refresh_job"
        assert schedule.name == f"{exchange}_universe_refresh_schedule"
        assert schedule.default_status == dagster.DefaultScheduleStatus.STOPPED


def test_phase3_exchange_session_jobs_and_schedules_are_stopped_by_default() -> None:
    for exchange in ("nse", "tsx", "us"):
        job = getattr(definitions, f"{exchange}_exchange_sessions_job")
        schedule = getattr(definitions, f"{exchange}_exchange_sessions_schedule")
        assert job.name == f"{exchange}_exchange_sessions_job"
        assert schedule.name == f"{exchange}_exchange_sessions_schedule"
        assert schedule.default_status == dagster.DefaultScheduleStatus.STOPPED


def test_phase5_yfinance_queue_jobs_and_schedules_are_stopped_by_default() -> None:
    for role in ("planner", "worker"):
        job = getattr(definitions, f"yfinance_daily_work_{role}_job")
        schedule = getattr(definitions, f"yfinance_daily_work_{role}_schedule")
        assert job.name == f"yfinance_daily_work_{role}_job"
        assert schedule.name == f"yfinance_daily_work_{role}_schedule"
        assert schedule.default_status == dagster.DefaultScheduleStatus.STOPPED


def test_bigquery_export_job_and_schedule_are_registered_but_stopped() -> None:
    assert definitions.bigquery_export_sync_job.name == "bigquery_export_sync_job"
    assert definitions.bigquery_daily_sync_schedule.name == "bigquery_daily_sync_schedule"
    assert (
        definitions.bigquery_daily_sync_schedule.default_status
        == dagster.DefaultScheduleStatus.STOPPED
    )
