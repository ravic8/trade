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
