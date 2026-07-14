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
