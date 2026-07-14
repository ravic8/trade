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
