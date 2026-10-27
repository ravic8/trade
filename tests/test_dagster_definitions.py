from dagster import DefaultScheduleStatus, Definitions

from trade_data.dagster.definitions import defs


def test_only_official_nse_jobs_are_loaded_and_repair_is_manual():
    Definitions.validate_loadable(defs)
    assert {j.name for j in defs.jobs} == {
        "nse_daily_bhavcopy_job",
        "nse_exchange_sessions_job",
        "nse_one_year_repair_job",
    }
    assert {s.name for s in defs.schedules} == {
        "nse_daily_bhavcopy_schedule",
        "nse_exchange_sessions_schedule",
    }
    assert all(s.default_status == DefaultScheduleStatus.STOPPED for s in defs.schedules)
