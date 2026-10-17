from uuid import UUID

from trade_data.storage.timescale import TimescaleStore, ingestion_runs_table


def test_ingestion_run_lifecycle_keeps_public_store_contract() -> None:
    store = TimescaleStore("sqlite://")
    ingestion_runs_table.create(store.engine)

    run_id = store.start_ingestion_run(
        job_name="daily_ingestion",
        exchange="NSE",
        source="yfinance",
        items_requested=3,
        run_metadata={"trigger": "unit_test"},
    )
    assert isinstance(run_id, UUID)
    started = store.ingestion_run(str(run_id))
    assert started is not None
    assert started["status"] == "running"
    assert started["items_requested"] == 3
    assert started["run_metadata"] == {"trigger": "unit_test"}

    store.finish_ingestion_run(
        run_id=run_id,
        status="completed",
        items_processed=3,
        items_succeeded=2,
        items_failed=1,
        run_metadata_patch={"batch_count": 1},
    )
    finished = store.ingestion_run(str(run_id))
    assert finished is not None
    assert finished["status"] == "completed"
    assert finished["items_succeeded"] == 2
    assert finished["items_failed"] == 1
    assert finished["finished_at"] is not None
    assert finished["run_metadata"] == {"trigger": "unit_test", "batch_count": 1}
