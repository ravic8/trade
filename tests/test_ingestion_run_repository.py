from uuid import UUID

from sqlalchemy import select

from trade_data.storage.ingestion_runs import IngestionRunRepository
from trade_data.storage.timescale import TimescaleStore, ingestion_runs_table


def test_ingestion_run_lifecycle_keeps_public_store_contract() -> None:
    store = TimescaleStore("sqlite://")
    ingestion_runs_table.create(store.engine)
    engine = store.engine
    store = IngestionRunRepository(engine, ingestion_runs_table)

    def get_run(run_id):
        with engine.connect() as c:
            return (
                c.execute(
                    select(ingestion_runs_table).where(ingestion_runs_table.c.run_id == str(run_id))
                )
                .mappings()
                .one()
            )

    run_id = store.start(
        job_name="daily_ingestion",
        exchange="NSE",
        source="yfinance",
        items_requested=3,
        run_metadata={"trigger": "unit_test"},
    )
    assert isinstance(run_id, UUID)
    started = get_run(run_id)
    assert started is not None
    assert started["status"] == "running"
    assert started["items_requested"] == 3
    assert started["run_metadata"] == {"trigger": "unit_test"}

    store.finish(
        run_id=run_id,
        status="completed",
        items_processed=3,
        items_succeeded=2,
        items_failed=1,
        run_metadata_patch={"batch_count": 1},
    )
    finished = get_run(run_id)
    assert finished is not None
    assert finished["status"] == "completed"
    assert finished["items_succeeded"] == 2
    assert finished["items_failed"] == 1
    assert finished["finished_at"] is not None
    assert finished["run_metadata"] == {"trigger": "unit_test", "batch_count": 1}
