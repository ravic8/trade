from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine

from trade_research.operations import WorkflowRequestStore
from trade_research.storage.timescale import metadata


def _store(tmp_path) -> WorkflowRequestStore:
    engine = create_engine(f"sqlite:///{tmp_path / 'workflows.sqlite'}")
    metadata.create_all(engine)
    return WorkflowRequestStore(engine=engine)


def test_workflow_submission_is_durable_and_idempotent(tmp_path) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 4, tzinfo=UTC)
    first, created = store.submit(
        workflow_type="upstox_daily_ohlcv",
        request_payload={"symbols": ["INFY"], "start_date": "2026-09-01"},
        idempotency_key="user-request-1",
        requested_by="user@example.com",
        now=now,
    )
    replay, replay_created = store.submit(
        workflow_type="upstox_daily_ohlcv",
        request_payload={"start_date": "2026-09-01", "symbols": ["INFY"]},
        idempotency_key="user-request-1",
        requested_by="user@example.com",
        now=now,
    )

    assert created is True
    assert replay_created is False
    assert replay.workflow_id == first.workflow_id
    assert [row.workflow_id for row in store.queued("upstox_daily_ohlcv")] == [
        first.workflow_id
    ]


def test_workflow_idempotency_key_cannot_be_rebound(tmp_path) -> None:
    store = _store(tmp_path)
    store.submit(
        workflow_type="upstox_daily_ohlcv",
        request_payload={"symbols": ["INFY"]},
        idempotency_key="user-request-1",
        requested_by="user@example.com",
    )

    with pytest.raises(ValueError, match="different workflow request"):
        store.submit(
            workflow_type="upstox_daily_ohlcv",
            request_payload={"symbols": ["TCS"]},
            idempotency_key="user-request-1",
            requested_by="user@example.com",
        )


def test_workflow_state_tracks_dagster_and_result_runs(tmp_path) -> None:
    store = _store(tmp_path)
    workflow, _ = store.submit(
        workflow_type="upstox_daily_ohlcv",
        request_payload={"symbols": ["INFY"]},
        idempotency_key="user-request-1",
        requested_by="user@example.com",
    )

    store.mark_running(workflow.workflow_id, "dagster-run-1")
    running = store.get(workflow.workflow_id)
    assert running is not None
    assert running.status == "running"
    assert running.dagster_run_id == "dagster-run-1"

    store.mark_completed(workflow.workflow_id, result_run_id="ingestion-run-1")
    completed = store.get(workflow.workflow_id)
    assert completed is not None
    assert completed.status == "succeeded"
    assert completed.result_run_id == "ingestion-run-1"
    assert completed.completed_at is not None
