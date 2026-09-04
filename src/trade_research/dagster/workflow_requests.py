from __future__ import annotations

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    job,
    op,
    sensor,
)

from trade_research.config import get_settings
from trade_research.credentials import resolve_provider_token
from trade_research.data.coverage import CoveragePreviewInput
from trade_research.data.on_demand import run_daily_ohlcv_request
from trade_research.operations import WorkflowRequestStore
from trade_research.schemas import DataPipelineRequest
from trade_research.storage import TimescaleStore


@op(config_schema={"workflow_id": str})
def execute_data_pipeline_request(context) -> str:
    """Execute one durable request inside the Dagster authority boundary."""

    workflow_id = str(context.op_config["workflow_id"])
    settings = get_settings()
    requests = WorkflowRequestStore(settings.database_url)
    workflow = requests.get(workflow_id)
    if workflow is None:
        raise ValueError(f"Workflow request not found: {workflow_id}")
    if workflow.status == "succeeded" and workflow.result_run_id:
        return workflow.result_run_id
    if workflow.workflow_type != "upstox_daily_ohlcv":
        raise ValueError(f"Unsupported workflow type: {workflow.workflow_type}")

    requests.mark_running(workflow_id, context.run_id)
    store = TimescaleStore(settings.database_url)
    try:
        body = DataPipelineRequest.model_validate(workflow.request_payload)
        access_token = resolve_provider_token(
            store=store,
            provider="upstox",
            fallback_token=settings.upstox_access_token,
            app_secret_key=settings.app_secret_key,
        )
        result = run_daily_ohlcv_request(
            CoveragePreviewInput(
                provider=body.provider,
                exchange=body.exchange,
                symbols=tuple(body.symbols),
                unit=body.unit,
                interval=body.interval,
                start_date=body.start_date,
                end_date=body.end_date,
            ),
            store=store,
            access_token=access_token,
            throttle_seconds=settings.data_pipeline_throttle_seconds,
            max_concurrent_fetches=settings.data_pipeline_max_concurrent_fetches,
        )
        requests.mark_completed(workflow_id, result_run_id=result.run_id)
        return result.run_id
    except Exception as exc:
        requests.mark_failed(workflow_id, error_message=str(exc))
        raise


@job(name="data_pipeline_request_job")
def data_pipeline_request_job():
    execute_data_pipeline_request()


@sensor(
    name="data_pipeline_request_sensor",
    job=data_pipeline_request_job,
    minimum_interval_seconds=15,
    default_status=DefaultSensorStatus.STOPPED,
)
def data_pipeline_request_sensor(_context):
    """Dispatch queued API requests; run keys make sensor retries idempotent."""

    requests = WorkflowRequestStore(get_settings().database_url)
    for workflow in requests.queued("upstox_daily_ohlcv"):
        yield RunRequest(
            run_key=workflow.workflow_id,
            tags={
                "workflow_id": workflow.workflow_id,
                "requested_by": workflow.requested_by,
            },
            run_config={
                "ops": {
                    "execute_data_pipeline_request": {
                        "config": {"workflow_id": workflow.workflow_id}
                    }
                }
            },
        )
