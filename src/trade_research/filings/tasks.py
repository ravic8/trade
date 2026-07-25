from __future__ import annotations

import logging
import os
import time
from typing import Any
from uuid import uuid4

from celery import Celery

from trade_research.config import get_settings
from trade_research.filings.runtime import RetryableFilingError, get_filing_runtime
from trade_research.filings.telemetry import configure_telemetry

logger = logging.getLogger(__name__)
settings = get_settings()

celery_app = Celery(
    "trade_research.filings",
    broker=settings.redis_url,
    backend=settings.redis_url,
)
celery_app.conf.update(
    task_default_queue=settings.filing_queue_name,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    result_expires=86_400,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)
configure_telemetry(settings, celery_app=celery_app)


@celery_app.task(
    bind=True,
    name="filings.process_run",
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_filing_run(
    self,
    run_id: str,
    resume_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime = get_filing_runtime()
    try:
        run = runtime.run_once(
            run_id,
            resume_payload=resume_payload,
            worker_id=f"celery:{self.request.hostname}:{self.request.id}",
        )
    except RetryableFilingError as exc:
        run = runtime.store.run(run_id)
        attempts = run.attempt_count if run else 1
        maximum = run.max_attempts if run else 1
        if attempts >= maximum:
            return {
                "run_id": run_id,
                "status": "failed",
                "error": str(exc),
            }
        countdown = min(2 ** max(attempts, 1), 60)
        raise self.retry(exc=exc, countdown=countdown, max_retries=maximum - 1) from exc
    return {
        "run_id": run.run_id,
        "status": run.status.value,
        "current_node": run.current_node,
    }


@celery_app.task(
    bind=True,
    name="filings.worker_recovery_probe",
    acks_late=True,
    reject_on_worker_lost=True,
)
def worker_recovery_probe(
    self,
    probe_id: str,
    *,
    hold_seconds: int = 120,
) -> dict[str, Any]:
    from trade_research.filings.resilience import (
        begin_worker_probe_attempt,
        complete_worker_probe,
        current_process_start_ticks,
        timeout_worker_probe,
    )

    bounded_hold = min(max(int(hold_seconds), 30), 300)
    status = begin_worker_probe_attempt(
        settings,
        probe_id=probe_id,
        worker_pid=os.getpid(),
        process_start_ticks=current_process_start_ticks(),
        hostname=str(self.request.hostname),
    )
    if status["attempt_count"] == 1:
        time.sleep(bounded_hold)
        timeout_worker_probe(settings, probe_id=probe_id)
        raise RuntimeError("worker termination was not observed during resilience probe")
    return complete_worker_probe(settings, probe_id=probe_id)


def dispatch_filing_run(
    run_id: str,
    *,
    resume_payload: dict[str, Any] | None = None,
    runtime=None,
) -> None:
    runtime = runtime or get_filing_runtime()
    runtime.store.mark_run_queued(run_id)
    if runtime.settings.filing_queue_mode == "inline":
        runtime.run_once(run_id, resume_payload=resume_payload, worker_id="inline")
        return
    process_filing_run.apply_async(
        args=[run_id],
        kwargs={"resume_payload": resume_payload},
        queue=runtime.settings.filing_queue_name,
        task_id=f"filing-{run_id}-{uuid4()}",
    )
