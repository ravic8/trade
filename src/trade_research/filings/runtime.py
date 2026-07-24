from __future__ import annotations

import logging
import socket
import threading
import time
from functools import lru_cache
from typing import Any
from uuid import uuid4

from langgraph.checkpoint.memory import InMemorySaver

from trade_research.config import Settings, get_settings
from trade_research.filings.indexing import FilingEvidenceIndexer
from trade_research.filings.models import FilingRun, FilingRunStatus
from trade_research.filings.parsers import (
    FilingParser,
    LocalFilingArtifactStore,
    S3FilingArtifactStore,
)
from trade_research.filings.store import FilingStore
from trade_research.filings.telemetry import (
    configure_telemetry,
    current_trace_id,
    filing_metrics,
    flush_configured_langfuse,
    operation_span,
)
from trade_research.filings.validators import FilingFactValidator
from trade_research.filings.workflow import (
    FilingCancelled,
    FilingWorkflow,
    WorkflowServices,
    workflow_checkpointer,
)

logger = logging.getLogger(__name__)


class RetryableFilingError(RuntimeError):
    pass


class FilingRuntime:
    def __init__(self, settings: Settings, store: FilingStore | None = None) -> None:
        self.settings = settings
        self.store = store or FilingStore(settings.database_url)
        self.store.initialize()
        self.artifact_store = (
            S3FilingArtifactStore(
                bucket=settings.filing_s3_bucket,
                prefix=settings.filing_s3_prefix,
                endpoint_url=settings.filing_s3_endpoint_url,
                region=settings.filing_s3_region,
                access_key_id=settings.filing_s3_access_key_id or "",
                secret_access_key=settings.filing_s3_secret_access_key or "",
            )
            if settings.filing_artifact_backend == "s3"
            else LocalFilingArtifactStore(settings.filing_artifact_dir)
        )
        self.parser = FilingParser(
            artifact_store=self.artifact_store,
            max_document_bytes=settings.filing_max_document_bytes,
            pdf_max_pages=settings.filing_pdf_max_pages,
            min_quality=settings.filing_parse_min_quality,
        )
        self.validator = FilingFactValidator(
            auto_approve_confidence=settings.filing_min_auto_approve_confidence
        )
        self.indexer = (
            FilingEvidenceIndexer(settings=settings, store=self.store)
            if settings.filing_index_enabled
            else None
        )
        self.memory_saver = InMemorySaver()
        configure_telemetry(settings, engine=self.store.engine)

    def run_once(
        self,
        run_id: str,
        *,
        resume_payload: dict[str, Any] | None = None,
        worker_id: str | None = None,
    ) -> FilingRun:
        worker_id = worker_id or f"{socket.gethostname()}:{uuid4()}"
        claimed = self.store.claim_run(
            run_id,
            worker_id=worker_id,
            lease_seconds=self.settings.filing_worker_lease_seconds,
        )
        if not claimed:
            existing = self.store.run(run_id)
            if existing is None:
                raise KeyError(f"filing run not found: {run_id}")
            return existing

        heartbeat = _LeaseHeartbeat(
            store=self.store,
            run_id=run_id,
            worker_id=worker_id,
            interval_seconds=self.settings.filing_worker_heartbeat_seconds,
            lease_seconds=self.settings.filing_worker_lease_seconds,
        )
        heartbeat.start()
        workflow_started = time.monotonic()
        run = self.store.run(run_id)
        assert run is not None
        try:
            with operation_span(
                self.settings,
                "filing.document.intelligence",
                observation_type="chain",
                metadata={
                    "run_id": run.run_id,
                    "thread_id": run.thread_id,
                    "workspace_id": run.workspace_id,
                    "company_id": run.company_id,
                    "filing_id": run.filing_id,
                    "graph_version": "filing-document-v1",
                    "release": self.settings.telemetry_release,
                },
            ):
                self.store.transition_run(
                    run_id,
                    status=FilingRunStatus.RUNNING,
                    current_node="graph_start",
                    progress=run.progress,
                    trace_id=current_trace_id(),
                )
                with workflow_checkpointer(
                    self.settings,
                    self.store,
                    memory_saver=self.memory_saver,
                ) as checkpointer:
                    workflow = FilingWorkflow(
                        WorkflowServices(
                            settings=self.settings,
                            store=self.store,
                            parser=self.parser,
                            artifact_store=self.artifact_store,
                            validator=self.validator,
                            indexer=self.indexer,
                            worker_id=worker_id,
                        ),
                        checkpointer=checkpointer,
                    )
                    workflow.invoke(run_id, resume_payload=resume_payload)
        except FilingCancelled:
            logger.info("filing run cancelled run_id=%s", run_id)
        except Exception as exc:
            latest = self.store.run(run_id)
            attempts = latest.attempt_count if latest else 1
            maximum = latest.max_attempts if latest else 1
            if attempts >= maximum:
                self.store.transition_run(
                    run_id,
                    status=FilingRunStatus.FAILED,
                    current_node="worker_failed",
                    error_code=type(exc).__name__,
                    error_message=str(exc)[:2_000],
                    trace_id=current_trace_id(),
                )
                logger.exception("filing run exhausted retries run_id=%s", run_id)
            else:
                self.store.transition_run(
                    run_id,
                    status=FilingRunStatus.RETRYING,
                    current_node="retry_wait",
                    error_code=type(exc).__name__,
                    error_message=str(exc)[:2_000],
                    trace_id=current_trace_id(),
                )
                logger.exception(
                    "filing run failed and will retry run_id=%s attempt=%s/%s",
                    run_id,
                    attempts,
                    maximum,
                )
                raise RetryableFilingError(str(exc)) from exc
        finally:
            heartbeat.stop()
            latest_status = self.store.run(run_id)
            filing_metrics().workflow_duration.record(
                max(time.monotonic() - workflow_started, 0),
                {
                    "graph": "filing.document.intelligence",
                    "status": (
                        latest_status.status.value if latest_status else "unknown"
                    ),
                },
            )
            flush_configured_langfuse(self.settings)
        latest = self.store.run(run_id)
        if latest is None:
            raise RuntimeError("filing run disappeared after execution")
        return latest


class _LeaseHeartbeat:
    def __init__(
        self,
        *,
        store: FilingStore,
        run_id: str,
        worker_id: str,
        interval_seconds: int,
        lease_seconds: int,
    ) -> None:
        self.store = store
        self.run_id = run_id
        self.worker_id = worker_id
        self.interval_seconds = interval_seconds
        self.lease_seconds = lease_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"filing-heartbeat-{run_id[:8]}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(self.interval_seconds * 2, 1))

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                alive = self.store.heartbeat_run(
                    self.run_id,
                    worker_id=self.worker_id,
                    lease_seconds=self.lease_seconds,
                )
                if not alive:
                    return
            except Exception:
                logger.exception("filing worker heartbeat failed run_id=%s", self.run_id)


@lru_cache(maxsize=1)
def get_filing_runtime() -> FilingRuntime:
    return FilingRuntime(get_settings())
