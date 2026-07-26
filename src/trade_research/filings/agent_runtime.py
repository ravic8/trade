from __future__ import annotations

import logging

from trade_research.filings.agent_llm import FilingAgentLLM
from trade_research.filings.agent_tools import InvestigationToolGateway
from trade_research.filings.agent_workflow import (
    InvestigationServices,
    InvestigationWorkflow,
)
from trade_research.filings.models import InvestigationRun, InvestigationStatus
from trade_research.filings.telemetry import (
    current_trace_id,
    flush_configured_langfuse,
    operation_span,
)
from trade_research.filings.workflow import workflow_checkpointer

logger = logging.getLogger(__name__)


def run_investigation_once(runtime, analysis_id: str) -> InvestigationRun:
    run = runtime.store.investigation(analysis_id)
    if not run:
        raise KeyError(f"filing investigation not found: {analysis_id}")
    llm = FilingAgentLLM(runtime.settings)
    try:
        with operation_span(
            runtime.settings,
            "filing.market.investigation",
            observation_type="agent",
            metadata={
                "analysis_id": run.analysis_id,
                "thread_id": run.thread_id,
                "workspace_id": run.workspace_id,
                "universe_id": run.universe_id,
                "graph_version": "filing-market-investigation-v1",
                "prompt_version": runtime.settings.filing_agent_prompt_version,
            },
        ):
            runtime.store.transition_investigation(
                analysis_id,
                status=InvestigationStatus.RUNNING,
                current_node="graph_start",
                progress=0.04,
                detail={"status": "running"},
                trace_id=current_trace_id(),
            )
            with workflow_checkpointer(
                runtime.settings,
                runtime.store,
                memory_saver=runtime.memory_saver,
            ) as checkpointer:
                InvestigationWorkflow(
                    InvestigationServices(
                        settings=runtime.settings,
                        store=runtime.store,
                        tools=InvestigationToolGateway(store=runtime.store),
                        llm=llm,
                    ),
                    checkpointer=checkpointer,
                ).invoke(analysis_id)
    except Exception as exc:
        runtime.store.transition_investigation(
            analysis_id,
            status=InvestigationStatus.FAILED,
            current_node="failed",
            progress=1.0,
            detail={"error_type": type(exc).__name__},
            error_code=type(exc).__name__,
            error_message=str(exc),
            trace_id=current_trace_id(),
        )
        logger.exception("filing investigation failed analysis_id=%s", analysis_id)
    finally:
        llm.close()
        flush_configured_langfuse(runtime.settings)
    latest = runtime.store.investigation(analysis_id)
    if not latest:
        raise RuntimeError("filing investigation disappeared after execution")
    return latest
