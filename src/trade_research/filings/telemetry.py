from __future__ import annotations

import logging
from contextlib import ExitStack, contextmanager
from functools import lru_cache
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from trade_research.config import Settings

logger = logging.getLogger(__name__)
_configured = False
_instrumented_apps: set[int] = set()
_instrumented_engines: set[int] = set()
_instrumented_celery: set[int] = set()


def configure_telemetry(
    settings: Settings,
    *,
    app: Any | None = None,
    engine: Any | None = None,
    celery_app: Any | None = None,
) -> None:
    global _configured
    if settings.otel_enabled and not _configured:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        resource = Resource.create(
            {
                "service.name": settings.otel_service_name,
                "deployment.environment": settings.app_env,
                "service.version": settings.telemetry_release,
            }
        )
        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=_signal_endpoint(settings, "traces"))
            )
        )
        trace.set_tracer_provider(tracer_provider)
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=_signal_endpoint(settings, "metrics"))
        )
        metrics.set_meter_provider(
            MeterProvider(resource=resource, metric_readers=[metric_reader])
        )
        _configured = True

    if app is not None and id(app) not in _instrumented_apps and settings.otel_enabled:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
        _instrumented_apps.add(id(app))
    if engine is not None and id(engine) not in _instrumented_engines and settings.otel_enabled:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        SQLAlchemyInstrumentor().instrument(engine=engine)
        _instrumented_engines.add(id(engine))
    if celery_app is not None and id(celery_app) not in _instrumented_celery:
        if settings.otel_enabled:
            from opentelemetry.instrumentation.celery import CeleryInstrumentor

            CeleryInstrumentor().instrument()
        _instrumented_celery.add(id(celery_app))


def current_trace_id() -> str:
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return "0" * 32
    return format(span_context.trace_id, "032x")


@contextmanager
def operation_span(
    settings: Settings,
    name: str,
    *,
    observation_type: str = "span",
    metadata: dict[str, Any] | None = None,
):
    safe = sanitize_telemetry(metadata or {})
    tracer = trace.get_tracer("trade_research.filings")
    with ExitStack() as stack:
        otel_span = stack.enter_context(tracer.start_as_current_span(name))
        for key, value in _flat_attributes(safe).items():
            otel_span.set_attribute(key, value)
        client = _langfuse_client(
            enabled=settings.langfuse_enabled,
            public_key=settings.langfuse_public_key or "",
            secret_key=settings.langfuse_secret_key or "",
            base_url=settings.langfuse_base_url,
            environment=settings.app_env,
            release=settings.telemetry_release,
            sample_rate=settings.langfuse_sample_rate,
        )
        langfuse_observation = None
        if client is not None:
            try:
                langfuse_observation = stack.enter_context(
                    client.start_as_current_observation(
                        name=name,
                        as_type=observation_type,
                        metadata=safe,
                        version=settings.telemetry_release,
                    )
                )
            except Exception:
                logger.exception("unable to start Langfuse observation name=%s", name)
        try:
            yield otel_span, langfuse_observation
        except Exception as exc:
            otel_span.record_exception(exc)
            raise


class FilingMetrics:
    def __init__(self) -> None:
        meter = metrics.get_meter("trade_research.filings")
        self.workflow_runs = meter.create_counter(
            "filing.workflow.runs",
            description="Filing workflow terminal and transition events.",
        )
        self.workflow_duration = meter.create_histogram(
            "filing.workflow.duration",
            unit="s",
            description="End-to-end filing workflow duration.",
        )
        self.node_duration = meter.create_histogram(
            "filing.workflow.node.duration",
            unit="s",
            description="Filing workflow node duration.",
        )
        self.extraction_candidates = meter.create_counter(
            "filing.extraction.candidates",
            description="Extracted filing candidates.",
        )
        self.validation_defects = meter.create_counter(
            "filing.validation.defects",
            description="Validation defects by stable rule and severity.",
        )
        self.review_decisions = meter.create_counter(
            "filing.review.decisions",
            description="Human review decisions.",
        )
        self.resume_count = meter.create_counter(
            "filing.workflow.resumes",
            description="Durable workflow resumes.",
        )


@lru_cache(maxsize=1)
def filing_metrics() -> FilingMetrics:
    return FilingMetrics()


@lru_cache(maxsize=8)
def _langfuse_client(
    *,
    enabled: bool,
    public_key: str,
    secret_key: str,
    base_url: str,
    environment: str,
    release: str,
    sample_rate: float,
):
    if not enabled:
        return None
    try:
        from langfuse import Langfuse

        return Langfuse(
            public_key=public_key,
            secret_key=secret_key,
            base_url=base_url,
            environment=environment,
            release=release,
            sample_rate=sample_rate,
            mask=sanitize_telemetry,
        )
    except Exception:
        logger.exception("unable to initialize Langfuse; continuing without export")
        return None


def flush_configured_langfuse(settings: Settings) -> None:
    client = _langfuse_client(
        enabled=settings.langfuse_enabled,
        public_key=settings.langfuse_public_key or "",
        secret_key=settings.langfuse_secret_key or "",
        base_url=settings.langfuse_base_url,
        environment=settings.app_env,
        release=settings.telemetry_release,
        sample_rate=settings.langfuse_sample_rate,
    )
    if client is not None:
        try:
            client.flush()
        except Exception:
            logger.exception("unable to flush Langfuse events")


def sanitize_telemetry(value: Any) -> Any:
    sensitive_keys = {
        "raw_text",
        "text",
        "content",
        "document_text",
        "snippet",
        "prompt",
        "completion",
    }
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if str(key).lower() in sensitive_keys:
                sanitized[str(key)] = "[REDACTED]"
            else:
                sanitized[str(key)] = sanitize_telemetry(item)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [sanitize_telemetry(item) for item in value]
    if isinstance(value, str) and len(value) > 500:
        return value[:120] + "...[TRUNCATED]"
    return value


def _signal_endpoint(settings: Settings, signal: str) -> str:
    endpoint = (settings.otel_exporter_otlp_endpoint or "").rstrip("/")
    if endpoint.endswith(f"/v1/{signal}"):
        return endpoint
    return f"{endpoint}/v1/{signal}"


def _flat_attributes(metadata: dict[str, Any]) -> dict[str, str | int | float | bool]:
    output: dict[str, str | int | float | bool] = {}
    for key, value in metadata.items():
        attribute_key = f"filing.{key}"
        if isinstance(value, (str, int, float, bool)):
            output[attribute_key] = value
        elif value is not None:
            output[attribute_key] = str(value)[:500]
    return output
