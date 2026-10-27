"""Optional OpenTelemetry setup for daily API and database operations."""

from __future__ import annotations

from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from trade_data.config import Settings

_configured = False
_instrumented_apps: set[int] = set()
_instrumented_engines: set[int] = set()


def configure_telemetry(
    settings: Settings,
    *,
    app: Any | None = None,
    engine: Any | None = None,
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
            BatchSpanProcessor(OTLPSpanExporter(endpoint=_signal_endpoint(settings, "traces")))
        )
        trace.set_tracer_provider(tracer_provider)
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=_signal_endpoint(settings, "metrics"))
        )
        metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))
        _configured = True

    if app is not None and id(app) not in _instrumented_apps and settings.otel_enabled:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
        _instrumented_apps.add(id(app))
    if engine is not None and id(engine) not in _instrumented_engines and settings.otel_enabled:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        SQLAlchemyInstrumentor().instrument(engine=engine)
        _instrumented_engines.add(id(engine))


def _signal_endpoint(settings: Settings, signal: str) -> str:
    endpoint = (settings.otel_exporter_otlp_endpoint or "").rstrip("/")
    if endpoint.endswith(f"/v1/{signal}"):
        return endpoint
    return f"{endpoint}/v1/{signal}"
