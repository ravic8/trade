import logging
from datetime import UTC, date, datetime, timedelta
from time import monotonic
from typing import Annotated, Literal
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import SQLAlchemyError

from trade_data.api.daily_schemas import DailyCandle, DailyCandlesResponse
from trade_data.api.security import (
    cors_origins,
    require_admin_request,
)
from trade_data.config import Settings, get_settings
from trade_data.credentials import (
    CredentialEncryptionError,
    encrypt_secret,
    provider_credential_status,
    resolve_provider_token,
)
from trade_data.dagster.schedule_policy import schedule_policy
from trade_data.dagster.status import read_dagster_schedule_statuses
from trade_data.data.coverage import CoveragePreviewInput, build_daily_coverage_preview
from trade_data.data.provider_capabilities import provider_capability
from trade_data.data.upstox import validate_upstox_access_token
from trade_data.exchange_sessions import (
    expected_dates_for_instrument,
    resolve_expected_session_dates,
)
from trade_data.market_calendar import (
    fetch_exchange_holidays,
    validated_exchange_calendar_years,
)
from trade_data.market_data.cutover import (
    CutoverStatus,
    NseProviderCutoverRepository,
)
from trade_data.market_data.health import MarketDataHealthRepository
from trade_data.operations import WorkflowRequestStore
from trade_data.schemas import (
    DataAvailabilityResponse,
    DataBulkFetchPreviewResponse,
    DataCoveragePreviewRequest,
    DataCoveragePreviewResponse,
    DataInstrumentSearchRow,
    DataPipelineHealthResponse,
    DataPipelineRequest,
    DataPipelineRunDetail,
    DataPipelineRunSummary,
    DataPipelineRunWorkItemRow,
    DataPipelineSubmission,
    DataPipelineWorkflowStatus,
    DataUniverseMemberRow,
    DataUniverseRow,
    MarketDataAvailabilityHealthRow,
    MarketDataHealthResponse,
    MarketDataQualityHealthRow,
    MarketDataQualityIssueRow,
    MarketDataRawLineageRow,
    MarketDataReplicationHealthRow,
    NseCutoverApprovalRequest,
    NseCutoverDecisionResponse,
    NseCutoverEligibilityResponse,
    NseCutoverRollbackRequest,
    NseProviderCutoverStatusResponse,
    NseProviderEvidenceRow,
    OperationsAdaptiveRateStateRow,
    OperationsFreshnessRow,
    OperationsLifecycleEventRow,
    OperationsLifecycleEventsResponse,
    OperationsOverviewResponse,
    OperationsQueueGroup,
    OperationsUniverseSnapshotRow,
    OperationsWorkItemRow,
    OperationsWorkItemsResponse,
    PipelineScheduleStatusRow,
    ProviderCapabilityResponse,
    ProviderCredentialStatusResponse,
    ProviderCredentialTestRequest,
    ProviderCredentialTestResponse,
    ProviderCredentialTokenRequest,
    ProviderRequestLogRow,
    ProviderRequestSummaryRow,
    UniverseReconciliationResponse,
)
from trade_data.storage.timescale import TimescaleStore
from trade_data.telemetry import configure_telemetry
from trade_data.universe import yfinance_universe

# Trade owns telemetry initialization through OTEL_ENABLED. Newer FastAPI
# versions can otherwise enable exporters from the endpoint environment alone.
app = FastAPI(title="Trade Daily Data API", version="0.1.0", telemetry={"auto_configure": False})
logger = logging.getLogger(__name__)
settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins(settings.api_cors_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=[
        "Content-Type",
        "X-Workspace-ID",
        "X-Actor-ID",
        "X-Idempotency-Key",
    ],
)
configure_telemetry(settings, app=app)


def _canonical_data_exchange(value: str) -> str:
    """Normalize compatibility aliases before validating or querying data."""

    normalized = value.strip().upper()
    return "TSX" if normalized == "CA" else normalized


def _operations_exchange(value: str | None) -> str | None:
    if value is None:
        return None
    exchange = _canonical_data_exchange(value)
    if exchange not in {"NSE", "TSX", "US"}:
        raise HTTPException(status_code=400, detail="exchange must be NSE, TSX, or US")
    return exchange


def _require_admin(request: Request) -> str:
    return require_admin_request(request, get_settings())


@app.middleware("http")
async def log_request_summary(request: Request, call_next):
    started_at = monotonic()
    response = await call_next(request)
    logger.info(
        "api request method=%s path=%s status=%s duration_ms=%s",
        request.method,
        request.url.path,
        response.status_code,
        max(int((monotonic() - started_at) * 1000), 0),
    )
    return response


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "time": datetime.now(UTC).isoformat()}


@app.get(
    "/api/data/provider-capabilities/upstox",
    response_model=ProviderCapabilityResponse,
)
def upstox_provider_capabilities() -> ProviderCapabilityResponse:
    capability = provider_capability("upstox")
    return ProviderCapabilityResponse(
        provider=capability.provider,
        api_version=capability.api_version,
        source_url=capability.source_url,
        historical=[
            {
                "unit": item.unit,
                "interval_min": item.interval_min,
                "interval_max": item.interval_max,
                "available_from": item.available_from,
                "max_window": item.max_window,
                "notes": item.notes,
            }
            for item in capability.historical
        ],
        rate_limits={
            "standard_api_per_second": capability.rate_limits.standard_api_per_second,
            "standard_api_per_minute": capability.rate_limits.standard_api_per_minute,
            "standard_api_per_30_minutes": capability.rate_limits.standard_api_per_30_minutes,
        },
        notes=list(capability.notes),
    )


@app.get(
    "/api/admin/provider-credentials/upstox/status",
    response_model=ProviderCredentialStatusResponse,
)
def upstox_credential_status(
    _admin_email: Annotated[str, Depends(_require_admin)],
) -> ProviderCredentialStatusResponse:
    settings = get_settings()
    try:
        status = provider_credential_status(
            _initialized_store(),
            provider="upstox",
            fallback_token=settings.upstox_access_token,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return ProviderCredentialStatusResponse(**status.__dict__)


@app.post(
    "/api/admin/provider-credentials/upstox/test",
    response_model=ProviderCredentialTestResponse,
)
def test_upstox_credential(
    request: ProviderCredentialTestRequest,
    _admin_email: Annotated[str, Depends(_require_admin)],
) -> ProviderCredentialTestResponse:
    settings = get_settings()
    try:
        token = request.access_token or _stored_upstox_access_token(_initialized_store(), settings)
    except CredentialEncryptionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    if not token:
        raise HTTPException(status_code=400, detail="No Upstox access token is configured.")

    valid, message = validate_upstox_access_token(token)
    return ProviderCredentialTestResponse(
        provider="upstox",
        valid=valid,
        checked_at=datetime.now(UTC),
        message=message,
    )


@app.post(
    "/api/admin/provider-credentials/upstox/token",
    response_model=ProviderCredentialStatusResponse,
)
def save_upstox_credential(
    request: ProviderCredentialTokenRequest,
    admin_email: Annotated[str, Depends(_require_admin)],
) -> ProviderCredentialStatusResponse:
    settings = get_settings()
    checked_at = None
    validation_status = "skipped"
    validation_message = "Validation skipped."
    if request.validate_token:
        valid, validation_message = validate_upstox_access_token(request.access_token)
        checked_at = datetime.now(UTC)
        validation_status = "valid" if valid else "invalid"
        if not valid:
            raise HTTPException(status_code=400, detail=validation_message)

    try:
        store = _initialized_store()
        store.upsert_provider_credential(
            provider="upstox",
            credential_type="access_token",
            encrypted_value=encrypt_secret(request.access_token, settings.app_secret_key),
            updated_by=admin_email,
            validation_status=validation_status,
            validation_message=validation_message,
            last_validated_at=checked_at,
        )
        status = provider_credential_status(
            store,
            provider="upstox",
            fallback_token=settings.upstox_access_token,
        )
    except CredentialEncryptionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return ProviderCredentialStatusResponse(**status.__dict__)


@app.post("/api/data/coverage/preview", response_model=DataCoveragePreviewResponse)
def data_coverage_preview(
    request: DataCoveragePreviewRequest,
) -> DataCoveragePreviewResponse:
    try:
        store = _store()
        _ensure_exchange_holidays(store, request.exchange, request.start_date, request.end_date)
        preview = build_daily_coverage_preview(
            CoveragePreviewInput(
                provider=request.provider,
                exchange=request.exchange,
                symbols=tuple(request.symbols),
                unit=request.unit,
                interval=request.interval,
                start_date=request.start_date,
                end_date=request.end_date,
            ),
            store=store,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return DataCoveragePreviewResponse(**preview)


@app.get("/api/data/coverage", response_model=DataCoveragePreviewResponse)
def data_coverage(
    symbols: Annotated[list[str], Query(min_length=1)],
    provider: str = "upstox",
    exchange: str = "NSE",
    unit: str = "days",
    interval: int = 1,
    start_date: date | None = None,
    end_date: date | None = None,
) -> DataCoveragePreviewResponse:
    if start_date is None or end_date is None:
        raise HTTPException(status_code=400, detail="start_date and end_date are required")
    try:
        store = _store()
        _ensure_exchange_holidays(store, exchange, start_date, end_date)
        preview = build_daily_coverage_preview(
            CoveragePreviewInput(
                provider=provider,
                exchange=exchange,
                symbols=tuple(symbols),
                unit=unit,
                interval=interval,
                start_date=start_date,
                end_date=end_date,
            ),
            store=store,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return DataCoveragePreviewResponse(**preview)


@app.get("/api/data/candles", response_model=DailyCandlesResponse)
def daily_candles(
    instrument_key: Annotated[str, Query(min_length=1, max_length=256)],
    start_date: date,
    end_date: date,
    provider: Literal["yfinance", "upstox"] = "yfinance",
    exchange: str = "NSE",
) -> DailyCandlesResponse:
    canonical_exchange = _operations_exchange(exchange)
    if provider == "upstox" and canonical_exchange != "NSE":
        raise HTTPException(status_code=400, detail="Upstox daily candles support only NSE.")
    if start_date.year < 2 or not 0 <= (end_date - start_date).days <= 3660:
        raise HTTPException(status_code=400, detail="Choose an ordered range of at most ten years.")
    if not instrument_key.strip():
        raise HTTPException(status_code=400, detail="instrument_key must not be blank.")
    try:
        frame = _store().daily_ohlcv_frame(
            exchange=canonical_exchange,
            source=provider,
            start_date=start_date,
            end_date=end_date,
            instrument_keys=[instrument_key.strip()],
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Daily candle data is unavailable") from exc
    return DailyCandlesResponse(
        provider=provider,
        exchange=canonical_exchange,
        instrument_key=instrument_key.strip(),
        start_date=start_date,
        end_date=end_date,
        rows=[DailyCandle(**row) for row in frame.to_dict("records")],
    )


@app.get("/api/data/availability", response_model=DataAvailabilityResponse)
def data_availability(
    provider: str = "upstox",
    exchange: str = "NSE",
    interval: str = "1d",
    start_date: date | None = None,
    end_date: date | None = None,
    query: str | None = None,
    universe_id: str | None = None,
    coverage_status: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    sort: str = "symbol",
) -> DataAvailabilityResponse:
    provider_normalized = provider.lower()
    exchange_normalized = _canonical_data_exchange(exchange)
    if provider_normalized not in {"upstox", "yfinance"}:
        raise HTTPException(
            status_code=400,
            detail="Only provider=upstox or provider=yfinance is supported.",
        )
    if provider_normalized == "upstox" and exchange_normalized != "NSE":
        raise HTTPException(status_code=400, detail="provider=upstox supports only exchange=NSE.")
    if provider_normalized == "yfinance" and exchange_normalized not in {
        "NSE",
        "US",
        "TSX",
    }:
        raise HTTPException(
            status_code=400,
            detail=("provider=yfinance supports only exchange=NSE, exchange=US, or exchange=TSX."),
        )
    interval_normalized = "1d" if interval == "daily" else interval
    if interval_normalized != "1d":
        raise HTTPException(status_code=400, detail="Only interval=1d is supported.")
    if (start_date is None) != (end_date is None):
        raise HTTPException(
            status_code=400,
            detail="start_date and end_date must be supplied together.",
        )
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be on or before end_date.")

    try:
        store = _store()
        if start_date and end_date:
            _ensure_exchange_holidays(store, exchange_normalized, start_date, end_date)
        expected_dates = (
            _expected_daily_dates(store, exchange_normalized, start_date, end_date)
            if start_date is not None and end_date is not None
            else []
        )
        expected_rows = len(expected_dates)
        if provider_normalized == "yfinance":
            if exchange_normalized == "NSE" and universe_id:
                raise ValueError("universe_id is not supported for persisted NSE coverage.")
            persisted = (
                store.persisted_universe_instruments(exchange_normalized)
                if universe_id is None
                else []
            )
            if persisted:
                symbols = [
                    {
                        "symbol": str(symbol["symbol"]),
                        "name": symbol.get("name"),
                        "instrument_key": str(symbol["instrument_key"]),
                    }
                    for symbol in persisted
                ]
            else:
                if exchange_normalized == "NSE":
                    raise ValueError("No accepted NSE universe snapshot is available.")
                seed_universe = universe_id or (
                    "canada_seed" if exchange_normalized == "TSX" else "us_seed"
                )
                seeded_symbols = yfinance_universe(seed_universe)
                invalid_symbols = [
                    symbol.symbol
                    for symbol in seeded_symbols
                    if symbol.exchange.upper() != exchange_normalized
                ]
                if invalid_symbols:
                    raise ValueError(
                        f"universe_id={seed_universe} does not match "
                        f"exchange={exchange_normalized}."
                    )
                symbols = [
                    {
                        "symbol": symbol.symbol,
                        "name": symbol.name,
                        "instrument_key": f"YF|{symbol.yahoo_symbol or symbol.symbol}",
                    }
                    for symbol in seeded_symbols
                ]
            payload = store.seeded_daily_ohlcv_availability(
                symbols=symbols,
                source=provider_normalized,
                exchange=exchange_normalized,
                start_date=start_date,
                end_date=end_date,
                query_text=query,
                coverage_status=coverage_status,
                expected_rows_per_symbol=expected_rows,
                expected_session_dates=expected_dates,
                limit=limit,
                offset=offset,
                sort=sort,
            )
        else:
            payload = store.daily_ohlcv_availability(
                source=provider_normalized,
                exchange=exchange_normalized,
                start_date=start_date,
                end_date=end_date,
                query_text=query,
                universe_id=universe_id,
                coverage_status=coverage_status,
                expected_rows_per_symbol=expected_rows,
                expected_session_dates=expected_dates,
                limit=limit,
                offset=offset,
                sort=sort,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    return DataAvailabilityResponse(
        provider=provider_normalized,
        exchange=exchange_normalized,
        interval="1d",
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
        sort=sort,
        total=payload["total"],
        rows=payload["rows"],
        summary=payload["summary"],
    )


@app.get("/api/data/provider-runs", response_model=list[DataPipelineRunSummary])
def provider_runs(
    provider: str | None = None,
    exchange: str | None = None,
    job: str | None = None,
    status: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[DataPipelineRunSummary]:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be on or before end_date.")
    try:
        rows = _store().provider_runs(
            limit=limit,
            offset=offset,
            source=provider,
            exchange=_canonical_data_exchange(exchange) if exchange else None,
            job_name=job,
            status=status,
            start_date=start_date,
            end_date=end_date,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [_to_data_pipeline_run_summary(row) for row in rows]


@app.get("/api/data/provider-request-summary", response_model=list[ProviderRequestSummaryRow])
def provider_request_summary(
    run_id: str | None = None,
    provider: str | None = None,
    exchange: str | None = None,
    job: str | None = None,
    endpoint_group: str | None = None,
    status: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[ProviderRequestSummaryRow]:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be on or before end_date.")
    try:
        rows = _store().provider_request_summary(
            run_id=run_id,
            provider=provider,
            endpoint_group=endpoint_group,
            status=status,
            exchange=_canonical_data_exchange(exchange) if exchange else None,
            job_name=job,
            start_date=start_date,
            end_date=end_date,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [_to_provider_request_summary_row(row) for row in rows]


@app.get("/api/data/provider-request-logs", response_model=list[ProviderRequestLogRow])
def provider_request_logs(
    run_id: str | None = None,
    provider: str | None = None,
    exchange: str | None = None,
    job: str | None = None,
    endpoint_group: str | None = None,
    status: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[ProviderRequestLogRow]:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be on or before end_date.")
    try:
        rows = _store().provider_request_logs(
            limit=limit,
            offset=offset,
            run_id=run_id,
            provider=provider,
            endpoint_group=endpoint_group,
            status=status,
            exchange=_canonical_data_exchange(exchange) if exchange else None,
            job_name=job,
            start_date=start_date,
            end_date=end_date,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [_to_provider_request_log_row(row) for row in rows]


@app.get("/api/data/schedules/status", response_model=list[PipelineScheduleStatusRow])
def data_schedule_status() -> list[PipelineScheduleStatusRow]:
    current = get_settings()
    policies = schedule_policy(current)
    actual = read_dagster_schedule_statuses(
        current.dagster_readonly_home,
        {row.schedule_name: row.job_name for row in policies},
    )
    rows: list[PipelineScheduleStatusRow] = []
    observed_at = datetime.now(UTC)
    for policy in policies:
        observed = actual.get(policy.schedule_name)
        actual_status = observed.actual_status if observed else "unknown"
        status_drift = (
            actual_status != policy.desired_status if actual_status != "unknown" else None
        )
        if observed and observed.origin_drift:
            status_drift = True
        if policy.desired_status == "stopped":
            freshness_status = "not_applicable"
        elif not observed or not observed.last_successful_run_at:
            freshness_status = "unknown"
        elif (
            observed_at - observed.last_successful_run_at
        ).total_seconds() > policy.freshness_sla_minutes * 60:
            freshness_status = "stale"
        else:
            freshness_status = "fresh"
        drift_reasons: list[str] = []
        if status_drift:
            drift_reasons.append("status_or_origin")
        if freshness_status == "stale":
            drift_reasons.append("freshness_sla")
        rows.append(
            PipelineScheduleStatusRow(
                schedule_name=policy.schedule_name,
                job_name=policy.job_name,
                cron_schedule=policy.cron_schedule,
                execution_timezone=policy.execution_timezone,
                exchange=policy.exchange,
                freshness_sla_minutes=policy.freshness_sla_minutes,
                upstream_dependencies=list(policy.upstream_dependencies),
                alert_owner=policy.alert_owner,
                desired_status=policy.desired_status,
                intended_status=policy.desired_status,
                actual_status=actual_status,
                status_drift=status_drift,
                origin_health=observed.origin_health if observed else "unknown",
                origin_drift=observed.origin_drift if observed else None,
                stored_origin_count=observed.stored_origin_count if observed else 0,
                active_origin_count=observed.active_origin_count if observed else 0,
                last_tick_status=observed.last_tick_status if observed else None,
                last_tick_at=observed.last_tick_at if observed else None,
                last_run_status=observed.last_run_status if observed else None,
                last_run_at=observed.last_run_at if observed else None,
                last_successful_run_at=(observed.last_successful_run_at if observed else None),
                freshness_status=freshness_status,
                drift_reasons=drift_reasons,
                notes=policy.notes,
            )
        )
    return rows


@app.get("/api/data/bulk-fetch-preview", response_model=DataBulkFetchPreviewResponse)
def data_bulk_fetch_preview(
    provider: str = "yfinance",
    exchange: str = "US",
    interval: str = "1d",
    universe_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    query: str | None = None,
    coverage_status: str | None = None,
    min_avg_daily_turnover: Annotated[float | None, Query(ge=0.0)] = None,
    min_coverage_pct: Annotated[float | None, Query(ge=0.0, le=1.0)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    sort: str = "-missing_rows",
) -> DataBulkFetchPreviewResponse:
    if start_date is None or end_date is None:
        raise HTTPException(status_code=400, detail="start_date and end_date are required")
    provider_normalized = provider.lower()
    exchange_normalized = _canonical_data_exchange(exchange)
    if provider_normalized != "yfinance":
        raise HTTPException(
            status_code=400,
            detail="Only provider=yfinance is supported for bulk fetch preview.",
        )
    if exchange_normalized not in {"US", "TSX"}:
        raise HTTPException(
            status_code=400,
            detail="provider=yfinance supports only exchange=US or exchange=TSX.",
        )
    if interval not in {"1d", "daily"}:
        raise HTTPException(status_code=400, detail="Only interval=1d is supported.")
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be on or before end_date.")

    try:
        store = _store()
        payload = _build_yfinance_bulk_fetch_preview(
            store=store,
            exchange=exchange_normalized,
            universe_id=universe_id,
            start_date=start_date,
            end_date=end_date,
            query=query,
            coverage_status=coverage_status,
            min_avg_daily_turnover=min_avg_daily_turnover,
            min_coverage_pct=min_coverage_pct,
            limit=limit,
            offset=offset,
            sort=sort,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    return DataBulkFetchPreviewResponse(**payload)


@app.get("/api/data/instruments/search", response_model=list[DataInstrumentSearchRow])
def data_instruments_search(
    query: Annotated[str, Query(min_length=1, max_length=80)],
    provider: str = "upstox",
    exchange: str = "NSE",
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
) -> list[DataInstrumentSearchRow]:
    normalized_provider = provider.lower()
    normalized_exchange = _canonical_data_exchange(exchange)
    if normalized_provider not in {"upstox", "yfinance"}:
        raise HTTPException(
            status_code=400,
            detail="Only provider=upstox or provider=yfinance is supported.",
        )
    if normalized_provider == "upstox" and normalized_exchange != "NSE":
        raise HTTPException(
            status_code=400,
            detail="provider=upstox supports only exchange=NSE.",
        )
    if normalized_provider == "yfinance" and normalized_exchange not in {
        "NSE",
        "TSX",
        "US",
    }:
        raise HTTPException(
            status_code=400,
            detail="provider=yfinance search supports exchange=NSE, TSX, or US.",
        )
    try:
        store = _store()
        if normalized_provider == "yfinance":
            rows = store.search_persisted_symbols(
                query,
                provider=normalized_provider,
                exchange=normalized_exchange,
                limit=limit,
            )
        else:
            rows = store.search_provider_instruments(
                query_text=query,
                source=normalized_provider,
                exchange=normalized_exchange,
                limit=limit,
            )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [DataInstrumentSearchRow(**row) for row in rows]


@app.get("/api/data/universes", response_model=list[DataUniverseRow])
def data_universes(
    exchange: str = "NSE",
    source: str | None = None,
) -> list[DataUniverseRow]:
    if exchange.upper() != "NSE":
        raise HTTPException(status_code=400, detail="Only exchange=NSE is supported.")
    try:
        rows = _store().tradable_universes(exchange=exchange.upper(), source=source)
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [DataUniverseRow(**row) for row in rows]


@app.get(
    "/api/data/universes/{universe_id}/members",
    response_model=list[DataUniverseMemberRow],
)
def data_universe_members(
    universe_id: str,
    limit: Annotated[int, Query(ge=1, le=500)] = 500,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[DataUniverseMemberRow]:
    try:
        rows = _store().tradable_universe_members(
            universe_id=universe_id,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [DataUniverseMemberRow(**row) for row in rows]


@app.get(
    "/api/data/universe-reconciliation",
    response_model=UniverseReconciliationResponse,
)
def data_universe_reconciliation(
    exchange: str = "TSX",
) -> UniverseReconciliationResponse:
    canonical_exchange = _canonical_data_exchange(exchange)
    if canonical_exchange != "TSX":
        raise HTTPException(status_code=400, detail="Only the TSX reconciliation is available.")
    try:
        summary = _store().universe_reconciliation_summary(canonical_exchange)
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    snapshot = summary["snapshot"]
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No accepted TSX snapshot is available.")
    diagnostics = dict(snapshot.get("validation_json") or {}).get("source_diagnostics") or {}
    return UniverseReconciliationResponse(
        exchange=canonical_exchange,
        snapshot_id=str(snapshot["snapshot_id"]),
        fetched_at=snapshot["fetched_at"],
        symbol_count=int(snapshot["symbol_count"]),
        diagnostics=diagnostics,
        groups=summary["groups"],
    )


@app.get("/api/data/provider-history")
def data_provider_history(
    exchange: str = "TSX",
    provider: str = "yfinance",
) -> dict:
    canonical_exchange = _canonical_data_exchange(exchange)
    if canonical_exchange not in {"NSE", "TSX", "US"}:
        raise HTTPException(status_code=400, detail="exchange must be NSE, TSX, or US")
    if provider.lower() != "yfinance":
        raise HTTPException(status_code=400, detail="Only provider=yfinance is supported.")
    try:
        return _store().provider_daily_history_summary(
            canonical_exchange,
            provider=provider.lower(),
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc


@app.get(
    "/api/data/operations/overview",
    response_model=OperationsOverviewResponse,
)
def data_operations_overview(
    provider: str | None = None,
    exchange: str | None = None,
    recent_run_limit: Annotated[int, Query(ge=1, le=100)] = 20,
    lifecycle_limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> OperationsOverviewResponse:
    canonical_exchange = _operations_exchange(exchange)
    normalized_provider = provider.lower() if provider else None
    try:
        store = _store()
        lifecycle = store.symbol_lifecycle_events_page(
            exchange=canonical_exchange,
            limit=lifecycle_limit,
        )
        snapshots = store.latest_accepted_universe_snapshots(
            exchange=canonical_exchange,
        )
        runs = store.provider_runs(
            limit=recent_run_limit,
            source=normalized_provider,
            exchange=canonical_exchange,
        )
        return OperationsOverviewResponse(
            generated_at=datetime.now(UTC),
            provider=normalized_provider,
            exchange=canonical_exchange,
            queue=[
                OperationsQueueGroup(**row)
                for row in store.pipeline_work_queue_groups(
                    provider=normalized_provider,
                    exchange=canonical_exchange,
                )
            ],
            freshness=[
                OperationsFreshnessRow(**row)
                for row in store.provider_data_freshness(
                    provider=normalized_provider,
                    exchange=canonical_exchange,
                )
            ],
            adaptive_rates=[
                OperationsAdaptiveRateStateRow(**row)
                for row in store.adaptive_rate_states(provider=normalized_provider)
            ],
            latest_universes=[_to_operations_universe_snapshot(row) for row in snapshots],
            recent_runs=[_to_data_pipeline_run_summary(row) for row in runs],
            recent_lifecycle_events=[
                OperationsLifecycleEventRow(**row) for row in lifecycle["rows"]
            ],
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc


@app.get(
    "/api/data/operations/work-items",
    response_model=OperationsWorkItemsResponse,
)
def data_operations_work_items(
    provider: str | None = "yfinance",
    exchange: str | None = None,
    status: str | None = None,
    work_type: str | None = None,
    symbol: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> OperationsWorkItemsResponse:
    canonical_exchange = _operations_exchange(exchange)
    try:
        payload = _store().pipeline_work_items_page(
            provider=provider.lower() if provider else None,
            exchange=canonical_exchange,
            status=status,
            work_type=work_type,
            symbol=symbol,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return OperationsWorkItemsResponse(
        total=payload["total"],
        limit=limit,
        offset=offset,
        rows=[OperationsWorkItemRow(**row) for row in payload["rows"]],
    )


@app.get(
    "/api/data/operations/lifecycle-events",
    response_model=OperationsLifecycleEventsResponse,
)
def data_operations_lifecycle_events(
    exchange: str | None = None,
    event_type: str | None = None,
    symbol: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> OperationsLifecycleEventsResponse:
    canonical_exchange = _operations_exchange(exchange)
    try:
        payload = _store().symbol_lifecycle_events_page(
            exchange=canonical_exchange,
            event_type=event_type,
            symbol=symbol,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return OperationsLifecycleEventsResponse(
        total=payload["total"],
        limit=limit,
        offset=offset,
        rows=[OperationsLifecycleEventRow(**row) for row in payload["rows"]],
    )


@app.get(
    "/api/data/operations/rate-limits",
    response_model=list[OperationsAdaptiveRateStateRow],
)
def data_operations_rate_limits(
    provider: str | None = None,
) -> list[OperationsAdaptiveRateStateRow]:
    try:
        rows = _store().adaptive_rate_states(
            provider=provider.lower() if provider else None,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [OperationsAdaptiveRateStateRow(**row) for row in rows]


@app.get(
    "/api/data/operations/market-data-health",
    response_model=MarketDataHealthResponse,
)
def market_data_health(
    http_request: Request,
    provider: Annotated[str, Query(min_length=1, max_length=64)] = "yfinance",
    exchange: Annotated[str, Query(min_length=1, max_length=32)] = "NSE",
    issue_limit: Annotated[int, Query(ge=1, le=200)] = 50,
    lineage_limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> MarketDataHealthResponse:
    canonical_exchange = _canonical_data_exchange(exchange)
    if canonical_exchange != "NSE":
        raise HTTPException(
            status_code=400,
            detail="Phase 3 market-data health is available only for NSE",
        )
    canonical_provider = provider.strip().lower()
    if canonical_provider != "yfinance":
        raise HTTPException(
            status_code=400,
            detail="Phase 3 market-data health currently supports yfinance",
        )
    workspace_id = http_request.headers.get("X-Workspace-ID", "default").strip()
    if not workspace_id or len(workspace_id) > 64:
        raise HTTPException(status_code=400, detail="X-Workspace-ID is invalid")
    current_settings = get_settings()
    try:
        snapshot = _market_data_health_repository().snapshot(
            workspace_id=workspace_id,
            provider=canonical_provider,
            exchange=canonical_exchange,
            issue_limit=issue_limit,
            lineage_limit=lineage_limit,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return MarketDataHealthResponse(
        enabled=current_settings.phase3_market_data_enabled,
        clickhouse_enabled=current_settings.clickhouse_enabled,
        workspace_id=snapshot.workspace_id,
        provider=snapshot.provider,
        exchange=snapshot.exchange,
        health_status=snapshot.health_status,
        checked_at=datetime.now(UTC),
        quality=[MarketDataQualityHealthRow(**vars(row)) for row in snapshot.quality],
        issues=[MarketDataQualityIssueRow(**vars(row)) for row in snapshot.issues],
        raw_lineage=[MarketDataRawLineageRow(**vars(row)) for row in snapshot.raw_lineage],
        replication=[MarketDataReplicationHealthRow(**vars(row)) for row in snapshot.replication],
        availability=[
            MarketDataAvailabilityHealthRow(**vars(row)) for row in snapshot.availability
        ],
    )


@app.get(
    "/api/data/operations/nse-provider-cutover",
    response_model=NseProviderCutoverStatusResponse,
)
def nse_provider_cutover_status(
    http_request: Request,
    evidence_limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> NseProviderCutoverStatusResponse:
    workspace_id = _workspace_id(http_request)
    current_settings = get_settings()
    try:
        status = _nse_provider_cutover_repository(workspace_id).status(
            configured_primary=current_settings.nse_daily_primary_source,
            required_passing_windows=current_settings.nse_cutover_required_passing_windows,
            evidence_limit=evidence_limit,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return _nse_cutover_status_response(status)


@app.post(
    "/api/admin/nse-provider-cutover/approve",
    response_model=NseProviderCutoverStatusResponse,
)
def approve_nse_yfinance_cutover(
    body: NseCutoverApprovalRequest,
    http_request: Request,
    admin_email: Annotated[str, Depends(_require_admin)],
) -> NseProviderCutoverStatusResponse:
    workspace_id = _workspace_id(http_request)
    idempotency_key = _required_idempotency_key(http_request)
    current_settings = get_settings()
    repository = _nse_provider_cutover_repository(workspace_id)
    try:
        repository.approve(
            actor_email=admin_email,
            reason=body.reason,
            idempotency_key=idempotency_key,
            expected_evidence_bundle_sha256=body.expected_evidence_bundle_sha256,
            required_passing_windows=current_settings.nse_cutover_required_passing_windows,
        )
        status = repository.status(
            configured_primary=current_settings.nse_daily_primary_source,
            required_passing_windows=current_settings.nse_cutover_required_passing_windows,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return _nse_cutover_status_response(status)


@app.post(
    "/api/admin/nse-provider-cutover/rollback",
    response_model=NseProviderCutoverStatusResponse,
)
def rollback_nse_yfinance_cutover(
    body: NseCutoverRollbackRequest,
    http_request: Request,
    admin_email: Annotated[str, Depends(_require_admin)],
) -> NseProviderCutoverStatusResponse:
    workspace_id = _workspace_id(http_request)
    idempotency_key = _required_idempotency_key(http_request)
    current_settings = get_settings()
    repository = _nse_provider_cutover_repository(workspace_id)
    try:
        repository.rollback(
            actor_email=admin_email,
            reason=body.reason,
            idempotency_key=idempotency_key,
            expected_current_decision_sha256=body.expected_current_decision_sha256,
        )
        status = repository.status(
            configured_primary=current_settings.nse_daily_primary_source,
            required_passing_windows=current_settings.nse_cutover_required_passing_windows,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return _nse_cutover_status_response(status)


@app.get("/api/data/pipeline-health", response_model=DataPipelineHealthResponse)
def data_pipeline_health() -> DataPipelineHealthResponse:
    settings = get_settings()
    try:
        token_configured = provider_credential_status(
            _store(),
            provider="upstox",
            fallback_token=settings.upstox_access_token,
        ).configured
    except SQLAlchemyError:
        token_configured = bool(settings.upstox_access_token)
    return DataPipelineHealthResponse(
        provider="upstox",
        exchange="NSE",
        daily_ohlcv_enabled=True,
        upstox_access_token_configured=token_configured,
        max_concurrent_fetches=settings.data_pipeline_max_concurrent_fetches,
        checked_at=datetime.now(UTC),
    )


@app.get("/api/data/pipeline-runs", response_model=list[DataPipelineRunSummary])
def data_pipeline_runs(
    provider: str | None = "upstox",
    exchange: str | None = "NSE",
    status: str | None = None,
    limit: int = 20,
) -> list[DataPipelineRunSummary]:
    try:
        rows = _store().latest_runs(
            limit=min(max(limit, 1), 100),
            source=provider,
            exchange=_canonical_data_exchange(exchange) if exchange else None,
            status=status,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [_to_data_pipeline_run_summary(row) for row in rows]


@app.get("/api/data/pipeline-runs/{run_id}", response_model=DataPipelineRunDetail)
def data_pipeline_run_detail(
    run_id: str,
    exchange: str | None = None,
) -> DataPipelineRunDetail:
    selected_exchange = _operations_exchange(exchange)
    try:
        store = _store()
        row = store.ingestion_run(run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        run_metadata = row.get("run_metadata")
        claimed_work_item_ids = (
            run_metadata.get("claimed_work_item_ids", []) if isinstance(run_metadata, dict) else []
        )
        work_items = store.pipeline_work_items_for_run(
            run_id,
            claimed_work_item_ids=claimed_work_item_ids,
            exchange=selected_exchange,
        )
        provider_requests = store.provider_request_logs_for_run(
            run_id,
            provider=row["source"],
            limit=500,
        )
        if selected_exchange and work_items:
            provider_symbols = {str(item["provider_symbol"]).upper() for item in work_items}
            instrument_keys = {
                str(item.get("provider_instrument_key") or "").upper()
                for item in work_items
                if item.get("provider_instrument_key")
            }
            provider_requests = [
                request
                for request in provider_requests
                if str(request.get("symbol") or "").upper() in provider_symbols
                or str(request.get("instrument_key") or "").upper() in instrument_keys
                or any(
                    symbol in str(request.get("request_key") or "").upper()
                    for symbol in provider_symbols
                )
            ]
        coverage = store.daily_ohlcv_fetch_coverage_for_run(
            run_id,
            source=row["source"],
            exchange=selected_exchange or row["exchange"],
        )
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return DataPipelineRunDetail(
        run=_to_data_pipeline_run_summary(row),
        selected_exchange=selected_exchange,
        fetch_coverage=coverage,
        work_items=[DataPipelineRunWorkItemRow(**item) for item in work_items],
        provider_requests=[_to_provider_request_log_row(item) for item in provider_requests],
    )


@app.post(
    "/api/data/pipeline-requests",
    response_model=DataPipelineSubmission,
    status_code=202,
)
def create_data_pipeline_request(
    body: DataPipelineRequest,
    http_request: Request,
) -> DataPipelineSubmission:
    if set(body.steps) != {"fetch_ohlcv", "validate_ohlcv"}:
        raise HTTPException(
            status_code=400,
            detail="MVP data pipeline requests must include fetch_ohlcv and validate_ohlcv.",
        )
    idempotency_key = http_request.headers.get("X-Idempotency-Key") or str(uuid4())
    if len(idempotency_key) > 200:
        raise HTTPException(status_code=400, detail="X-Idempotency-Key is too long")
    requested_by = http_request.headers.get("X-Actor-ID", "authenticated-user")
    try:
        workflow, created = _workflow_store().submit(
            workflow_type="upstox_daily_ohlcv",
            request_payload=body.model_dump(mode="json"),
            idempotency_key=idempotency_key,
            requested_by=requested_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return DataPipelineSubmission(
        workflow_id=workflow.workflow_id,
        status=workflow.status,
        created=created,
        status_url=f"/api/data/workflow-requests/{workflow.workflow_id}",
    )


@app.get(
    "/api/data/workflow-requests/{workflow_id}",
    response_model=DataPipelineWorkflowStatus,
)
def data_pipeline_workflow_status(workflow_id: str) -> DataPipelineWorkflowStatus:
    try:
        workflow = _workflow_store().get(workflow_id)
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    if workflow is None:
        raise HTTPException(status_code=404, detail="Workflow request not found")
    return DataPipelineWorkflowStatus(**workflow.__dict__)


@app.get("/api/market/status")
def market_status() -> list[dict]:
    try:
        rows = _store().market_status()
        if rows:
            return [
                {
                    "exchange": row["exchange"],
                    "universeSize": row["universe_size"],
                    "lastOhlcvRun": _iso(row["last_ohlcv_run"]),
                    "lastScreenerRun": None,
                    "staleSymbols": max(row["universe_size"] - row["candle_symbols"], 0),
                    "dataQualityScore": (
                        round(100.0 * row["candle_symbols"] / row["universe_size"], 2)
                        if row["universe_size"]
                        else 0.0
                    ),
                    "latestCandle": _iso(row["latest_candle"]),
                    "lastOhlcvStatus": row["last_ohlcv_status"],
                }
                for row in rows
            ]
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Market status is unavailable") from exc
    return []


@app.get("/api/jobs/latest")
def latest_jobs() -> list[dict]:
    try:
        rows = _store().latest_runs()
        if rows:
            return [
                {
                    "id": row["run_id"],
                    "name": row["job_name"],
                    "status": row["status"],
                    "startedAt": _iso(row["started_at"]),
                    "durationSeconds": _duration_seconds(row["started_at"], row["finished_at"]),
                    "itemsProcessed": row["items_processed"],
                    "itemsSucceeded": row["items_succeeded"],
                    "itemsFailed": row["items_failed"],
                    "exchange": row["exchange"],
                    "source": row["source"],
                    "errorMessage": row["error_message"],
                }
                for row in rows
            ]
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Job history is unavailable") from exc
    return []


def _to_data_pipeline_run_summary(row: dict) -> DataPipelineRunSummary:
    work_item_exchanges = row.get("work_item_exchanges")
    if not isinstance(work_item_exchanges, (list, tuple, set)):
        work_item_exchanges = [] if str(row["exchange"]).upper() == "MULTI" else [row["exchange"]]
    run_metadata = row.get("run_metadata")
    normalized_metadata = run_metadata if isinstance(run_metadata, dict) else {}
    exchange_results = normalized_metadata.get("exchange_results", [])
    if not isinstance(exchange_results, list):
        exchange_results = []
    return DataPipelineRunSummary(
        id=str(row["run_id"]),
        name=str(row["job_name"]),
        status=str(row["status"]),
        exchange=str(row["exchange"]),
        work_item_exchanges=sorted(str(value) for value in work_item_exchanges),
        source=str(row["source"]),
        started_at=row["started_at"],
        finished_at=row.get("finished_at"),
        duration_seconds=_duration_seconds(row["started_at"], row.get("finished_at")),
        items_requested=int(row.get("items_requested") or 0),
        items_processed=int(row.get("items_processed") or 0),
        items_succeeded=int(row.get("items_succeeded") or 0),
        items_failed=int(row.get("items_failed") or 0),
        error_message=row.get("error_message"),
        run_metadata=normalized_metadata,
        exchange_results=exchange_results,
    )


def _to_provider_request_summary_row(row: dict) -> ProviderRequestSummaryRow:
    return ProviderRequestSummaryRow(
        provider=str(row["provider"]),
        endpoint_group=str(row["endpoint_group"]),
        status=str(row["status"]),
        requests=int(row.get("requests") or 0),
        rate_limited_requests=int(row.get("rate_limited_requests") or 0),
        wait_seconds=float(row.get("wait_seconds") or 0.0),
        avg_duration_ms=float(row.get("avg_duration_ms") or 0.0),
    )


def _to_provider_request_log_row(row: dict) -> ProviderRequestLogRow:
    return ProviderRequestLogRow(
        id=str(row["id"]),
        run_id=row.get("run_id"),
        provider=str(row["provider"]),
        endpoint_group=str(row["endpoint_group"]),
        request_key=str(row["request_key"]),
        instrument_key=row.get("instrument_key"),
        symbol=row.get("symbol"),
        interval=row.get("interval"),
        window_start=row.get("window_start"),
        window_end=row.get("window_end"),
        status_code=row.get("status_code"),
        status=str(row["status"]),
        error_message=row.get("error_message"),
        retry_count=int(row.get("retry_count") or 0),
        rate_limited=bool(row.get("rate_limited")),
        wait_seconds=float(row.get("wait_seconds") or 0.0),
        duration_ms=float(row.get("duration_ms") or 0.0),
        created_at=row["created_at"],
    )


def _to_operations_universe_snapshot(row: dict) -> OperationsUniverseSnapshotRow:
    validation_json = row.get("validation_json")
    validation = validation_json if isinstance(validation_json, dict) else {}
    return OperationsUniverseSnapshotRow(
        snapshot_id=str(row["snapshot_id"]),
        exchange=str(row["exchange"]),
        source=str(row["source"]),
        status=str(row["status"]),
        fetched_at=row["fetched_at"],
        symbol_count=int(row.get("symbol_count") or 0),
        validation=validation,
        error_message=row.get("error_message"),
    )


def _expected_daily_dates(
    store: TimescaleStore,
    exchange: str,
    start_date: date,
    end_date: date,
) -> list[date]:
    exchange = _canonical_data_exchange(exchange)
    if exchange not in {"NSE", "TSX", "US"}:
        current = start_date
        trading_dates = []
        while current <= end_date:
            if current.weekday() < 5:
                trading_dates.append(current)
            current += timedelta(days=1)
        return trading_dates

    resolution = resolve_expected_session_dates(
        store,
        exchange,
        start_date,
        end_date,
        use_materialized_sessions=settings.materialized_exchange_sessions_enabled,
    )
    return list(resolution.dates)


def _build_yfinance_bulk_fetch_preview(
    store: TimescaleStore,
    exchange: str,
    universe_id: str | None,
    start_date: date,
    end_date: date,
    query: str | None,
    coverage_status: str | None,
    min_avg_daily_turnover: float | None,
    min_coverage_pct: float | None,
    limit: int,
    offset: int,
    sort: str,
) -> dict:
    exchange = _canonical_data_exchange(exchange)
    seed_universe = universe_id or ("canada_seed" if exchange == "TSX" else "us_seed")
    symbols = yfinance_universe(seed_universe)
    if any(symbol.exchange.upper() != exchange for symbol in symbols):
        raise ValueError(f"universe_id={seed_universe} does not match exchange={exchange}.")
    if coverage_status and coverage_status.lower() not in {"complete", "partial", "empty"}:
        raise ValueError("coverage_status must be complete, partial, or empty.")

    expected_dates = _expected_daily_dates(store, exchange, start_date, end_date)
    seed_rows = [
        {
            "symbol": symbol.symbol.upper(),
            "name": symbol.name,
            "instrument_key": f"YF|{symbol.yahoo_symbol or symbol.symbol}",
        }
        for symbol in symbols
        if symbol.yahoo_symbol
    ]
    if query:
        needle = query.strip().upper()
        seed_rows = [
            row
            for row in seed_rows
            if needle in row["symbol"]
            or needle in str(row.get("name") or "").upper()
            or needle in row["instrument_key"].upper()
        ]

    stored_dates = store.daily_ohlcv_dates_by_instrument(
        [row["instrument_key"] for row in seed_rows],
        start_date,
        end_date,
        source="yfinance",
        exchange=exchange,
    )
    first_date_loader = getattr(
        store,
        "first_daily_ohlcv_dates_by_instrument",
        None,
    )
    first_trade_dates = (
        first_date_loader(
            [row["instrument_key"] for row in seed_rows],
            source="yfinance",
            exchange=exchange,
        )
        if first_date_loader is not None
        else {key: min(values) for key, values in stored_dates.items() if values}
    )
    avg_turnover_by_key = store.daily_ohlcv_average_turnover_by_instrument(
        [row["instrument_key"] for row in seed_rows],
        start_date,
        end_date,
        source="yfinance",
        exchange=exchange,
    )
    rows = []
    for row in seed_rows:
        key = row["instrument_key"]
        avg_turnover = avg_turnover_by_key.get(key)
        if min_avg_daily_turnover is not None and (
            avg_turnover is None or avg_turnover < min_avg_daily_turnover
        ):
            continue
        instrument_stored_dates = stored_dates.get(key, set())
        instrument_expected_dates = expected_dates_for_instrument(
            expected_dates,
            coverage_start=start_date,
            first_trade_date=first_trade_dates.get(key),
        )
        instrument_expected_set = set(instrument_expected_dates)
        expected_rows = len(instrument_expected_dates)
        present_dates = instrument_expected_set.intersection(instrument_stored_dates)
        missing_dates = sorted(instrument_expected_set.difference(present_dates))
        stored_count = len(present_dates)
        missing_count = len(missing_dates)
        status = _coverage_status(stored_count, expected_rows)
        if coverage_status and status != coverage_status.lower():
            continue
        coverage_pct = min(stored_count / expected_rows, 1.0) if expected_rows else 0.0
        if min_coverage_pct is not None and coverage_pct < min_coverage_pct:
            continue
        tasks = [
            {
                "symbol": row["symbol"],
                "trading_symbol": key.removeprefix("YF|"),
                "instrument_key": key,
                "fetch_start": window_start,
                "fetch_end": window_end,
                "missing_rows": len(dates),
                "status": "queued",
            }
            for window_start, window_end, dates in _contiguous_date_windows(missing_dates)
        ]
        rows.append(
            {
                "symbol": row["symbol"],
                "name": row.get("name"),
                "instrument_key": key,
                "provider": "yfinance",
                "exchange": exchange,
                "interval": "1d",
                "first_stored_date": min(present_dates) if present_dates else None,
                "latest_stored_date": max(present_dates) if present_dates else None,
                "stored_rows": stored_count,
                "expected_rows": expected_rows,
                "coverage_pct": coverage_pct,
                "missing_rows": missing_count,
                "coverage_status": status,
                "last_successful_run": None,
                "last_fetch_status": None,
                "avg_daily_turnover": avg_turnover,
                "tasks": tasks,
            }
        )

    rows = _sort_bulk_fetch_preview_rows(rows, sort)
    summary = _availability_summary(rows)
    paginated_rows = rows[offset : offset + limit]
    paginated_tasks = [task for row in paginated_rows for task in row["tasks"]]
    warnings = []
    if not expected_dates:
        warnings.append("No expected trading sessions in the requested date range.")
    if exchange not in {"NSE", "TSX", "US"}:
        warnings.append("No stored exchange holiday calendar found; preview uses weekdays only.")

    return {
        "provider": "yfinance",
        "exchange": exchange,
        "universe_id": seed_universe,
        "interval": "1d",
        "start_date": start_date,
        "end_date": end_date,
        "query": query,
        "coverage_status": coverage_status,
        "min_avg_daily_turnover": min_avg_daily_turnover,
        "min_coverage_pct": min_coverage_pct,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "total": len(rows),
        "rows": paginated_rows,
        "summary": summary,
        "tasks": paginated_tasks,
        "warnings": warnings,
    }


def _coverage_status(stored_rows: int, expected_rows: int) -> str:
    if stored_rows == 0:
        return "empty"
    if expected_rows > 0 and stored_rows >= expected_rows:
        return "complete"
    return "partial"


def _availability_summary(rows: list[dict]) -> dict:
    return {
        "symbols_total": len(rows),
        "symbols_complete": sum(1 for row in rows if row["coverage_status"] == "complete"),
        "symbols_partial": sum(1 for row in rows if row["coverage_status"] == "partial"),
        "symbols_empty": sum(1 for row in rows if row["coverage_status"] == "empty"),
        "expected_rows": sum(int(row["expected_rows"]) for row in rows),
        "stored_rows": sum(int(row["stored_rows"]) for row in rows),
        "missing_rows": sum(int(row["missing_rows"]) for row in rows),
        "estimated_provider_calls_for_missing": sum(
            len(row["tasks"]) for row in rows if row["missing_rows"] > 0
        ),
    }


def _sort_bulk_fetch_preview_rows(rows: list[dict], sort: str) -> list[dict]:
    sorters = {
        "symbol": lambda row: (row["symbol"],),
        "-symbol": lambda row: (row["symbol"],),
        "coverage_pct": lambda row: (row["coverage_pct"], row["symbol"]),
        "-coverage_pct": lambda row: (row["coverage_pct"], row["symbol"]),
        "missing_rows": lambda row: (row["missing_rows"], row["symbol"]),
        "-missing_rows": lambda row: (row["missing_rows"], row["symbol"]),
        "avg_daily_turnover": lambda row: (
            row["avg_daily_turnover"] is not None,
            row["avg_daily_turnover"],
            row["symbol"],
        ),
        "-avg_daily_turnover": lambda row: (
            row["avg_daily_turnover"] is not None,
            row["avg_daily_turnover"],
            row["symbol"],
        ),
        "latest_stored_date": lambda row: (
            row["latest_stored_date"] is not None,
            row["latest_stored_date"],
            row["symbol"],
        ),
        "-latest_stored_date": lambda row: (
            row["latest_stored_date"] is not None,
            row["latest_stored_date"],
            row["symbol"],
        ),
    }
    if sort not in sorters:
        raise ValueError("Unsupported sort value.")
    return sorted(rows, key=sorters[sort], reverse=sort.startswith("-"))


def _contiguous_date_windows(missing_dates: list[date]) -> list[tuple[date, date, list[date]]]:
    if not missing_dates:
        return []
    windows = []
    current = [missing_dates[0]]
    for candle_date in missing_dates[1:]:
        if candle_date == current[-1] + timedelta(days=1):
            current.append(candle_date)
            continue
        windows.append((current[0], current[-1], current))
        current = [candle_date]
    windows.append((current[0], current[-1], current))
    return windows


def _ensure_exchange_holidays(
    store: TimescaleStore,
    exchange: str,
    start_date: date,
    end_date: date,
) -> None:
    if settings.materialized_exchange_sessions_enabled:
        return
    exchange = _canonical_data_exchange(exchange)
    if exchange not in {"NSE", "TSX", "US"}:
        return

    for year in validated_exchange_calendar_years(start_date, end_date):
        cached = store.exchange_holidays(exchange, year)
        if cached is not None and (cached.get("closed_dates") or cached.get("early_close_dates")):
            continue
        try:
            holidays = fetch_exchange_holidays(exchange, year)
        except Exception as exc:
            logger.warning(
                "exchange holiday fetch failed exchange=%s year=%s error=%s",
                exchange,
                year,
                exc,
            )
            continue
        store.upsert_exchange_holidays(
            exchange=exchange,
            year=year,
            closed_dates=holidays.closed_dates,
            early_close_dates=holidays.early_close_dates,
            source_url=holidays.source_url,
        )


def _store() -> TimescaleStore:
    settings = get_settings()
    return TimescaleStore(settings.database_url)


def _market_data_health_repository() -> MarketDataHealthRepository:
    return MarketDataHealthRepository(_store().engine)


def _nse_provider_cutover_repository(
    workspace_id: str,
) -> NseProviderCutoverRepository:
    return NseProviderCutoverRepository(_store().engine, workspace_id=workspace_id)


def _workspace_id(request: Request) -> str:
    workspace_id = request.headers.get("X-Workspace-ID", "default").strip()
    if not workspace_id or len(workspace_id) > 64:
        raise HTTPException(status_code=400, detail="X-Workspace-ID is invalid")
    return workspace_id


def _required_idempotency_key(request: Request) -> str:
    key = request.headers.get("X-Idempotency-Key", "").strip()
    if len(key) < 8 or len(key) > 200:
        raise HTTPException(
            status_code=400,
            detail="X-Idempotency-Key must contain between 8 and 200 characters",
        )
    return key


def _nse_cutover_status_response(
    status: CutoverStatus,
) -> NseProviderCutoverStatusResponse:
    decision = status.active_decision
    return NseProviderCutoverStatusResponse(
        configured_primary=status.configured_primary,
        effective_primary=status.effective_primary,
        yfinance_approved=status.yfinance_approved,
        eligibility=NseCutoverEligibilityResponse(**vars(status.eligibility)),
        active_decision=(
            NseCutoverDecisionResponse(**vars(decision)) if decision is not None else None
        ),
        evidence=[NseProviderEvidenceRow(**row) for row in status.evidence],
    )


def _workflow_store() -> WorkflowRequestStore:
    return WorkflowRequestStore(get_settings().database_url)


def _initialized_store() -> TimescaleStore:
    store = _store()
    if hasattr(store, "initialize"):
        store.initialize()
    return store


def _stored_upstox_access_token(store: TimescaleStore, settings: Settings) -> str | None:
    return resolve_provider_token(
        store=store,
        provider="upstox",
        fallback_token=settings.upstox_access_token,
        app_secret_key=settings.app_secret_key,
    )


def _upstox_access_token(store: TimescaleStore, settings: Settings) -> str | None:
    return _stored_upstox_access_token(store, settings)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _duration_seconds(started_at: datetime | None, finished_at: datetime | None) -> int | None:
    if not started_at or not finished_at:
        return None
    return int((finished_at - started_at).total_seconds())
