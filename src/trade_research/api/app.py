import logging
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from math import cos, sin
from time import monotonic
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import SQLAlchemyError

from trade_research.api.security import (
    ChatRateLimitMiddleware,
    cors_origins,
    require_admin_request,
)
from trade_research.chat import ChatLLMClient, ChatOrchestrator, ChatPolicy, ChatToolGateway
from trade_research.config import Settings, get_settings
from trade_research.credentials import (
    CredentialEncryptionError,
    encrypt_secret,
    provider_credential_status,
    resolve_provider_token,
)
from trade_research.data.coverage import CoveragePreviewInput, build_daily_coverage_preview
from trade_research.data.on_demand import run_daily_ohlcv_request
from trade_research.data.provider_capabilities import provider_capability
from trade_research.data.upstox import validate_upstox_access_token
from trade_research.exchange_sessions import (
    expected_dates_for_instrument,
    resolve_expected_session_dates,
)
from trade_research.market_calendar import (
    fetch_exchange_holidays,
    validated_exchange_calendar_years,
)
from trade_research.research.artifacts import ResearchArtifactReader
from trade_research.research.embeddings import OpenAIEmbeddingClient
from trade_research.research.ml_artifacts import MLArtifactReader
from trade_research.schemas import (
    ChatFeedbackRequest,
    ChatFeedbackResponse,
    ChatQueryRequest,
    ChatQueryResponse,
    ChatSourcesResponse,
    DataAvailabilityResponse,
    DataBulkFetchPreviewResponse,
    DataCoveragePreviewRequest,
    DataCoveragePreviewResponse,
    DataInstrumentSearchRow,
    DataPipelineHealthResponse,
    DataPipelineRequest,
    DataPipelineRunDetail,
    DataPipelineRunSummary,
    DataUniverseMemberRow,
    DataUniverseRow,
    PipelineScheduleStatusRow,
    ProviderCapabilityResponse,
    ProviderCredentialStatusResponse,
    ProviderCredentialTestRequest,
    ProviderCredentialTestResponse,
    ProviderCredentialTokenRequest,
    ProviderRequestLogRow,
    ProviderRequestSummaryRow,
    ScreenerResult,
    SourcesPayload,
)
from trade_research.storage.timescale import TimescaleStore
from trade_research.storage.vector import QdrantVectorStore
from trade_research.universe import yfinance_intraday_universe, yfinance_universe

app = FastAPI(title="Trade Research API", version="0.1.0")
logger = logging.getLogger(__name__)
settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins(settings.api_cors_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)
app.add_middleware(ChatRateLimitMiddleware, settings_getter=get_settings)


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
    exchange_normalized = exchange.upper()
    if provider_normalized not in {"upstox", "yfinance"}:
        raise HTTPException(
            status_code=400,
            detail="Only provider=upstox or provider=yfinance is supported.",
        )
    if provider_normalized == "upstox" and exchange_normalized != "NSE":
        raise HTTPException(status_code=400, detail="provider=upstox supports only exchange=NSE.")
    if provider_normalized == "yfinance" and exchange_normalized not in {"US", "CA", "GLOBAL"}:
        raise HTTPException(
            status_code=400,
            detail="provider=yfinance supports only exchange=US, exchange=CA, or exchange=GLOBAL.",
        )
    interval_normalized = "1d" if interval == "daily" else interval
    if interval_normalized not in {"1d", "5m"}:
        raise HTTPException(status_code=400, detail="Only interval=1d or interval=5m is supported.")
    if interval_normalized == "5m" and (
        provider_normalized != "yfinance" or exchange_normalized != "GLOBAL"
    ):
        raise HTTPException(
            status_code=400,
            detail="interval=5m is supported only for provider=yfinance exchange=GLOBAL.",
        )
    if interval_normalized == "1d" and exchange_normalized == "GLOBAL":
        raise HTTPException(
            status_code=400,
            detail="exchange=GLOBAL is supported only with interval=5m.",
        )
    if (start_date is None) != (end_date is None):
        raise HTTPException(
            status_code=400,
            detail="start_date and end_date must be supplied together.",
        )
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be on or before end_date.")

    try:
        store = _store()
        if interval_normalized == "5m":
            start_ts, end_ts = _intraday_bounds(start_date, end_date)
            expected_rows = _expected_intraday_rows(start_ts, end_ts, minutes=5)
            symbols = yfinance_intraday_universe(universe_id or "yfinance_fx_crypto_5m")
            payload = store.seeded_intraday_ohlcv_availability(
                symbols=[
                    {
                        "symbol": symbol.symbol,
                        "name": symbol.name,
                        "instrument_key": symbol.instrument_key,
                        "asset_class": symbol.asset_class,
                    }
                    for symbol in symbols
                ],
                source=provider_normalized,
                exchange=exchange_normalized,
                interval=interval_normalized,
                start_ts=start_ts,
                end_ts=end_ts,
                query_text=query,
                coverage_status=coverage_status,
                expected_rows_per_symbol=expected_rows,
                limit=limit,
                offset=offset,
                sort=sort,
            )
            return DataAvailabilityResponse(
                provider=provider_normalized,
                exchange=exchange_normalized,
                interval=interval_normalized,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                offset=offset,
                sort=sort,
                total=payload["total"],
                rows=payload["rows"],
                summary=payload["summary"],
            )

        if start_date and end_date:
            _ensure_exchange_holidays(store, exchange, start_date, end_date)
        expected_rows = _expected_daily_rows(store, exchange, start_date, end_date)
        if provider_normalized == "yfinance":
            seed_universe = universe_id or (
                "canada_seed" if exchange_normalized == "CA" else "us_seed"
            )
            symbols = yfinance_universe(seed_universe)
            invalid_symbols = [
                symbol.symbol
                for symbol in symbols
                if symbol.exchange.upper() != exchange_normalized
            ]
            if invalid_symbols:
                raise ValueError(
                    f"universe_id={seed_universe} does not match exchange={exchange_normalized}."
                )
            payload = store.seeded_daily_ohlcv_availability(
                symbols=[
                    {
                        "symbol": symbol.symbol,
                        "name": symbol.name,
                        "instrument_key": f"YF|{symbol.yahoo_symbol or symbol.symbol}",
                    }
                    for symbol in symbols
                ],
                source=provider_normalized,
                exchange=exchange_normalized,
                start_date=start_date,
                end_date=end_date,
                query_text=query,
                coverage_status=coverage_status,
                expected_rows_per_symbol=expected_rows,
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
            exchange=exchange,
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
            exchange=exchange,
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
            exchange=exchange,
            job_name=job,
            start_date=start_date,
            end_date=end_date,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [_to_provider_request_log_row(row) for row in rows]


@app.get("/api/data/schedules/status", response_model=list[PipelineScheduleStatusRow])
def data_schedule_status() -> list[PipelineScheduleStatusRow]:
    return [
        PipelineScheduleStatusRow(
            schedule_name="daily_research_schedule",
            job_name="daily_research_pipeline_job",
            cron_schedule="30 19 * * 1-5",
            execution_timezone="Asia/Kolkata",
            intended_status="stopped",
            notes="Read-only status; Dagster remains private.",
        ),
        PipelineScheduleStatusRow(
            schedule_name="north_america_daily_yfinance_schedule",
            job_name="north_america_daily_yfinance_job",
            cron_schedule="30 3 * * 2-6",
            execution_timezone="Asia/Kolkata",
            intended_status="stopped",
            notes="Read-only status; cadence is controlled in private Dagster.",
        ),
        PipelineScheduleStatusRow(
            schedule_name="fx_intraday_dukascopy_schedule",
            job_name="fx_intraday_dukascopy_job",
            cron_schedule="15 * * * 1-5",
            execution_timezone="UTC",
            intended_status="stopped",
            notes="Keep stopped because Dukascopy datafeed times out.",
        ),
        PipelineScheduleStatusRow(
            schedule_name="yfinance_fx_intraday_schedule",
            job_name="yfinance_fx_intraday_job",
            cron_schedule="20 * * * *",
            execution_timezone="UTC",
            intended_status="stopped",
            notes="Keep stopped until intraday cadence is explicitly chosen.",
        ),
    ]


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
    exchange_normalized = exchange.upper()
    if provider_normalized != "yfinance":
        raise HTTPException(
            status_code=400,
            detail="Only provider=yfinance is supported for bulk fetch preview.",
        )
    if exchange_normalized not in {"US", "CA"}:
        raise HTTPException(
            status_code=400,
            detail="provider=yfinance supports only exchange=US or exchange=CA.",
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
    if provider.lower() != "upstox":
        raise HTTPException(status_code=400, detail="Only provider=upstox is supported.")
    if exchange.upper() != "NSE":
        raise HTTPException(status_code=400, detail="Only exchange=NSE is supported.")
    try:
        rows = _store().search_provider_instruments(
            query_text=query,
            source=provider.lower(),
            exchange=exchange.upper(),
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
            exchange=exchange,
            status=status,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return [_to_data_pipeline_run_summary(row) for row in rows]


@app.get("/api/data/pipeline-runs/{run_id}", response_model=DataPipelineRunDetail)
def data_pipeline_run_detail(run_id: str) -> DataPipelineRunDetail:
    try:
        store = _store()
        row = store.ingestion_run(run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        coverage = store.daily_ohlcv_fetch_coverage_for_run(
            run_id,
            source=row["source"],
            exchange=row["exchange"],
        )
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return DataPipelineRunDetail(
        run=_to_data_pipeline_run_summary(row),
        fetch_coverage=coverage,
    )


@app.post("/api/data/pipeline-requests", response_model=DataPipelineRunDetail)
def create_data_pipeline_request(request: DataPipelineRequest) -> DataPipelineRunDetail:
    if set(request.steps) != {"fetch_ohlcv", "validate_ohlcv"}:
        raise HTTPException(
            status_code=400,
            detail="MVP data pipeline requests must include fetch_ohlcv and validate_ohlcv.",
        )
    settings = get_settings()
    try:
        store = _store()
        _ensure_exchange_holidays(store, request.exchange, request.start_date, request.end_date)
        access_token = _upstox_access_token(store, settings)
        result = run_daily_ohlcv_request(
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
            access_token=access_token,
            throttle_seconds=settings.data_pipeline_throttle_seconds,
            max_concurrent_fetches=settings.data_pipeline_max_concurrent_fetches,
        )
        row = store.ingestion_run(result.run_id)
        if row is None:
            raise HTTPException(status_code=500, detail="Pipeline run was not persisted")
        coverage = store.daily_ohlcv_fetch_coverage_for_run(
            result.run_id,
            source=row["source"],
            exchange=row["exchange"],
        )
    except HTTPException:
        raise
    except (CredentialEncryptionError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return DataPipelineRunDetail(
        run=_to_data_pipeline_run_summary(row),
        fetch_coverage=coverage,
    )


@app.post("/api/chat/query", response_model=ChatQueryResponse)
def chat_query(request: ChatQueryRequest) -> ChatQueryResponse:
    settings = get_settings()
    if not settings.chat_enabled:
        raise HTTPException(status_code=503, detail="Chat is disabled")
    try:
        return _chat_orchestrator().handle_query(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/chat/sources/{response_id}", response_model=ChatSourcesResponse)
def chat_sources(response_id: str) -> ChatSourcesResponse:
    settings = get_settings()
    if not settings.chat_enabled:
        raise HTTPException(status_code=503, detail="Chat is disabled")
    sources = _chat_orchestrator().get_sources(response_id)
    return ChatSourcesResponse(response_id=response_id, sources=SourcesPayload(**sources))


@app.get("/api/chat/audit/{response_id}")
def chat_audit(response_id: str) -> dict:
    settings = get_settings()
    if not settings.chat_enabled:
        raise HTTPException(status_code=503, detail="Chat is disabled")
    payload = _chat_orchestrator().get_audit(response_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Audit record not found")
    return payload


@app.post("/api/chat/feedback", response_model=ChatFeedbackResponse)
def chat_feedback(_request: ChatFeedbackRequest) -> ChatFeedbackResponse:
    settings = get_settings()
    if not settings.chat_enabled:
        raise HTTPException(status_code=503, detail="Chat is disabled")
    return ChatFeedbackResponse(status="accepted")


@app.get("/api/chat/health")
def chat_health() -> dict[str, object]:
    settings = get_settings()
    return {
        "enabled": settings.chat_enabled,
        "strictCitationRequired": settings.chat_strict_citation_required,
        "qdrantConfigured": bool(settings.qdrant_url),
        "embeddingConfigured": bool(settings.openai_api_key),
        "llmProvider": "gemini",
        "llmConfigured": bool(_llm_api_key_present(settings)),
        "llmAnswerEnabled": settings.chat_use_llm_answer,
        "checkedAt": datetime.now(UTC).isoformat(),
    }


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
                    "dataQualityScore": 100.0 if row["latest_candle"] else 0.0,
                    "latestCandle": _iso(row["latest_candle"]),
                    "lastOhlcvStatus": row["last_ohlcv_status"],
                }
                for row in rows
            ]
    except SQLAlchemyError:
        pass

    now = datetime.now(UTC)
    return [
        {
            "exchange": "NSE",
            "universeSize": 2365,
            "lastOhlcvRun": (now - timedelta(minutes=35)).isoformat(),
            "lastScreenerRun": (now - timedelta(minutes=8)).isoformat(),
            "staleSymbols": 18,
            "dataQualityScore": 98.2,
        },
        {
            "exchange": "TSX",
            "universeSize": 687,
            "lastOhlcvRun": (now - timedelta(minutes=52)).isoformat(),
            "lastScreenerRun": (now - timedelta(minutes=16)).isoformat(),
            "staleSymbols": 11,
            "dataQualityScore": 96.8,
        },
    ]


@app.get("/api/screeners/intraday-range/latest")
def latest_intraday_range() -> list[dict]:
    now = datetime.now(UTC)
    rows = [
        {
            "ticker": "RELIANCE.NS",
            "exchange": "NSE",
            "company": "Reliance Industries",
            "signal": "Intraday range expansion",
            "liquidity": 2_145_000_000,
            "d5Up0100": 4,
            "d5Dn0100": 3,
            "d5ClUp0200": 1,
            "d5VUp0200": 2,
            "matchedAt": now.isoformat(),
        },
        {
            "ticker": "INFY.NS",
            "exchange": "NSE",
            "company": "Infosys",
            "signal": "Range expansion without close shock",
            "liquidity": 1_128_000_000,
            "d5Up0100": 3,
            "d5Dn0100": 4,
            "d5ClUp0200": 0,
            "d5VUp0200": 2,
            "matchedAt": now.isoformat(),
        },
        {
            "ticker": "WDO.TO",
            "exchange": "TSX",
            "company": "Wesdome Gold Mines",
            "signal": "Two-way intraday expansion",
            "liquidity": 43_800_000,
            "d5Up0100": 3,
            "d5Dn0100": 3,
            "d5ClUp0200": 2,
            "d5VUp0200": 1,
            "matchedAt": now.isoformat(),
        },
    ]
    return [
        ScreenerResult(**_to_screener_result(row)).model_dump(mode="json") | row
        for row in rows
    ]


@app.get("/api/symbols/{ticker}/candles")
def symbol_candles(ticker: str) -> list[dict]:
    try:
        rows = _store().candles(ticker)
        if rows:
            return [
                {
                    "time": _iso(row["ts"]),
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "ticker": row["ticker"],
                }
                for row in rows
            ]
    except SQLAlchemyError:
        pass

    start = datetime.now(UTC).date() - timedelta(days=90)
    candles = []
    for index in range(70):
        base = 100 + sin(index / 5) * 4 + index * 0.08
        open_price = round(base + sin(index) * 0.8, 2)
        close = round(base + cos(index / 2) * 0.9, 2)
        high = round(max(open_price, close) + 1.2 + (index % 4) * 0.18, 2)
        low = round(min(open_price, close) - 1.1 - (index % 3) * 0.15, 2)
        candles.append(
            {
                "time": (start + timedelta(days=index)).isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 850_000 + index * 12_000 + (index % 5) * 70_000,
                "ticker": ticker,
            }
        )
    return candles


@app.get("/api/research/notes")
def research_notes(ticker: str | None = None) -> list[dict]:
    now = datetime.now(UTC)
    notes = [
        {
            "id": "note-1",
            "ticker": "RELIANCE.NS",
            "title": "Energy and telecom subsidiaries remain primary catalyst cluster",
            "sourceType": "agent",
            "publishedAt": (now - timedelta(minutes=12)).isoformat(),
            "summary": (
                "The signal is price-behavior driven. Current retrieved context points "
                "to sector flows rather than a single confirmed company-specific event."
            ),
            "confidence": 0.74,
        },
        {
            "id": "note-2",
            "ticker": "INFY.NS",
            "title": "IT sector range widening with muted close-to-open follow-through",
            "sourceType": "news",
            "publishedAt": (now - timedelta(minutes=44)).isoformat(),
            "summary": (
                "Recent documents mention broad IT rotation. The screener pattern suggests "
                "active intraday participation without large overnight shock."
            ),
            "confidence": 0.69,
        },
        {
            "id": "note-3",
            "ticker": "WDO.TO",
            "title": "Gold miners screen as volatility candidates",
            "sourceType": "exchange",
            "publishedAt": (now - timedelta(hours=1)).isoformat(),
            "summary": (
                "Commodity-linked names are clustering in the range screen. Confirm "
                "source-level news before attributing the move."
            ),
            "confidence": 0.66,
        },
    ]
    if ticker:
        return [note for note in notes if note["ticker"].upper() == ticker.upper()]
    return notes


@app.get("/api/research/progress")
def research_progress() -> dict:
    return ResearchArtifactReader(settings.data_dir).progress()


@app.get("/api/research/factors/summary")
def research_factor_summary() -> dict:
    return ResearchArtifactReader(settings.data_dir).factor_summary()


@app.get("/api/research/factors/ic")
def research_factor_ic(
    target: str | None = None,
    sort: str = "mean_rank_ic",
    direction: str = "desc",
    limit: int = 100,
) -> dict:
    normalized_direction = direction.lower()
    if normalized_direction not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="direction must be asc or desc")
    return ResearchArtifactReader(settings.data_dir).factor_ic(
        target=target,
        sort=sort,
        direction=normalized_direction,
        limit=min(max(limit, 0), 500),
    )


@app.get("/api/research/ml/summary")
def research_ml_summary() -> dict:
    return MLArtifactReader(settings.data_dir).summary()


@app.get("/api/research/ml/model-metrics")
def research_ml_model_metrics(run: str = "all") -> dict:
    if run not in {"all", "baselines", "lightgbm"}:
        raise HTTPException(status_code=400, detail="run must be all, baselines, or lightgbm")
    return MLArtifactReader(settings.data_dir).model_metrics(run=run)


@app.get("/api/research/ml/backtests")
def research_ml_backtests(group: str = "all") -> dict:
    if group not in {"all", "baselines", "lightgbm"}:
        raise HTTPException(status_code=400, detail="group must be all, baselines, or lightgbm")
    return MLArtifactReader(settings.data_dir).backtests(group=group)


@app.get("/api/research/ml/candidates")
def research_ml_candidates(
    model_id: str = "momentum_1d",
    top_n: int = 5,
    run: str = "baselines",
    limit: int = 200,
) -> dict:
    if run not in {"baselines", "lightgbm"}:
        raise HTTPException(status_code=400, detail="run must be baselines or lightgbm")
    return MLArtifactReader(settings.data_dir).candidates(
        model_id=model_id,
        top_n=min(max(top_n, 1), 100),
        run=run,
        limit=min(max(limit, 0), 1000),
    )


@app.get("/api/research/ml/latest-candidates")
def research_ml_latest_candidates(
    run: str = "baselines",
    top_n: int = 5,
) -> dict:
    if run not in {"baselines", "lightgbm"}:
        raise HTTPException(status_code=400, detail="run must be baselines or lightgbm")
    return MLArtifactReader(settings.data_dir).latest_candidates(
        run=run,
        top_n=min(max(top_n, 1), 50),
    )


@app.get("/api/research/ml/equity-curve")
def research_ml_equity_curve(
    group: str = "baselines",
    model_id: str = "momentum_1d",
    top_n: int = 5,
) -> dict:
    if group not in {"baselines", "lightgbm"}:
        raise HTTPException(status_code=400, detail="group must be baselines or lightgbm")
    return MLArtifactReader(settings.data_dir).equity_curve(
        group=group,
        model_id=model_id,
        top_n=min(max(top_n, 1), 100),
    )


@app.get("/api/research/ml/robustness")
def research_ml_robustness(
    group: str = "baselines",
    model_id: str = "momentum_1d",
    top_n: int = 5,
) -> dict:
    if group not in {"baselines", "lightgbm"}:
        raise HTTPException(status_code=400, detail="group must be baselines or lightgbm")
    return MLArtifactReader(settings.data_dir).robustness(
        group=group,
        model_id=model_id,
        top_n=min(max(top_n, 1), 100),
    )


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
    except SQLAlchemyError:
        pass

    now = datetime.now(UTC)
    return [
        {
            "id": "job-101",
            "name": "NSE OHLCV ingestion",
            "status": "completed",
            "startedAt": (now - timedelta(minutes=50)).isoformat(),
            "durationSeconds": 615,
            "itemsProcessed": 2347,
        },
        {
            "id": "job-102",
            "name": "Intraday range screener",
            "status": "completed",
            "startedAt": (now - timedelta(minutes=18)).isoformat(),
            "durationSeconds": 18,
            "itemsProcessed": 1654,
        },
        {
            "id": "job-103",
            "name": "Research document embedding",
            "status": "running",
            "startedAt": (now - timedelta(minutes=4)).isoformat(),
            "durationSeconds": None,
            "itemsProcessed": 128,
        },
    ]


def _to_data_pipeline_run_summary(row: dict) -> DataPipelineRunSummary:
    return DataPipelineRunSummary(
        id=str(row["run_id"]),
        name=str(row["job_name"]),
        status=str(row["status"]),
        exchange=str(row["exchange"]),
        source=str(row["source"]),
        started_at=row["started_at"],
        finished_at=row.get("finished_at"),
        duration_seconds=_duration_seconds(row["started_at"], row.get("finished_at")),
        items_requested=int(row.get("items_requested") or 0),
        items_processed=int(row.get("items_processed") or 0),
        items_succeeded=int(row.get("items_succeeded") or 0),
        items_failed=int(row.get("items_failed") or 0),
        error_message=row.get("error_message"),
        run_metadata=row.get("run_metadata") if isinstance(row.get("run_metadata"), dict) else {},
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


def _expected_daily_rows(
    store: TimescaleStore,
    exchange: str,
    start_date: date | None,
    end_date: date | None,
) -> int:
    if start_date is None or end_date is None:
        return 0

    return len(_expected_daily_dates(store, exchange, start_date, end_date))


def _intraday_bounds(
    start_date: date | None,
    end_date: date | None,
) -> tuple[datetime | None, datetime | None]:
    if start_date is None or end_date is None:
        return None, None
    start_ts = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=UTC)
    exclusive_end = datetime.combine(end_date + timedelta(days=1), datetime.min.time()).replace(
        tzinfo=UTC
    )
    return start_ts, exclusive_end - timedelta(minutes=5)


def _expected_intraday_rows(
    start_ts: datetime | None,
    end_ts: datetime | None,
    minutes: int,
) -> int:
    if start_ts is None or end_ts is None or start_ts > end_ts:
        return 0
    return int((end_ts - start_ts).total_seconds() // (minutes * 60)) + 1


def _expected_daily_dates(
    store: TimescaleStore,
    exchange: str,
    start_date: date,
    end_date: date,
) -> list[date]:
    if exchange.upper() not in {"NSE", "TSX", "US", "CA"}:
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
    seed_universe = universe_id or ("canada_seed" if exchange == "CA" else "us_seed")
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
        else {
            key: min(values)
            for key, values in stored_dates.items()
            if values
        }
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
    if exchange not in {"NSE", "TSX", "US", "CA"}:
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
    if exchange.upper() not in {"NSE", "TSX", "US", "CA"}:
        return

    for year in validated_exchange_calendar_years(start_date, end_date):
        cached = store.exchange_holidays(exchange, year)
        if cached is not None and (
            cached.get("closed_dates")
            or cached.get("early_close_dates")
        ):
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
def _to_screener_result(row: dict) -> dict:
    return {
        "ticker": row["ticker"],
        "strategy": "intraday_range_v1",
        "matched_at": datetime.fromisoformat(row["matchedAt"]),
        "metrics": {
            "exchange": row["exchange"],
            "company": row["company"],
            "signal": row["signal"],
            "liquidity": row["liquidity"],
            "d5Up0100": row["d5Up0100"],
            "d5Dn0100": row["d5Dn0100"],
            "d5ClUp0200": row["d5ClUp0200"],
            "d5VUp0200": row["d5VUp0200"],
        },
    }


def _store() -> TimescaleStore:
    settings = get_settings()
    return TimescaleStore(settings.database_url)


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


@lru_cache(maxsize=1)
def _chat_orchestrator() -> ChatOrchestrator:
    settings = get_settings()
    embedding_client = (
        OpenAIEmbeddingClient(
            api_key=settings.openai_api_key,
            model=settings.openai_embedding_model,
            base_url=settings.openai_base_url,
        )
        if settings.openai_api_key
        else None
    )
    gateway = ChatToolGateway(
        settings=settings,
        timescale_store=TimescaleStore(settings.database_url),
        vector_store=QdrantVectorStore(
            url=settings.qdrant_url,
            api_key=settings.qdrant_api_key,
            collection=settings.qdrant_collection,
        ),
        embedding_client=embedding_client,
    )
    llm_client = None
    if settings.chat_use_llm_answer and _llm_api_key_present(settings):
        llm_client = ChatLLMClient(settings=settings)
    return ChatOrchestrator(
        settings=settings,
        tools=gateway,
        llm_client=llm_client,
        policy=ChatPolicy(),
    )


def _llm_api_key_present(settings: Settings) -> bool:
    return bool(settings.gemini_api_key)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _duration_seconds(started_at: datetime | None, finished_at: datetime | None) -> int | None:
    if not started_at or not finished_at:
        return None
    return int((finished_at - started_at).total_seconds())
