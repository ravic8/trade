"""Small NSE daily backend: official-source reads and operational coverage."""

import sqlite3
from datetime import UTC, date, datetime
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from trade_data.api.access import AccessRequests
from trade_data.api.daily_schemas import DailyCandle, DailyCandlesResponse
from trade_data.api.operations_schemas import NseJobRun, NsePipelineHealth
from trade_data.api.security import (
    authenticated_email,
    cors_origins,
    require_admin_request,
    role_for_email,
)
from trade_data.config import get_settings
from trade_data.dagster.status import read_nse_job_runs
from trade_data.data.nse_bhavcopy import SOURCE, rolling_year_start
from trade_data.storage.nse import nse_session_freshness, nse_sessions
from trade_data.storage.timescale import TimescaleStore

app = FastAPI(title="Trade — NSE daily data")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins(get_settings().api_cors_origins),
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def _store():
    return TimescaleStore(get_settings().database_url)


def _access_repo() -> AccessRequests:
    return AccessRequests(_store().engine)


class AccessRequestIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    email: str = Field(min_length=3, max_length=320)
    use_case: str = Field(default="", max_length=1000)


@app.get("/api/me")
def me(request: Request):
    settings = get_settings()
    email = authenticated_email(request, settings.admin_email_headers)
    if email is None:
        raise HTTPException(401, "Not authenticated")
    return {"email": email.lower(), "role": role_for_email(email, settings)}


@app.post("/api/access-requests", status_code=201)
def submit_access_request(body: AccessRequestIn):
    if "@" not in body.email or "." not in body.email.split("@")[-1]:
        raise HTTPException(422, "Enter a valid email")
    try:
        row = _access_repo().submit(body.name, body.email, body.use_case)
    except SQLAlchemyError:
        raise HTTPException(503, "Access request storage unavailable") from None
    print(
        f"ACCESS_REQUEST pending id={row['id']} email={row['email']} notify=ravi@chain8.org",
        flush=True,
    )
    return {"id": row["id"], "email": row["email"], "status": row["status"]}


@app.get("/api/admin/access-requests")
def list_access_requests(request: Request):
    require_admin_request(request, get_settings())
    try:
        return _access_repo().list_all()
    except SQLAlchemyError:
        raise HTTPException(503, "Access request storage unavailable") from None


@app.post("/api/admin/access-requests/{request_id}/approve")
def approve_access_request(request_id: int, request: Request):
    admin = require_admin_request(request, get_settings())
    return _decide(request_id, "approved", admin)


@app.post("/api/admin/access-requests/{request_id}/reject")
def reject_access_request(request_id: int, request: Request):
    admin = require_admin_request(request, get_settings())
    return _decide(request_id, "rejected", admin)


def _decide(request_id: int, status: str, admin: str):
    try:
        row = _access_repo().decide(request_id, status, admin)
    except SQLAlchemyError:
        raise HTTPException(503, "Access request storage unavailable") from None
    if row is None:
        raise HTTPException(404, "Access request not found")
    return row


@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.now(UTC).isoformat()}


@app.get("/api/data/candles", response_model=DailyCandlesResponse)
def daily_candles(
    instrument_key: Annotated[str, Query(min_length=1, max_length=200)],
    start_date: date,
    end_date: date,
    provider: Literal["nse_bhavcopy"] = SOURCE,
    exchange: Literal["NSE"] = "NSE",
):
    if (
        not instrument_key.strip()
        or start_date.year < 2
        or not 0 <= (end_date - start_date).days <= 3660
    ):
        raise HTTPException(400, "Choose an ordered range of at most ten years")
    try:
        frame = _store().daily_ohlcv_frame(
            exchange="NSE",
            source=SOURCE,
            start_date=start_date,
            end_date=end_date,
            instrument_keys=[instrument_key.strip()],
        )
    except SQLAlchemyError:
        raise HTTPException(503, "Daily candle storage unavailable") from None
    return DailyCandlesResponse(
        provider=SOURCE,
        exchange="NSE",
        instrument_key=instrument_key,
        start_date=start_date,
        end_date=end_date,
        rows=[DailyCandle(**r) for r in frame.to_dict("records")],
    )


@app.get("/api/data/instruments/search")
def instruments(query: str = "", limit: Annotated[int, Query(ge=1, le=500)] = 50):
    try:
        with _store().engine.connect() as c:
            return [
                dict(r)
                for r in c.execute(
                    text("""SELECT DISTINCT s.symbol,
                s.provider_instrument_key AS instrument_key, s.canonical_instrument_id,
                s.source_identity AS isin FROM symbols s JOIN ohlcv_daily o
                ON o.instrument_key=s.provider_instrument_key AND o.exchange=s.exchange
                WHERE o.source=:source AND s.exchange='NSE'
                AND s.symbol ILIKE :query ORDER BY s.symbol LIMIT :limit"""),
                    {"source": SOURCE, "query": "%" + query + "%", "limit": limit},
                ).mappings()
            ]
    except SQLAlchemyError:
        raise HTTPException(503, "Instrument storage unavailable") from None


@app.get("/api/data/pipeline-health", response_model=NsePipelineHealth)
def pipeline_health(include_sessions: bool = True):
    settings = get_settings()
    store = _store()
    try:
        expected, latest_verified = nse_session_freshness(
            store, provider=SOURCE, grace_minutes=settings.nse_publication_grace_minutes
        )
        expected_date = expected["session_date"] if expected else None
        rows = []
        if include_sessions:
            with store.engine.connect() as c:
                rows = [
                    dict(r)
                    for r in c.execute(
                        select(nse_sessions).order_by(nse_sessions.c.session_date.desc()).limit(10)
                    ).mappings()
                ]
        year = None
        if expected:
            end = expected["session_date"]
            with store.engine.connect() as c:
                year = dict(
                    c.execute(
                        text("""SELECT COUNT(*) AS calendar_dates,
                    COUNT(*) FILTER (WHERE e.is_trading_day OR
                        (n.status='complete' AND n.details->>'source_provider'=:provider))
                        AS expected_sessions,
                    COUNT(*) FILTER (WHERE n.status='complete'
                        AND n.details->>'source_provider'=:provider) AS complete_sessions
                    FROM exchange_sessions e LEFT JOIN nse_bhavcopy_sessions n
                    ON n.session_date=e.session_date
                    WHERE e.exchange='NSE'
                    AND e.validation_status LIKE 'valid%'
                    AND e.session_date BETWEEN :start AND :end"""),
                        {"start": rolling_year_start(end), "end": end, "provider": SOURCE},
                    )
                    .mappings()
                    .one()
                )
            year.update(
                start_date=rolling_year_start(end),
                end_date=end,
                calendar_complete=year["calendar_dates"]
                == (end - rolling_year_start(end)).days + 1,
                complete=(
                    year["calendar_dates"] == (end - rolling_year_start(end)).days + 1
                    and year["expected_sessions"] == year["complete_sessions"]
                ),
            )
        return {
            "provider": SOURCE,
            "expected_session": expected_date,
            "latest_verified_session": latest_verified,
            "complete": expected_date is not None and latest_verified == expected_date,
            "sessions": rows,
            "rolling_year": year,
            "coverage_basis": "eligible ordinary equity records in official final bhavcopy",
        }
    except SQLAlchemyError:
        raise HTTPException(503, "Coverage status unknown: database unavailable") from None


@app.get("/api/jobs/latest", response_model=list[NseJobRun])
def latest_jobs(limit: Annotated[int, Query(ge=1, le=50)] = 20):
    try:
        return read_nse_job_runs(get_settings().dagster_readonly_home, limit)
    except (OSError, sqlite3.Error):
        raise HTTPException(503, "Dagster run history unavailable") from None


@app.get("/api/market/status")
def market_status():
    try:
        with _store().engine.connect() as c:
            return [
                dict(r)
                for r in c.execute(
                    text("""SELECT exchange,
                MAX(date) AS latest_session, COUNT(*) AS rows FROM ohlcv_daily
                WHERE source=:source GROUP BY exchange"""),
                    {"source": SOURCE},
                ).mappings()
            ]
    except SQLAlchemyError:
        raise HTTPException(503, "Market status unknown: database unavailable") from None
