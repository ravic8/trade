"""Small NSE daily backend: official-source reads and operational coverage."""

from datetime import UTC, date, datetime
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from trade_data.api.daily_schemas import DailyCandle, DailyCandlesResponse
from trade_data.api.security import cors_origins
from trade_data.config import get_settings
from trade_data.data.nse_bhavcopy import SOURCE, rolling_year_start
from trade_data.storage.nse import nse_sessions
from trade_data.storage.timescale import TimescaleStore

app = FastAPI(title="Trade — NSE daily data")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins(get_settings().api_cors_origins),
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _store():
    return TimescaleStore(get_settings().database_url)


@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.now(UTC).isoformat()}


@app.get("/api/data/candles", response_model=DailyCandlesResponse)
def daily_candles(
    instrument_key: Annotated[str, Query(min_length=1, max_length=200)],
    start_date: date,
    end_date: date,
    provider: Literal["nse_official_bhavcopy"] = SOURCE,
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


@app.get("/api/data/pipeline-health")
def pipeline_health():
    settings = get_settings()
    store = _store()
    try:
        expected = store.latest_provider_eligible_exchange_session(
            "NSE", provider_grace_minutes=settings.nse_publication_grace_minutes
        )
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
                        text("""SELECT COUNT(*) AS expected_sessions,
                    COUNT(*) FILTER (WHERE n.status='complete') AS complete_sessions
                    FROM exchange_sessions e LEFT JOIN nse_bhavcopy_sessions n
                    ON n.session_date=e.session_date
                    WHERE e.exchange='NSE' AND e.is_trading_day
                    AND e.validation_status LIKE 'valid%'
                    AND e.session_date BETWEEN :start AND :end"""),
                        {"start": rolling_year_start(end), "end": end},
                    )
                    .mappings()
                    .one()
                )
            year.update(
                start_date=rolling_year_start(end),
                end_date=end,
                complete=year["expected_sessions"] == year["complete_sessions"],
            )
        latest = rows[0] if rows else None
        complete = bool(
            expected
            and latest
            and latest["session_date"] == expected["session_date"]
            and latest["status"] == "complete"
        )
        return {
            "provider": SOURCE,
            "expected_session": expected["session_date"] if expected else None,
            "complete": complete,
            "sessions": rows,
            "rolling_year": year,
            "coverage_basis": "eligible ordinary equity records in official final bhavcopy",
        }
    except SQLAlchemyError:
        raise HTTPException(503, "Coverage status unknown: database unavailable") from None


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
