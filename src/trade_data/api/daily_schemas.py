"""Bounded daily candle read contract for independent operations clients."""

from datetime import date

from pydantic import BaseModel


class DailyCandle(BaseModel):
    date: date
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    quality_status: str


class DailyCandlesResponse(BaseModel):
    provider: str
    exchange: str
    instrument_key: str
    start_date: date
    end_date: date
    rows: list[DailyCandle]
