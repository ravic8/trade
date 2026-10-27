from datetime import date, datetime

from pydantic import BaseModel, Field


class Symbol(BaseModel):
    symbol: str
    exchange: str
    yahoo_symbol: str | None = None
    name: str | None = None
    currency: str | None = None
    source: str
    source_url: str | None = None
    source_identity: str | None = None
    listing_status: str = "active"
    listing_status_reason: str | None = None
    listing_status_effective_at: datetime | None = None
    pipeline_eligibility: str = "incremental"
    instrument_type: str = "unknown"
    reconciliation_status: str = "not_required"
    reconciliation_reason: str | None = None
    official_sector: str | None = None
    official_security_type: str | None = None
    official_source_updated_at: datetime | None = None


class MarketDataQualityReport(BaseModel):
    ticker: str
    rows: int
    start_date: date | None = None
    end_date: date | None = None
    missing_ohlcv_rows: int = 0
    zero_or_negative_close_rows: int = 0
    zero_volume_rows: int = 0
    warnings: list[str] = Field(default_factory=list)
