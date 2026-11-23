from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel


class NseSessionProof(BaseModel):
    session_date: date
    status: str
    source_url: str | None
    source_sha256: str | None
    expected_rows: int | None
    postgres_rows: int | None
    clickhouse_rows: int | None
    source_digest: str | None
    details: dict[str, Any]
    updated_at: datetime


class NseRollingYearCoverage(BaseModel):
    start_date: date
    end_date: date
    calendar_dates: int
    expected_sessions: int
    complete_sessions: int
    calendar_complete: bool
    complete: bool


class NsePipelineHealth(BaseModel):
    provider: Literal["nse_bhavcopy"]
    expected_session: date | None
    latest_verified_session: date | None
    complete: bool
    sessions: list[NseSessionProof]
    rolling_year: NseRollingYearCoverage | None
    coverage_basis: str


class NseJobRun(BaseModel):
    id: str
    name: str
    status: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
