"""Storage interface consumed by the current daily-ingestion workflows.

This contract preserves PostgreSQL/SQLAlchemy semantics. It does not advertise
compatibility with arbitrary databases or hide the optional Phase 3 engine seam.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

import pandas as pd
from sqlalchemy.engine import Engine

from trade_data.data.adaptive_rate import AdaptiveRateStore
from trade_data.data.daily_work import DailyWorkItem


class DailyIngestionStore(AdaptiveRateStore, Protocol):
    @property
    def engine(self) -> Engine:
        """SQLAlchemy engine used by the existing Phase 3 repositories."""
        ...

    def initialize(self) -> None: ...

    def insert_daily_ohlcv_fetch_coverage(
        self, run_id: str, coverage: pd.DataFrame, source: str = "upstox", exchange: str = "NSE"
    ) -> int: ...

    def insert_provider_request_logs(self, logs: Iterable[Mapping[str, Any]]) -> int: ...

    def upsert_exchange_holidays(
        self,
        exchange: str,
        year: int,
        closed_dates: Iterable[date],
        early_close_dates: Iterable[date],
        source_url: str,
        fetched_at: datetime | None = None,
    ) -> int: ...

    def exchange_holidays(
        self, exchange: str, year: int, max_age_days: int | None = None
    ) -> dict[str, Any] | None: ...

    def exchange_sessions(
        self, exchange: str, start_date: date, end_date: date
    ) -> list[dict[str, Any]]: ...

    def latest_provider_eligible_exchange_session(
        self, exchange: str, *, at: datetime | None = None, provider_grace_minutes: int = 0
    ) -> dict[str, Any] | None: ...

    def active_yfinance_daily_instruments(self, exchange: str) -> list[dict[str, Any]]: ...

    def enqueue_pipeline_work_items(
        self, work_items: Iterable[Mapping[str, Any] | DailyWorkItem]
    ) -> int: ...

    def upsert_provider_daily_history_evidence(
        self, evidence_rows: Iterable[Mapping[str, Any]]
    ) -> int: ...

    def provider_daily_history_evidence(
        self,
        instrument_keys: Iterable[str],
        *,
        provider: str = "yfinance",
        interval: str = "1d",
        active_only: bool = True,
        chunk_size: int = 500,
    ) -> dict[str, list[dict[str, Any]]]: ...

    def cancel_pipeline_work_items_before_listing(
        self, *, exchange: str, provider: str = "yfinance", at: datetime | None = None
    ) -> int: ...

    def cancel_pending_pipeline_work_for_instruments(
        self,
        canonical_instrument_ids: Iterable[str],
        *,
        provider: str = "yfinance",
        reason: str,
        message: str,
        at: datetime | None = None,
    ) -> int: ...

    def cancel_pipeline_work_items_covered_by_provider_history(
        self, *, exchange: str | None = None, provider: str = "yfinance", at: datetime | None = None
    ) -> int: ...

    def claim_pipeline_work_items(
        self,
        *,
        worker_id: str,
        limit: int,
        at: datetime | None = None,
        provider: str = "yfinance",
        exchange: str | None = None,
        work_type: str | None = None,
        provider_symbols: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]: ...

    def heartbeat_pipeline_work_items(
        self, *, worker_id: str, work_item_ids: Iterable[str], at: datetime | None = None
    ) -> int: ...

    def transition_pipeline_work_item(
        self,
        *,
        work_item_id: str,
        worker_id: str,
        status: str,
        status_code: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        run_id: str | None = None,
        retry_delay: timedelta | None = None,
        at: datetime | None = None,
    ) -> dict[str, Any] | None: ...

    def update_symbol_provider_status(
        self,
        canonical_instrument_id: str,
        *,
        status: str,
        reason: str | None = None,
        at: datetime | None = None,
    ) -> int: ...

    def recover_stale_pipeline_work_items(
        self,
        *,
        stale_before: datetime,
        at: datetime | None = None,
        provider: str = "yfinance",
        exchange: str | None = None,
        work_type: str | None = None,
        provider_symbols: Iterable[str] | None = None,
    ) -> int: ...

    def pipeline_work_queue_summary(self, *, provider: str = "yfinance") -> dict[str, int]: ...

    def upsert_daily_ohlcv(
        self, frame: pd.DataFrame, exchange: str = "NSE", source: str = "upstox"
    ) -> int: ...

    def upsert_daily_price_adjustments(
        self, frame: pd.DataFrame, exchange: str = "NSE", source: str = "upstox"
    ) -> int: ...

    def latest_daily_ohlcv_dates(
        self,
        instrument_keys: list[str],
        source: str = "upstox",
        *,
        valid_only: bool = False,
        chunk_size: int = 250,
    ) -> dict[str, date]: ...

    def daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "upstox",
        exchange: str = "NSE",
        *,
        valid_only: bool = False,
    ) -> dict[str, set[date]]: ...

    def first_daily_ohlcv_dates_by_instrument(
        self, instrument_keys: list[str], source: str = "upstox", exchange: str = "NSE"
    ) -> dict[str, date]: ...

    def daily_ohlcv_average_turnover_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> dict[str, float]: ...

    def daily_ohlcv_frame(
        self,
        exchange: str = "NSE",
        source: str = "upstox",
        limit: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        instrument_keys: Sequence[str] | None = None,
    ) -> pd.DataFrame: ...

    def insert_data_quality_audits(
        self, audit: pd.DataFrame, dataset_name: str, source: str, interval: str
    ) -> int: ...

    def start_ingestion_run(
        self,
        job_name: str,
        exchange: str,
        source: str,
        items_requested: int,
        run_metadata: Mapping[str, Any] | None = None,
    ) -> UUID: ...

    def finish_ingestion_run(
        self,
        run_id: UUID,
        status: str,
        items_processed: int,
        items_succeeded: int,
        items_failed: int,
        error_message: str | None = None,
        run_metadata_patch: Mapping[str, Any] | None = None,
    ) -> None: ...
