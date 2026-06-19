from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pandas as pd
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert

from trade_research.schemas import Symbol

metadata = MetaData()

symbols_table = Table(
    "symbols",
    metadata,
    Column("symbol", String, primary_key=True),
    Column("exchange", String, primary_key=True),
    Column("yahoo_symbol", String),
    Column("name", String),
    Column("currency", String),
    Column("source", String, nullable=False),
    Column("source_url", String),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
)

ohlcv_hourly_table = Table(
    "ohlcv_hourly",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("ts", DateTime(timezone=True), primary_key=True),
    Column("source", String, primary_key=True),
    Column("exchange", String, nullable=False),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("quality_status", String, nullable=False),
)

ingestion_runs_table = Table(
    "ingestion_runs",
    metadata,
    Column("run_id", String, primary_key=True),
    Column("job_name", String, nullable=False),
    Column("status", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("source", String, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("items_requested", BigInteger, nullable=False, default=0),
    Column("items_processed", BigInteger, nullable=False, default=0),
    Column("items_succeeded", BigInteger, nullable=False, default=0),
    Column("items_failed", BigInteger, nullable=False, default=0),
    Column("error_message", String),
    Column("run_metadata", JSON, nullable=False, default=dict),
)

exchange_holidays_table = Table(
    "exchange_holidays",
    metadata,
    Column("exchange", String, primary_key=True),
    Column("year", BigInteger, primary_key=True),
    Column("source_url", String, nullable=False),
    Column("closed_dates", JSON, nullable=False, default=list),
    Column("early_close_dates", JSON, nullable=False, default=list),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
)

feed_health_table = Table(
    "feed_health",
    metadata,
    Column("symbol", String, primary_key=True),
    Column("exchange", String, primary_key=True),
    Column("source", String, primary_key=True),
    Column("yahoo_symbol", String),
    Column("status", String, nullable=False),
    Column("last_success_at", DateTime(timezone=True)),
    Column("last_failure_at", DateTime(timezone=True)),
    Column("consecutive_failures", BigInteger, nullable=False, default=0),
    Column("success_count", BigInteger, nullable=False, default=0),
    Column("failure_count", BigInteger, nullable=False, default=0),
    Column("last_error_code", String),
    Column("last_error_message", String),
    Column("latest_candle_ts", DateTime(timezone=True)),
    Column("next_retry_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

hourly_backlog_windows_table = Table(
    "hourly_backlog_windows",
    metadata,
    Column("exchange", String, primary_key=True),
    Column("window_start", DateTime(timezone=True), primary_key=True),
    Column("source", String, primary_key=True),
    Column("window_end", DateTime(timezone=True), nullable=False),
    Column("status", String, nullable=False),
    Column("expected_symbol_count", BigInteger, nullable=False),
    Column("observed_symbol_count", BigInteger, nullable=False),
    Column("coverage_ratio", Float, nullable=False),
    Column("attempt_count", BigInteger, nullable=False, default=0),
    Column("first_detected_at", DateTime(timezone=True), nullable=False),
    Column("last_checked_at", DateTime(timezone=True), nullable=False),
    Column("last_recovery_at", DateTime(timezone=True)),
    Column("recovery_run_id", String),
    Column("last_error", String),
)

provider_instruments_table = Table(
    "provider_instruments",
    metadata,
    Column("source", String, primary_key=True),
    Column("instrument_key", String, primary_key=True),
    Column("exchange", String),
    Column("segment", String),
    Column("asset_type", String),
    Column("trading_symbol", String),
    Column("name", String),
    Column("isin", String),
    Column("lot_size", BigInteger),
    Column("tick_size", Float),
    Column("expiry", Date),
    Column("strike", Float),
    Column("option_type", String),
    Column("underlying_symbol", String),
    Column("underlying_key", String),
    Column("exchange_token", String),
    Column("active", Boolean, nullable=False, default=True),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("raw", JSON, nullable=False, default=dict),
)

tradable_universes_table = Table(
    "tradable_universes",
    metadata,
    Column("universe_id", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("description", String),
    Column("exchange", String, nullable=False),
    Column("source", String, nullable=False),
    Column("criteria_json", JSON, nullable=False, default=dict),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

tradable_universe_members_table = Table(
    "tradable_universe_members",
    metadata,
    Column("universe_id", String, primary_key=True),
    Column("symbol", String, primary_key=True),
    Column("instrument_key", String),
    Column("rank", BigInteger),
    Column("avg_daily_volume", Float),
    Column("avg_daily_turnover", Float),
    Column("trading_days", BigInteger),
    Column("zero_volume_ratio", Float),
    Column("start_date", Date),
    Column("end_date", Date),
    Column("included_at", DateTime(timezone=True), nullable=False),
)

ohlcv_daily_table = Table(
    "ohlcv_daily",
    metadata,
    Column("instrument_key", String, primary_key=True),
    Column("source", String, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("open_interest", BigInteger),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("quality_status", String, nullable=False),
)

data_quality_audits_table = Table(
    "data_quality_audits",
    metadata,
    Column("audit_id", String, primary_key=True),
    Column("dataset_name", String, nullable=False),
    Column("source", String, nullable=False),
    Column("symbol", String),
    Column("instrument_key", String),
    Column("interval", String),
    Column("start_date", Date),
    Column("end_date", Date),
    Column("rows", BigInteger, nullable=False),
    Column("missing_dates", BigInteger, nullable=False, default=0),
    Column("null_rows", BigInteger, nullable=False, default=0),
    Column("duplicate_rows", BigInteger, nullable=False, default=0),
    Column("zero_volume_rows", BigInteger, nullable=False, default=0),
    Column("zero_or_negative_close_rows", BigInteger, nullable=False, default=0),
    Column("coverage_ratio", Float),
    Column("status", String, nullable=False),
    Column("warnings_json", JSON, nullable=False, default=list),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


class TimescaleStore:
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url, pool_pre_ping=True)

    def initialize(self) -> None:
        metadata.create_all(self.engine)
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ohlcv_hourly_exchange_ts "
                    "ON ohlcv_hourly (exchange, ts DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ingestion_runs_started_at "
                    "ON ingestion_runs (started_at DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_feed_health_exchange_status "
                    "ON feed_health (exchange, status, next_retry_at)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_hourly_backlog_exchange_status "
                    "ON hourly_backlog_windows (exchange, status, window_start)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_provider_instruments_symbol "
                    "ON provider_instruments (source, exchange, segment, trading_symbol)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol_date "
                    "ON ohlcv_daily (symbol, date DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_data_quality_dataset "
                    "ON data_quality_audits (dataset_name, source, created_at DESC)"
                )
            )

        with self.engine.begin() as connection:
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
            connection.execute(
                text(
                    "SELECT create_hypertable("
                    "'ohlcv_hourly', 'ts', if_not_exists => TRUE, migrate_data => TRUE)"
                )
            )
            connection.execute(
                text(
                    "SELECT create_hypertable("
                    "'ohlcv_daily', 'date', if_not_exists => TRUE, migrate_data => TRUE)"
                )
            )

    def upsert_symbols(self, symbols: Iterable[Symbol], fetched_at: datetime | None = None) -> int:
        fetched = fetched_at or datetime.now(UTC)
        rows = [
            {
                **symbol.model_dump(),
                "is_active": True,
                "fetched_at": fetched,
            }
            for symbol in symbols
        ]
        if not rows:
            return 0

        statement = insert(symbols_table).values(rows)
        update_columns = {
            column.name: getattr(statement.excluded, column.name)
            for column in symbols_table.columns
            if column.name not in {"symbol", "exchange"}
        }
        with self.engine.begin() as connection:
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=["symbol", "exchange"],
                    set_=update_columns,
                )
            )
        return len(rows)

    def active_symbols(self, exchange: str, max_age_days: int | None = None) -> list[Symbol]:
        exchange_code = exchange.upper()
        with self.engine.begin() as connection:
            if max_age_days is not None:
                max_fetched_at = connection.execute(
                    text(
                        """
                        SELECT max(fetched_at)
                        FROM symbols
                        WHERE exchange = :exchange AND is_active = true
                        """
                    ),
                    {"exchange": exchange_code},
                ).scalar_one_or_none()
                if max_fetched_at is None or _as_utc(max_fetched_at) < datetime.now(
                    UTC
                ) - timedelta(days=max_age_days):
                    return []

            rows = connection.execute(
                symbols_table.select()
                .where(symbols_table.c.exchange == exchange_code)
                .where(symbols_table.c.is_active.is_(True))
                .order_by(symbols_table.c.symbol)
            ).mappings()
            return [
                Symbol(
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    yahoo_symbol=row["yahoo_symbol"],
                    name=row["name"],
                    currency=row["currency"],
                    source=row["source"],
                    source_url=row["source_url"],
                )
                for row in rows
            ]

    def upsert_exchange_holidays(
        self,
        exchange: str,
        year: int,
        closed_dates: Iterable[date],
        early_close_dates: Iterable[date],
        source_url: str,
        fetched_at: datetime | None = None,
    ) -> int:
        row = {
            "exchange": exchange.upper(),
            "year": year,
            "source_url": source_url,
            "closed_dates": sorted(item.isoformat() for item in closed_dates),
            "early_close_dates": sorted(item.isoformat() for item in early_close_dates),
            "fetched_at": fetched_at or datetime.now(UTC),
        }
        statement = insert(exchange_holidays_table).values(row)
        update_columns = {
            column.name: getattr(statement.excluded, column.name)
            for column in exchange_holidays_table.columns
            if column.name not in {"exchange", "year"}
        }
        with self.engine.begin() as connection:
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=["exchange", "year"],
                    set_=update_columns,
                )
            )
        return 1

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict[str, Any] | None:
        query = (
            exchange_holidays_table.select()
            .where(exchange_holidays_table.c.exchange == exchange.upper())
            .where(exchange_holidays_table.c.year == year)
        )
        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
        if row is None:
            return None
        if max_age_days is not None and _as_utc(row["fetched_at"]) < datetime.now(
            UTC
        ) - timedelta(days=max_age_days):
            return None
        return dict(row)

    def fetchable_symbols(
        self,
        symbols: list[Symbol],
        source: str = "yahoo",
        limit: int | None = None,
    ) -> list[Symbol]:
        candidates = [symbol for symbol in symbols if symbol.yahoo_symbol]
        if not candidates:
            return []

        exchange_code = candidates[0].exchange.upper()
        symbol_ids = [symbol.symbol for symbol in candidates]
        health_by_symbol = self._feed_health_by_symbol(
            exchange=exchange_code,
            source=source,
            symbols=symbol_ids,
        )
        now = datetime.now(UTC)
        fetchable: list[Symbol] = []
        for symbol in candidates:
            health = health_by_symbol.get(symbol.symbol)
            if health and not _feed_health_can_fetch(health, now):
                continue
            fetchable.append(symbol)
            if limit and len(fetchable) >= limit:
                break
        return fetchable

    def fetchable_symbol_count(self, exchange: str, source: str = "yahoo") -> int:
        return len(self.fetchable_symbols(self.active_symbols(exchange), source=source))

    def update_feed_health(
        self,
        symbols: list[Symbol],
        successful_latest_candles: Mapping[str, datetime],
        source: str = "yahoo",
        failure_threshold: int = 5,
        max_backoff_hours: int = 24,
        unsupported_retry_days: int = 7,
        failure_message: str | None = None,
    ) -> dict[str, int]:
        tracked_symbols = [symbol for symbol in symbols if symbol.yahoo_symbol]
        if not tracked_symbols:
            return {"updated": 0, "succeeded": 0, "failed": 0}

        exchange_code = tracked_symbols[0].exchange.upper()
        existing = self._feed_health_by_symbol(
            exchange=exchange_code,
            source=source,
            symbols=[symbol.symbol for symbol in tracked_symbols],
        )
        now = datetime.now(UTC)
        success_tickers = {ticker.upper(): ts for ticker, ts in successful_latest_candles.items()}
        rows: list[dict[str, Any]] = []
        succeeded = 0
        failed = 0
        for symbol in tracked_symbols:
            current = existing.get(symbol.symbol, {})
            yahoo_symbol = str(symbol.yahoo_symbol).upper()
            if yahoo_symbol in success_tickers:
                succeeded += 1
                rows.append(
                    {
                        "symbol": symbol.symbol,
                        "exchange": exchange_code,
                        "source": source,
                        "yahoo_symbol": yahoo_symbol,
                        "status": "active",
                        "last_success_at": now,
                        "last_failure_at": current.get("last_failure_at"),
                        "consecutive_failures": 0,
                        "success_count": int(current.get("success_count") or 0) + 1,
                        "failure_count": int(current.get("failure_count") or 0),
                        "last_error_code": None,
                        "last_error_message": None,
                        "latest_candle_ts": success_tickers[yahoo_symbol],
                        "next_retry_at": None,
                        "updated_at": now,
                    }
                )
                continue

            failed += 1
            consecutive_failures = int(current.get("consecutive_failures") or 0) + 1
            if consecutive_failures >= failure_threshold:
                status = "unsupported"
                next_retry_at = now + timedelta(days=unsupported_retry_days)
            else:
                status = "temporarily_failed"
                backoff_hours = min(2 ** (consecutive_failures - 1), max_backoff_hours)
                next_retry_at = now + timedelta(hours=backoff_hours)
            rows.append(
                {
                    "symbol": symbol.symbol,
                    "exchange": exchange_code,
                    "source": source,
                    "yahoo_symbol": yahoo_symbol,
                    "status": status,
                    "last_success_at": current.get("last_success_at"),
                    "last_failure_at": now,
                    "consecutive_failures": consecutive_failures,
                    "success_count": int(current.get("success_count") or 0),
                    "failure_count": int(current.get("failure_count") or 0) + 1,
                    "last_error_code": "no_hourly_data",
                    "last_error_message": failure_message or "Yahoo returned no hourly candles",
                    "latest_candle_ts": current.get("latest_candle_ts"),
                    "next_retry_at": next_retry_at,
                    "updated_at": now,
                }
            )

        if rows:
            statement = insert(feed_health_table).values(rows)
            update_columns = {
                column.name: getattr(statement.excluded, column.name)
                for column in feed_health_table.columns
                if column.name not in {"symbol", "exchange", "source"}
            }
            with self.engine.begin() as connection:
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["symbol", "exchange", "source"],
                        set_=update_columns,
                    )
                )
        return {"updated": len(rows), "succeeded": succeeded, "failed": failed}

    def feed_health_summary(self, exchange: str | None = None) -> list[dict[str, Any]]:
        where_clause = "WHERE exchange = :exchange" if exchange else ""
        query = text(
            f"""
            SELECT exchange, source, status, count(*) AS symbols
            FROM feed_health
            {where_clause}
            GROUP BY exchange, source, status
            ORDER BY exchange, source, status
            """
        )
        params = {"exchange": exchange.upper()} if exchange else {}
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query, params).mappings()]

    def _feed_health_by_symbol(
        self,
        exchange: str,
        source: str,
        symbols: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}
        query = (
            feed_health_table.select()
            .where(feed_health_table.c.exchange == exchange.upper())
            .where(feed_health_table.c.source == source)
            .where(feed_health_table.c.symbol.in_(symbols))
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query).mappings()
            return {row["symbol"]: dict(row) for row in rows}

    def scan_hourly_backlog_windows(
        self,
        exchange: str,
        windows: Iterable[tuple[datetime, datetime]],
        expected_symbol_count: int,
        coverage_threshold: float,
        source: str = "yahoo",
    ) -> list[dict[str, Any]]:
        pairs = [(_as_utc(start), _as_utc(end)) for start, end in windows]
        if not pairs or expected_symbol_count <= 0:
            return []

        counts = self._hourly_symbol_counts(
            exchange=exchange,
            window_starts=[start for start, _ in pairs],
            source=source,
        )
        pending: list[dict[str, Any]] = []
        for window_start, window_end in pairs:
            observed = counts.get(window_start, 0)
            coverage = observed / expected_symbol_count
            row = self.upsert_hourly_backlog_window(
                exchange=exchange,
                source=source,
                window_start=window_start,
                window_end=window_end,
                expected_symbol_count=expected_symbol_count,
                observed_symbol_count=observed,
                coverage_ratio=coverage,
                is_complete=coverage >= coverage_threshold,
            )
            if row["status"] in {"missing", "partial", "failed"}:
                pending.append(row)
        return pending

    def hourly_backlog_candidates(
        self,
        exchange: str,
        limit: int,
        max_attempts: int,
        stale_recovery_minutes: int,
        source: str = "yahoo",
    ) -> list[dict[str, Any]]:
        query = (
            hourly_backlog_windows_table.select()
            .where(hourly_backlog_windows_table.c.exchange == exchange.upper())
            .where(hourly_backlog_windows_table.c.source == source)
            .where(
                hourly_backlog_windows_table.c.status.in_(
                    ["missing", "partial", "failed", "queued", "running"]
                )
            )
            .where(hourly_backlog_windows_table.c.attempt_count < max_attempts)
            .order_by(hourly_backlog_windows_table.c.window_start)
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]

        stale_before = datetime.now(UTC) - timedelta(minutes=stale_recovery_minutes)
        candidates: list[dict[str, Any]] = []
        for row in rows:
            if row["status"] in {"queued", "running"}:
                last_recovery_at = row["last_recovery_at"]
                if last_recovery_at and _as_utc(last_recovery_at) >= stale_before:
                    continue
            candidates.append(row)
            if len(candidates) >= limit:
                break
        return candidates

    def upsert_hourly_backlog_window(
        self,
        exchange: str,
        source: str,
        window_start: datetime,
        window_end: datetime,
        expected_symbol_count: int,
        observed_symbol_count: int,
        coverage_ratio: float,
        is_complete: bool,
    ) -> dict[str, Any]:
        exchange_code = exchange.upper()
        now = datetime.now(UTC)
        key_filter = (
            (hourly_backlog_windows_table.c.exchange == exchange_code)
            & (hourly_backlog_windows_table.c.window_start == _as_utc(window_start))
            & (hourly_backlog_windows_table.c.source == source)
        )
        with self.engine.begin() as connection:
            existing = connection.execute(
                hourly_backlog_windows_table.select().where(key_filter)
            ).mappings().first()
            existing_status = existing["status"] if existing else None
            if is_complete:
                status = "recovered"
            elif existing_status in {"queued", "running"}:
                status = existing_status
            elif existing_status in {"partial", "failed"}:
                status = existing_status
            else:
                status = "missing"

            values = {
                "exchange": exchange_code,
                "window_start": _as_utc(window_start),
                "source": source,
                "window_end": _as_utc(window_end),
                "status": status,
                "expected_symbol_count": expected_symbol_count,
                "observed_symbol_count": observed_symbol_count,
                "coverage_ratio": coverage_ratio,
                "attempt_count": int(existing["attempt_count"]) if existing else 0,
                "first_detected_at": existing["first_detected_at"] if existing else now,
                "last_checked_at": now,
                "last_recovery_at": existing["last_recovery_at"] if existing else None,
                "recovery_run_id": existing["recovery_run_id"] if existing else None,
                "last_error": existing["last_error"] if existing else None,
            }
            statement = insert(hourly_backlog_windows_table).values(values)
            update_columns = {
                column.name: getattr(statement.excluded, column.name)
                for column in hourly_backlog_windows_table.columns
                if column.name not in {"exchange", "window_start", "source", "first_detected_at"}
            }
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=["exchange", "window_start", "source"],
                    set_=update_columns,
                )
            )
            row = connection.execute(
                hourly_backlog_windows_table.select().where(key_filter)
            ).mappings().one()
        return dict(row)

    def mark_hourly_backlog_queued(
        self,
        exchange: str,
        window_start: datetime,
        source: str = "yahoo",
    ) -> None:
        self._update_hourly_backlog_status(
            exchange=exchange,
            window_start=window_start,
            source=source,
            status="queued",
            increment_attempt=True,
        )

    def mark_hourly_backlog_running(
        self,
        exchange: str,
        window_start: datetime,
        recovery_run_id: str,
        source: str = "yahoo",
    ) -> None:
        self._update_hourly_backlog_status(
            exchange=exchange,
            window_start=window_start,
            source=source,
            status="running",
            recovery_run_id=recovery_run_id,
        )

    def finish_hourly_backlog_recovery(
        self,
        exchange: str,
        window_start: datetime,
        observed_symbol_count: int,
        expected_symbol_count: int,
        coverage_threshold: float,
        error_message: str | None = None,
        source: str = "yahoo",
    ) -> dict[str, Any] | None:
        coverage = observed_symbol_count / expected_symbol_count if expected_symbol_count else 0.0
        if error_message:
            status = "failed"
        elif coverage >= coverage_threshold:
            status = "recovered"
        else:
            status = "partial"
        values = {
            "status": status,
            "expected_symbol_count": expected_symbol_count,
            "observed_symbol_count": observed_symbol_count,
            "coverage_ratio": coverage,
            "last_checked_at": datetime.now(UTC),
            "last_recovery_at": datetime.now(UTC),
            "last_error": error_message,
        }
        query = (
            hourly_backlog_windows_table.update()
            .where(hourly_backlog_windows_table.c.exchange == exchange.upper())
            .where(hourly_backlog_windows_table.c.window_start == _as_utc(window_start))
            .where(hourly_backlog_windows_table.c.source == source)
            .values(**values)
            .returning(hourly_backlog_windows_table)
        )
        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
        return dict(row) if row else None

    def hourly_window_symbol_count(
        self,
        exchange: str,
        window_start: datetime,
        source: str = "yahoo",
    ) -> int:
        return self._hourly_symbol_counts(exchange, [_as_utc(window_start)], source).get(
            _as_utc(window_start),
            0,
        )

    def _hourly_symbol_counts(
        self,
        exchange: str,
        window_starts: list[datetime],
        source: str,
    ) -> dict[datetime, int]:
        if not window_starts:
            return {}
        query = (
            select(ohlcv_hourly_table.c.ts, func.count(func.distinct(ohlcv_hourly_table.c.ticker)))
            .where(ohlcv_hourly_table.c.exchange == exchange.upper())
            .where(ohlcv_hourly_table.c.source == source)
            .where(ohlcv_hourly_table.c.ts.in_(window_starts))
            .group_by(ohlcv_hourly_table.c.ts)
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query).all()
        return {_as_utc(ts): int(count) for ts, count in rows}

    def _update_hourly_backlog_status(
        self,
        exchange: str,
        window_start: datetime,
        source: str,
        status: str,
        increment_attempt: bool = False,
        recovery_run_id: str | None = None,
    ) -> None:
        values: dict[str, Any] = {
            "status": status,
            "last_recovery_at": datetime.now(UTC),
        }
        if recovery_run_id:
            values["recovery_run_id"] = recovery_run_id
        if increment_attempt:
            values["attempt_count"] = hourly_backlog_windows_table.c.attempt_count + 1
        query = (
            hourly_backlog_windows_table.update()
            .where(hourly_backlog_windows_table.c.exchange == exchange.upper())
            .where(hourly_backlog_windows_table.c.window_start == _as_utc(window_start))
            .where(hourly_backlog_windows_table.c.source == source)
            .values(**values)
        )
        with self.engine.begin() as connection:
            connection.execute(query)

    def upsert_hourly_ohlcv(self, frame: pd.DataFrame, exchange: str, source: str = "yahoo") -> int:
        rows = self._ohlcv_rows(frame, exchange=exchange, source=source)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(ohlcv_hourly_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in ohlcv_hourly_table.columns
                    if column.name not in {"ticker", "ts", "source"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["ticker", "ts", "source"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def upsert_provider_instruments(self, frame: pd.DataFrame, source: str = "upstox") -> int:
        rows = self._instrument_rows(frame, source=source)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(provider_instruments_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in provider_instruments_table.columns
                    if column.name not in {"source", "instrument_key"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["source", "instrument_key"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def upsert_tradable_universe(
        self,
        universe_id: str,
        name: str,
        exchange: str,
        source: str,
        criteria: Mapping[str, Any],
        members: pd.DataFrame,
        description: str | None = None,
    ) -> int:
        created_at = datetime.now(UTC)
        member_rows = self._universe_member_rows(members, universe_id, created_at)
        with self.engine.begin() as connection:
            universe_statement = insert(tradable_universes_table).values(
                universe_id=universe_id,
                name=name,
                description=description,
                exchange=exchange,
                source=source,
                criteria_json=dict(criteria),
                created_at=created_at,
            )
            connection.execute(
                universe_statement.on_conflict_do_update(
                    index_elements=["universe_id"],
                    set_={
                        "name": universe_statement.excluded.name,
                        "description": universe_statement.excluded.description,
                        "exchange": universe_statement.excluded.exchange,
                        "source": universe_statement.excluded.source,
                        "criteria_json": universe_statement.excluded.criteria_json,
                        "created_at": universe_statement.excluded.created_at,
                    },
                )
            )
            connection.execute(
                tradable_universe_members_table.delete().where(
                    tradable_universe_members_table.c.universe_id == universe_id
                )
            )
            if member_rows:
                for chunk in _chunks(member_rows, size=1_000):
                    connection.execute(tradable_universe_members_table.insert().values(chunk))
        return len(member_rows)

    def upsert_daily_ohlcv(
        self,
        frame: pd.DataFrame,
        exchange: str = "NSE",
        source: str = "upstox",
    ) -> int:
        rows = self._daily_ohlcv_rows(frame, exchange=exchange, source=source)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(ohlcv_daily_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in ohlcv_daily_table.columns
                    if column.name not in {"instrument_key", "source", "date"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["instrument_key", "source", "date"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def latest_daily_ohlcv_dates(
        self,
        instrument_keys: list[str],
        source: str = "upstox",
    ) -> dict[str, date]:
        if not instrument_keys:
            return {}
        with self.engine.begin() as connection:
            rows = (
                connection.execute(
                    select(
                        ohlcv_daily_table.c.instrument_key,
                        func.max(ohlcv_daily_table.c.date).label("latest_date"),
                    )
                    .where(ohlcv_daily_table.c.source == source)
                    .where(ohlcv_daily_table.c.instrument_key.in_(instrument_keys))
                    .group_by(ohlcv_daily_table.c.instrument_key)
                )
                .mappings()
                .all()
            )
        return {
            str(row["instrument_key"]): row["latest_date"]
            for row in rows
            if row["latest_date"] is not None
        }

    def insert_data_quality_audits(
        self,
        audit: pd.DataFrame,
        dataset_name: str,
        source: str,
        interval: str,
    ) -> int:
        rows = self._audit_rows(
            audit,
            dataset_name=dataset_name,
            source=source,
            interval=interval,
        )
        if not rows:
            return 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                connection.execute(data_quality_audits_table.insert().values(chunk))
        return len(rows)

    def start_ingestion_run(
        self,
        job_name: str,
        exchange: str,
        source: str,
        items_requested: int,
        run_metadata: Mapping[str, Any] | None = None,
    ) -> UUID:
        run_id = uuid4()
        with self.engine.begin() as connection:
            connection.execute(
                ingestion_runs_table.insert().values(
                    run_id=str(run_id),
                    job_name=job_name,
                    status="running",
                    exchange=exchange,
                    source=source,
                    started_at=datetime.now(UTC),
                    items_requested=items_requested,
                    items_processed=0,
                    items_succeeded=0,
                    items_failed=0,
                    run_metadata=dict(run_metadata or {}),
                )
            )
        return run_id

    def finish_ingestion_run(
        self,
        run_id: UUID,
        status: str,
        items_processed: int,
        items_succeeded: int,
        items_failed: int,
        error_message: str | None = None,
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                ingestion_runs_table.update()
                .where(ingestion_runs_table.c.run_id == str(run_id))
                .values(
                    status=status,
                    finished_at=datetime.now(UTC),
                    items_processed=items_processed,
                    items_succeeded=items_succeeded,
                    items_failed=items_failed,
                    error_message=error_message,
                )
            )

    def latest_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        query = (
            ingestion_runs_table.select()
            .order_by(ingestion_runs_table.c.started_at.desc())
            .limit(limit)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def market_status(self) -> list[dict[str, Any]]:
        query = text(
            """
            WITH universe AS (
                SELECT exchange, count(*) AS universe_size
                FROM symbols
                WHERE is_active = true
                GROUP BY exchange
            ),
            candles AS (
                SELECT exchange, max(ts) AS latest_candle, count(DISTINCT ticker) AS candle_symbols
                FROM ohlcv_hourly
                GROUP BY exchange
            ),
            runs AS (
                SELECT DISTINCT ON (exchange) exchange, started_at, status
                FROM ingestion_runs
                WHERE job_name LIKE '%_hourly_ohlcv'
                ORDER BY exchange, started_at DESC
            )
            SELECT
                COALESCE(universe.exchange, candles.exchange, runs.exchange) AS exchange,
                COALESCE(universe.universe_size, 0) AS universe_size,
                candles.latest_candle,
                COALESCE(candles.candle_symbols, 0) AS candle_symbols,
                runs.started_at AS last_ohlcv_run,
                runs.status AS last_ohlcv_status
            FROM universe
            FULL OUTER JOIN candles ON universe.exchange = candles.exchange
            FULL OUTER JOIN runs ON COALESCE(universe.exchange, candles.exchange) = runs.exchange
            ORDER BY exchange
            """
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def candles(self, ticker: str, limit: int = 300) -> list[dict[str, Any]]:
        query = (
            ohlcv_hourly_table.select()
            .where(ohlcv_hourly_table.c.ticker == ticker.upper())
            .order_by(ohlcv_hourly_table.c.ts.desc())
            .limit(limit)
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return list(reversed(rows))

    def latest_candles(
        self,
        exchange: str,
        symbols: list[str] | None = None,
        lookback_hours: int = 72,
        source: str = "yahoo",
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "exchange": exchange.upper(),
            "source": source,
            "lookback_start": datetime.now(UTC) - timedelta(hours=lookback_hours),
        }
        symbol_filter = ""
        if symbols:
            params["symbols"] = [symbol.upper() for symbol in symbols]
            symbol_filter = "AND ticker = ANY(:symbols)"

        query = text(
            f"""
            WITH ranked AS (
                SELECT
                    ticker,
                    ts,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    quality_status,
                    fetched_at,
                    row_number() OVER (PARTITION BY ticker ORDER BY ts DESC) AS rn
                FROM ohlcv_hourly
                WHERE exchange = :exchange
                  AND source = :source
                  AND ts >= :lookback_start
                  {symbol_filter}
            )
            SELECT ticker, ts, open, high, low, close, volume, quality_status, fetched_at
            FROM ranked
            WHERE rn = 1
            ORDER BY ticker
            """
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query, params).mappings()]

    def symbol_timeseries(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        source: str = "yahoo",
    ) -> list[dict[str, Any]]:
        query = (
            ohlcv_hourly_table.select()
            .where(ohlcv_hourly_table.c.exchange == exchange.upper())
            .where(ohlcv_hourly_table.c.ticker == symbol.upper())
            .where(ohlcv_hourly_table.c.source == source)
            .where(ohlcv_hourly_table.c.ts >= _as_utc(start_time))
            .where(ohlcv_hourly_table.c.ts <= _as_utc(end_time))
            .order_by(ohlcv_hourly_table.c.ts)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def session_summary(
        self,
        exchange: str,
        session_date: date | None = None,
        source: str = "yahoo",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"exchange": exchange.upper(), "source": source}
        date_filter = ""
        if session_date:
            params["session_date"] = session_date
            date_filter = "WHERE session_date = :session_date"

        query = text(
            f"""
            WITH exchange_rows AS (
                SELECT
                    ticker,
                    ts,
                    close,
                    date(ts AT TIME ZONE 'UTC') AS session_date
                FROM ohlcv_hourly
                WHERE exchange = :exchange AND source = :source
            ),
            target_session AS (
                SELECT session_date
                FROM exchange_rows
                {date_filter}
                ORDER BY session_date DESC
                LIMIT 1
            ),
            session_rows AS (
                SELECT er.*
                FROM exchange_rows er
                JOIN target_session ts ON er.session_date = ts.session_date
            ),
            last_close AS (
                SELECT DISTINCT ON (ticker)
                    ticker,
                    close,
                    ts
                FROM session_rows
                ORDER BY ticker, ts DESC
            ),
            first_close AS (
                SELECT DISTINCT ON (ticker)
                    ticker,
                    close,
                    ts
                FROM session_rows
                ORDER BY ticker, ts ASC
            ),
            deltas AS (
                SELECT
                    lc.ticker,
                    fc.close AS first_close,
                    lc.close AS last_close,
                    CASE
                        WHEN fc.close > 0 THEN ((lc.close - fc.close) / fc.close) * 100.0
                        ELSE NULL
                    END AS return_pct
                FROM last_close lc
                JOIN first_close fc ON lc.ticker = fc.ticker
            )
            SELECT
                (SELECT session_date FROM target_session) AS session_date,
                count(*) AS symbol_count,
                count(*) FILTER (WHERE return_pct > 0) AS advancers,
                count(*) FILTER (WHERE return_pct < 0) AS decliners,
                count(*) FILTER (WHERE return_pct = 0) AS unchanged,
                avg(return_pct) AS avg_return_pct,
                min(return_pct) AS min_return_pct,
                max(return_pct) AS max_return_pct
            FROM deltas
            """
        )
        with self.engine.begin() as connection:
            row = connection.execute(query, params).mappings().first()
        return dict(row) if row else {}

    def data_quality_snapshot(self, exchange: str, source: str = "yahoo") -> dict[str, Any]:
        params = {"exchange": exchange.upper(), "source": source}
        query = text(
            """
            WITH latest_ts AS (
                SELECT max(ts) AS max_ts
                FROM ohlcv_hourly
                WHERE exchange = :exchange AND source = :source
            ),
            latest_counts AS (
                SELECT count(DISTINCT ticker) AS latest_candle_symbols
                FROM ohlcv_hourly o
                JOIN latest_ts l ON o.ts = l.max_ts
                WHERE o.exchange = :exchange AND o.source = :source
            ),
            active_universe AS (
                SELECT count(*) AS active_symbols
                FROM symbols
                WHERE exchange = :exchange AND is_active = true
            ),
            backlog AS (
                SELECT
                    count(*) FILTER (
                        WHERE status IN ('missing', 'partial', 'failed')
                    ) AS open_backlog_windows
                FROM hourly_backlog_windows
                WHERE exchange = :exchange AND source = :source
            )
            SELECT
                :exchange AS exchange,
                (SELECT max_ts FROM latest_ts) AS latest_candle_ts,
                coalesce(
                    (SELECT latest_candle_symbols FROM latest_counts),
                    0
                ) AS latest_candle_symbols,
                coalesce((SELECT active_symbols FROM active_universe), 0) AS active_symbols,
                coalesce((SELECT open_backlog_windows FROM backlog), 0) AS open_backlog_windows
            """
        )
        with self.engine.begin() as connection:
            row = connection.execute(query, params).mappings().first()
        if not row:
            return {}
        data = dict(row)
        active_symbols = int(data["active_symbols"] or 0)
        latest_candle_symbols = int(data["latest_candle_symbols"] or 0)
        data["completeness_ratio"] = (
            (latest_candle_symbols / active_symbols) if active_symbols > 0 else 0.0
        )
        return data

    @staticmethod
    def _ohlcv_rows(frame: pd.DataFrame, exchange: str, source: str) -> list[dict[str, Any]]:
        if frame.empty:
            return []

        column_map = {column.lower(): column for column in frame.columns}
        required = ["ticker", "datetime", "open", "high", "low", "close", "volume"]
        if missing := [column for column in required if column not in column_map]:
            raise ValueError(f"Missing OHLCV columns: {', '.join(missing)}")

        fetched_at = datetime.now(UTC)
        rows: list[dict[str, Any]] = []
        for record in frame.to_dict(orient="records"):
            row = {key.lower(): value for key, value in record.items()}
            if any(pd.isna(row[column]) for column in required):
                continue
            rows.append(
                {
                    "ticker": str(row["ticker"]).upper(),
                    "ts": pd.Timestamp(row["datetime"]).to_pydatetime(),
                    "source": source,
                    "exchange": exchange,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]),
                    "fetched_at": fetched_at,
                    "quality_status": _quality_status(row),
                }
            )
        return rows

    @staticmethod
    def _instrument_rows(frame: pd.DataFrame, source: str) -> list[dict[str, Any]]:
        fetched_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            instrument_key = _clean_string(record.get("instrument_key"))
            if not instrument_key:
                continue
            rows.append(
                {
                    "source": source,
                    "instrument_key": instrument_key,
                    "exchange": _clean_string(record.get("exchange")),
                    "segment": _clean_string(record.get("segment")),
                    "asset_type": _clean_string(record.get("asset_type")),
                    "trading_symbol": _clean_string(record.get("trading_symbol")),
                    "name": _clean_string(record.get("name")),
                    "isin": _clean_string(record.get("isin")),
                    "lot_size": _nullable_int(record.get("lot_size")),
                    "tick_size": _nullable_float(record.get("tick_size")),
                    "expiry": _nullable_date(record.get("expiry")),
                    "strike": _nullable_float(record.get("strike")),
                    "option_type": _clean_string(record.get("option_type")),
                    "underlying_symbol": _clean_string(record.get("underlying_symbol")),
                    "underlying_key": _clean_string(record.get("underlying_key")),
                    "exchange_token": _clean_string(record.get("exchange_token")),
                    "active": True,
                    "fetched_at": fetched_at,
                    "raw": record.get("raw") if isinstance(record.get("raw"), dict) else {},
                }
            )
        return rows

    @staticmethod
    def _universe_member_rows(
        members: pd.DataFrame,
        universe_id: str,
        included_at: datetime,
    ) -> list[dict[str, Any]]:
        rows = []
        for record in members.to_dict(orient="records"):
            rows.append(
                {
                    "universe_id": universe_id,
                    "symbol": str(record["symbol"]).upper(),
                    "instrument_key": _clean_string(record.get("instrument_key")),
                    "rank": _nullable_int(record.get("rank")),
                    "avg_daily_volume": _nullable_float(record.get("avg_daily_volume")),
                    "avg_daily_turnover": _nullable_float(record.get("avg_daily_turnover")),
                    "trading_days": _nullable_int(record.get("trading_days")),
                    "zero_volume_ratio": _nullable_float(record.get("zero_volume_ratio")),
                    "start_date": _nullable_date(record.get("first_date")),
                    "end_date": _nullable_date(record.get("last_date")),
                    "included_at": included_at,
                }
            )
        return rows

    @staticmethod
    def _daily_ohlcv_rows(frame: pd.DataFrame, exchange: str, source: str) -> list[dict[str, Any]]:
        if frame.empty:
            return []

        fetched_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
            if any(pd.isna(record.get(column)) for column in required_columns):
                continue
            row = {
                "instrument_key": str(record["InstrumentKey"]),
                "source": source,
                "date": _nullable_date(record["Date"]),
                "symbol": str(record["Symbol"]).upper(),
                "exchange": exchange,
                "open": float(record["Open"]),
                "high": float(record["High"]),
                "low": float(record["Low"]),
                "close": float(record["Close"]),
                "volume": int(record["Volume"]),
                "open_interest": _nullable_int(record.get("OpenInterest")),
                "fetched_at": fetched_at,
            }
            row["quality_status"] = _quality_status(row)
            rows.append(row)
        return rows

    @staticmethod
    def _audit_rows(
        audit: pd.DataFrame,
        dataset_name: str,
        source: str,
        interval: str,
    ) -> list[dict[str, Any]]:
        created_at = datetime.now(UTC)
        rows = []
        for record in audit.to_dict(orient="records"):
            rows.append(
                {
                    "audit_id": str(uuid4()),
                    "dataset_name": dataset_name,
                    "source": source,
                    "symbol": _clean_string(record.get("symbol")),
                    "instrument_key": _clean_string(record.get("instrument_key")),
                    "interval": interval,
                    "start_date": _nullable_date(record.get("start_date")),
                    "end_date": _nullable_date(record.get("end_date")),
                    "rows": int(record.get("rows") or 0),
                    "missing_dates": int(record.get("missing_dates") or 0),
                    "null_rows": int(record.get("null_ohlcv_rows") or 0),
                    "duplicate_rows": int(record.get("duplicate_date_rows") or 0),
                    "zero_volume_rows": int(record.get("zero_volume_rows") or 0),
                    "zero_or_negative_close_rows": int(
                        record.get("zero_or_negative_close_rows") or 0
                    ),
                    "coverage_ratio": _nullable_float(record.get("coverage_ratio")),
                    "status": str(record.get("status") or "unknown"),
                    "warnings_json": [],
                    "created_at": created_at,
                }
            )
        return rows


def make_timescale_store(database_url: str) -> TimescaleStore:
    return TimescaleStore(database_url)


def _quality_status(row: Mapping[str, Any]) -> str:
    if row["close"] <= 0 or row["high"] < row["low"]:
        return "suspicious"
    if row["volume"] < 0:
        return "suspicious"
    return "ok"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _chunks(rows: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _clean_string(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _nullable_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nullable_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _nullable_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _feed_health_can_fetch(health: Mapping[str, Any], now: datetime) -> bool:
    status = health.get("status")
    next_retry_at = health.get("next_retry_at")
    if next_retry_at and _as_utc(next_retry_at) > now:
        return False
    return status not in {"quarantined"}
