"""Consistent read-only exports for independent research consumers."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.engine import Engine

from trade_data.market_data.price_quality import daily_price_errors
from trade_data.storage.timescale import exchange_sessions_table, ohlcv_daily_table


def export_daily_snapshot(
    engine: Engine,
    *,
    destination: Path,
    start_date: date,
    end_date: date,
    exchange: str = "NSE",
    provider: str = "yfinance",
    chunk_rows: int = 50_000,
) -> Path:
    if exchange != "NSE" or provider != "yfinance":
        raise ValueError("Snapshot exports currently support NSE/yfinance only.")
    if not 0 <= (end_date - start_date).days <= 36_525 or start_date.year < 2:
        raise ValueError("Choose an ordered date range of at most 36,525 days.")
    if not 1 <= chunk_rows <= 100_000:
        raise ValueError("chunk_rows must be between 1 and 100,000.")
    if destination.exists():
        raise FileExistsError(destination)
    if engine.dialect.name != "postgresql":
        raise ValueError("Consistent snapshots require PostgreSQL repeatable-read transactions.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".daily-snapshot-", dir=destination.parent))
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
            with connection.begin():
                connection.execute(text("SET TRANSACTION READ ONLY"))
                as_of = connection.execute(text("SELECT transaction_timestamp()")).scalar_one()
                sessions = (
                    select(exchange_sessions_table)
                    .where(
                        exchange_sessions_table.c.exchange == exchange,
                        exchange_sessions_table.c.session_date.between(start_date, end_date),
                    )
                    .order_by(exchange_sessions_table.c.session_date)
                )
                calendar = pd.DataFrame(connection.execute(sessions).mappings().all())
                required_dates = {
                    start_date + timedelta(days=offset)
                    for offset in range((end_date - start_date).days + 1)
                }
                if (
                    calendar.empty
                    or set(calendar["session_date"]) != required_dates
                    or calendar["session_date"].duplicated().any()
                    or not calendar["validation_status"].str.startswith("valid").all()
                ):
                    raise ValueError("A complete validated exchange calendar is required.")
                calendar = calendar[calendar["is_trading_day"]].copy()
                if calendar.empty:
                    raise ValueError("No validated trading sessions exist for this range.")
                calendar.to_parquet(stage / "sessions.parquet", index=False)
                eligible = select(exchange_sessions_table.c.session_date).where(
                    exchange_sessions_table.c.exchange == exchange,
                    exchange_sessions_table.c.session_date.between(start_date, end_date),
                    exchange_sessions_table.c.is_trading_day.is_(True),
                    exchange_sessions_table.c.validation_status.like("valid%"),
                )
                query = (
                    select(ohlcv_daily_table)
                    .where(
                        ohlcv_daily_table.c.exchange == exchange,
                        ohlcv_daily_table.c.source == provider,
                        ohlcv_daily_table.c.quality_status == "ok",
                        ohlcv_daily_table.c.date.in_(eligible),
                    )
                    .order_by(ohlcv_daily_table.c.instrument_key, ohlcv_daily_table.c.date)
                )
                files = []
                row_count = 0
                result = connection.execution_options(stream_results=True).execute(query).mappings()
                for number, rows in enumerate(result.partitions(chunk_rows), start=1):
                    if any(daily_price_errors(row) for row in rows):
                        raise ValueError(
                            "Accepted history contains invalid OHLCV; revalidate first."
                        )
                    frame = pd.DataFrame(rows)
                    name = f"ohlcv-{number:05d}.parquet"
                    frame.to_parquet(stage / name, index=False)
                    files.append({"path": name, "rows": len(frame)})
                    row_count += len(frame)
                if not files:
                    raise ValueError("No accepted daily candles exist in the selected sessions.")
                files.append({"path": "sessions.parquet", "rows": len(calendar)})
                for file in files:
                    file["sha256"] = hashlib.sha256((stage / file["path"]).read_bytes()).hexdigest()
                manifest = {
                    "schema_version": 1,
                    "contract_id": "daily.ohlcv.snapshot.v1",
                    "snapshot_id": str(uuid4()),
                    "as_of": as_of.isoformat(),
                    "exported_at": datetime.now(UTC).isoformat(),
                    "provider": provider,
                    "exchange": exchange,
                    "interval": "1d",
                    "calendar_complete": True,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "identity": "provider instrument_key; source and exchange scope are mandatory",
                    "adjustment": (
                        "Yahoo OHLCV as stored; auto_adjust=False; not total-return adjusted"
                    ),
                    "quality": (
                        "quality_status=ok; hard OHLCV invariants checked; "
                        "validated exchange sessions"
                    ),
                    "row_count": row_count,
                    "files": files,
                }
                (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        # A non-empty destination directory cannot be replaced by this rename.
        stage.rename(destination)
        return destination / "manifest.json"
    except BaseException:
        shutil.rmtree(stage)
        raise
