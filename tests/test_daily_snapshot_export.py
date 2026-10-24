import hashlib
import json
from contextlib import nullcontext
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from trade_data.operations.daily_snapshot import export_daily_snapshot


class SnapshotEngine:
    dialect = SimpleNamespace(name="postgresql")

    def __init__(self, rows=None):
        self.statements = []
        self.options = []
        dates = pd.bdate_range("2025-01-01", periods=230)
        self.sessions = [
            {
                "exchange": "NSE",
                "session_date": day.date(),
                "is_trading_day": day.date() in {d.date() for d in dates},
                "validation_status": "valid",
            }
            for day in pd.date_range("2025-01-01", "2026-01-01")
        ]
        self.rows = (
            rows
            if rows is not None
            else [
                {
                    "date": day.date(),
                    "instrument_key": "YF|TEST.NS",
                    "symbol": "TEST",
                    "source": "yfinance",
                    "exchange": "NSE",
                    "quality_status": "ok",
                    "open": 99.0 + i,
                    "high": 102.0 + i,
                    "low": 98.0 + i,
                    "close": 100.0 + i,
                    "volume": 100000 + i,
                    "open_interest": None,
                    "fetched_at": datetime(2026, 10, 16, tzinfo=UTC),
                }
                for i, day in enumerate(dates)
            ]
        )

    def connect(self):
        return self

    def execution_options(self, **kwargs):
        self.options.append(kwargs)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def begin(self):
        return nullcontext()

    def execute(self, statement):
        sql = str(statement)
        self.statements.append(sql)
        if sql == "SELECT transaction_timestamp()":
            return SimpleNamespace(scalar_one=lambda: datetime(2026, 10, 16, tzinfo=UTC))
        if sql == "SET TRANSACTION READ ONLY":
            return None
        if "FROM ohlcv_daily" in sql:
            return SimpleNamespace(
                mappings=lambda: SimpleNamespace(
                    partitions=lambda size: (
                        self.rows[i : i + size] for i in range(0, len(self.rows), size)
                    )
                )
            )
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: self.sessions))


def test_export_is_consistent_read_only_chunked_and_hashed(tmp_path):
    engine = SnapshotEngine()
    path = export_daily_snapshot(
        engine,
        destination=tmp_path / "snapshot",
        start_date=date(2025, 1, 1),
        end_date=date(2026, 1, 1),
        chunk_rows=100,
    )
    payload = json.loads(path.read_text())
    assert payload["row_count"] == 230
    assert len(payload["files"]) == 4
    assert engine.statements[0] == "SET TRANSACTION READ ONLY"
    assert engine.options[0] == {"isolation_level": "REPEATABLE READ"}
    assert all(sql.startswith(("SELECT", "SET TRANSACTION READ ONLY")) for sql in engine.statements)
    assert "ohlcv_daily.quality_status" in engine.statements[-1]
    for file in payload["files"]:
        assert (
            hashlib.sha256((path.parent / file["path"]).read_bytes()).hexdigest() == file["sha256"]
        )
    with pytest.raises(FileExistsError):
        export_daily_snapshot(
            engine, destination=path.parent, start_date=date(2025, 1, 1), end_date=date(2026, 1, 1)
        )


def test_empty_snapshot_does_not_publish_partial_bundle(tmp_path):
    with pytest.raises(ValueError, match="No accepted daily candles"):
        export_daily_snapshot(
            SnapshotEngine(rows=[]),
            destination=tmp_path / "snapshot",
            start_date=date(2025, 1, 1),
            end_date=date(2026, 1, 1),
        )
    assert list(tmp_path.iterdir()) == []


def test_incomplete_exchange_calendar_prevents_export(tmp_path):
    engine = SnapshotEngine()
    engine.sessions.pop()
    with pytest.raises(ValueError, match="complete validated exchange calendar"):
        export_daily_snapshot(
            engine,
            destination=tmp_path / "snapshot",
            start_date=date(2025, 1, 1),
            end_date=date(2026, 1, 1),
        )
    assert not (tmp_path / "snapshot").exists()


def test_legacy_ok_flag_cannot_publish_invalid_candle(tmp_path):
    engine = SnapshotEngine()
    engine.rows[0]["open"] = 0
    with pytest.raises(ValueError, match="invalid OHLCV"):
        export_daily_snapshot(
            engine,
            destination=tmp_path / "snapshot",
            start_date=date(2025, 1, 1),
            end_date=date(2026, 1, 1),
        )
    assert not (tmp_path / "snapshot").exists()
