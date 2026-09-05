from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from trade_research.storage.canary import (
    FEATURE_KEYS,
    build_canary_payload,
    cleanup_canary,
    load_canary,
    reconcile_canary,
    run_benchmarks,
)


class _ClickHouse:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str, dict[str, object]]] = {}
        self.queries: list[str] = []

    def insert(self, table: str, data, column_names) -> None:
        stored = self.tables.setdefault(table, {})
        for values in data:
            row = dict(zip(column_names, values, strict=True))
            stored[str(row["content_sha256"])] = row

    def query(self, query: str, parameters=None) -> SimpleNamespace:
        self.queries.append(query)
        if "SELECT content_sha256" in query:
            table = query.split("FROM research.", 1)[1].split()[0]
            rows = [(digest,) for digest in self.tables[f"research.{table}"]]
            return SimpleNamespace(result_rows=rows, column_names=["content_sha256"])
        if "SELECT count() FROM research." in query:
            table = query.split("FROM research.", 1)[1].split()[0]
            return SimpleNamespace(
                result_rows=[(len(self.tables[f"research.{table}"]),)],
                column_names=["count()"],
            )
        return SimpleNamespace(result_rows=[], column_names=[])

    def command(self, command: str, parameters=None) -> None:
        table = command.split("ALTER TABLE research.", 1)[1].split()[0]
        self.tables[f"research.{table}"] = {}


def _source_frame() -> pd.DataFrame:
    rows = []
    for instrument, base in (("NSE_EQ|ONE", 100.0), ("NSE_EQ|TWO", 200.0)):
        for offset, session in enumerate(pd.date_range("2026-01-01", periods=7)):
            close = base + offset
            rows.append(
                {
                    "instrument_key": instrument,
                    "source": "upstox",
                    "date": session.date(),
                    "symbol": instrument.rsplit("|", 1)[-1],
                    "exchange": "NSE",
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "volume": 1_000 + offset,
                    "quality_status": "ok",
                }
            )
    return pd.DataFrame(rows)


def test_canary_build_load_retry_reconcile_and_benchmark() -> None:
    payload = build_canary_payload(_source_frame(), maximum_rows=100)

    assert len(payload.ohlcv_rows) == 14
    assert len(payload.feature_rows) == 14 * len(FEATURE_KEYS)
    assert len(payload.target_rows) == 14
    assert payload.source_run_id.startswith("phase2-canary-")
    assert len(payload.content_sha256) == 64

    client = _ClickHouse()
    load_canary(client, payload, database="research")
    load_canary(client, payload, database="research")
    reconciliation = reconcile_canary(client, payload, database="research")

    assert all(item["passed"] for item in reconciliation.values())
    assert reconciliation["ohlcv_daily"]["actual_rows"] == 14
    benchmarks = run_benchmarks(
        client,
        payload,
        database="research",
        iterations=1,
        concurrency=2,
    )
    assert set(benchmarks) == {
        "exchange_date_scan",
        "percentile_distribution",
        "feature_target_join",
        "factor_aggregation",
        "dataset_extraction",
        "long_feature_scan",
        "wide_family_pivot",
        "concurrent_api_reads",
    }
    assert all(result["maximum_ms"] >= 0 for result in benchmarks.values())


def test_canary_rejects_unvalidated_or_unbounded_source() -> None:
    frame = _source_frame()
    frame.loc[0, "quality_status"] = "suspicious"
    with pytest.raises(ValueError, match="quality_status=ok"):
        build_canary_payload(frame, maximum_rows=100)

    with pytest.raises(ValueError, match="limit is 5"):
        build_canary_payload(_source_frame(), maximum_rows=5)


def test_canary_reconciliation_detects_missing_content() -> None:
    payload = build_canary_payload(_source_frame(), maximum_rows=100)
    client = _ClickHouse()
    load_canary(client, payload, database="research")
    client.tables["research.feature_observations_daily"].pop(
        payload.feature_rows[0]["content_sha256"]
    )

    result = reconcile_canary(client, payload, database="research")

    assert result["feature_observations_daily"]["passed"] is False
    assert result["feature_observations_daily"]["missing_digests"] == 1


def test_canary_cleanup_is_scoped_to_an_explicit_run_id() -> None:
    payload = build_canary_payload(_source_frame(), maximum_rows=100)
    client = _ClickHouse()
    load_canary(client, payload, database="research")

    deleted = cleanup_canary(
        client,
        source_run_id=payload.source_run_id,
        database="research",
    )

    assert deleted == {
        "ohlcv_daily": 14,
        "feature_observations_daily": 42,
        "target_observations_daily": 14,
    }
    assert all(not rows for rows in client.tables.values())
    with pytest.raises(ValueError, match="not a Phase 2 canary"):
        cleanup_canary(client, source_run_id="production", database="research")
