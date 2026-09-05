from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from trade_research.storage.clickhouse import ClickHouseClient

FEATURE_KEYS = ("return_1d", "close_to_sma_5", "volume_to_sma_5")
_CANARY_RUN_ID = re.compile(r"^phase2-canary-[0-9a-f]{20}$")
REQUIRED_SOURCE_COLUMNS = {
    "instrument_key",
    "source",
    "date",
    "symbol",
    "exchange",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quality_status",
}


@dataclass(frozen=True)
class CanaryPayload:
    workspace_id: str
    source_run_id: str
    content_sha256: str
    ohlcv_rows: list[dict[str, Any]]
    feature_rows: list[dict[str, Any]]
    target_rows: list[dict[str, Any]]


def _digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _nullable_number(value: Any) -> float | None:
    if pd.isna(value):
        return None
    number = float(value)
    if not math.isfinite(number):
        return None
    return number


def _version(digest: str) -> int:
    return int(digest[:15], 16)


def _validate_source(frame: pd.DataFrame, *, maximum_rows: int) -> pd.DataFrame:
    missing = REQUIRED_SOURCE_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"canary source is missing columns: {', '.join(sorted(missing))}")
    if frame.empty:
        raise ValueError("canary source contains no rows")
    if len(frame) > maximum_rows:
        raise ValueError(f"canary source has {len(frame)} rows; limit is {maximum_rows}")
    if frame.duplicated(["instrument_key", "source", "date"]).any():
        raise ValueError("canary source contains duplicate instrument/source/date keys")
    if set(frame["quality_status"].astype(str)) != {"ok"}:
        raise ValueError("canary source must contain only quality_status=ok rows")

    numeric = frame[["open", "high", "low", "close", "volume"]].apply(
        pd.to_numeric,
        errors="coerce",
    )
    invalid = (
        numeric.isna().any(axis=1)
        | (numeric[["open", "high", "low", "close"]] <= 0).any(axis=1)
        | (numeric["volume"] < 0)
        | (numeric["high"] < numeric[["open", "low", "close"]].max(axis=1))
        | (numeric["low"] > numeric[["open", "high", "close"]].min(axis=1))
    )
    if invalid.any():
        raise ValueError(f"canary source contains {int(invalid.sum())} invalid OHLCV rows")
    result = frame.copy()
    result[["open", "high", "low", "close", "volume"]] = numeric
    result["date"] = pd.to_datetime(result["date"]).dt.date
    return result.sort_values(["instrument_key", "date", "source"]).reset_index(drop=True)


def build_canary_payload(
    frame: pd.DataFrame,
    *,
    workspace_id: str = "phase2-canary",
    maximum_rows: int = 50_000,
) -> CanaryPayload:
    source = _validate_source(frame, maximum_rows=maximum_rows)
    source_manifest = [
        {
            "instrument_id": str(row.instrument_key),
            "source": str(row.source),
            "session_date": row.date.isoformat(),
            "symbol": str(row.symbol),
            "exchange": str(row.exchange),
            "open": f"{float(row.open):.8f}",
            "high": f"{float(row.high):.8f}",
            "low": f"{float(row.low):.8f}",
            "close": f"{float(row.close):.8f}",
            "volume": int(row.volume),
        }
        for row in source.itertuples(index=False)
    ]
    manifest_digest = hashlib.sha256(
        json.dumps(source_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    source_run_id = f"phase2-canary-{manifest_digest[:20]}"
    version = _version(manifest_digest)

    ohlcv_rows: list[dict[str, Any]] = []
    for item in source_manifest:
        row_digest = _digest(item)
        ohlcv_rows.append(
            {
                "workspace_id": workspace_id,
                **item,
                "source_run_id": source_run_id,
                "content_sha256": row_digest,
                "version": version,
            }
        )

    derived = source.copy()
    grouped = derived.groupby("instrument_key", sort=False)
    derived["return_1d"] = grouped["close"].pct_change(fill_method=None)
    derived["close_to_sma_5"] = (
        derived["close"] / grouped["close"].transform(lambda values: values.rolling(5).mean())
    ) - 1.0
    derived["volume_to_sma_5"] = (
        derived["volume"] / grouped["volume"].transform(lambda values: values.rolling(5).mean())
    ) - 1.0
    derived["target_return_1d"] = grouped["close"].transform(
        lambda values: values.shift(-1) / values - 1.0
    )

    feature_rows: list[dict[str, Any]] = []
    target_rows: list[dict[str, Any]] = []
    for row in derived.itertuples(index=False):
        for feature_key in FEATURE_KEYS:
            item = {
                "workspace_id": workspace_id,
                "feature_key": feature_key,
                "feature_version": "phase2-canary-v1",
                "instrument_id": str(row.instrument_key),
                "session_date": row.date,
                "value": _nullable_number(getattr(row, feature_key)),
                "dataset_snapshot_id": manifest_digest,
                "source_run_id": source_run_id,
            }
            item["content_sha256"] = _digest(
                {
                    key: str(value) if isinstance(value, date) else value
                    for key, value in item.items()
                }
            )
            item["version"] = version
            feature_rows.append(item)
        target = {
            "workspace_id": workspace_id,
            "target_key": "forward_return_1d",
            "target_version": "phase2-canary-v1",
            "instrument_id": str(row.instrument_key),
            "session_date": row.date,
            "horizon_sessions": 1,
            "value": _nullable_number(row.target_return_1d),
            "dataset_snapshot_id": manifest_digest,
            "source_run_id": source_run_id,
        }
        target["content_sha256"] = _digest(
            {key: str(value) if isinstance(value, date) else value for key, value in target.items()}
        )
        target["version"] = version
        target_rows.append(target)

    return CanaryPayload(
        workspace_id=workspace_id,
        source_run_id=source_run_id,
        content_sha256=manifest_digest,
        ohlcv_rows=ohlcv_rows,
        feature_rows=feature_rows,
        target_rows=target_rows,
    )


def load_canary(client: ClickHouseClient, payload: CanaryPayload, *, database: str) -> None:
    tables = (
        ("ohlcv_daily", payload.ohlcv_rows),
        ("feature_observations_daily", payload.feature_rows),
        ("target_observations_daily", payload.target_rows),
    )
    for table, rows in tables:
        client.insert(
            f"{database}.{table}",
            [[row[column] for column in rows[0]] for row in rows],
            column_names=list(rows[0]),
        )


def _stored_digests(
    client: ClickHouseClient,
    *,
    database: str,
    table: str,
    payload: CanaryPayload,
) -> Counter[str]:
    result = client.query(
        f"""
        SELECT content_sha256
        FROM {database}.{table} FINAL
        WHERE workspace_id = {{workspace_id:String}}
          AND source_run_id = {{source_run_id:String}}
        """,
        parameters={
            "workspace_id": payload.workspace_id,
            "source_run_id": payload.source_run_id,
        },
    )
    return Counter(str(row[0]) for row in result.result_rows)


def reconcile_canary(
    client: ClickHouseClient,
    payload: CanaryPayload,
    *,
    database: str,
) -> dict[str, dict[str, Any]]:
    report: dict[str, dict[str, Any]] = {}
    for table, rows in (
        ("ohlcv_daily", payload.ohlcv_rows),
        ("feature_observations_daily", payload.feature_rows),
        ("target_observations_daily", payload.target_rows),
    ):
        expected = Counter(str(row["content_sha256"]) for row in rows)
        actual = _stored_digests(
            client,
            database=database,
            table=table,
            payload=payload,
        )
        report[table] = {
            "expected_rows": sum(expected.values()),
            "actual_rows": sum(actual.values()),
            "missing_digests": sum((expected - actual).values()),
            "unexpected_digests": sum((actual - expected).values()),
            "passed": expected == actual,
        }
    return report


def _latency_summary(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    p95_index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return {
        "p50_ms": round(statistics.median(ordered), 3),
        "p95_ms": round(ordered[p95_index], 3),
        "maximum_ms": round(ordered[-1], 3),
    }


def run_benchmarks(
    client: ClickHouseClient,
    payload: CanaryPayload,
    *,
    database: str,
    iterations: int = 5,
    concurrency: int = 4,
    client_factory: Callable[[], ClickHouseClient] | None = None,
) -> dict[str, dict[str, float]]:
    if not 1 <= iterations <= 25:
        raise ValueError("iterations must be between 1 and 25")
    if not 1 <= concurrency <= 16:
        raise ValueError("concurrency must be between 1 and 16")
    parameters = {
        "workspace_id": payload.workspace_id,
        "source_run_id": payload.source_run_id,
    }
    queries = {
        "exchange_date_scan": f"""
            SELECT exchange, session_date, count(), avg(close)
            FROM {database}.ohlcv_daily FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND source_run_id = {{source_run_id:String}}
            GROUP BY exchange, session_date ORDER BY exchange, session_date
        """,
        "percentile_distribution": f"""
            SELECT feature_key, quantilesExact(0.25, 0.5, 0.75)(value)
            FROM {database}.feature_observations_daily FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND source_run_id = {{source_run_id:String}}
            GROUP BY feature_key ORDER BY feature_key
        """,
        "feature_target_join": f"""
            SELECT f.feature_key, count(), corr(f.value, t.value)
            FROM (
              SELECT * FROM {database}.feature_observations_daily FINAL
              WHERE workspace_id = {{workspace_id:String}}
                AND source_run_id = {{source_run_id:String}}
            ) AS f
            INNER JOIN (
              SELECT * FROM {database}.target_observations_daily FINAL
              WHERE workspace_id = {{workspace_id:String}}
                AND source_run_id = {{source_run_id:String}}
            ) AS t
              ON f.workspace_id = t.workspace_id
             AND f.instrument_id = t.instrument_id
             AND f.session_date = t.session_date
            GROUP BY f.feature_key ORDER BY f.feature_key
        """,
        "factor_aggregation": f"""
            SELECT session_date, feature_key, avg(value), stddevPop(value), count(value)
            FROM {database}.feature_observations_daily FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND source_run_id = {{source_run_id:String}}
            GROUP BY session_date, feature_key ORDER BY session_date, feature_key
        """,
        "dataset_extraction": f"""
            SELECT instrument_id, session_date, feature_key, value
            FROM {database}.feature_observations_daily FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND source_run_id = {{source_run_id:String}}
            ORDER BY instrument_id, session_date, feature_key
        """,
        "long_feature_scan": f"""
            SELECT instrument_id, session_date, feature_key, value
            FROM {database}.feature_observations_daily FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND source_run_id = {{source_run_id:String}}
            ORDER BY instrument_id, session_date, feature_key
        """,
        "wide_family_pivot": f"""
            SELECT instrument_id, session_date,
              anyIf(value, feature_key = 'return_1d') AS return_1d,
              anyIf(value, feature_key = 'close_to_sma_5') AS close_to_sma_5,
              anyIf(value, feature_key = 'volume_to_sma_5') AS volume_to_sma_5
            FROM {database}.feature_observations_daily FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND source_run_id = {{source_run_id:String}}
            GROUP BY instrument_id, session_date ORDER BY instrument_id, session_date
        """,
    }

    report: dict[str, dict[str, float]] = {}
    for name, query in queries.items():
        samples = []
        for _ in range(iterations):
            started = time.perf_counter()
            client.query(query, parameters=parameters)
            samples.append((time.perf_counter() - started) * 1000)
        report[name] = _latency_summary(samples)

    concurrent_query = queries["exchange_date_scan"]

    def execute_reads(read_client: ClickHouseClient) -> list[float]:
        samples = []
        for _ in range(iterations):
            started = time.perf_counter()
            read_client.query(concurrent_query, parameters=parameters)
            samples.append((time.perf_counter() - started) * 1000)
        return samples

    concurrent_clients = [
        client_factory() if client_factory is not None else client for _ in range(concurrency)
    ]
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        worker_samples = executor.map(execute_reads, concurrent_clients)
        concurrent_samples = [sample for samples in worker_samples for sample in samples]
    report["concurrent_api_reads"] = _latency_summary(concurrent_samples)
    return report


def cleanup_canary(
    client: ClickHouseClient,
    *,
    source_run_id: str,
    database: str,
    workspace_id: str = "phase2-canary",
) -> dict[str, int]:
    """Remove only one explicit canary run through the migration identity."""

    if not _CANARY_RUN_ID.fullmatch(source_run_id):
        raise ValueError("source_run_id is not a Phase 2 canary run id")
    deleted: dict[str, int] = {}
    for table in (
        "ohlcv_daily",
        "feature_observations_daily",
        "target_observations_daily",
    ):
        result = client.query(
            f"""
            SELECT count() FROM {database}.{table} FINAL
            WHERE workspace_id = {{workspace_id:String}}
              AND source_run_id = {{source_run_id:String}}
            """,
            parameters={"workspace_id": workspace_id, "source_run_id": source_run_id},
        )
        deleted[table] = int(result.result_rows[0][0])
        client.command(
            f"""
            ALTER TABLE {database}.{table}
            DELETE WHERE workspace_id = {{workspace_id:String}}
              AND source_run_id = {{source_run_id:String}}
            SETTINGS mutations_sync = 2
            """,
            parameters={"workspace_id": workspace_id, "source_run_id": source_run_id},
        )
    return deleted
