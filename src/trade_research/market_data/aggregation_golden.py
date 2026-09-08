from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from trade_research.market_data.aggregation import (
    AggregatedMarketCandle,
    IntradayAggregationRequest,
    aggregate_nse_minute_candles,
)
from trade_research.market_data.contracts import CandleInterval, MarketCandle

SCHEMA_VERSION = "nse-intraday-aggregation-golden-v1"
SOURCE_COLUMNS = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "provider_timestamp",
    "raw_artifact_id",
)
OUTPUT_COLUMNS = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source_rows",
    "expected_source_rows",
    "complete",
    "source_digest",
    "raw_artifact_ids",
)


@dataclass(frozen=True)
class AggregationGoldenReport:
    passed: bool
    dataset_id: str
    dataset_sha256: str
    cases_total: int
    cases_passed: int
    mismatches: tuple[str, ...]


def load_aggregation_golden(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported aggregation golden schema version")
    if tuple(payload.get("source_columns", ())) != SOURCE_COLUMNS:
        raise ValueError("aggregation golden source columns changed")
    if tuple(payload.get("output_columns", ())) != OUTPUT_COLUMNS:
        raise ValueError("aggregation golden output columns changed")
    if (
        not payload.get("dataset_id")
        or not payload.get("session_date")
        or not payload.get("source_rows")
    ):
        raise ValueError("aggregation golden dataset is empty")
    expected = payload.get("expected")
    if not isinstance(expected, dict):
        raise ValueError("aggregation golden expectations are missing")
    required_cases = {
        f"{interval}:{mode}"
        for interval in ("5m", "15m", "30m", "1h")
        for mode in ("complete_only", "include_incomplete")
    }
    if set(expected) != required_cases:
        raise ValueError("aggregation golden cases are incomplete")
    return payload


def evaluate_python_aggregation_golden(
    dataset: dict[str, Any],
) -> AggregationGoldenReport:
    candles = _source_candles(dataset)
    actual: dict[str, list[list[Any]]] = {}
    for interval in ("5m", "15m", "30m", "1h"):
        for mode in ("complete_only", "include_incomplete"):
            request = IntradayAggregationRequest(
                instrument_id=str(dataset["instrument"]["instrument_id"]),
                interval=CandleInterval(interval),
                window_start=_datetime(dataset["window_start"]),
                window_end=_datetime(dataset["window_end"]),
                complete_only=mode == "complete_only",
            )
            actual[f"{interval}:{mode}"] = [
                _output_row(candle)
                for candle in aggregate_nse_minute_candles(request, candles)
            ]
    return evaluate_aggregation_candidate(dataset, actual)


def evaluate_aggregation_candidate(
    dataset: dict[str, Any],
    candidate: dict[str, list[list[Any]]],
) -> AggregationGoldenReport:
    expected = dataset["expected"]
    mismatches: list[str] = []
    for case_id in sorted(expected):
        if case_id not in candidate:
            mismatches.append(f"{case_id}: candidate output is missing")
        elif candidate[case_id] != expected[case_id]:
            mismatches.append(f"{case_id}: output does not match the locked fixture")
    unexpected = sorted(set(candidate) - set(expected))
    mismatches.extend(f"{case_id}: unexpected candidate case" for case_id in unexpected)
    return AggregationGoldenReport(
        passed=not mismatches,
        dataset_id=str(dataset["dataset_id"]),
        dataset_sha256=_dataset_digest(dataset),
        cases_total=len(expected),
        cases_passed=len(expected)
        - sum(case_id.split(": ")[0] in expected for case_id in mismatches),
        mismatches=tuple(mismatches),
    )


def load_candidate_output(path: Path) -> dict[str, list[list[Any]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("candidate output uses the wrong schema version")
    output = payload.get("output")
    if not isinstance(output, dict):
        raise ValueError("candidate output is missing")
    return output


def _source_candles(dataset: dict[str, Any]) -> list[MarketCandle]:
    instrument = dataset["instrument"]
    candles: list[MarketCandle] = []
    for values in dataset["source_rows"]:
        row = dict(zip(SOURCE_COLUMNS, values, strict=True))
        timestamp = _datetime(row["timestamp"])
        candles.append(
            MarketCandle(
                instrument_id=str(instrument["instrument_id"]),
                provider_symbol=str(instrument["provider_symbol"]),
                exchange="NSE",
                session_date=date.fromisoformat(str(dataset["session_date"])),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=int(row["volume"]),
                currency="INR",
                provider="yfinance",
                provider_timestamp=_datetime(row["provider_timestamp"]),
                request_id=str(dataset["request_id"]),
                adapter_version=str(dataset["adapter_version"]),
                interval=CandleInterval.ONE_MINUTE,
                timestamp=timestamp,
                raw_artifact_id=(
                    str(row["raw_artifact_id"])
                    if row["raw_artifact_id"] is not None
                    else None
                ),
                symbol=str(instrument["symbol"]),
            )
        )
    return candles


def _output_row(candle: AggregatedMarketCandle) -> list[Any]:
    return [
        candle.candle_timestamp.astimezone(UTC).isoformat(),
        format(candle.open, "f"),
        format(candle.high, "f"),
        format(candle.low, "f"),
        format(candle.close, "f"),
        candle.volume,
        candle.source_rows,
        candle.expected_source_rows,
        candle.complete,
        candle.source_digest,
        list(candle.raw_artifact_ids),
    ]


def _dataset_digest(dataset: dict[str, Any]) -> str:
    encoded = json.dumps(dataset, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("golden datetimes must be timezone-aware")
    return parsed.astimezone(UTC)
