#!/usr/bin/env python3
"""Bounded yfinance rate probe with raw Yahoo HTTP status capture.

This is an operational diagnostic, not a benchmark. It ramps through explicit
request-start rates, spaces starts evenly, and stops immediately on HTTP 429.
Other non-200 responses are recorded by default because invalid symbols can
produce errors unrelated to throttling; an option enables stopping on them.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yfinance as yf
from curl_cffi import requests as curl_requests
from yfinance.exceptions import YFRateLimitError

from trade_research.universe.nse import NSEUniverseProvider
from trade_research.universe.yfinance_full import (
    YFinanceCanadaUniverseProvider,
    YFinanceUSUniverseProvider,
)

DEFAULT_STAGES = (10, 15, 20, 25, 30)
FALLBACK_SYMBOLS = (
    "AAPL",
    "MSFT",
    "AMZN",
    "GOOGL",
    "META",
    "NVDA",
    "JPM",
    "V",
    "WMT",
    "COST",
    "RELIANCE.NS",
    "TCS.NS",
    "HDFCBANK.NS",
    "INFY.NS",
    "ITC.NS",
    "RY.TO",
    "TD.TO",
    "SHOP.TO",
    "ENB.TO",
    "CNR.TO",
)


@dataclass(frozen=True)
class HttpEvent:
    logical_request: int
    stage_rpm: int
    symbol: str
    method: str
    host: str
    path: str
    status_code: int
    elapsed_ms: float
    observed_at: str


@dataclass(frozen=True)
class ProbeResult:
    logical_request: int
    stage_rpm: int
    symbol: str
    started_at: str
    duration_ms: float
    final_status_code: int | None
    chart_status_codes: str
    yahoo_status_codes: str
    rows_returned: int
    outcome: str
    error_type: str
    error_message: str


class RecordingSession(curl_requests.Session):
    """curl-cffi session that records Yahoo responses made by yfinance."""

    def __init__(self) -> None:
        super().__init__(impersonate="chrome")
        self.events: list[HttpEvent] = []
        self.logical_request = 0
        self.stage_rpm = 0
        self.symbol = ""

    def request(self, method: str, url: str, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        started = time.perf_counter()
        response = super().request(method, url, *args, **kwargs)
        elapsed_ms = (time.perf_counter() - started) * 1_000
        parsed = urlsplit(str(response.url or url))
        if parsed.hostname and parsed.hostname.endswith("yahoo.com"):
            self.events.append(
                HttpEvent(
                    logical_request=self.logical_request,
                    stage_rpm=self.stage_rpm,
                    symbol=self.symbol,
                    method=method.upper(),
                    host=parsed.hostname,
                    path=parsed.path,
                    status_code=int(response.status_code),
                    elapsed_ms=round(elapsed_ms, 3),
                    observed_at=datetime.now(UTC).isoformat(),
                )
            )
        return response


def _parse_stages(value: str) -> tuple[int, ...]:
    try:
        stages = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("stages must be comma-separated integers") from exc
    if not stages or any(stage <= 0 for stage in stages):
        raise argparse.ArgumentTypeError("stages must contain positive integers")
    if any(stage > 60 for stage in stages):
        raise argparse.ArgumentTypeError("this bounded probe refuses stages above 60 RPM")
    if tuple(sorted(set(stages))) != stages:
        raise argparse.ArgumentTypeError("stages must be unique and increasing")
    return stages


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stages",
        type=_parse_stages,
        default=DEFAULT_STAGES,
        help="Increasing logical ticker requests/minute stages (default: 10,15,20,25,30).",
    )
    parser.add_argument(
        "--stage-seconds",
        type=float,
        default=60.0,
        help="Duration of each stage; keep 60 for a real RPM test (default: 60).",
    )
    parser.add_argument(
        "--universes",
        default="nse,us,tsx",
        help="Comma-separated symbol sources: nse, us, tsx (default: all three).",
    )
    parser.add_argument(
        "--period",
        default="5d",
        choices=("1d", "5d", "1mo"),
        help="Small daily-history window used by each request (default: 5d).",
    )
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--seed", type=int, default=20260717)
    parser.add_argument(
        "--stop-on-non-200",
        action="store_true",
        help="Stop on the first non-200 final status; HTTP 429 always stops the probe.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("tmp/yfinance_rate_probe"),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the request plan without contacting Yahoo.",
    )
    return parser


def _requested_count(rpm: int, stage_seconds: float) -> int:
    return max(1, math.ceil(rpm * stage_seconds / 60.0))


def _load_symbols(universes: str, required: int, seed: int) -> list[str]:
    requested = {item.strip().lower() for item in universes.split(",") if item.strip()}
    unsupported = requested - {"nse", "us", "tsx"}
    if unsupported:
        raise ValueError(f"unsupported universes: {', '.join(sorted(unsupported))}")

    symbols: list[str] = []
    providers = {
        "nse": NSEUniverseProvider,
        "us": YFinanceUSUniverseProvider,
        "tsx": YFinanceCanadaUniverseProvider,
    }
    for universe in ("nse", "us", "tsx"):
        if universe not in requested:
            continue
        try:
            symbols.extend(
                item.yahoo_symbol
                for item in providers[universe]().fetch()
                if item.yahoo_symbol
            )
        except Exception as exc:
            print(
                f"warning: could not load {universe} universe ({type(exc).__name__}: {exc})",
                file=sys.stderr,
            )

    symbols = list(dict.fromkeys(symbols))
    if len(symbols) < required:
        symbols.extend(item for item in FALLBACK_SYMBOLS if item not in symbols)
    if not symbols:
        raise RuntimeError("no probe symbols are available")

    random.Random(seed).shuffle(symbols)
    if len(symbols) >= required:
        return symbols[:required]
    return [symbols[index % len(symbols)] for index in range(required)]


def _final_status(events: list[HttpEvent]) -> int | None:
    chart = [event.status_code for event in events if "/v8/finance/chart/" in event.path]
    if chart:
        return chart[-1]
    return events[-1].status_code if events else None


def _status_text(events: list[HttpEvent], *, chart_only: bool) -> str:
    statuses = [
        str(event.status_code)
        for event in events
        if not chart_only or "/v8/finance/chart/" in event.path
    ]
    return ",".join(statuses)


def _run_request(
    session: RecordingSession,
    symbol: str,
    logical_request: int,
    stage_rpm: int,
    period: str,
    timeout: float,
) -> ProbeResult:
    session.logical_request = logical_request
    session.stage_rpm = stage_rpm
    session.symbol = symbol
    event_start = len(session.events)
    started_at = datetime.now(UTC).isoformat()
    started = time.perf_counter()
    rows_returned = 0
    error: Exception | None = None
    try:
        frame = yf.Ticker(symbol, session=session).history(
            period=period,
            interval="1d",
            actions=False,
            auto_adjust=False,
            repair=False,
            timeout=timeout,
        )
        rows_returned = len(frame)
    except Exception as exc:  # diagnostic must persist the provider response
        error = exc

    duration_ms = (time.perf_counter() - started) * 1_000
    events = session.events[event_start:]
    status = _final_status(events)
    if isinstance(error, YFRateLimitError) or any(event.status_code == 429 for event in events):
        outcome = "rate_limited"
        status = 429
    elif error is not None:
        outcome = "error"
    elif status == 200 and rows_returned > 0:
        outcome = "success"
    elif status == 200:
        outcome = "empty"
    elif status is None:
        outcome = "no_status"
    else:
        outcome = "http_error"

    return ProbeResult(
        logical_request=logical_request,
        stage_rpm=stage_rpm,
        symbol=symbol,
        started_at=started_at,
        duration_ms=round(duration_ms, 3),
        final_status_code=status,
        chart_status_codes=_status_text(events, chart_only=True),
        yahoo_status_codes=_status_text(events, chart_only=False),
        rows_returned=rows_returned,
        outcome=outcome,
        error_type=type(error).__name__ if error is not None else "",
        error_message=str(error)[:500] if error is not None else "",
    )


def _write_csv(path: Path, rows: list[Any]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    dictionaries = [asdict(row) for row in rows]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(dictionaries[0]))
        writer.writeheader()
        writer.writerows(dictionaries)


def main() -> int:
    args = _parser().parse_args()
    if args.stage_seconds <= 0 or args.stage_seconds > 300:
        raise SystemExit("--stage-seconds must be greater than 0 and at most 300")
    if args.timeout <= 0 or args.timeout > 60:
        raise SystemExit("--timeout must be greater than 0 and at most 60")

    # Surface yfinance provider exceptions instead of silently returning empty frames.
    yf.config.debug.hide_exceptions = False

    counts = [_requested_count(rpm, args.stage_seconds) for rpm in args.stages]
    total_planned = sum(counts)
    print(
        json.dumps(
            {
                "yfinance_version": yf.__version__,
                "stages_rpm": args.stages,
                "stage_seconds": args.stage_seconds,
                "requests_per_stage": counts,
                "total_planned": total_planned,
                "stop_on_non_200": args.stop_on_non_200,
            },
            indent=2,
        )
    )
    if args.dry_run:
        return 0

    symbols = _load_symbols(args.universes, total_planned, args.seed)
    session = RecordingSession()
    results: list[ProbeResult] = []
    stop_reason = "completed"
    symbol_index = 0

    for stage_index, (rpm, request_count) in enumerate(zip(args.stages, counts, strict=True)):
        stage_started = time.monotonic()
        spacing_seconds = 60.0 / rpm
        print(f"stage_start rpm={rpm} requests={request_count} spacing={spacing_seconds:.3f}s")
        for request_index in range(request_count):
            scheduled = stage_started + (request_index * spacing_seconds)
            delay = scheduled - time.monotonic()
            if delay > 0:
                time.sleep(delay)

            result = _run_request(
                session=session,
                symbol=symbols[symbol_index],
                logical_request=len(results) + 1,
                stage_rpm=rpm,
                period=args.period,
                timeout=args.timeout,
            )
            symbol_index += 1
            results.append(result)
            print(
                f"request={result.logical_request} rpm={rpm} symbol={result.symbol} "
                f"status={result.final_status_code} outcome={result.outcome} "
                f"rows={result.rows_returned} duration_ms={result.duration_ms:.0f}",
                flush=True,
            )

            if result.outcome == "rate_limited":
                stop_reason = f"rate_limited_at_{rpm}_rpm"
                break
            if result.final_status_code != 200 and args.stop_on_non_200:
                stop_reason = f"non_200_at_{rpm}_rpm"
                break

        if stop_reason != "completed":
            break
        if stage_index < len(args.stages) - 1:
            remaining = (stage_started + args.stage_seconds) - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_dir / timestamp
    _write_csv(output_dir / "logical_requests.csv", results)
    _write_csv(output_dir / "http_events.csv", session.events)

    successful_rpms = sorted(
        {
            result.stage_rpm
            for result in results
            if result.final_status_code == 200 and result.outcome == "success"
        }
    )
    stage_summaries = {}
    for rpm, expected in zip(args.stages, counts, strict=True):
        stage_results = [result for result in results if result.stage_rpm == rpm]
        if not stage_results:
            continue
        stage_summaries[str(rpm)] = {
            "planned": expected,
            "started": len(stage_results),
            "outcomes": dict(Counter(result.outcome for result in stage_results)),
            "final_statuses": dict(
                Counter(result.final_status_code for result in stage_results)
            ),
            "rate_limited": any(result.outcome == "rate_limited" for result in stage_results),
        }
    summary = {
        "started_requests": len(results),
        "planned_requests": total_planned,
        "stop_reason": stop_reason,
        "highest_rpm_with_a_200": max(successful_rpms, default=None),
        "highest_rpm_without_429": max(
            (
                rpm
                for rpm in args.stages
                if any(result.stage_rpm == rpm for result in results)
                and not any(
                    result.stage_rpm == rpm and result.outcome == "rate_limited"
                    for result in results
                )
            ),
            default=None,
        ),
        "highest_fully_completed_rpm": max(
            (
                rpm
                for rpm, expected in zip(args.stages, counts, strict=True)
                if sum(1 for result in results if result.stage_rpm == rpm) == expected
                and all(
                    result.final_status_code == 200
                    for result in results
                    if result.stage_rpm == rpm
                )
            ),
            default=None,
        ),
        "logical_outcomes": dict(Counter(result.outcome for result in results)),
        "logical_final_statuses": dict(Counter(result.final_status_code for result in results)),
        "all_yahoo_http_statuses": dict(Counter(event.status_code for event in session.events)),
        "stages": stage_summaries,
        "artifacts": {
            "logical_requests": str(output_dir / "logical_requests.csv"),
            "http_events": str(output_dir / "http_events.csv"),
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0 if stop_reason == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
