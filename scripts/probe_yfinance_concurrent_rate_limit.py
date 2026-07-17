#!/usr/bin/env python3
"""Paced concurrent yfinance probe that stops when Yahoo returns HTTP 429."""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import threading
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import yfinance as yf
from probe_yfinance_rate_limit import (
    HttpEvent,
    ProbeResult,
    RecordingSession,
    _load_symbols,
    _run_request,
)


def _parse_stages(value: str) -> tuple[int, ...]:
    try:
        stages = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("stages must be comma-separated integers") from exc
    if not stages or any(stage <= 0 or stage > 1_200 for stage in stages):
        raise argparse.ArgumentTypeError("stages must be between 1 and 1,200 RPM")
    if tuple(sorted(set(stages))) != stages:
        raise argparse.ArgumentTypeError("stages must be unique and increasing")
    return stages


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stages", type=_parse_stages, required=True)
    parser.add_argument("--stage-seconds", type=float, default=60.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--universes", default="us")
    parser.add_argument("--period", choices=("1d", "5d", "1mo"), default="5d")
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--seed", type=int, default=20260719)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("tmp/yfinance_rate_probe")
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _write_csv(path: Path, rows: list[ProbeResult] | list[HttpEvent]) -> None:
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
    if not 10 <= args.stage_seconds <= 300:
        raise SystemExit("--stage-seconds must be between 10 and 300")
    if not 1 <= args.workers <= 32:
        raise SystemExit("--workers must be between 1 and 32")
    if not 0 < args.timeout <= 60:
        raise SystemExit("--timeout must be greater than 0 and at most 60")

    yf.config.debug.hide_exceptions = False
    counts = [math.ceil(rpm * args.stage_seconds / 60) for rpm in args.stages]
    plan = {
        "yfinance_version": yf.__version__,
        "stages_rpm": args.stages,
        "stage_seconds": args.stage_seconds,
        "workers": args.workers,
        "requests_per_stage": counts,
        "total_planned": sum(counts),
    }
    print(json.dumps(plan, indent=2), flush=True)
    if args.dry_run:
        return 0

    symbols = _load_symbols(args.universes, sum(counts), args.seed)
    # Avoid known provider-normalization failures for preferred-share symbols.
    symbols = [symbol for symbol in symbols if "$" not in symbol]
    if len(symbols) < sum(counts):
        symbols = [symbols[index % len(symbols)] for index in range(sum(counts))]
    random.Random(args.seed).shuffle(symbols)

    local = threading.local()
    sessions: list[RecordingSession] = []
    sessions_lock = threading.Lock()
    rate_limited = threading.Event()

    def execute(
        symbol: str, logical_request: int, rpm: int
    ) -> tuple[ProbeResult, list[HttpEvent]]:
        session = getattr(local, "session", None)
        if session is None:
            session = RecordingSession()
            local.session = session
            with sessions_lock:
                sessions.append(session)
        event_start = len(session.events)
        result = _run_request(
            session, symbol, logical_request, rpm, args.period, args.timeout
        )
        if result.outcome == "rate_limited":
            rate_limited.set()
        return result, session.events[event_start:]

    results: list[ProbeResult] = []
    events: list[HttpEvent] = []
    futures: list[Future[tuple[ProbeResult, list[HttpEvent]]]] = []
    submitted = 0
    stop_reason = "completed"
    probe_started = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for stage_index, (rpm, count) in enumerate(zip(args.stages, counts, strict=True)):
            stage_started = time.monotonic()
            spacing = 60 / rpm
            print(
                f"stage_start rpm={rpm} requests={count} spacing={spacing:.4f}s",
                flush=True,
            )
            for request_index in range(count):
                if rate_limited.is_set():
                    stop_reason = f"rate_limited_at_{rpm}_rpm"
                    break
                scheduled = stage_started + request_index * spacing
                delay = scheduled - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                submitted += 1
                futures.append(
                    executor.submit(execute, symbols[submitted - 1], submitted, rpm)
                )
            if rate_limited.is_set():
                stop_reason = f"rate_limited_at_{rpm}_rpm"
                break
            if stage_index < len(args.stages) - 1:
                remaining = stage_started + args.stage_seconds - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)

        for future in as_completed(futures):
            result, request_events = future.result()
            results.append(result)
            events.extend(request_events)

    results.sort(key=lambda result: result.logical_request)
    events.sort(key=lambda event: event.observed_at)
    if rate_limited.is_set() and stop_reason == "completed":
        limited_rpms = [r.stage_rpm for r in results if r.outcome == "rate_limited"]
        stop_reason = f"rate_limited_at_{min(limited_rpms)}_rpm"

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_dir / f"concurrent-{timestamp}"
    _write_csv(output_dir / "logical_requests.csv", results)
    _write_csv(output_dir / "http_events.csv", events)

    stage_summaries: dict[str, object] = {}
    for rpm, planned in zip(args.stages, counts, strict=True):
        selected = [result for result in results if result.stage_rpm == rpm]
        if selected:
            stage_summaries[str(rpm)] = {
                "planned": planned,
                "completed": len(selected),
                "outcomes": dict(Counter(result.outcome for result in selected)),
                "final_statuses": dict(
                    Counter(result.final_status_code for result in selected)
                ),
            }

    summary = {
        **plan,
        "elapsed_seconds": round(time.monotonic() - probe_started, 3),
        "submitted_requests": submitted,
        "completed_requests": len(results),
        "stop_reason": stop_reason,
        "logical_outcomes": dict(Counter(result.outcome for result in results)),
        "logical_final_statuses": dict(
            Counter(result.final_status_code for result in results)
        ),
        "all_yahoo_http_statuses": dict(Counter(event.status_code for event in events)),
        "stages": stage_summaries,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2), flush=True)
    print(f"artifacts={output_dir}", flush=True)
    return 2 if rate_limited.is_set() else 0


if __name__ == "__main__":
    raise SystemExit(main())
