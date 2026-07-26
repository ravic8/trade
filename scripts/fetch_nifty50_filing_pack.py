#!/usr/bin/env python3
"""Fetch bounded quarterly XBRL packs for a versioned Nifty 50 snapshot."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        default="data/filings/nse/NIFTY50/snapshot.json",
    )
    parser.add_argument("--output-root", default="data/filings/nse")
    parser.add_argument("--quarters", type=int, default=5)
    parser.add_argument("--throttle-seconds", type=float, default=1.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--report-path")
    parser.add_argument("--continue-on-error", action="store_true")
    args = parser.parse_args()

    if not 2 <= args.quarters <= 12:
        raise ValueError("--quarters must be between 2 and 12")
    snapshot_path = Path(args.snapshot).expanduser().resolve()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    members = list(snapshot["members"])
    if args.limit:
        members = members[: args.limit]
    effective = date.fromisoformat(snapshot["effective_date"])
    from_date = effective - timedelta(days=args.quarters * 105)
    output_root = Path(args.output_root).expanduser().resolve()
    fetcher = Path(__file__).with_name("fetch_nse_filing_pack.py")
    results = []
    for index, member in enumerate(members, start=1):
        symbol = str(member["symbol"])
        command = [
            sys.executable,
            str(fetcher),
            "--symbol",
            symbol,
            "--company",
            str(member["name"]),
            "--isin",
            str(member.get("isin") or ""),
            "--from-date",
            from_date.strftime("%d-%m-%Y"),
            "--to-date",
            effective.strftime("%d-%m-%Y"),
            "--first-fiscal-year",
            str(from_date.year - 1),
            "--last-fiscal-year",
            str(effective.year),
            "--output",
            str(output_root / symbol),
            "--financial-only",
        ]
        print(f"[{index}/{len(members)}] fetching {symbol}", flush=True)
        completed = subprocess.run(command, check=False)
        results.append(
            {
                "symbol": symbol,
                "exit_code": completed.returncode,
                "manifest": str(output_root / symbol / "manifest.json"),
            }
        )
        if completed.returncode and not args.continue_on_error:
            raise SystemExit(completed.returncode)
        if index < len(members):
            time.sleep(max(args.throttle_seconds, 0))
    failed = [item for item in results if item["exit_code"]]
    report = {
        "schema_version": 1,
        "universe_id": snapshot["universe_id"],
        "snapshot_effective_date": snapshot["effective_date"],
        "snapshot_source_hash": snapshot["source_hash"],
        "quarters": args.quarters,
        "requested": len(results),
        "succeeded": len(results) - len(failed),
        "failed": failed,
        "results": results,
    }
    report_path = (
        Path(args.report_path).expanduser().resolve()
        if args.report_path
        else output_root
        / "NIFTY50"
        / f"acquisition-report-{str(snapshot['source_hash'])[:12]}.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2))
    print(f"Acquisition report: {report_path}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
