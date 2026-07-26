#!/usr/bin/env python3
"""Fetch and hash the official Nifty 50 constituent CSV."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import date, datetime
from io import StringIO
from pathlib import Path

import httpx

DEFAULT_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv"


def fetch_snapshot(*, url: str, effective_date: date) -> dict:
    response = httpx.get(
        url,
        timeout=30,
        follow_redirects=True,
        headers={
            "User-Agent": "trade-research/0.1 Nifty50 filing universe",
            "Accept": "text/csv,text/plain,*/*",
        },
    )
    response.raise_for_status()
    source_bytes = response.content
    frame = list(csv.DictReader(StringIO(response.text)))
    members = []
    seen: set[str] = set()
    for row in frame:
        symbol = str(row.get("Symbol") or "").strip().upper()
        name = str(row.get("Company Name") or "").strip()
        if not symbol or not name or symbol in seen:
            continue
        seen.add(symbol)
        members.append(
            {
                "company_id": f"NSE:{symbol}",
                "symbol": symbol,
                "name": name,
                "industry": str(row.get("Industry") or "").strip(),
                "series": str(row.get("Series") or "").strip(),
                "isin": str(row.get("ISIN Code") or "").strip(),
            }
        )
    if len(members) != 50:
        raise ValueError(
            f"official Nifty 50 snapshot must contain exactly 50 members; got {len(members)}"
        )
    return {
        "schema_version": 1,
        "universe_id": "NIFTY50",
        "effective_date": effective_date.isoformat(),
        "source_url": url,
        "source_hash": hashlib.sha256(source_bytes).hexdigest(),
        "member_count": len(members),
        "members": members,
        "fetched_at": datetime.now().astimezone().isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--effective-date", default=date.today().isoformat())
    parser.add_argument(
        "--output",
        default="data/filings/nse/NIFTY50/snapshot.json",
    )
    args = parser.parse_args()
    snapshot = fetch_snapshot(
        url=args.url,
        effective_date=date.fromisoformat(args.effective_date),
    )
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output": str(output),
                "member_count": snapshot["member_count"],
                "source_hash": snapshot["source_hash"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
