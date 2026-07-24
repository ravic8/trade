#!/usr/bin/env python3
"""Fetch an evidence-grade NSE filing pack for one listed company.

The script uses NSE's public corporate-filings endpoints, downloads original
attachments from nsearchives.nseindia.com, and writes JSON/CSV manifests with
hashes and source metadata. It is intentionally dependency-free.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

NSE_BASE = "https://www.nseindia.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
)


def run_curl(args: list[str]) -> None:
    subprocess.run(
        ["curl", "--http1.1", "--retry", "4", "--retry-all-errors", *args],
        check=True,
    )


def bootstrap_cookie(cookie_path: Path) -> None:
    run_curl(
        [
            "-c",
            str(cookie_path),
            "-A",
            USER_AGENT,
            "-H",
            "Accept-Language: en-US,en;q=0.9",
            "--silent",
            "--show-error",
            f"{NSE_BASE}/",
            "-o",
            "/dev/null",
        ]
    )


def fetch_json(url: str, cookie_path: Path) -> Any:
    with tempfile.NamedTemporaryFile(suffix=".json") as tmp:
        run_curl(
            [
                "-b",
                str(cookie_path),
                "-A",
                USER_AGENT,
                "-H",
                "Accept: application/json,text/plain,*/*",
                "-H",
                f"Referer: {NSE_BASE}/companies-listing/corporate-filings-announcements",
                "--fail",
                "--location",
                "--silent",
                "--show-error",
                url,
                "-o",
                tmp.name,
            ]
        )
        return json.loads(Path(tmp.name).read_text(encoding="utf-8"))


def safe_filename(url: str) -> str:
    raw = Path(urlparse(url).path).name or "attachment"
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._")
    return name or "attachment"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sniff_type(path: Path) -> str:
    prefix = path.read_bytes()[:32].lstrip()
    if prefix.startswith(b"%PDF"):
        return "application/pdf"
    if prefix.startswith(b"<?xml") or prefix.startswith(b"<xbrl"):
        return "application/xml"
    if prefix.lower().startswith((b"<!doctype html", b"<html")):
        return "text/html"
    return "application/octet-stream"


def is_valid_url(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("https://")
        and not value.endswith("/null")
        and value != "-"
    )


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    for fmt in (
        "%d-%b-%Y %H:%M:%S",
        "%d-%b-%Y %H:%M",
        "%d-%b-%Y",
        "%d-%m-%Y",
    ):
        try:
            return datetime.strptime(value.strip(), fmt).isoformat()
        except ValueError:
            pass
    return value


def add_candidate(
    candidates: dict[str, dict[str, Any]],
    *,
    url: Any,
    category: str,
    title: str,
    source_api: str,
    filing_date: str | None = None,
    period_end: str | None = None,
    scope: str | None = None,
    audited: str | None = None,
    submission_type: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    if not is_valid_url(url):
        return
    item = candidates.setdefault(
        url,
        {
            "url": url,
            "categories": set(),
            "titles": [],
            "source_apis": [],
            "filing_date": parse_date(filing_date),
            "period_end": period_end,
            "scope": scope,
            "audited": audited,
            "submission_type": submission_type,
            "metadata": [],
        },
    )
    item["categories"].add(category)
    if title and title not in item["titles"]:
        item["titles"].append(title)
    if source_api not in item["source_apis"]:
        item["source_apis"].append(source_api)
    if metadata and metadata not in item["metadata"]:
        item["metadata"].append(metadata)
    for key, value in (
        ("filing_date", parse_date(filing_date)),
        ("period_end", period_end),
        ("scope", scope),
        ("audited", audited),
        ("submission_type", submission_type),
    ):
        if not item.get(key) and value:
            item[key] = value


def announcement_categories(row: dict[str, Any]) -> list[str]:
    desc = (row.get("desc") or "").lower()
    text = (row.get("attchmntText") or "").lower()
    categories: list[str] = []

    financial_text = re.search(
        r"financial result|financial statement|period ended|quarter ended", text
    )
    if (
        "financial result" in desc
        or "integrated filing- financial" in desc
        or "outcome of board meeting" in desc
        or ("auditor" in desc and financial_text)
        or ("clarification" in desc and "financial result" in desc)
    ):
        categories.append("quarterly_results")

    if "auditor" in text and ("financial statement" in text or "quarter ended" in text):
        categories.append("audit_reports")

    if re.search(
        r"earnings call transcript|transcripts? of the press conference and earnings call",
        text,
    ):
        categories.append("earnings_transcripts")

    if (
        re.search(r"\bagm\b|annual general meeting|postal ballot", text)
        and (
            "shareholders meeting" in desc
            or "transcript" in text
            or "notice" in text
            or "voting result" in text
            or "scrutinizer" in text
        )
    ):
        categories.append("shareholder_meetings")

    if "copy of newspaper publication" in desc and financial_text:
        categories.append("result_advertisements")

    return categories


def choose_directory(categories: set[str]) -> str:
    priority = (
        "annual_reports",
        "quarterly_results",
        "audit_reports",
        "xbrl_financial",
        "ixbrl_financial",
        "earnings_transcripts",
        "shareholder_meetings",
        "voting_results",
        "xbrl_voting",
        "xbrl_governance",
        "ixbrl_governance",
        "result_advertisements",
    )
    for category in priority:
        if category in categories:
            return category
    return sorted(categories)[0]


def download_attachment(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".part")
    run_curl(
        [
            "-A",
            USER_AGENT,
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            url,
            "-o",
            str(temp),
        ]
    )
    temp.replace(destination)


def build_pack(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output).resolve()
    metadata_dir = root / "metadata"
    documents_dir = root / "documents"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    documents_dir.mkdir(parents=True, exist_ok=True)

    endpoints = {
        "annual_reports": (
            f"{NSE_BASE}/api/annual-reports?index=equities&symbol={args.symbol}"
        ),
        "announcements": (
            f"{NSE_BASE}/api/corporate-announcements?index=equities"
            f"&symbol={args.symbol}&from_date={args.from_date}&to_date={args.to_date}"
        ),
        "legacy_financial_xbrl": (
            f"{NSE_BASE}/api/corporates-financial-results?index=equities"
            f"&period=Quarterly&symbol={args.symbol}"
        ),
        "integrated_filings": (
            f"{NSE_BASE}/api/integrated-filing-results?index=equities"
            f"&symbol={args.symbol}&from_date={args.from_date}&to_date={args.to_date}"
            "&page=1&size=100"
        ),
        "postal_ballot": (
            f"{NSE_BASE}/api/postal-ballot?index=equities"
            f"&symbol={args.symbol}&from_date={args.from_date}&to_date={args.to_date}"
        ),
        "voting_results": (
            f"{NSE_BASE}/api/corporate-voting-results?index=equities"
            f"&symbol={args.symbol}&from_date={args.from_date}&to_date={args.to_date}"
        ),
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        cookie_path = Path(temp_dir) / "nse.cookies"
        bootstrap_cookie(cookie_path)
        payloads = {
            name: fetch_json(url, cookie_path) for name, url in endpoints.items()
        }

    for name, payload in payloads.items():
        (metadata_dir / f"{name}.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    candidates: dict[str, dict[str, Any]] = {}

    for row in payloads["annual_reports"].get("data", []):
        from_year = int(row.get("fromYr") or 0)
        if args.first_fiscal_year <= from_year <= args.last_fiscal_year:
            add_candidate(
                candidates,
                url=row.get("fileName"),
                category="annual_reports",
                title=(
                    f"Annual report FY{row.get('fromYr')}-{row.get('toYr')} "
                    f"({row.get('submission_type')})"
                ),
                source_api=endpoints["annual_reports"],
                filing_date=row.get("broadcast_dttm"),
                period_end=f"{row.get('toYr')}-03-31",
                submission_type=row.get("submission_type"),
                metadata=row,
            )

    for row in payloads["announcements"]:
        for category in announcement_categories(row):
            add_candidate(
                candidates,
                url=row.get("attchmntFile"),
                category=category,
                title=row.get("attchmntText") or row.get("desc") or category,
                source_api=endpoints["announcements"],
                filing_date=row.get("an_dt"),
                metadata=row,
            )

    range_start = datetime.strptime(args.from_date, "%d-%m-%Y").date()
    range_end = datetime.strptime(args.to_date, "%d-%m-%Y").date()
    for row in payloads["legacy_financial_xbrl"]:
        try:
            period_end = datetime.strptime(row["toDate"], "%d-%b-%Y").date()
        except (KeyError, TypeError, ValueError):
            continue
        if range_start <= period_end <= range_end:
            add_candidate(
                candidates,
                url=row.get("xbrl"),
                category="xbrl_financial",
                title=(
                    f"{row.get('relatingTo')} financial XBRL "
                    f"({row.get('consolidated')})"
                ),
                source_api=endpoints["legacy_financial_xbrl"],
                filing_date=row.get("broadCastDate"),
                period_end=period_end.isoformat(),
                scope=row.get("consolidated"),
                audited=row.get("audited"),
                submission_type=("Revised" if row.get("reInd") == "Y" else "Original"),
                metadata=row,
            )

    for row in payloads["integrated_filings"].get("data", []):
        filing_type = row.get("type") or ""
        if "Financial" in filing_type:
            xbrl_category = "xbrl_financial"
            ixbrl_category = "ixbrl_financial"
        elif "Governance" in filing_type:
            xbrl_category = "xbrl_governance"
            ixbrl_category = "ixbrl_governance"
        else:
            continue
        common = {
            "title": f"{filing_type} {row.get('qe_Date') or ''}".strip(),
            "source_api": endpoints["integrated_filings"],
            "filing_date": row.get("broadcast_Date"),
            "period_end": row.get("qe_Date"),
            "scope": row.get("consolidated"),
            "audited": row.get("audited"),
            "submission_type": row.get("type_Sub"),
            "metadata": row,
        }
        add_candidate(candidates, url=row.get("xbrl"), category=xbrl_category, **common)
        if args.include_ixbrl:
            add_candidate(
                candidates, url=row.get("ixbrl"), category=ixbrl_category, **common
            )

    for row in payloads["postal_ballot"].get("data", []):
        add_candidate(
            candidates,
            url=row.get("attachment"),
            category="shareholder_meetings",
            title=row.get("text") or f"{row.get('type')} shareholder meeting",
            source_api=endpoints["postal_ballot"],
            filing_date=row.get("date"),
            period_end=row.get("bdt"),
            metadata=row,
        )

    for item in payloads["voting_results"]:
        row = item.get("metadata") or {}
        common = {
            "title": f"{row.get('vrMeetingType')} voting results",
            "source_api": endpoints["voting_results"],
            "filing_date": row.get("vrSystemDt") or row.get("vrRevisedDate"),
            "period_end": row.get("vrTimestamp"),
            "submission_type": row.get("vrTypeOfSubmission"),
            "metadata": row,
        }
        add_candidate(
            candidates,
            url=row.get("vrAttachment"),
            category="voting_results",
            **common,
        )
        add_candidate(
            candidates,
            url=row.get("vrXbrlFilename"),
            category="xbrl_voting",
            **common,
        )

    records: list[dict[str, Any]] = []
    for url, item in sorted(candidates.items()):
        categories = set(item["categories"])
        directory = choose_directory(categories)
        filename = safe_filename(url)
        destination = documents_dir / directory / filename
        error = None
        if destination.exists() and destination.stat().st_size > 0:
            acquisition_status = "existing"
        else:
            try:
                download_attachment(url, destination)
                acquisition_status = "downloaded"
            except subprocess.CalledProcessError as exc:
                acquisition_status = "failed"
                error = f"curl exit code {exc.returncode}"
                partial = destination.with_suffix(destination.suffix + ".part")
                if partial.exists():
                    partial.unlink()

        record = {
                "symbol": args.symbol,
                "company": args.company,
                "url": url,
                "categories": sorted(categories),
                "titles": item["titles"],
                "source_apis": item["source_apis"],
                "filing_date": item.get("filing_date"),
                "period_end": item.get("period_end"),
                "scope": item.get("scope"),
                "audited": item.get("audited"),
                "submission_type": item.get("submission_type"),
                "relative_path": str(destination.relative_to(root)),
                "filename": filename,
                "bytes": destination.stat().st_size if destination.exists() else None,
                "sha256": sha256_file(destination) if destination.exists() else None,
                "detected_content_type": (
                    sniff_type(destination) if destination.exists() else None
                ),
                "acquisition_status": acquisition_status,
                "error": error,
                "source_metadata": item["metadata"],
            }
        records.append(record)

    acquired_records = [
        record for record in records if record["acquisition_status"] != "failed"
    ]
    failed_records = [
        record for record in records if record["acquisition_status"] == "failed"
    ]
    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now().astimezone().isoformat(),
        "company": args.company,
        "symbol": args.symbol,
        "isin": args.isin,
        "coverage": {
            "announcement_from_date": args.from_date,
            "announcement_to_date": args.to_date,
            "fiscal_years": [
                f"{year}-{year + 1}"
                for year in range(args.first_fiscal_year, args.last_fiscal_year + 1)
            ],
            "includes_latest_partial_year": True,
        },
        "source": "National Stock Exchange of India",
        "source_endpoints": endpoints,
        "candidate_count": len(records),
        "document_count": len(acquired_records),
        "failed_download_count": len(failed_records),
        "category_counts": dict(
            sorted(
                Counter(c for r in acquired_records for c in r["categories"]).items()
            )
        ),
        "total_bytes": sum(r["bytes"] for r in acquired_records),
        "documents": records,
    }
    (root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    columns = (
        "symbol",
        "filing_date",
        "period_end",
        "categories",
        "scope",
        "audited",
        "submission_type",
        "relative_path",
        "bytes",
        "sha256",
        "detected_content_type",
        "url",
    )
    with (root / "manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for record in records:
            row = {key: record.get(key) for key in columns}
            row["categories"] = "|".join(record["categories"])
            writer.writerow(row)

    (root / "coverage.json").write_text(
        json.dumps(
            {
                "document_count": manifest["document_count"],
                "candidate_count": manifest["candidate_count"],
                "category_counts": manifest["category_counts"],
                "total_bytes": manifest["total_bytes"],
                "total_megabytes": round(manifest["total_bytes"] / 1024 / 1024, 2),
                "failed_downloads": manifest["failed_download_count"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="INFY")
    parser.add_argument("--company", default="Infosys Limited")
    parser.add_argument("--isin", default="INE009A01021")
    parser.add_argument("--from-date", default="01-04-2023")
    parser.add_argument("--to-date", default="24-07-2026")
    parser.add_argument("--first-fiscal-year", type=int, default=2023)
    parser.add_argument("--last-fiscal-year", type=int, default=2025)
    parser.add_argument(
        "--output",
        default="data/filings/nse/INFY",
    )
    parser.add_argument(
        "--include-ixbrl",
        action="store_true",
        help="Also download rendered iXBRL HTML alongside source XBRL XML.",
    )
    args = parser.parse_args()
    manifest = build_pack(args)
    print(
        json.dumps(
            {
                "output": str(Path(args.output).resolve()),
                "document_count": manifest["document_count"],
                "category_counts": manifest["category_counts"],
                "total_megabytes": round(manifest["total_bytes"] / 1024 / 1024, 2),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
