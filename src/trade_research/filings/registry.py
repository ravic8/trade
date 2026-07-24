from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from trade_research.filings.models import (
    ConsolidationScope,
    FilingDocument,
    ManifestImportResponse,
)
from trade_research.filings.store import FilingStore, stable_id

_REVISION_WORDS = re.compile(
    r"\b(original|revised|revision|new|clarification|replacement|resubmission)\b",
    re.IGNORECASE,
)
_NON_KEY = re.compile(r"[^a-z0-9]+")


def import_manifest(
    store: FilingStore,
    *,
    manifest_path: Path,
    workspace_id: str,
    verify_hashes: bool = True,
) -> ManifestImportResponse:
    manifest_path = manifest_path.expanduser().resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    symbol = str(payload["symbol"]).strip().upper()
    company_name = str(payload["company"]).strip()
    company_id = f"NSE:{symbol}"
    pack_root = manifest_path.parent.resolve()
    registered = 0
    existing = 0
    skipped_failed = 0
    superseded = 0
    filing_ids: list[str] = []

    for item in payload.get("documents", []):
        if item.get("acquisition_status") == "failed" or item.get("error"):
            skipped_failed += 1
            continue
        relative_path = str(item["relative_path"])
        source_path = (pack_root / relative_path).resolve()
        if not source_path.is_relative_to(pack_root):
            raise ValueError(f"manifest path escapes filing pack: {relative_path}")
        if not source_path.is_file():
            raise FileNotFoundError(source_path)
        expected_hash = str(item["sha256"]).lower()
        if verify_hashes:
            actual_hash = sha256_file(source_path)
            if actual_hash != expected_hash:
                raise ValueError(
                    f"source hash mismatch for {relative_path}: "
                    f"expected {expected_hash}, got {actual_hash}"
                )
        byte_size = source_path.stat().st_size
        expected_bytes = int(item.get("bytes") or byte_size)
        if expected_bytes != byte_size:
            raise ValueError(
                f"source size mismatch for {relative_path}: "
                f"expected {expected_bytes}, got {byte_size}"
            )

        categories = sorted({str(value) for value in item.get("categories", [])})
        titles = [str(value).strip() for value in item.get("titles", []) if str(value).strip()]
        title = titles[0] if titles else None
        period_end = _parse_date(item.get("period_end"))
        filing_date = _parse_datetime(item.get("filing_date"))
        scope = normalize_scope(item.get("scope"))
        document_key = build_document_key(
            company_id=company_id,
            categories=categories,
            title=title,
            period_end=period_end,
            scope=scope,
            filename=str(item["filename"]),
        )
        filing_id = stable_id("filing-document", workspace_id, company_id, expected_hash)
        document = FilingDocument(
            filing_id=filing_id,
            workspace_id=workspace_id,
            company_id=company_id,
            symbol=symbol,
            company_name=company_name,
            categories=categories,
            title=title,
            source_url=str(item["url"]),
            source_apis=[str(value) for value in item.get("source_apis", [])],
            filing_date=filing_date,
            period_end=period_end,
            consolidation_scope=scope,
            audited=_parse_audited(item.get("audited")),
            submission_type=item.get("submission_type"),
            relative_path=relative_path,
            object_uri=source_path.as_uri(),
            filename=str(item["filename"]),
            byte_size=byte_size,
            sha256=expected_hash,
            content_type=str(item.get("detected_content_type") or _content_type(source_path)),
            document_key=document_key,
            source_metadata=list(item.get("source_metadata") or []),
        )
        persisted, created, replaced = store.register_document(document)
        filing_ids.append(persisted.filing_id)
        if created:
            registered += 1
        else:
            existing += 1
        if replaced:
            superseded += 1

    return ManifestImportResponse(
        workspace_id=workspace_id,
        company_id=company_id,
        registered=registered,
        existing=existing,
        skipped_failed=skipped_failed,
        superseded=superseded,
        filing_ids=filing_ids,
    )


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_scope(value: Any) -> ConsolidationScope:
    normalized = str(value or "").strip().lower()
    if normalized in {"consolidated", "group"}:
        return ConsolidationScope.CONSOLIDATED
    if normalized in {"non-consolidated", "non consolidated", "standalone", "stand-alone"}:
        return ConsolidationScope.STANDALONE
    return ConsolidationScope.UNKNOWN


def build_document_key(
    *,
    company_id: str,
    categories: list[str],
    title: str | None,
    period_end: date | None,
    scope: ConsolidationScope,
    filename: str,
) -> str:
    category = categories[0] if categories else "uncategorized"
    normalized_title = _REVISION_WORDS.sub("", title or "")
    normalized_title = _NON_KEY.sub("-", normalized_title.lower()).strip("-")
    if not normalized_title:
        normalized_title = _NON_KEY.sub("-", Path(filename).stem.lower()).strip("-")
    period_key = period_end.isoformat() if period_end else "undated"
    return "|".join(
        [
            company_id,
            category,
            period_key,
            scope.value,
            normalized_title[:180],
        ]
    )


def _parse_date(value: Any) -> date | None:
    normalized = str(value or "").strip()
    if not normalized or normalized in {"-", "—", "N/A", "NA"}:
        return None
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError:
        pass
    for pattern in ("%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"unsupported filing date: {normalized}")


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value))


def _parse_audited(value: Any) -> bool | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"true", "yes", "audited"}:
        return True
    if normalized in {"false", "no", "unaudited"}:
        return False
    return None


def _content_type(path: Path) -> str:
    return "application/pdf" if path.suffix.lower() == ".pdf" else "application/xml"
