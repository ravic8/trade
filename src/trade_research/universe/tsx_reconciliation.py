from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from io import BytesIO
from typing import Any

import httpx
import pandas as pd
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from trade_research.schemas import Symbol
from trade_research.universe.base import UniverseProvider
from trade_research.universe.tsx import TSXUniverseProvider

DEFAULT_TMX_ISSUER_URL = "https://www.tsx.com/en/resource/571"
DEFAULT_TMX_DIRECTORY_BASE_URL = "https://www.tsx.com/json/company-directory"
RECONCILIATION_VERSION = "tsx-v1"

_EXCLUDED_SECTORS = {
    "CDR": ("cdr", "excluded_product_cdr"),
    "Closed-End Funds": ("closed_end_fund", "excluded_product_closed_end_fund"),
    "ETP": ("etp", "excluded_product_etp"),
    "SPAC": ("spac", "excluded_product_spac"),
}
_PREFERRED_SUFFIX = re.compile(r"^\.(?:P[A-Z]*|PR(?:\.[A-Z]+)?)$")
_DEBT_SUFFIX = re.compile(r"^\.DB[A-Z]*$")
_RIGHT_SUFFIX = re.compile(r"^\.(?:R|RT|RIGHTS?)$")
_WARRANT_SUFFIX = re.compile(r"^\.(?:W|WT|WTS?)$")
_COMMON_CLASS_SUFFIX = re.compile(r"^\.[A-TV-Z]$")


@dataclass(frozen=True)
class TMXIssuer:
    source_identity: str
    root_ticker: str
    name: str
    exchange: str
    sector: str | None
    sub_sector: str | None
    security_type: str | None
    listing_date: datetime | None


@dataclass(frozen=True)
class TMXDirectoryEntry:
    symbol: str
    name: str
    effective_at: datetime | None


@dataclass(frozen=True)
class TMXOfficialSnapshot:
    issuers: tuple[TMXIssuer, ...]
    recently_listed: dict[str, TMXDirectoryEntry]
    recently_delisted: dict[str, TMXDirectoryEntry]
    suspended: dict[str, TMXDirectoryEntry]
    checked_at: datetime
    source_updated_at: datetime | None
    invalid_issuer_rows: int = 0


class TMXOfficialDirectoryProvider:
    """Fetch the public TMX issuer workbook and lifecycle directory views."""

    def __init__(
        self,
        *,
        issuer_url: str = DEFAULT_TMX_ISSUER_URL,
        directory_base_url: str = DEFAULT_TMX_DIRECTORY_BASE_URL,
        timeout_seconds: float = 30.0,
        retry_attempts: int = 3,
    ) -> None:
        self.issuer_url = issuer_url
        self.directory_base_url = directory_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.retry_attempts = retry_attempts

    def fetch(self) -> TMXOfficialSnapshot:
        checked_at = datetime.now(UTC)
        with httpx.Client(
            timeout=self.timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": "trade-research/0.1 TSX universe reconciliation"},
        ) as client:
            workbook = self._get(client, self.issuer_url)
            recently_listed, recent_updated = self._directory_entries(client, "recent")
            recently_delisted, delisted_updated = self._directory_entries(client, "delisted")
            suspended, suspended_updated = self._directory_entries(client, "suspended")

        issuers, invalid_rows = _parse_issuer_workbook(workbook.content)
        workbook_updated = _http_last_modified(workbook.headers.get("last-modified"))
        source_dates = [
            value
            for value in (
                workbook_updated,
                recent_updated,
                delisted_updated,
                suspended_updated,
            )
            if value is not None
        ]
        return TMXOfficialSnapshot(
            issuers=tuple(issuers),
            recently_listed=recently_listed,
            recently_delisted=recently_delisted,
            suspended=suspended,
            checked_at=checked_at,
            source_updated_at=max(source_dates) if source_dates else None,
            invalid_issuer_rows=invalid_rows,
        )

    def _get(self, client: httpx.Client, url: str) -> httpx.Response:
        for attempt in Retrying(
            stop=stop_after_attempt(self.retry_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(httpx.HTTPError),
            reraise=True,
        ):
            with attempt:
                response = client.get(url)
                response.raise_for_status()
                return response
        raise RuntimeError("TMX request retry loop ended without a response")

    def _directory_entries(
        self,
        client: httpx.Client,
        action: str,
    ) -> tuple[dict[str, TMXDirectoryEntry], datetime | None]:
        response = self._get(client, f"{self.directory_base_url}/{action}/tsx")
        payload = response.json()
        if payload.get("isHttpError"):
            raise ValueError(f"TMX company directory returned an error for {action}.")
        results = payload.get("results")
        if not isinstance(results, list):
            raise ValueError(f"TMX company directory returned invalid {action} results.")
        entries: dict[str, TMXDirectoryEntry] = {}
        for row in results:
            if not isinstance(row, dict):
                continue
            symbol = _normalized_text(row.get("symbol"))
            if not symbol:
                continue
            entries[symbol] = TMXDirectoryEntry(
                symbol=symbol,
                name=_text(row.get("name")) or symbol,
                effective_at=_unix_datetime(row.get("date")),
            )
        return entries, _unix_datetime(payload.get("last_updated"))


class ReconciledTSXUniverseProvider(UniverseProvider):
    """Fail-closed intersection of Yahoo-formatted candidates and official TMX data."""

    exchange = "TSX"
    source = "tsx_tmx_reconciled"

    def __init__(
        self,
        *,
        candidate_provider: UniverseProvider | None = None,
        official_provider: TMXOfficialDirectoryProvider | None = None,
    ) -> None:
        self.candidate_provider = candidate_provider or TSXUniverseProvider()
        self.official_provider = official_provider or TMXOfficialDirectoryProvider()
        self.last_diagnostics: dict[str, Any] = {}

    def fetch(self) -> list[Symbol]:
        candidates = self.candidate_provider.fetch()
        official = self.official_provider.fetch()
        issuers_by_root = {item.root_ticker: item for item in official.issuers}
        candidate_roots = {_root_ticker(item.symbol) for item in candidates}
        delisted_roots = set(official.recently_delisted)

        reconciled = [
            _reconcile_symbol(
                item,
                official,
                issuers_by_root.get(_root_ticker(item.symbol)),
            ).model_copy(update={"source_url": self.official_provider.issuer_url})
            for item in candidates
        ]
        eligible_issuer_roots = {
            item.root_ticker
            for item in official.issuers
            if _issuer_product_eligible(item) and item.root_ticker not in delisted_roots
        }
        provider_unmapped = sorted(eligible_issuer_roots - candidate_roots)
        candidate_diagnostics = _candidate_diagnostics(self.candidate_provider)
        status_counts: dict[str, int] = {}
        type_counts: dict[str, int] = {}
        for item in reconciled:
            status_counts[item.reconciliation_status] = (
                status_counts.get(item.reconciliation_status, 0) + 1
            )
            type_counts[item.instrument_type] = type_counts.get(item.instrument_type, 0) + 1

        eligible_symbols = sum(item.pipeline_eligibility == "incremental" for item in reconciled)
        official_eligible = sum(_issuer_product_eligible(item) for item in official.issuers)
        self.last_diagnostics = {
            **candidate_diagnostics,
            "reconciliation_version": RECONCILIATION_VERSION,
            "official_source_url": self.official_provider.issuer_url,
            "official_source_updated_at": (
                official.source_updated_at.isoformat() if official.source_updated_at else None
            ),
            "official_rows": len(official.issuers),
            "official_invalid_rows": official.invalid_issuer_rows,
            "official_eligible_issuers": official_eligible,
            "official_excluded_product_issuers": len(official.issuers) - official_eligible,
            "candidate_symbols": len(candidates),
            "eligible_symbols": eligible_symbols,
            "excluded_symbols": len(reconciled) - eligible_symbols,
            "provider_unmapped_official_issuers": len(provider_unmapped),
            "provider_unmapped_sample": provider_unmapped[:20],
            "reconciliation_status_counts": status_counts,
            "instrument_type_counts": type_counts,
            "recently_listed_rows": len(official.recently_listed),
            "recently_delisted_rows": len(official.recently_delisted),
            "suspended_rows": len(official.suspended),
        }
        return sorted(reconciled, key=lambda item: item.symbol)

    def diagnostics(self) -> dict[str, Any]:
        return dict(self.last_diagnostics)


def classify_tsx_security(symbol: str, issuer: TMXIssuer) -> tuple[bool, str, str]:
    excluded = _EXCLUDED_SECTORS.get(issuer.sector or "")
    if excluded:
        instrument_type, reason = excluded
        return False, instrument_type, reason

    root = issuer.root_ticker
    suffix = symbol.removeprefix(root)
    if not suffix:
        return True, "common_equity", "eligible_common_equity"
    if suffix == ".UN":
        if issuer.sector == "Real Estate":
            return True, "reit_unit", "eligible_reit_unit"
        return False, "non_reit_unit", "excluded_non_reit_unit"
    if suffix == ".U":
        return False, "alternate_currency_unit", "excluded_alternate_currency_unit"
    if _PREFERRED_SUFFIX.fullmatch(suffix):
        return False, "preferred_share", "excluded_preferred_share"
    if _DEBT_SUFFIX.fullmatch(suffix):
        return False, "debt_security", "excluded_debt_security"
    if _RIGHT_SUFFIX.fullmatch(suffix):
        return False, "right", "excluded_right"
    if _WARRANT_SUFFIX.fullmatch(suffix):
        return False, "warrant", "excluded_warrant"
    if _COMMON_CLASS_SUFFIX.fullmatch(suffix):
        return True, "common_equity_class", "eligible_common_equity_class"
    return False, "unsupported_security", "excluded_unsupported_security_suffix"


def _reconcile_symbol(
    candidate: Symbol,
    official: TMXOfficialSnapshot,
    issuer: TMXIssuer | None,
) -> Symbol:
    symbol = candidate.symbol.strip().upper()
    root = _root_ticker(symbol)
    source_updated_at = official.source_updated_at or official.checked_at
    source_identity = None
    official_sector = None
    official_security_type = None
    name = candidate.name

    if issuer is not None:
        source_identity = _security_source_identity(issuer, symbol)
        official_sector = issuer.sector
        official_security_type = issuer.security_type
        name = issuer.name or name

    suspended = _directory_match(official.suspended, symbol, root)
    if suspended is not None:
        return _resolved_symbol(
            candidate,
            name=name or suspended.name,
            source_identity=source_identity,
            listing_status="halted",
            listing_status_reason="official_suspended",
            listing_status_effective_at=suspended.effective_at,
            instrument_type="suspended",
            reconciliation_status="official_suspended",
            reconciliation_reason="excluded_official_suspended",
            official_sector=official_sector,
            official_security_type=official_security_type,
            official_source_updated_at=source_updated_at,
        )

    delisted = _directory_match(official.recently_delisted, symbol, root)
    if delisted is not None:
        return _resolved_symbol(
            candidate,
            name=name or delisted.name,
            source_identity=source_identity,
            listing_status="delisted",
            listing_status_reason="official_recently_delisted",
            listing_status_effective_at=delisted.effective_at,
            instrument_type="delisted",
            reconciliation_status="official_delisted",
            reconciliation_reason="excluded_official_delisted",
            official_sector=official_sector,
            official_security_type=official_security_type,
            official_source_updated_at=source_updated_at,
        )

    if issuer is None:
        recent = _directory_match(official.recently_listed, symbol, root)
        status = "official_recent_unclassified" if recent else "candidate_only"
        reason = "awaiting_official_classification" if recent else "missing_from_official_current"
        return _resolved_symbol(
            candidate,
            name=name or (recent.name if recent else None),
            source_identity=None,
            listing_status="active",
            listing_status_reason=reason,
            listing_status_effective_at=recent.effective_at if recent else None,
            instrument_type="unclassified",
            reconciliation_status=status,
            reconciliation_reason=reason,
            official_sector=None,
            official_security_type=None,
            official_source_updated_at=source_updated_at,
        )

    eligible, instrument_type, reason = classify_tsx_security(symbol, issuer)
    return _resolved_symbol(
        candidate,
        name=name,
        source_identity=source_identity,
        listing_status="active",
        listing_status_reason=None,
        listing_status_effective_at=issuer.listing_date,
        instrument_type=instrument_type,
        reconciliation_status="official_eligible" if eligible else "official_excluded",
        reconciliation_reason=reason,
        official_sector=official_sector,
        official_security_type=official_security_type,
        official_source_updated_at=source_updated_at,
        eligible=eligible,
    )


def _resolved_symbol(
    candidate: Symbol,
    *,
    name: str | None,
    source_identity: str | None,
    listing_status: str,
    listing_status_reason: str | None,
    listing_status_effective_at: datetime | None,
    instrument_type: str,
    reconciliation_status: str,
    reconciliation_reason: str,
    official_sector: str | None,
    official_security_type: str | None,
    official_source_updated_at: datetime,
    eligible: bool = False,
) -> Symbol:
    return Symbol(
        symbol=candidate.symbol,
        exchange="TSX",
        yahoo_symbol=candidate.yahoo_symbol,
        name=name,
        currency="CAD",
        source="tsx_tmx_reconciled",
        source_url=DEFAULT_TMX_ISSUER_URL,
        source_identity=source_identity,
        listing_status=listing_status,
        listing_status_reason=listing_status_reason,
        listing_status_effective_at=listing_status_effective_at,
        pipeline_eligibility="incremental" if eligible else "none",
        instrument_type=instrument_type,
        reconciliation_status=reconciliation_status,
        reconciliation_reason=reconciliation_reason,
        official_sector=official_sector,
        official_security_type=official_security_type,
        official_source_updated_at=official_source_updated_at,
    )


def _parse_issuer_workbook(content: bytes) -> tuple[list[TMXIssuer], int]:
    payload = BytesIO(content)
    excel = pd.ExcelFile(payload, engine="openpyxl")
    sheet_name = next((name for name in excel.sheet_names if name.startswith("TSX Issuers")), None)
    if sheet_name is None:
        raise ValueError("TMX issuer workbook does not contain a TSX Issuers sheet.")
    preview = pd.read_excel(payload, sheet_name=sheet_name, header=None, nrows=25)
    header_row = next(
        (
            index
            for index, row in preview.iterrows()
            if {"Co_ID", "Exchange", "Root Ticker"}.issubset(
                {_column_name(value) for value in row.tolist()}
            )
        ),
        None,
    )
    if header_row is None:
        raise ValueError("TMX issuer workbook schema header was not found.")
    payload.seek(0)
    frame = pd.read_excel(payload, sheet_name=sheet_name, header=header_row, engine="openpyxl")
    frame.columns = [_column_name(value) for value in frame.columns]
    required = {"Co_ID", "Exchange", "Name", "Root Ticker", "Sector"}
    if not required.issubset(frame.columns):
        missing = ",".join(sorted(required - set(frame.columns)))
        raise ValueError(f"TMX issuer workbook is missing required columns: {missing}")

    issuers: list[TMXIssuer] = []
    invalid_rows = 0
    for row in frame.to_dict(orient="records"):
        exchange = _normalized_text(row.get("Exchange"))
        source_identity = _text(row.get("Co_ID"))
        root_ticker = _normalized_text(row.get("Root Ticker"))
        name = _text(row.get("Name"))
        if exchange != "TSX" or not source_identity or not root_ticker or not name:
            invalid_rows += 1
            continue
        issuers.append(
            TMXIssuer(
                source_identity=source_identity,
                root_ticker=root_ticker,
                name=name,
                exchange=exchange,
                sector=_text(row.get("Sector")),
                sub_sector=_text(row.get("Sub Sector")),
                security_type=_text(row.get("SP_Type")),
                listing_date=_workbook_date(row.get("Listing Date")),
            )
        )
    if not issuers:
        raise ValueError("TMX issuer workbook contained no valid TSX issuer rows.")
    return issuers, invalid_rows


def _issuer_product_eligible(issuer: TMXIssuer) -> bool:
    return (issuer.sector or "") not in _EXCLUDED_SECTORS


def _security_source_identity(issuer: TMXIssuer, symbol: str) -> str:
    suffix = symbol.removeprefix(issuer.root_ticker).removeprefix(".") or "ROOT"
    return f"tmx:{issuer.source_identity}:{suffix}"


def _directory_match(
    entries: dict[str, TMXDirectoryEntry],
    symbol: str,
    root: str,
) -> TMXDirectoryEntry | None:
    return entries.get(symbol) or entries.get(root)


def _root_ticker(symbol: str) -> str:
    return symbol.strip().upper().split(".", 1)[0]


def _candidate_diagnostics(provider: UniverseProvider) -> dict[str, Any]:
    diagnostics = getattr(provider, "diagnostics", None)
    if not callable(diagnostics):
        return {}
    payload = diagnostics()
    return dict(payload) if isinstance(payload, dict) else {}


def _column_name(value: object) -> str:
    return " ".join(str(value if value is not None else "").replace("\n", " ").split())


def _text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = " ".join(str(value).split())
    return normalized or None


def _normalized_text(value: object) -> str | None:
    text = _text(value)
    return text.upper() if text else None


def _workbook_date(value: object) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(str(value).split(".", 1)[0], format="%Y%m%d", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime().replace(tzinfo=UTC)


def _unix_datetime(value: object) -> datetime | None:
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except (TypeError, ValueError, OSError):
        return None


def _http_last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
