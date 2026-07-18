from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from io import StringIO
from xml.etree import ElementTree

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from trade_research.schemas import Symbol
from trade_research.universe.base import UniverseProvider
from trade_research.universe.tsx import TSXUniverseProvider
from trade_research.universe.yfinance_seed import yfinance_seed_universe

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
NASDAQ_HALTS_URL = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
NASDAQ_RSS_NAMESPACE = "{http://www.nasdaqtrader.com/}"


class YFinanceUSUniverseProvider(UniverseProvider):
    exchange = "US"
    source = "nasdaq_trader_symbol_directory"

    def __init__(
        self,
        nasdaq_url: str = NASDAQ_LISTED_URL,
        other_url: str = OTHER_LISTED_URL,
        sec_tickers_url: str = SEC_TICKERS_URL,
        halts_url: str = NASDAQ_HALTS_URL,
    ) -> None:
        self.nasdaq_url = nasdaq_url
        self.other_url = other_url
        self.sec_tickers_url = sec_tickers_url
        self.halts_url = halts_url

    def fetch(self) -> list[Symbol]:
        rows = [
            *self._fetch_nasdaq_listed(self.nasdaq_url),
            *self._fetch_other_listed(self.other_url),
        ]
        sec_identities = _optional_sec_identities(self.sec_tickers_url)
        current_halts, halt_feed_available = _optional_current_halts(self.halts_url)
        deduped: dict[str, Symbol] = {}
        for symbol in rows:
            halt = current_halts.get(symbol.symbol)
            issuer_identity = sec_identities.get(symbol.symbol)
            identity = (
                f"{issuer_identity}:{_security_class_key(symbol.name)}" if issuer_identity else None
            )
            deduped.setdefault(
                symbol.symbol,
                symbol.model_copy(
                    update={
                        "source_identity": identity,
                        "listing_status": (
                            "halted" if halt else "active" if halt_feed_available else "unknown"
                        ),
                        "listing_status_reason": halt[0] if halt else None,
                        "listing_status_effective_at": halt[1] if halt else None,
                        "pipeline_eligibility": (
                            "none" if halt else "incremental" if halt_feed_available else "preserve"
                        ),
                    }
                ),
            )
        return sorted(deduped.values(), key=lambda item: item.symbol)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def _fetch_nasdaq_listed(self, url: str) -> list[Symbol]:
        frame = _read_pipe_table(_fetch_text(url))
        required = {"Symbol", "Security Name", "Test Issue", "ETF"}
        if not required.issubset(frame.columns):
            raise ValueError("nasdaqlisted.txt has an unexpected schema")
        symbols = []
        for row in frame.to_dict(orient="records"):
            ticker = _clean_us_ticker(row.get("Symbol"))
            if not ticker or _is_file_timestamp_row(ticker):
                continue
            if str(row.get("Test Issue", "")).upper() == "Y":
                continue
            if str(row.get("ETF", "")).upper() == "Y":
                continue
            name = str(row.get("Security Name", "")).strip()
            if not _is_supported_equity_name(name):
                continue
            symbols.append(
                Symbol(
                    symbol=ticker,
                    exchange=self.exchange,
                    yahoo_symbol=_to_yfinance_us_symbol(ticker),
                    name=name or None,
                    currency="USD",
                    source="nasdaq_trader_symbol_directory",
                    source_url=url,
                )
            )
        return symbols

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def _fetch_other_listed(self, url: str) -> list[Symbol]:
        frame = _read_pipe_table(_fetch_text(url))
        required = {"ACT Symbol", "Security Name", "Test Issue", "ETF"}
        if not required.issubset(frame.columns):
            raise ValueError("otherlisted.txt has an unexpected schema")
        symbols = []
        for row in frame.to_dict(orient="records"):
            ticker = _clean_us_ticker(row.get("ACT Symbol"))
            if not ticker or _is_file_timestamp_row(ticker):
                continue
            if str(row.get("Test Issue", "")).upper() == "Y":
                continue
            if str(row.get("ETF", "")).upper() == "Y":
                continue
            name = str(row.get("Security Name", "")).strip()
            if not _is_supported_equity_name(name):
                continue
            symbols.append(
                Symbol(
                    symbol=ticker,
                    exchange=self.exchange,
                    yahoo_symbol=_to_yfinance_us_symbol(ticker),
                    name=name or None,
                    currency="USD",
                    source="nasdaq_trader_symbol_directory",
                    source_url=url,
                )
            )
        return symbols


class YFinanceCanadaUniverseProvider(UniverseProvider):
    exchange = "CA"
    source = "tsx_google_sheet"

    def __init__(self, tsx_provider: TSXUniverseProvider | None = None) -> None:
        self.tsx_provider = tsx_provider or TSXUniverseProvider()

    def fetch(self) -> list[Symbol]:
        symbols = []
        for item in self.tsx_provider.fetch():
            symbols.append(
                Symbol(
                    symbol=item.symbol,
                    exchange=self.exchange,
                    yahoo_symbol=item.yahoo_symbol,
                    name=item.name,
                    currency="CAD",
                    source=item.source,
                    source_url=item.source_url,
                )
            )
        return symbols


def yfinance_universe(name: str) -> list[Symbol]:
    normalized = name.strip().lower().replace("-", "_")
    if normalized in {"us", "usa", "united_states", "us_all"}:
        return YFinanceUSUniverseProvider().fetch()
    if normalized in {"ca", "canada", "canada_all", "tsx_all"}:
        return YFinanceCanadaUniverseProvider().fetch()
    return yfinance_seed_universe(name)


def yfinance_universe_id(name: str) -> str:
    normalized = name.strip().lower().replace("-", "_")
    if normalized in {"us", "usa", "united_states", "us_all"}:
        return "us_all"
    if normalized in {"ca", "canada", "canada_all", "tsx_all"}:
        return "canada_all"
    if normalized in {"us_seed"}:
        return "us_seed"
    if normalized in {"canada_seed", "tsx_seed"}:
        return "canada_seed"
    yfinance_seed_universe(name)
    return normalized


def yfinance_exchange_for_universe(name: str) -> str:
    universe_id = yfinance_universe_id(name)
    if universe_id in {"us_all", "us_seed"}:
        return "US"
    if universe_id in {"canada_all", "canada_seed"}:
        return "CA"
    raise ValueError(f"Unsupported yfinance universe: {name}")


def _fetch_text(url: str) -> str:
    user_agent = (
        os.getenv(
            "SEC_USER_AGENT",
            "trade-research/0.1 contact=operations@chain8.org",
        )
        if "sec.gov" in url.lower()
        else "Mozilla/5.0"
    )
    headers = {"User-Agent": user_agent, "Accept": "text/plain,*/*"}
    with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
    return response.text


def _optional_sec_identities(url: str) -> dict[str, str]:
    """Return stable SEC issuer identities without making universe refresh fragile."""
    try:
        payload = httpx.Response(200, text=_fetch_text(url)).json()
        fields = [str(value) for value in payload.get("fields", [])]
        field_index = {name: index for index, name in enumerate(fields)}
        cik_index = field_index["cik"]
        ticker_index = field_index["ticker"]
        return {
            str(row[ticker_index]).strip().upper(): f"SEC_CIK:{int(row[cik_index]):010d}"
            for row in payload.get("data", [])
            if len(row) > max(cik_index, ticker_index) and row[ticker_index]
        }
    except Exception:
        return {}


def _optional_current_halts(
    url: str,
) -> tuple[dict[str, tuple[str, datetime]], bool]:
    """Return unresolved Nasdaq halts; resumed entries are deliberately ignored."""
    try:
        root = ElementTree.fromstring(_fetch_text(url).lstrip("\ufeff"))
    except Exception:
        return {}, False
    halts: dict[str, tuple[str, datetime]] = {}
    for item in root.findall("./channel/item"):
        symbol = _rss_text(item, "IssueSymbol").upper()
        resumed = _rss_text(item, "ResumptionDate")
        halt_date = _rss_text(item, "HaltDate")
        if not symbol or resumed or not halt_date:
            continue
        try:
            effective_at = datetime.strptime(halt_date, "%m/%d/%Y").replace(tzinfo=UTC)
        except ValueError:
            continue
        reason = _rss_text(item, "ReasonCode") or "exchange_halt"
        halts[symbol] = (reason, effective_at)
    return halts, True


def _rss_text(item: ElementTree.Element, name: str) -> str:
    node = item.find(f"{NASDAQ_RSS_NAMESPACE}{name}")
    return str(node.text or "").strip() if node is not None else ""


def _security_class_key(name: str | None) -> str:
    normalized = str(name or "").upper()
    match = re.search(r"\bCLASS\s+([A-Z0-9]+)\b", normalized)
    if match:
        return f"CLASS_{match.group(1)}"
    if "ORDINARY" in normalized or " ORD " in f" {normalized} ":
        return "ORDINARY"
    return "COMMON"


def _read_pipe_table(text: str) -> pd.DataFrame:
    return pd.read_csv(StringIO(text), sep="|")


def _clean_us_ticker(value: object) -> str:
    ticker = str(value or "").strip().upper()
    if ticker in {"", "NAN"} or "$" in ticker:
        return ""
    return ticker


def _is_file_timestamp_row(ticker: str) -> bool:
    return ticker.startswith("FILE CREATION TIME")


def _to_yfinance_us_symbol(ticker: str) -> str:
    return ticker.replace(".", "-")


def _is_supported_equity_name(name: str) -> bool:
    normalized = name.lower()
    excluded_fragments = (
        " warrant",
        " warrants",
        " unit",
        " units",
        " right",
        " rights",
        " preferred",
        " preference",
        " note",
        " notes",
        " bond",
        " debenture",
        " etf",
        " fund",
        " trust preferred",
    )
    return not any(fragment in normalized for fragment in excluded_fragments)
