from io import StringIO

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from trade_research.schemas import Symbol
from trade_research.universe.base import UniverseProvider

NSE_EQUITY_URLS = [
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
]


class NSEUniverseProvider(UniverseProvider):
    exchange = "NSE"

    def __init__(self, urls: list[str] | None = None) -> None:
        self.urls = urls or NSE_EQUITY_URLS

    def fetch(self) -> list[Symbol]:
        last_error: Exception | None = None
        for url in self.urls:
            try:
                return self._fetch_from_url(url)
            except Exception as exc:  # pragma: no cover - used for fallback URLs
                last_error = exc
        raise RuntimeError("Could not fetch NSE equity universe") from last_error

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def _fetch_from_url(self, url: str) -> list[Symbol]:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,application/csv,text/plain,*/*",
            "Referer": "https://www.nseindia.com/market-data/securities-available-for-trading",
        }
        with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))
        if "SERIES" in df.columns:
            df = df[df["SERIES"].eq("EQ")].copy()

        symbols: list[Symbol] = []
        for row in df.to_dict(orient="records"):
            raw_symbol = str(row["SYMBOL"]).strip()
            symbols.append(
                Symbol(
                    symbol=raw_symbol,
                    exchange=self.exchange,
                    yahoo_symbol=f"{raw_symbol}.NS",
                    name=str(row.get("NAME OF COMPANY", "")).strip() or None,
                    currency="INR",
                    source="nse_equity_list",
                    source_url=url,
                )
            )
        return symbols
