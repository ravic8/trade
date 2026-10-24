import re
from io import StringIO

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from trade_data.schemas import Symbol
from trade_data.universe.base import UniverseProvider

NSE_EQUITY_URLS = [
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
]


class NSEUniverseProvider(UniverseProvider):
    exchange = "NSE"
    source = "nse_equity_list"

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
        df.columns = df.columns.str.strip()
        if not {"SYMBOL", "SERIES"}.issubset(df.columns):
            raise ValueError("NSE equity list requires SYMBOL and SERIES columns")
        if not {"NAME OF COMPANY", "ISIN NUMBER"}.issubset(df.columns):
            raise ValueError("NSE equity classification requires company name and ISIN NUMBER")
        df = df[df.apply(is_ordinary_nse_equity, axis=1)].copy()

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
                    source_identity=str(row["ISIN NUMBER"]).strip().upper(),
                    instrument_type="common_stock",
                    official_security_type="fully_paid_equity:" + str(row["SERIES"]).strip(),
                    reconciliation_status="matched",
                    reconciliation_reason="official_equity_list_isin_series_v2",
                    source_url=url,
                )
            )
        return symbols


def is_ordinary_nse_equity(row: dict | pd.Series) -> bool:
    """Classify the official equity-share list, not a general security master.

    INE is necessary, not sufficient: rights also have issuer ISINs. Separate
    ETF/fund, debt and partly paid lists are not inputs to this classifier.
    Unknown series/identities fail closed; BE/BZ are trading regimes, not types.
    """
    symbol = str(row.get("SYMBOL", "")).strip().upper()
    series = str(row.get("SERIES", "")).strip().upper()
    isin = str(row.get("ISIN NUMBER", "")).strip().upper()
    name = str(row.get("NAME OF COMPANY", "")).strip().upper()
    return bool(
        symbol
        and name
        and name != "NAN"
        and series in {"EQ", "BE", "BZ", "SM", "ST", "SZ"}
        and re.fullmatch(r"IN[E9][A-Z0-9]{9}", isin)
        and not re.search(r"-RE\d*$|-(?:PP|P1|P2)$", symbol)
        and not re.search(r"RIGHTS? ENTITLEMENT|PARTLY PAID|EXCHANGE TRADED|\bETF\b", name)
    )
