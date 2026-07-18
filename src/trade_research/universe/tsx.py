import re

import pandas as pd

from trade_research.schemas import Symbol
from trade_research.universe.base import UniverseProvider

DEFAULT_TSX_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1oVNsn3BXvxBJdKF-e1jJDuLka9G6Q2G0LvoUMhChr1Y/export?format=csv&gid=0"
)


class TSXUniverseProvider(UniverseProvider):
    exchange = "TSX"
    source = "tsx_google_sheet"

    def __init__(self, csv_url: str = DEFAULT_TSX_SHEET_URL) -> None:
        self.csv_url = csv_url
        self.last_diagnostics: dict[str, int] = {}

    def fetch(self) -> list[Symbol]:
        df = pd.read_csv(self.csv_url)
        if "Symbol" not in df.columns:
            raise ValueError("TSX universe CSV must include a Symbol column")

        name_col = "Stock Name" if "Stock Name" in df.columns else None
        symbols: dict[str, Symbol] = {}
        diagnostics = {
            "source_rows": 0,
            "tsx_rows": 0,
            "excluded_tsxv_rows": 0,
            "excluded_cboe_canada_rows": 0,
            "invalid_rows": 0,
            "duplicate_rows": 0,
        }
        for row in df.to_dict(orient="records"):
            diagnostics["source_rows"] += 1
            raw_value = row["Symbol"]
            raw = str(raw_value if raw_value is not None else "").strip().upper()
            if raw.endswith(".V"):
                diagnostics["excluded_tsxv_rows"] += 1
                continue
            if raw.endswith(".NE"):
                diagnostics["excluded_cboe_canada_rows"] += 1
                continue
            normalized = _normalize_tsx_symbol(row["Symbol"])
            if normalized is None:
                diagnostics["invalid_rows"] += 1
                continue
            exchange_symbol, yahoo_symbol = normalized
            if yahoo_symbol in symbols:
                diagnostics["duplicate_rows"] += 1
                continue
            symbols.setdefault(
                yahoo_symbol,
                Symbol(
                    symbol=exchange_symbol,
                    exchange=self.exchange,
                    yahoo_symbol=yahoo_symbol,
                    name=_optional_text(row.get(name_col)) if name_col else None,
                    currency="CAD",
                    source="tsx_google_sheet",
                    source_url=self.csv_url,
                ),
            )
            diagnostics["tsx_rows"] += 1
        self.last_diagnostics = diagnostics
        return sorted(symbols.values(), key=lambda item: item.symbol)

    def diagnostics(self) -> dict[str, int]:
        return dict(self.last_diagnostics)


_TSX_NATIVE_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9.]*$")
_NON_TSX_YAHOO_SUFFIXES = (".V", ".NE")


def _normalize_tsx_symbol(value: object) -> tuple[str, str] | None:
    """Return the native TSX symbol and its Yahoo mapping.

    The configured directory contains Yahoo symbols for TSX, TSXV, and Cboe
    Canada. Only `.TO` rows belong to this universe. Native symbols are still
    accepted for compatibility with older test fixtures and replacement feeds.
    """

    raw = str(value if value is not None else "").strip().upper()
    if not raw or raw == "NAN":
        return None
    if raw.endswith(_NON_TSX_YAHOO_SUFFIXES):
        return None
    if raw.endswith(".TO"):
        yahoo_symbol = raw
        native_symbol = raw.removesuffix(".TO").replace("-", ".")
    else:
        native_symbol = raw
        yahoo_symbol = f"{raw.replace('.', '-')}.TO"
    if _TSX_NATIVE_SYMBOL.fullmatch(native_symbol) is None:
        return None
    return native_symbol, yahoo_symbol


def _optional_text(value: object) -> str | None:
    text = str(value if value is not None else "").strip()
    return None if not text or text.lower() == "nan" else text
