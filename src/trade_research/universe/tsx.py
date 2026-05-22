import pandas as pd

from trade_research.schemas import Symbol
from trade_research.universe.base import UniverseProvider

DEFAULT_TSX_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1oVNsn3BXvxBJdKF-e1jJDuLka9G6Q2G0LvoUMhChr1Y/export?format=csv&gid=0"
)


class TSXUniverseProvider(UniverseProvider):
    exchange = "TSX"

    def __init__(self, csv_url: str = DEFAULT_TSX_SHEET_URL) -> None:
        self.csv_url = csv_url

    def fetch(self) -> list[Symbol]:
        df = pd.read_csv(self.csv_url)
        if "Symbol" not in df.columns:
            raise ValueError("TSX universe CSV must include a Symbol column")

        name_col = "Stock Name" if "Stock Name" in df.columns else None
        symbols: list[Symbol] = []
        for row in df.to_dict(orient="records"):
            raw_symbol = str(row["Symbol"]).strip()
            if not raw_symbol or raw_symbol.lower() == "nan":
                continue
            symbols.append(
                Symbol(
                    symbol=raw_symbol,
                    exchange=self.exchange,
                    yahoo_symbol=f"{raw_symbol.replace('.', '-')}.TO",
                    name=str(row.get(name_col, "")).strip() if name_col else None,
                    currency="CAD",
                    source="tsx_google_sheet",
                    source_url=self.csv_url,
                )
            )
        return symbols
