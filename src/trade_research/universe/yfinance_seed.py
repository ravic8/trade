from __future__ import annotations

from trade_research.schemas import Symbol

US_SEED_ROWS: tuple[tuple[str, str], ...] = (
    ("AAPL", "Apple"),
    ("MSFT", "Microsoft"),
    ("NVDA", "NVIDIA"),
    ("AMZN", "Amazon"),
    ("META", "Meta Platforms"),
    ("GOOGL", "Alphabet"),
    ("BRK-B", "Berkshire Hathaway"),
    ("LLY", "Eli Lilly"),
    ("AVGO", "Broadcom"),
    ("JPM", "JPMorgan Chase"),
    ("XOM", "Exxon Mobil"),
    ("UNH", "UnitedHealth"),
    ("V", "Visa"),
    ("MA", "Mastercard"),
    ("COST", "Costco"),
    ("HD", "Home Depot"),
    ("PG", "Procter & Gamble"),
    ("NFLX", "Netflix"),
    ("AMD", "Advanced Micro Devices"),
    ("CRM", "Salesforce"),
)


CANADA_SEED_ROWS: tuple[tuple[str, str], ...] = (
    ("SHOP", "Shopify"),
    ("RY", "Royal Bank of Canada"),
    ("TD", "Toronto-Dominion Bank"),
    ("BN", "Brookfield"),
    ("BMO", "Bank of Montreal"),
    ("BNS", "Bank of Nova Scotia"),
    ("CNQ", "Canadian Natural Resources"),
    ("CP", "Canadian Pacific Kansas City"),
    ("CNR", "Canadian National Railway"),
    ("ENB", "Enbridge"),
    ("TRI", "Thomson Reuters"),
    ("CSU", "Constellation Software"),
    ("ATD", "Alimentation Couche-Tard"),
    ("MFC", "Manulife Financial"),
    ("SLF", "Sun Life Financial"),
    ("SU", "Suncor Energy"),
    ("BCE", "BCE"),
    ("T", "TELUS"),
    ("WCN", "Waste Connections"),
    ("GIB-A", "CGI"),
)


def yfinance_seed_universe(name: str) -> list[Symbol]:
    normalized = name.strip().lower().replace("-", "_")
    if normalized in {"us", "us_seed", "usa", "united_states"}:
        return [_seed_symbol(symbol, name, "US", "USD", symbol) for symbol, name in US_SEED_ROWS]
    if normalized in {"ca", "canada", "canada_seed", "tsx_seed"}:
        return [
            _seed_symbol(symbol, name, "CA", "CAD", f"{symbol}.TO")
            for symbol, name in CANADA_SEED_ROWS
        ]
    raise ValueError(f"Unsupported yfinance seed universe: {name}")


def _seed_symbol(
    symbol: str,
    name: str,
    exchange: str,
    currency: str,
    yahoo_symbol: str,
) -> Symbol:
    return Symbol(
        symbol=symbol,
        exchange=exchange,
        yahoo_symbol=yahoo_symbol,
        name=name,
        currency=currency,
        source="seed",
    )
