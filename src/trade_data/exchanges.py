from __future__ import annotations

CANONICAL_EQUITY_EXCHANGES = frozenset({"NSE", "TSX", "US"})

_EXCHANGE_ALIASES = {
    "CA": "TSX",
    "CANADA": "TSX",
    "NASDAQ": "US",
    "NYSE": "US",
    "USA": "US",
}


def canonical_equity_exchange(value: str) -> str:
    """Return the canonical exchange code used by new equity pipeline writes."""

    normalized = value.strip().upper()
    canonical = _EXCHANGE_ALIASES.get(normalized, normalized)
    if canonical not in CANONICAL_EQUITY_EXCHANGES:
        supported = ", ".join(sorted(CANONICAL_EQUITY_EXCHANGES))
        raise ValueError(f"Unsupported equity exchange {value!r}; expected one of {supported}.")
    return canonical


def is_legacy_equity_exchange_alias(value: str) -> bool:
    """Return whether a value is accepted only as a compatibility alias."""

    return value.strip().upper() in _EXCHANGE_ALIASES
