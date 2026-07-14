from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class YFinanceIntradayInstrument:
    symbol: str
    yahoo_symbol: str
    instrument_key: str
    name: str
    exchange: str
    asset_class: str
    currency: str


YFINANCE_INTRADAY_UNIVERSE_ID = "yfinance_fx_crypto_5m"

YFINANCE_INTRADAY_INSTRUMENTS: tuple[YFinanceIntradayInstrument, ...] = (
    YFinanceIntradayInstrument(
        symbol="EUR/USD",
        yahoo_symbol="EURUSD=X",
        instrument_key="YF_INTRADAY|EURUSD=X",
        name="Euro vs US Dollar",
        exchange="FX",
        asset_class="fx",
        currency="USD",
    ),
    YFinanceIntradayInstrument(
        symbol="USD/JPY",
        yahoo_symbol="JPY=X",
        instrument_key="YF_INTRADAY|JPY=X",
        name="US Dollar vs Japanese Yen",
        exchange="FX",
        asset_class="fx",
        currency="JPY",
    ),
    YFinanceIntradayInstrument(
        symbol="USD/CAD",
        yahoo_symbol="CAD=X",
        instrument_key="YF_INTRADAY|CAD=X",
        name="US Dollar vs Canadian Dollar",
        exchange="FX",
        asset_class="fx",
        currency="CAD",
    ),
    YFinanceIntradayInstrument(
        symbol="USD/CNH",
        yahoo_symbol="CNH=X",
        instrument_key="YF_INTRADAY|CNH=X",
        name="US Dollar vs Offshore Chinese Renminbi",
        exchange="FX",
        asset_class="fx",
        currency="CNH",
    ),
    YFinanceIntradayInstrument(
        symbol="GBP/USD",
        yahoo_symbol="GBPUSD=X",
        instrument_key="YF_INTRADAY|GBPUSD=X",
        name="Pound Sterling vs US Dollar",
        exchange="FX",
        asset_class="fx",
        currency="USD",
    ),
    YFinanceIntradayInstrument(
        symbol="BTC/USD",
        yahoo_symbol="BTC-USD",
        instrument_key="YF_INTRADAY|BTC-USD",
        name="Bitcoin vs US Dollar",
        exchange="CRYPTO",
        asset_class="crypto",
        currency="USD",
    ),
)


def yfinance_intraday_universe(
    universe: str = YFINANCE_INTRADAY_UNIVERSE_ID,
) -> list[YFinanceIntradayInstrument]:
    if universe != YFINANCE_INTRADAY_UNIVERSE_ID:
        raise ValueError(f"Unsupported yfinance intraday universe: {universe}")
    return list(YFINANCE_INTRADAY_INSTRUMENTS)
