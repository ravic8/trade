from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DukascopyInstrument:
    symbol: str
    dukascopy_id: str
    instrument_key: str
    name: str
    exchange: str
    asset_class: str
    currency: str


DUKASCOPY_INTRADAY_UNIVERSE_ID = "dukascopy_fx_crypto_5m"

DUKASCOPY_INTRADAY_INSTRUMENTS: tuple[DukascopyInstrument, ...] = (
    DukascopyInstrument(
        symbol="EUR/USD",
        dukascopy_id="eurusd",
        instrument_key="DUKAS|EURUSD",
        name="Euro vs US Dollar",
        exchange="FX",
        asset_class="fx",
        currency="USD",
    ),
    DukascopyInstrument(
        symbol="USD/JPY",
        dukascopy_id="usdjpy",
        instrument_key="DUKAS|USDJPY",
        name="US Dollar vs Japanese Yen",
        exchange="FX",
        asset_class="fx",
        currency="JPY",
    ),
    DukascopyInstrument(
        symbol="USD/CAD",
        dukascopy_id="usdcad",
        instrument_key="DUKAS|USDCAD",
        name="US Dollar vs Canadian Dollar",
        exchange="FX",
        asset_class="fx",
        currency="CAD",
    ),
    DukascopyInstrument(
        symbol="USD/CNH",
        dukascopy_id="usdcnh",
        instrument_key="DUKAS|USDCNH",
        name="US Dollar vs Offshore Chinese Renminbi",
        exchange="FX",
        asset_class="fx",
        currency="CNH",
    ),
    DukascopyInstrument(
        symbol="GBP/USD",
        dukascopy_id="gbpusd",
        instrument_key="DUKAS|GBPUSD",
        name="Pound Sterling vs US Dollar",
        exchange="FX",
        asset_class="fx",
        currency="USD",
    ),
    DukascopyInstrument(
        symbol="BTC/USD",
        dukascopy_id="btcusd",
        instrument_key="DUKAS|BTCUSD",
        name="Bitcoin vs US Dollar",
        exchange="CRYPTO",
        asset_class="crypto",
        currency="USD",
    ),
)


def dukascopy_intraday_universe(
    universe: str = DUKASCOPY_INTRADAY_UNIVERSE_ID,
) -> list[DukascopyInstrument]:
    if universe != DUKASCOPY_INTRADAY_UNIVERSE_ID:
        raise ValueError(f"Unsupported Dukascopy universe: {universe}")
    return list(DUKASCOPY_INTRADAY_INSTRUMENTS)
