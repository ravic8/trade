from __future__ import annotations

import gzip
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import httpx
import pandas as pd

NIFTY50_INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
UPSTOX_COMPLETE_INSTRUMENTS_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
)


class UpstoxAPIError(RuntimeError):
    """Raised when Upstox returns an unsuccessful API response."""


@dataclass(frozen=True)
class UpstoxFuturesContract:
    instrument_key: str
    trading_symbol: str
    expiry: date
    lot_size: int | None = None


@dataclass(frozen=True)
class UpstoxInstrumentMasterAudit:
    rows: int
    missing_instrument_key_rows: int
    duplicate_instrument_key_rows: int
    nse_equity_rows: int
    fetched_at: str


@dataclass(frozen=True)
class UpstoxDailyOhlcvAudit:
    symbol: str
    instrument_key: str
    rows: int
    start_date: date | None
    end_date: date | None
    missing_dates: int
    null_ohlcv_rows: int
    duplicate_date_rows: int
    zero_volume_rows: int
    zero_or_negative_close_rows: int
    status: str


class UpstoxInstrumentMasterProvider:
    """Fetch and normalize the full Upstox BOD instrument master."""

    def __init__(
        self,
        instruments_url: str = UPSTOX_COMPLETE_INSTRUMENTS_URL,
        timeout_seconds: float = 60.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.instruments_url = instruments_url
        self._owned_client = client is None
        self.client = client or httpx.Client(timeout=timeout_seconds, follow_redirects=True)

    def close(self) -> None:
        if self._owned_client:
            self.client.close()

    def __enter__(self) -> UpstoxInstrumentMasterProvider:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def fetch(self) -> pd.DataFrame:
        response = self.client.get(self.instruments_url)
        response.raise_for_status()
        payload = _decode_instrument_payload(response)
        return normalize_instruments(payload)


class UpstoxHistoricalDataProvider:
    """Fetch batch historical candles from Upstox v3."""

    def __init__(
        self,
        access_token: str,
        base_url: str = "https://api.upstox.com",
        timeout_seconds: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._owned_client = client is None
        self.client = client or httpx.Client(timeout=timeout_seconds)
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    def close(self) -> None:
        if self._owned_client:
            self.client.close()

    def __enter__(self) -> UpstoxHistoricalDataProvider:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def fetch_daily_candles(
        self,
        instrument_key: str,
        start: date,
        end: date,
        symbol: str,
        trading_symbol: str | None = None,
    ) -> pd.DataFrame:
        instrument_path = _encode_path_segment(instrument_key)
        payload = self._get_json(
            f"/v3/historical-candle/{instrument_path}/days/1/"
            f"{end.isoformat()}/{start.isoformat()}"
        )
        return _daily_candles_to_frame(
            payload.get("data", {}).get("candles", []),
            instrument_key=instrument_key,
            symbol=symbol,
            trading_symbol=trading_symbol or symbol,
            source="upstox",
        )

    def _get_json(self, path: str) -> dict[str, Any]:
        response = self.client.get(f"{self.base_url}{path}", headers=self.headers)
        if response.status_code >= 400:
            raise UpstoxAPIError(
                f"Upstox request failed: {response.status_code} {response.text}"
            )
        payload = response.json()
        if payload.get("status") not in {None, "success"}:
            raise UpstoxAPIError(f"Upstox request failed: {payload}")
        return payload


class UpstoxNiftyFuturesHistoryProvider:
    """Fetch historical NIFTY futures candles from Upstox.

    Expired futures history is an Upstox Plus API. Current live contracts can be
    fetched with the regular historical candle API when the caller supplies the
    active futures instrument key from the BOD instruments file.
    """

    def __init__(
        self,
        access_token: str,
        base_url: str = "https://api.upstox.com",
        timeout_seconds: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._owned_client = client is None
        self.client = client or httpx.Client(timeout=timeout_seconds)
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    def close(self) -> None:
        if self._owned_client:
            self.client.close()

    def __enter__(self) -> UpstoxNiftyFuturesHistoryProvider:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def get_expiries(self, instrument_key: str = NIFTY50_INSTRUMENT_KEY) -> list[date]:
        payload = self._get_json(
            "/v2/expired-instruments/expiries",
            params={"instrument_key": instrument_key},
        )
        return sorted(date.fromisoformat(value) for value in payload.get("data", []))

    def get_expired_future_contracts(
        self,
        expiry: date,
        instrument_key: str = NIFTY50_INSTRUMENT_KEY,
    ) -> list[UpstoxFuturesContract]:
        payload = self._get_json(
            "/v2/expired-instruments/future/contract",
            params={
                "instrument_key": instrument_key,
                "expiry_date": expiry.isoformat(),
            },
        )
        return [
            UpstoxFuturesContract(
                instrument_key=item["instrument_key"],
                trading_symbol=item["trading_symbol"],
                expiry=date.fromisoformat(item["expiry"]),
                lot_size=item.get("lot_size"),
            )
            for item in payload.get("data", [])
            if item.get("instrument_type") == "FUT"
        ]

    def fetch_expired_historical_candles(
        self,
        contract: UpstoxFuturesContract,
        start: date,
        end: date,
        interval: str = "1minute",
    ) -> pd.DataFrame:
        instrument_path = _encode_path_segment(contract.instrument_key)
        payload = self._get_json(
            f"/v2/expired-instruments/historical-candle/"
            f"{instrument_path}/{interval}/{end.isoformat()}/{start.isoformat()}",
            params=None,
        )
        return _candles_to_frame(
            payload.get("data", {}).get("candles", []),
            instrument_key=contract.instrument_key,
            trading_symbol=contract.trading_symbol,
            expiry=contract.expiry,
            source="upstox_expired",
        )

    def fetch_active_historical_candles(
        self,
        instrument_key: str,
        start: date,
        end: date,
        interval: str = "1minute",
        trading_symbol: str = "NIFTY CURRENT FUT",
    ) -> pd.DataFrame:
        unit, unit_interval = _active_interval(interval)
        frames: list[pd.DataFrame] = []
        for chunk_start, chunk_end in _active_chunks(start, end, interval):
            instrument_path = _encode_path_segment(instrument_key)
            payload = self._get_json(
                f"/v3/historical-candle/{instrument_path}/{unit}/{unit_interval}/"
                f"{chunk_end.isoformat()}/{chunk_start.isoformat()}",
                params=None,
            )
            frame = _candles_to_frame(
                payload.get("data", {}).get("candles", []),
                instrument_key=instrument_key,
                trading_symbol=trading_symbol,
                expiry=None,
                source="upstox_active",
            )
            if not frame.empty:
                frames.append(frame)

        return _concat(frames)

    def fetch_nifty50_futures_history(
        self,
        start: date,
        end: date,
        interval: str = "1minute",
        active_instrument_key: str | None = None,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        expiries = [
            expiry
            for expiry in self.get_expiries(NIFTY50_INSTRUMENT_KEY)
            if start <= expiry <= end
        ]
        for expiry in expiries:
            for contract in self.get_expired_future_contracts(expiry, NIFTY50_INSTRUMENT_KEY):
                frame = self.fetch_expired_historical_candles(contract, start, end, interval)
                if not frame.empty:
                    frames.append(frame)

        if active_instrument_key:
            active = self.fetch_active_historical_candles(
                active_instrument_key,
                start,
                end,
                interval,
            )
            if not active.empty:
                frames.append(active)

        return _concat(frames)

    def _get_json(self, path: str, params: dict[str, str] | None) -> dict[str, Any]:
        response = self.client.get(f"{self.base_url}{path}", headers=self.headers, params=params)
        if response.status_code >= 400:
            raise UpstoxAPIError(
                f"Upstox request failed: {response.status_code} {response.text}"
            )
        payload = response.json()
        if payload.get("status") not in {None, "success"}:
            raise UpstoxAPIError(f"Upstox request failed: {payload}")
        return payload


def _candles_to_frame(
    candles: list[list[Any]],
    instrument_key: str,
    trading_symbol: str,
    expiry: date | None,
    source: str,
) -> pd.DataFrame:
    rows = []
    for candle in candles:
        if len(candle) < 6:
            continue
        rows.append(
            {
                "Datetime": pd.to_datetime(candle[0]),
                "Open": candle[1],
                "High": candle[2],
                "Low": candle[3],
                "Close": candle[4],
                "Volume": candle[5],
                "OpenInterest": candle[6] if len(candle) > 6 else None,
                "InstrumentKey": instrument_key,
                "TradingSymbol": trading_symbol,
                "Expiry": expiry.isoformat() if expiry else None,
                "Ticker": trading_symbol,
                "Source": source,
            }
        )

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values(["TradingSymbol", "Datetime"])
        .reset_index(drop=True)
    )


def normalize_instruments(payload: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in payload:
        rows.append(
            {
                "source": "upstox",
                "instrument_key": _get_first(item, "instrument_key", "instrumentKey"),
                "exchange": _get_first(item, "exchange"),
                "segment": _get_first(item, "segment"),
                "asset_type": _get_first(item, "instrument_type", "instrumentType"),
                "trading_symbol": _get_first(item, "trading_symbol", "tradingSymbol"),
                "name": _get_first(item, "name", "company_name", "companyName"),
                "isin": _get_first(item, "isin"),
                "lot_size": _get_first(item, "lot_size", "lotSize"),
                "tick_size": _get_first(item, "tick_size", "tickSize"),
                "expiry": _normalize_date(_get_first(item, "expiry")),
                "strike": _get_first(item, "strike_price", "strikePrice", "strike"),
                "option_type": _get_first(item, "option_type", "optionType"),
                "underlying_symbol": _get_first(
                    item,
                    "underlying_symbol",
                    "underlyingSymbol",
                    "underlying_key",
                    "underlyingKey",
                ),
                "underlying_key": _get_first(item, "underlying_key", "underlyingKey"),
                "exchange_token": _get_first(item, "exchange_token", "exchangeToken"),
                "raw": item,
            }
        )
    return pd.DataFrame(rows)


def instrument_master_audit(frame: pd.DataFrame) -> UpstoxInstrumentMasterAudit:
    if frame.empty:
        return UpstoxInstrumentMasterAudit(
            rows=0,
            missing_instrument_key_rows=0,
            duplicate_instrument_key_rows=0,
            nse_equity_rows=0,
            fetched_at=pd.Timestamp.now("UTC").isoformat(),
        )
    missing_keys = frame["instrument_key"].isna() | frame["instrument_key"].eq("")
    duplicate_keys = frame.duplicated(subset=["instrument_key"], keep=False)
    nse_equities = _nse_equity_instruments(frame)
    return UpstoxInstrumentMasterAudit(
        rows=len(frame),
        missing_instrument_key_rows=int(missing_keys.sum()),
        duplicate_instrument_key_rows=int(duplicate_keys.sum()),
        nse_equity_rows=len(nse_equities),
        fetched_at=pd.Timestamp.now("UTC").isoformat(),
    )


def map_liquid_universe_to_upstox(
    liquid_universe: pd.DataFrame,
    instruments: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    nse_equities = _nse_equity_instruments(instruments).copy()
    nse_equities["symbol_key"] = nse_equities["trading_symbol"].astype(str).str.upper()

    universe = liquid_universe.copy()
    universe["symbol_key"] = universe["symbol"].astype(str).str.upper()
    mapping_columns = [
        "symbol_key",
        "instrument_key",
        "trading_symbol",
        "name",
        "isin",
        "segment",
        "asset_type",
        "lot_size",
        "tick_size",
    ]
    mapped = universe.merge(
        nse_equities[mapping_columns].drop_duplicates("symbol_key"),
        on="symbol_key",
        how="left",
        suffixes=("", "_upstox"),
    )
    matched = mapped[mapped["instrument_key"].notna()].drop(columns=["symbol_key"])
    unmatched = mapped[mapped["instrument_key"].isna()].drop(columns=["symbol_key"])
    return matched.reset_index(drop=True), unmatched.reset_index(drop=True)


def audit_daily_ohlcv(frame: pd.DataFrame, expected_symbols: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            [
                UpstoxDailyOhlcvAudit(
                    symbol=str(row["symbol"]),
                    instrument_key=str(row["instrument_key"]),
                    rows=0,
                    start_date=None,
                    end_date=None,
                    missing_dates=0,
                    null_ohlcv_rows=0,
                    duplicate_date_rows=0,
                    zero_volume_rows=0,
                    zero_or_negative_close_rows=0,
                    status="failed",
                ).__dict__
                for row in expected_symbols.to_dict(orient="records")
            ]
        )

    data = frame.copy()
    data["Date"] = pd.to_datetime(data["Date"]).dt.date
    expected_dates = pd.Index(sorted(data["Date"].dropna().unique()))
    rows = []
    for item in expected_symbols.to_dict(orient="records"):
        symbol = str(item["symbol"])
        instrument_key = str(item["instrument_key"])
        group = data[data["InstrumentKey"].eq(instrument_key)]
        duplicate_rows = int(group.duplicated(subset=["Date"], keep=False).sum())
        null_rows = int(group[["Open", "High", "Low", "Close", "Volume"]].isna().any(axis=1).sum())
        zero_volume_rows = int((group["Volume"].fillna(0) <= 0).sum())
        zero_or_negative_close_rows = int((group["Close"].fillna(0) <= 0).sum())
        if group.empty:
            missing_dates = 0
        else:
            first_date = group["Date"].min()
            last_date = group["Date"].max()
            symbol_expected_dates = expected_dates[
                (expected_dates >= first_date) & (expected_dates <= last_date)
            ]
            missing_dates = len(
                symbol_expected_dates.difference(pd.Index(group["Date"].dropna().unique()))
            )
        status = "passed"
        if group.empty or duplicate_rows or null_rows or zero_or_negative_close_rows:
            status = "failed"
        elif missing_dates or zero_volume_rows:
            status = "warning"
        audit = UpstoxDailyOhlcvAudit(
            symbol=symbol,
            instrument_key=instrument_key,
            rows=len(group),
            start_date=group["Date"].min() if not group.empty else None,
            end_date=group["Date"].max() if not group.empty else None,
            missing_dates=missing_dates,
            null_ohlcv_rows=null_rows,
            duplicate_date_rows=duplicate_rows,
            zero_volume_rows=zero_volume_rows,
            zero_or_negative_close_rows=zero_or_negative_close_rows,
            status=status,
        )
        rows.append(audit.__dict__)
    return pd.DataFrame(rows)


def _daily_candles_to_frame(
    candles: list[list[Any]],
    instrument_key: str,
    symbol: str,
    trading_symbol: str,
    source: str,
) -> pd.DataFrame:
    rows = []
    for candle in candles:
        if len(candle) < 6:
            continue
        rows.append(
            {
                "Date": pd.to_datetime(candle[0]).date(),
                "Open": candle[1],
                "High": candle[2],
                "Low": candle[3],
                "Close": candle[4],
                "Volume": candle[5],
                "OpenInterest": candle[6] if len(candle) > 6 else None,
                "InstrumentKey": instrument_key,
                "Symbol": symbol,
                "TradingSymbol": trading_symbol,
                "Source": source,
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["Symbol", "Date"]).reset_index(drop=True)


def _decode_instrument_payload(response: httpx.Response) -> list[dict[str, Any]]:
    try:
        payload = response.json()
    except ValueError:
        payload = httpx.Response(
            200,
            content=gzip.decompress(response.content),
        ).json()
    if not isinstance(payload, list):
        raise UpstoxAPIError("Upstox instrument master payload was not a list")
    return payload


def _nse_equity_instruments(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    segment = frame["segment"].fillna("").astype(str).str.upper()
    exchange = frame["exchange"].fillna("").astype(str).str.upper()
    asset_type = frame["asset_type"].fillna("").astype(str).str.upper()
    return frame[
        (segment.eq("NSE_EQ") | (exchange.eq("NSE") & segment.str.contains("EQ")))
        & asset_type.isin({"EQ", "EQUITY", ""})
    ].copy()


def _get_first(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in item:
            return item[key]
    return None


def _normalize_date(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        return str(value)


def _concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(
        ["TradingSymbol", "Datetime"]
    ).reset_index(drop=True)


def _encode_path_segment(value: str) -> str:
    return httpx.URL(path=value).raw_path.decode("ascii").lstrip("/")


def _active_interval(interval: str) -> tuple[str, str]:
    if interval == "day":
        return "days", "1"
    if interval.endswith("minute"):
        return "minutes", interval.removesuffix("minute")
    raise ValueError("active futures interval must be one of 1minute, 3minute, 5minute, "
                     "15minute, 30minute, or day")


def _active_chunks(start: date, end: date, interval: str) -> list[tuple[date, date]]:
    if not interval.endswith("minute"):
        return [(start, end)]

    chunks: list[tuple[date, date]] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=30), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks
