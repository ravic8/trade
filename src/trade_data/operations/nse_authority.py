"""Verify an audited NSE archive dataset and prepare source-labelled daily batches.

This is a bounded historical import, not a scheduled collector. Yahoo data and
corporate-action adjustments are deliberately kept as separate source evidence.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from trade_data.config import Settings
from trade_data.market_data.contracts import CandleInterval, MarketCandle, ProviderRequest
from trade_data.market_data.ingestion import (
    ValidatedMarketDataBatch,
    _write_raw_snapshot_if_enabled,
)
from trade_data.market_data.validation import MarketDataValidationError, validate_candle_batch

NSE_SOURCE = "nse_official_bhavcopy"
FIELDS = ("open", "high", "low", "close", "volume")
SOURCE_COLUMNS = {
    "TckrSymb": "exchange_symbol",
    "ISIN": "isin",
    "SctySrs": "series",
    "OpnPric": "open",
    "HghPric": "high",
    "LwPric": "low",
    "ClsPric": "close",
    "TtlTradgVol": "volume",
}


def verify_nse_dataset(
    path: Path, *, sha256: str, archives: Path, eligible_sessions: set[date]
) -> pd.DataFrame:
    """Fail closed on a changed artifact, unsupported basis, or any source difference."""
    if hashlib.sha256(path.read_bytes()).hexdigest() != sha256:
        raise ValueError("NSE dataset checksum does not match the approved artifact")
    frame = pd.read_parquet(path)
    required = {
        "symbol",
        "exchange_symbol",
        "isin",
        "current_isin",
        "series",
        "exchange",
        "date",
        "source",
        "price_basis",
        "volume_basis",
        "evidence_url",
        "evidence_sha256",
        *FIELDS,
    }
    if not required.issubset(frame.columns) or frame.empty:
        raise ValueError("NSE dataset is empty or missing required provenance")
    if (
        not frame.source.eq(NSE_SOURCE).all()
        or not frame.exchange.eq("NSE").all()
        or not frame.price_basis.eq("as_traded").all()
        or not frame.volume_basis.eq("exchange_reported_shares").all()
        or frame.symbol.str.contains(r"-RE\d*\.NS$|-(?:PP|P1|P2)\.NS$", regex=True).any()
    ):
        raise ValueError("Unsupported source, instrument or OHLCV basis")
    if frame[list(required)].isna().any().any() or frame.duplicated(["symbol", "date"]).any():
        raise ValueError("NSE dataset contains nulls or duplicate stock/date identities")
    if set(frame.date) != eligible_sessions:
        raise ValueError("NSE dataset does not cover the exact validated trading calendar")
    values = frame[list(FIELDS)].to_numpy(dtype=float)
    if (
        not np.isfinite(values).all()
        or (values[:, :4] <= 0).any()
        or (values[:, 1] < values[:, :4].max(axis=1)).any()
        or (values[:, 2] > values[:, :4].min(axis=1)).any()
        or (values[:, 4] < 0).any()
        or not pd.api.types.is_integer_dtype(frame.volume.dtype)
    ):
        raise ValueError("NSE dataset contains invalid OHLCV or non-integer volume")
    identity = ["exchange_symbol", "isin", "series"]
    for day, group in frame.groupby("date"):
        archive = archives / f"{day}.zip"
        meta = json.loads(archive.with_suffix(".json").read_text())
        raw = archive.read_bytes()
        host = urlparse(meta["url"]).hostname
        if host not in {"nsearchives.nseindia.com", "archives.nseindia.com"}:
            raise ValueError("Source evidence must reference an official NSE archive")
        if (
            hashlib.sha256(raw).hexdigest() != meta["sha256"]
            or not group.evidence_sha256.eq(meta["sha256"]).all()
            or not group.evidence_url.eq(meta["url"]).all()
        ):
            raise ValueError("NSE source archive or provenance checksum changed")
        with zipfile.ZipFile(archive) as z:
            names = z.namelist()
            if len(names) != 1:
                raise ValueError("Expected one original NSE bhavcopy per archive")
            source = pd.read_csv(z.open(names[0]))
        source.columns = source.columns.str.strip()
        source = source.rename(columns=SOURCE_COLUMNS)
        checked = group.merge(
            source[[*identity, *FIELDS]],
            on=identity,
            how="left",
            suffixes=("", "_nse"),
            validate="one_to_one",
        )
        if not all(checked[f].eq(checked[f + "_nse"]).all() for f in FIELDS):
            raise ValueError(f"NSE dataset differs from original exchange OHLCV on {day}")
    return frame


def prepare_nse_batch(
    frame: pd.DataFrame,
    *,
    identities: dict[str, dict],
    settings: Settings,
    database_engine,
    run_id: str,
    retrieved_at: datetime,
    eligible_sessions: set[date],
    source: str = NSE_SOURCE,
) -> ValidatedMarketDataBatch:
    """Retain raw provenance and validate canonical identity without Yahoo corrections."""
    if frame.empty:
        raise ValueError("NSE import batch must not be empty")
    missing = set(frame.symbol) - identities.keys()
    if missing:
        raise ValueError(f"Unmapped NSE securities: {sorted(missing)[:5]}")
    chosen = {s: identities[s] for s in set(frame.symbol)}
    ids = [v["canonical_instrument_id"] for v in chosen.values()]
    keys = [v["provider_instrument_key"] for v in chosen.values()]
    if (
        not all(ids)
        or not all(keys)
        or len(ids) != len(set(ids))
        or len(keys) != len(set(keys))
        or any(
            not frame.loc[frame.symbol.eq(s), "current_isin"].eq(v["source_identity"]).all()
            for s, v in chosen.items()
        )
    ):
        raise ValueError("NSE canonical identity is missing, ambiguous or changed")
    request = ProviderRequest(
        request_id=run_id,
        provider=source,
        exchange="NSE",
        interval=CandleInterval.ONE_DAY,
        window_start=datetime.combine(frame.date.min(), time.min, tzinfo=UTC),
        window_end=datetime.combine(frame.date.max() + timedelta(days=1), time.min, tzinfo=UTC),
        provider_symbols=tuple(sorted(chosen)),
        retrieved_at=retrieved_at,
        adapter_version="nse-as-traded-archive-v1",
        parameters={"price_basis": "as_traded", "volume_basis": "exchange_reported_shares"},
    )
    snapshot = _write_raw_snapshot_if_enabled(
        settings=settings,
        database_engine=database_engine,
        request=request,
        frame=frame,
    )
    candles = tuple(
        MarketCandle(
            instrument_id=identities[r.symbol]["canonical_instrument_id"],
            provider_symbol=r.symbol,
            symbol=r.symbol,
            exchange="NSE",
            session_date=r.date,
            **{f: Decimal(str(getattr(r, f))) for f in FIELDS[:4]},
            volume=int(r.volume),
            currency="INR",
            provider=source,
            provider_timestamp=retrieved_at,
            request_id=request.request_id,
            adapter_version=request.adapter_version,
            interval=CandleInterval.ONE_DAY,
            raw_artifact_id=snapshot.artifact_manifest_id if snapshot else None,
        )
        for r in frame.itertuples()
    )
    validation = validate_candle_batch(
        request,
        candles,
        eligible_sessions=eligible_sessions,
        observed_at=retrieved_at,
    )
    if not validation.valid:
        raise MarketDataValidationError(validation)
    normalized = frame.copy()
    normalized["instrument_key"] = normalized.symbol.map(
        lambda s: identities[s]["provider_instrument_key"]
    )
    normalized["quality_status"] = "ok"
    normalized["fetched_at"] = retrieved_at
    return ValidatedMarketDataBatch(request, normalized, candles, validation, snapshot)
