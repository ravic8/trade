"""Bounded, in-memory official NSE final bhavcopy parsing; no provider fallback."""

from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

import httpx
import pandas as pd

SOURCE = "nse_bhavcopy"
BASE = "https://nsearchives.nseindia.com/content/cm/"
SERIES = frozenset({"EQ", "BE", "BZ", "SM", "ST", "SZ"})
FIELDS = {"OpnPric": "open", "HghPric": "high", "LwPric": "low", "ClsPric": "close"}
MAX_BYTES = 20_000_000


class BhavcopyUnavailable(RuntimeError):
    """The official final file has not been published or cannot be retrieved."""

    def __init__(self, message: str, *, not_found: bool = False):
        super().__init__(message)
        self.not_found = not_found


@dataclass(frozen=True)
class Bhavcopy:
    session: date
    url: str
    sha256: str
    frame: pd.DataFrame
    excluded: dict[str, int]
    total_records: int


def archive_url(session: date) -> str:
    return BASE + f"BhavCopy_NSE_CM_0_0_0_{session:%Y%m%d}_F_0000.csv.zip"


def exclusion(row: dict[str, str]) -> str | None:
    if row["FinInstrmTp"] != "STK" or row["SctySrs"] not in SERIES:
        return "non_equity_series"
    if not re.fullmatch(r"IN[E9][A-Z0-9]{9}", row["ISIN"]) or row["ISIN"][7:9] != "01":
        return "non_ordinary_equity_isin"
    if re.search(r"-RE\d*$|-(?:PP|P1|P2)$", row["TckrSymb"]) or re.search(
        r"RIGHTS? ENTITLEMENT|PARTLY PAID|EXCHANGE TRADED|\bETF\b", row["FinInstrmNm"]
    ):
        return "rights_partly_paid_or_fund"
    return None


def parse_bhavcopy(raw: bytes, session: date) -> Bhavcopy:
    if not raw or len(raw) > MAX_BYTES:
        raise ValueError("Unexpected NSE archive size")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        names = archive.infolist()
        expected = f"BhavCopy_NSE_CM_0_0_0_{session:%Y%m%d}_F_0000.csv"
        if len(names) != 1 or names[0].filename != expected or names[0].file_size > MAX_BYTES:
            raise ValueError("Unexpected final bhavcopy archive member")
        text = archive.read(names[0]).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    required = {
        "TradDt",
        "Sgmt",
        "Src",
        "FinInstrmTp",
        "TckrSymb",
        "ISIN",
        "SctySrs",
        "FinInstrmNm",
        "TtlTradgVol",
        *FIELDS,
    }
    if not required.issubset(reader.fieldnames or []):
        raise ValueError("NSE bhavcopy schema changed")
    accepted, excluded, identities = [], Counter(), set()
    total = 0
    for source in reader:
        total += 1
        row = {k: (v or "").strip() for k, v in source.items()}
        if row["TradDt"] != session.isoformat() or row["Sgmt"] != "CM" or row["Src"] != "NSE":
            raise ValueError("NSE file contains unexpected session, segment or exchange")
        reason = exclusion(row)
        if reason:
            excluded[reason] += 1
            continue
        identity = (row["TckrSymb"], row["ISIN"])
        if not identity[0] or identity in identities:
            raise ValueError("Ambiguous duplicate NSE equity identity/series")
        identities.add(identity)
        try:
            prices = {dest: Decimal(row[src]) for src, dest in FIELDS.items()}
            volume = Decimal(row["TtlTradgVol"])
        except InvalidOperation as exc:
            raise ValueError(f"Invalid NSE numeric value: {identity[0]}") from exc
        if not (
            all(v.is_finite() and v > 0 for v in prices.values())
            and prices["high"] >= max(prices.values())
            and prices["low"] <= min(prices.values())
            and volume.is_finite()
            and volume >= 0
            and volume == volume.to_integral_value()
            and volume <= 2**63 - 1
        ):
            raise ValueError(f"Invalid NSE OHLCV: {identity[0]}")
        accepted.append(
            {
                "exchange_symbol": identity[0],
                "isin": identity[1],
                "series": row["SctySrs"],
                "name": row["FinInstrmNm"],
                "date": session,
                **prices,
                "volume": int(volume),
            }
        )
    if not accepted or total != len(accepted) + sum(excluded.values()):
        raise ValueError("Empty or unaccounted NSE source records")
    return Bhavcopy(
        session,
        archive_url(session),
        hashlib.sha256(raw).hexdigest(),
        pd.DataFrame(accepted),
        dict(excluded),
        total,
    )


def fetch_bhavcopy(session: date) -> Bhavcopy:
    # Retry on the next durable scheduled run, never fan out or bypass exchange limits.
    try:
        with httpx.Client(timeout=45, headers={"User-Agent": "Mozilla/5.0"}) as client:
            with client.stream("GET", archive_url(session)) as response:
                response.raise_for_status()
                raw = bytearray()
                for chunk in response.iter_bytes():
                    raw.extend(chunk)
                    if len(raw) > MAX_BYTES:
                        raise ValueError("Oversized NSE response")
    except httpx.HTTPError as exc:
        raise BhavcopyUnavailable(
            f"Official NSE final file unavailable for {session}",
            not_found=isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404,
        ) from exc
    return parse_bhavcopy(bytes(raw), session)


def rolling_year_start(end: date) -> date:
    """Same inclusive one-year convention as the retained 24 Oct–23 Oct snapshot."""
    anniversary = (
        end.replace(year=end.year - 1, day=28)
        if (end.month, end.day) == (2, 29)
        else end.replace(year=end.year - 1)
    )
    return anniversary + timedelta(days=1)


def probe_special_session(session: date, *, loader=fetch_bhavcopy) -> Bhavcopy | None:
    """A closed-day 404 is normal; denied access and outages are not proof of closure."""
    try:
        return loader(session)
    except BhavcopyUnavailable as exc:
        if exc.not_found:
            return None
        raise
