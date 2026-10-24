"""Read-only comparison of stored Yahoo bars with preserved official NSE bhavcopies.

Run inside the configured backend environment after downloading the exchange
archives. No provider prices or database states are changed by this command.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import zipfile
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text

from trade_data.config import get_settings
from trade_data.storage.timescale import TimescaleStore


def compare_session(stored: pd.DataFrame, official: pd.DataFrame) -> pd.DataFrame:
    official.columns = official.columns.str.strip()
    official = official.rename(
        columns={
            "TckrSymb": "SYMBOL",
            "SctySrs": "SERIES",
            "OpnPric": "OPEN",
            "HghPric": "HIGH",
            "LwPric": "LOW",
            "ClsPric": "CLOSE",
            "TtlTradgVol": "TOTTRDQTY",
        }
    )
    official = official[official.SERIES.isin(["EQ", "BE", "BZ", "SM", "ST", "SZ"])].copy()
    if official.SYMBOL.duplicated().any():
        # An ambiguous security/series must not silently select one observation.
        official = official[~official.SYMBOL.duplicated(keep=False)]
    official = official.set_index("SYMBOL")
    symbols = stored.symbol.str.replace(r"\.NS$", "", regex=True)
    source = official.reindex(symbols)
    source.index = stored.index
    y = stored[["open", "high", "low", "close"]].to_numpy(dtype=float)
    n = source[["OPEN", "HIGH", "LOW", "CLOSE"]].to_numpy(dtype=float)
    exists = source.OPEN.notna().to_numpy()
    matches = np.isclose(y, n, rtol=1e-7, atol=0.011).all(axis=1)
    volumes = stored.volume.eq(source.TOTTRDQTY).to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        ratios = y / n
        factor = np.median(ratios, axis=1)
        coherent = np.isclose(y, n * factor[:, None], rtol=1e-5, atol=0.011).all(axis=1)
    status = np.full(len(stored), "exchange_price_disagreement", dtype=object)
    status[coherent] = "adjustment_basis_review"
    status[matches & ~volumes] = "volume_disagreement_review"
    status[matches & volumes] = "exchange_match"
    status[~exists] = "exchange_record_unavailable"
    if "ISIN" in source:
        non_stock = source.ISIN.fillna("").str.startswith("INF").to_numpy()
        status[non_stock] = "non_stock_instrument"
    result = stored[["instrument_key", "symbol", "date"]].copy()
    result["exchange_verification"] = status
    result["exchange_series"] = source.SERIES.fillna("")
    result["source_price_factor"] = factor
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-directory", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    args.destination.mkdir(parents=True, exist_ok=False)
    path = args.destination / "source-validation.parquet"
    schema = pa.schema(
        [
            pa.field("instrument_key", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("date", pa.date32()),
            pa.field("exchange_verification", pa.string()),
            pa.field("exchange_series", pa.string()),
            pa.field("source_price_factor", pa.float64()),
            pa.field("evidence_url", pa.string()),
            pa.field("evidence_sha256", pa.string()),
        ]
    )
    counts = Counter()
    evidence = []
    engine = TimescaleStore(get_settings().database_url).engine
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as c:
        with c.begin(), pq.ParquetWriter(path, schema, compression="zstd") as writer:
            c.execute(text("SET TRANSACTION READ ONLY"))
            as_of = c.execute(text("SELECT transaction_timestamp()")).scalar_one()
            days = (
                c.execute(
                    text(
                        "SELECT DISTINCT date FROM ohlcv_daily WHERE exchange='NSE' "
                        "AND source='yfinance' AND date BETWEEN :start AND :end ORDER BY date"
                    ),
                    {"start": args.start_date, "end": args.end_date},
                )
                .scalars()
                .all()
            )
            for i, day in enumerate(days):
                stored = pd.DataFrame(
                    c.execute(
                        text(
                            "SELECT instrument_key,symbol,date,open,high,low,close,volume "
                            "FROM ohlcv_daily WHERE exchange='NSE' "
                            "AND source='yfinance' AND date=:day ORDER BY instrument_key"
                        ),
                        {"day": day},
                    )
                    .mappings()
                    .all()
                )
                metadata = args.archive_directory / (str(day) + ".json")
                archive = args.archive_directory / (str(day) + ".zip")
                meta = json.loads(metadata.read_text()) if metadata.exists() else {}
                if archive.exists() and meta.get("http_status") == 200:
                    raw = archive.read_bytes()
                    digest = hashlib.sha256(raw).hexdigest()
                    if digest != meta["sha256"]:
                        raise ValueError("Exchange archive checksum mismatch: " + str(day))
                    with zipfile.ZipFile(io.BytesIO(raw)) as z:
                        official = pd.read_csv(z.open(z.namelist()[0]))
                    result = compare_session(stored, official)
                    evidence.append({"date": str(day), "url": meta["url"], "sha256": digest})
                else:
                    result = stored[["instrument_key", "symbol", "date"]].copy()
                    result["exchange_verification"] = "exchange_archive_unavailable"
                    result["exchange_series"] = ""
                    result["source_price_factor"] = np.nan
                result["evidence_url"] = meta.get("url", "")
                result["evidence_sha256"] = meta.get("sha256", "")
                counts.update(result.exchange_verification)
                writer.write_table(
                    pa.Table.from_pandas(result, schema=schema, preserve_index=False)
                )
                if i % 50 == 0:
                    (args.destination / "progress.json").write_text(
                        json.dumps({"done": i + 1, "total": len(days), "counts": dict(counts)})
                    )
    manifest = {
        "as_of": str(as_of),
        "counts": dict(counts),
        "sessions": len(days),
        "verified_archives": len(evidence),
        "archive_evidence": evidence,
        "rows": pq.ParquetFile(path).metadata.num_rows,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "scope": "Stored Yahoo vs official NSE prices; coherent ratios need action review",
    }
    (args.destination / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({k: v for k, v in manifest.items() if k != "archive_evidence"}))


if __name__ == "__main__":
    main()
