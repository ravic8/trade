"""Bounded-memory, repeatable-read audit and export of the full stored NSE history.

This is internal consistency validation, not a claim of independent exchange
verification. Every excluded row and every review flag remains inspectable.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text
from sqlalchemy.engine import Engine

from trade_data.market_data.price_quality import VALIDATION_VERSION


class DailyHistoryAuditor:
    def __init__(self, source_validation: pd.Series | None = None) -> None:
        self.source_validation = source_validation
        self.previous: tuple[str, date, float] | None = None
        self.counts: Counter[str] = Counter()
        self.coverage: dict[str, dict[str, Any]] = {}

    def annotate(self, frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.copy().reset_index(drop=True)
        if frame.empty:
            return frame
        keys = list(zip(frame.instrument_key, frame.date, strict=True))
        if any(a >= b for a, b in zip(keys, keys[1:], strict=False)) or (
            self.previous is not None and keys[0] <= self.previous[:2]
        ):
            raise ValueError("Daily history must have unique, ascending instrument/date keys")
        frame["exchange_verification"] = (
            self.source_validation.reindex(pd.MultiIndex.from_tuples(keys))
            .fillna("not_checked")
            .to_numpy()
            if self.source_validation is not None
            else "not_checked"
        )
        prices = frame[["open", "high", "low", "close"]].to_numpy(dtype=float)
        finite = np.isfinite(prices).all(axis=1)
        volume = frame.volume.to_numpy(dtype=float)
        masks = {
            "non_finite_price": ~finite,
            "non_positive_price": finite & (prices <= 0).any(axis=1),
            "invalid_ohlc_range": finite
            & ((prices[:, 1] < prices.max(axis=1)) | (prices[:, 2] > prices.min(axis=1))),
            "invalid_volume": ~np.isfinite(volume) | (volume < 0) | (volume != np.floor(volume)),
            "unvalidated_session": ~frame.session_valid.fillna(False).astype(bool).to_numpy(),
            "stored_quarantine": ~frame.quality_status.eq("ok").to_numpy(),
            "exchange_price_disagreement": frame.exchange_verification.eq(
                "exchange_price_disagreement"
            ).to_numpy(),
            "non_stock_instrument": (
                frame.symbol.str.replace(r"\.NS$", "", regex=True).str.contains(
                    r"-RE\d*$|-(?:PP|P1|P2)$", regex=True
                )
                | frame.exchange_verification.eq("non_stock_instrument")
            ).to_numpy(),
        }
        hard = np.logical_or.reduce(list(masks.values()))
        previous_close = frame.groupby("instrument_key", sort=False).close.shift()
        previous_date = frame.groupby("instrument_key", sort=False).date.shift()
        if self.previous is not None and self.previous[0] == keys[0][0]:
            previous_date.iloc[0], previous_close.iloc[0] = self.previous[1:]
        with np.errstate(divide="ignore", invalid="ignore"):
            returns = frame.close / previous_close - 1
        masks.update(
            {
                "zero_volume": volume == 0,
                "zero_volume_price_change": (volume == 0) & frame.open.ne(frame.close).to_numpy(),
                "extreme_return_review": returns.abs().gt(0.5).to_numpy(),
                "adjustment_unavailable": frame.adjustment_factor.isna().to_numpy(),
                "adjustment_basis_mismatch": (
                    frame.adjustment_raw_close.notna()
                    & ~np.isclose(frame.close, frame.adjustment_raw_close, rtol=1e-9, atol=1e-8)
                ).to_numpy(),
            }
        )
        if self.source_validation is not None:
            for reason in (
                "adjustment_basis_review",
                "volume_disagreement_review",
                "exchange_record_unavailable",
                "exchange_archive_unavailable",
                "not_checked",
            ):
                masks[reason] = frame.exchange_verification.eq(reason).to_numpy()
        flags = np.full(len(frame), "", dtype=object)
        for name, mask in masks.items():
            flags[mask] += name + ";"
            self.counts[name] += int(mask.sum())
        frame["quality_flags"] = pd.Series(flags).str.rstrip(";")
        frame["quality_status"] = np.where(
            hard, "quarantined", np.where(frame.quality_flags.ne(""), "valid_with_flags", "valid")
        )
        frame["validation_version"] = VALIDATION_VERSION
        frame["previous_date"] = previous_date.where(previous_date.notna(), None)
        frame["previous_close"] = previous_close
        frame["close_return"] = returns
        self.counts["rows"] += len(frame)
        self.counts["quarantined"] += int(hard.sum())
        self.counts["validated"] += int((~hard).sum())
        for key, group in frame.groupby("instrument_key", sort=False):
            item = self.coverage.setdefault(
                key,
                {
                    "instrument_key": key,
                    "symbol": group.symbol.iloc[0],
                    "first_date": group.date.iloc[0],
                    "last_date": group.date.iloc[-1],
                    "rows": 0,
                    "validated_rows": 0,
                    "quarantined_rows": 0,
                },
            )
            item["last_date"] = group.date.iloc[-1]
            item["rows"] += len(group)
            item["quarantined_rows"] += int(group.quality_status.eq("quarantined").sum())
            item["validated_rows"] = item["rows"] - item["quarantined_rows"]
        self.previous = (keys[-1][0], keys[-1][1], float(frame.close.iloc[-1]))
        return frame


def export_validated_daily_history(
    engine: Engine,
    *,
    destination: Path,
    start_date: date,
    end_date: date,
    chunk_rows: int = 50_000,
    source_validation: Path | None = None,
) -> Path:
    if engine.dialect.name != "postgresql":
        raise ValueError("PostgreSQL repeatable-read is required")
    if start_date > end_date or not 1 <= chunk_rows <= 100_000:
        raise ValueError("Invalid history date range or chunk size")
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_frame = None
    if source_validation is not None:
        source_frame = pd.read_parquet(
            source_validation, columns=["instrument_key", "date", "exchange_verification"]
        ).set_index(["instrument_key", "date"])
        if not source_frame.index.is_unique:
            raise ValueError("Source validation has duplicate instrument/date keys")
    stage = Path(tempfile.mkdtemp(prefix=".validated-history-", dir=destination.parent))
    auditor = DailyHistoryAuditor(
        source_frame["exchange_verification"] if source_frame is not None else None
    )
    schema = pa.schema(
        [
            *[pa.field(k, pa.string()) for k in ("instrument_key", "symbol", "exchange", "source")],
            pa.field("date", pa.date32()),
            *[pa.field(k, pa.float64()) for k in ("open", "high", "low", "close")],
            pa.field("volume", pa.int64()),
            pa.field("open_interest", pa.int64()),
            pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
            pa.field("quality_status", pa.string()),
            pa.field("exchange_verification", pa.string()),
            pa.field("session_valid", pa.bool_()),
            *[
                pa.field(k, pa.float64())
                for k in ("adjustment_raw_close", "adjusted_close", "adjustment_factor")
            ],
            pa.field("quality_flags", pa.string()),
            pa.field("validation_version", pa.string()),
            pa.field("previous_date", pa.date32()),
            pa.field("previous_close", pa.float64()),
            pa.field("close_return", pa.float64()),
        ]
    )
    names = ("nse_daily_validated.parquet", "quarantined.parquet", "review_flags.parquet")
    writers: list[pq.ParquetWriter] = []
    try:
        writers = [pq.ParquetWriter(stage / name, schema, compression="zstd") for name in names]
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as c:
            with c.begin():
                c.execute(text("SET TRANSACTION READ ONLY"))
                c.execute(text("SET LOCAL statement_timeout='10min'"))
                as_of = c.execute(text("SELECT transaction_timestamp()")).scalar_one()
                calendar = pd.DataFrame(
                    c.execute(
                        text("""
                    SELECT * FROM exchange_sessions WHERE exchange='NSE'
                    AND session_date BETWEEN :start AND :end ORDER BY session_date
                """),
                        {"start": start_date, "end": end_date},
                    )
                    .mappings()
                    .all()
                )
                calendar.to_parquet(stage / "sessions.parquet", index=False)
                rows = (
                    c.execution_options(stream_results=True)
                    .execute(
                        text("""
                    SELECT d.*, (s.is_trading_day AND s.validation_status LIKE 'valid%')
                        AS session_valid, a.raw_close AS adjustment_raw_close,
                        a.adjusted_close, a.adjustment_factor
                    FROM ohlcv_daily d
                    LEFT JOIN exchange_sessions s ON s.exchange=d.exchange AND s.session_date=d.date
                    LEFT JOIN price_adjustments_daily a ON a.instrument_key=d.instrument_key
                        AND a.source=d.source AND a.date=d.date
                    WHERE d.exchange='NSE' AND d.source='yfinance'
                        AND d.date BETWEEN :start AND :end
                    ORDER BY d.instrument_key,d.date
                """),
                        {"start": start_date, "end": end_date},
                    )
                    .mappings()
                )
                for chunk in rows.partitions(chunk_rows):
                    frame = auditor.annotate(pd.DataFrame(chunk))
                    quarantine = frame.quality_status.eq("quarantined")
                    for writer, mask in zip(
                        writers,
                        (~quarantine, quarantine, frame.quality_flags.ne("") & ~quarantine),
                        strict=True,
                    ):
                        writer.write_table(
                            pa.Table.from_pandas(
                                frame.loc[mask], schema=schema, preserve_index=False
                            )
                        )
        for writer in writers:
            writer.close()
        writers = []
        pd.DataFrame(auditor.coverage.values()).to_parquet(
            stage / "instrument_coverage.parquet", index=False
        )
        files = []
        for path in sorted(stage.glob("*.parquet")):
            with path.open("rb") as f:
                digest = hashlib.file_digest(f, "sha256").hexdigest()
            files.append(
                {
                    "path": path.name,
                    "sha256": digest,
                    "rows": pq.ParquetFile(path).metadata.num_rows,
                }
            )
        manifest = {
            "validation_version": VALIDATION_VERSION,
            "as_of": as_of.isoformat(),
            "created_at": datetime.now(UTC).isoformat(),
            "exchange": "NSE",
            "provider": "yfinance",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "counts": dict(auditor.counts),
            "files": files,
            "adjustment": "Yahoo auto_adjust=False OHLC; adjusted_close/factor are provider values",
            "verification": "Consistency checked; inspect exchange_verification and source audit",
            "source_validation_sha256": (
                hashlib.sha256(source_validation.read_bytes()).hexdigest()
                if source_validation is not None
                else None
            ),
            "limitations": [
                "Extreme returns and zero volume are flags, not automatic data errors",
                "Corporate actions are not inferred from return thresholds",
                "Stored observations do not prove a complete historical listed universe",
                "A missing session is not synthesized or forward-filled",
                "Review flags must be resolved before assuming research readiness",
            ],
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        stage.rename(destination)
        return destination / "manifest.json"
    except BaseException:
        for writer in writers:
            writer.close()
        shutil.rmtree(stage)
        raise


def quarantine_invalid_stored_history(
    engine: Engine, *, evidence_directory: Path, run_id: str
) -> dict[str, int]:
    """Preserve contradictory rows and record reasons before changing acceptance.

    Values and fetched_at are never rewritten. Optimistic predicates prevent
    overwriting a concurrently repaired candle. Repeating the same run is safe.
    """
    from trade_data.market_data.price_quality import daily_price_errors
    from trade_data.market_data.quality import (
        MarketDataQualityOutcome,
        MarketDataQualityRepository,
        MarketDataQualityStatus,
    )

    if engine.dialect.name != "postgresql":
        raise ValueError("Historical quarantine requires PostgreSQL")
    evidence_directory.mkdir(parents=True, exist_ok=True)
    non_finite = " OR ".join(
        f"d.{key} = ANY(ARRAY['NaN','Infinity','-Infinity']::double precision[])"
        for key in ("open", "high", "low", "close")
    )
    query = text(
        """
        SELECT d.*, s.canonical_instrument_id FROM ohlcv_daily d
        LEFT JOIN symbols s ON s.exchange=d.exchange AND s.yahoo_symbol=d.symbol
        WHERE d.exchange='NSE' AND d.source='yfinance' AND (
            least(d.open,d.high,d.low,d.close)<=0 OR d.volume<0
            OR d.high<greatest(d.open,d.low,d.close)
            OR d.low>least(d.open,d.high,d.close) OR """
        + non_finite
        + """
        ) ORDER BY d.instrument_key,d.date
    """
    )
    with engine.connect() as c:
        candidates = [dict(r) for r in c.execute(query).mappings()]
    # Atomic creation preserves the first observed before-state on every retry.
    evidence = evidence_directory / "invalid-before.jsonl"
    if not evidence.exists():
        with evidence.open("x") as f:
            for row in candidates:
                f.write(json.dumps(row, default=str) + "\n")
    observed_at = datetime.now(UTC)
    outcomes = []
    for row in candidates:
        for reason in daily_price_errors(row):
            outcomes.append(
                MarketDataQualityOutcome(
                    source_run_id=run_id,
                    request_id=run_id,
                    provider="yfinance",
                    exchange="NSE",
                    interval="1d",
                    instrument_id=row["canonical_instrument_id"] or row["instrument_key"],
                    provider_symbol=row["symbol"],
                    session_date=row["date"],
                    status=MarketDataQualityStatus.INVALID,
                    reason_code=reason,
                    severity="error",
                    expected=True,
                    retryable=False,
                    observed_at=observed_at,
                    details={
                        "validation_version": VALIDATION_VERSION,
                        "before": json.loads(json.dumps(row, default=str)),
                    },
                )
            )
    MarketDataQualityRepository(engine).record(outcomes)
    changed = 0
    with engine.begin() as c:
        for row in candidates:
            if not daily_price_errors(row):
                continue
            changed += c.execute(
                text("""
                UPDATE ohlcv_daily SET quality_status='suspicious'
                WHERE instrument_key=:instrument_key AND source=:source AND date=:date
                AND fetched_at=:fetched_at AND quality_status='ok'
                AND open IS NOT DISTINCT FROM :open AND high IS NOT DISTINCT FROM :high
                AND low IS NOT DISTINCT FROM :low AND close IS NOT DISTINCT FROM :close
                AND volume=:volume
            """),
                row,
            ).rowcount
    result = {"candidates": len(candidates), "outcomes": len(outcomes), "changed": changed}
    (evidence_directory / "quarantine-result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result
