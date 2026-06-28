from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED_PATH = ROOT / "data/processed/equities/nse_daily_ohlcv_upstox.parquet"
AUDIT_PATH = ROOT / "data/processed/equities/nse_daily_ohlcv_upstox_audit.csv"
FAILURES_PATH = ROOT / "data/processed/equities/nse_daily_ohlcv_upstox_failures.csv"
SKIPPED_PATH = ROOT / "data/processed/equities/nse_daily_ohlcv_upstox_skipped.csv"
INSTRUMENTS_PATH = ROOT / "data/processed/instruments/upstox_instruments.parquet"
MAPPING_PATH = ROOT / "data/processed/universe/liquid_nse_upstox_mapping.csv"
VALIDATION_DIR = ROOT / "data/processed/validation"


def main() -> None:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

    processed = pd.read_parquet(PROCESSED_PATH)
    instruments = pd.read_parquet(INSTRUMENTS_PATH)
    mapping = pd.read_csv(MAPPING_PATH)
    audit = _read_csv(AUDIT_PATH)
    failures = _read_csv(FAILURES_PATH)
    skipped = _read_csv(SKIPPED_PATH)

    normalized = _normalize_processed(processed)
    invalid_rows = _invalid_rows(normalized, instruments, mapping)
    symbol_health = _symbol_health(normalized, invalid_rows, mapping, instruments)
    date_coverage = _date_coverage(normalized, len(mapping))
    raw_files = _find_raw_response_files()
    audit_compare = _compare_existing_audit(audit, symbol_health)

    invalid_output = VALIDATION_DIR / "processed_ohlcv_invalid_rows.parquet"
    health_output = VALIDATION_DIR / "processed_ohlcv_symbol_health.parquet"
    coverage_output = VALIDATION_DIR / "processed_ohlcv_date_coverage.parquet"
    metadata_output = VALIDATION_DIR / "raw_to_processed_metadata.json"
    report_output = VALIDATION_DIR / "raw_to_processed_validation_report.md"

    invalid_rows.to_parquet(invalid_output, index=False)
    symbol_health.to_parquet(health_output, index=False)
    date_coverage.to_parquet(coverage_output, index=False)

    metadata = {
        "generated_at": datetime.now(UTC).isoformat(),
        "raw_response_files": [str(path.relative_to(ROOT)) for path in raw_files],
        "raw_response_files_found": len(raw_files),
        "full_raw_replay_possible": bool(raw_files),
        "processed_path": str(PROCESSED_PATH.relative_to(ROOT)),
        "processed_rows": int(len(normalized)),
        "processed_symbols": int(normalized["InstrumentKey"].nunique(dropna=True)),
        "processed_date_min": _json_date(normalized["parsed_date"].min()),
        "processed_date_max": _json_date(normalized["parsed_date"].max()),
        "mapping_path": str(MAPPING_PATH.relative_to(ROOT)),
        "mapped_instruments": int(mapping["instrument_key"].nunique(dropna=True)),
        "instrument_master_path": str(INSTRUMENTS_PATH.relative_to(ROOT)),
        "instrument_master_rows": int(len(instruments)),
        "failures_path": str(FAILURES_PATH.relative_to(ROOT)),
        "failure_rows": int(len(failures)),
        "skipped_path": str(SKIPPED_PATH.relative_to(ROOT)),
        "skipped_rows": int(len(skipped)),
        "invalid_rows": int(len(invalid_rows)),
        "negative_volume_rows": int((normalized["Volume"] < 0).sum()),
        "duplicate_instrument_date_rows": int(
            normalized.duplicated(["InstrumentKey", "parsed_date"], keep=False).sum()
        ),
        "unknown_instrument_key_rows": int(
            (
                ~normalized["InstrumentKey"].astype(str).isin(
                    set(instruments["instrument_key"].dropna().astype(str))
                )
            ).sum()
        ),
        "symbols_missing_processed_rows": int(
            symbol_health["processed_rows"].fillna(0).eq(0).sum()
        ),
        "symbols_ending_before_latest_dataset_date": int(
            symbol_health["ends_before_dataset_latest_date"].sum()
        ),
        "low_coverage_dates": int(date_coverage["unusually_low_coverage"].sum()),
        "existing_audit_false_passes": int(audit_compare["false_passes"]),
        "existing_audit_false_warnings": int(audit_compare["false_warnings"]),
        "existing_audit_false_failures": int(audit_compare["false_failures"]),
        "safe_for_features_targets": bool(len(invalid_rows) == 0),
    }
    metadata_output.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    report_output.write_text(
        _build_report(
            metadata=metadata,
            normalized=normalized,
            invalid_rows=invalid_rows,
            symbol_health=symbol_health,
            date_coverage=date_coverage,
            audit=audit,
            failures=failures,
            skipped=skipped,
            audit_compare=audit_compare,
        ),
        encoding="utf-8",
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _find_raw_response_files() -> list[Path]:
    candidates: list[Path] = []
    suffixes = {".json", ".jsonl", ".gz"}
    for path in (ROOT / "data").rglob("*"):
        if not path.is_file() or path.suffix.lower() not in suffixes:
            continue
        parts = {part.lower() for part in path.relative_to(ROOT).parts}
        name = path.name.lower()
        raw_named = any(
            token in name
            for token in (
                "raw",
                "response",
                "payload",
                "historical-candle",
                "daily-candle",
                "candles",
            )
        )
        if ("raw" in parts or "responses" in parts) and raw_named:
            candidates.append(path)
    return sorted(candidates)


def _normalize_processed(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["parsed_datetime"] = pd.to_datetime(out["Date"], errors="coerce")
    out["parsed_date"] = out["parsed_datetime"].dt.date
    for column in ["Open", "High", "Low", "Close", "Volume", "OpenInterest"]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in ["InstrumentKey", "Symbol", "TradingSymbol", "Source"]:
        if column in out.columns:
            out[column] = out[column].astype("string")
    return out


def _invalid_rows(
    frame: pd.DataFrame,
    instruments: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    known_instruments = set(instruments["instrument_key"].dropna().astype(str))
    mapped_instruments = set(mapping["instrument_key"].dropna().astype(str))
    duplicate_mask = frame.duplicated(["InstrumentKey", "parsed_date"], keep=False)

    checks = {
        "missing_instrument_key": frame["InstrumentKey"].isna() | frame["InstrumentKey"].eq(""),
        "missing_symbol": frame["Symbol"].isna() | frame["Symbol"].eq(""),
        "missing_trading_symbol": frame["TradingSymbol"].isna() | frame["TradingSymbol"].eq(""),
        "invalid_date": frame["parsed_date"].isna(),
        "unknown_upstox_instrument": ~frame["InstrumentKey"].astype(str).isin(known_instruments),
        "not_in_liquid_mapping": ~frame["InstrumentKey"].astype(str).isin(mapped_instruments),
        "duplicate_instrument_date": duplicate_mask,
        "missing_ohlcv": frame[["Open", "High", "Low", "Close", "Volume"]].isna().any(axis=1),
        "open_not_positive": frame["Open"].isna() | (frame["Open"] <= 0),
        "high_not_positive": frame["High"].isna() | (frame["High"] <= 0),
        "low_not_positive": frame["Low"].isna() | (frame["Low"] <= 0),
        "close_not_positive": frame["Close"].isna() | (frame["Close"] <= 0),
        "high_below_low": frame["High"] < frame["Low"],
        "high_below_open": frame["High"] < frame["Open"],
        "high_below_close": frame["High"] < frame["Close"],
        "low_above_open": frame["Low"] > frame["Open"],
        "low_above_close": frame["Low"] > frame["Close"],
        "missing_volume": frame["Volume"].isna(),
        "negative_volume": frame["Volume"] < 0,
    }

    invalid_mask = pd.Series(False, index=frame.index)
    reasons = pd.Series("", index=frame.index, dtype="string")
    for reason, mask in checks.items():
        mask = mask.fillna(False)
        invalid_mask |= mask
        reasons.loc[mask] = reasons.loc[mask].where(
            reasons.loc[mask].eq(""),
            reasons.loc[mask] + ";",
        ) + reason

    columns = [
        "Date",
        "parsed_date",
        "InstrumentKey",
        "Symbol",
        "TradingSymbol",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "OpenInterest",
        "Source",
    ]
    invalid = frame.loc[invalid_mask, columns].copy()
    invalid["invalid_reasons"] = reasons.loc[invalid_mask].to_numpy()
    return invalid.sort_values(["InstrumentKey", "parsed_date"]).reset_index(drop=True)


def _symbol_health(
    frame: pd.DataFrame,
    invalid_rows: pd.DataFrame,
    mapping: pd.DataFrame,
    instruments: pd.DataFrame,
) -> pd.DataFrame:
    latest_date = frame["parsed_date"].max()
    grouped = frame.groupby("InstrumentKey", dropna=False)
    health = grouped.agg(
        symbol=("Symbol", "first"),
        trading_symbol=("TradingSymbol", "first"),
        processed_rows=("parsed_date", "size"),
        start_date=("parsed_date", "min"),
        end_date=("parsed_date", "max"),
        unique_dates=("parsed_date", "nunique"),
        duplicate_date_rows=("parsed_date", lambda s: int(s.duplicated(keep=False).sum())),
        zero_volume_rows=("Volume", lambda s: int((s == 0).sum())),
        negative_volume_rows=("Volume", lambda s: int((s < 0).sum())),
    ).reset_index()

    invalid_counts = (
        invalid_rows.groupby("InstrumentKey")
        .size()
        .rename("invalid_rows")
        .reset_index()
    )
    health = health.merge(invalid_counts, on="InstrumentKey", how="left")
    health["invalid_rows"] = health["invalid_rows"].fillna(0).astype(int)

    mapped = mapping[["symbol", "instrument_key", "trading_symbol"]].drop_duplicates()
    mapped = mapped.rename(
        columns={
            "symbol": "mapped_symbol",
            "instrument_key": "InstrumentKey",
            "trading_symbol": "mapped_trading_symbol",
        }
    )
    health = mapped.merge(health, on="InstrumentKey", how="left")

    known = set(instruments["instrument_key"].dropna().astype(str))
    health["known_upstox_instrument"] = health["InstrumentKey"].astype(str).isin(known)
    health["has_processed_rows"] = health["processed_rows"].fillna(0).gt(0)
    health["processed_rows"] = health["processed_rows"].fillna(0).astype(int)
    health["invalid_rows"] = health["invalid_rows"].fillna(0).astype(int)
    health["ends_before_dataset_latest_date"] = (
        health["has_processed_rows"] & health["end_date"].lt(latest_date)
    )
    health["coverage_status"] = "passed"
    health.loc[~health["has_processed_rows"], "coverage_status"] = "missing"
    health.loc[health["invalid_rows"].gt(0), "coverage_status"] = "invalid"
    health.loc[
        health["ends_before_dataset_latest_date"] & health["coverage_status"].eq("passed"),
        "coverage_status",
    ] = "ended_before_latest"
    return health.sort_values(["coverage_status", "mapped_symbol"]).reset_index(drop=True)


def _date_coverage(frame: pd.DataFrame, expected_symbols: int) -> pd.DataFrame:
    coverage = (
        frame.groupby("parsed_date")
        .agg(
            processed_rows=("InstrumentKey", "size"),
            symbols_with_rows=("InstrumentKey", "nunique"),
            duplicate_rows=("InstrumentKey", lambda s: int(s.duplicated(keep=False).sum())),
            invalid_price_rows=("Close", lambda s: int((s <= 0).sum())),
            negative_volume_rows=("Volume", lambda s: int((s < 0).sum())),
            zero_volume_rows=("Volume", lambda s: int((s == 0).sum())),
        )
        .reset_index()
        .sort_values("parsed_date")
    )
    coverage["expected_mapped_symbols"] = expected_symbols
    coverage["coverage_pct"] = coverage["symbols_with_rows"] / max(expected_symbols, 1)
    fullish_coverage = coverage["symbols_with_rows"].quantile(0.75)
    low_threshold = max(1, int(fullish_coverage * 0.95))
    coverage["unusually_low_coverage"] = coverage["symbols_with_rows"] < low_threshold
    coverage["low_coverage_threshold"] = low_threshold
    return coverage.reset_index(drop=True)


def _compare_existing_audit(audit: pd.DataFrame, health: pd.DataFrame) -> dict[str, int]:
    if audit.empty:
        return {
            "audit_rows": 0,
            "false_passes": 0,
            "false_warnings": 0,
            "false_failures": 0,
            "audit_missing_symbols": int(len(health)),
        }

    compare = audit.merge(
        health,
        left_on="instrument_key",
        right_on="InstrumentKey",
        how="outer",
        suffixes=("_audit", "_computed"),
    )
    computed_failed = (
        compare["processed_rows"].fillna(0).eq(0)
        | compare["invalid_rows"].fillna(0).gt(0)
        | compare["duplicate_date_rows_computed"].fillna(0).gt(0)
        | ~compare["known_upstox_instrument"].fillna(False)
    )
    computed_warning = (
        compare["ends_before_dataset_latest_date"].fillna(False)
        | compare["zero_volume_rows_computed"].fillna(0).gt(0)
    ) & ~computed_failed
    status = compare["status"].fillna("missing")
    return {
        "audit_rows": int(len(audit)),
        "false_passes": int(status.eq("passed").where(computed_failed, False).sum()),
        "false_warnings": int(status.eq("warning").where(~computed_warning, False).sum()),
        "false_failures": int(status.eq("failed").where(~computed_failed, False).sum()),
        "audit_missing_symbols": int(status.eq("missing").sum()),
    }


def _build_report(
    metadata: dict[str, object],
    normalized: pd.DataFrame,
    invalid_rows: pd.DataFrame,
    symbol_health: pd.DataFrame,
    date_coverage: pd.DataFrame,
    audit: pd.DataFrame,
    failures: pd.DataFrame,
    skipped: pd.DataFrame,
    audit_compare: dict[str, int],
) -> str:
    latest_date = metadata["processed_date_max"]
    row_counts = (
        normalized.groupby(["InstrumentKey", "Symbol"])
        .size()
        .rename("processed_rows")
        .reset_index()
        .sort_values(["InstrumentKey", "Symbol"])
    )
    count_mismatches = symbol_health[
        symbol_health["processed_rows"].fillna(0).astype(int).ne(
            symbol_health["unique_dates"].fillna(0).astype(int)
        )
    ]
    missing_symbols = symbol_health[~symbol_health["has_processed_rows"]]
    ended_early = symbol_health[symbol_health["ends_before_dataset_latest_date"]]
    low_coverage = date_coverage[date_coverage["unusually_low_coverage"]]

    lines = [
        "# Raw to Processed Upstox OHLCV Validation",
        "",
        "## Verdict",
        "",
        (
            "The processed OHLCV dataset is safe to use for downstream feature and "
            "target generation."
            if metadata["safe_for_features_targets"]
            else "The processed OHLCV dataset is not safe to use until invalid rows are fixed."
        ),
        "",
        "Full raw-to-processed replay validation is not possible from this checkout "
        "because no saved raw Upstox candle API response files were found under `data/`.",
        "",
        "## Raw Files",
        "",
        f"- Raw Upstox response files found: {metadata['raw_response_files_found']}",
        "- Raw candle counts per instrument: unavailable",
        "- Total raw candles: unavailable",
        "- Rows dropped from raw payloads: cannot be replayed without raw files",
        "",
        "## Conversion Code Inspected",
        "",
        "- `src/trade_research/data/upstox.py`: `UpstoxHistoricalDataProvider.fetch_daily_candles` calls the v3 `/historical-candle/.../days/1` endpoint and passes `payload['data']['candles']` to `_daily_candles_to_frame`.",
        "- `_daily_candles_to_frame` maps candle `[0]` to `Date`, `[1]` to `Open`, `[2]` to `High`, `[3]` to `Low`, `[4]` to `Close`, `[5]` to `Volume`, and `[6]` to `OpenInterest` when present.",
        "- `src/trade_research/cli.py`: `fetch-upstox-nse-daily` concatenates per-symbol DataFrames and writes `nse_daily_ohlcv_upstox.parquet`; it writes failures and audit CSVs but not raw JSON payloads.",
        "- `src/trade_research/storage/timescale.py`: DB conversion preserves `Date`, `InstrumentKey`, `Symbol`, OHLCV, and `OpenInterest`, while dropping rows only when one of `Date/Open/High/Low/Close/Volume` is null.",
        "",
        "## Processed Dataset",
        "",
        f"- File: `{metadata['processed_path']}`",
        f"- Rows: {metadata['processed_rows']}",
        f"- Instruments with rows: {metadata['processed_symbols']}",
        f"- Date range: {metadata['processed_date_min']} to {latest_date}",
        f"- Expected mapped instruments: {metadata['mapped_instruments']}",
        f"- Fetch failures: {metadata['failure_rows']}",
        f"- Skipped/current symbols: {metadata['skipped_rows']}",
        "",
        "## Row Reconciliation",
        "",
        "- Raw candle count vs processed row count: not replay-verifiable because raw responses were not saved.",
        f"- Processed total rows: {len(normalized)}",
        f"- Sum of processed rows per instrument: {int(row_counts['processed_rows'].sum())}",
        f"- Per-instrument duplicate/date count mismatches: {len(count_mismatches)}",
        f"- Processed rows missing from mapped universe: {int((~normalized['InstrumentKey'].isin(set(symbol_health['InstrumentKey']))).sum())}",
        "",
        "## Invalid Rows",
        "",
        f"- Invalid processed rows: {len(invalid_rows)}",
        f"- Duplicate `InstrumentKey + Date` rows: {metadata['duplicate_instrument_date_rows']}",
        f"- Negative volume rows: {metadata['negative_volume_rows']}",
        f"- Unknown Upstox instrument rows: {metadata['unknown_instrument_key_rows']}",
        "- Negative volume is treated as invalid.",
        "",
        "## Coverage",
        "",
        f"- Instruments missing processed rows: {len(missing_symbols)}",
        f"- Symbols ending before latest dataset date ({latest_date}): {len(ended_early)}",
        f"- Dates with unusually low symbol coverage: {len(low_coverage)}",
        "",
        "## Existing Audit Comparison",
        "",
        f"- Existing audit rows: {audit_compare['audit_rows']}",
        f"- Existing audit false passes: {audit_compare['false_passes']}",
        f"- Existing audit false warnings: {audit_compare['false_warnings']}",
        f"- Existing audit false failures: {audit_compare['false_failures']}",
        "- Note: the existing audit warns on zero volume but does not separately flag negative volume; this validation treats negative volume as invalid.",
        "",
        "## Outputs",
        "",
        "- `data/processed/validation/raw_to_processed_validation_report.md`",
        "- `data/processed/validation/processed_ohlcv_invalid_rows.parquet`",
        "- `data/processed/validation/processed_ohlcv_symbol_health.parquet`",
        "- `data/processed/validation/processed_ohlcv_date_coverage.parquet`",
        "- `data/processed/validation/raw_to_processed_metadata.json`",
    ]

    if not failures.empty:
        lines.extend(["", "## Fetch Failures", "", _markdown_table(failures)])
    if not missing_symbols.empty:
        lines.extend(
            [
                "",
                "## Missing Symbols",
                "",
                _markdown_table(
                    missing_symbols[["mapped_symbol", "InstrumentKey", "coverage_status"]]
                ),
            ]
        )
    if not ended_early.empty:
        sample = ended_early[
            ["mapped_symbol", "InstrumentKey", "processed_rows", "end_date"]
        ].head(30)
        lines.extend(["", "## Symbols Ending Early Sample", "", _markdown_table(sample)])
    if not low_coverage.empty:
        sample = low_coverage[
            ["parsed_date", "symbols_with_rows", "expected_mapped_symbols", "coverage_pct"]
        ].head(30)
        lines.extend(["", "## Low Coverage Dates Sample", "", _markdown_table(sample)])
    if not skipped.empty:
        skipped_counts = skipped["skip_reason"].fillna("").value_counts().reset_index()
        skipped_counts.columns = ["skip_reason", "rows"]
        lines.extend(["", "## Skipped Current Summary", "", _markdown_table(skipped_counts)])

    return "\n".join(lines) + "\n"


def _json_date(value: object) -> str | None:
    if pd.isna(value):
        return None
    return str(value)


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_None._"
    text = frame.copy()
    text = text.astype(object).where(pd.notna(text), "")
    headers = [str(column) for column in text.columns]
    rows = [[str(value) for value in row] for row in text.to_numpy().tolist()]
    separator = ["---"] * len(headers)
    rendered = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(separator) + " |",
    ]
    rendered.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(rendered)


if __name__ == "__main__":
    main()
