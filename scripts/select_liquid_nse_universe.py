from __future__ import annotations

# ruff: noqa: E402, I001

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trade_research.data.yahoo import YahooFinanceMarketDataProvider  # noqa: E402
from trade_research.universe.nse import NSEUniverseProvider  # noqa: E402


DEFAULT_OUTPUT = ROOT / "data/processed/universe/liquid_nse_stocks.csv"
DEFAULT_AUDIT_OUTPUT = ROOT / "data/processed/universe/liquid_nse_stock_audit.csv"
DEFAULT_SUMMARY_OUTPUT = ROOT / "data/processed/universe/liquid_nse_universe_summary.json"


@dataclass(frozen=True)
class AuditSummary:
    generated_at: str
    universe_size: int
    tickers_requested: int
    tickers_with_data: int
    output_rows: int
    start_date: str
    end_date: str
    top_n: int
    min_trading_days: int
    max_zero_volume_ratio: float
    min_avg_daily_turnover: float | None
    null_rows: int
    duplicate_ticker_date_rows: int


def main() -> None:
    args = _parse_args()
    end = _parse_date(args.end_date) if args.end_date else date.today()
    start = (
        _parse_date(args.start_date)
        if args.start_date
        else end - timedelta(days=args.lookback_days)
    )

    symbols = NSEUniverseProvider().fetch()
    if args.limit:
        symbols = symbols[: args.limit]

    tickers = [symbol.yahoo_symbol for symbol in symbols if symbol.yahoo_symbol]
    provider = YahooFinanceMarketDataProvider(
        batch_size=args.batch_size,
        throttle_seconds=args.throttle_seconds,
        max_workers=args.max_workers,
        retry_attempts=args.retry_attempts,
    )
    raw = provider.fetch_daily_ohlcv(tickers=tickers, start=start, end=end)
    if raw.empty:
        raise SystemExit("No yfinance OHLCV rows returned; cannot select a liquid universe.")

    prepared = _prepare_daily_frame(raw)
    audit = _audit_liquidity_frame(prepared)
    profile = _liquidity_profile(prepared, audit)
    profile = profile.merge(
        _symbol_master_frame(symbols),
        how="left",
        left_on="ticker",
        right_on="yahoo_symbol",
    )

    selected = _select_liquid_universe(
        profile,
        top_n=args.top_n,
        min_trading_days=args.min_trading_days,
        max_zero_volume_ratio=args.max_zero_volume_ratio,
        min_avg_daily_turnover=args.min_avg_daily_turnover,
    )

    output_columns = [
        "rank",
        "symbol",
        "ticker",
        "name",
        "exchange",
        "avg_daily_volume",
        "avg_daily_turnover",
        "trading_days",
        "expected_trading_days",
        "missing_trading_days",
        "zero_volume_days",
        "zero_volume_ratio",
        "duplicate_ticker_date_rows",
        "null_ohlcv_rows",
        "first_date",
        "last_date",
    ]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.audit_output.parent.mkdir(parents=True, exist_ok=True)
    args.summary_output.parent.mkdir(parents=True, exist_ok=True)

    if selected.empty:
        selected_output = pd.DataFrame(columns=output_columns)
    else:
        selected_output = selected[output_columns]

    selected_output.to_csv(args.output, index=False)
    audit.sort_values("ticker").to_csv(args.audit_output, index=False)

    summary = AuditSummary(
        generated_at=datetime.now(UTC).isoformat(),
        universe_size=len(symbols),
        tickers_requested=len(tickers),
        tickers_with_data=int(profile["ticker"].nunique()),
        output_rows=len(selected_output),
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        top_n=args.top_n,
        min_trading_days=args.min_trading_days,
        max_zero_volume_ratio=args.max_zero_volume_ratio,
        min_avg_daily_turnover=args.min_avg_daily_turnover,
        null_rows=int(audit["null_ohlcv_rows"].sum()),
        duplicate_ticker_date_rows=int(audit["duplicate_ticker_date_rows"].sum()),
    )
    args.summary_output.write_text(json.dumps(asdict(summary), indent=2) + "\n")

    print(f"Selected {len(selected_output)} liquid NSE stocks")
    print(f"Wrote universe: {args.output}")
    print(f"Wrote audit: {args.audit_output}")
    print(f"Wrote summary: {args.summary_output}")
    if selected_output.empty:
        raise SystemExit(
            "No tickers passed the liquidity filters. Relax thresholds or inspect the audit output."
        )
    print(selected_output.head(min(10, len(selected_output))).to_string(index=False))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select the most liquid NSE equities using six-month yfinance ADV/ADT."
    )
    parser.add_argument("--lookback-days", type=int, default=183)
    parser.add_argument("--start-date", type=str, default=None, help="Optional YYYY-MM-DD start.")
    parser.add_argument("--end-date", type=str, default=None, help="Optional YYYY-MM-DD end.")
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--min-trading-days", type=int, default=90)
    parser.add_argument("--max-zero-volume-ratio", type=float, default=0.03)
    parser.add_argument("--min-avg-daily-turnover", type=float, default=None)
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--throttle-seconds", type=float, default=1.0)
    parser.add_argument("--retry-attempts", type=int, default=3)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional smoke-test universe limit.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--audit-output", type=Path, default=DEFAULT_AUDIT_OUTPUT)
    parser.add_argument("--summary-output", type=Path, default=DEFAULT_SUMMARY_OUTPUT)
    return parser.parse_args()


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM-DD date, got {value!r}") from exc


def _prepare_daily_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    time_column = "Date" if "Date" in out.columns else "Datetime"
    out = out.rename(columns={time_column: "date"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["ticker"] = out["Ticker"].astype(str)
    for column in ["Open", "High", "Low", "Close", "Volume"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out.dropna(subset=["Open", "High", "Low", "Close", "Volume"], how="all")
    return out


def _audit_liquidity_frame(frame: pd.DataFrame) -> pd.DataFrame:
    observed_dates = pd.Index(sorted(frame["date"].dropna().unique()))
    expected_trading_days = len(observed_dates)

    duplicate_counts = (
        frame.duplicated(subset=["ticker", "date"], keep=False).groupby(frame["ticker"]).sum()
    )
    null_ohlcv = (
        frame[["Open", "High", "Low", "Close", "Volume"]]
        .isna()
        .any(axis=1)
        .groupby(frame["ticker"])
        .sum()
    )
    grouped = frame.groupby("ticker", dropna=False)
    audit = grouped.agg(
        trading_days=("date", "nunique"),
        first_date=("date", "min"),
        last_date=("date", "max"),
        zero_volume_days=("Volume", lambda values: int((values.fillna(0) <= 0).sum())),
    ).reset_index()
    audit["expected_trading_days"] = expected_trading_days
    audit["missing_trading_days"] = audit["expected_trading_days"] - audit["trading_days"]
    audit["zero_volume_ratio"] = audit["zero_volume_days"] / audit["trading_days"].clip(lower=1)
    audit["duplicate_ticker_date_rows"] = (
        audit["ticker"].map(duplicate_counts).fillna(0).astype(int)
    )
    audit["null_ohlcv_rows"] = audit["ticker"].map(null_ohlcv).fillna(0).astype(int)
    return audit


def _liquidity_profile(frame: pd.DataFrame, audit: pd.DataFrame) -> pd.DataFrame:
    clean = frame.dropna(subset=["ticker", "date", "Close", "Volume"]).copy()
    clean = clean.drop_duplicates(subset=["ticker", "date"], keep="last")
    clean = clean[clean["Close"] > 0]
    clean["turnover"] = clean["Close"] * clean["Volume"]
    profile = clean.groupby("ticker").agg(
        avg_daily_volume=("Volume", "mean"),
        avg_daily_turnover=("turnover", "mean"),
    ).reset_index()
    return profile.merge(audit, on="ticker", how="left")


def _symbol_master_frame(symbols: list) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol.symbol,
                "exchange": symbol.exchange,
                "yahoo_symbol": symbol.yahoo_symbol,
                "name": symbol.name,
            }
            for symbol in symbols
        ]
    )


def _select_liquid_universe(
    profile: pd.DataFrame,
    top_n: int,
    min_trading_days: int,
    max_zero_volume_ratio: float,
    min_avg_daily_turnover: float | None,
) -> pd.DataFrame:
    selected = profile[
        (profile["trading_days"] >= min_trading_days)
        & (profile["zero_volume_ratio"] <= max_zero_volume_ratio)
        & (profile["duplicate_ticker_date_rows"] == 0)
        & (profile["null_ohlcv_rows"] == 0)
    ].copy()
    if min_avg_daily_turnover is not None:
        selected = selected[selected["avg_daily_turnover"] >= min_avg_daily_turnover]

    selected = selected.sort_values(
        ["avg_daily_turnover", "avg_daily_volume"],
        ascending=[False, False],
    ).head(top_n)
    selected.insert(0, "rank", range(1, len(selected) + 1))
    return selected


if __name__ == "__main__":
    main()
