import pandas as pd

from trade_research.schemas import MarketDataQualityReport

REQUIRED_OHLCV_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]


def validate_ohlcv(df: pd.DataFrame) -> list[MarketDataQualityReport]:
    missing_columns = [col for col in REQUIRED_OHLCV_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(f"OHLCV data is missing required columns: {missing_columns}")

    reports: list[MarketDataQualityReport] = []
    for ticker, group in df.groupby("Ticker"):
        sorted_group = group.sort_values("Date")
        missing_ohlcv = int(
            sorted_group[["Open", "High", "Low", "Close", "Volume"]]
            .isna()
            .any(axis=1)
            .sum()
        )
        bad_close = int((sorted_group["Close"] <= 0).fillna(False).sum())
        zero_volume = int((sorted_group["Volume"] <= 0).fillna(False).sum())

        warnings: list[str] = []
        if missing_ohlcv:
            warnings.append("missing_ohlcv")
        if bad_close:
            warnings.append("zero_or_negative_close")
        if zero_volume:
            warnings.append("zero_or_negative_volume")

        reports.append(
            MarketDataQualityReport(
                ticker=str(ticker),
                rows=len(sorted_group),
                start_date=sorted_group["Date"].min().date(),
                end_date=sorted_group["Date"].max().date(),
                missing_ohlcv_rows=missing_ohlcv,
                zero_or_negative_close_rows=bad_close,
                zero_volume_rows=zero_volume,
                warnings=warnings,
            )
        )
    return reports
