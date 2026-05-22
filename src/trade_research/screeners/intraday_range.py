from datetime import UTC, datetime

import pandas as pd

from trade_research.schemas import ScreenerResult


class IntradayRangeScreener:
    strategy_name = "intraday_range_v1"

    required_columns = [
        "d5Up0100",
        "d5Dn0100",
        "d5ClUp0200",
        "d5ClDn0200",
        "d5ONUp0200",
        "d5ONDn0200",
        "d5VUp0200",
        "d5VDn0200",
    ]

    def run(self, feature_df: pd.DataFrame) -> pd.DataFrame:
        missing = [col for col in self.required_columns if col not in feature_df.columns]
        if missing:
            raise ValueError(f"Feature table is missing screener columns: {missing}")

        return feature_df[
            (feature_df["d5Up0100"] >= 3)
            & (feature_df["d5Dn0100"] >= 3)
            & (feature_df["d5ClUp0200"] <= 3)
            & (feature_df["d5ClDn0200"] <= 3)
            & (feature_df["d5ONUp0200"] <= 3)
            & (feature_df["d5ONDn0200"] <= 3)
            & (feature_df["d5VUp0200"] <= 3)
            & (feature_df["d5VDn0200"] <= 3)
        ].copy()

    def to_results(self, screened_df: pd.DataFrame) -> list[ScreenerResult]:
        matched_at = datetime.now(UTC)
        results: list[ScreenerResult] = []
        for row in screened_df.to_dict(orient="records"):
            ticker = str(row["Ticker"])
            metrics = {key: value for key, value in row.items() if key != "Ticker"}
            results.append(
                ScreenerResult(
                    ticker=ticker,
                    strategy=self.strategy_name,
                    matched_at=matched_at,
                    metrics=metrics,
                )
            )
        return results
