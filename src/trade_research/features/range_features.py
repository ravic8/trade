from collections.abc import Iterable, Mapping

import pandas as pd

DEFAULT_WINDOWS = {
    "5D": 5,
    "20D": 20,
    "60D": 60,
    "120D": 120,
    "250D": 250,
    "500D": 500,
}

DEFAULT_THRESHOLDS = [0.0, 0.0025, 0.005, 0.0075, 0.01, 0.02, 0.03, 0.04, 0.05]


class RangeFeatureBuilder:
    def __init__(
        self,
        windows: Mapping[str, int] | None = None,
        thresholds: Iterable[float] | None = None,
        min_median_dollar_volume: float = 5_000_000,
    ) -> None:
        self.windows = dict(windows or DEFAULT_WINDOWS)
        self.thresholds = list(thresholds or DEFAULT_THRESHOLDS)
        self.min_median_dollar_volume = min_median_dollar_volume

    def build(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        df = self._filter_liquid(ohlcv)
        feature_rows: list[dict[str, int | str]] = []

        for ticker, group in df.groupby("Ticker"):
            g = group.sort_values("Date").copy()
            g["prev_close"] = g["Close"].shift(1)
            g["on_ret"] = (g["Open"] / g["prev_close"]) - 1
            g["prev_vol"] = g["Volume"].shift(1)
            g["v_change"] = (g["Volume"] / g["prev_vol"]) - 1

            row: dict[str, int | str] = {"Ticker": str(ticker)}
            for window_size in self.windows.values():
                g_win = g.tail(window_size)
                if len(g_win) < window_size:
                    continue

                for threshold in self.thresholds:
                    bp_label = f"{int(threshold * 10000):04d}"
                    row[f"d{window_size}Up{bp_label}"] = int(
                        (g_win["High"] >= g_win["Open"] * (1 + threshold)).sum()
                    )
                    row[f"d{window_size}Dn{bp_label}"] = int(
                        (g_win["Low"] <= g_win["Open"] * (1 - threshold)).sum()
                    )
                    row[f"d{window_size}ClUp{bp_label}"] = int(
                        (g_win["Close"] >= g_win["Open"] * (1 + threshold)).sum()
                    )
                    row[f"d{window_size}ClDn{bp_label}"] = int(
                        (g_win["Close"] <= g_win["Open"] * (1 - threshold)).sum()
                    )
                    row[f"d{window_size}ONUp{bp_label}"] = int(
                        (g_win["on_ret"] >= threshold).sum()
                    )
                    row[f"d{window_size}ONDn{bp_label}"] = int(
                        (g_win["on_ret"] <= -threshold).sum()
                    )
                    row[f"d{window_size}VUp{bp_label}"] = int(
                        (g_win["v_change"] >= threshold).sum()
                    )
                    row[f"d{window_size}VDn{bp_label}"] = int(
                        (g_win["v_change"] <= -threshold).sum()
                    )

            feature_rows.append(row)

        return pd.DataFrame(feature_rows)

    def _filter_liquid(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        df = ohlcv.copy()
        df["dollar_volume"] = df["Close"] * df["Volume"]
        liquid_tickers = (
            df.groupby("Ticker")["dollar_volume"]
            .median()
            .loc[lambda series: series >= self.min_median_dollar_volume]
            .index
        )
        return df[df["Ticker"].isin(liquid_tickers)].copy()
