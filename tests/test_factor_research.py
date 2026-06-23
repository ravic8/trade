from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest

from trade_research.features import FEATURE_VERSION_V1_0
from trade_research.research import DailyFactorResearchBuilder, join_features_and_targets
from trade_research.targets import DAILY_FORWARD_TARGET_VERSION_V1_0


def _features_and_targets(days: int = 12) -> tuple[pd.DataFrame, pd.DataFrame]:
    feature_rows = []
    target_rows = []
    for offset in range(days):
        value_date = date(2025, 1, 1) + timedelta(days=offset)
        for idx, symbol in enumerate(["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]):
            feature_value = float(idx + offset)
            forward_return = feature_value / 100.0
            key = f"NSE_EQ|{symbol}"
            feature_rows.append(
                {
                    "instrument_key": key,
                    "symbol": symbol,
                    "exchange": "NSE",
                    "source": "upstox",
                    "date": value_date,
                    "feature_version": FEATURE_VERSION_V1_0,
                    "computed_at": datetime(2025, 1, 1, tzinfo=UTC),
                    "quality_status": "passed",
                    "ret_20d": feature_value,
                }
            )
            target_rows.append(
                {
                    "instrument_key": key,
                    "symbol": symbol,
                    "exchange": "NSE",
                    "source": "upstox",
                    "date": value_date,
                    "target_version": DAILY_FORWARD_TARGET_VERSION_V1_0,
                    "computed_at": datetime(2025, 1, 1, tzinfo=UTC),
                    "quality_status": "passed",
                    "forward_ret_1d": forward_return,
                    "forward_ret_5d": forward_return,
                    "forward_ret_10d": forward_return,
                    "forward_ret_20d": forward_return,
                    "forward_ret_60d": forward_return,
                    "forward_outperform_universe_20d": forward_return,
                    "top_quantile_forward_return_20d": idx >= 4,
                }
            )
    return pd.DataFrame(feature_rows), pd.DataFrame(target_rows)


def test_join_features_and_targets_keeps_versions_separate() -> None:
    features, targets = _features_and_targets(days=1)
    extra = targets.copy()
    extra["target_version"] = "other"
    targets = pd.concat([targets, extra], ignore_index=True)

    joined = join_features_and_targets(
        features,
        targets,
        feature_version=FEATURE_VERSION_V1_0,
        target_version=DAILY_FORWARD_TARGET_VERSION_V1_0,
    )

    assert len(joined) == 6
    assert set(joined["target_version"]) == {DAILY_FORWARD_TARGET_VERSION_V1_0}


def test_factor_research_outputs_ic_quantiles_and_hit_rates() -> None:
    features, targets = _features_and_targets()

    ic, quantiles, hit_rates, monthly, summary = DailyFactorResearchBuilder(
        min_month_rows=5
    ).build(features, targets, feature_columns=["ret_20d"], return_targets=["forward_ret_20d"])

    row = ic.iloc[0]
    assert row["feature"] == "ret_20d"
    assert row["target"] == "forward_ret_20d"
    assert row["mean_rank_ic"] == pytest.approx(1.0)
    assert row["positive_rank_ic_pct"] == pytest.approx(1.0)
    assert set(quantiles["feature_quantile"]) == {0, 1, 2, 3, 4, 5}
    assert hit_rates[hit_rates["feature_quantile"].eq(5)]["top_quantile_hit_rate"].iloc[
        0
    ] == pytest.approx(1.0)
    assert len(monthly) == 1
    assert summary.feature_count == 1
    assert summary.return_target_count == 1
