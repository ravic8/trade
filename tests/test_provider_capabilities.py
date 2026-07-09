from datetime import date

import pytest

from trade_research.data.provider_capabilities import provider_capability


def test_upstox_capability_documents_v3_historical_limits() -> None:
    capability = provider_capability("upstox")

    assert capability.provider == "upstox"
    assert capability.api_version == "v3"
    assert "get-historical-candle-data" in capability.source_url

    by_unit_and_range = {
        (item.unit, item.interval_min, item.interval_max): item
        for item in capability.historical
    }

    assert by_unit_and_range[("minutes", 1, 15)].available_from == date(2022, 1, 1)
    assert by_unit_and_range[("minutes", 1, 15)].max_window == "1 month"
    assert by_unit_and_range[("minutes", 16, 300)].max_window == "1 quarter"
    assert by_unit_and_range[("hours", 1, 5)].max_window == "1 quarter"
    assert by_unit_and_range[("days", 1, 1)].available_from == date(2000, 1, 1)
    assert by_unit_and_range[("days", 1, 1)].max_window == "10 years"
    assert by_unit_and_range[("weeks", 1, 1)].max_window is None
    assert by_unit_and_range[("months", 1, 1)].max_window is None

    assert capability.rate_limits.standard_api_per_second == 50
    assert capability.rate_limits.standard_api_per_minute == 500
    assert capability.rate_limits.standard_api_per_30_minutes == 2000


def test_provider_capability_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unsupported provider"):
        provider_capability("unknown")
