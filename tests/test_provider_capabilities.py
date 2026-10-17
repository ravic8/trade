from datetime import date

import pytest

from trade_data.data.provider_capabilities import provider_capability


def test_upstox_capability_documents_v3_historical_limits() -> None:
    capability = provider_capability("upstox")

    assert capability.provider == "upstox"
    assert capability.api_version == "v3"
    assert "get-historical-candle-data" in capability.source_url

    by_unit_and_range = {
        (item.unit, item.interval_min, item.interval_max): item for item in capability.historical
    }

    assert by_unit_and_range[("days", 1, 1)].available_from == date(2000, 1, 1)
    assert by_unit_and_range[("days", 1, 1)].max_window == "10 years"

    assert capability.rate_limits.standard_api_per_second == 50
    assert capability.rate_limits.standard_api_per_minute == 500
    assert capability.rate_limits.standard_api_per_30_minutes == 2000


def test_provider_capability_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unsupported provider"):
        provider_capability("unknown")


def test_yfinance_capability_documents_daily_storage_scope() -> None:
    capability = provider_capability("yfinance")

    assert capability.provider == "yfinance"
    assert capability.api_version == "library"
    assert capability.historical[0].unit == "days"
    assert capability.historical[0].interval_min == 1
    assert capability.rate_limits.standard_api_per_minute == 30
    assert any("price_adjustments_daily" in note for note in capability.notes)


def test_discarded_provider_is_not_supported() -> None:
    with pytest.raises(ValueError, match="Unsupported provider"):
        provider_capability("dukascopy")


@pytest.mark.parametrize("provider", ["upstox", "yfinance"])
def test_only_daily_adapter_capabilities_are_exposed(provider: str) -> None:
    capability = provider_capability(provider)
    assert [
        (item.unit, item.interval_min, item.interval_max) for item in capability.historical
    ] == [("days", 1, 1)]
