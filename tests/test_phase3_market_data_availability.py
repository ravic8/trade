from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd
from sqlalchemy import create_engine, func, select

from trade_research.control_plane.tables import (
    market_data_availability_observations_table,
)
from trade_research.market_data.availability import (
    MarketDataAvailabilityRepository,
    availability_session_maps,
    nse_minute_observed_session_windows,
    observe_nse_minute_availability,
)
from trade_research.market_data.contracts import CandleInterval, ProviderRequest
from trade_research.market_data.quality import (
    MarketDataQualityStatus,
    nse_minute_missing_quality_outcomes,
)


def _request() -> ProviderRequest:
    return ProviderRequest(
        request_id="minute-run-nse-1m",
        provider="yfinance",
        exchange="NSE",
        interval=CandleInterval.ONE_MINUTE,
        window_start=datetime(2026, 9, 4, 3, 45, tzinfo=UTC),
        window_end=datetime(2026, 9, 5, 10, 0, tzinfo=UTC),
        provider_symbols=("INFY.NS", "RELIANCE.NS", "TCS.NS"),
        retrieved_at=datetime(2026, 9, 5, 12, 0, tzinfo=UTC),
        adapter_version="yfinance-v1",
    )


def test_availability_is_derived_per_instrument_from_the_raw_response() -> None:
    request = _request()
    raw = pd.DataFrame(
        [
            {
                "TradingSymbol": "RELIANCE.NS",
                "Timestamp": datetime(2026, 9, 4, 3, 45, tzinfo=UTC),
            },
            {
                "TradingSymbol": "RELIANCE.NS",
                "Timestamp": datetime(2026, 9, 5, 3, 46, tzinfo=UTC),
            },
        ]
    )

    observations = observe_nse_minute_availability(
        request=request,
        source_run_id="minute-run",
        raw_frame=raw,
        canonical_instrument_ids={
            "INFY.NS": "nse-infy",
            "RELIANCE.NS": "nse-reliance",
            "TCS.NS": "nse-tcs",
        },
        eligible_sessions={date(2026, 9, 4), date(2026, 9, 5)},
        unavailable_provider_symbols={"TCS.NS"},
        raw_artifact_id="artifact-1",
    )
    by_symbol = {item.provider_symbol: item for item in observations}

    assert by_symbol["RELIANCE.NS"].status == "observed"
    assert by_symbol["RELIANCE.NS"].observed_row_count == 2
    assert by_symbol["RELIANCE.NS"].observed_sessions == (
        date(2026, 9, 4),
        date(2026, 9, 5),
    )
    assert by_symbol["INFY.NS"].status == "empty"
    assert by_symbol["INFY.NS"].reason_code == "provider_returned_no_data"
    assert by_symbol["TCS.NS"].status == "request_failed"
    assert by_symbol["TCS.NS"].reason_code == "provider_request_failed"


def test_availability_observations_are_content_addressed_and_idempotent() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    market_data_availability_observations_table.create(engine)
    observations = observe_nse_minute_availability(
        request=_request(),
        source_run_id="minute-run",
        raw_frame=pd.DataFrame(),
        canonical_instrument_ids={"INFY.NS": "nse-infy"},
        eligible_sessions={date(2026, 9, 4)},
    )
    repository = MarketDataAvailabilityRepository(engine)

    repository.record(observations)
    repository.record(observations)

    with engine.connect() as connection:
        count = connection.scalar(
            select(func.count()).select_from(market_data_availability_observations_table)
        )
    assert count == 1


def test_minute_missingness_uses_observed_sessions_not_configured_lookback() -> None:
    request = _request()
    observations = observe_nse_minute_availability(
        request=request,
        source_run_id="minute-run",
        raw_frame=pd.DataFrame(
            [
                {
                    "TradingSymbol": "RELIANCE.NS",
                    "Timestamp": datetime(2026, 9, 5, 3, 45, tzinfo=UTC),
                }
            ]
        ),
        canonical_instrument_ids={"RELIANCE.NS": "nse-reliance"},
        eligible_sessions={date(2026, 9, 4), date(2026, 9, 5)},
    )
    observed_sessions, reasons = availability_session_maps(observations)

    outcomes = nse_minute_missing_quality_outcomes(
        request=request,
        source_run_id="minute-run",
        canonical_instrument_ids={"RELIANCE.NS": "nse-reliance"},
        eligible_sessions={date(2026, 9, 4), date(2026, 9, 5)},
        accepted=(),
        observed_availability_sessions=observed_sessions,
        unavailable_reason_codes=reasons,
    )
    by_session = {
        session: [item for item in outcomes if item.session_date == session]
        for session in (date(2026, 9, 4), date(2026, 9, 5))
    }

    assert {item.status for item in by_session[date(2026, 9, 4)]} == {
        MarketDataQualityStatus.PROVIDER_UNAVAILABLE
    }
    assert {item.reason_code for item in by_session[date(2026, 9, 4)]} == {
        "session_outside_observed_availability"
    }
    assert {item.status for item in by_session[date(2026, 9, 5)]} == {
        MarketDataQualityStatus.MISSING
    }
    assert {item.reason_code for item in by_session[date(2026, 9, 5)]} == {"candle_absent"}


def test_empty_provider_response_has_an_explicit_availability_reason() -> None:
    request = _request()
    observations = observe_nse_minute_availability(
        request=request,
        source_run_id="minute-run",
        raw_frame=pd.DataFrame(),
        canonical_instrument_ids={"INFY.NS": "nse-infy"},
        eligible_sessions={date(2026, 9, 4)},
    )
    observed_sessions, reasons = availability_session_maps(observations)

    outcomes = nse_minute_missing_quality_outcomes(
        request=request,
        source_run_id="minute-run",
        canonical_instrument_ids={"INFY.NS": "nse-infy"},
        eligible_sessions={date(2026, 9, 4)},
        accepted=(),
        observed_availability_sessions=observed_sessions,
        unavailable_reason_codes=reasons,
    )

    assert {item.status for item in outcomes} == {MarketDataQualityStatus.PROVIDER_UNAVAILABLE}
    assert {item.reason_code for item in outcomes} == {"provider_returned_no_data"}


def test_minute_quality_uses_provider_observed_session_boundaries() -> None:
    request = _request()
    raw = pd.DataFrame(
        [
            {
                "TradingSymbol": "RELIANCE.NS",
                "Timestamp": datetime(2026, 9, 4, 3, 46, tzinfo=UTC),
            },
            {
                "TradingSymbol": "RELIANCE.NS",
                "Timestamp": datetime(2026, 9, 4, 9, 44, tzinfo=UTC),
            },
        ]
    )
    windows = nse_minute_observed_session_windows(raw, {date(2026, 9, 4)})

    outcomes = nse_minute_missing_quality_outcomes(
        request=request,
        source_run_id="minute-run",
        canonical_instrument_ids={"RELIANCE.NS": "nse-reliance"},
        eligible_sessions={date(2026, 9, 4)},
        accepted=(),
        observed_availability_sessions={"RELIANCE.NS": {date(2026, 9, 4)}},
        observed_session_windows=windows,
    )
    by_timestamp = {item.candle_timestamp: item for item in outcomes}

    assert windows["RELIANCE.NS"][date(2026, 9, 4)] == (
        datetime(2026, 9, 4, 3, 46, tzinfo=UTC),
        datetime(2026, 9, 4, 9, 44, tzinfo=UTC),
    )
    assert (
        by_timestamp[datetime(2026, 9, 4, 3, 45, tzinfo=UTC)].status
        == MarketDataQualityStatus.PROVIDER_UNAVAILABLE
    )
    assert (
        by_timestamp[datetime(2026, 9, 4, 9, 45, tzinfo=UTC)].reason_code
        == "outside_provider_observed_session_window"
    )
    assert (
        by_timestamp[datetime(2026, 9, 4, 3, 47, tzinfo=UTC)].status
        == MarketDataQualityStatus.MISSING
    )
