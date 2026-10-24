from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from trade_data.market_data.price_quality import daily_price_errors
from trade_data.operations.history_validation import DailyHistoryAuditor
from trade_data.storage.timescale import TimescaleStore


def row(**changes):
    return dict(
        instrument_key="YF|A.NS",
        symbol="A.NS",
        date=date(2025, 1, 9),
        open=215.0,
        high=220.0,
        low=210.0,
        close=215.0,
        volume=100,
        session_valid=True,
        quality_status="ok",
        adjustment_factor=1.0,
        adjustment_raw_close=215.0,
        **changes,
    )


def test_legacy_writer_cannot_override_invalid_ohlc_with_ok():
    records = pd.DataFrame(
        [
            dict(
                Date=date(2025, 1, 9),
                Symbol="A",
                InstrumentKey="YF|A.NS",
                Open=1045.4,
                High=1025.0,
                Low=1025.0,
                Close=1025.0,
                Volume=10,
                quality_status="ok",
            )
        ]
    )
    assert (
        TimescaleStore._daily_ohlcv_rows(records, "NSE", "yfinance")[0]["quality_status"]
        == "suspicious"
    )


@pytest.mark.parametrize("price", [float("nan"), float("inf"), Decimal("NaN")])
def test_non_finite_prices_are_rejected_without_comparison_errors(price):
    assert daily_price_errors(dict(open=price, high=10, low=1, close=2, volume=0)) == (
        "non_finite_price",
    )


def test_extreme_return_spans_chunks_zero_volume_is_soft_and_invalid_is_quarantined():
    auditor = DailyHistoryAuditor()
    auditor.annotate(pd.DataFrame([row()]))
    spike = row()
    spike.update(
        date=date(2025, 1, 10),
        open=21899.0,
        high=21899.0,
        low=20481.0,
        close=20505.0,
        volume=0,
        adjustment_raw_close=20505.0,
    )
    result = auditor.annotate(pd.DataFrame([spike]))
    assert result.quality_status.iloc[0] == "valid_with_flags"
    assert result.previous_close.iloc[0] == 215.0
    assert "extreme_return_review" in result.quality_flags.iloc[0]
    assert "zero_volume" in result.quality_flags.iloc[0]
    invalid = row()
    invalid.update(date=date(2025, 1, 13), open=0.0, close=225.0)
    result = auditor.annotate(pd.DataFrame([invalid]))
    assert result.quality_status.iloc[0] == "quarantined"
    assert "invalid_ohlc_range" in result.quality_flags.iloc[0]
    assert auditor.counts["rows"] == 3


def test_duplicate_and_out_of_order_keys_fail_closed():
    auditor = DailyHistoryAuditor()
    with pytest.raises(ValueError, match="unique, ascending"):
        auditor.annotate(pd.DataFrame([row(), row()]))
    auditor.annotate(pd.DataFrame([row()]))
    with pytest.raises(ValueError, match="unique, ascending"):
        auditor.annotate(pd.DataFrame([row()]))


def test_special_session_is_retained_but_unknown_calendar_is_quarantined():
    special = row()
    special.update(date=date(2025, 2, 1))
    assert DailyHistoryAuditor().annotate(pd.DataFrame([special])).quality_status.iloc[0] == "valid"
    special["session_valid"] = None
    assert (
        DailyHistoryAuditor().annotate(pd.DataFrame([special])).quality_status.iloc[0]
        == "quarantined"
    )


def test_verified_correction_replays_but_unexpected_basis_fails_closed():
    from trade_data.market_data.corrections import corrected_values

    before = dict(open=21899.0, high=21899.0, low=20481.0, close=20505.0, volume=53601)
    after = dict(open=218.99, high=218.99, low=204.81, close=205.05, volume=53601)
    evidence = dict(
        before_prices=before,
        after_prices=after,
        evidence_url="https://nsearchives.nseindia.com/example.zip",
        evidence_sha256="a" * 64,
    )
    assert corrected_values(before, evidence) == after
    assert corrected_values(after, evidence) == after
    with pytest.raises(ValueError, match="basis changed"):
        corrected_values({**before, "close": 99.0}, evidence)
    with pytest.raises(ValueError, match="source evidence"):
        corrected_values(before, {**evidence, "evidence_url": ""})


def test_verified_corrections_preserve_adjustment_factor_and_isolate_symbol():
    from datetime import UTC, datetime

    from sqlalchemy import create_engine

    from trade_data.control_plane.tables import market_data_quality_outcomes_table
    from trade_data.market_data.corrections import apply_verified_daily_corrections
    from trade_data.market_data.quality import (
        MarketDataQualityOutcome,
        MarketDataQualityRepository,
        MarketDataQualityStatus,
    )

    engine = create_engine("sqlite://")
    market_data_quality_outcomes_table.create(engine)
    before = dict(open=21899.0, high=21899.0, low=20481.0, close=20505.0, volume=53601)
    after = dict(open=218.99, high=218.99, low=204.81, close=205.05, volume=53601)
    MarketDataQualityRepository(engine).record(
        [
            MarketDataQualityOutcome(
                source_run_id="nse-daily-verified-corrections-v1",
                request_id="correction",
                provider="yfinance",
                exchange="NSE",
                interval="1d",
                instrument_id="cybertech",
                provider_symbol="CYBERTECH.NS",
                session_date=date(2025, 1, 10),
                status=MarketDataQualityStatus.VALID,
                reason_code="source_verified_correction",
                severity="info",
                expected=True,
                retryable=False,
                observed_at=datetime.now(UTC),
                details={
                    "instrument_key": "YF|CYBERTECH.NS",
                    "before_prices": before,
                    "after_prices": after,
                    "evidence_url": "https://nsearchives.nseindia.com/example.zip",
                    "evidence_sha256": "a" * 64,
                },
            )
        ]
    )
    f = pd.DataFrame(
        [
            dict(
                InstrumentKey="YF|CYBERTECH.NS",
                Date=date(2025, 1, 10),
                Open=21899.0,
                High=21899.0,
                Low=20481.0,
                Close=20505.0,
                Volume=53601,
                AdjClose=18023.6,
            )
        ]
    )
    corrected = apply_verified_daily_corrections(engine, f, exchange="NSE", provider="yfinance")
    assert f.Close.iloc[0] == 20505.0
    assert corrected.Close.iloc[0] == 205.05
    assert corrected.AdjClose.iloc[0] == pytest.approx(180.236)
    twice = apply_verified_daily_corrections(engine, corrected, exchange="NSE", provider="yfinance")
    pd.testing.assert_frame_equal(twice, corrected)


def test_full_history_export_round_trips_three_disjoint_outputs(tmp_path):
    import json

    import pyarrow.parquet as pq
    from test_daily_snapshot_export import SnapshotEngine

    from trade_data.operations.history_validation import export_validated_daily_history

    class HistoryEngine(SnapshotEngine):
        def execute(self, statement, parameters=None):
            if str(statement).startswith("SET LOCAL"):
                return None
            return super().execute(statement)

    engine = HistoryEngine()
    for item in engine.rows:
        item.update(
            session_valid=True,
            adjustment_raw_close=item["close"],
            adjusted_close=item["close"],
            adjustment_factor=1.0,
        )
    engine.rows[0]["open"] = 0.0
    engine.rows[1]["volume"] = 0
    manifest = export_validated_daily_history(
        engine,
        destination=tmp_path / "history",
        start_date=date(2025, 1, 1),
        end_date=date(2026, 1, 1),
        chunk_rows=17,
    )
    payload = json.loads(manifest.read_text())
    assert payload["counts"]["rows"] == 230
    assert payload["counts"]["quarantined"] == 1
    assert pq.ParquetFile(manifest.parent / "nse_daily_validated.parquet").metadata.num_rows == 229
    assert pq.ParquetFile(manifest.parent / "quarantined.parquet").metadata.num_rows == 1
    assert pq.ParquetFile(manifest.parent / "review_flags.parquet").metadata.num_rows >= 1
    assert payload["counts"]["validated"] + payload["counts"]["quarantined"] == 230


def test_exchange_conflicts_quarantine_and_coherent_adjustments_remain_flags():
    import runpy
    from pathlib import Path

    compare = runpy.run_path(
        str(Path(__file__).parents[1] / "scripts/validate_nse_history_sources.py")
    )["compare_session"]
    stored = pd.DataFrame(
        [
            dict(
                instrument_key="YF|A.NS",
                symbol="A.NS",
                date=date(2025, 1, 9),
                open=10.0,
                high=12.0,
                low=9.0,
                close=11.0,
                volume=100,
            ),
            dict(
                instrument_key="YF|B.NS",
                symbol="B.NS",
                date=date(2025, 1, 9),
                open=5.0,
                high=6.0,
                low=4.5,
                close=5.5,
                volume=200,
            ),
            dict(
                instrument_key="YF|C.NS",
                symbol="C.NS",
                date=date(2025, 1, 9),
                open=10.0,
                high=12.0,
                low=9.0,
                close=100.0,
                volume=100,
            ),
        ]
    )
    official = pd.DataFrame(
        [
            dict(SYMBOL=s, SERIES="EQ", OPEN=10.0, HIGH=12.0, LOW=9.0, CLOSE=11.0, TOTTRDQTY=100)
            for s in ["A", "B", "C"]
        ]
    )
    result = compare(stored, official)
    assert result.exchange_verification.tolist() == [
        "exchange_match",
        "adjustment_basis_review",
        "exchange_price_disagreement",
    ]
    sample = row()
    source = pd.Series(
        ["exchange_price_disagreement"],
        index=pd.MultiIndex.from_tuples([(sample["instrument_key"], sample["date"])]),
    )
    assert (
        DailyHistoryAuditor(source).annotate(pd.DataFrame([sample])).quality_status.iloc[0]
        == "quarantined"
    )


def test_exchange_comparison_resolves_renamed_security_only_by_unique_isin():
    import runpy
    from pathlib import Path

    compare = runpy.run_path(
        str(Path(__file__).parents[1] / "scripts/validate_nse_history_sources.py")
    )["compare_session"]
    stored = pd.DataFrame(
        [
            dict(
                instrument_key="YF|NEW.NS",
                symbol="NEW.NS",
                date=date(2025, 1, 9),
                open=10.0,
                high=12.0,
                low=9.0,
                close=11.0,
                volume=100,
                isin="INE001A01010",
            )
        ]
    )
    official = pd.DataFrame(
        [
            dict(
                SYMBOL="OLD",
                SERIES="EQ",
                ISIN="INE001A01010",
                OPEN=10.0,
                HIGH=12.0,
                LOW=9.0,
                CLOSE=11.0,
                TOTTRDQTY=100,
            )
        ]
    )
    result = compare(stored, official)
    assert result.exchange_verification.iloc[0] == "exchange_match"
    assert result.identity_match.iloc[0] == "isin"
    ambiguous = pd.concat([official, official.assign(SYMBOL="OTHER")], ignore_index=True)
    assert compare(stored, ambiguous).exchange_verification.iloc[0] == "exchange_record_unavailable"
