import csv
import io
import zipfile
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, func, select

from trade_data.config import Settings
from trade_data.data.nse_bhavcopy import SOURCE, parse_bhavcopy
from trade_data.pipelines import nse_daily as pipeline
from trade_data.storage.nse import nse_sessions
from trade_data.storage.timescale import TimescaleStore, ohlcv_daily_table, symbols_table

DAY = date(2026, 10, 23)


def archive(rows=None, *, day=DAY, member=None):
    row = dict(
        TradDt=str(day),
        Sgmt="CM",
        Src="NSE",
        FinInstrmTp="STK",
        TckrSymb="AAA",
        ISIN="INE002A01018",
        SctySrs="EQ",
        FinInstrmNm="AAA LIMITED",
        OpnPric="10.25",
        HghPric="12.50",
        LwPric="9.00",
        ClsPric="11.75",
        TtlTradgVol="9007199254740993",
    )
    records = [{**row, **r} for r in (rows if rows is not None else [{}])]
    text = io.StringIO()
    writer = csv.DictWriter(text, fieldnames=list(row))
    writer.writeheader()
    writer.writerows(records)
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as z:
        z.writestr(member or f"BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv", text.getvalue())
    return raw.getvalue()


def test_parser_keeps_exact_integer_volume_and_decimal_prices():
    source = parse_bhavcopy(archive(), DAY)
    assert source.frame.iloc[0].volume == 9007199254740993
    assert source.frame.iloc[0].open == Decimal("10.25")
    assert len(source.sha256) == 64


@pytest.mark.parametrize(
    "changes",
    [
        {"TradDt": "2026-10-22"},
        {"Src": "BSE"},
        {"Sgmt": "FO"},
        {"TtlTradgVol": "1.1"},
        {"TtlTradgVol": "-1"},
        {"TtlTradgVol": "9223372036854775808"},
        {"OpnPric": "NaN"},
        {"LwPric": "11"},
        {"ClsPric": "99"},
    ],
)
def test_parser_fails_closed_on_wrong_identity_date_or_invalid_ohlcv(changes):
    with pytest.raises(ValueError):
        parse_bhavcopy(archive([changes]), DAY)


def test_parser_accounts_for_non_stock_exclusions_and_keeps_be_bz_sme():
    rows = [{"SctySrs": s, "TckrSymb": s} for s in ("EQ", "BE", "BZ", "SM", "ST")]
    rows += [
        {"TckrSymb": "FUND", "ISIN": "INF002A01018"},
        {"TckrSymb": "BOND", "SctySrs": "N1"},
        {"TckrSymb": "AAA-RE"},
        {"TckrSymb": "AAA-PP"},
    ]
    source = parse_bhavcopy(archive(rows), DAY)
    assert len(source.frame) == 5
    assert source.total_records == 9
    assert sum(source.excluded.values()) == 4


def test_parser_rejects_duplicate_series_and_wrong_archive():
    with pytest.raises(ValueError):
        parse_bhavcopy(archive([{}, {"SctySrs": "BE"}]), DAY)
    with pytest.raises(ValueError):
        parse_bhavcopy(archive(member="../wrong.csv"), DAY)


def test_zero_volume_is_an_explicit_flag_not_invalidity():
    assert parse_bhavcopy(archive([{"TtlTradgVol": "0"}]), DAY).frame.iloc[0].volume == 0


@pytest.fixture
def storage():
    engine = create_engine("sqlite:///:memory:")
    for table in (symbols_table, ohlcv_daily_table, nse_sessions):
        table.create(engine)
    store = TimescaleStore.__new__(TimescaleStore)
    store.engine = engine
    with engine.begin() as c:
        c.execute(
            symbols_table.insert().values(
                symbol="AAA",
                exchange="NSE",
                source="legacy",
                source_identity="INE002A01018",
                canonical_instrument_id="id-aaa",
                provider_instrument_key="legacy|AAA",
                yahoo_symbol="AAA.NS",
                fetched_at=datetime.now(UTC),
            )
        )
    return store


def memory_replica(monkeypatch, *, corrupt=False):
    records = {}

    def publish(settings, batch, **kwargs):
        for candle in batch.candles:
            row = {
                k: getattr(candle, k)
                for k in (
                    "instrument_id",
                    "provider_symbol",
                    "symbol",
                    "exchange",
                    "session_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "provider_timestamp",
                )
            }
            row["source"] = SOURCE
            if corrupt:
                row["volume"] += 1
            records[candle.identity] = row

    class Replica:
        def __init__(self, *args, **kwargs):
            pass

        def read_daily_partition(self, **kwargs):
            return list(records.values())

    monkeypatch.setattr(pipeline, "replicate_validated_batch", publish)
    monkeypatch.setattr(pipeline, "create_clickhouse_client", lambda settings: None)
    monkeypatch.setattr(pipeline, "ClickHouseMarketDataRepository", Replica)


def test_ingest_proves_exchange_coverage_and_idempotent_replay_without_touching_legacy(
    monkeypatch,
    storage,
):
    memory_replica(monkeypatch)
    source = parse_bhavcopy(archive(), DAY)
    settings = Settings(_env_file=None)
    # A different source with different values must remain untouched.
    with storage.engine.begin() as c:
        c.execute(
            ohlcv_daily_table.insert().values(
                instrument_key="legacy|AAA",
                source="legacy",
                date=DAY,
                symbol="AAA.NS",
                exchange="NSE",
                open=1,
                high=1,
                low=1,
                close=1,
                volume=7,
                fetched_at=datetime.now(UTC),
                quality_status="ok",
            )
        )
    first = pipeline._ingest(storage, settings, source)
    second = pipeline._ingest(storage, settings, source)
    assert first["digest"] == second["digest"]
    with storage.engine.connect() as c:
        assert c.execute(select(func.count()).select_from(ohlcv_daily_table)).scalar() == 2
        assert c.execute(select(nse_sessions.c.status)).scalar() == "complete"
        assert (
            c.execute(
                select(ohlcv_daily_table.c.volume).where(ohlcv_daily_table.c.source == "legacy")
            ).scalar()
            == 7
        )


def test_volume_difference_cannot_be_reported_complete(monkeypatch, storage):
    memory_replica(monkeypatch, corrupt=True)
    with pytest.raises(ValueError, match="source/replica"):
        pipeline._ingest(storage, Settings(_env_file=None), parse_bhavcopy(archive(), DAY))
    with storage.engine.connect() as c:
        assert c.execute(select(nse_sessions.c.status)).scalar() == "mismatch"


def test_new_ticker_identity_mismatch_fails_closed(storage):
    source = parse_bhavcopy(archive([{"ISIN": "INE003A01018"}]), DAY)
    with pytest.raises(ValueError, match="identity changed"):
        pipeline._identities(storage, source)


def test_new_security_can_be_discovered_without_a_legacy_snapshot(storage):
    source = parse_bhavcopy(archive([{"TckrSymb": "NEW", "ISIN": "INE003A01018"}]), DAY)
    ids = pipeline._identities(storage, source)
    assert ids["NEW"]["provider_instrument_key"] == "NSE|INE003A01018"
    assert ids["NEW"]["source_identity"] == "INE003A01018"
