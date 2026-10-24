import httpx
import pytest

from trade_data.universe.nse import NSEUniverseProvider, is_ordinary_nse_equity


@pytest.mark.parametrize("header", ["SYMBOL, SERIES", " SYMBOL , SERIES "])
def test_nse_csv_classifies_equities_across_trading_series(monkeypatch, header):
    csv = (
        header
        + ",NAME OF COMPANY,ISIN NUMBER\n"
        + "\n".join(
            [
                " RELIANCE , EQ ,Reliance Industries,INE002A01018",
                "TRADE,BE,Trade Limited,INE001A01010",
                "RESTRICTED,BZ,Restricted Limited,INE001B01010",
                "OLAELEC-RE,BE,Ola Electric Mobility Ltd-RE,INE0LXG20016",
                "FUND,EQ,Exchange Traded Fund,INF001A01010",
                "JISLDVREQS,EQ,Jain Irrigation Systems Limited,IN9175A01010",
            ]
        )
    )
    client_type = httpx.Client
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=csv))
    monkeypatch.setattr(httpx, "Client", lambda **kw: client_type(transport=transport, **kw))
    symbols = NSEUniverseProvider(urls=["https://example.test/nse.csv"]).fetch()
    assert [row.symbol for row in symbols] == ["RELIANCE", "TRADE", "RESTRICTED", "JISLDVREQS"]
    assert symbols[1].source_identity == "INE001A01010"
    assert symbols[1].official_security_type == "fully_paid_equity:BE"


@pytest.mark.parametrize(
    "symbol,name,isin,series",
    [
        ("ABC-RE1", "ABC Limited", "INE001A01010", "BE"),
        ("ABC", "Rights Entitlement", "INE001A01010", "BE"),
        ("ABC", "ABC Limited", "INE001A01010", "XX"),
        ("ABC", "ABC Limited", "", "EQ"),
        ("ABC", "Partly Paid Shares", "INE001A01010", "EQ"),
    ],
)
def test_non_stock_and_unclassified_rows_fail_closed(symbol, name, isin, series):
    assert not is_ordinary_nse_equity(
        {"SYMBOL": symbol, "NAME OF COMPANY": name, "ISIN NUMBER": isin, "SERIES": series}
    )


def test_nse_missing_series_fails_closed(monkeypatch):
    client_type = httpx.Client
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text="SYMBOL\nRELIANCE\n"))
    monkeypatch.setattr(httpx, "Client", lambda **kw: client_type(transport=transport, **kw))
    provider = NSEUniverseProvider()
    with pytest.raises(ValueError, match="SYMBOL and SERIES"):
        provider._fetch_from_url.__wrapped__(provider, "https://example.test/nse.csv")
