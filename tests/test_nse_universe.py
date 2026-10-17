import httpx
import pytest

from trade_data.universe.nse import NSEUniverseProvider


@pytest.mark.parametrize("header", ["SYMBOL, SERIES", " SYMBOL , SERIES "])
def test_nse_csv_padding_does_not_admit_non_eq_series(monkeypatch, header):
    csv = header + "\n RELIANCE , EQ \nTRADE,BE\nRESTRICTED,BZ\n"
    client_type = httpx.Client
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=csv))
    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: client_type(transport=transport, **kwargs)
    )

    symbols = NSEUniverseProvider(urls=["https://example.test/nse.csv"]).fetch()

    assert [(row.symbol, row.yahoo_symbol) for row in symbols] == [
        ("RELIANCE", "RELIANCE.NS")
    ]


def test_nse_missing_series_fails_closed(monkeypatch):
    client_type = httpx.Client
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="SYMBOL\nRELIANCE\n")
    )
    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: client_type(transport=transport, **kwargs)
    )
    provider = NSEUniverseProvider()

    with pytest.raises(ValueError, match="SYMBOL and SERIES"):
        provider._fetch_from_url.__wrapped__(provider, "https://example.test/nse.csv")
