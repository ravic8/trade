from io import StringIO

import pandas as pd

from trade_research.universe.tsx import TSXUniverseProvider


def test_tsx_universe_uses_yahoo_hyphens_for_classes_and_units(monkeypatch) -> None:
    csv = StringIO("Symbol,Stock Name\nBBD.B,Bombardier\nAP.UN,Allied Properties\n")
    read_csv = pd.read_csv

    monkeypatch.setattr(pd, "read_csv", lambda _: read_csv(csv))

    symbols = TSXUniverseProvider(csv_url="test").fetch()

    assert [symbol.yahoo_symbol for symbol in symbols] == ["AP-UN.TO", "BBD-B.TO"]


def test_tsx_universe_filters_other_canadian_venues_and_preserves_yahoo_symbols(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "trade_research.universe.tsx.pd.read_csv",
        lambda _: pd.DataFrame(
            {
                "Symbol": [
                    "SHOP.TO",
                    "BBD-B.TO",
                    "AP-UN.TO",
                    "AAPL.NE",
                    "ABRA.V",
                    "SHOP.TO",
                ],
                "Stock Name": [
                    "Shopify",
                    "Bombardier",
                    "Allied Properties",
                    "Apple CDR",
                    "AbraSilver",
                    "Duplicate Shopify",
                ],
            }
        ),
    )

    symbols = TSXUniverseProvider(csv_url="test").fetch()

    assert [(item.symbol, item.yahoo_symbol) for item in symbols] == [
        ("AP.UN", "AP-UN.TO"),
        ("BBD.B", "BBD-B.TO"),
        ("SHOP", "SHOP.TO"),
    ]
    assert {item.exchange for item in symbols} == {"TSX"}
    assert next(item for item in symbols if item.symbol == "SHOP").name == "Shopify"
    assert TSXUniverseProvider(csv_url="unused").last_diagnostics == {}

    diagnostics = TSXUniverseProvider(csv_url="test")
    diagnostics.fetch()
    assert diagnostics.diagnostics() == {
        "source_rows": 6,
        "tsx_rows": 3,
        "excluded_tsxv_rows": 1,
        "excluded_cboe_canada_rows": 1,
        "invalid_rows": 0,
        "duplicate_rows": 1,
    }
