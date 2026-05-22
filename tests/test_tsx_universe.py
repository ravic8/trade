from io import StringIO

import pandas as pd

from trade_research.universe.tsx import TSXUniverseProvider


def test_tsx_universe_uses_yahoo_hyphens_for_classes_and_units(monkeypatch) -> None:
    csv = StringIO("Symbol,Stock Name\nBBD.B,Bombardier\nAP.UN,Allied Properties\n")
    read_csv = pd.read_csv

    monkeypatch.setattr(pd, "read_csv", lambda _: read_csv(csv))

    symbols = TSXUniverseProvider(csv_url="test").fetch()

    assert [symbol.yahoo_symbol for symbol in symbols] == ["BBD-B.TO", "AP-UN.TO"]
