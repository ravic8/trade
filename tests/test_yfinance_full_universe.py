from trade_research.schemas import Symbol
from trade_research.universe.yfinance_full import (
    YFinanceCanadaUniverseProvider,
    YFinanceUSUniverseProvider,
    yfinance_exchange_for_universe,
    yfinance_universe_id,
)


def test_yfinance_us_universe_parses_nasdaq_symbol_directory(monkeypatch) -> None:
    payloads = {
        "nasdaq": (
            "Symbol|Security Name|Market Category|Test Issue|Financial Status|"
            "Round Lot Size|ETF|NextShares\n"
            "AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N\n"
            "TEST|NASDAQ TEST STOCK|Q|Y|N|100|N|N\n"
            "QQQ|Invesco QQQ Trust|Q|N|N|100|Y|N\n"
            "File Creation Time: 0714202618:04|||||||\n"
        ),
        "other": (
            "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|"
            "Test Issue|NASDAQ Symbol\n"
            "BRK.B|Berkshire Hathaway Inc. Class B|N|BRK.B|N|100|N|BRK.B\n"
            "BAC$E|Bank of America Depositary Shares|N|BAC$E|N|100|N|BAC$E\n"
            "SPY|SPDR S&P 500 ETF Trust|P|SPY|Y|100|N|SPY\n"
            "AAC.U|Ares Acquisition Corporation III Units|N|AAC.U|N|100|N|AAC.U\n"
        ),
    }

    monkeypatch.setattr(
        "trade_research.universe.yfinance_full._fetch_text",
        lambda url: payloads[url],
    )

    symbols = YFinanceUSUniverseProvider(nasdaq_url="nasdaq", other_url="other").fetch()

    assert [item.symbol for item in symbols] == ["AAPL", "BRK.B"]
    assert [item.yahoo_symbol for item in symbols] == ["AAPL", "BRK-B"]
    assert symbols[0].source == "nasdaq_trader_symbol_directory"


def test_yfinance_canada_universe_wraps_tsx_provider() -> None:
    class FakeTSXProvider:
        def fetch(self):
            return [
                Symbol(
                    symbol="SHOP",
                    exchange="TSX",
                    yahoo_symbol="SHOP.TO",
                    name="Shopify",
                    currency="CAD",
                    source="tsx_test",
                    source_url="test",
                )
            ]

    symbols = YFinanceCanadaUniverseProvider(tsx_provider=FakeTSXProvider()).fetch()

    assert len(symbols) == 1
    assert symbols[0].symbol == "SHOP"
    assert symbols[0].exchange == "CA"
    assert symbols[0].yahoo_symbol == "SHOP.TO"
    assert symbols[0].source == "tsx_test"


def test_yfinance_universe_ids_and_exchanges_support_full_aliases() -> None:
    assert yfinance_universe_id("us_all") == "us_all"
    assert yfinance_universe_id("canada_all") == "canada_all"
    assert yfinance_exchange_for_universe("us_all") == "US"
    assert yfinance_exchange_for_universe("canada_all") == "CA"
