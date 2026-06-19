import pandas as pd

from trade_research.modeling.one_percent_open import reached_one_percent_from_open


def test_reached_one_percent_from_open() -> None:
    frame = pd.DataFrame(
        {
            "Open": [100.0, 100.0, 200.0],
            "High": [101.0, 100.99, 203.0],
        }
    )

    assert reached_one_percent_from_open(frame).tolist() == [True, False, True]
