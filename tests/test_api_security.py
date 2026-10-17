from trade_data.api.security import cors_origins


def test_cors_origins_drop_blank_values() -> None:
    assert cors_origins(" https://data.example.com, ,http://localhost:5173 ") == [
        "https://data.example.com", "http://localhost:5173",
    ]
