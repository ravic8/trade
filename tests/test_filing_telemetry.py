from trade_research.filings.telemetry import sanitize_telemetry


def test_sanitize_telemetry_accepts_langfuse_keyword_contract() -> None:
    result = sanitize_telemetry(
        data={
            "run_id": "run-1",
            "nested": {
                "raw_text": "sensitive filing text",
                "safe": "retained",
            },
            "long_value": "x" * 501,
        },
        observation_type="span",
    )

    assert result == {
        "run_id": "run-1",
        "nested": {
            "raw_text": "[REDACTED]",
            "safe": "retained",
        },
        "long_value": ("x" * 120) + "...[TRUNCATED]",
    }


def test_sanitize_telemetry_preserves_internal_positional_contract() -> None:
    assert sanitize_telemetry(
        {
            "prompt": "sensitive prompt",
            "items": [{"content": "sensitive content"}, "safe"],
        }
    ) == {
        "prompt": "[REDACTED]",
        "items": [{"content": "[REDACTED]"}, "safe"],
    }
