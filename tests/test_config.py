from __future__ import annotations

from brakerscalp.config import Settings


def test_chat_id_parsing_from_csv() -> None:
    settings = Settings(
        bot_token="x",
        allowed_chat_ids="1, 2, 3",
        alert_chat_ids="10,11",
    )
    assert settings.allowed_chat_ids == [1, 2, 3]
    assert settings.effective_alert_chat_ids == [10, 11]


def test_alert_chat_ids_fallback_to_allowed() -> None:
    settings = Settings(
        bot_token="x",
        allowed_chat_ids="100,200",
        alert_chat_ids="",
        enable_okx=False,
    )
    assert settings.effective_alert_chat_ids == [100, 200]
    assert settings.enabled_venues == ["binance", "bybit"]
