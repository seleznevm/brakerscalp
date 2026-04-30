from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from brakerscalp.config import Settings
from brakerscalp.domain.models import AlertMessage, SignalClass
from brakerscalp.services.bot_service import BotService


@pytest.mark.asyncio
async def test_bot_service_falls_back_to_text_when_photo_send_fails(repository, cache) -> None:
    settings = Settings(
        _env_file=None,
        environment="test",
        bot_token="123456:TESTTOKEN",
        allowed_chat_ids=[1],
        alert_chat_ids=[1],
        database_url="sqlite+aiosqlite:///ignored.db",
        redis_url="redis://localhost:6379/0",
    )
    service = BotService(settings, repository, cache)
    service._build_alert_chart = AsyncMock(return_value=(b"fake-chart", "caption"))
    service.bot.send_photo = AsyncMock(side_effect=RuntimeError("photo upload failed"))
    service.bot.send_message = AsyncMock()

    await service._send_alert_bundle(
        AlertMessage(
            signal_id="signal-1",
            alert_key="alert-1",
            chat_id=1,
            text="test text",
            signal_class=SignalClass.WATCHLIST,
        )
    )

    service.bot.send_photo.assert_awaited_once()
    service.bot.send_message.assert_awaited_once()
    await service.bot.session.close()


@pytest.mark.asyncio
async def test_bot_service_falls_back_to_text_when_chart_render_fails(repository, cache) -> None:
    settings = Settings(
        _env_file=None,
        environment="test",
        bot_token="123456:TESTTOKEN",
        allowed_chat_ids=[1],
        alert_chat_ids=[1],
        database_url="sqlite+aiosqlite:///ignored.db",
        redis_url="redis://localhost:6379/0",
    )
    service = BotService(settings, repository, cache)
    service._build_alert_chart = AsyncMock(side_effect=RuntimeError("chart render failed"))
    service.bot.send_photo = AsyncMock()
    service.bot.send_message = AsyncMock()

    await service._send_alert_bundle(
        AlertMessage(
            signal_id="signal-2",
            alert_key="alert-2",
            chat_id=1,
            text="text only",
            signal_class=SignalClass.ACTIONABLE,
            created_at=datetime.now(tz=timezone.utc),
        )
    )

    service.bot.send_photo.assert_not_awaited()
    service.bot.send_message.assert_awaited_once()
    await service.bot.session.close()
