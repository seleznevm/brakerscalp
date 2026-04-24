from __future__ import annotations

import httpx
import pytest

from brakerscalp.config import Settings
from brakerscalp.services.api_service import build_api


@pytest.mark.asyncio
async def test_command_center_root_and_services_pages_render(repository, cache) -> None:
    settings = Settings(
        _env_file=None,
        environment="test",
        bot_token="test-token",
        allowed_chat_ids=[1],
        alert_chat_ids=[1],
        database_url="sqlite+aiosqlite:///ignored.db",
        redis_url="redis://localhost:6379/0",
    )
    app = build_api(repository, cache, settings, universe=[], adapters={})
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        root = await client.get("/")
        services = await client.get("/services")

    assert root.status_code == 200
    assert "Командный пункт импульсного скальпинга" in root.text
    assert "/screener" in root.text
    assert services.status_code == 200
    assert "Проверка всех сервисов" in services.text
