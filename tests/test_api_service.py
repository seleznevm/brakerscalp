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
    assert "/screener" in root.text
    assert "/statistics" in root.text
    assert services.status_code == 200
    assert "PostgreSQL" in services.text


@pytest.mark.asyncio
async def test_statistics_page_and_threshold_route_render(repository, cache) -> None:
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
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver", follow_redirects=False) as client:
        statistics = await client.get("/statistics")
        apply_threshold = await client.get("/settings/apply-threshold?value=74.5")

    assert statistics.status_code == 200
    assert "/statistics?range=week" in statistics.text
    assert apply_threshold.status_code == 303
    assert apply_threshold.headers["location"] == "/settings?threshold_saved=1"
    assert await cache.get_minimum_alert_confidence(65.0) == 74.5
