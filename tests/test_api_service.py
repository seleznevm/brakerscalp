from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import httpx
import openpyxl
import pytest

from brakerscalp.config import Settings
from brakerscalp.domain.models import DataHealth, Direction, MarketCandle, ScoreContribution, SetupType, SignalClass, SignalDecision, Timeframe, UniverseSymbol, Venue
from brakerscalp.services.api_service import build_api
from brakerscalp.universe import save_universe


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
        export = await client.get("/statistics/export.xlsx")
        apply_threshold = await client.get("/settings/apply-threshold?value=74.5")

    assert statistics.status_code == 200
    assert "Setup Statistics" in statistics.text
    assert "/statistics?range=week" in statistics.text
    assert "Export Excel" in statistics.text
    assert export.status_code == 200
    assert export.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert "attachment; filename=" in export.headers["content-disposition"]
    assert apply_threshold.status_code == 303
    assert apply_threshold.headers["location"] == "/settings?threshold_saved=1"
    assert await cache.get_minimum_alert_confidence(65.0) == 74.5


@pytest.mark.asyncio
async def test_statistics_export_contains_trade_simulation_columns(repository, cache) -> None:
    detected_at = datetime.now(tz=timezone.utc) - timedelta(days=1)
    decision = SignalDecision(
        symbol="BTCUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.M15,
        setup=SetupType.BREAKOUT,
        direction=Direction.LONG,
        signal_class=SignalClass.ACTIONABLE,
        confidence=88.0,
        level_id="btc-breakout-level",
        alert_key="btc-breakout-test",
        detected_at=detected_at,
        entry_price=100.0,
        invalidation_price=95.0,
        targets=[105.0, 110.0],
        expected_rr=1.2,
        rationale=["Impulse confirmed", "Volume expansion"],
        why_not_higher=["Limited live history"],
        contributions=[ScoreContribution(group="level", score=20.0, max_score=25.0, reason="Strong level")],
        data_health=DataHealth(venue=Venue.BINANCE, symbol="BTCUSDT", is_fresh=True, freshness_ms=0),
        feature_snapshot={"atr_15m": 2.5},
        render_context={"trigger": "15m close above 100.0", "price_zone": "99.0 - 100.0"},
    )
    await repository.save_signal(decision)
    await repository.upsert_candles(
        [
            MarketCandle(
                symbol="BTCUSDT",
                venue=Venue.BINANCE,
                timeframe=Timeframe.M15,
                open_time=detected_at - timedelta(minutes=15),
                close_time=detected_at,
                open=99.0,
                high=101.0,
                low=98.5,
                close=100.5,
                volume=1000.0,
                quote_volume=100500.0,
                trade_count=10,
                taker_buy_volume=550.0,
                vwap=100.0,
            ),
            MarketCandle(
                symbol="BTCUSDT",
                venue=Venue.BINANCE,
                timeframe=Timeframe.M15,
                open_time=detected_at,
                close_time=detected_at + timedelta(minutes=15),
                open=100.5,
                high=106.0,
                low=100.0,
                close=105.5,
                volume=1300.0,
                quote_volume=137150.0,
                trade_count=12,
                taker_buy_volume=700.0,
                vwap=103.0,
            ),
        ]
    )

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
        export = await client.get(
            f"/statistics/export.xlsx?range=custom&start={(detected_at.date() - timedelta(days=1)).isoformat()}&end={detected_at.date().isoformat()}"
        )

    workbook = openpyxl.load_workbook(BytesIO(export.content))
    signals_sheet = workbook["signals"]
    headers = [cell.value for cell in next(signals_sheet.iter_rows(min_row=1, max_row=1))]
    values = [cell.value for cell in next(signals_sheet.iter_rows(min_row=2, max_row=2))]

    assert "Trigger" in headers
    assert "Rationale" in headers
    assert "Entry price" in headers
    assert "TP1 price" in headers
    assert "TP2 price" in headers
    assert "SL price" in headers
    assert "Entry date" in headers
    assert "TP1 date" in headers
    assert "Final PnL %" in headers
    assert "Trade duration" in headers
    assert "BTCUSDT" in values


@pytest.mark.asyncio
async def test_settings_universe_add_and_remove_updates_repository_and_cache(repository, cache, tmp_path: Path) -> None:
    universe_path = tmp_path / "universe.json"
    save_universe(universe_path, [UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)])
    await repository.replace_runtime_universe([UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)])
    await cache.store_universe([UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)])

    settings = Settings(
        _env_file=None,
        environment="test",
        bot_token="test-token",
        allowed_chat_ids=[1],
        alert_chat_ids=[1],
        database_url="sqlite+aiosqlite:///ignored.db",
        redis_url="redis://localhost:6379/0",
        universe_path=universe_path,
    )
    app = build_api(repository, cache, settings, universe=[UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)], adapters={Venue.BINANCE: object()})
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver", follow_redirects=False) as client:
        add_response = await client.get("/settings/universe/add?symbol=ETH&venue=binance")
        remove_response = await client.get("/settings/universe/remove?symbol=BTCUSDT")

    runtime_universe = await repository.list_runtime_universe()
    cached_universe = await cache.get_universe_symbols()

    assert add_response.status_code == 303
    assert remove_response.status_code == 303
    assert {item.symbol for item in runtime_universe} == {"ETHUSDT"}
    assert {item.symbol for item in cached_universe} == {"ETHUSDT"}
