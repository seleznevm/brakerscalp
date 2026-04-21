from __future__ import annotations

from brakerscalp.domain.models import Timeframe, UniverseSymbol, Venue
from brakerscalp.services.engine_service import EngineService


async def test_engine_generates_alert(repository, cache, make_candles, make_book, make_health, make_derivatives) -> None:
    universe = [UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)]
    candles_4h = make_candles(timeframe=Timeframe.H4, count=40, step=50)
    candles_1h = make_candles(timeframe=Timeframe.H1, count=200, step=15)
    candles_15m = make_candles(timeframe=Timeframe.M15, count=80, step=10)
    candles_5m = make_candles(timeframe=Timeframe.M5, count=80, step=6)
    candles_15m[-1].close += 500
    candles_15m[-1].high = candles_15m[-1].close + 20
    candles_15m[-1].volume *= 8

    await cache.store_candles("binance", "BTCUSDT", "4h", candles_4h)
    await cache.store_candles("binance", "BTCUSDT", "1h", candles_1h)
    await cache.store_candles("binance", "BTCUSDT", "15m", candles_15m)
    await cache.store_candles("binance", "BTCUSDT", "5m", candles_5m)
    await cache.store_book("binance", "BTCUSDT", make_book())
    await cache.store_derivative_context("binance", "BTCUSDT", make_derivatives())
    await cache.store_health("binance", "BTCUSDT", make_health())
    await cache.store_health("bybit", "BTCUSDT", make_health(venue=Venue.BYBIT))
    await cache.store_health("okx", "BTCUSDT", make_health(venue=Venue.OKX))

    service = EngineService(repository, cache, universe, alert_chat_ids=[123], interval_seconds=1)
    await service.run_once()
    alerts = await repository.list_latest_alerts()
    assert alerts
    queued = await cache.pop_alert(timeout=1)
    assert queued is not None
    assert queued.chat_id == 123
