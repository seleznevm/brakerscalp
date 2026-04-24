from __future__ import annotations

from brakerscalp.domain.models import Timeframe, UniverseSymbol, Venue
from brakerscalp.services.engine_service import EngineService


async def test_engine_generates_alert(repository, cache, make_breakout_market, make_book, make_health, make_derivatives) -> None:
    universe = [UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)]
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market()

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


async def test_engine_filters_unclosed_candle(repository, cache, make_breakout_market, make_book, make_health, make_derivatives) -> None:
    from datetime import datetime, timedelta, timezone

    universe = [UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)]
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market()
    candles_15m[-1].close_time = datetime.now(tz=timezone.utc) + timedelta(minutes=10)

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
    closed = service._closed_candles(candles_15m)
    assert len(closed) == len(candles_15m) - 1
    assert all(item.close_time <= datetime.now(tz=timezone.utc) for item in closed)


async def test_engine_suppresses_duplicate_setup(repository, cache, make_breakout_market, make_book, make_health, make_derivatives) -> None:
    universe = [UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)]
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market()

    await cache.store_candles("binance", "BTCUSDT", "4h", candles_4h)
    await cache.store_candles("binance", "BTCUSDT", "1h", candles_1h)
    await cache.store_candles("binance", "BTCUSDT", "15m", candles_15m)
    await cache.store_candles("binance", "BTCUSDT", "5m", candles_5m)
    await cache.store_book("binance", "BTCUSDT", make_book())
    await cache.store_derivative_context("binance", "BTCUSDT", make_derivatives())
    await cache.store_health("binance", "BTCUSDT", make_health())
    await cache.store_health("bybit", "BTCUSDT", make_health(venue=Venue.BYBIT))
    await cache.store_health("okx", "BTCUSDT", make_health(venue=Venue.OKX))

    service = EngineService(repository, cache, universe, alert_chat_ids=[123], interval_seconds=1, signal_duplicate_window_minutes=180)
    await service.run_once()
    first_alert = await cache.pop_alert(timeout=1)
    assert first_alert is not None

    await service.run_once()
    second_alert = await cache.pop_alert(timeout=1)
    assert second_alert is None


async def test_engine_respects_runtime_minimum_alert_confidence(repository, cache, make_breakout_market, make_book, make_health, make_derivatives) -> None:
    universe = [UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE)]
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market()

    await cache.store_candles("binance", "BTCUSDT", "4h", candles_4h)
    await cache.store_candles("binance", "BTCUSDT", "1h", candles_1h)
    await cache.store_candles("binance", "BTCUSDT", "15m", candles_15m)
    await cache.store_candles("binance", "BTCUSDT", "5m", candles_5m)
    await cache.store_book("binance", "BTCUSDT", make_book())
    await cache.store_derivative_context("binance", "BTCUSDT", make_derivatives())
    await cache.store_health("binance", "BTCUSDT", make_health())
    await cache.store_health("bybit", "BTCUSDT", make_health(venue=Venue.BYBIT))
    await cache.store_health("okx", "BTCUSDT", make_health(venue=Venue.OKX))
    await cache.set_minimum_alert_confidence(99.0)

    service = EngineService(repository, cache, universe, alert_chat_ids=[123], interval_seconds=1, minimum_alert_confidence=65.0)
    await service.run_once()
    alerts = await repository.list_latest_alerts()
    assert alerts
    queued = await cache.pop_alert(timeout=1)
    assert queued is None
