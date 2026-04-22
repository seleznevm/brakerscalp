from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone

import fakeredis.aioredis
import pytest

from brakerscalp.domain.models import BookSnapshot, DataHealth, DerivativeContext, MarketCandle, OrderBookLevel, Timeframe, Venue
from brakerscalp.storage.cache import StateCache
from brakerscalp.storage.db import create_engine, create_session_factory, init_db
from brakerscalp.storage.repository import Repository


@pytest.fixture
async def repository(tmp_path):
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    engine = create_engine(database_url)
    await init_db(engine)
    session_factory = create_session_factory(engine)
    yield Repository(session_factory)
    await engine.dispose()


@pytest.fixture
async def cache():
    redis = fakeredis.aioredis.FakeRedis()
    store = StateCache(redis)
    yield store
    await store.close()


@pytest.fixture
def make_candles() -> Callable[..., list[MarketCandle]]:
    def _make_candles(
        venue: Venue = Venue.BINANCE,
        symbol: str = "BTCUSDT",
        timeframe: Timeframe = Timeframe.M15,
        count: int = 60,
        start_price: float = 65000.0,
        step: float = 25.0,
        volume: float = 1000.0,
    ) -> list[MarketCandle]:
        candles: list[MarketCandle] = []
        price = start_price
        minutes = {"5m": 5, "15m": 15, "1h": 60, "4h": 240}[timeframe.value]
        now = datetime.now(tz=timezone.utc) - timedelta(minutes=count * minutes)
        for index in range(count):
            open_time = now + timedelta(minutes=index * minutes)
            close_time = open_time + timedelta(minutes=minutes)
            close = price + step
            candles.append(
                MarketCandle(
                    symbol=symbol,
                    venue=venue,
                    timeframe=timeframe,
                    open_time=open_time,
                    close_time=close_time,
                    open=price,
                    high=max(price, close) + 10,
                    low=min(price, close) - 10,
                    close=close,
                    volume=volume + index * 10,
                    quote_volume=(volume + index * 10) * close,
                    trade_count=100 + index,
                    taker_buy_volume=(volume + index * 10) * 0.55,
                    vwap=(price + close) / 2,
                )
            )
            price = close
        return candles

    return _make_candles


@pytest.fixture
def make_book() -> Callable[..., BookSnapshot]:
    def _make_book(symbol: str = "BTCUSDT", venue: Venue = Venue.BINANCE, mid: float = 66000.0) -> BookSnapshot:
        return BookSnapshot(
            symbol=symbol,
            venue=venue,
            timestamp=datetime.now(tz=timezone.utc),
            bids=[OrderBookLevel(price=mid - 0.5, size=20), OrderBookLevel(price=mid - 1.0, size=18)],
            asks=[OrderBookLevel(price=mid + 0.5, size=10), OrderBookLevel(price=mid + 1.0, size=8)],
            sequence_id="1",
            is_gap=False,
        )

    return _make_book


@pytest.fixture
def make_health() -> Callable[..., DataHealth]:
    def _make_health(symbol: str = "BTCUSDT", venue: Venue = Venue.BINANCE) -> DataHealth:
        return DataHealth(
            venue=venue,
            symbol=symbol,
            is_fresh=True,
            has_sequence_gap=False,
            spread_ratio=1.2,
            freshness_ms=100,
        )

    return _make_health


@pytest.fixture
def make_derivatives() -> Callable[..., DerivativeContext]:
    def _make_derivatives(symbol: str = "BTCUSDT", venue: Venue = Venue.BINANCE) -> DerivativeContext:
        return DerivativeContext(
            symbol=symbol,
            venue=venue,
            timestamp=datetime.now(tz=timezone.utc),
            funding_rate=0.0001,
            open_interest=1000000,
            mark_price=66000,
            index_price=65990,
            basis_bps=1.5,
        )

    return _make_derivatives
