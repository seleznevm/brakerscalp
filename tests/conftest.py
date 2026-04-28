from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone

import fakeredis.aioredis
import pytest

from brakerscalp.domain.models import BookSnapshot, DataHealth, DerivativeContext, MarketCandle, OrderBookLevel, Side, Timeframe, TradeTick, Venue
from brakerscalp.signals.levels import LevelDetector
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


@pytest.fixture
def make_trades() -> Callable[..., list[TradeTick]]:
    def _make_trades(
        symbol: str = "BTCUSDT",
        venue: Venue = Venue.BINANCE,
        *,
        bias: str = "buy",
        count: int = 40,
        start_price: float = 66000.0,
    ) -> list[TradeTick]:
        trades: list[TradeTick] = []
        start = datetime.now(tz=timezone.utc) - timedelta(minutes=count)
        for index in range(count):
            dominant_side = Side.BUY if bias == "buy" else Side.SELL
            secondary_side = Side.SELL if dominant_side == Side.BUY else Side.BUY
            side = dominant_side if index % 5 else secondary_side
            size = 2.4 if side == dominant_side else 0.7
            trades.append(
                TradeTick(
                    symbol=symbol,
                    venue=venue,
                    timestamp=start + timedelta(seconds=index * 30),
                    price=start_price + index * 2.0,
                    size=size,
                    side=side,
                )
            )
        return trades

    return _make_trades


@pytest.fixture
def make_breakout_market(make_candles):
    def _make(symbol: str = "BTCUSDT", venue: Venue = Venue.BINANCE):
        candles_4h = make_candles(venue=venue, symbol=symbol, timeframe=Timeframe.H4, count=40, step=50.0)
        candles_1h = make_candles(venue=venue, symbol=symbol, timeframe=Timeframe.H1, count=200, step=15.0)
        levels = LevelDetector().detect(symbol, venue, candles_4h, candles_1h)
        resistance_levels = [item for item in levels if item.kind.value == "resistance"]
        reference_level = max(resistance_levels, key=lambda item: item.reference_price)

        consolidation_base = reference_level.upper_price - 25.0
        candles_15m = make_candles(
            venue=venue,
            symbol=symbol,
            timeframe=Timeframe.M15,
            count=80,
            step=0.0,
            start_price=consolidation_base - 30.0,
            volume=1200.0,
        )
        candles_5m = make_candles(
            venue=venue,
            symbol=symbol,
            timeframe=Timeframe.M5,
            count=80,
            step=0.0,
            start_price=consolidation_base - 20.0,
            volume=900.0,
        )

        lows = [reference_level.upper_price - 85.0, reference_level.upper_price - 72.0, reference_level.upper_price - 60.0, reference_level.upper_price - 48.0, reference_level.upper_price - 38.0, reference_level.upper_price - 30.0, reference_level.upper_price - 22.0]
        closes = [reference_level.upper_price - 42.0, reference_level.upper_price - 37.0, reference_level.upper_price - 33.0, reference_level.upper_price - 28.0, reference_level.upper_price - 24.0, reference_level.upper_price - 19.0, reference_level.upper_price - 14.0]
        for candle, low, close in zip(candles_15m[-8:-1], lows, closes, strict=True):
            candle.open = close - 4.0
            candle.close = close
            candle.low = low
            candle.high = min(reference_level.upper_price - 4.0, close + 10.0)
            candle.volume = 1500.0
            candle.quote_volume = candle.volume * candle.close
            candle.vwap = (candle.open + candle.close) / 2

        breakout = candles_15m[-1]
        breakout.open = reference_level.upper_price - 18.0
        breakout.low = reference_level.upper_price - 22.0
        breakout.close = reference_level.upper_price + 95.0
        breakout.high = breakout.close + 8.0
        breakout.volume = 12000.0
        breakout.quote_volume = breakout.volume * breakout.close
        breakout.vwap = (breakout.open + breakout.close) / 2

        for index, candle in enumerate(candles_5m[-3:], start=1):
            candle.open = reference_level.upper_price + 8.0 * (index - 1)
            candle.low = reference_level.upper_price + 4.0 * (index - 1)
            candle.close = reference_level.upper_price + 14.0 * index
            candle.high = candle.close + 6.0
            candle.volume = 2200.0 + index * 200.0
            candle.quote_volume = candle.volume * candle.close
            candle.vwap = (candle.open + candle.close) / 2

        return candles_4h, candles_1h, candles_15m, candles_5m

    return _make
