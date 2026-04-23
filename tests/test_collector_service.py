from __future__ import annotations

from collections import defaultdict

import pytest

from brakerscalp.domain.models import DataHealth, UniverseSymbol, Venue
from brakerscalp.exchanges.base import ExchangeAdapter
from brakerscalp.services.collector_service import CollectorService


class RecordingRepository:
    def __init__(self) -> None:
        self.candles = []
        self.health = []

    async def upsert_candles(self, candles) -> None:
        self.candles.extend(candles)

    async def upsert_health(self, health) -> None:
        self.health.append(health)


class RecordingCache:
    def __init__(self) -> None:
        self.candles = {}
        self.books = {}
        self.trades = {}
        self.derivatives = {}
        self.health = {}

    async def store_candles(self, venue: str, symbol: str, timeframe: str, candles) -> None:
        self.candles[(venue, symbol, timeframe)] = candles

    async def store_book(self, venue: str, symbol: str, book) -> None:
        self.books[(venue, symbol)] = book

    async def store_trades(self, venue: str, symbol: str, trades) -> None:
        self.trades[(venue, symbol)] = trades

    async def store_derivative_context(self, venue: str, symbol: str, context) -> None:
        self.derivatives[(venue, symbol)] = context

    async def store_health(self, venue: str, symbol: str, health) -> None:
        self.health[(venue, symbol)] = health


class FakeAdapter(ExchangeAdapter):
    venue: Venue
    base_url = "https://example.com"

    def __init__(
        self,
        *,
        venue: Venue,
        candles_factory,
        book_factory,
        derivatives_factory,
        health_factory,
        fail_primary_symbols: set[str] | None = None,
        fail_health_symbols: set[str] | None = None,
    ) -> None:
        super().__init__(timeout_seconds=1.0, base_url=self.base_url)
        self.venue = venue
        self.candles_factory = candles_factory
        self.book_factory = book_factory
        self.derivatives_factory = derivatives_factory
        self.health_factory = health_factory
        self.fail_primary_symbols = fail_primary_symbols or set()
        self.fail_health_symbols = fail_health_symbols or set()
        self.calls: dict[str, list] = defaultdict(list)

    async def fetch_recent_candles(self, symbol, timeframe, limit=300):
        self.calls["candles"].append((symbol, timeframe.value))
        if symbol in self.fail_primary_symbols:
            raise RuntimeError(f"candles unavailable for {symbol}")
        count = 24 if timeframe.value in {"1h", "4h"} else 12
        return self.candles_factory(venue=self.venue, symbol=symbol, timeframe=timeframe, count=count)

    async def fetch_top_book(self, symbol, depth=10):
        self.calls["book"].append(symbol)
        if symbol in self.fail_primary_symbols:
            raise RuntimeError(f"book unavailable for {symbol}")
        return self.book_factory(symbol=symbol, venue=self.venue)

    async def fetch_trades(self, symbol, limit=50):
        self.calls["trades"].append(symbol)
        if symbol in self.fail_primary_symbols:
            raise RuntimeError(f"trades unavailable for {symbol}")
        return []

    async def fetch_derivative_context(self, symbol):
        self.calls["derivatives"].append(symbol)
        if symbol in self.fail_primary_symbols:
            raise RuntimeError(f"derivatives unavailable for {symbol}")
        return self.derivatives_factory(symbol=symbol, venue=self.venue)

    async def healthcheck(self, symbol) -> DataHealth:
        self.calls["health"].append(symbol)
        if symbol in self.fail_primary_symbols or symbol in self.fail_health_symbols:
            raise RuntimeError(f"health unavailable for {symbol}")
        return self.health_factory(symbol=symbol, venue=self.venue)


@pytest.mark.asyncio
async def test_collector_collects_every_symbol_on_primary_venue(make_candles, make_book, make_derivatives, make_health):
    repository = RecordingRepository()
    cache = RecordingCache()
    universe = [
        UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE),
        UniverseSymbol(symbol="ETHUSDT", primary_venue=Venue.BYBIT),
        UniverseSymbol(symbol="SOLUSDT", primary_venue=Venue.OKX),
    ]
    adapters = {
        Venue.BINANCE: FakeAdapter(
            venue=Venue.BINANCE,
            candles_factory=make_candles,
            book_factory=make_book,
            derivatives_factory=make_derivatives,
            health_factory=make_health,
        ),
        Venue.BYBIT: FakeAdapter(
            venue=Venue.BYBIT,
            candles_factory=make_candles,
            book_factory=make_book,
            derivatives_factory=make_derivatives,
            health_factory=make_health,
        ),
        Venue.OKX: FakeAdapter(
            venue=Venue.OKX,
            candles_factory=make_candles,
            book_factory=make_book,
            derivatives_factory=make_derivatives,
            health_factory=make_health,
        ),
    }
    service = CollectorService(adapters, repository, cache, universe, poll_interval_seconds=1, symbol_concurrency=3)

    try:
        await service.collect_once()
    finally:
        for adapter in adapters.values():
            await adapter.aclose()

    assert {candle.symbol for candle in repository.candles} == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    assert {symbol for (_, symbol) in cache.books} == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    assert {symbol for (_, symbol) in cache.derivatives} == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}

    assert {symbol for symbol, _ in adapters[Venue.BINANCE].calls["candles"]} == {"BTCUSDT"}
    assert {symbol for symbol, _ in adapters[Venue.BYBIT].calls["candles"]} == {"ETHUSDT"}
    assert {symbol for symbol, _ in adapters[Venue.OKX].calls["candles"]} == {"SOLUSDT"}

    for adapter in adapters.values():
        assert set(adapter.calls["health"]) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}


@pytest.mark.asyncio
async def test_collector_isolates_symbol_failures_and_continues(make_candles, make_book, make_derivatives, make_health):
    repository = RecordingRepository()
    cache = RecordingCache()
    universe = [
        UniverseSymbol(symbol="BTCUSDT", primary_venue=Venue.BINANCE),
        UniverseSymbol(symbol="ETHUSDT", primary_venue=Venue.BYBIT),
        UniverseSymbol(symbol="SOLUSDT", primary_venue=Venue.OKX),
    ]
    adapters = {
        Venue.BINANCE: FakeAdapter(
            venue=Venue.BINANCE,
            candles_factory=make_candles,
            book_factory=make_book,
            derivatives_factory=make_derivatives,
            health_factory=make_health,
            fail_health_symbols={"SOLUSDT"},
        ),
        Venue.BYBIT: FakeAdapter(
            venue=Venue.BYBIT,
            candles_factory=make_candles,
            book_factory=make_book,
            derivatives_factory=make_derivatives,
            health_factory=make_health,
            fail_primary_symbols={"ETHUSDT"},
        ),
        Venue.OKX: FakeAdapter(
            venue=Venue.OKX,
            candles_factory=make_candles,
            book_factory=make_book,
            derivatives_factory=make_derivatives,
            health_factory=make_health,
        ),
    }
    service = CollectorService(adapters, repository, cache, universe, poll_interval_seconds=1, symbol_concurrency=3)

    try:
        await service.collect_once()
    finally:
        for adapter in adapters.values():
            await adapter.aclose()

    collected_symbols = {candle.symbol for candle in repository.candles}
    assert "BTCUSDT" in collected_symbols
    assert "SOLUSDT" in collected_symbols
    assert "ETHUSDT" not in collected_symbols

    assert cache.health[("bybit", "ETHUSDT")].is_fresh is False
    assert cache.health[("binance", "SOLUSDT")].is_fresh is False
    assert "primary" in cache.health[("bybit", "ETHUSDT")].notes[0]
    assert "secondary-health" in cache.health[("binance", "SOLUSDT")].notes[0]
