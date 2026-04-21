from __future__ import annotations

import asyncio

from brakerscalp.domain.models import Timeframe, UniverseSymbol
from brakerscalp.exchanges.base import ExchangeAdapter
from brakerscalp.logging import get_logger
from brakerscalp.metrics import VENUE_HEALTH
from brakerscalp.storage.cache import StateCache
from brakerscalp.storage.repository import Repository


class CollectorService:
    def __init__(
        self,
        adapters: dict,
        repository: Repository,
        cache: StateCache,
        universe: list[UniverseSymbol],
        poll_interval_seconds: int,
        exchange_book_depth: int = 10,
        exchange_trades_limit: int = 50,
    ) -> None:
        self.adapters: dict = adapters
        self.repository = repository
        self.cache = cache
        self.universe = universe
        self.poll_interval_seconds = poll_interval_seconds
        self.exchange_book_depth = exchange_book_depth
        self.exchange_trades_limit = exchange_trades_limit
        self.logger = get_logger("collector")

    async def run(self) -> None:
        while True:
            try:
                await self.collect_once()
            except Exception as exc:
                self.logger.exception("collector-cycle-failed", error=str(exc))
            await asyncio.sleep(self.poll_interval_seconds)

    async def collect_once(self) -> None:
        for item in self.universe:
            for venue, adapter in self.adapters.items():
                await self._collect_symbol(adapter, venue.value, item.symbol)

    async def _collect_symbol(self, adapter: ExchangeAdapter, venue: str, symbol: str) -> None:
        timeframes = [Timeframe.H4, Timeframe.H1, Timeframe.M15, Timeframe.M5]
        candles_by_timeframe = await asyncio.gather(
            *[adapter.fetch_recent_candles(symbol, timeframe, limit=240 if timeframe in {Timeframe.H1, Timeframe.H4} else 120) for timeframe in timeframes]
        )
        all_candles = [candle for candles in candles_by_timeframe for candle in candles]
        await self.repository.upsert_candles(all_candles)
        for timeframe, candles in zip(timeframes, candles_by_timeframe, strict=True):
            await self.cache.store_candles(venue, symbol, timeframe.value, candles[-240:])

        book, trades, derivatives, health = await asyncio.gather(
            adapter.fetch_top_book(symbol, depth=self.exchange_book_depth),
            adapter.fetch_trades(symbol, limit=self.exchange_trades_limit),
            adapter.fetch_derivative_context(symbol),
            adapter.healthcheck(symbol),
        )
        await self.cache.store_book(venue, symbol, book)
        await self.cache.store_trades(venue, symbol, trades)
        await self.cache.store_derivative_context(venue, symbol, derivatives)
        await self.cache.store_health(venue, symbol, health)
        await self.repository.upsert_health(health)
        VENUE_HEALTH.labels(venue=venue, symbol=symbol).set(1 if health.is_fresh and not health.has_sequence_gap else 0)
        self.logger.info("collector-updated", venue=venue, symbol=symbol, candles=len(all_candles), trades=len(trades))
