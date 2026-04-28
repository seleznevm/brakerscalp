from __future__ import annotations

import asyncio

from brakerscalp.domain.models import DataHealth, Timeframe, UniverseSymbol, Venue
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
        symbol_concurrency: int = 6,
        exchange_book_depth: int = 10,
        exchange_trades_limit: int = 50,
    ) -> None:
        self.adapters: dict = adapters
        self.repository = repository
        self.cache = cache
        self.universe = universe
        self.poll_interval_seconds = poll_interval_seconds
        self.symbol_concurrency = max(symbol_concurrency, 1)
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
        runtime_universe = await self._current_universe()
        semaphore = asyncio.Semaphore(self.symbol_concurrency)

        async def _collect(item: UniverseSymbol) -> None:
            async with semaphore:
                await self._collect_universe_symbol(item)

        await asyncio.gather(*[_collect(item) for item in runtime_universe])
        if hasattr(self.cache, "store_service_heartbeat"):
            await self.cache.store_service_heartbeat(
                "collector",
                {
                    "symbols": len(runtime_universe),
                    "concurrency": self.symbol_concurrency,
                },
            )

    async def _collect_universe_symbol(self, item: UniverseSymbol) -> None:
        primary_adapter = self.adapters.get(item.primary_venue)
        tasks = []
        if primary_adapter is None:
            await self._record_collection_failure(
                item.primary_venue,
                item.symbol,
                RuntimeError("primary venue adapter is disabled or unavailable"),
                stage="primary-config",
            )
        else:
            tasks.append(self._collect_primary_symbol(primary_adapter, item.primary_venue, item.symbol))

        for venue, adapter in self.adapters.items():
            if venue == item.primary_venue:
                continue
            tasks.append(self._collect_secondary_health(adapter, venue, item.symbol))

        if tasks:
            await asyncio.gather(*tasks)

    async def _collect_primary_symbol(self, adapter: ExchangeAdapter, venue: Venue, symbol: str) -> None:
        try:
            await self._collect_symbol(adapter, venue.value, symbol)
        except Exception as exc:
            await self._record_collection_failure(venue, symbol, exc, stage="primary")

    async def _collect_secondary_health(self, adapter: ExchangeAdapter, venue: Venue, symbol: str) -> None:
        try:
            health = await adapter.healthcheck(symbol)
        except Exception as exc:
            await self._record_collection_failure(venue, symbol, exc, stage="secondary-health")
            return

        await self.cache.store_health(venue.value, symbol, health)
        await self.repository.upsert_health(health)
        VENUE_HEALTH.labels(venue=venue.value, symbol=symbol).set(1 if health.is_fresh and not health.has_sequence_gap else 0)

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

    async def _record_collection_failure(self, venue: Venue, symbol: str, exc: Exception, stage: str) -> None:
        health = DataHealth(
            venue=venue,
            symbol=symbol,
            is_fresh=False,
            has_sequence_gap=True,
            freshness_ms=86_400_000,
            notes=[f"{stage}: {type(exc).__name__}: {exc}"],
        )
        await self.cache.store_health(venue.value, symbol, health)
        await self.repository.upsert_health(health)
        VENUE_HEALTH.labels(venue=venue.value, symbol=symbol).set(0)
        self.logger.warning("collector-symbol-failed", venue=venue.value, symbol=symbol, stage=stage, error=str(exc))

    async def _current_universe(self) -> list[UniverseSymbol]:
        allowed_venues = set(self.adapters)
        if hasattr(self.repository, "list_runtime_universe"):
            persisted = await self.repository.list_runtime_universe(enabled_venues=[item.value for item in allowed_venues])
            if persisted:
                if hasattr(self.cache, "store_universe"):
                    await self.cache.store_universe(persisted)
                return [item for item in persisted if item.primary_venue in allowed_venues]
        if hasattr(self.cache, "get_universe_symbols"):
            runtime_universe = await self.cache.get_universe_symbols(self.universe)
            if runtime_universe:
                return [item for item in runtime_universe if item.primary_venue in allowed_venues]
        return [item for item in self.universe if item.primary_venue in allowed_venues]
