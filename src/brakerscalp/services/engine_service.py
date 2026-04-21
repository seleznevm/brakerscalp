from __future__ import annotations

import asyncio

from brakerscalp.domain.models import BookSnapshot, DataHealth, DerivativeContext, MarketCandle, SignalClass, UniverseSymbol, Venue
from brakerscalp.logging import get_logger
from brakerscalp.metrics import ALERTS_TOTAL, SIGNALS_IN_DB, STALE_DATA_RATIO
from brakerscalp.serialization import loads
from brakerscalp.signals.engine import EngineInput, RuleEngine
from brakerscalp.signals.levels import LevelDetector
from brakerscalp.signals.rendering import to_alert_message
from brakerscalp.storage.cache import StateCache
from brakerscalp.storage.repository import Repository


def parse_model_list(raw: list[dict], model):
    return [model.model_validate(item) for item in raw]


class EngineService:
    def __init__(
        self,
        repository: Repository,
        cache: StateCache,
        universe: list[UniverseSymbol],
        alert_chat_ids: list[int],
        interval_seconds: int,
        signal_dedupe_ttl_seconds: int = 14400,
    ) -> None:
        self.repository = repository
        self.cache = cache
        self.universe = universe
        self.alert_chat_ids = alert_chat_ids
        self.interval_seconds = interval_seconds
        self.signal_dedupe_ttl_seconds = signal_dedupe_ttl_seconds
        self.level_detector = LevelDetector()
        self.rule_engine = RuleEngine()
        self.logger = get_logger("engine")

    async def run(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception as exc:
                self.logger.exception("engine-cycle-failed", error=str(exc))
            await asyncio.sleep(self.interval_seconds)

    async def run_once(self) -> None:
        stale_count = 0
        total = 0
        for item in self.universe:
            total += 1
            if await self._process_symbol(item):
                stale_count += 1
        ratio = (stale_count / total) if total else 0
        STALE_DATA_RATIO.set(ratio)
        SIGNALS_IN_DB.set(await self.repository.signal_count())

    async def _process_symbol(self, symbol_config: UniverseSymbol) -> int:
        primary_venue = symbol_config.primary_venue.value
        symbol = symbol_config.symbol
        candles_4h = parse_model_list(await self.cache.get_candles(primary_venue, symbol, "4h"), MarketCandle)
        candles_1h = parse_model_list(await self.cache.get_candles(primary_venue, symbol, "1h"), MarketCandle)
        candles_15m = parse_model_list(await self.cache.get_candles(primary_venue, symbol, "15m"), MarketCandle)
        candles_5m = parse_model_list(await self.cache.get_candles(primary_venue, symbol, "5m"), MarketCandle)
        health_payload = await self.cache.get_health(primary_venue, symbol)
        book_payload = await self.cache.get_book(primary_venue, symbol)
        derivatives_payload = await self.cache.get_derivative_context(primary_venue, symbol)
        if not candles_4h or not candles_1h or not candles_15m or not health_payload:
            return 1

        health = DataHealth.model_validate(health_payload)
        stale = 0 if health.is_fresh else 1
        levels = self.level_detector.detect(symbol, symbol_config.primary_venue, candles_4h, candles_1h)
        await self.repository.replace_levels(symbol, primary_venue, levels)

        cross_health = []
        for venue in Venue:
            payload = await self.cache.get_health(venue.value, symbol)
            if payload:
                cross_health.append(DataHealth.model_validate(payload))

        decision = self.rule_engine.evaluate(
            EngineInput(
                symbol=symbol,
                venue=symbol_config.primary_venue,
                candles_4h=candles_4h,
                candles_1h=candles_1h,
                candles_15m=candles_15m,
                candles_5m=candles_5m,
                levels=levels,
                book=BookSnapshot.model_validate(book_payload) if book_payload else None,
                derivative_context=DerivativeContext.model_validate(derivatives_payload) if derivatives_payload else None,
                health=health,
                cross_venue_health=cross_health,
            )
        )
        if decision is None:
            return stale

        await self.repository.save_signal(decision)
        ALERTS_TOTAL.labels(signal_class=decision.signal_class.value, setup=decision.setup.value).inc()
        if decision.signal_class != SignalClass.SUPPRESSED and await self.cache.acquire_alert_key(
            decision.alert_key,
            ttl_seconds=self.signal_dedupe_ttl_seconds,
        ):
            for chat_id in self.alert_chat_ids:
                alert = to_alert_message(decision, chat_id)
                await self.repository.ensure_delivery(alert)
                await self.cache.enqueue_alert(alert)
        self.logger.info("signal-evaluated", symbol=symbol, signal_class=decision.signal_class.value, confidence=decision.confidence)
        return stale
