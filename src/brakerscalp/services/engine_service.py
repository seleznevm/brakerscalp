from __future__ import annotations

import asyncio
from datetime import datetime, timezone

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
        alert_message_thread_id: int | None = None,
        signal_duplicate_window_minutes: int = 180,
        minimum_alert_confidence: float = 65.0,
    ) -> None:
        self.repository = repository
        self.cache = cache
        self.universe = universe
        self.alert_chat_ids = alert_chat_ids
        self.interval_seconds = interval_seconds
        self.signal_dedupe_ttl_seconds = signal_dedupe_ttl_seconds
        self.alert_message_thread_id = alert_message_thread_id
        self.signal_duplicate_window_minutes = signal_duplicate_window_minutes
        self.minimum_alert_confidence = minimum_alert_confidence
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
        detected = 0
        runtime_universe = await self._current_universe()
        runtime_minimum_alert_confidence = await self._runtime_minimum_alert_confidence()
        for item in runtime_universe:
            total += 1
            stale, has_signal = await self._process_symbol(item, runtime_minimum_alert_confidence)
            if stale:
                stale_count += 1
            if has_signal:
                detected += 1
        ratio = (stale_count / total) if total else 0
        STALE_DATA_RATIO.set(ratio)
        signal_count = await self.repository.signal_count()
        SIGNALS_IN_DB.set(signal_count)
        if hasattr(self.cache, "store_service_heartbeat"):
            await self.cache.store_service_heartbeat(
                "engine",
                {
                    "symbols": total,
                    "stale_symbols": stale_count,
                    "detected_signals": detected,
                    "signals_in_db": signal_count,
                    "minimum_alert_confidence": runtime_minimum_alert_confidence,
                },
            )

    async def _process_symbol(self, symbol_config: UniverseSymbol, runtime_minimum_alert_confidence: float) -> tuple[int, bool]:
        primary_venue = symbol_config.primary_venue.value
        symbol = symbol_config.symbol
        candles_4h = self._closed_candles(parse_model_list(await self.cache.get_candles(primary_venue, symbol, "4h"), MarketCandle))
        candles_1h = self._closed_candles(parse_model_list(await self.cache.get_candles(primary_venue, symbol, "1h"), MarketCandle))
        candles_15m = self._closed_candles(parse_model_list(await self.cache.get_candles(primary_venue, symbol, "15m"), MarketCandle))
        candles_5m = self._closed_candles(parse_model_list(await self.cache.get_candles(primary_venue, symbol, "5m"), MarketCandle))
        health_payload = await self.cache.get_health(primary_venue, symbol)
        book_payload = await self.cache.get_book(primary_venue, symbol)
        derivatives_payload = await self.cache.get_derivative_context(primary_venue, symbol)
        if not candles_4h or not candles_1h or not candles_15m or not health_payload:
            return 1, False

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
            return stale, False

        duplicate = await self.repository.find_recent_signal_match(
            symbol=decision.symbol,
            venue=decision.venue.value,
            setup=decision.setup.value,
            direction=decision.direction.value,
            level_id=decision.level_id,
            within_minutes=max(self.signal_duplicate_window_minutes, 480),
        )
        if duplicate is not None:
            self.logger.info(
                "signal-duplicate-suppressed",
                symbol=decision.symbol,
                setup=decision.setup.value,
                direction=decision.direction.value,
                level_id=decision.level_id,
                previous_detected_at=duplicate.detected_at.isoformat(),
            )
            return stale, False

        await self.repository.save_signal(decision)
        ALERTS_TOTAL.labels(signal_class=decision.signal_class.value, setup=decision.setup.value).inc()
        if decision.confidence < runtime_minimum_alert_confidence:
            self.logger.info(
                "signal-below-runtime-threshold",
                symbol=decision.symbol,
                confidence=decision.confidence,
                minimum_alert_confidence=runtime_minimum_alert_confidence,
            )
        elif decision.signal_class != SignalClass.SUPPRESSED and await self.cache.acquire_alert_key(
            decision.alert_key,
            ttl_seconds=self.signal_dedupe_ttl_seconds,
        ):
            for chat_id in self.alert_chat_ids:
                alert = to_alert_message(decision, chat_id, message_thread_id=self.alert_message_thread_id)
                await self.repository.ensure_delivery(alert)
                await self.cache.enqueue_alert(alert)
        self.logger.info("signal-evaluated", symbol=symbol, signal_class=decision.signal_class.value, confidence=decision.confidence)
        return stale, True

    async def _runtime_minimum_alert_confidence(self) -> float:
        if hasattr(self.cache, "get_minimum_alert_confidence"):
            return await self.cache.get_minimum_alert_confidence(self.minimum_alert_confidence)
        return self.minimum_alert_confidence

    async def _current_universe(self) -> list[UniverseSymbol]:
        allowed_venues = {item.primary_venue for item in self.universe}
        if hasattr(self.cache, "get_universe_symbols"):
            runtime_universe = await self.cache.get_universe_symbols(self.universe)
            if runtime_universe:
                return [item for item in runtime_universe if item.primary_venue in allowed_venues]
        return [item for item in self.universe if item.primary_venue in allowed_venues]

    def _closed_candles(self, candles: list[MarketCandle]) -> list[MarketCandle]:
        now = datetime.now(tz=timezone.utc)
        return [item for item in candles if item.close_time <= now]
