from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from brakerscalp.domain.models import AlertMessage, BookSnapshot, MarketCandle, OrderFlowSnapshot, SignalClass, TradeTick, UniverseSymbol, Venue
from brakerscalp.logging import get_logger
from brakerscalp.services.daily_summary import SETUP_STATUS_EXECUTED, evaluate_setup_lifecycle
from brakerscalp.signals.engine import StrategyRuntimeConfig
from brakerscalp.signals.orderflow import compute_order_flow_snapshot
from brakerscalp.storage.cache import StateCache
from brakerscalp.storage.models import SignalRecord
from brakerscalp.storage.repository import Repository


class OrderFlowAnalyzerService:
    def __init__(
        self,
        repository: Repository,
        cache: StateCache,
        universe: list[UniverseSymbol],
        alert_chat_ids: list[int],
        interval_seconds: int,
        alert_message_thread_id: int | None = None,
        strategy_defaults: dict[str, object] | None = None,
    ) -> None:
        self.repository = repository
        self.cache = cache
        self.universe = universe
        self.alert_chat_ids = alert_chat_ids
        self.interval_seconds = interval_seconds
        self.alert_message_thread_id = alert_message_thread_id
        self.strategy_defaults = strategy_defaults or StrategyRuntimeConfig().model_dump(mode="json")
        self.logger = get_logger("orderflow")

    async def run(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception as exc:
                self.logger.exception("orderflow-cycle-failed", error=str(exc))
            await asyncio.sleep(self.interval_seconds)

    async def run_once(self) -> None:
        strategy = await self._runtime_strategy_config()
        runtime_universe = await self._current_universe()
        snapshots = 0
        velocity_alerts = 0
        for item in runtime_universe:
            created = await self._process_symbol(item, strategy)
            snapshots += 1 if created else 0
            velocity_alerts += created
        management_alerts = await self._process_active_signals(strategy)
        if hasattr(self.cache, "store_service_heartbeat"):
            await self.cache.store_service_heartbeat(
                "orderflow",
                {
                    "symbols": len(runtime_universe),
                    "snapshots": snapshots,
                    "velocity_alerts": velocity_alerts,
                    "management_alerts": management_alerts,
                    "interval_seconds": self.interval_seconds,
                },
            )

    async def _process_symbol(self, item: UniverseSymbol, strategy: StrategyRuntimeConfig) -> int:
        venue = item.primary_venue.value
        history = self._parse_model_list(await self.cache.get_trade_history(venue, item.symbol), TradeTick)
        snapshot = compute_order_flow_snapshot(item.symbol, item.primary_venue, history)
        await self.cache.store_order_flow_snapshot(venue, item.symbol, snapshot)
        if not strategy.enable_tick_velocity_alerts:
            return 0
        if snapshot.tick_velocity_ratio < strategy.tick_velocity_alert_multiplier:
            return 0
        minute_bucket = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M")
        dedupe_key = f"{venue}:{item.symbol}:{minute_bucket}"
        if not await self.cache.acquire_once_key("tick-velocity-alert", dedupe_key, ttl_seconds=max(self.interval_seconds * 3, 120)):
            return 0
        current_price = await self._current_price(item)
        text = (
            f"⚡ {item.symbol} | TICK VELOCITY\n"
            f"#ORDERFLOW #{self._coin_hashtag(item.symbol)}\n"
            f"Velocity: {snapshot.tick_velocity_ratio:.2f}x of baseline\n"
            f"Recent trades: {snapshot.recent_trade_count} / 30s\n"
            f"Delta ratio: {snapshot.delta_ratio:.2f}\n"
            f"CVD slope: {snapshot.cvd_slope:.2f}\n"
            f"Price: {current_price:.4f}"
        )
        await self._queue_alert(
            signal_id=f"velocity:{venue}:{item.symbol}:{minute_bucket}",
            alert_key=f"velocity:{venue}:{item.symbol}:{minute_bucket}",
            text=text,
            signal_class=SignalClass.WATCHLIST,
        )
        return 1

    async def _process_active_signals(self, strategy: StrategyRuntimeConfig) -> int:
        if not strategy.enable_time_stop_alerts and not strategy.enable_dynamic_breakeven_alerts:
            return 0
        now = datetime.now(tz=timezone.utc)
        signals = await self.repository.list_signals_between(
            now - timedelta(days=1),
            now + timedelta(minutes=1),
            signal_classes=["actionable", "watchlist"],
        )
        count = 0
        for signal in signals:
            if str((signal.render_context or {}).get("setup_stage", "")) != "activated":
                continue
            if await self._maybe_send_breakeven(signal, strategy, now):
                count += 1
            if await self._maybe_send_time_stop(signal, strategy, now):
                count += 1
        return count

    async def _maybe_send_breakeven(self, signal: SignalRecord, strategy: StrategyRuntimeConfig, now: datetime) -> bool:
        if not strategy.enable_dynamic_breakeven_alerts:
            return False
        lifecycle = await self._signal_lifecycle(signal, now)
        if lifecycle.status != SETUP_STATUS_EXECUTED:
            return False
        current_price = await self._signal_current_price(signal)
        gain_pct = self._pnl_pct(signal.direction, float(signal.entry_price), current_price)
        if gain_pct < strategy.breakeven_trigger_pct:
            return False
        dedupe_key = f"{signal.decision_id}:breakeven"
        if not await self.cache.acquire_once_key("management-alert", dedupe_key, ttl_seconds=172800):
            return False
        text = (
            f"🛡️ {signal.symbol} | {signal.setup.upper()} | {signal.direction.upper()} | {signal.timeframe}\n"
            f"#{signal.setup.upper()} #{self._coin_hashtag(signal.symbol)}\n"
            f"Dynamic breakeven trigger hit.\n"
            f"Move SL to entry: {float(signal.entry_price):.4f}\n"
            f"Current price: {current_price:.4f}\n"
            f"Open PnL: {gain_pct:.2f}%\n"
            f"ACTIVATED"
        )
        await self._queue_alert(
            signal_id=f"{signal.decision_id}#breakeven",
            alert_key=f"{signal.alert_key}:breakeven",
            text=text,
            signal_class=SignalClass.ACTIONABLE,
        )
        return True

    async def _maybe_send_time_stop(self, signal: SignalRecord, strategy: StrategyRuntimeConfig, now: datetime) -> bool:
        if not strategy.enable_time_stop_alerts:
            return False
        if now < signal.detected_at + timedelta(minutes=strategy.time_stop_minutes):
            return False
        lifecycle = await self._signal_lifecycle(signal, now)
        if lifecycle.status != SETUP_STATUS_EXECUTED:
            return False
        max_move_pct = await self._max_favorable_move_pct(signal)
        if strategy.enable_dynamic_breakeven_alerts and max_move_pct >= strategy.breakeven_trigger_pct:
            return False
        if max_move_pct >= strategy.time_stop_min_move_pct:
            return False
        dedupe_key = f"{signal.decision_id}:time-stop"
        if not await self.cache.acquire_once_key("management-alert", dedupe_key, ttl_seconds=172800):
            return False
        current_price = await self._signal_current_price(signal)
        text = (
            f"⏱️ {signal.symbol} | {signal.setup.upper()} | {signal.direction.upper()} | {signal.timeframe}\n"
            f"#{signal.setup.upper()} #{self._coin_hashtag(signal.symbol)}\n"
            f"Time-stop triggered: close by market.\n"
            f"No {strategy.time_stop_min_move_pct:.2f}% impulse within {strategy.time_stop_minutes} minutes.\n"
            f"Current price: {current_price:.4f}\n"
            f"Best move seen: {max_move_pct:.2f}%\n"
            f"ACTIVATED"
        )
        await self._queue_alert(
            signal_id=f"{signal.decision_id}#time-stop",
            alert_key=f"{signal.alert_key}:time-stop",
            text=text,
            signal_class=SignalClass.ACTIONABLE,
        )
        return True

    async def _signal_lifecycle(self, signal: SignalRecord, now: datetime):
        candles = await self.repository.get_candles_between(
            signal.venue,
            signal.symbol,
            signal.timeframe,
            signal.detected_at - timedelta(minutes=5),
            now + timedelta(minutes=5),
        )
        return evaluate_setup_lifecycle(signal, candles, analysis_end=now)

    async def _signal_current_price(self, signal: SignalRecord) -> float:
        item = UniverseSymbol(symbol=signal.symbol, primary_venue=self._venue_from_value(signal.venue))
        return await self._current_price(item)

    async def _current_price(self, item: UniverseSymbol) -> float:
        venue = item.primary_venue.value
        book_payload = await self.cache.get_book(venue, item.symbol)
        if book_payload:
            book = BookSnapshot.model_validate(book_payload)
            if book.best_bid and book.best_ask:
                return (book.best_bid + book.best_ask) / 2.0
            if book.best_bid:
                return book.best_bid
            if book.best_ask:
                return book.best_ask
        trades = self._parse_model_list(await self.cache.get_trade_history(venue, item.symbol), TradeTick)
        if trades:
            return trades[-1].price
        candles = self._parse_model_list(await self.cache.get_candles(venue, item.symbol, "5m"), MarketCandle)
        if candles:
            return candles[-1].close
        return 0.0

    async def _max_favorable_move_pct(self, signal: SignalRecord) -> float:
        item = UniverseSymbol(symbol=signal.symbol, primary_venue=self._venue_from_value(signal.venue))
        trades = self._parse_model_list(await self.cache.get_trade_history(item.primary_venue.value, signal.symbol), TradeTick)
        relevant = [trade.price for trade in trades if trade.timestamp >= signal.detected_at]
        if not relevant:
            current_price = await self._signal_current_price(signal)
            relevant = [current_price] if current_price else []
        if not relevant:
            return 0.0
        entry_price = float(signal.entry_price)
        if signal.direction == "short":
            best_price = min(relevant)
            return max(((entry_price - best_price) / max(entry_price, 1e-9)) * 100.0, 0.0)
        best_price = max(relevant)
        return max(((best_price - entry_price) / max(entry_price, 1e-9)) * 100.0, 0.0)

    async def _queue_alert(self, *, signal_id: str, alert_key: str, text: str, signal_class: SignalClass) -> None:
        for chat_id in self.alert_chat_ids:
            alert = AlertMessage(
                signal_id=signal_id,
                alert_key=alert_key,
                chat_id=chat_id,
                message_thread_id=self.alert_message_thread_id,
                text=text,
                signal_class=signal_class,
            )
            await self.repository.ensure_delivery(alert)
            await self.cache.enqueue_alert(alert)

    async def _runtime_strategy_config(self) -> StrategyRuntimeConfig:
        if hasattr(self.cache, "get_strategy_config"):
            return StrategyRuntimeConfig.model_validate(
                await self.cache.get_strategy_config(default=self.strategy_defaults)
            )
        return StrategyRuntimeConfig.model_validate(self.strategy_defaults)

    async def _current_universe(self) -> list[UniverseSymbol]:
        allowed_venues = {item.primary_venue for item in self.universe}
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

    def _parse_model_list(self, raw: list[dict], model):
        return [model.model_validate(item) for item in raw]

    def _coin_hashtag(self, symbol: str) -> str:
        if symbol.endswith("USDT"):
            return symbol[:-4].upper()
        return symbol.upper()

    def _pnl_pct(self, direction: str, entry_price: float, current_price: float) -> float:
        if direction == "short":
            return ((entry_price - current_price) / max(entry_price, 1e-9)) * 100.0
        return ((current_price - entry_price) / max(entry_price, 1e-9)) * 100.0

    def _venue_from_value(self, value: str):
        try:
            return Venue(value)
        except ValueError:
            return Venue.BINANCE
