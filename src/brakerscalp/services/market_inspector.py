from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from brakerscalp.config import Settings
from brakerscalp.domain.models import BookSnapshot, DataHealth, DerivativeContext, Direction, MarketCandle, Timeframe, UniverseSymbol, Venue
from brakerscalp.exchanges.base import ExchangeAdapter
from brakerscalp.services.daily_summary import classify_signal_outcome
from brakerscalp.signals.charting import render_signal_chart
from brakerscalp.signals.engine import EngineInput, RuleEngine, ScreeningResult
from brakerscalp.signals.levels import LevelDetector
from brakerscalp.storage.cache import StateCache
from brakerscalp.storage.models import CandleRecord, SignalRecord
from brakerscalp.storage.repository import Repository


@dataclass(slots=True)
class ManualScanResult:
    source: str
    report: ScreeningResult | None
    errors: list[str]


@dataclass(slots=True)
class SetupCard:
    signal: SignalRecord
    outcome: str


@dataclass(slots=True)
class ChartSignalSnapshot:
    symbol: str
    entry_price: float
    invalidation_price: float
    targets: list[float]
    render_context: dict


class MarketInspector:
    ACTIVE_STATUSES = {"actionable", "watchlist", "arming", "monitor"}

    def __init__(
        self,
        repository: Repository,
        cache: StateCache,
        settings: Settings,
        universe: list[UniverseSymbol],
        adapters: dict[Venue, ExchangeAdapter],
    ) -> None:
        self.repository = repository
        self.cache = cache
        self.settings = settings
        self.universe = universe
        self.adapters = adapters
        self.level_detector = LevelDetector()
        self.rule_engine = RuleEngine()
        self._universe_by_symbol = {item.symbol.upper(): item for item in universe}

    def normalize_symbol(self, raw_symbol: str) -> str:
        symbol = raw_symbol.strip().upper()
        if not symbol:
            return ""
        if symbol.endswith("-USDT-SWAP"):
            return symbol.replace("-USDT-SWAP", "USDT")
        if symbol.endswith("-USDT"):
            return symbol.replace("-", "")
        if symbol.endswith("USDT"):
            return symbol
        return f"{symbol}USDT"

    async def screen_universe(self, scope: str = "active") -> list[ScreeningResult]:
        results: list[ScreeningResult] = []
        for item in self.universe:
            payload = await self._load_cached_payload(item)
            if payload is None:
                continue
            report = self.rule_engine.inspect(payload)
            if scope == "active" and report.status not in self.ACTIVE_STATUSES:
                continue
            results.append(report)
        if scope == "active" and not results:
            for item in self.universe:
                payload = await self._load_cached_payload(item)
                if payload is None:
                    continue
                results.append(self.rule_engine.inspect(payload))
        return sorted(results, key=self._sort_screening_result)

    async def manual_scan(self, raw_symbol: str) -> ManualScanResult:
        symbol = self.normalize_symbol(raw_symbol)
        if not symbol:
            return ManualScanResult(source="none", report=None, errors=["Символ не указан."])

        universe_item = self._universe_by_symbol.get(symbol)
        if universe_item is not None:
            payload = await self._load_cached_payload(universe_item)
            if payload is not None:
                return ManualScanResult(source=f"cache:{universe_item.primary_venue.value}", report=self.rule_engine.inspect(payload), errors=[])

        errors: list[str] = []
        for venue, adapter in self.adapters.items():
            try:
                payload = await self._load_live_payload(symbol, venue, adapter)
            except Exception as exc:
                errors.append(f"{venue.value}: {type(exc).__name__}: {exc}")
                continue
            return ManualScanResult(source=f"live:{venue.value}", report=self.rule_engine.inspect(payload), errors=errors)
        return ManualScanResult(source="unavailable", report=None, errors=errors or ["Не удалось загрузить данные по символу."])

    async def list_active_setups(self, within_hours: int = 72, limit: int = 24) -> list[SetupCard]:
        end_at = datetime.now(tz=timezone.utc)
        start_at = end_at - timedelta(hours=within_hours)
        signals = await self.repository.list_signals_between(start_at, end_at, signal_classes=["actionable", "watchlist"])
        cards: list[SetupCard] = []
        for signal in reversed(signals[-limit:]):
            candles = await self.repository.get_candles_between(signal.venue, signal.symbol, signal.timeframe, signal.detected_at, end_at)
            cards.append(SetupCard(signal=signal, outcome=classify_signal_outcome(signal, candles)))
        outcome_order = {"pending": 0, "success": 1, "failed": 2}
        return sorted(cards, key=lambda item: (outcome_order.get(item.outcome, 9), -item.signal.detected_at.timestamp()))

    async def render_manual_chart(self, raw_symbol: str) -> bytes | None:
        scan = await self.manual_scan(raw_symbol)
        if scan.report is None or scan.report.level_lower is None or scan.report.level_upper is None or scan.report.direction is None:
            return None
        candles = await self._manual_chart_candles(scan.report.symbol)
        if len(candles) < 5:
            return None
        signal = self._chart_signal_for_report(scan.report)
        return render_signal_chart(candles, signal)

    async def render_signal_chart(self, decision_id: str) -> bytes | None:
        signal = await self.repository.get_signal_by_decision_id(decision_id)
        if signal is None:
            return None
        candles = await self.repository.get_candles_before(signal.venue, signal.symbol, signal.timeframe, signal.detected_at, limit=64)
        return render_signal_chart(candles, signal)

    async def _manual_chart_candles(self, symbol: str) -> list[MarketCandle] | list[CandleRecord]:
        universe_item = self._universe_by_symbol.get(symbol)
        if universe_item is not None:
            payload = await self._load_cached_payload(universe_item)
            if payload is not None:
                return payload.candles_15m[-64:]
        scan = await self.manual_scan(symbol)
        if scan.report is None:
            return []
        for venue, adapter in self.adapters.items():
            try:
                candles = await adapter.fetch_recent_candles(symbol, Timeframe.M15, 120)
                return candles[-64:]
            except Exception:
                continue
        return []

    def _chart_signal_for_report(self, report: ScreeningResult) -> ChartSignalSnapshot:
        direction = report.direction or Direction.LONG
        if direction == Direction.LONG:
            invalidation = (report.level_lower or report.last_price) - report.atr_15m * 0.15
            risk = max(report.last_price - invalidation, report.atr_15m * 0.22)
            targets = [report.last_price + risk * 1.2, report.last_price + risk * 2.0]
        else:
            invalidation = (report.level_upper or report.last_price) + report.atr_15m * 0.15
            risk = max(invalidation - report.last_price, report.atr_15m * 0.22)
            targets = [report.last_price - risk * 1.2, report.last_price - risk * 2.0]
        return ChartSignalSnapshot(
            symbol=report.symbol,
            entry_price=report.last_price,
            invalidation_price=invalidation,
            targets=targets,
            render_context={
                "level_lower": report.level_lower,
                "level_upper": report.level_upper,
            },
        )

    async def _load_cached_payload(self, symbol_config: UniverseSymbol) -> EngineInput | None:
        venue = symbol_config.primary_venue.value
        symbol = symbol_config.symbol
        candles_4h = self._closed_candles(self._parse_model_list(await self.cache.get_candles(venue, symbol, "4h"), MarketCandle))
        candles_1h = self._closed_candles(self._parse_model_list(await self.cache.get_candles(venue, symbol, "1h"), MarketCandle))
        candles_15m = self._closed_candles(self._parse_model_list(await self.cache.get_candles(venue, symbol, "15m"), MarketCandle))
        candles_5m = self._closed_candles(self._parse_model_list(await self.cache.get_candles(venue, symbol, "5m"), MarketCandle))
        health_payload = await self.cache.get_health(venue, symbol)
        if not candles_4h or not candles_1h or not candles_15m or not health_payload:
            return None

        levels = self.level_detector.detect(symbol, symbol_config.primary_venue, candles_4h, candles_1h)
        cross_health = []
        for other_venue in Venue:
            payload = await self.cache.get_health(other_venue.value, symbol)
            if payload:
                cross_health.append(DataHealth.model_validate(payload))
        book_payload = await self.cache.get_book(venue, symbol)
        derivatives_payload = await self.cache.get_derivative_context(venue, symbol)
        return EngineInput(
            symbol=symbol,
            venue=symbol_config.primary_venue,
            candles_4h=candles_4h,
            candles_1h=candles_1h,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            levels=levels,
            book=BookSnapshot.model_validate(book_payload) if book_payload else None,
            derivative_context=DerivativeContext.model_validate(derivatives_payload) if derivatives_payload else None,
            health=DataHealth.model_validate(health_payload),
            cross_venue_health=cross_health,
        )

    async def _load_live_payload(self, symbol: str, venue: Venue, adapter: ExchangeAdapter) -> EngineInput:
        candles_4h = self._closed_candles(await adapter.fetch_recent_candles(symbol, Timeframe.H4, 120))
        candles_1h = self._closed_candles(await adapter.fetch_recent_candles(symbol, Timeframe.H1, 220))
        candles_15m = self._closed_candles(await adapter.fetch_recent_candles(symbol, Timeframe.M15, 140))
        candles_5m = self._closed_candles(await adapter.fetch_recent_candles(symbol, Timeframe.M5, 140))
        levels = self.level_detector.detect(symbol, venue, candles_4h, candles_1h)
        book = await adapter.fetch_top_book(symbol, depth=self.settings.exchange_book_depth)
        derivatives = await adapter.fetch_derivative_context(symbol)
        health = await adapter.healthcheck(symbol)
        return EngineInput(
            symbol=symbol,
            venue=venue,
            candles_4h=candles_4h,
            candles_1h=candles_1h,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            levels=levels,
            book=book,
            derivative_context=derivatives,
            health=health,
            cross_venue_health=[],
        )

    def _closed_candles(self, candles: list[MarketCandle]) -> list[MarketCandle]:
        now = datetime.now(tz=timezone.utc)
        return [item for item in candles if item.close_time <= now]

    def _parse_model_list(self, raw: list[dict], model):
        return [model.model_validate(item) for item in raw]

    def _sort_screening_result(self, item: ScreeningResult) -> tuple[int, float, int]:
        order = {
            "actionable": 0,
            "watchlist": 1,
            "arming": 2,
            "monitor": 3,
            "cold": 4,
            "stale": 5,
            "insufficient": 6,
        }
        return (order.get(item.status, 9), -item.confidence, -int(item.updated_at.timestamp()))
