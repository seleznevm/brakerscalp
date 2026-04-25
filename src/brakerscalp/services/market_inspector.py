from __future__ import annotations

from collections import defaultdict
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
class TradeSimulation:
    outcome: str
    entry_at: datetime | None
    tp1_at: datetime | None
    tp2_at: datetime | None
    sl_at: datetime | None
    exit_at: datetime | None
    exit_reason: str | None
    pnl_pct: float | None
    duration_seconds: int | None


@dataclass(slots=True)
class VenueProbe:
    symbol: str
    venue: str
    available: bool
    message: str


@dataclass(slots=True)
class StatisticsRow:
    symbol: str
    total: int
    success: int
    failed: int
    pending: int
    actionable: int
    watchlist: int
    avg_confidence: float
    win_rate: float


@dataclass(slots=True)
class StatisticsSnapshot:
    start_at: datetime
    end_at: datetime
    total: int
    success: int
    failed: int
    pending: int
    actionable: int
    watchlist: int
    avg_confidence: float
    win_rate: float
    rows: list[StatisticsRow]


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

    async def list_universe(self) -> list[UniverseSymbol]:
        allowed_venues = set(self.adapters)
        if hasattr(self.cache, "get_universe_symbols"):
            runtime_universe = await self.cache.get_universe_symbols(self.universe)
            if runtime_universe:
                if not allowed_venues:
                    return runtime_universe
                return [item for item in runtime_universe if item.primary_venue in allowed_venues]
        if not allowed_venues:
            return list(self.universe)
        return [item for item in self.universe if item.primary_venue in allowed_venues]

    async def screen_universe(self, scope: str = "active") -> list[ScreeningResult]:
        results: list[ScreeningResult] = []
        for item in await self.list_universe():
            payload = await self._load_cached_payload(item)
            if payload is None:
                continue
            report = self.rule_engine.inspect(payload)
            if scope == "active" and report.status not in self.ACTIVE_STATUSES:
                continue
            results.append(report)
        if scope == "active" and not results:
            for item in await self.list_universe():
                payload = await self._load_cached_payload(item)
                if payload is None:
                    continue
                results.append(self.rule_engine.inspect(payload))
        return sorted(results, key=self._sort_screening_result)

    async def manual_scan(self, raw_symbol: str) -> ManualScanResult:
        symbol = self.normalize_symbol(raw_symbol)
        if not symbol:
            return ManualScanResult(source="none", report=None, errors=["Символ не указан."])

        universe_item = {item.symbol.upper(): item for item in await self.list_universe()}.get(symbol)
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

    async def list_active_setups(
        self,
        within_hours: int = 72,
        limit: int = 24,
        outcome_filter: str = "all",
        symbol_query: str | None = None,
        minimum_confidence: float | None = None,
    ) -> list[SetupCard]:
        end_at = datetime.now(tz=timezone.utc)
        start_at = end_at - timedelta(hours=within_hours)
        signals = await self.repository.list_signals_between(start_at, end_at, signal_classes=["actionable", "watchlist"])
        cards: list[SetupCard] = []
        query = (symbol_query or "").strip().upper()
        for signal in reversed(signals):
            if query and query not in signal.symbol.upper():
                continue
            if minimum_confidence is not None and signal.confidence < minimum_confidence:
                continue
            candles = await self.repository.get_candles_between(signal.venue, signal.symbol, signal.timeframe, signal.detected_at, end_at)
            outcome = classify_signal_outcome(signal, candles)
            if outcome_filter != "all" and outcome != outcome_filter:
                continue
            cards.append(SetupCard(signal=signal, outcome=outcome))
        outcome_order = {"pending": 0, "success": 1, "failed": 2}
        cards = sorted(cards, key=lambda item: (outcome_order.get(item.outcome, 9), -item.signal.detected_at.timestamp()))
        return cards[:limit]

    async def build_statistics(
        self,
        *,
        start_at: datetime,
        end_at: datetime,
        symbol_query: str | None = None,
    ) -> StatisticsSnapshot:
        signals = await self.repository.list_signals_between(start_at, end_at, signal_classes=["actionable", "watchlist"])
        query = (symbol_query or "").strip().upper()
        by_symbol: dict[str, dict[str, float]] = defaultdict(
            lambda: {
                "total": 0,
                "success": 0,
                "failed": 0,
                "pending": 0,
                "actionable": 0,
                "watchlist": 0,
                "confidence_sum": 0.0,
            }
        )

        for signal in signals:
            if query and query not in signal.symbol.upper():
                continue
            candles = await self.repository.get_candles_between(signal.venue, signal.symbol, signal.timeframe, signal.detected_at, end_at)
            outcome = classify_signal_outcome(signal, candles)
            bucket = by_symbol[signal.symbol]
            bucket["total"] += 1
            bucket[outcome] += 1
            bucket[signal.signal_class] += 1
            bucket["confidence_sum"] += float(signal.confidence)

        rows_payload: list[dict[str, float | int | str]] = []
        for symbol, values in by_symbol.items():
            resolved = int(values["success"] + values["failed"])
            win_rate = (float(values["success"]) / resolved * 100.0) if resolved else 0.0
            avg_confidence = (float(values["confidence_sum"]) / float(values["total"])) if values["total"] else 0.0
            rows_payload.append(
                {
                    "symbol": symbol,
                    "total": int(values["total"]),
                    "success": int(values["success"]),
                    "failed": int(values["failed"]),
                    "pending": int(values["pending"]),
                    "actionable": int(values["actionable"]),
                    "watchlist": int(values["watchlist"]),
                    "avg_confidence": avg_confidence,
                    "win_rate": win_rate,
                }
            )
        await self.repository.replace_statistics_snapshot(
            start_at=start_at,
            end_at=end_at,
            symbol_query=query,
            rows=rows_payload,
        )
        persisted_rows = await self.repository.list_statistics_snapshot(
            start_at=start_at,
            end_at=end_at,
            symbol_query=query,
        )
        rows = [
            StatisticsRow(
                symbol=item.symbol,
                total=item.total,
                success=item.success,
                failed=item.failed,
                pending=item.pending,
                actionable=item.actionable,
                watchlist=item.watchlist,
                avg_confidence=float(item.avg_confidence),
                win_rate=float(item.win_rate),
            )
            for item in persisted_rows
        ]
        rows.sort(key=lambda item: (-item.total, -item.win_rate, item.symbol))

        total = sum(item.total for item in rows)
        success = sum(item.success for item in rows)
        failed = sum(item.failed for item in rows)
        pending = sum(item.pending for item in rows)
        actionable = sum(item.actionable for item in rows)
        watchlist = sum(item.watchlist for item in rows)
        resolved = success + failed
        overall_win_rate = (success / resolved * 100.0) if resolved else 0.0
        overall_avg_confidence = (
            sum(item.avg_confidence * item.total for item in rows) / total
            if total
            else 0.0
        )
        return StatisticsSnapshot(
            start_at=start_at,
            end_at=end_at,
            total=total,
            success=success,
            failed=failed,
            pending=pending,
            actionable=actionable,
            watchlist=watchlist,
            avg_confidence=overall_avg_confidence,
            win_rate=overall_win_rate,
            rows=rows,
        )

    async def simulate_trade(self, signal: SignalRecord, end_at: datetime | None = None) -> TradeSimulation:
        analysis_end = end_at or datetime.now(tz=timezone.utc)
        before = await self.repository.get_candles_before(signal.venue, signal.symbol, signal.timeframe, signal.detected_at, limit=2)
        after = await self.repository.get_candles_between(signal.venue, signal.symbol, signal.timeframe, signal.detected_at, analysis_end)
        candles = sorted({item.close_time: item for item in [*before, *after]}.values(), key=lambda item: item.close_time)
        return self._simulate_trade(signal, candles)

    async def discover_symbol_venues(self, raw_symbol: str) -> tuple[str, list[VenueProbe]]:
        symbol = self.normalize_symbol(raw_symbol)
        if not symbol:
            return "", []
        probes: list[VenueProbe] = []
        for venue, adapter in self.adapters.items():
            try:
                candles = await adapter.fetch_recent_candles(symbol, Timeframe.M15, 5)
                if candles:
                    probes.append(VenueProbe(symbol=symbol, venue=venue.value, available=True, message="Available"))
                else:
                    probes.append(VenueProbe(symbol=symbol, venue=venue.value, available=False, message="No candles returned"))
            except Exception as exc:
                probes.append(VenueProbe(symbol=symbol, venue=venue.value, available=False, message=f"{type(exc).__name__}: {exc}"))
        return symbol, probes

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

    def _simulate_trade(self, signal: SignalRecord, candles: list[CandleRecord]) -> TradeSimulation:
        timeframe_delta = self._timeframe_delta(signal.timeframe)
        entry_threshold = signal.detected_at - timeframe_delta
        entry_index: int | None = None
        entry_at: datetime | None = None
        for index, candle in enumerate(candles):
            if candle.close_time < entry_threshold:
                continue
            if signal.direction == "long" and candle.close >= signal.entry_price:
                entry_index = index
                entry_at = candle.close_time
                break
            if signal.direction == "short" and candle.close <= signal.entry_price:
                entry_index = index
                entry_at = candle.close_time
                break

        if entry_index is None or entry_at is None:
            return TradeSimulation(
                outcome="not_entered",
                entry_at=None,
                tp1_at=None,
                tp2_at=None,
                sl_at=None,
                exit_at=None,
                exit_reason=None,
                pnl_pct=None,
                duration_seconds=None,
            )

        tp1 = float(signal.targets[0]) if signal.targets else float(signal.entry_price)
        tp2 = float(signal.targets[1]) if len(signal.targets) > 1 else tp1
        tp1_at: datetime | None = None
        tp2_at: datetime | None = None
        sl_at: datetime | None = None

        for candle in candles[entry_index + 1 :]:
            if signal.direction == "long":
                if tp1_at is None and candle.high >= tp1:
                    tp1_at = candle.close_time
                if tp2_at is None and candle.high >= tp2:
                    tp2_at = candle.close_time
                if sl_at is None and candle.low <= signal.invalidation_price:
                    sl_at = candle.close_time
            else:
                if tp1_at is None and candle.low <= tp1:
                    tp1_at = candle.close_time
                if tp2_at is None and candle.low <= tp2:
                    tp2_at = candle.close_time
                if sl_at is None and candle.high >= signal.invalidation_price:
                    sl_at = candle.close_time

        exit_reason: str | None = None
        exit_at: datetime | None = None
        exit_price: float | None = None
        outcome = "pending"
        if tp2_at is not None and (sl_at is None or tp2_at < sl_at):
            exit_reason = "TP2"
            exit_at = tp2_at
            exit_price = tp2
            outcome = "success"
        elif tp1_at is not None and (sl_at is None or tp1_at < sl_at):
            exit_reason = "TP1"
            exit_at = tp1_at
            exit_price = tp1
            outcome = "success"
        elif sl_at is not None:
            exit_reason = "SL"
            exit_at = sl_at
            exit_price = float(signal.invalidation_price)
            outcome = "failed"

        pnl_pct = self._pnl_percent(signal.direction, float(signal.entry_price), exit_price) if exit_price is not None else None
        duration_seconds = int((exit_at - entry_at).total_seconds()) if exit_at is not None else None
        return TradeSimulation(
            outcome=outcome,
            entry_at=entry_at,
            tp1_at=tp1_at,
            tp2_at=tp2_at,
            sl_at=sl_at,
            exit_at=exit_at,
            exit_reason=exit_reason,
            pnl_pct=pnl_pct,
            duration_seconds=duration_seconds,
        )

    async def _manual_chart_candles(self, symbol: str) -> list[MarketCandle] | list[CandleRecord]:
        universe_item = {item.symbol.upper(): item for item in await self.list_universe()}.get(symbol)
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
                "chart_timeframe": Timeframe.M15.value,
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

    def _timeframe_delta(self, timeframe: str) -> timedelta:
        mapping = {
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "1h": timedelta(hours=1),
            "4h": timedelta(hours=4),
        }
        return mapping.get(timeframe, timedelta(minutes=15))

    def _pnl_percent(self, direction: str, entry_price: float, exit_price: float | None) -> float | None:
        if exit_price is None or not entry_price:
            return None
        if direction == "short":
            return ((entry_price - exit_price) / entry_price) * 100.0
        return ((exit_price - entry_price) / entry_price) * 100.0
