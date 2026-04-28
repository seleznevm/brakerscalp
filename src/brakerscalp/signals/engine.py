from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from pydantic import BaseModel

from brakerscalp.domain.models import (
    BookSnapshot,
    DataHealth,
    DerivativeContext,
    Direction,
    LevelCandidate,
    LevelKind,
    MarketCandle,
    OrderFlowSnapshot,
    ScoreContribution,
    SetupType,
    SignalClass,
    SignalDecision,
    TradeTick,
    Timeframe,
    Venue,
)
from brakerscalp.signals.indicators import average_true_range, median_spread, simple_moving_average, volume_zscore


class StrategyRuntimeConfig(BaseModel):
    timeframe: Timeframe = Timeframe.M5
    minimum_expected_rr: float = 2.0
    actionable_confidence_threshold: float = 88.0
    watchlist_confidence_threshold: float = 82.0
    pre_alert_confidence_threshold: float = 75.0
    volume_z_threshold: float = 1.80
    watchlist_volume_z_threshold: float = 1.05
    pre_alert_volume_z_threshold: float = 0.0
    min_touches: int = 3
    squeeze_threshold: float = 0.72
    pre_alert_squeeze_threshold: float = 0.60
    dist_to_level_atr: float = 0.35
    pre_alert_distance_atr_min: float = 0.2
    pre_alert_distance_atr_max: float = 1.5
    breakout_distance_atr: float = 0.18
    body_ratio_threshold: float = 0.58
    close_to_extreme_threshold: float = 0.22
    range_expansion_threshold: float = 1.25
    sl_multiplier: float = 0.22
    delta_ratio_threshold: float = 0.12
    watchlist_delta_ratio_threshold: float = 0.04
    cvd_slope_threshold: float = 0.06
    delta_divergence_threshold: float = 0.08
    enable_btc_eth_correlation_filter: bool = True
    btc_correlation_threshold: float = 0.45
    enable_liquidation_levels: bool = True
    enable_round_number_levels: bool = True
    enable_tick_velocity_alerts: bool = True
    tick_velocity_alert_multiplier: float = 1.8
    enable_time_stop_alerts: bool = True
    time_stop_minutes: int = 3
    time_stop_min_move_pct: float = 1.0
    enable_dynamic_breakeven_alerts: bool = True
    breakeven_trigger_pct: float = 0.5


@dataclass(slots=True)
class EngineInput:
    symbol: str
    venue: Venue
    candles_4h: list[MarketCandle]
    candles_1h: list[MarketCandle]
    candles_15m: list[MarketCandle]
    candles_5m: list[MarketCandle]
    levels: list[LevelCandidate]
    book: BookSnapshot | None
    derivative_context: DerivativeContext | None
    health: DataHealth
    cross_venue_health: list[DataHealth]
    trades: list[TradeTick] = field(default_factory=list)
    order_flow: OrderFlowSnapshot | None = None
    benchmark_candles_5m: dict[str, list[MarketCandle]] = field(default_factory=dict)


@dataclass(slots=True)
class TrendState:
    bias: Direction | None
    score: float
    fast_1h: float
    slow_1h: float
    fast_4h: float
    slow_4h: float
    bullish_votes: int
    bearish_votes: int


@dataclass(slots=True)
class CoinState:
    is_active: bool
    score: float
    volume_z_15m: float
    volume_z_1h: float
    range_expansion: float
    quote_activity_ratio: float


@dataclass(slots=True)
class StructureState:
    is_valid: bool
    score: float
    cascade_touches: int
    consolidation_range_atr: float
    squeeze_score: float
    anchor_price: float
    pre_break_window: list[MarketCandle]
    near_level_atr: float


@dataclass(slots=True)
class BreakoutState:
    signal_ready: bool
    score: float
    breakout_distance_atr: float
    body_ratio: float
    close_to_extreme: float
    range_expansion: float
    volume_z: float
    follow_through_5m: bool
    book_imbalance: float
    data_ok: bool
    spread_ratio: float
    cross_ok: bool
    delta_ratio: float
    directional_delta_ratio: float
    cvd_slope: float
    directional_cvd_slope: float
    delta_divergence: bool
    aggressive_flow_support: bool
    watch_flow_support: bool
    tick_velocity_ratio: float
    round_number_score: float
    liquidation_cluster_score: float
    correlation_headwind: bool
    benchmark_support_score: float


@dataclass(slots=True)
class ScreeningResult:
    symbol: str
    venue: Venue
    setup: SetupType
    status: str
    confidence: float
    direction: Direction | None
    decision: SignalDecision | None
    last_price: float
    level_id: str | None
    level_source: str | None
    level_timeframe: Timeframe | None
    level_lower: float | None
    level_upper: float | None
    trend_bias: Direction | None
    trend_score: float
    coin_score: float
    is_coin_in_play: bool
    atr_15m: float
    volume_z_15m: float
    volume_z_1h: float
    range_expansion: float
    quote_activity_ratio: float
    squeeze_score: float
    cascade_touches: int
    consolidation_range_atr: float
    breakout_distance_atr: float
    body_ratio: float
    follow_through_5m: bool
    book_imbalance: float
    delta_ratio: float
    cvd_slope: float
    delta_divergence: bool
    tick_velocity_ratio: float
    freshness_ms: int
    spread_ratio: float
    notes: list[str]
    updated_at: datetime


class RuleEngine:
    score_weights = {
        "level_quality": 25.0,
        "trend_alignment": 20.0,
        "coin_in_play": 20.0,
        "structure_pressure": 20.0,
        "breakout_confirmation": 15.0,
    }
    minimum_expected_rr = 2.0

    def __init__(self, strategy: StrategyRuntimeConfig | None = None) -> None:
        self.strategy = strategy or StrategyRuntimeConfig()
        self.minimum_expected_rr = self.strategy.minimum_expected_rr

    def configure(self, strategy: StrategyRuntimeConfig) -> None:
        self.strategy = strategy
        self.minimum_expected_rr = strategy.minimum_expected_rr

    def evaluate(self, payload: EngineInput) -> SignalDecision | None:
        return self.inspect(payload).decision

    def inspect(self, payload: EngineInput) -> ScreeningResult:
        if len(payload.candles_5m) < 40 or len(payload.candles_15m) < 25 or len(payload.candles_1h) < 60 or len(payload.candles_4h) < 20 or not payload.levels:
            return self._empty_result(
                payload=payload,
                status="insufficient",
                notes=["Недостаточно истории свечей или уровней для оценки сетапа."],
            )

        execution_timeframe = self.strategy.timeframe
        execution_candles = payload.candles_5m if execution_timeframe == Timeframe.M5 else payload.candles_15m
        if len(execution_candles) < 40:
            return self._empty_result(
                payload=payload,
                status="insufficient",
                notes=["Недостаточно свечей на execution-timeframe для оценки пробоя."],
            )

        current = execution_candles[-1]
        atr_5m = average_true_range(payload.candles_5m[-40:] or payload.candles_5m, period=14)
        atr_15m = average_true_range(payload.candles_15m[-30:] or payload.candles_15m, period=14)
        atr_1h = average_true_range(payload.candles_1h[-40:] or payload.candles_1h, period=14)
        if atr_5m <= 0 or atr_15m <= 0 or atr_1h <= 0:
            return self._empty_result(
                payload=payload,
                status="insufficient",
                notes=["ATR не рассчитан, сигнал пропускается."],
            )

        execution_atr = atr_5m if execution_timeframe == Timeframe.M5 else atr_15m
        trend = self._trend_state(payload.candles_1h, payload.candles_4h)
        coin = self._coin_in_play(payload.candles_15m, payload.candles_1h)
        candidates = self._candidate_levels(current, payload.levels, max(execution_atr, atr_15m * 0.35))
        if not candidates:
            return self._empty_result(
                payload=payload,
                status="cold",
                atr_15m=execution_atr,
                trend=trend,
                coin=coin,
                notes=["Цена слишком далеко от значимых HTF уровней для breakout scalp."],
            )

        best_bundle: tuple[float, LevelCandidate, Direction, StructureState, BreakoutState, float, list[ScoreContribution], SignalDecision | None] | None = None
        for level in candidates[:8]:
            direction = self._breakout_direction(current, level, execution_atr) or self._default_direction(level)
            structure = self._structure_state(payload.candles_15m, payload.candles_1h, level, atr_15m, direction)
            breakout = self._breakout_state(
                current=current,
                execution_candles=execution_candles,
                candles_5m=payload.candles_5m,
                level=level,
                atr_execution=execution_atr,
                direction=direction,
                trades=payload.trades,
                order_flow=payload.order_flow,
                book=payload.book,
                health=payload.health,
                cross_venue_health=payload.cross_venue_health,
                benchmark_candles_5m=payload.benchmark_candles_5m,
            )
            confidence, contributions = self._score_candidate(level, direction, trend, coin, structure, breakout)
            decision = None
            if breakout.signal_ready:
                decision = self._build_decision(
                    payload=payload,
                    level=level,
                    current=current,
                    atr_15m=execution_atr,
                    direction=direction,
                    trend=trend,
                    coin=coin,
                    structure=structure,
                    breakout=breakout,
                    confidence=confidence,
                    contributions=contributions,
                    signal_class=self._classify(confidence),
                    confirmed_breakout=True,
                )
            elif self._should_emit_watchlist(level, coin, structure, breakout, confidence):
                decision = self._build_decision(
                    payload=payload,
                    level=level,
                    current=current,
                    atr_15m=execution_atr,
                    direction=direction,
                    trend=trend,
                    coin=coin,
                    structure=structure,
                    breakout=breakout,
                    confidence=confidence,
                    contributions=contributions,
                    signal_class=SignalClass.WATCHLIST,
                    confirmed_breakout=False,
                )
            elif self._should_emit_pre_alert(level, coin, structure, breakout, confidence):
                decision = self._build_decision(
                    payload=payload,
                    level=level,
                    current=current,
                    atr_15m=execution_atr,
                    direction=direction,
                    trend=trend,
                    coin=coin,
                    structure=structure,
                    breakout=breakout,
                    confidence=confidence,
                    contributions=contributions,
                    signal_class=SignalClass.PRE_ALERT,
                    confirmed_breakout=False,
                )
            priority = confidence + (6.0 if breakout.signal_ready else 0.0) + (4.0 if structure.is_valid else 0.0) + (1.5 if decision and decision.signal_class == SignalClass.PRE_ALERT else 0.0)
            if best_bundle is None or priority > best_bundle[0]:
                best_bundle = (priority, level, direction, structure, breakout, confidence, contributions, decision)

        if best_bundle is None:
            return self._empty_result(
                payload=payload,
                status="cold",
                atr_15m=execution_atr,
                trend=trend,
                coin=coin,
                notes=["Подходящий сильный сетап не найден в текущем рыночном окне."],
            )

        _, level, direction, structure, breakout, confidence, _, decision = best_bundle
        status = decision.signal_class.value if decision is not None else self._status_without_decision(payload.health, coin, structure, breakout)
        notes = self._notes_for_report(payload.health, trend, coin, structure, breakout, direction, level)
        return ScreeningResult(
            symbol=payload.symbol,
            venue=payload.venue,
            setup=SetupType.BREAKOUT,
            status=status,
            confidence=decision.confidence if decision is not None else confidence,
            direction=direction,
            decision=decision,
            last_price=current.close,
            level_id=level.level_id,
            level_source=level.source,
            level_timeframe=level.timeframe,
            level_lower=level.lower_price,
            level_upper=level.upper_price,
            trend_bias=trend.bias,
            trend_score=trend.score,
            coin_score=coin.score,
            is_coin_in_play=coin.is_active,
            atr_15m=atr_15m,
            volume_z_15m=coin.volume_z_15m,
            volume_z_1h=coin.volume_z_1h,
            range_expansion=breakout.range_expansion,
            quote_activity_ratio=coin.quote_activity_ratio,
            squeeze_score=structure.squeeze_score,
            cascade_touches=structure.cascade_touches,
            consolidation_range_atr=structure.consolidation_range_atr,
            breakout_distance_atr=breakout.breakout_distance_atr,
            body_ratio=breakout.body_ratio,
            follow_through_5m=breakout.follow_through_5m,
            book_imbalance=breakout.book_imbalance,
            delta_ratio=breakout.delta_ratio,
            cvd_slope=breakout.cvd_slope,
            delta_divergence=breakout.delta_divergence,
            tick_velocity_ratio=breakout.tick_velocity_ratio,
            freshness_ms=payload.health.freshness_ms,
            spread_ratio=breakout.spread_ratio,
            notes=notes,
            updated_at=current.close_time,
        )

    def _empty_result(
        self,
        *,
        payload: EngineInput,
        status: str,
        notes: list[str],
        atr_15m: float = 0.0,
        trend: TrendState | None = None,
        coin: CoinState | None = None,
    ) -> ScreeningResult:
        current = payload.candles_5m[-1] if payload.candles_5m else payload.candles_15m[-1] if payload.candles_15m else None
        return ScreeningResult(
            symbol=payload.symbol,
            venue=payload.venue,
            setup=SetupType.BREAKOUT,
            status=status,
            confidence=0.0,
            direction=trend.bias if trend else None,
            decision=None,
            last_price=current.close if current else 0.0,
            level_id=None,
            level_source=None,
            level_timeframe=None,
            level_lower=None,
            level_upper=None,
            trend_bias=trend.bias if trend else None,
            trend_score=trend.score if trend else 0.0,
            coin_score=coin.score if coin else 0.0,
            is_coin_in_play=coin.is_active if coin else False,
            atr_15m=atr_15m,
            volume_z_15m=coin.volume_z_15m if coin else 0.0,
            volume_z_1h=coin.volume_z_1h if coin else 0.0,
            range_expansion=coin.range_expansion if coin else 0.0,
            quote_activity_ratio=coin.quote_activity_ratio if coin else 0.0,
            squeeze_score=0.0,
            cascade_touches=0,
            consolidation_range_atr=0.0,
            breakout_distance_atr=0.0,
            body_ratio=0.0,
            follow_through_5m=False,
            book_imbalance=0.0,
            delta_ratio=0.0,
            cvd_slope=0.0,
            delta_divergence=False,
            tick_velocity_ratio=0.0,
            freshness_ms=payload.health.freshness_ms,
            spread_ratio=payload.health.spread_ratio,
            notes=notes,
            updated_at=current.close_time if current else datetime.now(tz=timezone.utc),
        )

    def _candidate_levels(self, candle: MarketCandle, levels: list[LevelCandidate], atr_15m: float) -> list[LevelCandidate]:
        selected: list[LevelCandidate] = []
        for level in levels:
            if not self.strategy.enable_liquidation_levels and level.source.startswith("liquidation-cluster"):
                continue
            if not self.strategy.enable_round_number_levels and level.source == "round-number":
                continue
            distance = min(abs(candle.close - level.lower_price), abs(candle.close - level.upper_price))
            if distance <= atr_15m * 1.8 or self._is_breakout(candle, level, atr_15m):
                selected.append(level)
        return sorted(
            selected,
            key=lambda item: (
                min(abs(candle.close - item.reference_price), atr_15m * 4.0),
                -item.strength,
                item.detected_at,
            ),
        )

    def _is_breakout(self, candle: MarketCandle, level: LevelCandidate, atr_15m: float) -> bool:
        threshold = 0.03 * atr_15m
        if level.kind == LevelKind.RESISTANCE:
            return candle.close > level.upper_price + threshold
        return candle.close < level.lower_price - threshold

    def _default_direction(self, level: LevelCandidate) -> Direction:
        return Direction.LONG if level.kind == LevelKind.RESISTANCE else Direction.SHORT

    def _breakout_direction(
        self,
        candle: MarketCandle,
        level: LevelCandidate,
        atr_15m: float,
    ) -> Direction | None:
        if level.kind == LevelKind.RESISTANCE and candle.close > level.upper_price + atr_15m * 0.03:
            return Direction.LONG
        if level.kind == LevelKind.SUPPORT and candle.close < level.lower_price - atr_15m * 0.03:
            return Direction.SHORT
        return None

    def _trend_state(self, candles_1h: list[MarketCandle], candles_4h: list[MarketCandle]) -> TrendState:
        closes_1h = [item.close for item in candles_1h]
        closes_4h = [item.close for item in candles_4h]
        fast_1h = simple_moving_average(closes_1h, 12)
        slow_1h = simple_moving_average(closes_1h, 48)
        fast_4h = simple_moving_average(closes_4h, 6)
        slow_4h = simple_moving_average(closes_4h, 20)
        slope_1h = closes_1h[-1] - closes_1h[-12]
        slope_4h = closes_4h[-1] - closes_4h[-6]

        bullish_votes = sum([fast_1h >= slow_1h, fast_4h >= slow_4h * 0.997, slope_1h >= 0, slope_4h >= 0])
        bearish_votes = sum([fast_1h <= slow_1h, fast_4h <= slow_4h * 1.003, slope_1h <= 0, slope_4h <= 0])

        if bullish_votes >= 3 and bullish_votes > bearish_votes:
            score = min(
                1.0,
                0.42
                + min(max((fast_1h - slow_1h) / max(abs(slow_1h), 1e-9), 0.0) * 45, 0.28)
                + min(max((fast_4h - slow_4h) / max(abs(slow_4h), 1e-9), 0.0) * 30, 0.20)
                + (0.05 if slope_1h > 0 else 0.0)
                + (0.05 if slope_4h > 0 else 0.0),
            )
            return TrendState(Direction.LONG, score, fast_1h, slow_1h, fast_4h, slow_4h, bullish_votes, bearish_votes)
        if bearish_votes >= 3 and bearish_votes > bullish_votes:
            score = min(
                1.0,
                0.42
                + min(max((slow_1h - fast_1h) / max(abs(slow_1h), 1e-9), 0.0) * 45, 0.28)
                + min(max((slow_4h - fast_4h) / max(abs(slow_4h), 1e-9), 0.0) * 30, 0.20)
                + (0.05 if slope_1h < 0 else 0.0)
                + (0.05 if slope_4h < 0 else 0.0),
            )
            return TrendState(Direction.SHORT, score, fast_1h, slow_1h, fast_4h, slow_4h, bullish_votes, bearish_votes)
        return TrendState(None, 0.28, fast_1h, slow_1h, fast_4h, slow_4h, bullish_votes, bearish_votes)

    def _coin_in_play(self, candles_15m: list[MarketCandle], candles_1h: list[MarketCandle]) -> CoinState:
        current_15m = candles_15m[-1]
        ranges = [item.high - item.low for item in candles_15m[-21:-1]]
        current_range = max(current_15m.high - current_15m.low, 1e-9)
        range_expansion = current_range / max(median_spread(ranges), 1e-9)
        volume_z_15m = volume_zscore(candles_15m[-25:], period=20)
        volume_z_1h = volume_zscore(candles_1h[-25:], period=20)
        quote_baseline = [item.quote_volume for item in candles_15m[-13:-1]]
        baseline = simple_moving_average(quote_baseline, len(quote_baseline)) if quote_baseline else 0.0
        quote_activity_ratio = current_15m.quote_volume / max(baseline, 1e-9)
        score = min(
            1.0,
            max(volume_z_15m, 0.0) / 2.5 * 0.40
            + max(volume_z_1h, 0.0) / 2.0 * 0.15
            + min(range_expansion / 1.6, 1.0) * 0.25
            + min(max(quote_activity_ratio - 0.85, 0.0) / 0.90, 1.0) * 0.20,
        )
        is_active = score >= 0.35 or (volume_z_15m >= 1.0 and range_expansion >= 1.0)
        return CoinState(is_active, score, volume_z_15m, volume_z_1h, range_expansion, quote_activity_ratio)

    def _structure_state(
        self,
        candles_15m: list[MarketCandle],
        candles_1h: list[MarketCandle],
        level: LevelCandidate,
        atr_15m: float,
        direction: Direction,
    ) -> StructureState:
        pre_break_window = candles_15m[-8:-1]
        if len(pre_break_window) < 5:
            return StructureState(False, 0.0, 0, 99.0, 0.0, level.reference_price, pre_break_window, 99.0)

        highs = [item.high for item in pre_break_window]
        lows = [item.low for item in pre_break_window]
        closes = [item.close for item in pre_break_window]
        consolidation_range_atr = (max(highs) - min(lows)) / max(atr_15m, 1e-9)
        cascade_touches = self._cascade_touches(candles_1h, level, atr_15m)
        squeeze_score = self._squeeze_score(pre_break_window, direction)

        if direction == Direction.LONG:
            anchor_price = min(lows)
            near_level_atr = (level.upper_price - max(highs)) / max(atr_15m, 1e-9)
            contained = all(item.close <= level.upper_price + atr_15m * 0.18 for item in pre_break_window)
        else:
            anchor_price = max(highs)
            near_level_atr = (min(lows) - level.lower_price) / max(atr_15m, 1e-9)
            contained = all(item.close >= level.lower_price - atr_15m * 0.18 for item in pre_break_window)

        valid = (
            contained
            and -0.35 <= near_level_atr <= 0.90
            and consolidation_range_atr <= 3.4
            and squeeze_score >= min(self.strategy.squeeze_threshold * 0.45, 0.60)
        )
        if level.source.startswith("cascade") and max(cascade_touches, level.touches) < max(self.strategy.min_touches - 1, 2):
            valid = False

        source_bonus = 0.20 if level.source.startswith("cascade") else 0.14 if level.source.startswith("prev-") else 0.10
        touch_bonus = min(max(cascade_touches, level.touches) / 4.0, 1.0) * 0.35
        compression_bonus = min(max(2.6 - consolidation_range_atr, 0.0) / 2.6, 1.0) * 0.20
        squeeze_bonus = min(squeeze_score, 1.0) * 0.35
        proximity_bonus = min(max(0.90 - max(near_level_atr, 0.0), 0.0) / 0.90, 1.0) * 0.10
        score = min(1.0, source_bonus + touch_bonus + compression_bonus + squeeze_bonus + proximity_bonus)
        return StructureState(
            is_valid=valid,
            score=score,
            cascade_touches=max(cascade_touches, level.touches),
            consolidation_range_atr=consolidation_range_atr,
            squeeze_score=squeeze_score,
            anchor_price=anchor_price,
            pre_break_window=pre_break_window,
            near_level_atr=near_level_atr,
        )

    def _breakout_state(
        self,
        *,
        current: MarketCandle,
        execution_candles: list[MarketCandle],
        candles_5m: list[MarketCandle],
        level: LevelCandidate,
        atr_execution: float,
        direction: Direction,
        trades: list[TradeTick],
        order_flow: OrderFlowSnapshot | None,
        book: BookSnapshot | None,
        health: DataHealth,
        cross_venue_health: list[DataHealth],
        benchmark_candles_5m: dict[str, list[MarketCandle]],
    ) -> BreakoutState:
        candle_range = max(current.high - current.low, 1e-9)
        body_ratio = abs(current.close - current.open) / candle_range
        recent_ranges = [item.high - item.low for item in execution_candles[-25:-1]]
        range_expansion = candle_range / max(median_spread(recent_ranges), 1e-9)
        volume_z = volume_zscore(execution_candles[-40:], period=30)
        book_imbalance = self._book_imbalance(book)
        spread_ratio = health.spread_ratio
        fresh_cross = [item for item in cross_venue_health if item.venue != health.venue and item.is_fresh]
        cross_ok = True if not cross_venue_health else len(fresh_cross) >= 1
        data_ok = health.is_fresh and not health.has_sequence_gap and spread_ratio <= 3.5
        if order_flow is not None:
            delta_ratio = order_flow.delta_ratio
            cvd_slope = order_flow.cvd_slope
            directional_delta_ratio = delta_ratio if direction == Direction.LONG else -delta_ratio
            directional_cvd_slope = cvd_slope if direction == Direction.LONG else -cvd_slope
            tick_velocity_ratio = order_flow.tick_velocity_ratio
            recent_reference = execution_candles[-min(len(execution_candles), 6)]
            price_change = execution_candles[-1].close - recent_reference.close
            if direction == Direction.LONG:
                delta_divergence = price_change > 0 and delta_ratio <= -self.strategy.delta_divergence_threshold
            else:
                delta_divergence = price_change < 0 and delta_ratio >= self.strategy.delta_divergence_threshold
        else:
            delta_ratio, directional_delta_ratio, cvd_slope, directional_cvd_slope, delta_divergence = self._trade_flow_metrics(
                trades=trades,
                execution_candles=execution_candles,
                direction=direction,
            )
            tick_velocity_ratio = 0.0
        aggressive_flow_support = (
            directional_delta_ratio >= self.strategy.delta_ratio_threshold
            and directional_cvd_slope >= self.strategy.cvd_slope_threshold
            and not delta_divergence
        )
        watch_flow_support = (
            directional_delta_ratio >= self.strategy.watchlist_delta_ratio_threshold
            and directional_cvd_slope >= -(self.strategy.cvd_slope_threshold * 0.35)
            and not delta_divergence
        )

        if direction == Direction.LONG:
            breakout_distance_atr = (current.close - level.upper_price) / max(atr_execution, 1e-9)
            close_to_extreme = (current.high - current.close) / candle_range
            book_support = book_imbalance >= -0.22
        else:
            breakout_distance_atr = (level.lower_price - current.close) / max(atr_execution, 1e-9)
            close_to_extreme = (current.close - current.low) / candle_range
            book_support = book_imbalance <= 0.22

        velocity_support = (
            (not self.strategy.enable_tick_velocity_alerts)
            or tick_velocity_ratio >= max(self.strategy.tick_velocity_alert_multiplier * 0.80, 1.15)
        )
        round_number_score = self._round_number_score(level, book)
        liquidation_cluster_score = self._liquidation_cluster_score(level)
        benchmark_support_score, correlation_headwind = self._benchmark_support(
            direction=direction,
            execution_candles=execution_candles,
            benchmark_candles_5m=benchmark_candles_5m,
        )
        follow_through_5m = self._follow_through_5m(candles_5m[-3:], level, direction)
        signal_ready = (
            data_ok
            and breakout_distance_atr >= self.strategy.breakout_distance_atr
            and body_ratio >= self.strategy.body_ratio_threshold
            and close_to_extreme <= self.strategy.close_to_extreme_threshold
            and volume_z >= self.strategy.volume_z_threshold
            and range_expansion >= self.strategy.range_expansion_threshold
            and follow_through_5m
            and book_support
            and cross_ok
            and aggressive_flow_support
            and velocity_support
            and not correlation_headwind
        )
        score = min(
            1.0,
            min(max(breakout_distance_atr, 0.0) / 0.40, 1.0) * 0.28
            + min(body_ratio / 0.80, 1.0) * 0.24
            + min(max(volume_z, 0.0) / 3.0, 1.0) * 0.20
            + min(range_expansion / 1.8, 1.0) * 0.12
            + (0.10 if follow_through_5m else 0.0)
            + (0.06 if book_support else 0.0)
            + (0.08 if aggressive_flow_support else 0.0),
        )
        if velocity_support and tick_velocity_ratio > 0:
            score = min(score + min(tick_velocity_ratio / max(self.strategy.tick_velocity_alert_multiplier, 1.0), 1.0) * 0.08, 1.0)
        if round_number_score > 0:
            score = min(score + round_number_score * 0.05, 1.0)
        if liquidation_cluster_score > 0:
            score = min(score + liquidation_cluster_score * 0.06, 1.0)
        if benchmark_support_score > 0:
            score = min(score + benchmark_support_score * 0.04, 1.0)
        if correlation_headwind:
            score = max(score - 0.12, 0.0)
        return BreakoutState(
            signal_ready=signal_ready,
            score=score,
            breakout_distance_atr=breakout_distance_atr,
            body_ratio=body_ratio,
            close_to_extreme=close_to_extreme,
            range_expansion=range_expansion,
            volume_z=volume_z,
            follow_through_5m=follow_through_5m,
            book_imbalance=book_imbalance,
            data_ok=data_ok,
            spread_ratio=spread_ratio,
            cross_ok=cross_ok,
            delta_ratio=delta_ratio,
            directional_delta_ratio=directional_delta_ratio,
            cvd_slope=cvd_slope,
            directional_cvd_slope=directional_cvd_slope,
            delta_divergence=delta_divergence,
            aggressive_flow_support=aggressive_flow_support,
            watch_flow_support=watch_flow_support,
            tick_velocity_ratio=tick_velocity_ratio,
            round_number_score=round_number_score,
            liquidation_cluster_score=liquidation_cluster_score,
            correlation_headwind=correlation_headwind,
            benchmark_support_score=benchmark_support_score,
        )

    def _score_candidate(
        self,
        level: LevelCandidate,
        direction: Direction,
        trend: TrendState,
        coin: CoinState,
        structure: StructureState,
        breakout: BreakoutState,
    ) -> tuple[float, list[ScoreContribution]]:
        level_factor = min(
            level.strength
            + min(max(structure.cascade_touches, level.touches), 4) * 0.06
            + breakout.round_number_score * 0.10
            + breakout.liquidation_cluster_score * 0.12,
            1.0,
        )
        trend_factor = trend.score if trend.bias == direction else (0.38 if trend.bias is None else 0.12)
        structure_factor = structure.score if structure.is_valid else structure.score * 0.72
        breakout_factor = breakout.score if breakout.signal_ready else breakout.score * 0.78
        group_scores = {
            "level_quality": self.score_weights["level_quality"] * level_factor,
            "trend_alignment": self.score_weights["trend_alignment"] * trend_factor,
            "coin_in_play": self.score_weights["coin_in_play"] * coin.score,
            "structure_pressure": self.score_weights["structure_pressure"] * structure_factor,
            "breakout_confirmation": self.score_weights["breakout_confirmation"] * breakout_factor,
        }
        contributions = [
            ScoreContribution(
                group=name,
                score=score,
                max_score=self.score_weights[name],
                reason=self._reason_for_group(name, level, direction, trend, coin, structure, breakout),
            )
            for name, score in group_scores.items()
        ]
        return sum(item.score for item in contributions), contributions

    def _build_decision(
        self,
        *,
        payload: EngineInput,
        level: LevelCandidate,
        current: MarketCandle,
        atr_15m: float,
        direction: Direction,
        trend: TrendState,
        coin: CoinState,
        structure: StructureState,
        breakout: BreakoutState,
        confidence: float,
        contributions: list[ScoreContribution],
        signal_class: SignalClass,
        confirmed_breakout: bool,
    ) -> SignalDecision | None:
        if confirmed_breakout and signal_class == SignalClass.SUPPRESSED:
            return None
        if signal_class == SignalClass.WATCHLIST and confidence < self.strategy.watchlist_confidence_threshold:
            return None
        if signal_class == SignalClass.PRE_ALERT and confidence < self.strategy.pre_alert_confidence_threshold:
            return None

        if confirmed_breakout:
            entry_price = current.close
        elif direction == Direction.LONG:
            entry_price = level.upper_price + atr_15m * 0.03
        else:
            entry_price = level.lower_price - atr_15m * 0.03
        if direction == Direction.LONG:
            invalidation = min(structure.anchor_price - atr_15m * 0.10, level.lower_price - atr_15m * 0.15)
        else:
            invalidation = max(structure.anchor_price + atr_15m * 0.10, level.upper_price + atr_15m * 0.15)
        risk = max(abs(entry_price - invalidation), atr_15m * self.strategy.sl_multiplier)
        targets = self._project_targets(payload.levels, level, direction, entry_price, risk)
        if targets is None:
            return None
        t1, t2 = targets
        expected_rr = abs((t1 - entry_price) / risk)
        if expected_rr < self.minimum_expected_rr:
            return None
        if signal_class == SignalClass.PRE_ALERT:
            stage = "pre_alert"
        else:
            stage = "activated" if confirmed_breakout else "watch"
        alert_key = f"{payload.symbol}:breakout:{direction.value}:{level.level_id}:{self.strategy.timeframe.value}:{stage}:{level.detected_at.date().isoformat()}"

        why_not_higher: list[str] = []
        if signal_class == SignalClass.PRE_ALERT:
            why_not_higher.append("EARLY WARNING: breakout has not started yet. Wait for volume and level approach.")
        elif not confirmed_breakout:
            why_not_higher.append("Пробой еще не подтвержден закрытием 5m свечи за уровнем.")
        if trend.bias != direction:
            why_not_higher.append("Текущий HTF тренд не полностью синхронизирован с направлением пробоя.")
        if structure.cascade_touches < self.strategy.min_touches:
            why_not_higher.append(f"Каскад еще неглубокий: подтверждено только {structure.cascade_touches} касания.")
        if structure.squeeze_score < self.strategy.squeeze_threshold:
            why_not_higher.append(f"Поджатие есть, но не максимальное: squeeze score {structure.squeeze_score:.2f}.")
        if coin.volume_z_15m < 1.40:
            why_not_higher.append(f"Объем выше фона, но без экстремума: z-score {coin.volume_z_15m:.2f}.")
        if not breakout.follow_through_5m:
            why_not_higher.append("Нет уверенного follow-through на 5m после закрытия breakout-свечи.")
        if not breakout.cross_ok:
            why_not_higher.append("Кросс-биржевое подтверждение слабее, чем хотелось бы.")
        if breakout.delta_divergence:
            why_not_higher.append("Delta / CVD show divergence against the breakout direction.")
        elif not breakout.aggressive_flow_support:
            why_not_higher.append(
                f"Aggressive flow is still weak: delta ratio {breakout.delta_ratio:.2f}, CVD slope {breakout.cvd_slope:.2f}."
            )
        if breakout.tick_velocity_ratio < max(self.strategy.tick_velocity_alert_multiplier * 0.80, 1.15):
            why_not_higher.append(
                f"Tick velocity is not explosive enough yet: {breakout.tick_velocity_ratio:.2f}x of the 10m baseline."
            )
        if breakout.correlation_headwind:
            why_not_higher.append("BTC/ETH benchmark flow is too supportive for this alt short.")
        if expected_rr < 2.4:
            why_not_higher.append(f"Expected R:R remains close to the minimum threshold: {expected_rr:.2f}.")
        if not why_not_higher:
            why_not_higher.append("Сетап сильный, но confidence дополнительно ограничен до накопления live-статистики.")

        rationale = [
            f"Монета в игре: volume z-score {coin.volume_z_15m:.2f}, quote-volume x{coin.quote_activity_ratio:.2f} к фону.",
            f"Тренд 1h/4h: {trend.bias.value.upper() if trend.bias else 'MIXED'} | fast/slow 1h {trend.fast_1h:.4f}/{trend.slow_1h:.4f}.",
            f"Уровень: {level.timeframe.value} {level.source} | каскадных касаний {structure.cascade_touches}.",
            f"Наторговка перед пробоем: диапазон {structure.consolidation_range_atr:.2f} ATR.",
            f"Поджатие к уровню: squeeze score {structure.squeeze_score:.2f}.",
            (
                f"Подтверждение импульса: {breakout.breakout_distance_atr:.2f} ATR за уровнем, volume z {breakout.volume_z:.2f}, 5m follow-through {'да' if breakout.follow_through_5m else 'нет'}."
                if confirmed_breakout
                else f"Цена прижата к уровню: dist {breakout.breakout_distance_atr:.2f} ATR, volume z {breakout.volume_z:.2f}, ждем 5m close за зоной."
            ),
            f"Delta / CVD: delta ratio {breakout.delta_ratio:.2f}, CVD slope {breakout.cvd_slope:.2f}, divergence {'yes' if breakout.delta_divergence else 'no'}.",
            f"Tick velocity: {breakout.tick_velocity_ratio:.2f}x of the 10m baseline.",
        ]
        if breakout.round_number_score > 0:
            rationale.append(f"Round-number confluence: score {breakout.round_number_score:.2f}, level source {level.source}.")
        if breakout.liquidation_cluster_score > 0:
            rationale.append(f"Liquidation cluster confluence: score {breakout.liquidation_cluster_score:.2f}.")
        if breakout.correlation_headwind:
            rationale.append("BTC/ETH benchmark flow is pressing higher and raises false-breakdown risk for this short.")
        if payload.derivative_context is not None:
            rationale.append(
                f"Контекст derivatives: funding {payload.derivative_context.funding_rate:.5f}, basis {payload.derivative_context.basis_bps:.1f} bps, OI {payload.derivative_context.open_interest:.2f}."
            )
        else:
            rationale.append("Контекст derivatives: н/д.")

        health = payload.health.model_copy(update={"spread_ratio": breakout.spread_ratio})
        render_context = {
            "price_zone": level.zone_text,
            "htf_source": f"{level.timeframe.value} {level.source}",
            "trigger": self._trigger_text(
                direction,
                current,
                level,
                breakout.breakout_distance_atr,
                confirmed_breakout,
                entry_price,
                signal_class,
                structure.near_level_atr,
                structure.squeeze_score,
            ),
            "stop_logic": self._stop_logic(direction, invalidation, structure.anchor_price, level),
            "cancel_if": (
                "5m закрытие вернулось под уровень / данные устарели / спред резко расширился"
                if confirmed_breakout
                else "цена ушла от уровня и поджатие распалось / данные устарели / спред резко расширился"
            ),
            "venues_used": ", ".join(sorted({payload.venue.value, *[item.venue.value for item in payload.cross_venue_health if item.is_fresh]})),
            "level_lower": level.lower_price,
            "level_upper": level.upper_price,
            "entry_price": entry_price,
            "stop_price": invalidation,
            "tp1": round(t1, 6),
            "tp2": round(t2, 6),
            "minimum_expected_rr": self.minimum_expected_rr,
            "chart_timeframe": self.strategy.timeframe.value,
            "setup_stage": stage,
            "signal_class": signal_class.value,
            "trend_bias": trend.bias.value if trend.bias else None,
            "cascade_touches": structure.cascade_touches,
            "consolidation_range_atr": structure.consolidation_range_atr,
            "squeeze_score": structure.squeeze_score,
            "tick_velocity_ratio": breakout.tick_velocity_ratio,
            "round_number_score": breakout.round_number_score,
            "liquidation_cluster_score": breakout.liquidation_cluster_score,
            "correlation_headwind": breakout.correlation_headwind,
        }
        return SignalDecision(
            symbol=payload.symbol,
            venue=payload.venue,
            timeframe=self.strategy.timeframe,
            setup=SetupType.BREAKOUT,
            direction=direction,
            signal_class=signal_class,
            confidence=confidence,
            level_id=level.level_id,
            alert_key=alert_key,
            entry_price=entry_price,
            invalidation_price=invalidation,
            targets=[round(t1, 6), round(t2, 6)],
            expected_rr=expected_rr,
            rationale=rationale,
            why_not_higher=why_not_higher,
            contributions=contributions,
            data_health=health,
            feature_snapshot={
                "atr_15m": atr_15m,
                "trend_score": trend.score,
                "volume_zscore_15m": coin.volume_z_15m,
                "volume_zscore_1h": coin.volume_z_1h,
                "range_expansion": breakout.range_expansion,
                "quote_activity_ratio": coin.quote_activity_ratio,
                "level_strength": level.strength,
                "cascade_touches": structure.cascade_touches,
                "consolidation_range_atr": structure.consolidation_range_atr,
                "squeeze_score": structure.squeeze_score,
                "breakout_distance_atr": breakout.breakout_distance_atr,
                "body_ratio": breakout.body_ratio,
                "follow_through_5m": breakout.follow_through_5m,
                "book_imbalance": breakout.book_imbalance,
                "spread_ratio": breakout.spread_ratio,
                "confirmed_breakout": confirmed_breakout,
                "delta_ratio": breakout.delta_ratio,
                "cvd_slope": breakout.cvd_slope,
                "delta_divergence": breakout.delta_divergence,
                "tick_velocity_ratio": breakout.tick_velocity_ratio,
                "round_number_score": breakout.round_number_score,
                "liquidation_cluster_score": breakout.liquidation_cluster_score,
                "correlation_headwind": breakout.correlation_headwind,
                "signal_class": signal_class.value,
            },
            render_context=render_context,
        )

    def _project_targets(
        self,
        levels: list[LevelCandidate],
        active_level: LevelCandidate,
        direction: Direction,
        entry_price: float,
        risk: float,
    ) -> tuple[float, float] | None:
        minimum_target_price = entry_price + (risk * self.minimum_expected_rr if direction == Direction.LONG else -risk * self.minimum_expected_rr)
        fallback_t1 = minimum_target_price
        fallback_t2 = entry_price + (risk * 3.0 if direction == Direction.LONG else -risk * 3.0)
        projected: list[float] = []
        for level in levels:
            if level.level_id == active_level.level_id:
                continue
            target_price = self._target_price_for_level(level, direction)
            if target_price is None:
                continue
            if direction == Direction.LONG and target_price <= minimum_target_price:
                continue
            if direction == Direction.SHORT and target_price >= minimum_target_price:
                continue
            projected.append(target_price)

        projected = sorted(set(projected), reverse=(direction == Direction.SHORT))
        if not projected:
            return (fallback_t1, fallback_t2)

        t1 = projected[0]
        t2 = projected[1] if len(projected) > 1 else fallback_t2
        if direction == Direction.LONG:
            t2 = max(t2, t1 + risk * 0.75)
        else:
            t2 = min(t2, t1 - risk * 0.75)
        return (t1, t2)

    def _target_price_for_level(self, level: LevelCandidate, direction: Direction) -> float | None:
        if direction == Direction.LONG and level.kind == LevelKind.RESISTANCE:
            return float(level.lower_price)
        if direction == Direction.SHORT and level.kind == LevelKind.SUPPORT:
            return float(level.upper_price)
        return None

    def _cascade_touches(self, candles_1h: list[MarketCandle], level: LevelCandidate, atr_15m: float) -> int:
        tolerance = max(atr_15m * 0.25, level.reference_price * 0.0012)
        sample = candles_1h[-30:-1]
        if level.kind == LevelKind.RESISTANCE:
            return sum(1 for item in sample if abs(item.high - level.reference_price) <= tolerance)
        return sum(1 for item in sample if abs(item.low - level.reference_price) <= tolerance)

    def _squeeze_score(self, candles: list[MarketCandle], direction: Direction) -> float:
        if len(candles) < 3:
            return 0.0
        values = [item.low for item in candles] if direction == Direction.LONG else [item.high for item in candles]
        progress = 0
        for previous, current in zip(values, values[1:], strict=False):
            if direction == Direction.LONG and current >= previous:
                progress += 1
            if direction == Direction.SHORT and current <= previous:
                progress += 1
        return progress / max(len(values) - 1, 1)

    def _round_number_score(self, level: LevelCandidate, book: BookSnapshot | None) -> float:
        if level.source != "round-number":
            return 0.0
        score = 0.55
        if book is None:
            return score
        tolerance = max(abs(level.reference_price) * 0.0015, 1e-9)
        nearby_bid = sum(item.size for item in book.bids[:10] if abs(item.price - level.reference_price) <= tolerance)
        nearby_ask = sum(item.size for item in book.asks[:10] if abs(item.price - level.reference_price) <= tolerance)
        density = nearby_bid + nearby_ask
        if density <= 0:
            return score
        top_depth = sum(item.size for item in book.bids[:10]) + sum(item.size for item in book.asks[:10])
        density_ratio = density / max(top_depth, 1e-9)
        return min(score + min(density_ratio, 1.0) * 0.45, 1.0)

    def _liquidation_cluster_score(self, level: LevelCandidate) -> float:
        if not level.source.startswith("liquidation-cluster"):
            return 0.0
        return min(0.45 + min(level.touches, 6) * 0.08 + level.strength * 0.20, 1.0)

    def _benchmark_support(
        self,
        *,
        direction: Direction,
        execution_candles: list[MarketCandle],
        benchmark_candles_5m: dict[str, list[MarketCandle]],
    ) -> tuple[float, bool]:
        if direction != Direction.SHORT or not self.strategy.enable_btc_eth_correlation_filter:
            return 0.0, False
        if len(execution_candles) < 8:
            return 0.0, False
        alt_window = execution_candles[-6:]
        alt_return_pct = ((alt_window[-1].close - alt_window[0].open) / max(alt_window[0].open, 1e-9)) * 100.0
        strongest_benchmark_score = 0.0
        strongest_benchmark_return = 0.0
        for symbol in ("BTCUSDT", "ETHUSDT"):
            candles = benchmark_candles_5m.get(symbol, [])
            if len(candles) < 8:
                continue
            window = candles[-6:]
            benchmark_return_pct = ((window[-1].close - window[0].open) / max(window[0].open, 1e-9)) * 100.0
            benchmark_atr = average_true_range(candles[-20:] or candles, period=14)
            normalized_move = 0.0
            if benchmark_atr > 0:
                normalized_move = max(window[-1].close - window[0].open, 0.0) / benchmark_atr
            squeeze_long = self._squeeze_score(window, Direction.LONG)
            benchmark_score = 0.0
            if benchmark_return_pct > 0:
                benchmark_score = min(normalized_move / 1.8, 1.0) * 0.70 + min(squeeze_long, 1.0) * 0.30
            else:
                benchmark_score = min(squeeze_long, 1.0) * 0.20
            strongest_benchmark_score = max(strongest_benchmark_score, benchmark_score)
            strongest_benchmark_return = max(strongest_benchmark_return, benchmark_return_pct)
        if strongest_benchmark_score <= 0:
            return 0.0, False
        relative_weakness = strongest_benchmark_return - alt_return_pct
        support_score = min(max(relative_weakness, 0.0) / 1.5, 1.0)
        headwind = strongest_benchmark_score >= self.strategy.btc_correlation_threshold and support_score < 0.35
        return support_score, headwind

    def _follow_through_5m(self, candles_5m: list[MarketCandle], level: LevelCandidate, direction: Direction) -> bool:
        if len(candles_5m) < 2:
            return False
        if direction == Direction.LONG:
            return all(item.close >= level.upper_price for item in candles_5m[-2:])
        return all(item.close <= level.lower_price for item in candles_5m[-2:])

    def _trade_flow_metrics(
        self,
        *,
        trades: list[TradeTick],
        execution_candles: list[MarketCandle],
        direction: Direction,
    ) -> tuple[float, float, float, float, bool]:
        if not trades:
            return 0.0, 0.0, 0.0, 0.0, False
        ordered = sorted(trades, key=lambda item: item.timestamp)[-80:]
        signed_notionals: list[float] = []
        running_cvd = 0.0
        cvd_points: list[float] = []
        for trade in ordered:
            notional = max(trade.size * trade.price, 0.0)
            signed = notional if trade.side.value == "buy" else -notional
            signed_notionals.append(signed)
            running_cvd += signed
            cvd_points.append(running_cvd)
        total_notional = sum(abs(item) for item in signed_notionals)
        if total_notional <= 0:
            return 0.0, 0.0, 0.0, 0.0, False
        delta_ratio = sum(signed_notionals) / total_notional
        cvd_slope = (cvd_points[-1] - cvd_points[0]) / total_notional if len(cvd_points) > 1 else delta_ratio
        directional_delta_ratio = delta_ratio if direction == Direction.LONG else -delta_ratio
        directional_cvd_slope = cvd_slope if direction == Direction.LONG else -cvd_slope
        recent_reference = execution_candles[-min(len(execution_candles), 6)]
        price_change = execution_candles[-1].close - recent_reference.close
        if direction == Direction.LONG:
            divergence = price_change > 0 and delta_ratio <= -self.strategy.delta_divergence_threshold
        else:
            divergence = price_change < 0 and delta_ratio >= self.strategy.delta_divergence_threshold
        return delta_ratio, directional_delta_ratio, cvd_slope, directional_cvd_slope, divergence

    def _book_imbalance(self, book: BookSnapshot | None) -> float:
        if book is None:
            return 0.0
        bid = sum(item.size for item in book.bids[:5])
        ask = sum(item.size for item in book.asks[:5])
        total = bid + ask
        return ((bid - ask) / total) if total else 0.0

    def _reason_for_group(
        self,
        name: str,
        level: LevelCandidate,
        direction: Direction,
        trend: TrendState,
        coin: CoinState,
        structure: StructureState,
        breakout: BreakoutState,
    ) -> str:
        if name == "level_quality":
            return f"HTF уровень {level.source} и {structure.cascade_touches} касания рядом с ценой пробоя."
        if name == "trend_alignment":
            if trend.bias == direction:
                return f"HTF тренд поддерживает направление {direction.value.upper()}."
            if trend.bias is None:
                return "Тренд смешанный, поэтому группа получила частичный вес."
            return "Направление пробоя идет против доминирующего HTF тренда."
        if name == "coin_in_play":
            return f"Volume z-score {coin.volume_z_15m:.2f}, quote-volume x{coin.quote_activity_ratio:.2f}."
        if name == "structure_pressure":
            return f"Наторговка {structure.consolidation_range_atr:.2f} ATR и squeeze score {structure.squeeze_score:.2f}."
        return (
            f"Пробой на {breakout.breakout_distance_atr:.2f} ATR, volume z {breakout.volume_z:.2f}, "
            f"delta {breakout.delta_ratio:.2f}, CVD {breakout.cvd_slope:.2f}, "
            f"follow-through {'да' if breakout.follow_through_5m else 'нет'}."
        )

    def _trigger_text(
        self,
        direction: Direction,
        candle: MarketCandle,
        level: LevelCandidate,
        breakout_distance_atr: float,
        confirmed_breakout: bool,
        entry_price: float,
        signal_class: SignalClass,
        near_level_atr: float,
        squeeze_score: float,
    ) -> str:
        if signal_class == SignalClass.PRE_ALERT:
            return (
                f"Цена на подходе к уровню ({near_level_atr:.2f} ATR). "
                f"Идет сжатие волатильности (Squeeze: {squeeze_score:.2f}). "
                "Приготовьтесь к возможному пробою."
            )
        if direction == Direction.LONG and confirmed_breakout:
            return f"5m закрытие выше зоны сопротивления на {breakout_distance_atr:.2f} ATR ({candle.close:.4f})."
        if direction == Direction.SHORT and confirmed_breakout:
            return f"5m закрытие ниже зоны поддержки на {breakout_distance_atr:.2f} ATR ({candle.close:.4f})."
        if direction == Direction.LONG:
            return f"Цена стоит под сопротивлением. Для входа нужен 5m close выше {entry_price:.4f}."
        return f"Цена стоит над поддержкой. Для входа нужен 5m close ниже {entry_price:.4f}."

    def _stop_logic(
        self,
        direction: Direction,
        invalidation: float,
        anchor_price: float,
        level: LevelCandidate,
    ) -> str:
        if direction == Direction.LONG:
            return f"SL под наторговкой и ниже зоны ({invalidation:.4f}); локальный anchor {anchor_price:.4f}, нижняя граница уровня {level.lower_price:.4f}."
        return f"SL над наторговкой и выше зоны ({invalidation:.4f}); локальный anchor {anchor_price:.4f}, верхняя граница уровня {level.upper_price:.4f}."

    def _status_without_decision(
        self,
        health: DataHealth,
        coin: CoinState,
        structure: StructureState,
        breakout: BreakoutState,
    ) -> str:
        if not health.is_fresh or health.has_sequence_gap:
            return "stale"
        if breakout.breakout_distance_atr >= -0.35 and structure.score >= 0.48 and (coin.score >= 0.18 or breakout.volume_z >= 0.80):
            return "arming"
        if structure.score >= 0.40 or coin.score >= 0.35:
            return "monitor"
        return "cold"

    def _should_emit_pre_alert(
        self,
        level: LevelCandidate,
        coin: CoinState,
        structure: StructureState,
        breakout: BreakoutState,
        confidence: float,
    ) -> bool:
        return (
            confidence >= self.strategy.pre_alert_confidence_threshold
            and self.strategy.pre_alert_distance_atr_min <= structure.near_level_atr <= self.strategy.pre_alert_distance_atr_max
            and structure.squeeze_score >= self.strategy.pre_alert_squeeze_threshold
            and structure.cascade_touches >= self.strategy.min_touches
            and breakout.volume_z >= self.strategy.pre_alert_volume_z_threshold
            and coin.is_active
            and not breakout.correlation_headwind
            and not breakout.signal_ready
            and level.kind in {LevelKind.RESISTANCE, LevelKind.SUPPORT}
        )

    def _should_emit_watchlist(
        self,
        level: LevelCandidate,
        coin: CoinState,
        structure: StructureState,
        breakout: BreakoutState,
        confidence: float,
    ) -> bool:
        return (
            breakout.data_ok
            and confidence >= self.strategy.watchlist_confidence_threshold
            and breakout.breakout_distance_atr >= -0.22
            and structure.score >= 0.72
            and structure.cascade_touches >= self.strategy.min_touches
            and structure.squeeze_score >= self.strategy.squeeze_threshold
            and structure.near_level_atr <= self.strategy.dist_to_level_atr
            and coin.is_active
            and coin.score >= 0.58
            and breakout.volume_z >= self.strategy.watchlist_volume_z_threshold
            and breakout.watch_flow_support
            and not breakout.correlation_headwind
            and level.kind in {LevelKind.RESISTANCE, LevelKind.SUPPORT}
        )

    def _notes_for_report(
        self,
        health: DataHealth,
        trend: TrendState,
        coin: CoinState,
        structure: StructureState,
        breakout: BreakoutState,
        direction: Direction,
        level: LevelCandidate,
    ) -> list[str]:
        notes: list[str] = []
        if not health.is_fresh:
            notes.append("Данные устарели: сервис collector не успевает обновить market-state.")
        if health.has_sequence_gap:
            notes.append("Есть разрыв последовательности в стакане, сигнал не считается надежным.")
        if trend.bias is None:
            notes.append("HTF тренд смешанный, поэтому confidence снижен.")
        elif trend.bias != direction:
            notes.append("Пробой идет против доминирующего HTF тренда.")
        if not coin.is_active:
            notes.append("Монета пока не полностью в игре: импульс по объему и range еще умеренный.")
        if not structure.is_valid:
            notes.append(
                f"Наторговка перед уровнем слабая: {structure.consolidation_range_atr:.2f} ATR, squeeze {structure.squeeze_score:.2f}."
            )
        if breakout.breakout_distance_atr < 0.03:
            notes.append(f"Цена еще не закрылась достаточно далеко за уровень {level.source}.")
        if breakout.volume_z < 0.60:
            notes.append(f"Объем пробоя пока слабый: z-score {breakout.volume_z:.2f}.")
        if breakout.body_ratio < 0.42:
            notes.append(f"Тело breakout-свечи недостаточно сильное: body ratio {breakout.body_ratio:.2f}.")
        if breakout.close_to_extreme > 0.45:
            notes.append("Свеча закрылась не у экстремума и оставила слишком большой хвост.")
        if not breakout.follow_through_5m:
            notes.append("После пробоя нет clean follow-through на 5m.")
        if not breakout.cross_ok:
            notes.append("Кросс-биржевое подтверждение по secondary venue слабое.")
        if breakout.delta_divergence:
            notes.append("Delta / CVD diverge from price and raise false-breakout risk.")
        elif not breakout.aggressive_flow_support:
            notes.append(
                f"Aggressive flow does not confirm yet: delta ratio {breakout.delta_ratio:.2f}, CVD slope {breakout.cvd_slope:.2f}."
            )
        if not notes:
            notes.append("Сетап выглядит чисто и близок к actionable breakout scalp.")
        if breakout.tick_velocity_ratio < max(self.strategy.tick_velocity_alert_multiplier * 0.80, 1.15):
            notes.append(f"Tick velocity is still soft: {breakout.tick_velocity_ratio:.2f}x of the 10m baseline.")
        if breakout.correlation_headwind:
            notes.append("BTC/ETH benchmark flow is too strong for this alt short.")
        return notes

    def _classify(self, confidence: float) -> SignalClass:
        if confidence >= self.strategy.actionable_confidence_threshold:
            return SignalClass.ACTIONABLE
        if confidence >= self.strategy.watchlist_confidence_threshold:
            return SignalClass.WATCHLIST
        return SignalClass.SUPPRESSED
