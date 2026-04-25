from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from brakerscalp.domain.models import (
    BookSnapshot,
    DataHealth,
    DerivativeContext,
    Direction,
    LevelCandidate,
    LevelKind,
    MarketCandle,
    ScoreContribution,
    SetupType,
    SignalClass,
    SignalDecision,
    Timeframe,
    Venue,
)
from brakerscalp.signals.indicators import average_true_range, median_spread, simple_moving_average, volume_zscore


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

    def evaluate(self, payload: EngineInput) -> SignalDecision | None:
        return self.inspect(payload).decision

    def inspect(self, payload: EngineInput) -> ScreeningResult:
        if len(payload.candles_15m) < 25 or len(payload.candles_1h) < 60 or len(payload.candles_4h) < 20 or not payload.levels:
            return self._empty_result(
                payload=payload,
                status="insufficient",
                notes=["Недостаточно истории свечей или уровней для оценки сетапа."],
            )

        current = payload.candles_15m[-1]
        atr_15m = average_true_range(payload.candles_15m[-30:] or payload.candles_15m, period=14)
        atr_1h = average_true_range(payload.candles_1h[-40:] or payload.candles_1h, period=14)
        if atr_15m <= 0 or atr_1h <= 0:
            return self._empty_result(
                payload=payload,
                status="insufficient",
                notes=["ATR не рассчитался, сетап пропущен."],
            )

        trend = self._trend_state(payload.candles_1h, payload.candles_4h)
        coin = self._coin_in_play(payload.candles_15m, payload.candles_1h)
        candidates = self._candidate_levels(current, payload.levels, atr_15m)
        if not candidates:
            return self._empty_result(
                payload=payload,
                status="cold",
                atr_15m=atr_15m,
                trend=trend,
                coin=coin,
                notes=["Рядом с текущей ценой нет рабочего HTF уровня для breakout scalp."],
            )

        best_bundle: tuple[float, LevelCandidate, Direction, StructureState, BreakoutState, float, list[ScoreContribution], SignalDecision | None] | None = None
        for level in candidates[:8]:
            direction = self._breakout_direction(current, level, atr_15m) or self._default_direction(level)
            structure = self._structure_state(payload.candles_15m, payload.candles_1h, level, atr_15m, direction)
            breakout = self._breakout_state(
                current=current,
                candles_15m=payload.candles_15m,
                candles_5m=payload.candles_5m,
                level=level,
                atr_15m=atr_15m,
                direction=direction,
                book=payload.book,
                health=payload.health,
                cross_venue_health=payload.cross_venue_health,
            )
            confidence, contributions = self._score_candidate(level, direction, trend, coin, structure, breakout)
            decision = None
            if breakout.signal_ready:
                decision = self._build_decision(
                    payload=payload,
                    level=level,
                    current=current,
                    atr_15m=atr_15m,
                    direction=direction,
                    trend=trend,
                    coin=coin,
                    structure=structure,
                    breakout=breakout,
                    confidence=confidence,
                    contributions=contributions,
                    confirmed_breakout=True,
                )
            elif self._should_emit_watchlist(level, coin, structure, breakout, confidence):
                decision = self._build_decision(
                    payload=payload,
                    level=level,
                    current=current,
                    atr_15m=atr_15m,
                    direction=direction,
                    trend=trend,
                    coin=coin,
                    structure=structure,
                    breakout=breakout,
                    confidence=confidence,
                    contributions=contributions,
                    confirmed_breakout=False,
                )
            priority = confidence + (6.0 if breakout.signal_ready else 0.0) + (4.0 if structure.is_valid else 0.0)
            if best_bundle is None or priority > best_bundle[0]:
                best_bundle = (priority, level, direction, structure, breakout, confidence, contributions, decision)

        if best_bundle is None:
            return self._empty_result(
                payload=payload,
                status="cold",
                atr_15m=atr_15m,
                trend=trend,
                coin=coin,
                notes=["Подходящий уровень найден, но он не прошел внутренний ранжир."],
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
        current = payload.candles_15m[-1] if payload.candles_15m else None
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
            freshness_ms=payload.health.freshness_ms,
            spread_ratio=payload.health.spread_ratio,
            notes=notes,
            updated_at=current.close_time if current else datetime.now(tz=timezone.utc),
        )

    def _candidate_levels(self, candle: MarketCandle, levels: list[LevelCandidate], atr_15m: float) -> list[LevelCandidate]:
        selected: list[LevelCandidate] = []
        for level in levels:
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

        valid = contained and -0.35 <= near_level_atr <= 0.90 and consolidation_range_atr <= 3.4 and squeeze_score >= 0.33
        if level.source.startswith("cascade") and max(cascade_touches, level.touches) < 2:
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
        candles_15m: list[MarketCandle],
        candles_5m: list[MarketCandle],
        level: LevelCandidate,
        atr_15m: float,
        direction: Direction,
        book: BookSnapshot | None,
        health: DataHealth,
        cross_venue_health: list[DataHealth],
    ) -> BreakoutState:
        candle_range = max(current.high - current.low, 1e-9)
        body_ratio = abs(current.close - current.open) / candle_range
        recent_ranges = [item.high - item.low for item in candles_15m[-21:-1]]
        range_expansion = candle_range / max(median_spread(recent_ranges), 1e-9)
        volume_z = volume_zscore(candles_15m[-25:], period=20)
        book_imbalance = self._book_imbalance(book)
        spread_ratio = health.spread_ratio
        fresh_cross = [item for item in cross_venue_health if item.venue != health.venue and item.is_fresh]
        cross_ok = True if not cross_venue_health else len(fresh_cross) >= 1
        data_ok = health.is_fresh and not health.has_sequence_gap and spread_ratio <= 5.0

        if direction == Direction.LONG:
            breakout_distance_atr = (current.close - level.upper_price) / max(atr_15m, 1e-9)
            close_to_extreme = (current.high - current.close) / candle_range
            book_support = book_imbalance >= -0.22
        else:
            breakout_distance_atr = (level.lower_price - current.close) / max(atr_15m, 1e-9)
            close_to_extreme = (current.close - current.low) / candle_range
            book_support = book_imbalance <= 0.22

        follow_through_5m = self._follow_through_5m(candles_5m[-3:], level, direction)
        signal_ready = (
            data_ok
            and breakout_distance_atr >= 0.03
            and body_ratio >= 0.42
            and close_to_extreme <= 0.45
            and volume_z >= 0.60
            and range_expansion >= 0.95
            and (follow_through_5m or breakout_distance_atr >= 0.12 or book_support)
        )
        score = min(
            1.0,
            min(max(breakout_distance_atr, 0.0) / 0.28, 1.0) * 0.28
            + min(body_ratio / 0.70, 1.0) * 0.22
            + min(max(volume_z, 0.0) / 2.5, 1.0) * 0.18
            + min(range_expansion / 1.6, 1.0) * 0.14
            + (0.10 if follow_through_5m else 0.0)
            + (0.08 if book_support else 0.0),
        )
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
        level_factor = min(level.strength + min(max(structure.cascade_touches, level.touches), 4) * 0.06, 1.0)
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
        confirmed_breakout: bool,
    ) -> SignalDecision | None:
        signal_class = self._classify(confidence) if confirmed_breakout else SignalClass.WATCHLIST
        if confirmed_breakout and signal_class == SignalClass.SUPPRESSED:
            return None
        if not confirmed_breakout and confidence < 64:
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
        risk = max(abs(entry_price - invalidation), atr_15m * 0.22)
        targets = self._project_targets(payload.levels, level, direction, entry_price, risk)
        if targets is None:
            return None
        t1, t2 = targets
        expected_rr = abs((t1 - entry_price) / risk)
        if expected_rr < self.minimum_expected_rr:
            return None
        stage = "confirmed" if confirmed_breakout else "arming"
        alert_key = f"{payload.symbol}:breakout:{direction.value}:{level.level_id}:{Timeframe.M15.value}:{current.close_time.isoformat()}:{stage}"

        why_not_higher: list[str] = []
        if not confirmed_breakout:
            why_not_higher.append("Пробой еще не подтвержден закрытием 15m за уровнем.")
        if trend.bias != direction:
            why_not_higher.append("Текущий HTF тренд не полностью синхронизирован с направлением пробоя.")
        if structure.cascade_touches < 3:
            why_not_higher.append(f"Каскад еще неглубокий: подтверждено только {structure.cascade_touches} касания.")
        if structure.squeeze_score < 0.70:
            why_not_higher.append(f"Поджатие есть, но не максимальное: squeeze score {structure.squeeze_score:.2f}.")
        if coin.volume_z_15m < 1.40:
            why_not_higher.append(f"Объем выше фона, но без экстремума: z-score {coin.volume_z_15m:.2f}.")
        if not breakout.follow_through_5m:
            why_not_higher.append("Нет уверенного follow-through на 5m после закрытия breakout-свечи.")
        if not breakout.cross_ok:
            why_not_higher.append("Кросс-биржевое подтверждение слабее, чем хотелось бы.")
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
                else f"Цена прижата к уровню: dist {breakout.breakout_distance_atr:.2f} ATR, volume z {breakout.volume_z:.2f}, ждем 15m close за зоной."
            ),
        ]
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
            "trigger": self._trigger_text(direction, current, level, breakout.breakout_distance_atr, confirmed_breakout, entry_price),
            "stop_logic": self._stop_logic(direction, invalidation, structure.anchor_price, level),
            "cancel_if": (
                "15m вернулась в диапазон под уровень / данные устарели / спред резко расширился"
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
            "chart_timeframe": Timeframe.M15.value,
            "setup_stage": stage,
            "trend_bias": trend.bias.value if trend.bias else None,
            "cascade_touches": structure.cascade_touches,
            "consolidation_range_atr": structure.consolidation_range_atr,
            "squeeze_score": structure.squeeze_score,
        }
        return SignalDecision(
            symbol=payload.symbol,
            venue=payload.venue,
            timeframe=Timeframe.M15,
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

    def _follow_through_5m(self, candles_5m: list[MarketCandle], level: LevelCandidate, direction: Direction) -> bool:
        if len(candles_5m) < 2:
            return False
        if direction == Direction.LONG:
            return all(item.close >= level.upper_price for item in candles_5m[-2:])
        return all(item.close <= level.lower_price for item in candles_5m[-2:])

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
        return f"Пробой на {breakout.breakout_distance_atr:.2f} ATR, volume z {breakout.volume_z:.2f}, follow-through {'да' if breakout.follow_through_5m else 'нет'}."

    def _trigger_text(
        self,
        direction: Direction,
        candle: MarketCandle,
        level: LevelCandidate,
        breakout_distance_atr: float,
        confirmed_breakout: bool,
        entry_price: float,
    ) -> str:
        if direction == Direction.LONG and confirmed_breakout:
            return f"15m закрылась выше зоны сопротивления на {breakout_distance_atr:.2f} ATR ({candle.close:.4f})."
        if direction == Direction.SHORT and confirmed_breakout:
            return f"15m закрылась ниже зоны поддержки на {breakout_distance_atr:.2f} ATR ({candle.close:.4f})."
        if direction == Direction.LONG:
            return f"Цена стоит под сопротивлением. Для входа нужен 15m close выше {entry_price:.4f}."
        return f"Цена стоит над поддержкой. Для входа нужен 15m close ниже {entry_price:.4f}."

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
            and confidence >= 68
            and breakout.breakout_distance_atr >= -0.55
            and structure.score >= 0.48
            and structure.near_level_atr <= 0.95
            and (coin.is_active or coin.score >= 0.15 or breakout.volume_z >= 0.65)
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
        if not notes:
            notes.append("Сетап выглядит чисто и близок к actionable breakout scalp.")
        return notes

    def _classify(self, confidence: float) -> SignalClass:
        if confidence >= 74:
            return SignalClass.ACTIONABLE
        if confidence >= 58:
            return SignalClass.WATCHLIST
        return SignalClass.SUPPRESSED
