from __future__ import annotations

from dataclasses import dataclass

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


@dataclass(slots=True)
class BreakoutState:
    hard_gate_ok: bool
    score: float
    breakout_distance_atr: float
    body_ratio: float
    close_to_extreme: float
    range_expansion: float
    follow_through_5m: bool
    book_imbalance: float
    data_ok: bool
    spread_ratio: float
    cross_ok: bool


class RuleEngine:
    score_weights = {
        "level_quality": 25.0,
        "trend_alignment": 20.0,
        "coin_in_play": 20.0,
        "structure_pressure": 20.0,
        "breakout_confirmation": 15.0,
    }

    def evaluate(self, payload: EngineInput) -> SignalDecision | None:
        if len(payload.candles_15m) < 25 or len(payload.candles_1h) < 60 or len(payload.candles_4h) < 20 or not payload.levels:
            return None

        current = payload.candles_15m[-1]
        atr_15m = average_true_range(payload.candles_15m[-30:] or payload.candles_15m, period=14)
        atr_1h = average_true_range(payload.candles_1h[-40:] or payload.candles_1h, period=14)
        if atr_15m <= 0 or atr_1h <= 0:
            return None

        trend = self._trend_state(payload.candles_1h, payload.candles_4h)
        if trend.bias is None:
            return None

        coin = self._coin_in_play(payload.candles_15m, payload.candles_1h)
        if not coin.is_active:
            return None

        candidates = self._candidate_levels(current, payload.levels, atr_15m, trend.bias)
        decisions: list[SignalDecision] = []
        for level in candidates:
            direction = self._breakout_direction(current, level, atr_15m, trend.bias)
            if direction is None:
                continue

            structure = self._structure_state(payload.candles_15m, payload.candles_1h, level, atr_15m, direction)
            if not structure.is_valid:
                continue

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
            if not breakout.hard_gate_ok:
                continue

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
            )
            if decision is not None:
                decisions.append(decision)

        if not decisions:
            return None
        return max(decisions, key=lambda item: item.confidence)

    def _candidate_levels(
        self,
        candle: MarketCandle,
        levels: list[LevelCandidate],
        atr_15m: float,
        trend_bias: Direction,
    ) -> list[LevelCandidate]:
        selected: list[LevelCandidate] = []
        for level in levels:
            if trend_bias == Direction.LONG and level.kind != LevelKind.RESISTANCE:
                continue
            if trend_bias == Direction.SHORT and level.kind != LevelKind.SUPPORT:
                continue
            distance = min(abs(candle.close - level.lower_price), abs(candle.close - level.upper_price))
            if distance <= atr_15m * 1.4 or self._is_breakout(candle, level, atr_15m):
                selected.append(level)
        return sorted(selected, key=lambda item: (-item.strength, item.detected_at))

    def _is_breakout(self, candle: MarketCandle, level: LevelCandidate, atr_15m: float) -> bool:
        threshold = 0.10 * atr_15m
        if level.kind == LevelKind.RESISTANCE:
            return candle.close > level.upper_price + threshold
        return candle.close < level.lower_price - threshold

    def _breakout_direction(
        self,
        candle: MarketCandle,
        level: LevelCandidate,
        atr_15m: float,
        trend_bias: Direction,
    ) -> Direction | None:
        if level.kind == LevelKind.RESISTANCE and candle.close > level.upper_price + atr_15m * 0.10 and trend_bias == Direction.LONG:
            return Direction.LONG
        if level.kind == LevelKind.SUPPORT and candle.close < level.lower_price - atr_15m * 0.10 and trend_bias == Direction.SHORT:
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

        if fast_1h > slow_1h and fast_4h >= slow_4h and slope_1h > 0 and slope_4h >= 0:
            score = min(1.0, ((fast_1h - slow_1h) / max(abs(slow_1h), 1e-9)) * 60 + 0.55)
            return TrendState(Direction.LONG, score, fast_1h, slow_1h, fast_4h, slow_4h)
        if fast_1h < slow_1h and fast_4h <= slow_4h and slope_1h < 0 and slope_4h <= 0:
            score = min(1.0, ((slow_1h - fast_1h) / max(abs(slow_1h), 1e-9)) * 60 + 0.55)
            return TrendState(Direction.SHORT, score, fast_1h, slow_1h, fast_4h, slow_4h)
        return TrendState(None, 0.0, fast_1h, slow_1h, fast_4h, slow_4h)

    def _coin_in_play(self, candles_15m: list[MarketCandle], candles_1h: list[MarketCandle]) -> CoinState:
        current_15m = candles_15m[-1]
        ranges = [item.high - item.low for item in candles_15m[-21:-1]]
        current_range = max(current_15m.high - current_15m.low, 1e-9)
        range_expansion = current_range / max(median_spread(ranges), 1e-9)
        volume_z_15m = volume_zscore(candles_15m[-25:], period=20)
        volume_z_1h = volume_zscore(candles_1h[-25:], period=20)
        quote_baseline = [item.quote_volume for item in candles_15m[-13:-1]]
        quote_activity_ratio = current_15m.quote_volume / max(simple_moving_average(quote_baseline, len(quote_baseline)), 1e-9)
        score = min(
            1.0,
            max(volume_z_15m, 0.0) / 3.0 * 0.45
            + min(range_expansion / 1.8, 1.0) * 0.35
            + min(max(quote_activity_ratio - 1.0, 0.0) / 1.5, 1.0) * 0.20,
        )
        is_active = volume_z_15m >= 1.8 and range_expansion >= 1.2 and quote_activity_ratio >= 1.2
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
            return StructureState(False, 0.0, 0, 99.0, 0.0, level.reference_price, pre_break_window)

        highs = [item.high for item in pre_break_window]
        lows = [item.low for item in pre_break_window]
        closes = [item.close for item in pre_break_window]
        consolidation_range_atr = (max(highs) - min(lows)) / max(atr_15m, 1e-9)
        cascade_touches = self._cascade_touches(candles_1h, level, atr_15m)
        squeeze_score = self._squeeze_score(pre_break_window, direction)

        if direction == Direction.LONG:
            max_high = max(highs)
            near_level = (level.upper_price - max_high) / max(atr_15m, 1e-9)
            contained = all(item.close <= level.upper_price + atr_15m * 0.05 for item in pre_break_window)
            anchor_price = min(lows)
            valid = contained and -0.10 <= near_level <= 0.45 and consolidation_range_atr <= 2.5 and squeeze_score >= 0.55
        else:
            min_low = min(lows)
            near_level = (min_low - level.lower_price) / max(atr_15m, 1e-9)
            contained = all(item.close >= level.lower_price - atr_15m * 0.05 for item in pre_break_window)
            anchor_price = max(highs)
            valid = contained and -0.10 <= near_level <= 0.45 and consolidation_range_atr <= 2.5 and squeeze_score >= 0.55

        source_bonus = 0.15 if level.source.startswith("cascade") else 0.10 if level.source.startswith("prev-") else 0.0
        touch_bonus = min(cascade_touches / 4.0, 1.0) * 0.40
        compression_bonus = min(max(2.0 - consolidation_range_atr, 0.0) / 2.0, 1.0) * 0.25
        squeeze_bonus = min(squeeze_score, 1.0) * 0.35
        score = min(1.0, source_bonus + touch_bonus + compression_bonus + squeeze_bonus)
        if cascade_touches < 2 and not level.source.startswith("prev-"):
            valid = False

        return StructureState(valid, score, cascade_touches, consolidation_range_atr, squeeze_score, anchor_price, pre_break_window)

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
        cross_ok = sum(1 for item in cross_venue_health if item.is_fresh) >= 2
        data_ok = health.is_fresh and not health.has_sequence_gap and spread_ratio <= 3.0

        if direction == Direction.LONG:
            breakout_distance_atr = (current.close - level.upper_price) / max(atr_15m, 1e-9)
            close_to_extreme = (current.high - current.close) / candle_range
        else:
            breakout_distance_atr = (level.lower_price - current.close) / max(atr_15m, 1e-9)
            close_to_extreme = (current.close - current.low) / candle_range

        follow_through_5m = self._follow_through_5m(candles_5m[-3:], level, direction)
        book_ok = book_imbalance >= -0.10 if direction == Direction.LONG else book_imbalance <= 0.10
        hard_gate_ok = (
            data_ok
            and volume_z >= 1.8
            and range_expansion >= 1.2
            and breakout_distance_atr >= 0.10
            and body_ratio >= 0.55
            and close_to_extreme <= 0.25
            and follow_through_5m
            and book_ok
        )
        score = min(
            1.0,
            min(max(breakout_distance_atr, 0.0) / 0.35, 1.0) * 0.35
            + min(body_ratio / 0.75, 1.0) * 0.25
            + min(max(volume_z, 0.0) / 3.0, 1.0) * 0.20
            + min(range_expansion / 1.8, 1.0) * 0.10
            + (0.10 if follow_through_5m else 0.0),
        )
        return BreakoutState(
            hard_gate_ok=hard_gate_ok,
            score=score,
            breakout_distance_atr=breakout_distance_atr,
            body_ratio=body_ratio,
            close_to_extreme=close_to_extreme,
            range_expansion=range_expansion,
            follow_through_5m=follow_through_5m,
            book_imbalance=book_imbalance,
            data_ok=data_ok,
            spread_ratio=spread_ratio,
            cross_ok=cross_ok,
        )

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
    ) -> SignalDecision | None:
        derivatives = payload.derivative_context

        group_scores = {
            "level_quality": self.score_weights["level_quality"] * min(level.strength + min(structure.cascade_touches, 4) * 0.08, 1.0),
            "trend_alignment": self.score_weights["trend_alignment"] * trend.score,
            "coin_in_play": self.score_weights["coin_in_play"] * coin.score,
            "structure_pressure": self.score_weights["structure_pressure"] * structure.score,
            "breakout_confirmation": self.score_weights["breakout_confirmation"] * breakout.score,
        }
        contributions = [
            ScoreContribution(
                group=name,
                score=score,
                max_score=self.score_weights[name],
                reason=self._reason_for_group(name, level, trend, coin, structure, breakout),
            )
            for name, score in group_scores.items()
        ]
        confidence = sum(item.score for item in contributions)
        signal_class = self._classify(confidence)
        if signal_class == SignalClass.SUPPRESSED:
            return None

        entry_price = current.close
        if direction == Direction.LONG:
            invalidation = min(structure.anchor_price - atr_15m * 0.08, level.lower_price - atr_15m * 0.12)
        else:
            invalidation = max(structure.anchor_price + atr_15m * 0.08, level.upper_price + atr_15m * 0.12)
        risk = max(abs(entry_price - invalidation), atr_15m * 0.20)
        t1 = entry_price + (risk * 1.2 if direction == Direction.LONG else -risk * 1.2)
        t2 = entry_price + (risk * 2.0 if direction == Direction.LONG else -risk * 2.0)
        expected_rr = abs((t1 - entry_price) / risk)
        alert_key = f"{payload.symbol}:breakout:{direction.value}:{level.level_id}:{Timeframe.M15.value}:{current.close_time.isoformat()}"

        why_not_higher: list[str] = []
        if structure.cascade_touches < 3:
            why_not_higher.append(f"Каскад пока неглубокий: только {structure.cascade_touches} касания.")
        if structure.squeeze_score < 0.75:
            why_not_higher.append(f"Поджатие есть, но не максимальное: score {structure.squeeze_score:.2f}.")
        if coin.volume_z_15m < 2.5:
            why_not_higher.append(f"Объем высокий, но не экстремальный: {coin.volume_z_15m:.2f} z-score.")
        if abs(breakout.book_imbalance) < 0.10:
            why_not_higher.append("Стакан подтверждает импульс умеренно.")
        if not breakout.cross_ok:
            why_not_higher.append("Кросс-биржевое подтверждение слабее желаемого.")
        if not why_not_higher:
            why_not_higher.append("Импульс сильный, но confidence ограничен до накопления большей live-статистики.")

        rationale = [
            f"Монета в игре: volume z-score {coin.volume_z_15m:.2f}, quote-volume x{coin.quote_activity_ratio:.2f} к среднему.",
            f"Тренд 1h/4h: {trend.bias.value.upper()} | fast/slow 1h {trend.fast_1h:.2f}/{trend.slow_1h:.2f}.",
            f"Каскад уровней: {structure.cascade_touches} касания | HTF источник {level.timeframe.value} {level.source}.",
            f"Наторговка под уровнем: диапазон {structure.consolidation_range_atr:.2f} ATR.",
            f"Поджатие к уровню: squeeze score {structure.squeeze_score:.2f}.",
            f"Подтверждение пробоя: {breakout.breakout_distance_atr:.2f} ATR за уровнем, range x{breakout.range_expansion:.2f}, 5m follow-through {'да' if breakout.follow_through_5m else 'нет'}.",
        ]
        if derivatives is not None:
            rationale.append(
                f"Контекст derivatives: funding {derivatives.funding_rate:.5f}, basis {derivatives.basis_bps:.1f} bps, OI {derivatives.open_interest:.2f}."
            )
        else:
            rationale.append("Контекст derivatives: н/д.")

        health = payload.health.model_copy(update={"spread_ratio": breakout.spread_ratio})
        render_context = {
            "price_zone": level.zone_text,
            "htf_source": f"{level.timeframe.value} {level.source}",
            "trigger": self._trigger_text(direction, current, level, breakout.breakout_distance_atr),
            "stop_logic": self._stop_logic(direction, invalidation, structure.anchor_price, level),
            "cancel_if": "импульс погас и 15m вернулась под уровень / данные устарели / спред расширился",
            "venues_used": ", ".join(sorted({payload.venue.value, *[item.venue.value for item in payload.cross_venue_health]})),
            "level_lower": level.lower_price,
            "level_upper": level.upper_price,
            "entry_price": entry_price,
            "stop_price": invalidation,
            "tp1": round(t1, 6),
            "tp2": round(t2, 6),
            "chart_timeframe": Timeframe.M15.value,
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
            },
            render_context=render_context,
        )

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
        trend: TrendState,
        coin: CoinState,
        structure: StructureState,
        breakout: BreakoutState,
    ) -> str:
        if name == "level_quality":
            return f"HTF уровень {level.source} и {structure.cascade_touches} касания около цены пробоя."
        if name == "trend_alignment":
            return f"Тренд {trend.bias.value.upper()} подтвержден на 1h и 4h."
        if name == "coin_in_play":
            return f"Volume z-score {coin.volume_z_15m:.2f}, quote-volume x{coin.quote_activity_ratio:.2f}."
        if name == "structure_pressure":
            return f"Наторговка {structure.consolidation_range_atr:.2f} ATR и squeeze score {structure.squeeze_score:.2f}."
        return f"Пробой на {breakout.breakout_distance_atr:.2f} ATR с follow-through {'да' if breakout.follow_through_5m else 'нет'}."

    def _trigger_text(
        self,
        direction: Direction,
        candle: MarketCandle,
        level: LevelCandidate,
        breakout_distance_atr: float,
    ) -> str:
        if direction == Direction.LONG:
            return f"15m закрылась выше каскада сопротивлений на {breakout_distance_atr:.2f} ATR ({candle.close:.4f})."
        return f"15m закрылась ниже каскада поддержек на {breakout_distance_atr:.2f} ATR ({candle.close:.4f})."

    def _stop_logic(
        self,
        direction: Direction,
        invalidation: float,
        anchor_price: float,
        level: LevelCandidate,
    ) -> str:
        if direction == Direction.LONG:
            return f"SL под наторговкой и ниже зоны ({invalidation:.4f}); локальный anchor {anchor_price:.4f}, уровень {level.lower_price:.4f}."
        return f"SL над наторговкой и выше зоны ({invalidation:.4f}); локальный anchor {anchor_price:.4f}, уровень {level.upper_price:.4f}."

    def _classify(self, confidence: float) -> SignalClass:
        if confidence >= 80:
            return SignalClass.ACTIONABLE
        if confidence >= 65:
            return SignalClass.WATCHLIST
        return SignalClass.SUPPRESSED
