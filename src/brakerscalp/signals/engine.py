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
from brakerscalp.signals.indicators import average_true_range, median_spread, volume_zscore


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


class RuleEngine:
    breakout_weights = {
        "level_quality": 25.0,
        "price_confirmation": 25.0,
        "participation": 20.0,
        "microstructure": 15.0,
        "derivatives": 15.0,
    }

    def evaluate(self, payload: EngineInput) -> SignalDecision | None:
        if not payload.candles_15m or not payload.candles_1h or not payload.levels:
            return None

        current = payload.candles_15m[-1]
        atr = average_true_range(payload.candles_15m[-30:] or payload.candles_15m, period=14)
        if atr <= 0:
            return None

        candidates = self._candidate_levels(current, payload.levels, atr)
        if not candidates:
            return None

        decisions: list[SignalDecision] = []
        for level in candidates:
            setup, direction = self._select_setup(current, level, atr, payload.book)
            if setup is None or direction is None:
                continue
            decisions.append(self._build_decision(payload, level, current, atr, setup, direction))

        if not decisions:
            return None
        return max(decisions, key=lambda item: item.confidence)

    def _nearest_level(self, price: float, levels: list[LevelCandidate]) -> LevelCandidate | None:
        return min(levels, key=lambda item: min(abs(price - item.lower_price), abs(price - item.upper_price)), default=None)

    def _candidate_levels(self, candle: MarketCandle, levels: list[LevelCandidate], atr: float) -> list[LevelCandidate]:
        candidates: list[LevelCandidate] = []
        for level in levels:
            distance_to_level = min(abs(candle.close - level.lower_price), abs(candle.close - level.upper_price))
            if distance_to_level <= atr * 0.3:
                candidates.append(level)
                continue
            if self._is_breakout(candle, level, atr):
                candidates.append(level)
                continue
            inside_zone = level.lower_price <= candle.close <= level.upper_price
            pierced = candle.low < level.lower_price or candle.high > level.upper_price
            if pierced and inside_zone:
                candidates.append(level)
        if candidates:
            return candidates
        nearest = self._nearest_level(candle.close, levels)
        return [nearest] if nearest else []

    def _is_breakout(self, candle: MarketCandle, level: LevelCandidate, atr: float) -> bool:
        threshold = 0.15 * atr
        return (
            (level.kind == LevelKind.RESISTANCE and candle.close > level.upper_price + threshold)
            or (level.kind == LevelKind.SUPPORT and candle.close < level.lower_price - threshold)
        )

    def _select_setup(
        self,
        candle: MarketCandle,
        level: LevelCandidate,
        atr: float,
        book: BookSnapshot | None,
    ) -> tuple[SetupType | None, Direction | None]:
        wick = (candle.high - max(candle.open, candle.close)) if candle.close >= candle.open else (min(candle.open, candle.close) - candle.low)
        body = abs(candle.close - candle.open) or 1e-9
        wick_body_ratio = wick / body
        absorption = self._book_absorption(book, level)
        if self._is_breakout(candle, level, atr):
            direction = Direction.LONG if candle.close > level.reference_price else Direction.SHORT
            return SetupType.BREAKOUT, direction
        inside_zone = level.lower_price <= candle.close <= level.upper_price
        pierced = candle.low < level.lower_price or candle.high > level.upper_price
        if pierced and inside_zone and wick_body_ratio >= 1.5 and absorption:
            direction = Direction.LONG if candle.close >= level.reference_price else Direction.SHORT
            return SetupType.BOUNCE, direction
        return None, None

    def _book_absorption(self, book: BookSnapshot | None, level: LevelCandidate) -> bool:
        if book is None or not book.bids or not book.asks:
            return False
        bid_liquidity = sum(item.size for item in book.bids[:5])
        ask_liquidity = sum(item.size for item in book.asks[:5])
        if level.kind == LevelKind.SUPPORT:
            return bid_liquidity >= ask_liquidity
        return ask_liquidity >= bid_liquidity

    def _build_decision(
        self,
        payload: EngineInput,
        level: LevelCandidate,
        current: MarketCandle,
        atr: float,
        setup: SetupType,
        direction: Direction,
    ) -> SignalDecision:
        book = payload.book
        derivatives = payload.derivative_context
        book_spread = book.spread if book else 0.0
        median_recent_spread = median_spread([book_spread, book_spread * 0.9, book_spread * 1.1]) or 1e-9
        spread_ratio = book_spread / median_recent_spread if median_recent_spread else 99.0
        volume_score_raw = volume_zscore(payload.candles_15m[-25:], period=20)
        volume_gate_ok = volume_score_raw >= 1.5
        data_ok = payload.health.is_fresh and not payload.health.has_sequence_gap and payload.health.spread_ratio <= 3
        cross_ok = sum(1 for item in payload.cross_venue_health if item.is_fresh) >= 2

        if setup == SetupType.BREAKOUT:
            hard_gate_ok = self._is_breakout(current, level, atr) and volume_gate_ok and data_ok
        else:
            wick = max(current.high - max(current.open, current.close), min(current.open, current.close) - current.low)
            body = abs(current.close - current.open) or 1e-9
            hard_gate_ok = wick / body >= 1.5 and data_ok and self._book_absorption(book, level)

        group_scores = {
            "level_quality": min(self.breakout_weights["level_quality"], self.breakout_weights["level_quality"] * min(level.strength + 0.2, 1.0)),
            "price_confirmation": min(self.breakout_weights["price_confirmation"], self.breakout_weights["price_confirmation"] * (1.0 if hard_gate_ok else 0.4)),
            "participation": min(self.breakout_weights["participation"], self.breakout_weights["participation"] * min(max(volume_score_raw, 0.0) / 3.0, 1.0)),
            "microstructure": min(self.breakout_weights["microstructure"], self.breakout_weights["microstructure"] * (1.0 if book and spread_ratio <= 3 else 0.3)),
            "derivatives": min(
                self.breakout_weights["derivatives"],
                self.breakout_weights["derivatives"] * self._derivatives_alignment(direction, derivatives, cross_ok),
            ),
        }

        contributions = [
            ScoreContribution(group=name, score=score, max_score=self.breakout_weights[name], reason=self._reason_for_group(name, score, setup, volume_score_raw, payload.health))
            for name, score in group_scores.items()
        ]
        confidence = sum(item.score for item in contributions)
        signal_class = self._classify(confidence)
        if not hard_gate_ok:
            signal_class = SignalClass.SUPPRESSED
            confidence = min(confidence, 64)

        entry_price = current.close
        invalidation = (level.lower_price - atr * 0.2) if direction == Direction.LONG else (level.upper_price + atr * 0.2)
        risk = abs(entry_price - invalidation) or atr * 0.1
        t1 = entry_price + (risk * 2 if direction == Direction.LONG else -risk * 2)
        t2 = entry_price + (risk * 3 if direction == Direction.LONG else -risk * 3)
        expected_rr = abs((t1 - entry_price) / risk)
        alert_key = f"{payload.symbol}:{setup.value}:{direction.value}:{level.level_id}:{Timeframe.M15.value}:{current.close_time.isoformat()}"

        why_not_higher = []
        if volume_score_raw < 2.0:
            why_not_higher.append(f"Подтверждение по объему пока умеренное ({volume_score_raw:.2f} z-score).")
        if derivatives and abs(derivatives.funding_rate) > 0.0008:
            why_not_higher.append(f"Фандинг выглядит перегретым: {derivatives.funding_rate:.5f}.")
        if not cross_ok:
            why_not_higher.append("Кросс-биржевое подтверждение слабее желаемого.")
        if not why_not_higher:
            why_not_higher.append("Сигнал сильный, но confidence пока ограничен, пока не накоплена большая live-статистика.")

        rationale = [
            f"Z-score объема: {volume_score_raw:.2f}",
            f"Дисбаланс стакана: {self._book_imbalance(book):.2f}" if book else "Дисбаланс стакана: н/д",
            f"Прокси изменения OI: {derivatives.open_interest:.2f}" if derivatives else "Прокси изменения OI: н/д",
            f"Контекст funding / basis: {self._derivatives_summary(derivatives)}",
            f"Подтверждение ретеста / отскока: {'да' if setup == SetupType.BOUNCE else 'нет'}",
        ]
        health = payload.health.model_copy(update={"spread_ratio": spread_ratio})
        render_context = {
            "price_zone": level.zone_text,
            "htf_source": f"{level.timeframe.value} {level.source}",
            "trigger": self._trigger_text(setup, direction, current, level, atr),
            "stop_logic": f"{'ниже' if direction == Direction.LONG else 'выше'} уровня с буфером 0.2 ATR",
            "cancel_if": "две свечи закрылись обратно в зоне / устаревшие данные / всплеск спреда",
            "venues_used": ", ".join(sorted({payload.venue.value, *[item.venue.value for item in payload.cross_venue_health]})),
        }
        return SignalDecision(
            symbol=payload.symbol,
            venue=payload.venue,
            timeframe=Timeframe.M15,
            setup=setup,
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
                "atr": atr,
                "volume_zscore": volume_score_raw,
                "spread_ratio": spread_ratio,
                "funding_rate": derivatives.funding_rate if derivatives else 0.0,
                "open_interest": derivatives.open_interest if derivatives else 0.0,
                "level_strength": level.strength,
                "cross_venue_health_count": sum(1 for item in payload.cross_venue_health if item.is_fresh),
                "setup": setup.value,
            },
            render_context=render_context,
        )

    def _reason_for_group(self, name: str, score: float, setup: SetupType, volume_score: float, health: DataHealth) -> str:
        if name == "level_quality":
            return "HTF level overlaps with structural highs/lows and zone-based storage."
        if name == "price_confirmation":
            return "15m candle confirms breakout beyond the zone." if setup == SetupType.BREAKOUT else "Candle rejected the zone with a strong wick."
        if name == "participation":
            return f"Participation confirmed by volume z-score {volume_score:.2f}."
        if name == "microstructure":
            return f"Order book remains healthy with spread ratio {health.spread_ratio:.2f}."
        return "Derivatives context and cross-venue sanity-check are aligned."

    def _derivatives_alignment(self, direction: Direction, derivatives: DerivativeContext | None, cross_ok: bool) -> float:
        if derivatives is None:
            return 0.4 if cross_ok else 0.2
        basis_alignment = 1.0
        if direction == Direction.LONG and derivatives.basis_bps < -5:
            basis_alignment = 0.5
        if direction == Direction.SHORT and derivatives.basis_bps > 5:
            basis_alignment = 0.5
        funding_penalty = 0.8 if abs(derivatives.funding_rate) > 0.0008 else 1.0
        return max(0.0, min(1.0, basis_alignment * funding_penalty * (1.0 if cross_ok else 0.7)))

    def _book_imbalance(self, book: BookSnapshot | None) -> float:
        if book is None:
            return 0.0
        bid = sum(item.size for item in book.bids[:5])
        ask = sum(item.size for item in book.asks[:5])
        total = bid + ask
        return ((bid - ask) / total) if total else 0.0

    def _derivatives_summary(self, derivatives: DerivativeContext | None) -> str:
        if derivatives is None:
            return "н/д"
        return f"funding {derivatives.funding_rate:.5f}, basis {derivatives.basis_bps:.1f} bps"

    def _trigger_text(self, setup: SetupType, direction: Direction, candle: MarketCandle, level: LevelCandidate, atr: float) -> str:
        if setup == SetupType.BREAKOUT:
            side = "выше сопротивления" if direction == Direction.LONG else "ниже поддержки"
            return f"Закрытие 15m на {0.15:.2f} ATR {side} ({candle.close:.4f})"
        return f"Тенью сняли {level.zone_text} и закрылись обратно в зону ({candle.close:.4f})"

    def _classify(self, confidence: float) -> SignalClass:
        if confidence >= 80:
            return SignalClass.ACTIONABLE
        if confidence >= 65:
            return SignalClass.WATCHLIST
        return SignalClass.SUPPRESSED
