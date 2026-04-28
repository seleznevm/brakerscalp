from __future__ import annotations

from datetime import datetime, timezone

from brakerscalp.domain.models import LevelCandidate, LevelKind, Timeframe, Venue
from brakerscalp.signals.engine import EngineInput, RuleEngine, StrategyRuntimeConfig
from brakerscalp.signals.levels import LevelDetector


def test_breakout_signal(make_breakout_market, make_book, make_derivatives, make_health, make_trades) -> None:
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market()
    detector = LevelDetector()
    levels = detector.detect("BTCUSDT", Venue.BINANCE, candles_4h, candles_1h)
    engine = RuleEngine()
    decision = engine.evaluate(
        EngineInput(
            symbol="BTCUSDT",
            venue=Venue.BINANCE,
            candles_4h=candles_4h,
            candles_1h=candles_1h,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            levels=levels,
            trades=make_trades(start_price=candles_5m[-1].close - 60.0),
            book=make_book(),
            derivative_context=make_derivatives(),
            health=make_health(),
            cross_venue_health=[make_health(venue=Venue.BYBIT), make_health(venue=Venue.OKX)],
        )
    )
    assert decision is not None
    assert decision.setup.value == "breakout"
    assert decision.confidence >= 65
    assert "delta_ratio" in decision.feature_snapshot
    assert "cvd_slope" in decision.feature_snapshot


def test_pre_alert_signal(make_breakout_market, make_book, make_derivatives, make_health, make_trades) -> None:
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market(symbol="ETHUSDT")
    detector = LevelDetector()
    detected_levels = detector.detect("ETHUSDT", Venue.BINANCE, candles_4h, candles_1h)
    reference = max([item for item in detected_levels if item.kind.value == "resistance"], key=lambda item: item.reference_price)
    active_level = LevelCandidate(
        symbol="ETHUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.H1,
        kind=LevelKind.RESISTANCE,
        source="prev-day-high",
        lower_price=reference.upper_price + 8.0,
        upper_price=reference.upper_price + 28.0,
        reference_price=reference.upper_price + 18.0,
        detected_at=datetime.now(tz=timezone.utc),
        touches=4,
        age_hours=6.0,
        strength=0.92,
    )
    target_level = LevelCandidate(
        symbol="ETHUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.H4,
        kind=LevelKind.RESISTANCE,
        source="round-number",
        lower_price=active_level.upper_price + 160.0,
        upper_price=active_level.upper_price + 190.0,
        reference_price=active_level.upper_price + 175.0,
        detected_at=datetime.now(tz=timezone.utc),
        touches=2,
        age_hours=3.0,
        strength=0.78,
    )
    levels = [active_level, target_level]

    for candle in candles_5m[-3:]:
        candle.open = active_level.upper_price - 18.0
        candle.high = active_level.upper_price - 7.0
        candle.low = active_level.upper_price - 24.0
        candle.close = active_level.upper_price - 11.0
        candle.volume = 2800.0
        candle.quote_volume = candle.volume * candle.close
    candles_15m[-1].open = active_level.upper_price - 20.0
    candles_15m[-1].high = active_level.upper_price - 5.0
    candles_15m[-1].low = active_level.upper_price - 28.0
    candles_15m[-1].close = active_level.upper_price - 9.0
    candles_15m[-1].volume = 5200.0
    candles_15m[-1].quote_volume = candles_15m[-1].volume * candles_15m[-1].close

    engine = RuleEngine(StrategyRuntimeConfig(pre_alert_confidence_threshold=65.0, minimum_expected_rr=1.0))
    decision = engine.evaluate(
        EngineInput(
            symbol="ETHUSDT",
            venue=Venue.BINANCE,
            candles_4h=candles_4h,
            candles_1h=candles_1h,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            levels=levels,
            trades=make_trades(symbol="ETHUSDT", start_price=candles_5m[-1].close - 8.0),
            book=make_book(symbol="ETHUSDT"),
            derivative_context=make_derivatives(symbol="ETHUSDT"),
            health=make_health(symbol="ETHUSDT"),
            cross_venue_health=[make_health(venue=Venue.BYBIT, symbol="ETHUSDT"), make_health(venue=Venue.OKX, symbol="ETHUSDT")],
        )
    )

    assert decision is not None
    assert decision.signal_class.value == "pre_alert"
    assert decision.render_context["setup_stage"] == "pre_alert"
