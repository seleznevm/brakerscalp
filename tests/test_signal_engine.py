from __future__ import annotations

from datetime import datetime, timezone

from brakerscalp.domain.models import Direction, LevelCandidate, LevelKind, Timeframe, Venue
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


def test_pre_alert_is_suppressed_after_price_has_already_cleared_entry(
    make_breakout_market, make_book, make_derivatives, make_health, make_trades
) -> None:
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market(symbol="ORCAUSDT")
    detector = LevelDetector()
    detected_levels = detector.detect("ORCAUSDT", Venue.BINANCE, candles_4h, candles_1h)
    reference = max([item for item in detected_levels if item.kind.value == "resistance"], key=lambda item: item.reference_price)
    active_level = LevelCandidate(
        symbol="ORCAUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.H1,
        kind=LevelKind.RESISTANCE,
        source="cascade-high",
        lower_price=reference.upper_price + 8.0,
        upper_price=reference.upper_price + 28.0,
        reference_price=reference.upper_price + 18.0,
        detected_at=datetime.now(tz=timezone.utc),
        touches=5,
        age_hours=4.0,
        strength=0.95,
    )
    target_level = LevelCandidate(
        symbol="ORCAUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.H4,
        kind=LevelKind.RESISTANCE,
        source="round-number",
        lower_price=active_level.upper_price + 160.0,
        upper_price=active_level.upper_price + 190.0,
        reference_price=active_level.upper_price + 175.0,
        detected_at=datetime.now(tz=timezone.utc),
        touches=2,
        age_hours=2.0,
        strength=0.78,
    )
    levels = [active_level, target_level]

    for candle in candles_5m[-3:]:
        candle.open = active_level.upper_price + 8.0
        candle.low = active_level.upper_price + 2.0
        candle.high = active_level.upper_price + 14.0
        candle.close = active_level.upper_price + 10.0
        candle.volume = 1200.0
        candle.quote_volume = candle.volume * candle.close
    candles_15m[-1].open = active_level.upper_price + 7.0
    candles_15m[-1].low = active_level.upper_price + 1.0
    candles_15m[-1].high = active_level.upper_price + 13.0
    candles_15m[-1].close = active_level.upper_price + 9.0
    candles_15m[-1].volume = 1800.0
    candles_15m[-1].quote_volume = candles_15m[-1].volume * candles_15m[-1].close

    engine = RuleEngine(StrategyRuntimeConfig(pre_alert_confidence_threshold=65.0, minimum_expected_rr=1.0))
    decision = engine.evaluate(
        EngineInput(
            symbol="ORCAUSDT",
            venue=Venue.BINANCE,
            candles_4h=candles_4h,
            candles_1h=candles_1h,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            levels=levels,
            trades=[],
            book=make_book(symbol="ORCAUSDT"),
            derivative_context=make_derivatives(symbol="ORCAUSDT"),
            health=make_health(symbol="ORCAUSDT"),
            cross_venue_health=[make_health(venue=Venue.BYBIT, symbol="ORCAUSDT"), make_health(venue=Venue.OKX, symbol="ORCAUSDT")],
        )
    )

    assert decision is None


def test_trend_state_ignores_live_htf_spike(make_candles) -> None:
    candles_1h = make_candles(symbol="BTCUSDT", timeframe=Timeframe.H1, count=80, start_price=200.0, step=-2.0)
    candles_4h = make_candles(symbol="BTCUSDT", timeframe=Timeframe.H4, count=40, start_price=400.0, step=-5.0)
    candles_1h[-1].open = candles_1h[-2].close
    candles_1h[-1].low = min(candles_1h[-1].open, candles_1h[-1].close) - 1.0
    candles_1h[-1].close = 360.0
    candles_1h[-1].high = 362.0
    candles_4h[-1].open = candles_4h[-2].close
    candles_4h[-1].low = min(candles_4h[-1].open, candles_4h[-1].close) - 2.0
    candles_4h[-1].close = 520.0
    candles_4h[-1].high = 524.0

    trend = RuleEngine()._trend_state(candles_1h, candles_4h)

    assert trend.bias == Direction.SHORT


def test_benchmark_support_blocks_long_when_btc_and_eth_dump(make_candles) -> None:
    engine = RuleEngine(StrategyRuntimeConfig(enable_btc_eth_correlation_filter=True, btc_correlation_threshold=0.45))
    execution_candles = make_candles(symbol="ORCAUSDT", timeframe=Timeframe.M5, count=12, start_price=10.0, step=-0.02)
    btc_candles = make_candles(symbol="BTCUSDT", timeframe=Timeframe.M5, count=12, start_price=100.0, step=-2.0)
    eth_candles = make_candles(symbol="ETHUSDT", timeframe=Timeframe.M5, count=12, start_price=80.0, step=-1.6)

    support_score, headwind = engine._benchmark_support(
        direction=Direction.LONG,
        execution_candles=execution_candles,
        benchmark_candles_5m={"BTCUSDT": btc_candles, "ETHUSDT": eth_candles},
    )

    assert headwind is True
    assert support_score < 0.35


def test_live_spike_does_not_use_unclosed_candle_shape_for_actionable_signal(
    make_breakout_market, make_book, make_derivatives, make_health, make_trades
) -> None:
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market(symbol="BTCUSDT")
    levels = LevelDetector().detect("BTCUSDT", Venue.BINANCE, candles_4h, candles_1h)
    reference_level = max([item for item in levels if item.kind.value == "resistance"], key=lambda item: item.reference_price)

    weak_closed = candles_5m[-2]
    weak_closed.open = reference_level.upper_price + 0.8
    weak_closed.low = reference_level.upper_price - 2.0
    weak_closed.close = reference_level.upper_price + 1.0
    weak_closed.high = reference_level.upper_price + 18.0
    weak_closed.volume = 1800.0
    weak_closed.quote_volume = weak_closed.volume * weak_closed.close

    live_spike = candles_5m[-1]
    live_spike.open = reference_level.upper_price + 1.0
    live_spike.low = reference_level.upper_price + 0.5
    live_spike.close = reference_level.upper_price + 30.0
    live_spike.high = reference_level.upper_price + 30.5
    live_spike.volume = 4200.0
    live_spike.quote_volume = live_spike.volume * live_spike.close

    engine = RuleEngine(StrategyRuntimeConfig(enable_btc_eth_correlation_filter=False))
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

    assert decision is None or decision.signal_class.value != "actionable"
    if decision is not None:
        assert decision.signal_class.value != "actionable"
