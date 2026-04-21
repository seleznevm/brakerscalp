from __future__ import annotations

from brakerscalp.domain.models import Timeframe, UniverseSymbol, Venue
from brakerscalp.signals.engine import EngineInput, RuleEngine
from brakerscalp.signals.levels import LevelDetector


def test_breakout_signal(make_candles, make_book, make_derivatives, make_health) -> None:
    candles_4h = make_candles(timeframe=Timeframe.H4, count=30, step=50.0)
    candles_1h = make_candles(timeframe=Timeframe.H1, count=200, step=12.0)
    candles_15m = make_candles(timeframe=Timeframe.M15, count=80, step=8.0)
    candles_5m = make_candles(timeframe=Timeframe.M5, count=80, step=6.0)
    candles_15m[-1].close = candles_15m[-1].close + 500
    candles_15m[-1].high = candles_15m[-1].close + 10
    candles_15m[-1].volume = candles_15m[-1].volume * 5

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
            book=make_book(),
            derivative_context=make_derivatives(),
            health=make_health(),
            cross_venue_health=[make_health(venue=Venue.BYBIT), make_health(venue=Venue.OKX)],
        )
    )
    assert decision is not None
    assert decision.confidence >= 65

