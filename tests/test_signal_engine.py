from __future__ import annotations

from brakerscalp.domain.models import Timeframe, UniverseSymbol, Venue
from brakerscalp.signals.engine import EngineInput, RuleEngine
from brakerscalp.signals.levels import LevelDetector


def test_breakout_signal(make_breakout_market, make_book, make_derivatives, make_health) -> None:
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
            book=make_book(),
            derivative_context=make_derivatives(),
            health=make_health(),
            cross_venue_health=[make_health(venue=Venue.BYBIT), make_health(venue=Venue.OKX)],
        )
    )
    assert decision is not None
    assert decision.setup.value == "breakout"
    assert decision.confidence >= 65
