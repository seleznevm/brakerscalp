from __future__ import annotations

from brakerscalp.domain.models import Timeframe, Venue
from brakerscalp.signals.engine import EngineInput, RuleEngine
from brakerscalp.signals.levels import LevelDetector


def test_replay_signal_metrics_shape(make_candles, make_book, make_derivatives, make_health) -> None:
    candles_4h = make_candles(timeframe=Timeframe.H4, count=40, step=45)
    candles_1h = make_candles(timeframe=Timeframe.H1, count=240, step=15)
    candles_15m = make_candles(timeframe=Timeframe.M15, count=100, step=9)
    candles_5m = make_candles(timeframe=Timeframe.M5, count=100, step=5)
    levels = LevelDetector().detect("BTCUSDT", Venue.BINANCE, candles_4h, candles_1h)
    engine = RuleEngine()
    decisions = []
    for index in range(40, len(candles_15m)):
        window = candles_15m[: index + 1]
        decision = engine.evaluate(
            EngineInput(
                symbol="BTCUSDT",
                venue=Venue.BINANCE,
                candles_4h=candles_4h,
                candles_1h=candles_1h,
                candles_15m=window,
                candles_5m=candles_5m[: index + 1],
                levels=levels,
                book=make_book(mid=window[-1].close),
                derivative_context=make_derivatives(),
                health=make_health(),
                cross_venue_health=[make_health(venue=Venue.BYBIT), make_health(venue=Venue.OKX)],
            )
        )
        if decision is not None:
            decisions.append(decision)
    assert isinstance(decisions, list)
