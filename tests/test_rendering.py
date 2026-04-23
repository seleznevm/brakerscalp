from __future__ import annotations

from brakerscalp.domain.models import Timeframe, Venue
from brakerscalp.signals.engine import EngineInput, RuleEngine
from brakerscalp.signals.levels import LevelDetector
from brakerscalp.signals.rendering import render_signal


def test_render_signal_contains_required_sections(make_candles, make_book, make_derivatives, make_health) -> None:
    candles_4h = make_candles(timeframe=Timeframe.H4, count=30, step=50.0)
    candles_1h = make_candles(timeframe=Timeframe.H1, count=200, step=12.0)
    candles_15m = make_candles(timeframe=Timeframe.M15, count=80, step=8.0)
    candles_5m = make_candles(timeframe=Timeframe.M5, count=80, step=6.0)
    candles_15m[-1].close += 500
    candles_15m[-1].volume *= 5
    levels = LevelDetector().detect("BTCUSDT", Venue.BINANCE, candles_4h, candles_1h)
    decision = RuleEngine().evaluate(
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
    text = render_signal(decision)
    assert "#BREAKOUT" in text
    assert "#BTC" in text
    assert "Уверенность:" in text
    assert "Триггер:" in text
    assert "Обоснование:" in text
    assert "Инвалидация:" in text
    assert "Почему уверенность не выше:" in text
