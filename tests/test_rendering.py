from __future__ import annotations

from brakerscalp.domain.models import Timeframe, Venue
from brakerscalp.signals.engine import EngineInput, RuleEngine
from brakerscalp.signals.charting import render_signal_chart
from brakerscalp.signals.levels import LevelDetector
from brakerscalp.signals.rendering import render_chart_caption, render_signal


def test_render_signal_contains_required_sections(make_breakout_market, make_book, make_derivatives, make_health, make_trades) -> None:
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market()
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
            trades=make_trades(start_price=candles_5m[-1].close - 60.0),
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
    assert "Entry:" in text

    chart = render_signal_chart(candles_15m, decision)
    assert chart is not None
    assert chart.startswith(b"\x89PNG")


def test_render_signal_marks_activated_setups(make_breakout_market, make_book, make_derivatives, make_health, make_trades) -> None:
    candles_4h, candles_1h, candles_15m, candles_5m = make_breakout_market()
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
            trades=make_trades(start_price=candles_5m[-1].close - 60.0),
            book=make_book(),
            derivative_context=make_derivatives(),
            health=make_health(),
            cross_venue_health=[make_health(venue=Venue.BYBIT), make_health(venue=Venue.OKX)],
        )
    )
    decision.render_context["setup_stage"] = "activated"

    text = render_signal(decision)
    caption = render_chart_caption(decision)

    assert text.endswith("ACTIVATED")
    assert caption.endswith("ACTIVATED")
