from __future__ import annotations

from datetime import datetime, timezone

from brakerscalp.domain.models import DataHealth, Direction, ScoreContribution, SetupType, SignalClass, SignalDecision, Timeframe, Venue
from brakerscalp.signals.charting import render_signal_chart
from brakerscalp.signals.engine import EngineInput, RuleEngine
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
    assert decision is not None

    text = render_signal(decision)
    assert "#BREAKOUT" in text
    assert "#BTC" in text
    assert decision.signal_class.value.upper() in text
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
    assert decision is not None
    decision.render_context["setup_stage"] = "activated"

    text = render_signal(decision)
    caption = render_chart_caption(decision)

    assert decision.signal_class.value.upper() in caption
    assert text.endswith("ACTIVATED")
    assert caption.endswith("ACTIVATED")


def test_render_signal_keeps_precision_for_low_priced_assets() -> None:
    decision = SignalDecision(
        symbol="PENGUUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.M5,
        setup=SetupType.BREAKOUT,
        direction=Direction.LONG,
        signal_class=SignalClass.WATCHLIST,
        confidence=90.0,
        level_id="pengu-level",
        alert_key="pengu-alert",
        detected_at=datetime.now(tz=timezone.utc),
        entry_price=0.010003,
        invalidation_price=0.009947,
        targets=[0.010127, 0.010171],
        expected_rr=2.21,
        rationale=["Compression near resistance"],
        why_not_higher=["Tape still slow"],
        contributions=[ScoreContribution(group="level", score=20.0, max_score=25.0, reason="Strong level")],
        data_health=DataHealth(venue=Venue.BINANCE, symbol="PENGUUSDT", is_fresh=True, freshness_ms=0),
        feature_snapshot={},
        render_context={
            "price_zone": "0.009947 - 0.010003",
            "htf_source": "1h cascade-high",
            "trigger": "Цена стоит под сопротивлением. Для входа нужен 5m close выше 0.010003.",
            "stop_logic": "SL below the zone",
            "cancel_if": "Compression fails",
            "venues_used": "binance",
        },
    )

    text = render_signal(decision)
    caption = render_chart_caption(decision)

    assert "0.010003" in text
    assert "0.009947" in text
    assert "0.010127" in text
    assert "0.010171" in text
    assert "0.010003" in caption
