from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from brakerscalp.services.daily_summary import SignalOutcome, classify_signal_outcome, render_daily_summary
from brakerscalp.storage.models import CandleRecord, SignalRecord


def make_signal(direction: str = "long") -> SignalRecord:
    return SignalRecord(
        decision_id=f"decision-{direction}",
        alert_key=f"alert-{direction}",
        venue="binance",
        symbol="BTCUSDT",
        timeframe="15m",
        setup="breakout",
        direction=direction,
        signal_class="actionable",
        confidence=91.0,
        level_id="level-1",
        detected_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
        entry_price=100.0,
        invalidation_price=95.0 if direction == "long" else 105.0,
        targets=[110.0 if direction == "long" else 90.0, 120.0 if direction == "long" else 80.0],
        expected_rr=2.0,
        rationale=[],
        why_not_higher=[],
        contributions=[],
        data_health={},
        feature_snapshot={},
        render_context={},
    )


def make_candle(*, high: float, low: float, offset_minutes: int = 15) -> CandleRecord:
    open_time = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc) + timedelta(minutes=offset_minutes)
    return CandleRecord(
        venue="binance",
        symbol="BTCUSDT",
        timeframe="15m",
        open_time=open_time,
        close_time=open_time + timedelta(minutes=15),
        open=100.0,
        high=high,
        low=low,
        close=100.0,
        volume=1000.0,
        quote_volume=100000.0,
        trade_count=10,
        taker_buy_volume=500.0,
        vwap=100.0,
    )


def test_classify_signal_outcome_success_for_long() -> None:
    signal = make_signal(direction="long")
    candles = [make_candle(high=111.0, low=99.0)]
    assert classify_signal_outcome(signal, candles) == "success"


def test_classify_signal_outcome_failed_for_short() -> None:
    signal = make_signal(direction="short")
    candles = [make_candle(high=106.0, low=94.0)]
    assert classify_signal_outcome(signal, candles) == "failed"


def test_render_daily_summary_contains_hit_rate_and_lists() -> None:
    report = render_daily_summary(
        date(2026, 4, 23),
        [
            SignalOutcome(signal=make_signal("long"), status="success"),
            SignalOutcome(signal=make_signal("short"), status="failed"),
            SignalOutcome(signal=make_signal("long"), status="pending"),
        ],
    )
    assert "Сводка за 23.04.2026" in report
    assert "Процент отработки: 50.0%" in report
    assert "Успешные сигналы:" in report
    assert "Неотработанные сигналы:" in report
    assert "Сигналы в ожидании:" in report
    assert "#BREAKOUT #BTC" in report
