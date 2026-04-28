from __future__ import annotations

from datetime import datetime, timedelta, timezone

from brakerscalp.domain.models import Direction, Side, Timeframe, TradeTick, Venue
from brakerscalp.signals.engine import RuleEngine
from brakerscalp.signals.orderflow import compute_order_flow_snapshot


def test_compute_order_flow_snapshot_detects_velocity_and_delta() -> None:
    now = datetime.now(tz=timezone.utc)
    trades: list[TradeTick] = []
    baseline_start = now - timedelta(minutes=10)
    for index in range(60):
        trades.append(
            TradeTick(
                symbol="BTCUSDT",
                venue=Venue.BINANCE,
                timestamp=baseline_start + timedelta(seconds=index * 10),
                price=70000 + index,
                size=0.1,
                side=Side.BUY if index % 3 else Side.SELL,
            )
        )
    for index in range(40):
        trades.append(
            TradeTick(
                symbol="BTCUSDT",
                venue=Venue.BINANCE,
                timestamp=now - timedelta(seconds=25) + timedelta(milliseconds=index * 500),
                price=70600 + index,
                size=0.35,
                side=Side.BUY,
            )
        )

    snapshot = compute_order_flow_snapshot("BTCUSDT", Venue.BINANCE, trades, now=now)

    assert snapshot.tick_velocity_ratio > 2.0
    assert snapshot.delta_ratio > 0.0
    assert snapshot.cvd_slope > 0.0


def test_benchmark_support_blocks_weak_alt_short(make_candles) -> None:
    engine = RuleEngine()
    alt = make_candles(venue=Venue.BINANCE, symbol="ALTUSDT", timeframe=Timeframe.M5, count=30, step=0.02, start_price=10.0)
    btc = make_candles(venue=Venue.BINANCE, symbol="BTCUSDT", timeframe=Timeframe.M5, count=30, step=18.0, start_price=70000.0)
    eth = make_candles(venue=Venue.BINANCE, symbol="ETHUSDT", timeframe=Timeframe.M5, count=30, step=9.0, start_price=3500.0)

    score, headwind = engine._benchmark_support(
        direction=Direction.SHORT,
        execution_candles=alt,
        benchmark_candles_5m={"BTCUSDT": btc, "ETHUSDT": eth},
    )

    assert score >= 0.0
    assert headwind is True
