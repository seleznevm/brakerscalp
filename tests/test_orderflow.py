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


def test_compute_order_flow_snapshot_ignores_sparse_baseline() -> None:
    now = datetime.now(tz=timezone.utc)
    trades: list[TradeTick] = []
    for index in range(6):
        trades.append(
            TradeTick(
                symbol="ALTUSDT",
                venue=Venue.BINANCE,
                timestamp=now - timedelta(minutes=9) + timedelta(seconds=index * 45),
                price=10.0 + index * 0.01,
                size=5.0,
                side=Side.BUY if index % 2 else Side.SELL,
            )
        )
    for index in range(25):
        trades.append(
            TradeTick(
                symbol="ALTUSDT",
                venue=Venue.BINANCE,
                timestamp=now - timedelta(seconds=20) + timedelta(milliseconds=index * 600),
                price=10.2 + index * 0.001,
                size=8.0,
                side=Side.BUY,
            )
        )

    snapshot = compute_order_flow_snapshot("ALTUSDT", Venue.BINANCE, trades, now=now)

    assert snapshot.recent_trade_count == 25
    assert snapshot.baseline_trade_count == 6
    assert snapshot.baseline_tick_velocity == 0.0
    assert snapshot.tick_velocity_ratio == 0.0


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
