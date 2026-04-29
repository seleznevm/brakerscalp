from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from brakerscalp.domain.models import DataHealth, Direction, MarketCandle, ScoreContribution, SetupType, SignalClass, SignalDecision, Timeframe, Venue
from brakerscalp.services.order_flow_service import OrderFlowAnalyzerService
from brakerscalp.signals.engine import StrategyRuntimeConfig


@pytest.mark.asyncio
async def test_orderflow_service_sends_executed_alert(repository, cache) -> None:
    detected_at = datetime.now(tz=timezone.utc) - timedelta(minutes=30)
    decision = SignalDecision(
        symbol="SOLUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.M15,
        setup=SetupType.BREAKOUT,
        direction=Direction.LONG,
        signal_class=SignalClass.WATCHLIST,
        confidence=84.0,
        level_id="sol-watch-level",
        alert_key="sol-watch-alert",
        detected_at=detected_at,
        entry_price=150.0,
        invalidation_price=147.0,
        targets=[156.0, 162.0],
        expected_rr=2.0,
        rationale=["Pressure under resistance"],
        why_not_higher=["Waiting for execution"],
        contributions=[ScoreContribution(group="level", score=20.0, max_score=25.0, reason="Strong level")],
        data_health=DataHealth(venue=Venue.BINANCE, symbol="SOLUSDT", is_fresh=True, freshness_ms=0),
        feature_snapshot={"atr_15m": 2.0},
        render_context={"trigger": "Need a close above 150.0", "price_zone": "149.0 - 150.0", "setup_stage": "watch"},
    )
    await repository.save_signal(decision)
    await repository.upsert_candles(
        [
            MarketCandle(
                symbol="SOLUSDT",
                venue=Venue.BINANCE,
                timeframe=Timeframe.M15,
                open_time=detected_at,
                close_time=detected_at + timedelta(minutes=15),
                open=149.5,
                high=153.0,
                low=148.8,
                close=151.5,
                volume=1200.0,
                quote_volume=181800.0,
                trade_count=12,
                taker_buy_volume=720.0,
                vwap=150.8,
            )
        ]
    )

    service = OrderFlowAnalyzerService(
        repository=repository,
        cache=cache,
        universe=[],
        alert_chat_ids=[1],
        interval_seconds=5,
    )
    sent = await service._process_active_signals(
        StrategyRuntimeConfig(
            enable_time_stop_alerts=False,
            enable_dynamic_breakeven_alerts=False,
        )
    )
    alert = await cache.pop_alert(timeout=1)

    assert sent == 1
    assert alert is not None
    assert alert.signal_id.endswith("#executed")
    assert "SOLUSDT" in alert.text
    assert "Entry: 150.0000" in alert.text
    assert "TP1: 156.0000" in alert.text
    assert "TP2: 162.0000" in alert.text
    assert "SL: 147.0000" in alert.text
    assert alert.text.endswith("EXECUTED")


@pytest.mark.asyncio
async def test_orderflow_service_does_not_repeat_executed_alert(repository, cache) -> None:
    detected_at = datetime.now(tz=timezone.utc) - timedelta(minutes=30)
    decision = SignalDecision(
        symbol="ETHUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.M15,
        setup=SetupType.BREAKOUT,
        direction=Direction.LONG,
        signal_class=SignalClass.WATCHLIST,
        confidence=83.0,
        level_id="eth-watch-level",
        alert_key="eth-watch-alert",
        detected_at=detected_at,
        entry_price=2500.0,
        invalidation_price=2470.0,
        targets=[2560.0, 2620.0],
        expected_rr=2.0,
        rationale=["Pressure under resistance"],
        why_not_higher=["Waiting for execution"],
        contributions=[ScoreContribution(group="level", score=20.0, max_score=25.0, reason="Strong level")],
        data_health=DataHealth(venue=Venue.BINANCE, symbol="ETHUSDT", is_fresh=True, freshness_ms=0),
        feature_snapshot={"atr_15m": 18.0},
        render_context={"trigger": "Need a close above 2500.0", "price_zone": "2490.0 - 2500.0", "setup_stage": "watch"},
    )
    await repository.save_signal(decision)
    await repository.upsert_candles(
        [
            MarketCandle(
                symbol="ETHUSDT",
                venue=Venue.BINANCE,
                timeframe=Timeframe.M15,
                open_time=detected_at,
                close_time=detected_at + timedelta(minutes=15),
                open=2495.0,
                high=2514.0,
                low=2490.0,
                close=2508.0,
                volume=1600.0,
                quote_volume=4012800.0,
                trade_count=15,
                taker_buy_volume=980.0,
                vwap=2502.0,
            )
        ]
    )
    await cache.set_signal_lifecycle_status(decision.decision_id, "executed")

    service = OrderFlowAnalyzerService(
        repository=repository,
        cache=cache,
        universe=[],
        alert_chat_ids=[1],
        interval_seconds=5,
    )
    sent = await service._process_active_signals(
        StrategyRuntimeConfig(
            enable_time_stop_alerts=False,
            enable_dynamic_breakeven_alerts=False,
        )
    )
    alert = await cache.pop_alert(timeout=1)

    assert sent == 0
    assert alert is None
