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


@pytest.mark.asyncio
async def test_orderflow_service_sends_executed_for_fresh_actionable_signal(repository, cache) -> None:
    detected_at = datetime.now(tz=timezone.utc) - timedelta(minutes=12)
    decision = SignalDecision(
        symbol="BTCUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.M15,
        setup=SetupType.BREAKOUT,
        direction=Direction.LONG,
        signal_class=SignalClass.ACTIONABLE,
        confidence=91.0,
        level_id="btc-actionable-level",
        alert_key="btc-actionable-alert",
        detected_at=detected_at,
        entry_price=70000.0,
        invalidation_price=69650.0,
        targets=[70700.0, 71400.0],
        expected_rr=2.0,
        rationale=["Breakout already armed"],
        why_not_higher=["Need fresh execution"],
        contributions=[ScoreContribution(group="level", score=22.0, max_score=25.0, reason="Strong level")],
        data_health=DataHealth(venue=Venue.BINANCE, symbol="BTCUSDT", is_fresh=True, freshness_ms=0),
        feature_snapshot={"atr_15m": 120.0},
        render_context={"trigger": "5m close already above the level", "price_zone": "69950.0 - 70000.0", "setup_stage": "activated"},
    )
    await repository.save_signal(decision)
    await repository.upsert_candles(
        [
            MarketCandle(
                symbol="BTCUSDT",
                venue=Venue.BINANCE,
                timeframe=Timeframe.M15,
                open_time=detected_at,
                close_time=detected_at + timedelta(minutes=15),
                open=69980.0,
                high=70220.0,
                low=69940.0,
                close=70190.0,
                volume=2200.0,
                quote_volume=154418000.0,
                trade_count=22,
                taker_buy_volume=1300.0,
                vwap=70110.0,
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
    assert "BTCUSDT" in alert.text


@pytest.mark.asyncio
async def test_orderflow_service_does_not_replay_old_executed_signal_on_boot(repository, cache) -> None:
    detected_at = datetime.now(tz=timezone.utc) - timedelta(hours=2)
    decision = SignalDecision(
        symbol="XRPUSDT",
        venue=Venue.BINANCE,
        timeframe=Timeframe.M15,
        setup=SetupType.BREAKOUT,
        direction=Direction.LONG,
        signal_class=SignalClass.ACTIONABLE,
        confidence=89.0,
        level_id="xrp-actionable-level",
        alert_key="xrp-actionable-alert",
        detected_at=detected_at,
        entry_price=0.6000,
        invalidation_price=0.5850,
        targets=[0.6300, 0.6600],
        expected_rr=2.0,
        rationale=["Older actionable breakout"],
        why_not_higher=["Already stale for notification"],
        contributions=[ScoreContribution(group="level", score=20.0, max_score=25.0, reason="Strong level")],
        data_health=DataHealth(venue=Venue.BINANCE, symbol="XRPUSDT", is_fresh=True, freshness_ms=0),
        feature_snapshot={"atr_15m": 0.01},
        render_context={"trigger": "Older breakout", "price_zone": "0.5950 - 0.6000", "setup_stage": "activated"},
    )
    await repository.save_signal(decision)
    await repository.upsert_candles(
        [
            MarketCandle(
                symbol="XRPUSDT",
                venue=Venue.BINANCE,
                timeframe=Timeframe.M15,
                open_time=detected_at,
                close_time=detected_at + timedelta(minutes=15),
                open=0.5980,
                high=0.6120,
                low=0.5960,
                close=0.6080,
                volume=1800.0,
                quote_volume=1080.0,
                trade_count=20,
                taker_buy_volume=920.0,
                vwap=0.6040,
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

    assert sent == 0
    assert alert is None
