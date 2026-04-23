from __future__ import annotations

from datetime import datetime, timedelta, timezone

from brakerscalp.domain.models import AlertMessage, SignalClass
from brakerscalp.storage.models import CandleRecord, SignalRecord


async def test_alert_deduplication(cache) -> None:
    first = await cache.acquire_alert_key("abc")
    second = await cache.acquire_alert_key("abc")
    assert first is True
    assert second is False


async def test_outbox_roundtrip(cache) -> None:
    message = AlertMessage(
        signal_id="s1",
        alert_key="k1",
        chat_id=1,
        text="hello",
        signal_class=SignalClass.WATCHLIST,
    )
    await cache.enqueue_alert(message)
    popped = await cache.pop_alert(timeout=1)
    assert popped == message


async def test_delivery_persistence_and_recovery(repository) -> None:
    message = AlertMessage(
        signal_id="s2",
        alert_key="k2",
        chat_id=42,
        text="recover me",
        signal_class=SignalClass.ACTIONABLE,
    )
    await repository.ensure_delivery(message)
    recoverable = await repository.list_recoverable_deliveries(limit=10)
    assert len(recoverable) == 1
    assert recoverable[0].message_text == "recover me"
    counts = await repository.delivery_status_counts()
    assert counts["queued"] == 1


async def test_large_telegram_chat_id_is_supported(repository) -> None:
    message = AlertMessage(
        signal_id="s3",
        alert_key="k3",
        chat_id=-1003788053657,
        message_thread_id=475,
        text="large chat id",
        signal_class=SignalClass.WATCHLIST,
    )
    await repository.ensure_delivery(message)
    recoverable = await repository.list_recoverable_deliveries(limit=10)
    assert any(item.chat_id == -1003788053657 for item in recoverable)


async def test_repository_lists_signals_and_candles_for_daily_summary(repository) -> None:
    signal_time = datetime(2026, 4, 23, 10, 0, tzinfo=timezone.utc)
    next_candle_open = signal_time + timedelta(minutes=15)
    async with repository.session_factory() as session:
        session.add(
            SignalRecord(
                decision_id="summary-1",
                alert_key="summary-key-1",
                venue="binance",
                symbol="ETHUSDT",
                timeframe="15m",
                setup="breakout",
                direction="long",
                signal_class="actionable",
                confidence=88.0,
                level_id="level-summary-1",
                detected_at=signal_time,
                entry_price=2000.0,
                invalidation_price=1950.0,
                targets=[2100.0, 2200.0],
                expected_rr=2.0,
                rationale=[],
                why_not_higher=[],
                contributions=[],
                data_health={},
                feature_snapshot={},
                render_context={},
            )
        )
        session.add(
            CandleRecord(
                venue="binance",
                symbol="ETHUSDT",
                timeframe="15m",
                open_time=next_candle_open,
                close_time=next_candle_open + timedelta(minutes=15),
                open=2000.0,
                high=2110.0,
                low=1990.0,
                close=2105.0,
                volume=5000.0,
                quote_volume=10500000.0,
                trade_count=100,
                taker_buy_volume=2600.0,
                vwap=2050.0,
            )
        )
        await session.commit()

    signals = await repository.list_signals_between(
        datetime(2026, 4, 23, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 24, 0, 0, tzinfo=timezone.utc),
        signal_classes=["actionable", "watchlist"],
    )
    candles = await repository.get_candles_between(
        "binance",
        "ETHUSDT",
        "15m",
        signal_time,
        datetime(2026, 4, 24, 0, 0, tzinfo=timezone.utc),
    )

    assert len(signals) == 1
    assert signals[0].symbol == "ETHUSDT"
    assert len(candles) == 1
    assert candles[0].high == 2110.0
