from __future__ import annotations

from brakerscalp.domain.models import AlertMessage, SignalClass


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
