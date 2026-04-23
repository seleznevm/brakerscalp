from __future__ import annotations

from brakerscalp.domain.models import AlertMessage, SignalDecision


def _bullet_lines(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def _coin_hashtag(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4].upper()
    if "-" in symbol:
        return symbol.split("-", 1)[0].upper()
    return symbol.upper()


def _hashtags(decision: SignalDecision) -> str:
    return f"#{decision.setup.value.upper()} #{_coin_hashtag(decision.symbol)}"


def render_signal(decision: SignalDecision) -> str:
    context = decision.render_context
    rationale_lines = _bullet_lines(decision.rationale)
    why_lines = _bullet_lines(decision.why_not_higher)
    targets = decision.targets[:2]
    target_lines = "\n".join(f"- T{index + 1}: {price:.4f}" for index, price in enumerate(targets))
    return (
        f"🚨 {decision.symbol} | {decision.setup.value.upper()} | {decision.direction.value.upper()} | {decision.timeframe.value}\n"
        f"{_hashtags(decision)}\n"
        f"Уверенность: {decision.confidence:.0f}\n\n"
        f"Уровень:\n"
        f"{context['price_zone']} | HTF источник: {context['htf_source']}\n\n"
        f"Триггер:\n"
        f"{context['trigger']}\n\n"
        f"Обоснование:\n"
        f"{rationale_lines}\n\n"
        f"Инвалидация:\n"
        f"- Стоп-логика: {context['stop_logic']}\n"
        f"- Отмена, если: {context['cancel_if']}\n\n"
        f"Цели:\n"
        f"{target_lines}\n"
        f"- Ожидаемый R:R: {decision.expected_rr:.2f}\n\n"
        f"Почему уверенность не выше:\n"
        f"{why_lines}\n\n"
        f"Состояние данных:\n"
        f"- Свежесть: {decision.data_health.freshness_ms} ms\n"
        f"- Использованные биржи: {context['venues_used']}\n"
        f"- Разрывы последовательности: {'восстановлены' if decision.data_health.has_sequence_gap else 'нет'}"
    )


def to_alert_message(decision: SignalDecision, chat_id: int, message_thread_id: int | None = None) -> AlertMessage:
    return AlertMessage(
        signal_id=decision.decision_id,
        alert_key=decision.alert_key,
        chat_id=chat_id,
        message_thread_id=message_thread_id,
        text=render_signal(decision),
        signal_class=decision.signal_class,
    )
