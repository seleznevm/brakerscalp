from __future__ import annotations

from brakerscalp.domain.models import AlertMessage, SignalDecision


def render_signal(decision: SignalDecision) -> str:
    context = decision.render_context
    rationale_lines = "\n".join(f"• {item}" for item in decision.rationale)
    why_lines = "\n".join(f"• {item}" for item in decision.why_not_higher)
    targets = decision.targets[:2]
    target_lines = "\n".join(f"• T{index + 1}: {price:.4f}" for index, price in enumerate(targets))
    return (
        f"🚨 {decision.symbol} | {decision.setup.value.upper()} | {decision.direction.value.upper()} | {decision.timeframe.value}\n"
        f"Confidence: {decision.confidence:.0f}\n\n"
        f"Level:\n"
        f"{context['price_zone']} | HTF source: {context['htf_source']}\n\n"
        f"Trigger:\n"
        f"{context['trigger']}\n\n"
        f"Rationale:\n"
        f"{rationale_lines}\n\n"
        f"Invalidation:\n"
        f"• Stop logic: {context['stop_logic']}\n"
        f"• Cancel if: {context['cancel_if']}\n\n"
        f"Targets:\n"
        f"{target_lines}\n"
        f"• Expected R:R: {decision.expected_rr:.2f}\n\n"
        f"Why confidence is not higher:\n"
        f"{why_lines}\n\n"
        f"Data health:\n"
        f"• Freshness: {decision.data_health.freshness_ms} ms\n"
        f"• Venues used: {context['venues_used']}\n"
        f"• Sequence gaps: {'recovered' if decision.data_health.has_sequence_gap else 'none'}"
    )


def to_alert_message(decision: SignalDecision, chat_id: int) -> AlertMessage:
    return AlertMessage(
        signal_id=decision.decision_id,
        alert_key=decision.alert_key,
        chat_id=chat_id,
        text=render_signal(decision),
        signal_class=decision.signal_class,
    )

