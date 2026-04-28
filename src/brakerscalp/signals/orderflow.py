from __future__ import annotations

from datetime import datetime, timedelta, timezone

from brakerscalp.domain.models import OrderFlowSnapshot, TradeTick, Venue


def merge_trade_history(
    existing: list[TradeTick],
    incoming: list[TradeTick],
    *,
    now: datetime | None = None,
    max_age_seconds: int = 900,
    max_items: int = 4000,
) -> list[TradeTick]:
    reference_now = now or datetime.now(tz=timezone.utc)
    cutoff = reference_now - timedelta(seconds=max_age_seconds)
    merged: dict[tuple[str, str, float, float, str], TradeTick] = {}
    for trade in [*existing, *incoming]:
        timestamp = trade.timestamp if trade.timestamp.tzinfo else trade.timestamp.replace(tzinfo=timezone.utc)
        if timestamp < cutoff:
            continue
        key = (
            trade.symbol,
            timestamp.isoformat(),
            float(trade.price),
            float(trade.size),
            trade.side.value,
        )
        merged[key] = trade
    ordered = sorted(merged.values(), key=lambda item: item.timestamp)
    return ordered[-max_items:]


def compute_order_flow_snapshot(
    symbol: str,
    venue: Venue,
    trades: list[TradeTick],
    *,
    now: datetime | None = None,
    recent_seconds: int = 30,
    baseline_seconds: int = 600,
) -> OrderFlowSnapshot:
    reference_now = now or datetime.now(tz=timezone.utc)
    if not trades:
        return OrderFlowSnapshot(symbol=symbol, venue=venue, timestamp=reference_now)
    ordered = sorted(trades, key=lambda item: item.timestamp)
    baseline_cutoff = reference_now - timedelta(seconds=baseline_seconds)
    recent_cutoff = reference_now - timedelta(seconds=recent_seconds)
    baseline_trades = [item for item in ordered if item.timestamp >= baseline_cutoff]
    recent_trades = [item for item in baseline_trades if item.timestamp >= recent_cutoff]
    if not baseline_trades:
        baseline_trades = ordered[-min(len(ordered), 120):]
    recent_trade_count = len(recent_trades)
    baseline_trade_count = len(baseline_trades)
    tick_velocity = recent_trade_count / max(recent_seconds, 1)
    baseline_tick_velocity = baseline_trade_count / max(baseline_seconds, 1)
    tick_velocity_ratio = tick_velocity / max(baseline_tick_velocity, 1e-9) if baseline_tick_velocity else 0.0

    total_notional = 0.0
    delta_notional = 0.0
    cvd_series: list[float] = []
    running_cvd = 0.0
    for trade in baseline_trades:
        signed_notional = trade.price * trade.size
        if trade.side.value == "sell":
            signed_notional *= -1.0
        total_notional += abs(signed_notional)
        delta_notional += signed_notional
        running_cvd += signed_notional
        cvd_series.append(running_cvd)
    delta_ratio = delta_notional / max(total_notional, 1e-9) if total_notional else 0.0
    cvd_slope = ((cvd_series[-1] - cvd_series[0]) / max(total_notional, 1e-9)) if len(cvd_series) > 1 else delta_ratio

    return OrderFlowSnapshot(
        symbol=symbol,
        venue=venue,
        timestamp=reference_now,
        delta_ratio=delta_ratio,
        cvd_slope=cvd_slope,
        tick_velocity=tick_velocity,
        baseline_tick_velocity=baseline_tick_velocity,
        tick_velocity_ratio=tick_velocity_ratio,
        recent_trade_count=recent_trade_count,
        baseline_trade_count=baseline_trade_count,
    )
