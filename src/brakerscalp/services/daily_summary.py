from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from brakerscalp.storage.models import CandleRecord, SignalRecord


SETUP_STATUS_WATCH = "watch"
SETUP_STATUS_EXECUTED = "executed"
SETUP_STATUS_TP1 = "tp1"
SETUP_STATUS_TP2 = "tp2"
SETUP_STATUS_LOSS = "loss"
SETUP_STATUS_INVALIDATION = "invalidation"

PENDING_OUTCOMES = {SETUP_STATUS_WATCH, SETUP_STATUS_EXECUTED}
SUCCESS_OUTCOMES = {SETUP_STATUS_TP1, SETUP_STATUS_TP2}
FAILED_OUTCOMES = {SETUP_STATUS_LOSS, SETUP_STATUS_INVALIDATION}


@dataclass(slots=True)
class SignalOutcome:
    signal: SignalRecord
    status: str


@dataclass(slots=True)
class SetupLifecycle:
    status: str
    call_at: datetime
    entry_at: datetime | None
    tp1_at: datetime | None
    tp2_at: datetime | None
    sl_at: datetime | None
    invalidated_at: datetime | None
    exit_at: datetime | None
    exit_reason: str | None
    pnl_pct: float | None
    duration_seconds: int | None


def setup_group_key(signal: SignalRecord) -> str:
    return "|".join(
        [
            signal.venue,
            signal.symbol,
            signal.setup,
            signal.direction,
            signal.level_id,
        ]
    )


def coin_hashtag(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4].upper()
    if "-" in symbol:
        return symbol.split("-", 1)[0].upper()
    return symbol.upper()


def signal_hashtags(signal: SignalRecord) -> str:
    return f"#{signal.setup.upper()} #{coin_hashtag(signal.symbol)}"


def evaluate_setup_lifecycle(
    signal: SignalRecord,
    candles: list[CandleRecord],
    *,
    analysis_end: datetime | None = None,
    watch_expiry_hours: int = 8,
) -> SetupLifecycle:
    detected_at = _as_utc(signal.detected_at)
    entry_price = float(signal.entry_price)
    stop_price = float(signal.invalidation_price)
    tp1_price = float(signal.targets[0]) if signal.targets else entry_price
    tp2_price = float(signal.targets[1]) if len(signal.targets) > 1 else tp1_price
    is_long = signal.direction == "long"
    ordered_candles = sorted(candles, key=lambda item: _as_utc(item.close_time))
    expiry_at = detected_at + timedelta(hours=watch_expiry_hours)
    analysis_limit = _as_utc(analysis_end) if analysis_end is not None else datetime.now(tz=timezone.utc)

    entry_at: datetime | None = None
    tp1_at: datetime | None = None
    tp2_at: datetime | None = None
    sl_at: datetime | None = None
    invalidated_at: datetime | None = None

    for candle in ordered_candles:
        candle_close_time = _as_utc(candle.close_time)
        entry_hit = candle.high >= entry_price if is_long else candle.low <= entry_price
        tp1_hit = candle.high >= tp1_price if is_long else candle.low <= tp1_price
        tp2_hit = candle.high >= tp2_price if is_long else candle.low <= tp2_price
        stop_hit = candle.low <= stop_price if is_long else candle.high >= stop_price

        if entry_at is None:
            if entry_hit:
                entry_at = candle_close_time
            elif stop_hit:
                invalidated_at = candle_close_time
                return SetupLifecycle(
                    status=SETUP_STATUS_INVALIDATION,
                    call_at=detected_at,
                    entry_at=None,
                    tp1_at=None,
                    tp2_at=None,
                    sl_at=None,
                    invalidated_at=invalidated_at,
                    exit_at=invalidated_at,
                    exit_reason="INVALIDATION",
                    pnl_pct=None,
                    duration_seconds=None,
                )
            else:
                continue

        if entry_at is not None:
            if tp1_hit and tp1_at is None:
                tp1_at = candle_close_time
            if tp2_hit and tp2_at is None:
                tp2_at = candle_close_time
            if stop_hit:
                sl_at = candle_close_time
                return SetupLifecycle(
                    status=SETUP_STATUS_LOSS,
                    call_at=detected_at,
                    entry_at=entry_at,
                    tp1_at=tp1_at,
                    tp2_at=tp2_at,
                    sl_at=sl_at,
                    invalidated_at=None,
                    exit_at=sl_at,
                    exit_reason="SL",
                    pnl_pct=_pnl_percent(signal.direction, entry_price, stop_price),
                    duration_seconds=int((sl_at - entry_at).total_seconds()) if entry_at is not None else None,
                )
            if tp2_at is not None:
                return SetupLifecycle(
                    status=SETUP_STATUS_TP2,
                    call_at=detected_at,
                    entry_at=entry_at,
                    tp1_at=tp1_at or tp2_at,
                    tp2_at=tp2_at,
                    sl_at=None,
                    invalidated_at=None,
                    exit_at=tp2_at,
                    exit_reason="TP2",
                    pnl_pct=_pnl_percent(signal.direction, entry_price, tp2_price),
                    duration_seconds=int((tp2_at - entry_at).total_seconds()) if entry_at is not None else None,
                )

    if entry_at is not None:
        if tp1_at is not None:
            return SetupLifecycle(
                status=SETUP_STATUS_TP1,
                call_at=detected_at,
                entry_at=entry_at,
                tp1_at=tp1_at,
                tp2_at=None,
                sl_at=None,
                invalidated_at=None,
                exit_at=tp1_at,
                exit_reason="TP1",
                pnl_pct=_pnl_percent(signal.direction, entry_price, tp1_price),
                duration_seconds=int((tp1_at - entry_at).total_seconds()) if entry_at is not None else None,
            )
        return SetupLifecycle(
            status=SETUP_STATUS_EXECUTED,
            call_at=detected_at,
            entry_at=entry_at,
            tp1_at=None,
            tp2_at=None,
            sl_at=None,
            invalidated_at=None,
            exit_at=None,
            exit_reason=None,
            pnl_pct=None,
            duration_seconds=None,
        )

    if analysis_limit >= expiry_at:
        return SetupLifecycle(
            status=SETUP_STATUS_INVALIDATION,
            call_at=detected_at,
            entry_at=None,
            tp1_at=None,
            tp2_at=None,
            sl_at=None,
            invalidated_at=expiry_at,
            exit_at=expiry_at,
            exit_reason="EXPIRED",
            pnl_pct=None,
            duration_seconds=None,
        )

    return SetupLifecycle(
        status=SETUP_STATUS_WATCH,
        call_at=detected_at,
        entry_at=None,
        tp1_at=None,
        tp2_at=None,
        sl_at=None,
        invalidated_at=None,
        exit_at=None,
        exit_reason=None,
        pnl_pct=None,
        duration_seconds=None,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def classify_signal_outcome(signal: SignalRecord, candles: list[CandleRecord]) -> str:
    lifecycle = evaluate_setup_lifecycle(signal, candles)
    if lifecycle.status in SUCCESS_OUTCOMES:
        return "success"
    if lifecycle.status in FAILED_OUTCOMES:
        return "failed"
    return "pending"


def render_daily_summary(report_date: date, outcomes: list[SignalOutcome]) -> str:
    total = len(outcomes)
    success = [item for item in outcomes if item.status == "success"]
    failed = [item for item in outcomes if item.status == "failed"]
    pending = [item for item in outcomes if item.status == "pending"]
    resolved = len(success) + len(failed)
    hit_rate = (len(success) / resolved * 100.0) if resolved else 0.0

    lines = [
        f"📊 Сводка за {report_date.strftime('%d.%m.%Y')}",
        "",
        f"Всего сигналов: {total}",
        f"Успешно отработали: {len(success)}",
        f"Не отработали: {len(failed)}",
        f"В ожидании: {len(pending)}",
        f"Процент отработки: {hit_rate:.1f}%",
    ]

    if success:
        lines.extend(["", "Успешные сигналы:"])
        lines.extend(
            f"- {item.signal.symbol} | {item.signal.setup.upper()} | {item.signal.direction.upper()} | {signal_hashtags(item.signal)}"
            for item in success
        )

    if failed:
        lines.extend(["", "Неотработанные сигналы:"])
        lines.extend(
            f"- {item.signal.symbol} | {item.signal.setup.upper()} | {item.signal.direction.upper()} | {signal_hashtags(item.signal)}"
            for item in failed
        )

    if pending:
        lines.extend(["", "Сигналы в ожидании:"])
        lines.extend(
            f"- {item.signal.symbol} | {item.signal.setup.upper()} | {item.signal.direction.upper()} | {signal_hashtags(item.signal)}"
            for item in pending
        )

    if total == 0:
        lines.extend(["", "За сегодня actionable/watchlist сигналов не было."])

    return "\n".join(lines)


def _pnl_percent(direction: str, entry_price: float, exit_price: float | None) -> float | None:
    if exit_price is None or not entry_price:
        return None
    if direction == "short":
        return ((entry_price - exit_price) / entry_price) * 100.0
    return ((exit_price - entry_price) / entry_price) * 100.0
