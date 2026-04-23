from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from brakerscalp.storage.models import CandleRecord, SignalRecord


@dataclass(slots=True)
class SignalOutcome:
    signal: SignalRecord
    status: str


def coin_hashtag(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4].upper()
    if "-" in symbol:
        return symbol.split("-", 1)[0].upper()
    return symbol.upper()


def signal_hashtags(signal: SignalRecord) -> str:
    return f"#{signal.setup.upper()} #{coin_hashtag(signal.symbol)}"


def classify_signal_outcome(signal: SignalRecord, candles: list[CandleRecord]) -> str:
    first_target = float(signal.targets[0]) if signal.targets else float(signal.entry_price)
    for candle in candles:
        if signal.direction == "long":
            target_hit = candle.high >= first_target
            invalidated = candle.low <= signal.invalidation_price
        else:
            target_hit = candle.low <= first_target
            invalidated = candle.high >= signal.invalidation_price

        if target_hit and invalidated:
            return "failed"
        if target_hit:
            return "success"
        if invalidated:
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
