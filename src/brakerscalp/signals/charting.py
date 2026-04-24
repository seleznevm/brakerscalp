from __future__ import annotations

from io import BytesIO
from typing import Protocol

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle


class CandleLike(Protocol):
    open_time: object
    open: float
    high: float
    low: float
    close: float


class SignalLike(Protocol):
    symbol: str
    entry_price: float
    invalidation_price: float
    targets: list[float]
    render_context: dict


def render_signal_chart(candles: list[CandleLike], signal: SignalLike) -> bytes | None:
    if len(candles) < 5:
        return None

    window = candles[-48:]
    level_lower = float(signal.render_context.get("level_lower", min(item.low for item in window)))
    level_upper = float(signal.render_context.get("level_upper", max(item.high for item in window)))
    entry = float(signal.entry_price)
    stop = float(signal.invalidation_price)
    tp1 = float(signal.targets[0]) if signal.targets else entry
    tp2 = float(signal.targets[1]) if len(signal.targets) > 1 else tp1

    fig, ax = plt.subplots(figsize=(11, 5), constrained_layout=True)
    fig.patch.set_facecolor("#081018")
    ax.set_facecolor("#0f1720")
    ax.grid(True, color="#29404f", alpha=0.35, linewidth=0.6)
    ax.tick_params(colors="#d7e3ea", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#3c5566")

    x_values = [mdates.date2num(item.open_time) for item in window]
    candle_width = 0.007
    for x_pos, candle in zip(x_values, window, strict=True):
        color = "#35c48f" if candle.close >= candle.open else "#ff6b6b"
        ax.vlines(x_pos, candle.low, candle.high, color=color, linewidth=1.2, alpha=0.9)
        body_low = min(candle.open, candle.close)
        body_height = max(abs(candle.close - candle.open), 1e-9)
        ax.add_patch(
            Rectangle(
                (x_pos - candle_width / 2, body_low),
                candle_width,
                body_height,
                facecolor=color,
                edgecolor=color,
                linewidth=1.0,
            )
        )

    ax.axhspan(level_lower, level_upper, color="#f6c343", alpha=0.12, label="Level zone")
    ax.axhline(entry, color="#3fa7ff", linestyle="-", linewidth=1.2, label="Entry")
    ax.axhline(stop, color="#ff6b6b", linestyle="--", linewidth=1.2, label="SL")
    ax.axhline(tp1, color="#35c48f", linestyle="--", linewidth=1.1, label="TP1")
    ax.axhline(tp2, color="#8bd450", linestyle=":", linewidth=1.1, label="TP2")

    signal_index = len(window) - 1
    ax.scatter([x_values[signal_index]], [entry], color="#ffd166", s=46, zorder=5)
    ax.annotate(
        "Entry",
        (x_values[signal_index], entry),
        xytext=(10, 12),
        textcoords="offset points",
        color="#f7fbff",
        fontsize=8,
        bbox={"boxstyle": "round,pad=0.2", "fc": "#12212b", "ec": "#2c4759", "alpha": 0.95},
    )

    ax.set_title(f"{signal.symbol} breakout scalp", color="#f7fbff", fontsize=12, pad=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    ax.tick_params(axis="x", rotation=20)
    ax.legend(facecolor="#12212b", edgecolor="#2c4759", labelcolor="#d7e3ea", fontsize=8, loc="upper left")

    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=140)
    plt.close(fig)
    return buffer.getvalue()
