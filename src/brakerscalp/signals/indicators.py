from __future__ import annotations

import math
from collections import deque

from brakerscalp.domain.models import MarketCandle


def average_true_range(candles: list[MarketCandle], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    true_ranges: list[float] = []
    previous_close = candles[0].close
    for candle in candles[1:]:
        tr = max(
            candle.high - candle.low,
            abs(candle.high - previous_close),
            abs(candle.low - previous_close),
        )
        true_ranges.append(tr)
        previous_close = candle.close
    window = true_ranges[-period:] if len(true_ranges) >= period else true_ranges
    return sum(window) / len(window) if window else 0.0


def volume_zscore(candles: list[MarketCandle], period: int = 20) -> float:
    if len(candles) < 2:
        return 0.0
    volumes = [item.volume for item in candles[-period - 1 : -1]]
    current = candles[-1].volume
    if not volumes:
        return 0.0
    mean = sum(volumes) / len(volumes)
    variance = sum((value - mean) ** 2 for value in volumes) / len(volumes)
    std = math.sqrt(variance)
    if std == 0:
        return 0.0
    return (current - mean) / std


def rolling_vwap(candles: list[MarketCandle], period: int = 20) -> float:
    window = candles[-period:] if len(candles) >= period else candles
    numerator = sum((item.close * item.volume) for item in window)
    denominator = sum(item.volume for item in window)
    return numerator / denominator if denominator else (window[-1].close if window else 0.0)


def median_spread(spreads: list[float]) -> float:
    if not spreads:
        return 0.0
    ordered = sorted(spreads)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def local_extrema(values: list[float], window: int = 2) -> tuple[list[int], list[int]]:
    highs: list[int] = []
    lows: list[int] = []
    series = deque(values)
    if len(series) < (window * 2 + 1):
        return highs, lows
    for index in range(window, len(values) - window):
        point = values[index]
        left = values[index - window : index]
        right = values[index + 1 : index + 1 + window]
        if point >= max(left + right):
            highs.append(index)
        if point <= min(left + right):
            lows.append(index)
    return highs, lows

