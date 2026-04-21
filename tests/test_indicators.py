from __future__ import annotations

from brakerscalp.signals.indicators import average_true_range, rolling_vwap, volume_zscore


def test_average_true_range(make_candles) -> None:
    candles = make_candles(count=20)
    atr = average_true_range(candles, period=14)
    assert atr > 0


def test_volume_zscore(make_candles) -> None:
    candles = make_candles(count=25)
    candles[-1].volume = candles[-1].volume * 3
    score = volume_zscore(candles, period=20)
    assert score > 1


def test_rolling_vwap(make_candles) -> None:
    candles = make_candles(count=10)
    vwap = rolling_vwap(candles, period=10)
    assert vwap > 0
