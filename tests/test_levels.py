from __future__ import annotations

from brakerscalp.domain.models import Timeframe, Venue
from brakerscalp.signals.levels import LevelDetector


def test_detect_levels(make_candles) -> None:
    detector = LevelDetector()
    candles_4h = make_candles(timeframe=Timeframe.H4, count=40, step=50)
    candles_1h = make_candles(timeframe=Timeframe.H1, count=200, step=12)
    levels = detector.detect("BTCUSDT", Venue.BINANCE, candles_4h, candles_1h)
    assert levels
    assert any(level.kind.value == "resistance" for level in levels)


def test_level_ids_are_stable(make_candles) -> None:
    detector = LevelDetector()
    candles_4h = make_candles(timeframe=Timeframe.H4, count=40, step=50)
    candles_1h = make_candles(timeframe=Timeframe.H1, count=200, step=12)
    levels_first = detector.detect("BTCUSDT", Venue.BINANCE, candles_4h, candles_1h)
    levels_second = detector.detect("BTCUSDT", Venue.BINANCE, candles_4h, candles_1h)
    ids_first = [item.level_id for item in levels_first]
    ids_second = [item.level_id for item in levels_second]
    assert ids_first == ids_second
