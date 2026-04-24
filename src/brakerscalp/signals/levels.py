from __future__ import annotations

from hashlib import sha1
from datetime import timedelta

from brakerscalp.domain.models import LevelCandidate, LevelKind, MarketCandle, Timeframe, Venue
from brakerscalp.signals.indicators import average_true_range, local_extrema, rolling_vwap


class LevelDetector:
    def __init__(self, zone_atr_fraction: float = 0.15) -> None:
        self.zone_atr_fraction = zone_atr_fraction

    def detect(self, symbol: str, venue: Venue, candles_4h: list[MarketCandle], candles_1h: list[MarketCandle]) -> list[LevelCandidate]:
        if len(candles_1h) < 30 or len(candles_4h) < 10:
            return []

        atr = average_true_range(candles_1h[-30:])
        zone_half_width = max(atr * self.zone_atr_fraction, candles_1h[-1].close * 0.001)
        levels: list[LevelCandidate] = []

        closes = [item.close for item in candles_4h]
        highs = [item.high for item in candles_4h]
        lows = [item.low for item in candles_4h]
        high_indexes, low_indexes = local_extrema(closes, window=2)

        for index in high_indexes[-4:]:
            price = highs[index]
            levels.append(
                self._make_level(
                    symbol=symbol,
                    venue=venue,
                    timeframe=Timeframe.H4,
                    kind=LevelKind.RESISTANCE,
                    source="swing-high",
                    reference_price=price,
                    zone_half_width=zone_half_width,
                    candle=candles_4h[index],
                    strength=0.65,
                )
            )
        for index in low_indexes[-4:]:
            price = lows[index]
            levels.append(
                self._make_level(
                    symbol=symbol,
                    venue=venue,
                    timeframe=Timeframe.H4,
                    kind=LevelKind.SUPPORT,
                    source="swing-low",
                    reference_price=price,
                    zone_half_width=zone_half_width,
                    candle=candles_4h[index],
                    strength=0.65,
                )
            )

        prev_day = candles_1h[-24:]
        prev_week = candles_1h[-24 * 7 :] if len(candles_1h) >= 24 * 7 else candles_1h
        levels.extend(
            [
                self._make_level(symbol, venue, Timeframe.H1, LevelKind.RESISTANCE, "prev-day-high", max(item.high for item in prev_day), zone_half_width, prev_day[-1], 0.8),
                self._make_level(symbol, venue, Timeframe.H1, LevelKind.SUPPORT, "prev-day-low", min(item.low for item in prev_day), zone_half_width, prev_day[-1], 0.8),
                self._make_level(symbol, venue, Timeframe.H1, LevelKind.RESISTANCE, "prev-week-high", max(item.high for item in prev_week), zone_half_width, prev_week[-1], 0.9),
                self._make_level(symbol, venue, Timeframe.H1, LevelKind.SUPPORT, "prev-week-low", min(item.low for item in prev_week), zone_half_width, prev_week[-1], 0.9),
            ]
        )

        levels.extend(self._cascade_levels(symbol, venue, candles_1h, zone_half_width))

        vwap = rolling_vwap(candles_1h[-48:], period=min(48, len(candles_1h[-48:])))
        levels.append(self._make_level(symbol, venue, Timeframe.H1, LevelKind.SUPPORT, "vwap-zone", vwap, zone_half_width, candles_1h[-1], 0.6))
        levels.append(self._make_level(symbol, venue, Timeframe.H1, LevelKind.RESISTANCE, "vwap-zone", vwap, zone_half_width, candles_1h[-1], 0.6))

        deduped: dict[str, LevelCandidate] = {}
        for level in levels:
            deduped[level.level_id] = level
        return list(deduped.values())

    def _make_level(
        self,
        symbol: str,
        venue: Venue,
        timeframe: Timeframe,
        kind: LevelKind,
        source: str,
        reference_price: float,
        zone_half_width: float,
        candle: MarketCandle,
        strength: float,
    ) -> LevelCandidate:
        return LevelCandidate(
            level_id=self._stable_level_id(symbol, venue, timeframe, kind, source, reference_price),
            symbol=symbol,
            venue=venue,
            timeframe=timeframe,
            kind=kind,
            source=source,
            lower_price=reference_price - zone_half_width,
            upper_price=reference_price + zone_half_width,
            reference_price=reference_price,
            detected_at=candle.close_time + timedelta(seconds=1),
            touches=1,
            age_hours=0.0,
            strength=strength,
        )

    def _stable_level_id(
        self,
        symbol: str,
        venue: Venue,
        timeframe: Timeframe,
        kind: LevelKind,
        source: str,
        reference_price: float,
    ) -> str:
        payload = f"{venue.value}|{symbol}|{timeframe.value}|{kind.value}|{source}|{reference_price:.4f}"
        return sha1(payload.encode("utf-8")).hexdigest()[:24]

    def _cascade_levels(
        self,
        symbol: str,
        venue: Venue,
        candles_1h: list[MarketCandle],
        zone_half_width: float,
    ) -> list[LevelCandidate]:
        recent = candles_1h[-36:]
        if len(recent) < 12:
            return []

        resistance_clusters = self._cluster_prices([item.high for item in recent], tolerance=zone_half_width * 1.1)
        support_clusters = self._cluster_prices([item.low for item in recent], tolerance=zone_half_width * 1.1)
        levels: list[LevelCandidate] = []

        for cluster in resistance_clusters:
            if len(cluster) < 2:
                continue
            reference_price = sum(cluster) / len(cluster)
            strength = min(0.72 + len(cluster) * 0.06, 0.95)
            level = self._make_level(
                symbol=symbol,
                venue=venue,
                timeframe=Timeframe.H1,
                kind=LevelKind.RESISTANCE,
                source="cascade-high",
                reference_price=reference_price,
                zone_half_width=zone_half_width,
                candle=recent[-1],
                strength=strength,
            )
            level.touches = len(cluster)
            levels.append(level)

        for cluster in support_clusters:
            if len(cluster) < 2:
                continue
            reference_price = sum(cluster) / len(cluster)
            strength = min(0.72 + len(cluster) * 0.06, 0.95)
            level = self._make_level(
                symbol=symbol,
                venue=venue,
                timeframe=Timeframe.H1,
                kind=LevelKind.SUPPORT,
                source="cascade-low",
                reference_price=reference_price,
                zone_half_width=zone_half_width,
                candle=recent[-1],
                strength=strength,
            )
            level.touches = len(cluster)
            levels.append(level)

        return levels

    def _cluster_prices(self, prices: list[float], tolerance: float) -> list[list[float]]:
        if not prices:
            return []
        clusters: list[list[float]] = []
        for price in sorted(prices):
            matched = False
            for cluster in clusters:
                center = sum(cluster) / len(cluster)
                if abs(price - center) <= tolerance:
                    cluster.append(price)
                    matched = True
                    break
            if not matched:
                clusters.append([price])
        return clusters
