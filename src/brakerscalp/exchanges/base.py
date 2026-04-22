from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from brakerscalp.domain.models import BookSnapshot, DataHealth, DerivativeContext, MarketCandle, Side, Timeframe, TradeTick, Venue


def ms_to_dt(value: int | str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def timeframe_to_timedelta(timeframe: Timeframe) -> timedelta:
    mapping = {
        Timeframe.M5: timedelta(minutes=5),
        Timeframe.M15: timedelta(minutes=15),
        Timeframe.H1: timedelta(hours=1),
        Timeframe.H4: timedelta(hours=4),
    }
    return mapping[timeframe]


class ExchangeAdapter(ABC):
    venue: Venue
    base_url: str

    def __init__(self, timeout_seconds: float = 10.0, base_url: str | None = None) -> None:
        self.client = httpx.AsyncClient(base_url=base_url or self.base_url, timeout=timeout_seconds)

    async def aclose(self) -> None:
        await self.client.aclose()

    @abstractmethod
    async def fetch_recent_candles(self, symbol: str, timeframe: Timeframe, limit: int = 300) -> list[MarketCandle]:
        raise NotImplementedError

    @abstractmethod
    async def fetch_top_book(self, symbol: str, depth: int = 10) -> BookSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def fetch_trades(self, symbol: str, limit: int = 50) -> list[TradeTick]:
        raise NotImplementedError

    @abstractmethod
    async def fetch_derivative_context(self, symbol: str) -> DerivativeContext:
        raise NotImplementedError

    async def healthcheck(self, symbol: str) -> DataHealth:
        book = await self.fetch_top_book(symbol, depth=5)
        mid = (book.best_bid + book.best_ask) / 2 if book.best_bid and book.best_ask else 0.0
        spread_ratio = (book.spread / mid) if mid else 99.0
        return DataHealth(
            venue=self.venue,
            symbol=symbol,
            freshness_ms=max(int((datetime.now(tz=timezone.utc) - book.timestamp).total_seconds() * 1000), 0),
            spread_ratio=spread_ratio,
            is_fresh=True,
            has_sequence_gap=book.is_gap,
        )

    async def resync(self, symbol: str) -> None:
        await self.fetch_top_book(symbol, depth=10)

    @staticmethod
    def _trade_side_from_bool(is_buyer_maker: bool) -> Side:
        return Side.SELL if is_buyer_maker else Side.BUY

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
