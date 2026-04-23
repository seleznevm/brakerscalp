from __future__ import annotations

from typing import Any

from redis.asyncio import Redis

from brakerscalp.domain.models import AlertMessage, BookSnapshot, DataHealth, DerivativeContext, MarketCandle, TradeTick
from brakerscalp.serialization import dumps, loads


class StateCache:
    def __init__(self, redis: Redis):
        self.redis = redis

    @classmethod
    def from_url(cls, url: str) -> "StateCache":
        return cls(Redis.from_url(url, decode_responses=False))

    async def close(self) -> None:
        await self.redis.aclose()

    def _key(self, *parts: str) -> str:
        return ":".join(["brakerscalp", *parts])

    async def set_json(self, key: str, value: Any, ex: int | None = None) -> None:
        await self.redis.set(key, dumps(value), ex=ex)

    async def get_json(self, key: str) -> Any:
        return loads(await self.redis.get(key))

    async def store_candles(self, venue: str, symbol: str, timeframe: str, candles: list[MarketCandle]) -> None:
        await self.set_json(self._key("candles", venue, symbol, timeframe), [item.model_dump(mode="json") for item in candles], ex=7200)

    async def get_candles(self, venue: str, symbol: str, timeframe: str) -> list[dict]:
        return await self.get_json(self._key("candles", venue, symbol, timeframe)) or []

    async def store_book(self, venue: str, symbol: str, book: BookSnapshot) -> None:
        await self.set_json(self._key("book", venue, symbol), book.model_dump(mode="json"), ex=300)

    async def get_book(self, venue: str, symbol: str) -> dict | None:
        return await self.get_json(self._key("book", venue, symbol))

    async def store_derivative_context(self, venue: str, symbol: str, context: DerivativeContext) -> None:
        await self.set_json(self._key("derivatives", venue, symbol), context.model_dump(mode="json"), ex=600)

    async def get_derivative_context(self, venue: str, symbol: str) -> dict | None:
        return await self.get_json(self._key("derivatives", venue, symbol))

    async def store_trades(self, venue: str, symbol: str, trades: list[TradeTick]) -> None:
        await self.set_json(self._key("trades", venue, symbol), [item.model_dump(mode="json") for item in trades], ex=600)

    async def get_trades(self, venue: str, symbol: str) -> list[dict]:
        return await self.get_json(self._key("trades", venue, symbol)) or []

    async def store_health(self, venue: str, symbol: str, health: DataHealth) -> None:
        await self.set_json(self._key("health", venue, symbol), health.model_dump(mode="json"), ex=600)

    async def get_health(self, venue: str, symbol: str) -> dict | None:
        return await self.get_json(self._key("health", venue, symbol))

    async def acquire_alert_key(self, alert_key: str, ttl_seconds: int = 14400) -> bool:
        return bool(await self.redis.set(self._key("dedupe", alert_key), b"1", nx=True, ex=ttl_seconds))

    async def acquire_once_key(self, namespace: str, key: str, ttl_seconds: int) -> bool:
        return bool(await self.redis.set(self._key(namespace, key), b"1", nx=True, ex=ttl_seconds))

    async def enqueue_alert(self, alert: AlertMessage) -> None:
        await self.redis.rpush(self._key("outbox"), dumps(alert.model_dump(mode="json")))

    async def pop_alert(self, timeout: int = 5) -> AlertMessage | None:
        item = await self.redis.blpop(self._key("outbox"), timeout=timeout)
        if item is None:
            return None
        _, payload = item
        return AlertMessage.model_validate(loads(payload))

    async def set_chat_muted(self, chat_id: int, muted: bool) -> None:
        key = self._key("chat-muted", str(chat_id))
        if muted:
            await self.redis.set(key, b"1")
        else:
            await self.redis.delete(key)

    async def is_chat_muted(self, chat_id: int) -> bool:
        return bool(await self.redis.exists(self._key("chat-muted", str(chat_id))))
