from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis

from brakerscalp.domain.models import AlertMessage, BookSnapshot, DataHealth, DerivativeContext, MarketCandle, TradeTick, UniverseSymbol
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

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

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

    async def outbox_size(self) -> int:
        return int(await self.redis.llen(self._key("outbox")))

    async def set_chat_muted(self, chat_id: int, muted: bool) -> None:
        key = self._key("chat-muted", str(chat_id))
        if muted:
            await self.redis.set(key, b"1")
        else:
            await self.redis.delete(key)

    async def is_chat_muted(self, chat_id: int) -> bool:
        return bool(await self.redis.exists(self._key("chat-muted", str(chat_id))))

    async def store_service_heartbeat(self, service: str, payload: dict[str, Any], ttl_seconds: int = 180) -> None:
        data = {
            **payload,
            "service": service,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
        await self.set_json(self._key("heartbeat", service), data, ex=ttl_seconds)

    async def get_service_heartbeat(self, service: str) -> dict[str, Any] | None:
        return await self.get_json(self._key("heartbeat", service))

    async def set_minimum_alert_confidence(self, value: float) -> None:
        await self.redis.set(self._key("runtime", "minimum-alert-confidence"), str(float(value)).encode("utf-8"))

    async def get_minimum_alert_confidence(self, default: float) -> float:
        raw = await self.redis.get(self._key("runtime", "minimum-alert-confidence"))
        if raw in (None, b"", ""):
            return float(default)
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return float(raw)
        except (TypeError, ValueError):
            return float(default)

    async def set_risk_usdt(self, value: float) -> None:
        await self.redis.set(self._key("runtime", "risk-usdt"), str(float(value)).encode("utf-8"))

    async def get_risk_usdt(self, default: float) -> float:
        raw = await self.redis.get(self._key("runtime", "risk-usdt"))
        if raw in (None, b"", ""):
            return float(default)
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return float(raw)
        except (TypeError, ValueError):
            return float(default)

    async def store_universe(self, symbols: list[UniverseSymbol]) -> None:
        await self.set_json(
            self._key("runtime", "universe"),
            [item.model_dump(mode="json") for item in symbols],
        )

    async def get_universe(self) -> list[dict]:
        return await self.get_json(self._key("runtime", "universe")) or []

    async def get_universe_symbols(self, fallback: list[UniverseSymbol] | None = None) -> list[UniverseSymbol]:
        raw = await self.get_universe()
        if raw:
            return [UniverseSymbol.model_validate(item) for item in raw]
        return list(fallback or [])
