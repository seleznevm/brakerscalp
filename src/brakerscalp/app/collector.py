from __future__ import annotations

import asyncio

from brakerscalp.app.common import build_exchange_adapters, build_runtime
from brakerscalp.services.collector_service import CollectorService


async def amain() -> None:
    settings, repository, cache, universe = await build_runtime()
    adapters = build_exchange_adapters(settings)
    service = CollectorService(
        adapters,
        repository,
        cache,
        universe,
        settings.poll_interval_seconds,
        exchange_book_depth=settings.exchange_book_depth,
        exchange_trades_limit=settings.exchange_trades_limit,
    )
    try:
        await service.run()
    finally:
        await cache.close()
        for adapter in adapters.values():
            await adapter.aclose()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
