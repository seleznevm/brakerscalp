from __future__ import annotations

import asyncio

from brakerscalp.app.common import build_runtime
from brakerscalp.services.engine_service import EngineService


async def amain() -> None:
    settings, repository, cache, universe = await build_runtime()
    service = EngineService(
        repository=repository,
        cache=cache,
        universe=universe,
        alert_chat_ids=settings.effective_alert_chat_ids,
        interval_seconds=settings.engine_interval_seconds,
        signal_dedupe_ttl_seconds=settings.signal_dedupe_ttl_seconds,
        alert_message_thread_id=settings.alert_message_thread_id,
    )
    try:
        await service.run()
    finally:
        await cache.close()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
