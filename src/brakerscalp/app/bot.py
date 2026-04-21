from __future__ import annotations

import asyncio

from brakerscalp.app.common import build_runtime
from brakerscalp.services.bot_service import BotService


async def amain() -> None:
    settings, repository, cache, _ = await build_runtime()
    service = BotService(settings, repository, cache)
    try:
        await service.run()
    finally:
        await service.shutdown()
        await cache.close()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
