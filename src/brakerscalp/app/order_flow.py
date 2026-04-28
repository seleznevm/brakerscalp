from __future__ import annotations

import asyncio

from brakerscalp.app.common import build_runtime
from brakerscalp.services.order_flow_service import OrderFlowAnalyzerService


async def amain() -> None:
    settings, repository, cache, universe = await build_runtime()
    service = OrderFlowAnalyzerService(
        repository=repository,
        cache=cache,
        universe=universe,
        alert_chat_ids=settings.effective_alert_chat_ids,
        interval_seconds=settings.order_flow_interval_seconds,
        alert_message_thread_id=settings.alert_message_thread_id,
        strategy_defaults=settings.default_strategy_config(),
    )
    try:
        await service.run()
    finally:
        await cache.close()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
