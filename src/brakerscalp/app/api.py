from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import uvicorn

from brakerscalp.app.common import build_exchange_adapters, build_runtime
from brakerscalp.services.api_service import build_api


async def create_app():
    settings, repository, cache, universe = await build_runtime()
    adapters = build_exchange_adapters(settings)
    @asynccontextmanager
    async def lifespan(_app):
        yield
        await cache.close()
        for adapter in adapters.values():
            await adapter.aclose()

    app = build_api(repository, cache, settings, universe, adapters)
    app.router.lifespan_context = lifespan
    return settings, app


async def amain() -> None:
    settings, app = await create_app()
    config = uvicorn.Config(app, host=settings.api_host, port=settings.api_port, log_level=settings.log_level.lower())
    server = uvicorn.Server(config)
    await server.serve()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
