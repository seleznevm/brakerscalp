from __future__ import annotations

from brakerscalp.config import Settings, get_settings
from brakerscalp.exchanges.factory import build_adapters
from brakerscalp.logging import configure_logging
from brakerscalp.storage.cache import StateCache
from brakerscalp.storage.db import create_engine, create_session_factory, init_db
from brakerscalp.storage.repository import Repository
from brakerscalp.universe import load_universe, save_universe


async def build_runtime() -> tuple[Settings, Repository, StateCache, list]:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings.database_url)
    await init_db(engine)
    session_factory = create_session_factory(engine)
    repository = Repository(session_factory)
    cache = StateCache.from_url(settings.redis_url)
    file_universe = [item for item in load_universe(settings.universe_path) if item.primary_venue.value in settings.enabled_venues]
    db_universe = await repository.list_runtime_universe(enabled_venues=settings.enabled_venues)
    merged_map = {item.symbol.upper(): item for item in file_universe}
    for item in db_universe:
        merged_map[item.symbol.upper()] = item
    universe = sorted(merged_map.values(), key=lambda item: item.symbol.upper())
    if universe:
        await repository.replace_runtime_universe(universe)
        if universe != file_universe:
            save_universe(settings.universe_path, universe)
    await cache.store_universe(universe)
    return settings, repository, cache, universe


def build_exchange_adapters(settings: Settings):
    return build_adapters(settings)
