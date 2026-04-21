from __future__ import annotations

from brakerscalp.config import Settings
from brakerscalp.domain.models import Venue
from brakerscalp.exchanges.base import ExchangeAdapter
from brakerscalp.exchanges.binance import BinanceAdapter
from brakerscalp.exchanges.bybit import BybitAdapter
from brakerscalp.exchanges.okx import OkxAdapter


def build_adapters(settings: Settings) -> dict[Venue, ExchangeAdapter]:
    adapters: dict[Venue, ExchangeAdapter] = {}
    timeout = settings.exchange_request_timeout_seconds
    if settings.enable_binance:
        adapters[Venue.BINANCE] = BinanceAdapter(timeout_seconds=timeout)
    if settings.enable_bybit:
        adapters[Venue.BYBIT] = BybitAdapter(timeout_seconds=timeout)
    if settings.enable_okx:
        adapters[Venue.OKX] = OkxAdapter(timeout_seconds=timeout)
    return adapters
