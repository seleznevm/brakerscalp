from __future__ import annotations

import json
from pathlib import Path

from datetime import datetime, timezone

from brakerscalp.domain.models import BookSnapshot, Timeframe, Venue
from brakerscalp.exchanges.binance import BinanceAdapter
from brakerscalp.exchanges.bybit import BybitAdapter
from brakerscalp.exchanges.okx import OkxAdapter


def read_fixture(name: str) -> dict:
    return json.loads((Path(__file__).parent / "fixtures" / name).read_text(encoding="utf-8"))


def test_binance_book_contract() -> None:
    adapter = BinanceAdapter()
    book = adapter.parse_book_payload("BTCUSDT", read_fixture("binance_book.json"))
    assert book.best_bid == 65000.1
    assert book.best_ask == 65000.2


def test_bybit_book_contract() -> None:
    adapter = BybitAdapter()
    book = adapter.parse_book_payload("BTCUSDT", read_fixture("bybit_book.json"))
    assert book.sequence_id == "91234"
    assert book.bids[0].size == 11.0


def test_okx_book_contract() -> None:
    adapter = OkxAdapter()
    book = adapter.parse_book_payload("BTCUSDT", read_fixture("okx_book.json"))
    assert book.sequence_id == "78901"
    assert book.asks[0].price == 65020.5


def test_book_snapshot_coerces_numeric_sequence_id() -> None:
    book = BookSnapshot.model_validate(
        {
            "symbol": "BTCUSDT",
            "venue": Venue.OKX,
            "timestamp": datetime.now(tz=timezone.utc),
            "sequence_id": 123456,
            "bids": [{"price": 1.0, "size": 2.0}],
            "asks": [{"price": 1.1, "size": 3.0}],
        }
    )
    assert book.sequence_id == "123456"


def test_binance_candle_contract() -> None:
    adapter = BinanceAdapter()
    payload = [
        [1713700000000, "65000", "65100", "64900", "65050", "100", 1713700899999, "6505000", 120, "55", "0"]
    ]
    candles = adapter.parse_candles_payload("BTCUSDT", Timeframe.M15, payload)
    assert candles[0].quote_volume == 6505000.0
    assert candles[0].vwap == 65050.0
