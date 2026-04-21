from __future__ import annotations

from datetime import datetime, timezone

from brakerscalp.domain.models import BookSnapshot, DerivativeContext, MarketCandle, OrderBookLevel, Timeframe, TradeTick, Venue
from brakerscalp.exchanges.base import ExchangeAdapter, ms_to_dt


OKX_INTERVALS = {
    Timeframe.M5: "5m",
    Timeframe.M15: "15m",
    Timeframe.H1: "1H",
    Timeframe.H4: "4H",
}


def to_okx_symbol(symbol: str) -> str:
    base = symbol[:-4]
    return f"{base}-USDT-SWAP"


class OkxAdapter(ExchangeAdapter):
    venue = Venue.OKX
    base_url = "https://www.okx.com"

    async def fetch_recent_candles(self, symbol: str, timeframe: Timeframe, limit: int = 300) -> list[MarketCandle]:
        inst_id = to_okx_symbol(symbol)
        response = await self.client.get(
            "/api/v5/market/candles",
            params={"instId": inst_id, "bar": OKX_INTERVALS[timeframe], "limit": limit},
        )
        response.raise_for_status()
        return self.parse_candles_payload(symbol, timeframe, response.json())

    def parse_candles_payload(self, symbol: str, timeframe: Timeframe, payload: dict) -> list[MarketCandle]:
        candles: list[MarketCandle] = []
        for row in reversed(payload["data"]):
            volume = float(row[5])
            quote_volume = float(row[6]) if len(row) > 6 else 0.0
            candles.append(
                MarketCandle(
                    symbol=symbol,
                    venue=self.venue,
                    timeframe=timeframe,
                    open_time=ms_to_dt(row[0]),
                    close_time=ms_to_dt(int(row[0]) + 1),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=volume,
                    quote_volume=quote_volume,
                    trade_count=0,
                    taker_buy_volume=0.0,
                    vwap=(quote_volume / volume) if volume else float(row[4]),
                )
            )
        return candles

    async def fetch_top_book(self, symbol: str, depth: int = 10) -> BookSnapshot:
        inst_id = to_okx_symbol(symbol)
        response = await self.client.get("/api/v5/market/books", params={"instId": inst_id, "sz": depth})
        response.raise_for_status()
        return self.parse_book_payload(symbol, response.json())

    def parse_book_payload(self, symbol: str, payload: dict) -> BookSnapshot:
        result = payload["data"][0]
        return BookSnapshot(
            symbol=symbol,
            venue=self.venue,
            timestamp=ms_to_dt(result["ts"]),
            sequence_id=result.get("seqId"),
            bids=[OrderBookLevel(price=float(item[0]), size=float(item[1])) for item in result.get("bids", [])],
            asks=[OrderBookLevel(price=float(item[0]), size=float(item[1])) for item in result.get("asks", [])],
        )

    async def fetch_trades(self, symbol: str, limit: int = 50) -> list[TradeTick]:
        inst_id = to_okx_symbol(symbol)
        response = await self.client.get("/api/v5/market/trades", params={"instId": inst_id, "limit": limit})
        response.raise_for_status()
        return self.parse_trades_payload(symbol, response.json())

    def parse_trades_payload(self, symbol: str, payload: dict) -> list[TradeTick]:
        return [
            TradeTick(
                symbol=symbol,
                venue=self.venue,
                timestamp=ms_to_dt(item["ts"]),
                price=float(item["px"]),
                size=float(item["sz"]),
                side=item["side"].lower(),
            )
            for item in payload["data"]
        ]

    async def fetch_derivative_context(self, symbol: str) -> DerivativeContext:
        inst_id = to_okx_symbol(symbol)
        funding_response = await self.client.get("/api/v5/public/funding-rate", params={"instId": inst_id})
        oi_response = await self.client.get("/api/v5/public/open-interest", params={"instType": "SWAP", "instId": inst_id})
        mark_response = await self.client.get("/api/v5/public/mark-price", params={"instType": "SWAP", "instId": inst_id})
        funding_response.raise_for_status()
        oi_response.raise_for_status()
        mark_response.raise_for_status()
        funding = funding_response.json()["data"][0]
        oi = oi_response.json()["data"][0]
        mark_data = mark_response.json()["data"][0]
        mark = float(mark_data["markPx"])
        index = float(mark_data.get("idxPx") or mark)
        return DerivativeContext(
            symbol=symbol,
            venue=self.venue,
            timestamp=datetime.now(tz=timezone.utc),
            funding_rate=float(funding.get("fundingRate", 0.0)),
            open_interest=float(oi.get("oiUsd") or oi.get("oi") or 0.0),
            mark_price=mark,
            index_price=index,
            basis_bps=((mark - index) / index) * 10000 if index else 0.0,
        )
