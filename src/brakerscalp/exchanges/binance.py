from __future__ import annotations

from datetime import datetime, timezone

from brakerscalp.domain.models import BookSnapshot, DerivativeContext, MarketCandle, OrderBookLevel, Timeframe, TradeTick, Venue
from brakerscalp.exchanges.base import ExchangeAdapter, ms_to_dt


BINANCE_INTERVALS = {
    Timeframe.M5: "5m",
    Timeframe.M15: "15m",
    Timeframe.H1: "1h",
    Timeframe.H4: "4h",
}


class BinanceAdapter(ExchangeAdapter):
    venue = Venue.BINANCE
    base_url = "https://fapi.binance.com"

    async def fetch_recent_candles(self, symbol: str, timeframe: Timeframe, limit: int = 300) -> list[MarketCandle]:
        response = await self.client.get(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": BINANCE_INTERVALS[timeframe], "limit": limit},
        )
        response.raise_for_status()
        return self.parse_candles_payload(symbol, timeframe, response.json())

    def parse_candles_payload(self, symbol: str, timeframe: Timeframe, payload: list[list]) -> list[MarketCandle]:
        candles: list[MarketCandle] = []
        for row in payload:
            quote_volume = float(row[7]) if len(row) > 7 else 0.0
            volume = float(row[5])
            candles.append(
                MarketCandle(
                    symbol=symbol,
                    venue=self.venue,
                    timeframe=timeframe,
                    open_time=ms_to_dt(row[0]),
                    close_time=ms_to_dt(row[6]),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=volume,
                    quote_volume=quote_volume,
                    trade_count=int(row[8]) if len(row) > 8 else 0,
                    taker_buy_volume=float(row[9]) if len(row) > 9 else 0.0,
                    vwap=(quote_volume / volume) if volume else float(row[4]),
                )
            )
        return candles

    async def fetch_top_book(self, symbol: str, depth: int = 10) -> BookSnapshot:
        response = await self.client.get("/fapi/v1/depth", params={"symbol": symbol, "limit": depth})
        response.raise_for_status()
        return self.parse_book_payload(symbol, response.json())

    def parse_book_payload(self, symbol: str, payload: dict) -> BookSnapshot:
        return BookSnapshot(
            symbol=symbol,
            venue=self.venue,
            timestamp=datetime.now(tz=timezone.utc),
            sequence_id=str(payload.get("lastUpdateId")),
            bids=[OrderBookLevel(price=float(price), size=float(size)) for price, size in payload.get("bids", [])],
            asks=[OrderBookLevel(price=float(price), size=float(size)) for price, size in payload.get("asks", [])],
        )

    async def fetch_trades(self, symbol: str, limit: int = 50) -> list[TradeTick]:
        response = await self.client.get("/fapi/v1/trades", params={"symbol": symbol, "limit": limit})
        response.raise_for_status()
        return self.parse_trades_payload(symbol, response.json())

    def parse_trades_payload(self, symbol: str, payload: list[dict]) -> list[TradeTick]:
        return [
            TradeTick(
                symbol=symbol,
                venue=self.venue,
                timestamp=ms_to_dt(item["time"]),
                price=float(item["price"]),
                size=float(item["qty"]),
                side=self._trade_side_from_bool(item.get("isBuyerMaker", False)),
            )
            for item in payload
        ]

    async def fetch_derivative_context(self, symbol: str) -> DerivativeContext:
        premium_response = await self.client.get("/fapi/v1/premiumIndex", params={"symbol": symbol})
        oi_response = await self.client.get("/fapi/v1/openInterest", params={"symbol": symbol})
        premium_response.raise_for_status()
        oi_response.raise_for_status()
        premium = premium_response.json()
        oi = oi_response.json()
        mark = float(premium["markPrice"])
        index = float(premium["indexPrice"])
        return DerivativeContext(
            symbol=symbol,
            venue=self.venue,
            timestamp=ms_to_dt(premium["time"]),
            funding_rate=float(premium["lastFundingRate"]),
            open_interest=float(oi["openInterest"]),
            mark_price=mark,
            index_price=index,
            basis_bps=((mark - index) / index) * 10000 if index else 0.0,
        )
