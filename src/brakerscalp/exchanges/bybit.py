from __future__ import annotations

from datetime import datetime, timezone

from brakerscalp.domain.models import BookSnapshot, DerivativeContext, MarketCandle, OrderBookLevel, Timeframe, TradeTick, Venue
from brakerscalp.exchanges.base import ExchangeAdapter, ms_to_dt, timeframe_to_timedelta


BYBIT_INTERVALS = {
    Timeframe.M5: "5",
    Timeframe.M15: "15",
    Timeframe.H1: "60",
    Timeframe.H4: "240",
}

BYBIT_SYMBOL_ALIASES = {
    "PEPEUSDT": "1000PEPEUSDT",
}


def to_bybit_symbol(symbol: str) -> str:
    return BYBIT_SYMBOL_ALIASES.get(symbol.upper(), symbol.upper())


class BybitAdapter(ExchangeAdapter):
    venue = Venue.BYBIT
    base_url = "https://api.bybit.com"

    async def fetch_recent_candles(self, symbol: str, timeframe: Timeframe, limit: int = 300) -> list[MarketCandle]:
        response = await self.client.get(
            "/v5/market/kline",
            params={"category": "linear", "symbol": to_bybit_symbol(symbol), "interval": BYBIT_INTERVALS[timeframe], "limit": limit},
        )
        response.raise_for_status()
        return self.parse_candles_payload(symbol, timeframe, response.json())

    def parse_candles_payload(self, symbol: str, timeframe: Timeframe, payload: dict) -> list[MarketCandle]:
        result = self._require_result(payload)
        rows = result.get("list")
        if not isinstance(rows, list):
            raise ValueError(f"Bybit kline payload is missing result.list for {symbol}")
        candles: list[MarketCandle] = []
        for row in reversed(rows):
            volume = float(row[5])
            quote_volume = float(row[6])
            candles.append(
                MarketCandle(
                    symbol=symbol,
                    venue=self.venue,
                    timeframe=timeframe,
                    open_time=ms_to_dt(row[0]),
                    close_time=ms_to_dt(row[0]) + timeframe_to_timedelta(timeframe),
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
        response = await self.client.get(
            "/v5/market/orderbook",
            params={"category": "linear", "symbol": to_bybit_symbol(symbol), "limit": depth},
        )
        response.raise_for_status()
        return self.parse_book_payload(symbol, response.json())

    def parse_book_payload(self, symbol: str, payload: dict) -> BookSnapshot:
        result = self._require_result(payload)
        return BookSnapshot(
            symbol=symbol,
            venue=self.venue,
            timestamp=ms_to_dt(result["ts"]),
            sequence_id=str(result.get("u")),
            bids=[OrderBookLevel(price=float(price), size=float(size)) for price, size in result.get("b", [])],
            asks=[OrderBookLevel(price=float(price), size=float(size)) for price, size in result.get("a", [])],
        )

    async def fetch_trades(self, symbol: str, limit: int = 50) -> list[TradeTick]:
        response = await self.client.get(
            "/v5/market/recent-trade",
            params={"category": "linear", "symbol": to_bybit_symbol(symbol), "limit": limit},
        )
        response.raise_for_status()
        return self.parse_trades_payload(symbol, response.json())

    def parse_trades_payload(self, symbol: str, payload: dict) -> list[TradeTick]:
        result = self._require_result(payload)
        rows = result.get("list")
        if not isinstance(rows, list):
            raise ValueError(f"Bybit trades payload is missing result.list for {symbol}")
        return [
            TradeTick(
                symbol=symbol,
                venue=self.venue,
                timestamp=ms_to_dt(item["time"]),
                price=float(item["price"]),
                size=float(item["size"]),
                side=item["side"].lower(),
            )
            for item in rows
        ]

    async def fetch_derivative_context(self, symbol: str) -> DerivativeContext:
        response = await self.client.get(
            "/v5/market/tickers",
            params={"category": "linear", "symbol": to_bybit_symbol(symbol)},
        )
        response.raise_for_status()
        result_payload = self._require_result(response.json())
        rows = result_payload.get("list")
        if not rows:
            raise ValueError(f"Bybit tickers payload is empty for {symbol}")
        result = rows[0]
        mark = float(result["markPrice"])
        index = float(result["indexPrice"])
        return DerivativeContext(
            symbol=symbol,
            venue=self.venue,
            timestamp=datetime.now(tz=timezone.utc),
            funding_rate=float(result.get("fundingRate", 0.0)),
            open_interest=float(result.get("openInterestValue") or result.get("openInterest") or 0.0),
            mark_price=mark,
            index_price=index,
            basis_bps=((mark - index) / index) * 10000 if index else 0.0,
        )

    def _require_result(self, payload: dict) -> dict:
        ret_code = int(payload.get("retCode", 0))
        if ret_code != 0:
            raise ValueError(f"Bybit API error {ret_code}: {payload.get('retMsg', 'unknown error')}")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise ValueError("Bybit API payload is missing result object")
        return result
