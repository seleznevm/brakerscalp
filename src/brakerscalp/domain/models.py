from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def suggest_price_decimals(*values: float | int | None) -> int:
    numeric = [abs(float(value)) for value in values if value not in (None, 0)]
    if not numeric:
        return 4
    reference = min(numeric)
    if reference >= 1000:
        return 2
    if reference >= 100:
        return 3
    if reference >= 1:
        return 4
    if reference >= 0.1:
        return 5
    if reference >= 0.01:
        return 6
    if reference >= 0.001:
        return 7
    return 8


def format_price(value: float, *references: float | int | None) -> str:
    decimals = suggest_price_decimals(value, *references)
    return f"{float(value):.{decimals}f}"


def format_price_range(lower: float, upper: float) -> str:
    decimals = suggest_price_decimals(lower, upper)
    return f"{float(lower):.{decimals}f} - {float(upper):.{decimals}f}"


class Venue(StrEnum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"


class Timeframe(StrEnum):
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"
    H4 = "4h"


class LevelKind(StrEnum):
    SUPPORT = "support"
    RESISTANCE = "resistance"


class SetupType(StrEnum):
    BREAKOUT = "breakout"
    BOUNCE = "bounce"


class Direction(StrEnum):
    LONG = "long"
    SHORT = "short"


class SignalClass(StrEnum):
    PRE_ALERT = "pre_alert"
    ACTIONABLE = "actionable"
    WATCHLIST = "watchlist"
    SUPPRESSED = "suppressed"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderBookLevel(BaseModel):
    price: float
    size: float


class MarketCandle(BaseModel):
    symbol: str
    venue: Venue
    timeframe: Timeframe
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float = 0.0
    trade_count: int = 0
    taker_buy_volume: float = 0.0
    vwap: float | None = None


class BookSnapshot(BaseModel):
    symbol: str
    venue: Venue
    timestamp: datetime
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    sequence_id: str | None = None
    is_gap: bool = False

    @field_validator("sequence_id", mode="before")
    @classmethod
    def coerce_sequence_id(cls, value):
        if value in (None, ""):
            return None
        return str(value)

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 0.0

    @property
    def spread(self) -> float:
        if not self.bids or not self.asks:
            return 0.0
        return max(self.best_ask - self.best_bid, 0.0)


class TradeTick(BaseModel):
    symbol: str
    venue: Venue
    timestamp: datetime
    price: float
    size: float
    side: Side


class OrderFlowSnapshot(BaseModel):
    symbol: str
    venue: Venue
    timestamp: datetime = Field(default_factory=utcnow)
    delta_ratio: float = 0.0
    cvd_slope: float = 0.0
    tick_qty_per_5s: int = 0
    tick_window_seconds: int = 5
    recent_trade_count: int = 0
    baseline_trade_count: int = 0


class DerivativeContext(BaseModel):
    symbol: str
    venue: Venue
    timestamp: datetime
    funding_rate: float = 0.0
    open_interest: float = 0.0
    mark_price: float = 0.0
    index_price: float = 0.0
    basis_bps: float = 0.0


class DataHealth(BaseModel):
    venue: Venue
    symbol: str
    timestamp: datetime = Field(default_factory=utcnow)
    is_fresh: bool = True
    has_sequence_gap: bool = False
    spread_ratio: float = 1.0
    freshness_ms: int = 0
    reconnect_count: int = 0
    notes: list[str] = Field(default_factory=list)


class LevelCandidate(BaseModel):
    level_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    venue: Venue
    timeframe: Timeframe
    kind: LevelKind
    source: str
    lower_price: float
    upper_price: float
    reference_price: float
    detected_at: datetime = Field(default_factory=utcnow)
    touches: int = 0
    age_hours: float = 0.0
    strength: float = 0.0

    @property
    def zone_text(self) -> str:
        return format_price_range(self.lower_price, self.upper_price)


class ScoreContribution(BaseModel):
    group: str
    score: float
    max_score: float
    reason: str


class SignalDecision(BaseModel):
    decision_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    venue: Venue
    timeframe: Timeframe
    setup: SetupType
    direction: Direction
    signal_class: SignalClass
    confidence: float
    level_id: str
    alert_key: str
    detected_at: datetime = Field(default_factory=utcnow)
    entry_price: float
    invalidation_price: float
    targets: list[float]
    expected_rr: float
    rationale: list[str]
    why_not_higher: list[str]
    contributions: list[ScoreContribution]
    data_health: DataHealth
    feature_snapshot: dict[str, Any]
    render_context: dict[str, Any] = Field(default_factory=dict)


class AlertMessage(BaseModel):
    signal_id: str
    alert_key: str
    chat_id: int
    message_thread_id: int | None = None
    text: str
    signal_class: SignalClass
    created_at: datetime = Field(default_factory=utcnow)


class UniverseSymbol(BaseModel):
    symbol: str
    primary_venue: Venue
