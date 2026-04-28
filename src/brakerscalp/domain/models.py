from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


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
    tick_velocity: float = 0.0
    baseline_tick_velocity: float = 0.0
    tick_velocity_ratio: float = 0.0
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
        return f"{self.lower_price:.4f} - {self.upper_price:.4f}"


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
