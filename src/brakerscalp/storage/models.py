from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, BigInteger, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class CandleRecord(Base):
    __tablename__ = "candles"
    __table_args__ = (
        UniqueConstraint("venue", "symbol", "timeframe", "close_time", name="uq_candle_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timeframe: Mapped[str] = mapped_column(String(8), index=True)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    quote_volume: Mapped[float] = mapped_column(Float, default=0.0)
    trade_count: Mapped[int] = mapped_column(Integer, default=0)
    taker_buy_volume: Mapped[float] = mapped_column(Float, default=0.0)
    vwap: Mapped[float | None] = mapped_column(Float, nullable=True)


class LevelRecord(Base):
    __tablename__ = "levels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    level_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timeframe: Mapped[str] = mapped_column(String(8))
    kind: Mapped[str] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(64))
    lower_price: Mapped[float] = mapped_column(Float)
    upper_price: Mapped[float] = mapped_column(Float)
    reference_price: Mapped[float] = mapped_column(Float)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    touches: Mapped[int] = mapped_column(Integer, default=0)
    age_hours: Mapped[float] = mapped_column(Float, default=0.0)
    strength: Mapped[float] = mapped_column(Float, default=0.0)


class SignalRecord(Base):
    __tablename__ = "signals"
    __table_args__ = (UniqueConstraint("alert_key", name="uq_signal_alert_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    alert_key: Mapped[str] = mapped_column(String(256), index=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timeframe: Mapped[str] = mapped_column(String(8))
    setup: Mapped[str] = mapped_column(String(32))
    direction: Mapped[str] = mapped_column(String(16))
    signal_class: Mapped[str] = mapped_column(String(32), index=True)
    confidence: Mapped[float] = mapped_column(Float)
    level_id: Mapped[str] = mapped_column(String(64), index=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    entry_price: Mapped[float] = mapped_column(Float)
    invalidation_price: Mapped[float] = mapped_column(Float)
    targets: Mapped[list] = mapped_column(JSON)
    expected_rr: Mapped[float] = mapped_column(Float)
    rationale: Mapped[list] = mapped_column(JSON)
    why_not_higher: Mapped[list] = mapped_column(JSON)
    contributions: Mapped[list] = mapped_column(JSON)
    data_health: Mapped[dict] = mapped_column(JSON)
    feature_snapshot: Mapped[dict] = mapped_column(JSON)
    render_context: Mapped[dict] = mapped_column(JSON)


class AlertDeliveryRecord(Base):
    __tablename__ = "alert_deliveries"
    __table_args__ = (
        UniqueConstraint("signal_id", "chat_id", name="uq_delivery_signal_chat"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[str] = mapped_column(String(64), index=True)
    alert_key: Mapped[str] = mapped_column(String(256), index=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    message_thread_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    signal_class: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    message_text: Mapped[str] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class VenueHealthRecord(Base):
    __tablename__ = "venue_health"
    __table_args__ = (
        UniqueConstraint("venue", "symbol", name="uq_venue_symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    is_fresh: Mapped[bool] = mapped_column()
    has_sequence_gap: Mapped[bool] = mapped_column()
    spread_ratio: Mapped[float] = mapped_column(Float)
    freshness_ms: Mapped[int] = mapped_column(Integer)
    reconnect_count: Mapped[int] = mapped_column(Integer)
    notes: Mapped[list] = mapped_column(JSON)
