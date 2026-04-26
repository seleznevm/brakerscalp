from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha1
from typing import Any

from sqlalchemy import Select, delete, desc, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from brakerscalp.domain.models import AlertMessage, DataHealth, LevelCandidate, MarketCandle, SignalDecision, UniverseSymbol, Venue
from brakerscalp.storage.models import (
    AlertDeliveryRecord,
    CandleRecord,
    LevelRecord,
    RuntimeUniverseRecord,
    SignalRecord,
    StatisticsBySymbolRecord,
    VenueHealthRecord,
)


class Repository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    @staticmethod
    def statistics_snapshot_key(start_at: datetime, end_at: datetime, symbol_query: str | None = None) -> str:
        normalized_query = (symbol_query or "").strip().upper()
        raw = f"{start_at.isoformat()}|{end_at.isoformat()}|{normalized_query}"
        return sha1(raw.encode("utf-8")).hexdigest()

    async def upsert_candles(self, candles: list[MarketCandle]) -> None:
        if not candles:
            return
        async with self.session_factory() as session:
            for candle in candles:
                await self._upsert_candle(session, candle)
            await session.commit()

    async def _upsert_candle(self, session: AsyncSession, candle: MarketCandle) -> None:
        if session.bind and session.bind.dialect.name == "postgresql":
            stmt = pg_insert(CandleRecord).values(
                venue=candle.venue.value,
                symbol=candle.symbol,
                timeframe=candle.timeframe.value,
                open_time=candle.open_time,
                close_time=candle.close_time,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
                quote_volume=candle.quote_volume,
                trade_count=candle.trade_count,
                taker_buy_volume=candle.taker_buy_volume,
                vwap=candle.vwap,
            )
            stmt = stmt.on_conflict_do_update(
                constraint="uq_candle_key",
                set_={
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                    "quote_volume": candle.quote_volume,
                    "trade_count": candle.trade_count,
                    "taker_buy_volume": candle.taker_buy_volume,
                    "vwap": candle.vwap,
                },
            )
            await session.execute(stmt)
            return

        existing = await session.scalar(
            select(CandleRecord).where(
                CandleRecord.venue == candle.venue.value,
                CandleRecord.symbol == candle.symbol,
                CandleRecord.timeframe == candle.timeframe.value,
                CandleRecord.close_time == candle.close_time,
            )
        )
        if existing is None:
            session.add(
                CandleRecord(
                    venue=candle.venue.value,
                    symbol=candle.symbol,
                    timeframe=candle.timeframe.value,
                    open_time=candle.open_time,
                    close_time=candle.close_time,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                    quote_volume=candle.quote_volume,
                    trade_count=candle.trade_count,
                    taker_buy_volume=candle.taker_buy_volume,
                    vwap=candle.vwap,
                )
            )
        else:
            existing.open = candle.open
            existing.high = candle.high
            existing.low = candle.low
            existing.close = candle.close
            existing.volume = candle.volume
            existing.quote_volume = candle.quote_volume
            existing.trade_count = candle.trade_count
            existing.taker_buy_volume = candle.taker_buy_volume
            existing.vwap = candle.vwap

    async def replace_levels(self, symbol: str, venue: str, levels: list[LevelCandidate]) -> None:
        async with self.session_factory() as session:
            await session.execute(
                delete(LevelRecord).where(LevelRecord.symbol == symbol, LevelRecord.venue == venue)
            )
            await session.flush()
            for level in levels:
                session.add(
                    LevelRecord(
                        level_id=level.level_id,
                        venue=level.venue.value,
                        symbol=level.symbol,
                        timeframe=level.timeframe.value,
                        kind=level.kind.value,
                        source=level.source,
                        lower_price=level.lower_price,
                        upper_price=level.upper_price,
                        reference_price=level.reference_price,
                        detected_at=level.detected_at,
                        touches=level.touches,
                        age_hours=level.age_hours,
                        strength=level.strength,
                    )
                )
            await session.commit()

    async def save_signal(self, decision: SignalDecision) -> None:
        async with self.session_factory() as session:
            existing = await session.scalar(select(SignalRecord).where(SignalRecord.alert_key == decision.alert_key))
            if existing is None:
                session.add(
                    SignalRecord(
                        decision_id=decision.decision_id,
                        alert_key=decision.alert_key,
                        venue=decision.venue.value,
                        symbol=decision.symbol,
                        timeframe=decision.timeframe.value,
                        setup=decision.setup.value,
                        direction=decision.direction.value,
                        signal_class=decision.signal_class.value,
                        confidence=decision.confidence,
                        level_id=decision.level_id,
                        detected_at=decision.detected_at,
                        entry_price=decision.entry_price,
                        invalidation_price=decision.invalidation_price,
                        targets=decision.targets,
                        expected_rr=decision.expected_rr,
                        rationale=decision.rationale,
                        why_not_higher=decision.why_not_higher,
                        contributions=[item.model_dump(mode="json") for item in decision.contributions],
                        data_health=decision.data_health.model_dump(mode="json"),
                        feature_snapshot=decision.feature_snapshot,
                        render_context=decision.render_context,
                    )
                )
                await session.commit()

    async def find_recent_signal_match(
        self,
        *,
        symbol: str,
        venue: str,
        setup: str,
        direction: str,
        level_id: str,
        within_minutes: int,
        setup_stage: str | None = None,
    ) -> SignalRecord | None:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=within_minutes)
        async with self.session_factory() as session:
            result = await session.execute(
                select(SignalRecord)
                .where(
                    SignalRecord.symbol == symbol,
                    SignalRecord.venue == venue,
                    SignalRecord.setup == setup,
                    SignalRecord.direction == direction,
                    SignalRecord.level_id == level_id,
                    SignalRecord.signal_class.in_(["actionable", "watchlist"]),
                    SignalRecord.detected_at >= cutoff,
                )
                .order_by(desc(SignalRecord.detected_at))
                .limit(20)
            )
            matches = list(result.scalars())
            if setup_stage is None:
                return matches[0] if matches else None
            for item in matches:
                if str((item.render_context or {}).get("setup_stage", "")) == setup_stage:
                    return item
            return None

    async def ensure_delivery(self, alert: AlertMessage) -> None:
        now = datetime.now(tz=timezone.utc)
        async with self.session_factory() as session:
            existing = await session.scalar(
                select(AlertDeliveryRecord).where(
                    AlertDeliveryRecord.signal_id == alert.signal_id,
                    AlertDeliveryRecord.chat_id == alert.chat_id,
                )
            )
            if existing is None:
                session.add(
                    AlertDeliveryRecord(
                        signal_id=alert.signal_id,
                        alert_key=alert.alert_key,
                        chat_id=alert.chat_id,
                        message_thread_id=alert.message_thread_id,
                        signal_class=alert.signal_class.value,
                        status="queued",
                        message_text=alert.text,
                        error_message=None,
                        created_at=now,
                        updated_at=now,
                    )
                )
                await session.commit()
            else:
                existing.message_thread_id = alert.message_thread_id
                existing.message_text = alert.text
                existing.signal_class = alert.signal_class.value
                existing.updated_at = now
                await session.commit()

    async def mark_delivery(self, signal_id: str, chat_id: int, status: str, error_message: str | None = None) -> None:
        async with self.session_factory() as session:
            existing = await session.scalar(
                select(AlertDeliveryRecord).where(
                    AlertDeliveryRecord.signal_id == signal_id,
                    AlertDeliveryRecord.chat_id == chat_id,
                )
            )
            if existing is not None:
                existing.status = status
                existing.error_message = error_message
                existing.updated_at = datetime.now(tz=timezone.utc)
                await session.commit()

    async def upsert_health(self, health: DataHealth) -> None:
        async with self.session_factory() as session:
            existing = await session.scalar(
                select(VenueHealthRecord).where(
                    VenueHealthRecord.venue == health.venue.value,
                    VenueHealthRecord.symbol == health.symbol,
                )
            )
            if existing is None:
                session.add(
                    VenueHealthRecord(
                        venue=health.venue.value,
                        symbol=health.symbol,
                        timestamp=health.timestamp,
                        is_fresh=health.is_fresh,
                        has_sequence_gap=health.has_sequence_gap,
                        spread_ratio=health.spread_ratio,
                        freshness_ms=health.freshness_ms,
                        reconnect_count=health.reconnect_count,
                        notes=health.notes,
                    )
                )
            else:
                existing.timestamp = health.timestamp
                existing.is_fresh = health.is_fresh
                existing.has_sequence_gap = health.has_sequence_gap
                existing.spread_ratio = health.spread_ratio
                existing.freshness_ms = health.freshness_ms
                existing.reconnect_count = health.reconnect_count
                existing.notes = health.notes
            await session.commit()

    async def list_latest_alerts(self, limit: int = 10) -> list[SignalRecord]:
        async with self.session_factory() as session:
            result = await session.execute(select(SignalRecord).order_by(desc(SignalRecord.detected_at)).limit(limit))
            return list(result.scalars())

    async def get_signal_by_decision_id(self, decision_id: str) -> SignalRecord | None:
        async with self.session_factory() as session:
            return await session.scalar(
                select(SignalRecord).where(SignalRecord.decision_id == decision_id)
            )

    async def list_signals_between(
        self,
        start_at: datetime,
        end_at: datetime,
        signal_classes: list[str] | None = None,
    ) -> list[SignalRecord]:
        async with self.session_factory() as session:
            stmt = (
                select(SignalRecord)
                .where(
                    SignalRecord.detected_at >= start_at,
                    SignalRecord.detected_at < end_at,
                )
                .order_by(SignalRecord.detected_at.asc())
            )
            if signal_classes:
                stmt = stmt.where(SignalRecord.signal_class.in_(signal_classes))
            result = await session.execute(stmt)
            return list(result.scalars())

    async def list_latest_deliveries(self, limit: int = 20) -> list[AlertDeliveryRecord]:
        async with self.session_factory() as session:
            result = await session.execute(select(AlertDeliveryRecord).order_by(desc(AlertDeliveryRecord.updated_at)).limit(limit))
            return list(result.scalars())

    async def list_recoverable_deliveries(self, limit: int = 200) -> list[AlertDeliveryRecord]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(AlertDeliveryRecord)
                .where(AlertDeliveryRecord.status.in_(["queued", "requeued", "failed"]))
                .order_by(AlertDeliveryRecord.updated_at.asc())
                .limit(limit)
            )
            return list(result.scalars())

    async def delivery_status_counts(self) -> dict[str, int]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(AlertDeliveryRecord.status, func.count())
                .group_by(AlertDeliveryRecord.status)
            )
            return {status: int(count) for status, count in result.all()}

    async def list_latest_candidates(self, limit: int = 20) -> list[LevelRecord]:
        async with self.session_factory() as session:
            result = await session.execute(select(LevelRecord).order_by(desc(LevelRecord.detected_at)).limit(limit))
            return list(result.scalars())

    async def list_latest_health(self, limit: int = 50) -> list[VenueHealthRecord]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(VenueHealthRecord).order_by(desc(VenueHealthRecord.timestamp)).limit(limit)
            )
            return list(result.scalars())

    async def get_recent_candles(self, venue: str, symbol: str, timeframe: str, limit: int = 300) -> list[CandleRecord]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(CandleRecord)
                .where(
                    CandleRecord.venue == venue,
                    CandleRecord.symbol == symbol,
                    CandleRecord.timeframe == timeframe,
                )
                .order_by(desc(CandleRecord.close_time))
                .limit(limit)
            )
            return list(reversed(list(result.scalars())))

    async def get_candles_before(
        self,
        venue: str,
        symbol: str,
        timeframe: str,
        end_at: datetime,
        limit: int = 80,
    ) -> list[CandleRecord]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(CandleRecord)
                .where(
                    CandleRecord.venue == venue,
                    CandleRecord.symbol == symbol,
                    CandleRecord.timeframe == timeframe,
                    CandleRecord.close_time <= end_at,
                )
                .order_by(desc(CandleRecord.close_time))
                .limit(limit)
            )
            return list(reversed(list(result.scalars())))

    async def get_candles_between(
        self,
        venue: str,
        symbol: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[CandleRecord]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(CandleRecord)
                .where(
                    CandleRecord.venue == venue,
                    CandleRecord.symbol == symbol,
                    CandleRecord.timeframe == timeframe,
                    CandleRecord.open_time >= start_at,
                    CandleRecord.open_time < end_at,
                )
                .order_by(CandleRecord.open_time.asc())
            )
            return list(result.scalars())

    async def signal_count(self) -> int:
        async with self.session_factory() as session:
            stmt: Select[tuple[int]] = select(func.count()).select_from(SignalRecord)
            result = await session.execute(stmt)
            return int(result.scalar_one())

    async def list_runtime_universe(self, enabled_venues: list[str] | None = None) -> list[UniverseSymbol]:
        async with self.session_factory() as session:
            stmt = select(RuntimeUniverseRecord).order_by(RuntimeUniverseRecord.primary_venue.asc(), RuntimeUniverseRecord.symbol.asc())
            if enabled_venues:
                stmt = stmt.where(RuntimeUniverseRecord.primary_venue.in_(enabled_venues))
            result = await session.execute(stmt)
            rows = list(result.scalars())
            return [
                UniverseSymbol(symbol=item.symbol, primary_venue=Venue(item.primary_venue))
                for item in rows
            ]

    async def replace_runtime_universe(self, symbols: list[UniverseSymbol]) -> None:
        async with self.session_factory() as session:
            await session.execute(delete(RuntimeUniverseRecord))
            await session.flush()
            for item in symbols:
                session.add(
                    RuntimeUniverseRecord(
                        symbol=item.symbol,
                        primary_venue=item.primary_venue.value,
                    )
                )
            await session.commit()

    async def upsert_runtime_universe_symbol(self, symbol: UniverseSymbol) -> None:
        async with self.session_factory() as session:
            existing = await session.scalar(
                select(RuntimeUniverseRecord).where(RuntimeUniverseRecord.symbol == symbol.symbol)
            )
            if existing is None:
                session.add(
                    RuntimeUniverseRecord(
                        symbol=symbol.symbol,
                        primary_venue=symbol.primary_venue.value,
                    )
                )
            else:
                existing.primary_venue = symbol.primary_venue.value
            await session.commit()

    async def remove_runtime_universe_symbol(self, symbol: str) -> None:
        async with self.session_factory() as session:
            await session.execute(
                delete(RuntimeUniverseRecord).where(RuntimeUniverseRecord.symbol == symbol)
            )
            await session.commit()

    async def replace_statistics_snapshot(
        self,
        *,
        start_at: datetime,
        end_at: datetime,
        symbol_query: str | None,
        rows: list[dict[str, Any]],
    ) -> None:
        snapshot_key = self.statistics_snapshot_key(start_at, end_at, symbol_query)
        normalized_query = (symbol_query or "").strip().upper()
        now = datetime.now(tz=timezone.utc)
        async with self.session_factory() as session:
            await session.execute(
                delete(StatisticsBySymbolRecord).where(StatisticsBySymbolRecord.snapshot_key == snapshot_key)
            )
            await session.flush()
            for row in rows:
                session.add(
                    StatisticsBySymbolRecord(
                        snapshot_key=snapshot_key,
                        start_at=start_at,
                        end_at=end_at,
                        symbol_query=normalized_query,
                        symbol=str(row["symbol"]),
                        total=int(row["total"]),
                        success=int(row["success"]),
                        failed=int(row["failed"]),
                        pending=int(row["pending"]),
                        actionable=int(row["actionable"]),
                        watchlist=int(row["watchlist"]),
                        avg_confidence=float(row["avg_confidence"]),
                        win_rate=float(row["win_rate"]),
                        updated_at=now,
                    )
                )
            await session.commit()

    async def list_statistics_snapshot(
        self,
        *,
        start_at: datetime,
        end_at: datetime,
        symbol_query: str | None = None,
    ) -> list[StatisticsBySymbolRecord]:
        snapshot_key = self.statistics_snapshot_key(start_at, end_at, symbol_query)
        async with self.session_factory() as session:
            result = await session.execute(
                select(StatisticsBySymbolRecord)
                .where(StatisticsBySymbolRecord.snapshot_key == snapshot_key)
                .order_by(StatisticsBySymbolRecord.symbol.asc())
            )
            return list(result.scalars())
