from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import BotCommand, Message

from brakerscalp.config import Settings
from brakerscalp.domain.models import AlertMessage, SignalClass
from brakerscalp.logging import get_logger
from brakerscalp.metrics import ALERT_LATENCY_SECONDS
from brakerscalp.storage.cache import StateCache
from brakerscalp.storage.repository import Repository


class BotService:
    def __init__(self, settings: Settings, repository: Repository, cache: StateCache) -> None:
        self.settings = settings
        self.bot = Bot(
            token=settings.bot_token,
            default=DefaultBotProperties(
                parse_mode=ParseMode(settings.bot_parse_mode.upper()),
                link_preview_is_disabled=settings.bot_disable_link_preview,
            ),
        )
        self.dispatcher = Dispatcher()
        self.router = Router()
        self.allowed_chat_ids = set(settings.allowed_chat_ids)
        self.alert_chat_ids = set(settings.effective_alert_chat_ids)
        self.repository = repository
        self.cache = cache
        self.logger = get_logger("bot")
        self._consume_task: asyncio.Task | None = None
        self._register_routes()

    def _register_routes(self) -> None:
        @self.router.message(Command("start"))
        async def start(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer(
                f"{self.settings.app_name} ready.\n"
                "Commands: /status /last /mute /unmute /health /pending /config /testalert /help"
            )

        @self.router.message(Command("help"))
        async def help_command(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer(
                "/status - bot state\n"
                "/last - last stored signal\n"
                "/mute - mute current chat\n"
                "/unmute - unmute current chat\n"
                "/health - recent signal health\n"
                "/pending - delivery backlog snapshot\n"
                "/config - effective runtime config\n"
                "/testalert - enqueue local test alert"
            )

        @self.router.message(Command("status"))
        async def status(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            count = await self.repository.signal_count()
            muted = await self.cache.is_chat_muted(message.chat.id)
            delivery_counts = await self.repository.delivery_status_counts()
            await message.answer(
                f"Signals stored: {count}\n"
                f"Muted: {'yes' if muted else 'no'}\n"
                f"Allowed chats: {len(self.allowed_chat_ids)}\n"
                f"Alert chats: {len(self.alert_chat_ids)}\n"
                f"Delivery queue: {self._format_delivery_counts(delivery_counts)}"
            )

        @self.router.message(Command("last"))
        async def last(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            alerts = await self.repository.list_latest_alerts(limit=1)
            if not alerts:
                await message.answer("No alerts yet.")
                return
            last_signal = alerts[0]
            await message.answer(
                f"{last_signal.symbol} {last_signal.setup.upper()} "
                f"{last_signal.direction.upper()} {last_signal.signal_class.upper()} "
                f"{last_signal.confidence:.0f}"
            )

        @self.router.message(Command("mute"))
        async def mute(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await self.cache.set_chat_muted(message.chat.id, True)
            await message.answer("Alerts muted for this chat.")

        @self.router.message(Command("unmute"))
        async def unmute(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await self.cache.set_chat_muted(message.chat.id, False)
            await message.answer("Alerts unmuted for this chat.")

        @self.router.message(Command("health"))
        async def health(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            alerts = await self.repository.list_latest_alerts(limit=3)
            venue_health = await self.repository.list_latest_health(limit=6)
            signal_lines = (
                "\n".join(
                    f"{item.symbol} {item.signal_class.upper()} {item.detected_at.isoformat()}"
                    for item in alerts
                )
                or "No signals yet."
            )
            venue_lines = (
                "\n".join(
                    f"{item.venue}:{item.symbol} fresh={'yes' if item.is_fresh else 'no'} "
                    f"gap={'yes' if item.has_sequence_gap else 'no'} freshness={item.freshness_ms}ms"
                    for item in venue_health
                )
                or "No venue health yet."
            )
            await message.answer(f"Signals:\n{signal_lines}\n\nVenues:\n{venue_lines}")

        @self.router.message(Command("pending"))
        async def pending(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            deliveries = await self.repository.list_recoverable_deliveries(limit=10)
            if not deliveries:
                await message.answer("No recoverable deliveries.")
                return
            text = "\n".join(
                f"{item.chat_id} {item.status} {item.signal_class.upper()} {item.signal_id}"
                for item in deliveries
            )
            await message.answer(text)

        @self.router.message(Command("config"))
        async def config(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer(
                f"Environment: <code>{self.settings.environment}</code>\n"
                f"Timezone: <code>{self.settings.timezone}</code>\n"
                f"Venues: <code>{', '.join(self.settings.enabled_venues)}</code>\n"
                f"Collector poll: <code>{self.settings.poll_interval_seconds}s</code>\n"
                f"Engine poll: <code>{self.settings.engine_interval_seconds}s</code>\n"
                f"API bind: <code>{self.settings.api_host}:{self.settings.api_port}</code>\n"
                f"Universe: <code>{self.settings.universe_path}</code>\n"
                f"Alert chats: <code>{', '.join(map(str, self.settings.effective_alert_chat_ids))}</code>"
            )

        @self.router.message(Command("testalert"))
        async def testalert(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            ts = int(datetime.now(tz=timezone.utc).timestamp())
            alert = AlertMessage(
                signal_id=f"manual-{message.chat.id}-{ts}",
                alert_key=f"manual:{message.chat.id}:{ts}",
                chat_id=message.chat.id,
                message_thread_id=self.settings.alert_message_thread_id,
                signal_class=SignalClass.WATCHLIST,
                text=(
                    "TEST | BREAKOUT | LONG | 15m\n"
                    "Confidence: 72\n\n"
                    "Level:\n"
                    "65000.0000 - 65100.0000 | HTF source: 1h manual-test\n\n"
                    "Trigger:\n"
                    "Manual test alert generated from /testalert\n\n"
                    "Rationale:\n"
                    "- Telegram integration ok\n"
                    "- Outbox ok\n"
                    "- Current chat is allowlisted\n\n"
                    "Invalidation:\n"
                    "- Stop logic: below level with 0.2 ATR buffer\n"
                    "- Cancel if: manual test only\n\n"
                    "Targets:\n"
                    "- T1: 65250.0000\n"
                    "- T2: 65400.0000\n"
                    "- Expected R:R: 2.00\n\n"
                    "Why confidence is not higher:\n"
                    "- This is a synthetic test alert.\n\n"
                    "Data health:\n"
                    "- Freshness: 0 ms\n"
                    "- Venues used: manual\n"
                    "- Sequence gaps: none"
                ),
            )
            await self.repository.ensure_delivery(alert)
            await self.cache.enqueue_alert(alert)
            await message.answer("Test alert queued into outbox.")

        @self.router.message(F.text)
        async def fallback(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer("Use /help to see available commands.")

        self.dispatcher.include_router(self.router)

    async def _is_allowed(self, message: Message) -> bool:
        if message.chat.id not in self.allowed_chat_ids:
            await message.answer("Access denied.")
            return False
        return True

    async def _consume_outbox(self) -> None:
        while True:
            try:
                alert = await self.cache.pop_alert(timeout=5)
                if alert is None:
                    continue
                if await self.cache.is_chat_muted(alert.chat_id):
                    await self.repository.mark_delivery(alert.signal_id, alert.chat_id, "muted")
                    continue
                await self.bot.send_message(
                    alert.chat_id,
                    alert.text,
                    message_thread_id=alert.message_thread_id,
                )
                await self.repository.mark_delivery(alert.signal_id, alert.chat_id, "sent")
                ALERT_LATENCY_SECONDS.observe(0.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.exception("bot-delivery-failed", error=str(exc))

    async def _recover_pending_deliveries(self) -> int:
        deliveries = await self.repository.list_recoverable_deliveries(limit=200)
        recovered = 0
        for item in deliveries:
            alert = AlertMessage(
                signal_id=item.signal_id,
                alert_key=item.alert_key,
                chat_id=item.chat_id,
                message_thread_id=item.message_thread_id,
                text=item.message_text,
                signal_class=SignalClass(item.signal_class),
            )
            await self.cache.enqueue_alert(alert)
            await self.repository.mark_delivery(item.signal_id, item.chat_id, "requeued")
            recovered += 1
        if recovered:
            self.logger.info("bot-recovered-deliveries", count=recovered)
        return recovered

    async def _notify_lifecycle(self, action: str) -> None:
        if action == "startup" and not self.settings.bot_startup_notifications:
            return
        if action == "shutdown" and not self.settings.bot_shutdown_notifications:
            return
        if not self.alert_chat_ids:
            return
        text = (
            f"<b>{self.settings.app_name}</b> {action}\n"
            f"environment: <code>{self.settings.environment}</code>\n"
            f"venues: <code>{', '.join(self.settings.enabled_venues)}</code>\n"
            f"time: <code>{datetime.now(tz=timezone.utc).isoformat()}</code>"
        )
        for chat_id in self.alert_chat_ids:
            try:
                if await self.cache.is_chat_muted(chat_id):
                    continue
                await self.bot.send_message(
                    chat_id,
                    text,
                    message_thread_id=self.settings.alert_message_thread_id,
                )
            except Exception as exc:
                self.logger.exception("bot-lifecycle-notify-failed", action=action, chat_id=chat_id, error=str(exc))

    async def run(self) -> None:
        await self.bot.set_my_commands(
            [
                BotCommand(command="start", description="start bot"),
                BotCommand(command="status", description="show service state"),
                BotCommand(command="last", description="show last signal"),
                BotCommand(command="mute", description="mute this chat"),
                BotCommand(command="unmute", description="unmute this chat"),
                BotCommand(command="health", description="recent signal health"),
                BotCommand(command="pending", description="delivery backlog"),
                BotCommand(command="config", description="runtime config"),
                BotCommand(command="testalert", description="send synthetic alert"),
                BotCommand(command="help", description="command list"),
            ]
        )
        await self._recover_pending_deliveries()
        self._consume_task = asyncio.create_task(self._consume_outbox())
        await self._notify_lifecycle("startup")
        await self.dispatcher.start_polling(self.bot, polling_timeout=self.settings.bot_polling_timeout_seconds)

    async def shutdown(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._consume_task
        await self._notify_lifecycle("shutdown")
        await self.bot.session.close()

    def _format_delivery_counts(self, counts: dict[str, int]) -> str:
        if not counts:
            return "empty"
        return ", ".join(f"{status}={count}" for status, count in sorted(counts.items()))
