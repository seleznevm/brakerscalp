from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import BotCommand, BufferedInputFile, Message

from brakerscalp.config import Settings
from brakerscalp.domain.models import AlertMessage, SignalClass
from brakerscalp.logging import get_logger
from brakerscalp.metrics import ALERT_LATENCY_SECONDS
from brakerscalp.services.daily_summary import SignalOutcome, classify_signal_outcome, render_daily_summary
from brakerscalp.signals.charting import render_signal_chart
from brakerscalp.signals.rendering import render_chart_caption
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
        self.local_tz = self._load_timezone(settings.timezone)
        self._consume_task: asyncio.Task | None = None
        self._daily_summary_task: asyncio.Task | None = None
        self._register_routes()

    def _register_routes(self) -> None:
        @self.router.message(Command("start"))
        async def start(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer(
                f"{self.settings.app_name} готов.\n"
                "Команды: /status /last /mute /unmute /health /pending /summary /config /testalert /help"
            )

        @self.router.message(Command("help"))
        async def help_command(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer(
                "/status - состояние бота\n"
                "/last - последний сохраненный сигнал\n"
                "/mute - выключить алерты в этом чате\n"
                "/unmute - включить алерты в этом чате\n"
                "/health - свежесть сигналов и биржевых данных\n"
                "/pending - очередь доставок\n"
                "/summary - сводка по сигналам за сегодня\n"
                "/chatinfo - chat_id и topic_id текущего чата\n"
                "/config - текущая конфигурация runtime\n"
                "/testalert - положить тестовый алерт в outbox"
            )

        @self.router.message(Command("status"))
        async def status(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            count = await self.repository.signal_count()
            muted = await self.cache.is_chat_muted(message.chat.id)
            delivery_counts = await self.repository.delivery_status_counts()
            await message.answer(
                f"Сигналов в базе: {count}\n"
                f"Чат заглушен: {'да' if muted else 'нет'}\n"
                f"Разрешенных чатов: {len(self.allowed_chat_ids)}\n"
                f"Чатов для алертов: {len(self.alert_chat_ids)}\n"
                f"Очередь доставок: {self._format_delivery_counts(delivery_counts)}"
            )

        @self.router.message(Command("last"))
        async def last(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            alerts = await self.repository.list_latest_alerts(limit=1)
            if not alerts:
                await message.answer("Сигналов пока нет.")
                return
            last_signal = alerts[0]
            await message.answer(
                "Последний сигнал:\n"
                f"{last_signal.symbol} {last_signal.setup.upper()} "
                f"{last_signal.direction.upper()} {last_signal.signal_class.upper()} "
                f"{last_signal.confidence:.0f}\n"
                f"Время: {self._format_local_dt(last_signal.detected_at)}"
            )

        @self.router.message(Command("mute"))
        async def mute(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await self.cache.set_chat_muted(message.chat.id, True)
            await message.answer("Алерты для этого чата выключены.")

        @self.router.message(Command("unmute"))
        async def unmute(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await self.cache.set_chat_muted(message.chat.id, False)
            await message.answer("Алерты для этого чата снова включены.")

        @self.router.message(Command("health"))
        async def health(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            alerts = await self.repository.list_latest_alerts(limit=3)
            venue_health = await self.repository.list_latest_health(limit=6)
            signal_lines = (
                "\n".join(
                    f"{item.symbol} {item.signal_class.upper()} {self._format_local_dt(item.detected_at)}"
                    for item in alerts
                )
                or "Сигналов пока нет."
            )
            venue_lines = (
                "\n".join(
                    f"{item.venue}:{item.symbol} свежие={'да' if item.is_fresh else 'нет'} "
                    f"разрыв={'да' if item.has_sequence_gap else 'нет'} свежесть={item.freshness_ms}ms"
                    for item in venue_health
                )
                or "Данных по биржам пока нет."
            )
            await message.answer(f"Сигналы:\n{signal_lines}\n\nБиржи:\n{venue_lines}")

        @self.router.message(Command("pending"))
        async def pending(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            deliveries = await self.repository.list_recoverable_deliveries(limit=10)
            if not deliveries:
                await message.answer("Восстанавливаемых доставок нет.")
                return
            text = "\n".join(
                f"{item.chat_id} {item.status} {item.signal_class.upper()} {item.signal_id}"
                for item in deliveries
            )
            await message.answer(text)

        @self.router.message(Command("summary"))
        async def summary(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer(await self._build_daily_summary(datetime.now(self.local_tz).date()))

        @self.router.message(Command("config"))
        async def config(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer(
                f"Окружение: <code>{self.settings.environment}</code>\n"
                f"Часовой пояс: <code>{self.settings.timezone}</code>\n"
                f"Биржи: <code>{', '.join(self.settings.enabled_venues)}</code>\n"
                f"Интервал collector: <code>{self.settings.poll_interval_seconds}s</code>\n"
                f"Интервал engine: <code>{self.settings.engine_interval_seconds}s</code>\n"
                f"API bind: <code>{self.settings.api_host}:{self.settings.api_port}</code>\n"
                f"Universe: <code>{self.settings.universe_path}</code>\n"
                f"Чаты для алертов: <code>{', '.join(map(str, self.settings.effective_alert_chat_ids))}</code>\n"
                f"Topic для алертов: <code>{self.settings.alert_message_thread_id}</code>\n"
                f"Ежедневная сводка: <code>23:00 {self.settings.timezone}</code>"
            )

        @self.router.message(Command("chatinfo"))
        async def chatinfo(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer(
                f"chat_id: <code>{message.chat.id}</code>\n"
                f"chat_type: <code>{message.chat.type}</code>\n"
                f"message_thread_id: <code>{message.message_thread_id}</code>\n"
                f"configured_alert_thread_id: <code>{self.settings.alert_message_thread_id}</code>"
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
                    "#BREAKOUT #TEST\n"
                    "Уверенность: 72\n\n"
                    "Уровень:\n"
                    "65000.0000 - 65100.0000 | HTF источник: 1h manual-test\n\n"
                    "Триггер:\n"
                    "Тестовый алерт, сгенерированный через /testalert\n\n"
                    "Обоснование:\n"
                    "- Интеграция с Telegram работает\n"
                    "- Outbox работает\n"
                    "- Текущий чат в allowlist\n\n"
                    "Инвалидация:\n"
                    "- Стоп-логика: ниже уровня с буфером 0.2 ATR\n"
                    "- Отмена, если: это только тестовый алерт\n\n"
                    "Цели:\n"
                    "- T1: 65250.0000\n"
                    "- T2: 65400.0000\n"
                    "- Ожидаемый R:R: 2.00\n\n"
                    "Почему уверенность не выше:\n"
                    "- Это синтетический тестовый алерт.\n\n"
                    "Состояние данных:\n"
                    "- Свежесть: 0 ms\n"
                    "- Использованные биржи: manual\n"
                    "- Разрывы последовательности: нет"
                ),
            )
            await self.repository.ensure_delivery(alert)
            await self.cache.enqueue_alert(alert)
            await message.answer("Тестовый алерт поставлен в outbox.")

        @self.router.message(F.text)
        async def fallback(message: Message) -> None:
            if not await self._is_allowed(message):
                return
            await message.answer("Используйте /help, чтобы посмотреть доступные команды.")

        self.dispatcher.include_router(self.router)

    async def _is_allowed(self, message: Message) -> bool:
        if message.chat.id not in self.allowed_chat_ids:
            await message.answer("Доступ запрещен.")
            return False
        return True

    async def _consume_outbox(self) -> None:
        while True:
            alert: AlertMessage | None = None
            try:
                alert = await self.cache.pop_alert(timeout=5)
                if alert is None:
                    continue
                if await self.cache.is_chat_muted(alert.chat_id):
                    await self.repository.mark_delivery(alert.signal_id, alert.chat_id, "muted")
                    continue
                await self._send_alert_bundle(alert)
                await self.repository.mark_delivery(alert.signal_id, alert.chat_id, "sent")
                ALERT_LATENCY_SECONDS.observe(0.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if alert is not None:
                    await self.repository.mark_delivery(alert.signal_id, alert.chat_id, "failed", error_message=str(exc))
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

    async def _daily_summary_loop(self) -> None:
        while True:
            now_local = datetime.now(self.local_tz)
            scheduled_at = self._next_summary_at(now_local)
            delay = max((scheduled_at - now_local).total_seconds(), 1.0)
            await asyncio.sleep(delay)
            await self._send_daily_summary(datetime.now(self.local_tz).date())

    async def _send_daily_summary(self, report_date: date) -> bool:
        if not self.alert_chat_ids:
            return False
        dedupe_key = report_date.isoformat()
        if not await self.cache.acquire_once_key("daily-summary", dedupe_key, ttl_seconds=7 * 24 * 3600):
            return False
        text = await self._build_daily_summary(report_date)
        sent = False
        for chat_id in self.alert_chat_ids:
            try:
                if await self.cache.is_chat_muted(chat_id):
                    continue
                await self._send_with_thread_fallback(chat_id, text, self.settings.alert_message_thread_id)
                sent = True
            except Exception as exc:
                self.logger.exception("bot-daily-summary-failed", chat_id=chat_id, report_date=report_date.isoformat(), error=str(exc))
        return sent

    async def _build_daily_summary(self, report_date: date) -> str:
        start_utc, end_utc = self._local_day_bounds(report_date)
        signals = await self.repository.list_signals_between(
            start_utc,
            end_utc,
            signal_classes=["actionable", "watchlist"],
        )
        outcomes: list[SignalOutcome] = []
        for signal in signals:
            candles = await self.repository.get_candles_between(
                signal.venue,
                signal.symbol,
                signal.timeframe,
                signal.detected_at,
                end_utc,
            )
            outcomes.append(SignalOutcome(signal=signal, status=classify_signal_outcome(signal, candles)))
        return render_daily_summary(report_date, outcomes)

    async def _notify_lifecycle(self, action: str) -> None:
        if action == "startup" and not self.settings.bot_startup_notifications:
            return
        if action == "shutdown" and not self.settings.bot_shutdown_notifications:
            return
        if not self.alert_chat_ids:
            return
        action_text = "запущен" if action == "startup" else "остановлен"
        text = (
            f"<b>{self.settings.app_name}</b> {action_text}\n"
            f"окружение: <code>{self.settings.environment}</code>\n"
            f"биржи: <code>{', '.join(self.settings.enabled_venues)}</code>\n"
            f"время: <code>{self._format_local_dt(datetime.now(tz=timezone.utc))}</code>"
        )
        for chat_id in self.alert_chat_ids:
            try:
                if await self.cache.is_chat_muted(chat_id):
                    continue
                await self._send_with_thread_fallback(chat_id, text, self.settings.alert_message_thread_id)
            except Exception as exc:
                self.logger.exception("bot-lifecycle-notify-failed", action=action, chat_id=chat_id, error=str(exc))

    async def run(self) -> None:
        await self.bot.set_my_commands(
            [
                BotCommand(command="start", description="запустить бота"),
                BotCommand(command="status", description="состояние сервиса"),
                BotCommand(command="last", description="последний сигнал"),
                BotCommand(command="mute", description="выключить алерты в чате"),
                BotCommand(command="unmute", description="включить алерты в чате"),
                BotCommand(command="health", description="свежесть данных"),
                BotCommand(command="pending", description="очередь доставок"),
                BotCommand(command="summary", description="сводка за сегодня"),
                BotCommand(command="chatinfo", description="информация о чате"),
                BotCommand(command="config", description="runtime конфигурация"),
                BotCommand(command="testalert", description="тестовый алерт"),
                BotCommand(command="help", description="список команд"),
            ]
        )
        await self._recover_pending_deliveries()
        self._consume_task = asyncio.create_task(self._consume_outbox())
        self._daily_summary_task = asyncio.create_task(self._daily_summary_loop())
        await self._notify_lifecycle("startup")
        await self.dispatcher.start_polling(self.bot, polling_timeout=self.settings.bot_polling_timeout_seconds)

    async def shutdown(self) -> None:
        for task in [self._consume_task, self._daily_summary_task]:
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        await self._notify_lifecycle("shutdown")
        await self.bot.session.close()

    def _format_delivery_counts(self, counts: dict[str, int]) -> str:
        if not counts:
            return "пусто"
        return ", ".join(f"{status}={count}" for status, count in sorted(counts.items()))

    def _format_local_dt(self, value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(self.local_tz).strftime("%d.%m.%Y %H:%M:%S")

    def _local_day_bounds(self, report_date: date) -> tuple[datetime, datetime]:
        start_local = datetime.combine(report_date, time(0, 0), tzinfo=self.local_tz)
        end_local = start_local + timedelta(days=1)
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

    def _next_summary_at(self, now_local: datetime) -> datetime:
        target = now_local.replace(hour=23, minute=0, second=0, microsecond=0)
        if now_local >= target:
            target += timedelta(days=1)
        return target

    def _load_timezone(self, timezone_name: str) -> ZoneInfo:
        try:
            return ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            self.logger.warning("bot-invalid-timezone-fallback", timezone=timezone_name)
            return ZoneInfo("UTC")

    async def _send_alert_bundle(self, alert: AlertMessage) -> None:
        chart_bytes, chart_caption = await self._build_alert_chart(alert.signal_id)
        try:
            await self._deliver_alert_bundle(
                chat_id=alert.chat_id,
                text=alert.text,
                message_thread_id=alert.message_thread_id,
                chart_bytes=chart_bytes,
                chart_caption=chart_caption,
            )
        except TelegramBadRequest as exc:
            error_text = str(exc).lower()
            if alert.message_thread_id and ("message thread not found" in error_text or "chat not found" in error_text):
                self.logger.warning(
                    "bot-thread-fallback",
                    chat_id=alert.chat_id,
                    message_thread_id=alert.message_thread_id,
                    error=str(exc),
                )
                await self._deliver_alert_bundle(
                    chat_id=alert.chat_id,
                    text=alert.text,
                    message_thread_id=None,
                    chart_bytes=chart_bytes,
                    chart_caption=chart_caption,
                )
                return
            raise

    async def _deliver_alert_bundle(
        self,
        *,
        chat_id: int,
        text: str,
        message_thread_id: int | None,
        chart_bytes: bytes | None,
        chart_caption: str | None,
    ) -> None:
        if chart_bytes is not None and chart_caption is not None:
            chart_file = BufferedInputFile(chart_bytes, filename="signal-chart.png")
            await self.bot.send_photo(
                chat_id,
                photo=chart_file,
                caption=chart_caption,
                message_thread_id=message_thread_id,
            )
        await self.bot.send_message(
            chat_id,
            text,
            message_thread_id=message_thread_id,
        )

    async def _build_alert_chart(self, signal_id: str) -> tuple[bytes | None, str | None]:
        signal = await self.repository.get_signal_by_decision_id(signal_id)
        if signal is None:
            return None, None
        candles = await self.repository.get_candles_before(
            signal.venue,
            signal.symbol,
            signal.timeframe,
            signal.detected_at,
            limit=64,
        )
        chart_bytes = render_signal_chart(candles, signal)
        if chart_bytes is None:
            return None, None
        return chart_bytes, render_chart_caption(signal)

    async def _send_with_thread_fallback(self, chat_id: int, text: str, message_thread_id: int | None) -> None:
        try:
            await self.bot.send_message(
                chat_id,
                text,
                message_thread_id=message_thread_id,
            )
        except TelegramBadRequest as exc:
            error_text = str(exc).lower()
            if message_thread_id and (
                "message thread not found" in error_text or "chat not found" in error_text
            ):
                self.logger.warning(
                    "bot-thread-fallback",
                    chat_id=chat_id,
                    message_thread_id=message_thread_id,
                    error=str(exc),
                )
                await self.bot.send_message(chat_id, text)
                return
            raise
