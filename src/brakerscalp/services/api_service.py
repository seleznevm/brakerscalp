from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from html import escape
from io import BytesIO
from typing import Any
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from brakerscalp.config import Settings
from brakerscalp.domain.models import UniverseSymbol, Venue
from brakerscalp.services.market_inspector import ManualScanResult, MarketInspector
from brakerscalp.signals.engine import StrategyRuntimeConfig
from brakerscalp.storage.cache import StateCache
from brakerscalp.storage.repository import Repository
from brakerscalp.universe import save_universe


STATUS_LABELS = {
    "actionable": "Готов к входу",
    "watchlist": "Watchlist",
    "arming": "Поджатие у уровня",
    "monitor": "Под наблюдением",
    "cold": "Холодный",
    "stale": "Данные устарели",
    "insufficient": "Недостаточно данных",
    "online": "Онлайн",
    "warning": "Внимание",
    "offline": "Офлайн",
}

STATUS_TONES = {
    "actionable": "good",
    "watchlist": "warn",
    "arming": "accent",
    "monitor": "neutral",
    "cold": "neutral",
    "stale": "danger",
    "insufficient": "neutral",
    "online": "good",
    "warning": "warn",
    "offline": "danger",
}

OUTCOME_LABELS = {
    "watch": "WATCH",
    "executed": "EXECUTED",
    "tp1": "TP1",
    "tp2": "TP2",
    "loss": "LOSS",
    "invalidation": "INVALIDATION",
}

OUTCOME_TONES = {
    "watch": "warn",
    "executed": "accent",
    "tp1": "good",
    "tp2": "good",
    "loss": "danger",
    "invalidation": "danger",
}


def build_api(
    repository: Repository,
    cache: StateCache,
    settings: Settings,
    universe: list,
    adapters: dict,
) -> FastAPI:
    app = FastAPI(title="BrakerScalp API")
    inspector = MarketInspector(repository, cache, settings, universe, adapters)
    local_tz = _load_timezone(settings.timezone)

    @app.get("/", response_class=HTMLResponse)
    async def command_center() -> HTMLResponse:
        screener = await inspector.screen_universe(scope="active")
        setups = await inspector.list_active_setups(limit=6)
        service_statuses = await _service_statuses(repository, cache, settings, local_tz)
        venue_health = await repository.list_latest_health(limit=90)
        deliveries = await repository.list_latest_deliveries(limit=8)
        delivery_counts = await repository.delivery_status_counts()
        signal_count = await repository.signal_count()
        actionable_24h, watchlist_24h = await _signal_stats(repository)
        minimum_alert_confidence = await _current_minimum_alert_confidence(cache, settings)
        risk_usdt = await _current_risk_usdt(cache, settings)
        strategy_config = await _current_strategy_config(cache, settings)
        cards = "".join(
            [
                _metric_card("Сигналов в базе", str(signal_count), "Всего сохраненных решений."),
                _metric_card("Actionable 24ч", str(actionable_24h), "Пробои, которые дошли до actionable."),
                _metric_card("Watchlist 24ч", str(watchlist_24h), "Слабее actionable, но уже рядом с уровнем."),
                _metric_card("Outbox", str(await cache.outbox_size()), _format_delivery_counts(delivery_counts)),
                _metric_card("Min confidence", f"{minimum_alert_confidence:.1f}", "Minimum confidence required before a setup is sent to Telegram."),
                _metric_card("Risk USDT", f"{risk_usdt:.2f}", "Maximum planned loss per setup, used to size Qty on the setups page."),
            ]
        )
        opportunities = _opportunities_table(screener[:10], local_tz)
        setup_cards = "".join(_setup_card(item, local_tz, risk_usdt=risk_usdt) for item in setups) or _empty_block("Нет активных сетапов в окне 72 часов.")
        service_cards = "".join(_service_card(item) for item in service_statuses)
        venue_cards = "".join(_venue_health_card(item, local_tz) for item in venue_health[:8]) or _empty_block("Нет данных по venue health.")
        delivery_cards = "".join(_delivery_card(item, local_tz) for item in deliveries) or _empty_block("Нет записей по доставке.")
        body = f"""
        <section class="hero">
          <div>
            <p class="eyebrow">BrakerScalp / Command Center</p>
            <h1>Командный пункт импульсного скальпинга</h1>
            <p class="hero-copy">Оперативная панель по состоянию сервисов, свежим breakout scalp сетапам, ручной проверке токенов и скринеру рынка.</p>
          </div>
          <div class="hero-meta">
            <div class="hero-chip">Timezone: <strong>{escape(settings.timezone)}</strong></div>
            <div class="hero-chip">Universe: <strong>{len(universe)} symbols</strong></div>
            <div class="hero-chip">Venues: <strong>{escape(", ".join(settings.enabled_venues))}</strong></div>
          </div>
        </section>
        <section class="metrics-grid">{cards}</section>
        <section class="two-col">
          <div class="panel">
            <div class="panel-head">
              <h2>Текущие возможности</h2>
              <a href="/screener">Полный скринер</a>
            </div>
            {opportunities}
          </div>
          <div class="panel">
            <div class="panel-head">
              <h2>Состояние сервисов</h2>
              <a href="/services">Подробнее</a>
            </div>
            <div class="service-grid">{service_cards}</div>
          </div>
        </section>
        <section class="panel">
          <div class="panel-head">
            <h2>Действующие сетапы</h2>
            <a href="/setups">Открыть страницу сетапов</a>
          </div>
          <div class="setup-grid">{setup_cards}</div>
        </section>
        <section class="two-col">
          <div class="panel">
            <div class="panel-head">
              <h2>Venue health</h2>
              <a href="/debug/venues/health">JSON</a>
            </div>
            <div class="stack">{venue_cards}</div>
          </div>
          <div class="panel">
            <div class="panel-head">
              <h2>Последние доставки</h2>
              <a href="/debug/deliveries/latest">JSON</a>
            </div>
            <div class="stack">{delivery_cards}</div>
          </div>
        </section>
        """
        return HTMLResponse(_page("Командный пункт", "dashboard", body, refresh_seconds=30))

    @app.get("/services", response_class=HTMLResponse)
    async def services_page() -> HTMLResponse:
        statuses = await _service_statuses(repository, cache, settings, local_tz)
        cards = "".join(_service_detail_card(item, local_tz) for item in statuses)
        body = f"""
        <section class="hero compact">
          <div>
            <p class="eyebrow">Runtime / Services</p>
            <h1>Проверка всех сервисов</h1>
            <p class="hero-copy">Страница показывает реальную доступность API, PostgreSQL, Redis и heartbeat всех внутренних воркеров.</p>
          </div>
        </section>
        <section class="service-detail-grid">{cards}</section>
        """
        return HTMLResponse(_page("Сервисы", "services", body, refresh_seconds=20))

    @app.get("/setups", response_class=HTMLResponse)
    async def setups_page(
        limit: int = Query(default=24, ge=1, le=100),
        status: str = Query(default="all"),
        q: str = Query(default=""),
        min_confidence: str = Query(default=""),
    ) -> HTMLResponse:
        parsed_min_confidence = _parse_optional_float(min_confidence, minimum=0.0, maximum=100.0)
        risk_usdt = await _current_risk_usdt(cache, settings)
        all_setups = await inspector.list_active_setups(
            limit=250,
            outcome_filter="all",
            symbol_query=q,
            minimum_confidence=parsed_min_confidence,
        )
        available_statuses = _available_setup_statuses(all_setups)
        selected_status = status if status == "all" or status in available_statuses else "all"
        setups = [
            item for item in all_setups
            if selected_status == "all" or item.lifecycle.status == selected_status
        ][:limit]
        cards = "".join(_setup_card(item, local_tz, include_meta=True, risk_usdt=risk_usdt) for item in setups) or _empty_block("Нет сетапов в заданном окне.")
        body = f"""
        <section class="hero compact">
          <div>
            <p class="eyebrow">Signals / Active Setups</p>
            <h1>Действующие сетапы</h1>
            <p class="hero-copy">Последние actionable и watchlist сигналы с графиками, точкой входа, SL, TP и статусом отработки.</p>
          </div>
        </section>
        <section class="panel">{_setups_filter_form(status=selected_status, symbol_query=q, limit=limit, min_confidence=parsed_min_confidence, available_statuses=available_statuses)}</section>
        <section class="setup-grid large">{cards}</section>
        """
        return HTMLResponse(_page("Сетапы", "setups", body, refresh_seconds=30))

    @app.get("/screener", response_class=HTMLResponse)
    async def screener_page(
        scope: str = Query(default="active", pattern="^(active|all)$"),
        limit: int = Query(default=50, ge=1, le=200),
        sort_by: str = Query(default="score", pattern="^(symbol|status|bias|score|dist_atr|vol_z|quote_x|squeeze|touches|freshness)$"),
        sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    ) -> HTMLResponse:
        rows = await inspector.screen_universe(scope=scope)
        rows = _sort_screener_rows(rows, sort_by=sort_by, sort_dir=sort_dir)
        table = _screener_table(rows[:limit], local_tz, scope=scope, limit=limit, sort_by=sort_by, sort_dir=sort_dir)
        body = f"""
        <section class="hero compact">
          <div>
            <p class="eyebrow">Market / Screener</p>
            <h1>Скринер рынка</h1>
            <p class="hero-copy">Приоритетные монеты под breakout scalp. Видно статус, направление, расстояние до пробоя, объем, squeeze и свежесть данных.</p>
          </div>
          <div class="hero-meta">
            <a class="hero-chip linkish {'is-active' if scope == 'active' else ''}" href="/screener?scope=active">Только активные</a>
            <a class="hero-chip linkish {'is-active' if scope == 'all' else ''}" href="/screener?scope=all">Весь universe</a>
          </div>
        </section>
        <section class="panel">{table}</section>
        """
        return HTMLResponse(_page("Скринер", "screener", body, refresh_seconds=25))

    @app.get("/settings", response_class=HTMLResponse)
    async def settings_page(
        symbol: str | None = None,
        threshold_saved: int = 0,
        risk_saved: int = 0,
        strategy_saved: str | None = None,
        manage_symbol: str | None = None,
        universe_saved: str | None = None,
    ) -> HTMLResponse:
        scan = await inspector.manual_scan(symbol) if symbol else None
        minimum_alert_confidence = await _current_minimum_alert_confidence(cache, settings)
        risk_usdt = await _current_risk_usdt(cache, settings)
        strategy_config = await _current_strategy_config(cache, settings)
        manual_card = _manual_scan_card(symbol, scan, local_tz) if symbol else _manual_scan_form("")
        runtime_universe = await inspector.list_universe()
        discovered_symbol, venue_probes = await inspector.discover_symbol_venues(manage_symbol or "") if manage_symbol else ("", [])
        body = f"""
        <section class="hero compact">
          <div>
            <p class="eyebrow">Runtime / Settings</p>
            <h1>Настройки и ручная проверка токена</h1>
            <p class="hero-copy">Здесь собран runtime-конфиг и форма для принудительной проверки сетапа по любому символу.</p>
          </div>
        </section>
        <section class="two-col settings-layout">
          <div class="panel">
            <div class="panel-head">
              <h2>Текущая конфигурация</h2>
              <a href="/debug/runtime-config">JSON</a>
            </div>
            {_settings_table(settings, minimum_alert_confidence, risk_usdt)}
            {_runtime_settings_form(minimum_alert_confidence, risk_usdt, strategy_config, threshold_saved=bool(threshold_saved), risk_saved=bool(risk_saved), strategy_saved=strategy_saved)}
          </div>
          <div class="panel">
            <div class="panel-head">
              <h2>Ручной прогон символа</h2>
              <span class="muted">Проверка идет через cache или live adapter.</span>
            </div>
            {manual_card}
            {_universe_manage_form(manage_symbol or "", universe_saved)}
            {_discovered_venues_block(discovered_symbol, venue_probes)}
            {_universe_table(runtime_universe)}
          </div>
        </section>
        """
        return HTMLResponse(_page("Настройки", "settings", body, refresh_seconds=None))

    @app.get("/settings/apply-threshold")
    async def apply_threshold(value: float = Query(ge=0.0, le=100.0)) -> RedirectResponse:
        await cache.set_minimum_alert_confidence(value)
        return RedirectResponse(url="/settings?threshold_saved=1", status_code=303)

    @app.get("/settings/apply-risk")
    async def apply_risk(value: float = Query(ge=0.01, le=1000000.0)) -> RedirectResponse:
        await cache.set_risk_usdt(value)
        return RedirectResponse(url="/settings?risk_saved=1", status_code=303)

    @app.get("/settings/apply-strategy")
    async def apply_strategy(
        timeframe: str = Query(default="5m", pattern="^(5m|15m)$"),
        minimum_expected_rr: float = Query(ge=1.0, le=10.0),
        actionable_confidence_threshold: float = Query(ge=0.0, le=100.0),
        watchlist_confidence_threshold: float = Query(ge=0.0, le=100.0),
        volume_z_threshold: float = Query(ge=0.0, le=20.0),
        watchlist_volume_z_threshold: float = Query(ge=0.0, le=20.0),
        min_touches: int = Query(ge=1, le=10),
        squeeze_threshold: float = Query(ge=0.0, le=1.5),
        dist_to_level_atr: float = Query(ge=0.0, le=5.0),
        breakout_distance_atr: float = Query(ge=0.0, le=5.0),
        body_ratio_threshold: float = Query(ge=0.0, le=1.0),
        close_to_extreme_threshold: float = Query(ge=0.0, le=1.0),
        range_expansion_threshold: float = Query(ge=0.0, le=10.0),
        sl_multiplier: float = Query(ge=0.01, le=5.0),
    ) -> RedirectResponse:
        config = StrategyRuntimeConfig(
            timeframe=timeframe,
            minimum_expected_rr=minimum_expected_rr,
            actionable_confidence_threshold=actionable_confidence_threshold,
            watchlist_confidence_threshold=watchlist_confidence_threshold,
            volume_z_threshold=volume_z_threshold,
            watchlist_volume_z_threshold=watchlist_volume_z_threshold,
            min_touches=min_touches,
            squeeze_threshold=squeeze_threshold,
            dist_to_level_atr=dist_to_level_atr,
            breakout_distance_atr=breakout_distance_atr,
            body_ratio_threshold=body_ratio_threshold,
            close_to_extreme_threshold=close_to_extreme_threshold,
            range_expansion_threshold=range_expansion_threshold,
            sl_multiplier=sl_multiplier,
        )
        await cache.set_strategy_config(config.model_dump(mode="json"))
        return RedirectResponse(url="/settings?strategy_saved=applied", status_code=303)

    @app.get("/settings/strategy-defaults")
    async def strategy_defaults() -> RedirectResponse:
        await cache.set_strategy_config(settings.default_strategy_config())
        return RedirectResponse(url="/settings?strategy_saved=defaults", status_code=303)

    @app.get("/settings/universe/add")
    async def add_universe_symbol(symbol: str, venue: str) -> RedirectResponse:
        normalized = inspector.normalize_symbol(symbol)
        selected_venue = Venue(venue.lower())
        current = await inspector.list_universe()
        updated_map = {item.symbol.upper(): item for item in current}
        new_item = UniverseSymbol(symbol=normalized, primary_venue=selected_venue)
        updated_map[normalized] = new_item
        updated = sorted(updated_map.values(), key=lambda item: (item.primary_venue.value, item.symbol))
        await repository.upsert_runtime_universe_symbol(new_item)
        await cache.store_universe(updated)
        save_universe(settings.universe_path, updated)
        return RedirectResponse(url=f"/settings?manage_symbol={quote_plus(normalized)}&universe_saved=added", status_code=303)

    @app.get("/settings/universe/remove")
    async def remove_universe_symbol(symbol: str) -> RedirectResponse:
        normalized = inspector.normalize_symbol(symbol)
        current = await inspector.list_universe()
        updated = [item for item in current if item.symbol.upper() != normalized.upper()]
        await repository.remove_runtime_universe_symbol(normalized)
        await cache.store_universe(updated)
        save_universe(settings.universe_path, updated)
        return RedirectResponse(url=f"/settings?manage_symbol={quote_plus(normalized)}&universe_saved=removed", status_code=303)

    @app.get("/statistics", response_class=HTMLResponse)
    async def statistics_page(
        range: str = Query(default="day", pattern="^(day|week|month|custom)$"),
        start: str | None = None,
        end: str | None = None,
        q: str = Query(default=""),
        sort_by: str = Query(default="total", pattern="^(symbol|total|wins|losses|winrate|avg_confidence)$"),
        sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    ) -> HTMLResponse:
        start_local, end_local, start_utc, end_utc = _resolve_statistics_window(range_name=range, start=start, end=end, local_tz=local_tz)
        snapshot = await inspector.build_statistics(start_at=start_utc, end_at=end_utc, symbol_query=q)
        snapshot.rows = _sort_statistics_rows(snapshot.rows, sort_by=sort_by, sort_dir=sort_dir)
        export_href = _statistics_export_href(
            range_name=range,
            start_value=start_local.isoformat(),
            end_value=(end_local - timedelta(days=1)).isoformat(),
            query=q,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        body = f"""
        <section class="hero compact">
          <div>
            <p class="eyebrow">Performance / Statistics</p>
            <h1>Setup Statistics</h1>
            <p class="hero-copy">Performance summary for wins, losses, and pending setups with breakdowns by symbol and time range.</p>
          </div>
          <div class="hero-meta">
            {_statistics_range_links(selected=range, query=q)}
          </div>
        </section>
        <section class="panel">
          {_statistics_filter_form(range_name=range, start_value=start_local.isoformat(), end_value=(end_local - timedelta(days=1)).isoformat(), query=q, export_href=export_href, sort_by=sort_by, sort_dir=sort_dir)}
        </section>
        <section class="metrics-grid">
          {_metric_card("Total", str(snapshot.total), f"Range: {start_local.isoformat()} to {(end_local - timedelta(days=1)).isoformat()}")}
          {_metric_card("Wins", str(snapshot.success), "Setups where price reached TP1 before invalidation.")}
          {_metric_card("Losses", str(snapshot.failed), "Setups where invalidation was hit first.")}
          {_metric_card("Win rate", f"{snapshot.win_rate:.1f}%", "Calculated on resolved setups only.")}
          {_metric_card("Pending", str(snapshot.pending), "Setups that are still active or unresolved.")}
          {_metric_card("Avg confidence", f"{snapshot.avg_confidence:.1f}", "Average confidence over the selected range.")}
        </section>
        <section class="two-col">
          <div class="panel">
            <div class="panel-head">
              <h2>Range Summary</h2>
              <span class="muted">Actionable: {snapshot.actionable} · Watchlist: {snapshot.watchlist}</span>
            </div>
            {_statistics_overview(snapshot)}
          </div>
          <div class="panel">
            <div class="panel-head">
              <h2>By Symbol</h2>
              <span class="muted">Filter: {escape(q or 'not set')}</span>
            </div>
            {_statistics_table(snapshot.rows, range_name=range, start_value=start_local.isoformat(), end_value=(end_local - timedelta(days=1)).isoformat(), query=q, sort_by=sort_by, sort_dir=sort_dir)}
          </div>
        </section>
        """
        return HTMLResponse(_page("Statistics", "statistics", body, refresh_seconds=60))

    @app.get("/statistics/export.xlsx")
    async def statistics_export(
        range: str = Query(default="day", pattern="^(day|week|month|custom)$"),
        start: str | None = None,
        end: str | None = None,
        q: str = Query(default=""),
    ) -> Response:
        effective_range = "custom" if start and end else range
        start_local, end_local, start_utc, end_utc = _resolve_statistics_window(range_name=effective_range, start=start, end=end, local_tz=local_tz)
        snapshot = await inspector.build_statistics(start_at=start_utc, end_at=end_utc, symbol_query=q)
        export_rows = await _statistics_export_rows(repository, inspector, local_tz, start_utc, end_utc, q)
        workbook = _statistics_workbook(
            snapshot=snapshot,
            range_name=effective_range,
            start_local=start_local,
            end_local=end_local,
            symbol_query=q,
            export_rows=export_rows,
        )
        filename = f"brakerscalp-statistics-{effective_range}-{start_local.isoformat()}-{(end_local - timedelta(days=1)).isoformat()}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return Response(
            content=workbook.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    @app.get("/charts/signal/{decision_id}.png")
    async def signal_chart(decision_id: str) -> Response:
        chart = await inspector.render_signal_chart(decision_id)
        if chart is None:
            return Response(status_code=404)
        return Response(chart, media_type="image/png")

    @app.get("/charts/scan.png")
    async def scan_chart(symbol: str) -> Response:
        chart = await inspector.render_manual_chart(symbol)
        if chart is None:
            return Response(status_code=404)
        return Response(chart, media_type="image/png")

    @app.get("/health/live")
    async def health_live() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/health/ready")
    async def health_ready() -> JSONResponse:
        return JSONResponse({"status": "ready", "signals": await repository.signal_count()})

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/debug/candidates")
    async def debug_candidates() -> JSONResponse:
        items = await repository.list_latest_candidates(limit=20)
        return JSONResponse(
            [
                {
                    "level_id": item.level_id,
                    "symbol": item.symbol,
                    "venue": item.venue,
                    "timeframe": item.timeframe,
                    "kind": item.kind,
                    "source": item.source,
                    "zone": [item.lower_price, item.upper_price],
                    "strength": item.strength,
                }
                for item in items
            ]
        )

    @app.get("/debug/alerts/latest")
    async def debug_alerts_latest() -> JSONResponse:
        items = await repository.list_latest_alerts(limit=20)
        return JSONResponse(
            [
                {
                    "symbol": item.symbol,
                    "setup": item.setup,
                    "direction": item.direction,
                    "signal_class": item.signal_class,
                    "confidence": item.confidence,
                    "detected_at": item.detected_at.isoformat(),
                    "alert_key": item.alert_key,
                }
                for item in items
            ]
        )

    @app.get("/debug/deliveries/latest")
    async def debug_deliveries_latest() -> JSONResponse:
        items = await repository.list_latest_deliveries(limit=20)
        return JSONResponse(
            [
                {
                    "signal_id": item.signal_id,
                    "alert_key": item.alert_key,
                    "chat_id": item.chat_id,
                    "message_thread_id": item.message_thread_id,
                    "signal_class": item.signal_class,
                    "status": item.status,
                    "error_message": item.error_message,
                    "updated_at": item.updated_at.isoformat(),
                }
                for item in items
            ]
        )

    @app.get("/debug/deliveries/counts")
    async def debug_delivery_counts() -> JSONResponse:
        return JSONResponse(await repository.delivery_status_counts())

    @app.get("/debug/venues/health")
    async def debug_venues_health() -> JSONResponse:
        items = await repository.list_latest_health(limit=90)
        return JSONResponse(
            [
                {
                    "venue": item.venue,
                    "symbol": item.symbol,
                    "timestamp": item.timestamp.isoformat(),
                    "is_fresh": item.is_fresh,
                    "has_sequence_gap": item.has_sequence_gap,
                    "spread_ratio": item.spread_ratio,
                    "freshness_ms": item.freshness_ms,
                    "reconnect_count": item.reconnect_count,
                    "notes": item.notes,
                }
                for item in items
            ]
        )

    @app.get("/debug/runtime-config")
    async def debug_runtime_config() -> JSONResponse:
        minimum_alert_confidence = await _current_minimum_alert_confidence(cache, settings)
        risk_usdt = await _current_risk_usdt(cache, settings)
        strategy_config = await _current_strategy_config(cache, settings)
        return JSONResponse(
            {
                "app_name": settings.app_name,
                "environment": settings.environment,
                "timezone": settings.timezone,
                "enabled_venues": settings.enabled_venues,
                "allowed_chat_ids": settings.allowed_chat_ids,
                "alert_chat_ids": settings.effective_alert_chat_ids,
                "alert_message_thread_id": settings.alert_message_thread_id,
                "poll_interval_seconds": settings.poll_interval_seconds,
                "engine_interval_seconds": settings.engine_interval_seconds,
                "minimum_alert_confidence": settings.minimum_alert_confidence,
                "runtime_minimum_alert_confidence": minimum_alert_confidence,
                "risk_usdt": settings.risk_usdt,
                "runtime_risk_usdt": risk_usdt,
                "default_strategy_config": settings.default_strategy_config(),
                "runtime_strategy_config": strategy_config.model_dump(mode="json"),
                "exchange_book_depth": settings.exchange_book_depth,
                "exchange_trades_limit": settings.exchange_trades_limit,
                "universe_path": str(settings.universe_path),
            }
        )

    return app


def _load_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


async def _signal_stats(repository: Repository) -> tuple[int, int]:
    end_at = datetime.now(tz=timezone.utc)
    start_at = end_at - timedelta(hours=24)
    signals = await repository.list_signals_between(start_at, end_at, signal_classes=["actionable", "watchlist"])
    actionable = sum(1 for item in signals if item.signal_class == "actionable")
    watchlist = sum(1 for item in signals if item.signal_class == "watchlist")
    return actionable, watchlist


async def _current_minimum_alert_confidence(cache: StateCache, settings: Settings) -> float:
    if hasattr(cache, "get_minimum_alert_confidence"):
        return await cache.get_minimum_alert_confidence(settings.minimum_alert_confidence)
    return settings.minimum_alert_confidence


async def _current_risk_usdt(cache: StateCache, settings: Settings) -> float:
    if hasattr(cache, "get_risk_usdt"):
        return await cache.get_risk_usdt(settings.risk_usdt)
    return settings.risk_usdt


async def _current_strategy_config(cache: StateCache, settings: Settings) -> StrategyRuntimeConfig:
    default = settings.default_strategy_config()
    if hasattr(cache, "get_strategy_config"):
        return StrategyRuntimeConfig.model_validate(await cache.get_strategy_config(default=default))
    return StrategyRuntimeConfig.model_validate(default)


def _parse_optional_float(value: str | None, *, minimum: float, maximum: float) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < minimum or parsed > maximum:
        return None
    return parsed


def _resolve_statistics_window(
    *,
    range_name: str,
    start: str | None,
    end: str | None,
    local_tz: ZoneInfo,
) -> tuple[date, date, datetime, datetime]:
    today_local = datetime.now(local_tz).date()
    if range_name == "week":
        start_local = today_local - timedelta(days=6)
        end_local = today_local + timedelta(days=1)
    elif range_name == "month":
        start_local = today_local - timedelta(days=29)
        end_local = today_local + timedelta(days=1)
    elif range_name == "custom" and start and end:
        try:
            start_local = date.fromisoformat(start)
            end_local = date.fromisoformat(end) + timedelta(days=1)
        except ValueError:
            start_local = today_local
            end_local = today_local + timedelta(days=1)
    else:
        start_local = today_local
        end_local = today_local + timedelta(days=1)

    if end_local <= start_local:
        end_local = start_local + timedelta(days=1)

    start_dt = datetime.combine(start_local, datetime.min.time(), tzinfo=local_tz).astimezone(timezone.utc)
    end_dt = datetime.combine(end_local, datetime.min.time(), tzinfo=local_tz).astimezone(timezone.utc)
    return start_local, end_local, start_dt, end_dt


async def _service_statuses(repository: Repository, cache: StateCache, settings: Settings, local_tz: ZoneInfo) -> list[dict[str, Any]]:
    statuses: list[dict[str, Any]] = []
    now = datetime.now(tz=timezone.utc)
    statuses.append(
        {
            "name": "API",
            "status": "online",
            "summary": f"FastAPI слушает {settings.api_host}:{settings.api_port}",
            "details": ["Рут-командный пункт активен", "JSON debug endpoints доступны"],
            "updated_at": now,
        }
    )
    try:
        count = await repository.signal_count()
        statuses.append(
            {
                "name": "PostgreSQL",
                "status": "online",
                "summary": f"БД отвечает, signals={count}",
                "details": [f"Хост: {settings.postgres_host}:{settings.postgres_port}", f"База: {settings.postgres_db}"],
                "updated_at": now,
            }
        )
    except Exception as exc:
        statuses.append(
            {
                "name": "PostgreSQL",
                "status": "offline",
                "summary": str(exc),
                "details": [f"Хост: {settings.postgres_host}:{settings.postgres_port}"],
                "updated_at": now,
            }
        )
    try:
        redis_ok = await cache.ping()
        outbox = await cache.outbox_size()
        statuses.append(
            {
                "name": "Redis",
                "status": "online" if redis_ok else "warning",
                "summary": f"Redis отвечает, outbox={outbox}",
                "details": [f"Хост: {settings.redis_host}:{settings.redis_port}", f"DB: {settings.redis_db}"],
                "updated_at": now,
            }
        )
    except Exception as exc:
        statuses.append(
            {
                "name": "Redis",
                "status": "offline",
                "summary": str(exc),
                "details": [f"Хост: {settings.redis_host}:{settings.redis_port}"],
                "updated_at": now,
            }
        )
    for service_name in ["collector", "engine", "bot"]:
        heartbeat = await cache.get_service_heartbeat(service_name)
        statuses.append(_heartbeat_status(service_name, heartbeat, local_tz))
    return statuses


def _heartbeat_status(service_name: str, heartbeat: dict[str, Any] | None, local_tz: ZoneInfo) -> dict[str, Any]:
    labels = {
        "collector": "Collector",
        "engine": "Engine",
        "bot": "Bot",
    }
    if heartbeat is None:
        return {
            "name": labels.get(service_name, service_name),
            "status": "offline",
            "summary": "Heartbeat отсутствует.",
            "details": ["Сервис не писал heartbeat в Redis в рамках TTL."],
            "updated_at": None,
        }
    timestamp = datetime.fromisoformat(heartbeat["timestamp"])
    age_seconds = int((datetime.now(tz=timezone.utc) - timestamp).total_seconds())
    status = "online" if age_seconds <= 90 else "warning" if age_seconds <= 180 else "offline"
    details = [f"{key}: {value}" for key, value in heartbeat.items() if key not in {"timestamp", "service"}]
    details.append(f"Последний heartbeat: {_format_dt(timestamp, local_tz)}")
    return {
        "name": labels.get(service_name, service_name),
        "status": status,
        "summary": f"Heartbeat {age_seconds}s назад.",
        "details": details,
        "updated_at": timestamp,
    }


def _page(title: str, active_tab: str, body: str, refresh_seconds: int | None) -> str:
    refresh_meta = f'<meta http-equiv="refresh" content="{refresh_seconds}">' if refresh_seconds else ""
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {refresh_meta}
  <title>{escape(title)} · BrakerScalp</title>
  <style>
    :root {{
      --bg: #071018;
      --bg-alt: #0d1822;
      --panel: rgba(13, 24, 34, 0.82);
      --panel-strong: rgba(15, 29, 42, 0.94);
      --line: rgba(139, 176, 205, 0.18);
      --text: #edf3f7;
      --muted: #8aa4b6;
      --accent: #ffb74d;
      --accent-soft: rgba(255, 183, 77, 0.16);
      --good: #49d29a;
      --warn: #ffd166;
      --danger: #ff6b6b;
      --neutral: #7ea0b9;
      --link: #8cd9ff;
      --shadow: 0 24px 60px rgba(0, 0, 0, 0.35);
      --radius: 20px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(255, 183, 77, 0.10), transparent 28%),
        radial-gradient(circle at top right, rgba(97, 218, 251, 0.09), transparent 24%),
        linear-gradient(180deg, #061018 0%, #0b131b 40%, #071018 100%);
    }}
    a {{ color: var(--link); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .shell {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 24px;
    }}
    .topbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      margin-bottom: 22px;
    }}
    .brand {{
      display: flex;
      align-items: center;
      gap: 14px;
      padding: 12px 16px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 999px;
      box-shadow: var(--shadow);
    }}
    .brand-mark {{
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: linear-gradient(135deg, var(--accent), #ffe29a);
      box-shadow: 0 0 22px rgba(255, 183, 77, 0.55);
    }}
    .brand-copy strong {{
      display: block;
      font-size: 14px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .brand-copy span {{
      color: var(--muted);
      font-size: 12px;
    }}
    .nav {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      justify-content: flex-end;
    }}
    .nav a {{
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(13, 24, 34, 0.68);
      color: var(--muted);
      font-size: 13px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}
    .nav a.is-active {{
      background: var(--accent-soft);
      color: var(--text);
      border-color: rgba(255, 183, 77, 0.35);
    }}
    .hero {{
      display: flex;
      justify-content: space-between;
      gap: 20px;
      padding: 28px;
      background:
        linear-gradient(135deg, rgba(255, 183, 77, 0.10), transparent 35%),
        linear-gradient(180deg, rgba(15, 29, 42, 0.95), rgba(11, 20, 28, 0.92));
      border: 1px solid var(--line);
      border-radius: calc(var(--radius) + 2px);
      box-shadow: var(--shadow);
      margin-bottom: 22px;
      overflow: hidden;
      position: relative;
    }}
    .hero.compact {{ padding: 24px 28px; }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -80px -60px auto;
      width: 240px;
      height: 240px;
      background: radial-gradient(circle, rgba(140, 217, 255, 0.16), transparent 68%);
      pointer-events: none;
    }}
    .eyebrow {{
      margin: 0 0 10px;
      color: var(--accent);
      font-size: 12px;
      letter-spacing: 0.16em;
      text-transform: uppercase;
    }}
    h1, h2, h3, p {{ margin-top: 0; }}
    h1 {{ margin-bottom: 12px; font-size: clamp(28px, 3vw, 42px); line-height: 1.05; }}
    h2 {{ margin-bottom: 14px; font-size: 20px; }}
    h3 {{ margin-bottom: 10px; font-size: 16px; }}
    .hero-copy {{ max-width: 760px; color: var(--muted); line-height: 1.55; margin-bottom: 0; }}
    .hero-meta {{
      min-width: 280px;
      display: flex;
      flex-direction: column;
      gap: 10px;
      align-items: flex-end;
      justify-content: flex-start;
      z-index: 1;
    }}
    .hero-chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255, 255, 255, 0.08);
      color: var(--muted);
    }}
    .hero-chip strong {{ color: var(--text); }}
    .hero-chip.linkish.is-active {{
      background: var(--accent-soft);
      color: var(--text);
      border-color: rgba(255, 183, 77, 0.35);
    }}
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 22px;
    }}
    .metric-card, .panel, .service-card, .service-detail, .setup-card, .venue-card, .delivery-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }}
    .metric-card {{
      padding: 18px 20px;
      background:
        linear-gradient(180deg, rgba(255, 183, 77, 0.08), transparent 60%),
        var(--panel);
    }}
    .metric-card h3 {{ margin-bottom: 6px; color: var(--muted); font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; }}
    .metric-value {{ font-size: 34px; font-weight: 700; line-height: 1; margin-bottom: 8px; }}
    .metric-note {{ color: var(--muted); font-size: 13px; line-height: 1.45; }}
    .two-col {{
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 18px;
      margin-bottom: 22px;
    }}
    .panel {{
      padding: 20px;
    }}
    .panel-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      margin-bottom: 14px;
    }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    .service-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .service-card, .service-detail {{
      padding: 16px;
      background: var(--panel-strong);
    }}
    .service-detail-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
    }}
    .service-card p, .service-detail p {{ color: var(--muted); margin-bottom: 8px; line-height: 1.45; }}
    .stack {{
      display: flex;
      flex-direction: column;
      gap: 12px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 7px 10px;
      border-radius: 999px;
      border: 1px solid transparent;
      font-size: 12px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      white-space: nowrap;
    }}
    .badge.good {{ background: rgba(73, 210, 154, 0.14); color: #9bf0c8; border-color: rgba(73, 210, 154, 0.28); }}
    .badge.warn {{ background: rgba(255, 209, 102, 0.14); color: #ffe29a; border-color: rgba(255, 209, 102, 0.25); }}
    .badge.danger {{ background: rgba(255, 107, 107, 0.14); color: #ffb3b3; border-color: rgba(255, 107, 107, 0.25); }}
    .badge.accent {{ background: rgba(140, 217, 255, 0.14); color: #a8e8ff; border-color: rgba(140, 217, 255, 0.25); }}
    .badge.neutral {{ background: rgba(126, 160, 185, 0.14); color: #c6d6e3; border-color: rgba(126, 160, 185, 0.24); }}
    .table-wrap {{
      overflow: auto;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.02);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 820px;
    }}
    th, td {{
      padding: 14px 14px;
      text-align: left;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      font-size: 14px;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      background: rgba(255, 255, 255, 0.02);
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .setup-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }}
    .setup-grid.large {{
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }}
    .setup-card {{
      overflow: hidden;
      background: linear-gradient(180deg, rgba(255, 183, 77, 0.07), rgba(13, 24, 34, 0.94) 28%);
    }}
    .setup-card .top {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      padding: 18px 18px 14px;
    }}
    .setup-card .top h3 {{ margin-bottom: 6px; }}
    .setup-card .meta {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }}
    .setup-card img {{
      width: 100%;
      display: block;
      border-top: 1px solid var(--line);
      border-bottom: 1px solid var(--line);
      background: #061018;
    }}
    .setup-card .body {{
      padding: 16px 18px 18px;
    }}
    .setup-footer {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-top: 14px;
      padding-top: 12px;
      border-top: 1px solid var(--line);
    }}
    .kv-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px 14px;
      margin-bottom: 14px;
    }}
    .kv {{
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.03);
    }}
    .kv span {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .bullets {{
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
      line-height: 1.55;
    }}
    .venue-card, .delivery-card {{
      padding: 16px;
    }}
    .empty {{
      padding: 18px;
      border-radius: 16px;
      border: 1px dashed var(--line);
      color: var(--muted);
      background: rgba(255, 255, 255, 0.02);
    }}
    .settings-table {{
      width: 100%;
      min-width: 0;
    }}
    .manual-form {{
      display: flex;
      gap: 12px;
      margin-bottom: 14px;
      flex-wrap: wrap;
    }}
    .manual-form input,
    .manual-form select {{
      flex: 1 1 220px;
      min-width: 220px;
      padding: 14px 16px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.04);
      color: var(--text);
      font: inherit;
    }}
    .manual-form button {{
      padding: 14px 18px;
      border-radius: 14px;
      border: 0;
      background: linear-gradient(135deg, var(--accent), #ffcf7b);
      color: #091018;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }}
    .button-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 50px;
      padding: 14px 18px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.04);
      color: var(--text);
      font-weight: 700;
      white-space: nowrap;
    }}
    .manual-form select {{
      color-scheme: dark;
    }}
    .manual-form select option {{
      background: #0b1720;
      color: #f5fbff;
    }}
    .strategy-form {{
      align-items: flex-start;
    }}
    .field-block {{
      display: flex;
      flex: 1 1 220px;
      min-width: 220px;
      flex-direction: column;
      gap: 8px;
    }}
    .field-block input,
    .field-block select {{
      min-width: 0;
    }}
    .sort-link {{
      color: inherit;
      text-decoration: none;
    }}
    .tooltip-anchor {{
      position: relative;
      cursor: help;
      border-bottom: 1px dotted rgba(245, 251, 255, 0.28);
    }}
    .tooltip-anchor:hover::after {{
      content: attr(data-tooltip);
      position: absolute;
      left: 0;
      top: calc(100% + 8px);
      z-index: 20;
      width: min(260px, 60vw);
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #0b1720;
      color: #f5fbff;
      white-space: normal;
      font-size: 12px;
      line-height: 1.45;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
    }}
    .scan-card {{
      padding: 16px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.03);
    }}
    .scan-card img {{
      width: 100%;
      margin-top: 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: #061018;
    }}
    .code {{
      font-family: "IBM Plex Mono", "Consolas", monospace;
      font-size: 12px;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid var(--line);
      padding: 3px 7px;
      border-radius: 999px;
      color: #dbe8f0;
    }}
    @media (max-width: 1200px) {{
      .metrics-grid, .service-detail-grid, .setup-grid, .setup-grid.large, .two-col {{
        grid-template-columns: 1fr;
      }}
      .service-grid {{
        grid-template-columns: 1fr;
      }}
    }}
    @media (max-width: 860px) {{
      .shell {{ padding: 16px; }}
      .topbar, .hero {{
        flex-direction: column;
        align-items: stretch;
      }}
      .hero-meta {{ align-items: stretch; min-width: 0; }}
      .nav {{ justify-content: flex-start; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div class="brand">
        <div class="brand-mark"></div>
        <div class="brand-copy">
          <strong>BrakerScalp</strong>
          <span>Impulse Breakout Scalp Control</span>
        </div>
      </div>
      <nav class="nav">
        {_nav_link("/", "dashboard", active_tab, "Командный пункт")}
        {_nav_link("/services", "services", active_tab, "Сервисы")}
        {_nav_link("/setups", "setups", active_tab, "Сетапы")}
        {_nav_link("/screener", "screener", active_tab, "Скринер")}
        {_nav_link("/settings", "settings", active_tab, "Настройки")}
        {_nav_link("/statistics", "statistics", active_tab, "Statistics")}
      </nav>
    </header>
    {body}
  </div>
</body>
</html>"""


def _nav_link(href: str, tab: str, active_tab: str, label: str) -> str:
    class_name = "is-active" if tab == active_tab else ""
    return f'<a class="{class_name}" href="{href}">{escape(label)}</a>'


def _metric_card(title: str, value: str, note: str) -> str:
    return f"""
    <article class="metric-card">
      <h3>{escape(title)}</h3>
      <div class="metric-value">{escape(value)}</div>
      <div class="metric-note">{escape(note)}</div>
    </article>
    """


def _status_badge(status: str) -> str:
    label = STATUS_LABELS.get(status, status)
    tone = STATUS_TONES.get(status, "neutral")
    return f'<span class="badge {tone}">{escape(label)}</span>'


def _outcome_badge(status: str) -> str:
    label = OUTCOME_LABELS.get(status, status)
    tone = OUTCOME_TONES.get(status, "neutral")
    return f'<span class="badge {tone}">{escape(label)}</span>'


def _opportunities_table(rows: list, local_tz: ZoneInfo) -> str:
    if not rows:
        return _empty_block("Сейчас нет монет со статусом выше cold.")
    rendered_rows = []
    for item in rows:
        direction = item.direction.value.upper() if item.direction else "N/A"
        level = f"{item.level_lower:.4f} - {item.level_upper:.4f}" if item.level_lower is not None and item.level_upper is not None else "n/a"
        rendered_rows.append(
            f"""
            <tr>
              <td><strong>{escape(item.symbol)}</strong></td>
              <td>{_status_badge(item.status)}</td>
              <td>{escape(direction)}</td>
              <td>{item.confidence:.1f}</td>
              <td>{level}</td>
              <td>{item.volume_z_15m:.2f}</td>
              <td>{item.squeeze_score:.2f}</td>
              <td>{_format_dt(item.updated_at, local_tz)}</td>
            </tr>
            """
        )
    return f"""
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Монета</th>
            <th>Статус</th>
            <th>Bias</th>
            <th>Score</th>
            <th>Уровень</th>
            <th>Vol z</th>
            <th>Squeeze</th>
            <th>Обновлено</th>
          </tr>
        </thead>
        <tbody>{''.join(rendered_rows)}</tbody>
      </table>
    </div>
    """


def _service_card(item: dict[str, Any]) -> str:
    details = item.get("details") or []
    return f"""
    <article class="service-card">
      <div class="panel-head">
        <h3>{escape(item['name'])}</h3>
        {_status_badge(item['status'])}
      </div>
      <p>{escape(item['summary'])}</p>
      <div class="muted">{escape(details[0]) if details else ''}</div>
    </article>
    """


def _service_detail_card(item: dict[str, Any], local_tz: ZoneInfo) -> str:
    details = "".join(f"<li>{escape(str(line))}</li>" for line in item.get("details") or [])
    updated_at = item.get("updated_at")
    footer = f'<div class="muted">Обновлено: {_format_dt(updated_at, local_tz) if updated_at else "нет данных"}</div>'
    return f"""
    <article class="service-detail">
      <div class="panel-head">
        <h3>{escape(item['name'])}</h3>
        {_status_badge(item['status'])}
      </div>
      <p>{escape(item['summary'])}</p>
      <ul class="bullets">{details}</ul>
      {footer}
    </article>
    """


def _format_qty(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _position_metrics(signal, risk_usdt: float) -> tuple[float | None, float | None, float | None]:
    stop_distance = abs(float(signal.entry_price) - float(signal.invalidation_price))
    if stop_distance <= 0 or risk_usdt <= 0:
        return None, None, None
    qty = float(risk_usdt) / stop_distance
    tp1 = float(signal.targets[0]) if signal.targets else float(signal.entry_price)
    tp2 = float(signal.targets[1]) if len(signal.targets) > 1 else tp1
    tp1_profit = qty * abs(tp1 - float(signal.entry_price))
    tp2_profit = qty * abs(tp2 - float(signal.entry_price))
    return qty, tp1_profit, tp2_profit


def _setup_card(item, local_tz: ZoneInfo, include_meta: bool = False, risk_usdt: float = 0.0) -> str:
    signal = item.signal
    lifecycle = item.lifecycle
    reason_lines = "".join(f"<li>{escape(line)}</li>" for line in (signal.rationale or [])[:4])
    why_lines = "".join(f"<li>{escape(line)}</li>" for line in (signal.why_not_higher or [])[:3])
    chart_url = f"/charts/signal/{quote_plus(signal.decision_id)}.png"
    level_zone = "n/a"
    if signal.render_context:
        level_zone = signal.render_context.get("price_zone", "n/a")
    qty, tp1_profit, tp2_profit = _position_metrics(signal, risk_usdt)
    tp1_value = float(signal.targets[0]) if signal.targets else float(signal.entry_price)
    tp2_value = float(signal.targets[1]) if len(signal.targets) > 1 else tp1_value
    targets_display = (
        f"{tp1_value:.4f} ({tp1_profit:.2f} USDT) / {tp2_value:.4f} ({tp2_profit:.2f} USDT)"
        if tp1_profit is not None and tp2_profit is not None
        else f"{tp1_value:.4f} / {tp2_value:.4f}"
    )
    meta = f"Signal class: {escape(signal.signal_class.upper())}" if include_meta else escape(signal.signal_class.upper())
    return f"""
    <article class="setup-card">
      <div class="top">
        <div>
          <h3>{escape(signal.symbol)} · {escape(signal.setup.upper())} · {escape(signal.direction.upper())}</h3>
          <div class="meta">{meta}</div>
        </div>
        <div>{_outcome_badge(lifecycle.status)}</div>
      </div>
      <img src="{chart_url}" alt="{escape(signal.symbol)} chart">
      <div class="body">
        <div class="kv-grid">
          <div class="kv"><span>Confidence</span>{signal.confidence:.1f}</div>
          <div class="kv"><span>Timeframe</span>{escape(signal.timeframe)}</div>
          <div class="kv"><span>Entry</span>{signal.entry_price:.4f}</div>
          <div class="kv"><span>SL</span>{signal.invalidation_price:.4f}</div>
          <div class="kv"><span>T1 / T2</span>{targets_display}</div>
          <div class="kv"><span>Qty</span>{_format_qty(qty)}</div>
          <div class="kv"><span>Уровень</span>{escape(level_zone)}</div>
        </div>
        <h3>Обоснование</h3>
        <ul class="bullets">{reason_lines}</ul>
        <h3>Почему не выше</h3>
        <ul class="bullets">{why_lines}</ul>
        <div class="setup-footer">
          <span class="muted">Call time</span>
          <strong>{_format_dt(lifecycle.call_at, local_tz)}</strong>
        </div>
      </div>
    </article>
    """


def _venue_health_card(item, local_tz: ZoneInfo) -> str:
    status = "online" if item.is_fresh and not item.has_sequence_gap else "warning" if item.is_fresh else "offline"
    notes = ", ".join(item.notes[:2]) if item.notes else "Ошибок нет"
    return f"""
    <article class="venue-card">
      <div class="panel-head">
        <h3>{escape(item.venue.upper())} · {escape(item.symbol)}</h3>
        {_status_badge(status)}
      </div>
      <div class="muted">Обновлено: {_format_dt(item.timestamp, local_tz)}</div>
      <div class="kv-grid">
        <div class="kv"><span>Freshness</span>{item.freshness_ms} ms</div>
        <div class="kv"><span>Spread</span>{item.spread_ratio:.4f}</div>
      </div>
      <div class="muted">{escape(notes)}</div>
    </article>
    """


def _delivery_card(item, local_tz: ZoneInfo) -> str:
    tone = "good" if item.status == "sent" else "warn" if item.status in {"queued", "requeued"} else "danger"
    return f"""
    <article class="delivery-card">
      <div class="panel-head">
        <h3>{escape(item.signal_id[:16])}</h3>
        <span class="badge {tone}">{escape(item.status)}</span>
      </div>
      <div class="muted">chat_id: {item.chat_id}</div>
      <div class="muted">updated: {_format_dt(item.updated_at, local_tz)}</div>
      <div class="muted">{escape(item.error_message or item.signal_class.upper())}</div>
    </article>
    """


SCREENER_TOOLTIPS = {
    "bias": "HTF direction from the 1h/4h trend. LONG is stronger with an uptrend, SHORT with a downtrend.",
    "dist_atr": "Distance from the level in ATR units. Near zero means price is close to the breakout level.",
    "vol_z": "Volume z-score versus recent history. Above 1.0 is active; above 1.8 is a stronger impulse.",
    "quote_x": "Current quote-volume relative to its recent baseline. Higher values mean the coin is more active now.",
    "squeeze": "Compression score before the breakout. Higher values mean cleaner pressure into the level.",
    "touches": "How many relevant touches the level has. More touches usually mean a more important breakout level.",
    "freshness": "Market-state freshness in milliseconds. Lower is better.",
}


def _sort_screener_rows(rows: list, *, sort_by: str, sort_dir: str) -> list:
    reverse = sort_dir == "desc"
    status_order = {"actionable": 0, "watchlist": 1, "arming": 2, "monitor": 3, "cold": 4, "stale": 5, "insufficient": 6}
    bias_order = {"LONG": 0, "SHORT": 1, None: 2}

    def key(item):
        if sort_by == "symbol":
            return item.symbol
        if sort_by == "status":
            return status_order.get(item.status, 99)
        if sort_by == "bias":
            return bias_order.get(item.direction.value.upper() if item.direction else None, 99)
        if sort_by == "score":
            return float(item.confidence)
        if sort_by == "dist_atr":
            return float(item.breakout_distance_atr)
        if sort_by == "vol_z":
            return float(item.volume_z_15m)
        if sort_by == "quote_x":
            return float(item.quote_activity_ratio)
        if sort_by == "squeeze":
            return float(item.squeeze_score)
        if sort_by == "touches":
            return int(item.cascade_touches)
        if sort_by == "freshness":
            return int(item.freshness_ms)
        return float(item.confidence)

    return sorted(rows, key=key, reverse=reverse)


def _screener_sort_href(*, scope: str, limit: int, sort_by: str, sort_dir: str) -> str:
    return f"/screener?scope={quote_plus(scope)}&limit={limit}&sort_by={quote_plus(sort_by)}&sort_dir={quote_plus(sort_dir)}"


def _screener_header(label: str, key: str, *, scope: str, limit: int, current_sort_by: str, current_sort_dir: str, tooltip: str | None = None) -> str:
    next_dir = "asc" if current_sort_by != key or current_sort_dir == "desc" else "desc"
    arrow = " ^" if current_sort_by == key and current_sort_dir == "asc" else " v" if current_sort_by == key else ""
    text = escape(label) + arrow
    if tooltip:
        text = f'<span class="tooltip-anchor" data-tooltip="{escape(tooltip)}">{text}</span>'
    href = _screener_sort_href(scope=scope, limit=limit, sort_by=key, sort_dir=next_dir)
    return f'<a class="sort-link" href="{href}">{text}</a>'


def _screener_table(rows: list, local_tz: ZoneInfo, *, scope: str, limit: int, sort_by: str, sort_dir: str) -> str:
    if not rows:
        return _empty_block("Screener is empty.")
    rendered_rows = []
    for item in rows:
        level = f"{item.level_lower:.4f} - {item.level_upper:.4f}" if item.level_lower is not None and item.level_upper is not None else "n/a"
        direction = item.direction.value.upper() if item.direction else "N/A"
        rendered_rows.append(
            f"""
            <tr>
              <td><strong>{escape(item.symbol)}</strong></td>
              <td>{_status_badge(item.status)}</td>
              <td>{escape(direction)}</td>
              <td>{item.confidence:.1f}</td>
              <td>{item.last_price:.4f}</td>
              <td>{level}</td>
              <td>{item.breakout_distance_atr:.2f}</td>
              <td>{item.volume_z_15m:.2f}</td>
              <td>{item.quote_activity_ratio:.2f}</td>
              <td>{item.squeeze_score:.2f}</td>
              <td>{item.cascade_touches}</td>
              <td>{item.freshness_ms} ms</td>
              <td>{escape((item.notes or [''])[0])}</td>
              <td>{_format_dt(item.updated_at, local_tz)}</td>
            </tr>
            """
        )
    return f"""
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{_screener_header("Монета", "symbol", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
            <th>{_screener_header("Статус", "status", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
            <th>{_screener_header("BIAS", "bias", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir, tooltip=SCREENER_TOOLTIPS["bias"])}</th>
            <th>{_screener_header("SCORE", "score", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
            <th>Last</th>
            <th>Уровень</th>
            <th>{_screener_header("DIST ATR", "dist_atr", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir, tooltip=SCREENER_TOOLTIPS["dist_atr"])}</th>
            <th>{_screener_header("VOL Z", "vol_z", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir, tooltip=SCREENER_TOOLTIPS["vol_z"])}</th>
            <th>{_screener_header("QUOTE X", "quote_x", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir, tooltip=SCREENER_TOOLTIPS["quote_x"])}</th>
            <th>{_screener_header("SQUEEZE", "squeeze", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir, tooltip=SCREENER_TOOLTIPS["squeeze"])}</th>
            <th>{_screener_header("TOUCHES", "touches", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir, tooltip=SCREENER_TOOLTIPS["touches"])}</th>
            <th>{_screener_header("FRESHNESS", "freshness", scope=scope, limit=limit, current_sort_by=sort_by, current_sort_dir=sort_dir, tooltip=SCREENER_TOOLTIPS["freshness"])}</th>
            <th>Note</th>
            <th>Updated</th>
          </tr>
        </thead>
        <tbody>{''.join(rendered_rows)}</tbody>
      </table>
    </div>
    """


def _available_setup_statuses(items: list) -> list[str]:
    order = {"watch": 0, "executed": 1, "tp1": 2, "tp2": 3, "loss": 4, "invalidation": 5}
    present = {item.lifecycle.status for item in items}
    return sorted(present, key=lambda value: (order.get(value, 99), value))


def _setups_filter_form(
    *,
    status: str,
    symbol_query: str,
    limit: int,
    min_confidence: float | None,
    available_statuses: list[str],
) -> str:
    selected = {"all": "selected" if status == "all" else ""}
    options = ['<option value="all" ' + selected["all"] + '>All statuses</option>']
    for value in available_statuses:
        current = "selected" if status == value else ""
        label = OUTCOME_LABELS.get(value, value.replace("_", " ").title())
        options.append(f'<option value="{escape(value)}" {current}>{escape(label)}</option>')
    return f"""
    <form class="manual-form" method="get" action="/setups">
      <input type="text" name="q" value="{escape(symbol_query)}" placeholder="Filter by symbol, e.g. BTC">
      <input type="number" name="min_confidence" min="0" max="100" step="0.1" value="{'' if min_confidence is None else f'{min_confidence:.1f}'}" placeholder="Min confidence">
      <input type="hidden" name="limit" value="{limit}">
      <select name="status" class="filter-select">
        {''.join(options)}
      </select>
      <button type="submit">Apply</button>
    </form>
    """


def _settings_table(settings: Settings, minimum_alert_confidence: float, risk_usdt: float) -> str:
    rows = [
        ("Окружение", settings.environment),
        ("Timezone", settings.timezone),
        ("Биржи", ", ".join(settings.enabled_venues)),
        ("Collector interval", f"{settings.poll_interval_seconds}s"),
        ("Engine interval", f"{settings.engine_interval_seconds}s"),
        ("Min confidence env", f"{settings.minimum_alert_confidence:.1f}"),
        ("Min confidence runtime", f"{minimum_alert_confidence:.1f}"),
        ("Risk USDT env", f"{settings.risk_usdt:.2f}"),
        ("Risk USDT runtime", f"{risk_usdt:.2f}"),
        ("API bind", f"{settings.api_host}:{settings.api_port}"),
        ("Universe path", str(settings.universe_path)),
        ("Alert chats", ", ".join(map(str, settings.effective_alert_chat_ids))),
        ("Alert topic", str(settings.alert_message_thread_id)),
        ("Book depth", str(settings.exchange_book_depth)),
        ("Trades limit", str(settings.exchange_trades_limit)),
    ]
    rendered = "".join(
        f"<tr><th>{escape(label)}</th><td><span class='code'>{escape(value)}</span></td></tr>"
        for label, value in rows
    )
    return f"""
    <div class="table-wrap">
      <table class="settings-table">
        <tbody>{rendered}</tbody>
      </table>
    </div>
    """


STRATEGY_FIELD_HELP = {
    "timeframe": "Execution timeframe for the trigger. 5m is faster and stricter; 15m is slower and more selective.",
    "minimum_expected_rr": "Minimum expected reward-to-risk. Values below 2.0 are usually too weak for this bot.",
    "actionable_confidence_threshold": "Confidence threshold for an actionable setup.",
    "watchlist_confidence_threshold": "Confidence threshold for a watch setup before activation.",
    "volume_z_threshold": "Minimum volume z-score for an activated breakout.",
    "watchlist_volume_z_threshold": "Minimum volume z-score for a watch setup near the level.",
    "min_touches": "Minimum number of touches around the level. More touches usually mean a cleaner breakout level.",
    "squeeze_threshold": "Minimum squeeze score for a clean pressure build-up into the level.",
    "dist_to_level_atr": "How close price must stay to the level, in ATR units, to qualify as a watch setup.",
    "breakout_distance_atr": "How far the breakout close must extend beyond the level, in ATR units.",
    "body_ratio_threshold": "Minimum candle body ratio for the breakout candle. Higher means stronger close conviction.",
    "close_to_extreme_threshold": "How close the candle must close to its extreme. Lower values mean stronger impulse candles.",
    "range_expansion_threshold": "Required range expansion versus recent candles. Higher means more explosive breakouts only.",
    "sl_multiplier": "ATR floor used in stop-distance calculation. Higher values widen the minimum stop and reduce position size.",
}


def _strategy_input(name: str, label: str, value: str, *, tooltip: str, min_value: str | None = None, max_value: str | None = None, step: str | None = None) -> str:
    attrs = []
    if min_value is not None:
        attrs.append(f'min="{min_value}"')
    if max_value is not None:
        attrs.append(f'max="{max_value}"')
    if step is not None:
        attrs.append(f'step="{step}"')
    attr_text = ' '.join(attrs)
    return f"""
    <label class="field-block">
      <span class="tooltip-anchor" data-tooltip="{escape(tooltip)}">{escape(label)}</span>
      <input type="number" name="{escape(name)}" value="{escape(value)}" {attr_text}>
    </label>
    """


def _runtime_settings_form(
    minimum_alert_confidence: float,
    risk_usdt: float,
    strategy_config: StrategyRuntimeConfig,
    *,
    threshold_saved: bool,
    risk_saved: bool,
    strategy_saved: str | None,
) -> str:
    threshold_message = '<div class="muted">Threshold saved. Engine will apply it on the next cycle.</div>' if threshold_saved else ''
    risk_message = '<div class="muted">Risk USDT saved. Qty and projected TP profit are recalculated immediately in the UI.</div>' if risk_saved else ''
    strategy_message = ''
    if strategy_saved == "applied":
        strategy_message = '<div class="muted">Strategy runtime settings saved. Engine and screener will use them on the next cycle.</div>'
    elif strategy_saved == "defaults":
        strategy_message = '<div class="muted">Default strategy values restored.</div>'
    strategy_fields = ''.join([
        f"""<label class="field-block"><span class="tooltip-anchor" data-tooltip="{escape(STRATEGY_FIELD_HELP['timeframe'])}">Timeframe</span><select name="timeframe"><option value="5m" {'selected' if strategy_config.timeframe.value == '5m' else ''}>5m</option><option value="15m" {'selected' if strategy_config.timeframe.value == '15m' else ''}>15m</option></select></label>""",
        _strategy_input('minimum_expected_rr', 'Min expected R:R', f'{strategy_config.minimum_expected_rr:.2f}', tooltip=STRATEGY_FIELD_HELP['minimum_expected_rr'], min_value='1', max_value='10', step='0.1'),
        _strategy_input('actionable_confidence_threshold', 'Actionable confidence', f'{strategy_config.actionable_confidence_threshold:.1f}', tooltip=STRATEGY_FIELD_HELP['actionable_confidence_threshold'], min_value='0', max_value='100', step='0.1'),
        _strategy_input('watchlist_confidence_threshold', 'Watch confidence', f'{strategy_config.watchlist_confidence_threshold:.1f}', tooltip=STRATEGY_FIELD_HELP['watchlist_confidence_threshold'], min_value='0', max_value='100', step='0.1'),
        _strategy_input('volume_z_threshold', 'volume_z_threshold', f'{strategy_config.volume_z_threshold:.2f}', tooltip=STRATEGY_FIELD_HELP['volume_z_threshold'], min_value='0', max_value='20', step='0.05'),
        _strategy_input('watchlist_volume_z_threshold', 'watchlist_volume_z_threshold', f'{strategy_config.watchlist_volume_z_threshold:.2f}', tooltip=STRATEGY_FIELD_HELP['watchlist_volume_z_threshold'], min_value='0', max_value='20', step='0.05'),
        _strategy_input('min_touches', 'min_touches', str(strategy_config.min_touches), tooltip=STRATEGY_FIELD_HELP['min_touches'], min_value='1', max_value='10', step='1'),
        _strategy_input('squeeze_threshold', 'squeeze_threshold', f'{strategy_config.squeeze_threshold:.2f}', tooltip=STRATEGY_FIELD_HELP['squeeze_threshold'], min_value='0', max_value='1.5', step='0.01'),
        _strategy_input('dist_to_level_atr', 'dist_to_level_atr', f'{strategy_config.dist_to_level_atr:.2f}', tooltip=STRATEGY_FIELD_HELP['dist_to_level_atr'], min_value='0', max_value='5', step='0.01'),
        _strategy_input('breakout_distance_atr', 'breakout_distance_atr', f'{strategy_config.breakout_distance_atr:.2f}', tooltip=STRATEGY_FIELD_HELP['breakout_distance_atr'], min_value='0', max_value='5', step='0.01'),
        _strategy_input('body_ratio_threshold', 'body_ratio_threshold', f'{strategy_config.body_ratio_threshold:.2f}', tooltip=STRATEGY_FIELD_HELP['body_ratio_threshold'], min_value='0', max_value='1', step='0.01'),
        _strategy_input('close_to_extreme_threshold', 'close_to_extreme_threshold', f'{strategy_config.close_to_extreme_threshold:.2f}', tooltip=STRATEGY_FIELD_HELP['close_to_extreme_threshold'], min_value='0', max_value='1', step='0.01'),
        _strategy_input('range_expansion_threshold', 'range_expansion_threshold', f'{strategy_config.range_expansion_threshold:.2f}', tooltip=STRATEGY_FIELD_HELP['range_expansion_threshold'], min_value='0', max_value='10', step='0.05'),
        _strategy_input('sl_multiplier', 'sl_multiplier', f'{strategy_config.sl_multiplier:.2f}', tooltip=STRATEGY_FIELD_HELP['sl_multiplier'], min_value='0.01', max_value='5', step='0.01'),
    ])
    return f"""
    <div class="scan-card">
      <h3>Telegram send threshold</h3>
      <div class="muted">Signals below this confidence remain in the database, but are not enqueued for Telegram delivery.</div>
      <form class="manual-form" method="get" action="/settings/apply-threshold">
        <input type="number" name="value" min="0" max="100" step="0.1" value="{minimum_alert_confidence:.1f}">
        <button type="submit">Apply threshold</button>
      </form>
      {threshold_message}
      <h3>Risk USDT</h3>
      <div class="muted">Maximum planned loss per setup. Qty is calculated as Risk USDT divided by the distance between Entry and SL.</div>
      <form class="manual-form" method="get" action="/settings/apply-risk">
        <input type="number" name="value" min="0.01" max="1000000" step="0.01" value="{risk_usdt:.2f}">
        <button type="submit">Apply risk</button>
      </form>
      {risk_message}
      <h3>Strategy runtime</h3>
      <div class="muted">Adjust breakout filters without restarting the stack. The values are applied from Redis at runtime.</div>
      <form class="manual-form strategy-form" method="get" action="/settings/apply-strategy">
        {strategy_fields}
        <button type="submit">Apply</button>
        <a class="button-link" href="/settings/strategy-defaults">By default</a>
      </form>
      {strategy_message}
    </div>
    """


def _manual_scan_form(value: str) -> str:
    return f"""
    <form class="manual-form" method="get" action="/settings">
      <input type="text" name="symbol" value="{escape(value)}" placeholder="Например: BTC, ETHUSDT, POLUSDT">
      <button type="submit">Проверить токен</button>
    </form>
    <div class="muted">Поддерживаются символы из universe и внешний live-поиск по включенным биржам.</div>
    """


def _manual_scan_card(symbol: str | None, scan: ManualScanResult | None, local_tz: ZoneInfo) -> str:
    form = _manual_scan_form(symbol or "")
    if scan is None:
        return form
    if scan.report is None:
        errors = "".join(f"<li>{escape(line)}</li>" for line in scan.errors)
        return f"""
        {form}
        <div class="scan-card">
          {_status_badge("offline")}
          <h3>Символ не удалось проверить</h3>
          <div class="muted">Источник: {escape(scan.source)}</div>
          <ul class="bullets">{errors}</ul>
        </div>
        """
    report = scan.report
    notes = "".join(f"<li>{escape(line)}</li>" for line in report.notes)
    errors = "".join(f"<li>{escape(line)}</li>" for line in scan.errors)
    chart_block = f'<img src="/charts/scan.png?symbol={quote_plus(report.symbol)}" alt="{escape(report.symbol)} scan chart">' if report.level_lower is not None else ""
    level = f"{report.level_lower:.4f} - {report.level_upper:.4f}" if report.level_lower is not None and report.level_upper is not None else "n/a"
    direction = report.direction.value.upper() if report.direction else "N/A"
    error_section = f"<h3>Ошибки fallback</h3><ul class='bullets'>{errors}</ul>" if scan.errors else ""
    return f"""
    {form}
    <div class="scan-card">
      <div class="panel-head">
        <h3>{escape(report.symbol)} · ручная проверка</h3>
        {_status_badge(report.status)}
      </div>
      <div class="muted">Источник: {escape(scan.source)} · Обновлено: {_format_dt(report.updated_at, local_tz)}</div>
      <div class="kv-grid">
        <div class="kv"><span>Bias</span>{escape(direction)}</div>
        <div class="kv"><span>Score</span>{report.confidence:.1f}</div>
        <div class="kv"><span>Last</span>{report.last_price:.4f}</div>
        <div class="kv"><span>Уровень</span>{escape(level)}</div>
        <div class="kv"><span>Vol z</span>{report.volume_z_15m:.2f}</div>
        <div class="kv"><span>Quote x</span>{report.quote_activity_ratio:.2f}</div>
        <div class="kv"><span>Squeeze</span>{report.squeeze_score:.2f}</div>
        <div class="kv"><span>Freshness</span>{report.freshness_ms} ms</div>
      </div>
      <h3>Диагностика</h3>
      <ul class="bullets">{notes}</ul>
      {error_section}
      {chart_block}
    </div>
    """


def _universe_manage_form(value: str, universe_saved: str | None) -> str:
    saved = ""
    if universe_saved == "added":
        saved = '<div class="muted">Token added to the runtime universe.</div>'
    elif universe_saved == "removed":
        saved = '<div class="muted">Token removed from the runtime universe.</div>'
    return f"""
    <div class="scan-card">
      <h3>Universe Management</h3>
      <div class="muted">Enter a token symbol, check where it exists, then add it with a chosen primary venue.</div>
      <form class="manual-form" method="get" action="/settings">
        <input type="text" name="manage_symbol" value="{escape(value)}" placeholder="For example: BTC, ETHUSDT, WIFUSDT">
        <button type="submit">Find venues</button>
      </form>
      {saved}
    </div>
    """


def _discovered_venues_block(symbol: str, probes: list) -> str:
    if not symbol:
        return ""
    cards = []
    for item in probes:
        action = (
            f'<a class="button-link" href="/settings/universe/add?symbol={quote_plus(symbol)}&venue={quote_plus(item.venue)}">Add via {escape(item.venue.upper())}</a>'
            if item.available
            else ""
        )
        cards.append(
            f"""
            <article class="service-card">
              <div class="panel-head">
                <h3>{escape(item.venue.upper())}</h3>
                {_status_badge("online" if item.available else "offline")}
              </div>
              <div class="muted">{escape(item.message)}</div>
              {action}
            </article>
            """
        )
    return f"""
    <div class="scan-card">
      <div class="panel-head">
        <h3>Venue discovery for {escape(symbol)}</h3>
        <span class="muted">{len(probes)} venue checks</span>
      </div>
      <div class="service-grid">{''.join(cards)}</div>
    </div>
    """


def _universe_table(items: list[UniverseSymbol]) -> str:
    if not items:
        return _empty_block("Universe is empty.")
    rows = []
    for item in items:
        rows.append(
            f"""
            <tr>
              <td><strong>{escape(item.symbol)}</strong></td>
              <td>{escape(item.primary_venue.value.upper())}</td>
              <td><a href="/settings/universe/remove?symbol={quote_plus(item.symbol)}">Remove</a></td>
            </tr>
            """
        )
    return f"""
    <div class="scan-card">
      <div class="panel-head">
        <h3>Runtime Universe</h3>
        <span class="muted">{len(items)} symbols</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Primary venue</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
      </div>
    </div>
    """


def _statistics_range_links(*, selected: str, query: str) -> str:
    links = []
    for item, label in [("day", "Day"), ("week", "Week"), ("month", "Month"), ("custom", "Custom")]:
        active = "is-active" if item == selected else ""
        href = f"/statistics?range={item}&q={quote_plus(query)}"
        links.append(f'<a class="hero-chip linkish {active}" href="{href}">{label}</a>')
    return "".join(links)


def _statistics_export_href(*, range_name: str, start_value: str, end_value: str, query: str, sort_by: str, sort_dir: str) -> str:
    return (
        f"/statistics/export.xlsx?range={quote_plus(range_name)}"
        f"&start={quote_plus(start_value)}"
        f"&end={quote_plus(end_value)}"
        f"&q={quote_plus(query)}"
        f"&sort_by={quote_plus(sort_by)}"
        f"&sort_dir={quote_plus(sort_dir)}"
    )


def _statistics_filter_form(*, range_name: str, start_value: str, end_value: str, query: str, export_href: str, sort_by: str, sort_dir: str) -> str:
    return f"""
    <form class="manual-form" method="get" action="/statistics">
      <input type="hidden" name="range" value="{escape(range_name)}">
      <input type="hidden" name="sort_by" value="{escape(sort_by)}">
      <input type="hidden" name="sort_dir" value="{escape(sort_dir)}">
      <input type="date" name="start" value="{escape(start_value)}">
      <input type="date" name="end" value="{escape(end_value)}">
      <input type="text" name="q" value="{escape(query)}" placeholder="Filter by symbol, e.g. BTC">
      <button type="submit">Apply range</button>
      <button class="button-link" type="submit" formaction="/statistics/export.xlsx">Export Excel</button>
    </form>
    """


def _statistics_overview(snapshot) -> str:
    return f"""
    <div class="kv-grid">
      <div class="kv"><span>Resolved</span>{snapshot.success + snapshot.failed}</div>
      <div class="kv"><span>Pending</span>{snapshot.pending}</div>
      <div class="kv"><span>Actionable</span>{snapshot.actionable}</div>
      <div class="kv"><span>Watchlist</span>{snapshot.watchlist}</div>
      <div class="kv"><span>Winrate</span>{snapshot.win_rate:.1f}%</div>
      <div class="kv"><span>Avg confidence</span>{snapshot.avg_confidence:.1f}</div>
    </div>
    """


def _statistics_table(rows: list, *, range_name: str, start_value: str, end_value: str, query: str, sort_by: str, sort_dir: str) -> str:
    if not rows:
        return _empty_block("No setups matched the selected period and symbol filter.")
    rendered_rows = []
    for item in rows:
        rendered_rows.append(
            f"""
            <tr>
              <td><strong>{escape(item.symbol)}</strong></td>
              <td>{item.total}</td>
              <td>{item.success}</td>
              <td>{item.failed}</td>
              <td>{item.pending}</td>
              <td>{item.actionable}</td>
              <td>{item.watchlist}</td>
              <td>{item.win_rate:.1f}%</td>
              <td>{item.avg_confidence:.1f}</td>
            </tr>
            """
        )
    return f"""
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{_statistics_sort_link("Symbol", "symbol", range_name=range_name, start_value=start_value, end_value=end_value, query=query, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
            <th>{_statistics_sort_link("Total", "total", range_name=range_name, start_value=start_value, end_value=end_value, query=query, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
            <th>{_statistics_sort_link("Wins", "wins", range_name=range_name, start_value=start_value, end_value=end_value, query=query, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
            <th>{_statistics_sort_link("Losses", "losses", range_name=range_name, start_value=start_value, end_value=end_value, query=query, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
            <th>Pending</th>
            <th>Actionable</th>
            <th>Watchlist</th>
            <th>{_statistics_sort_link("Winrate", "winrate", range_name=range_name, start_value=start_value, end_value=end_value, query=query, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
            <th>{_statistics_sort_link("Avg confidence", "avg_confidence", range_name=range_name, start_value=start_value, end_value=end_value, query=query, current_sort_by=sort_by, current_sort_dir=sort_dir)}</th>
          </tr>
        </thead>
        <tbody>{''.join(rendered_rows)}</tbody>
      </table>
    </div>
    """


async def _statistics_export_rows(
    repository: Repository,
    inspector: MarketInspector,
    local_tz: ZoneInfo,
    start_at: datetime,
    end_at: datetime,
    symbol_query: str,
) -> list[dict[str, Any]]:
    signals = await repository.list_signals_between(start_at, end_at, signal_classes=["actionable", "watchlist"])
    query = symbol_query.strip().upper()
    rows: list[dict[str, Any]] = []
    grouped_signals = inspector._group_signals_by_setup(signals)
    for group in grouped_signals.values():
        signal = group[0]
        if query and query not in signal.symbol.upper():
            continue
        simulation = await inspector.simulate_trade(signal, end_at=end_at)
        trigger = ""
        level_zone = ""
        if signal.render_context:
            trigger = str(signal.render_context.get("trigger", ""))
            level_zone = str(signal.render_context.get("price_zone", ""))
        rows.append(
            {
                "Call date": _format_date(simulation.call_at, local_tz),
                "Call time": _format_time(simulation.call_at, local_tz),
                "Symbol": signal.symbol,
                "Venue": signal.venue.upper(),
                "Setup": signal.setup.upper(),
                "Direction": signal.direction.upper(),
                "Signal class": signal.signal_class,
                "Confidence": round(float(signal.confidence), 2),
                "Level zone": level_zone,
                "Outcome": simulation.outcome,
                "Entry price": round(float(signal.entry_price), 6),
                "TP1 price": round(float(signal.targets[0]), 6) if signal.targets else "",
                "TP2 price": round(float(signal.targets[1]), 6) if len(signal.targets) > 1 else "",
                "SL price": round(float(signal.invalidation_price), 6),
                "Trigger": trigger,
                "Rationale": "\n".join(signal.rationale or []),
                "Entry date": _format_date(simulation.entry_at, local_tz),
                "Entry time": _format_time(simulation.entry_at, local_tz),
                "TP1 date": _format_date(simulation.tp1_at, local_tz),
                "TP1 time": _format_time(simulation.tp1_at, local_tz),
                "TP2 date": _format_date(simulation.tp2_at, local_tz),
                "TP2 time": _format_time(simulation.tp2_at, local_tz),
                "SL date": _format_date(simulation.sl_at, local_tz),
                "SL time": _format_time(simulation.sl_at, local_tz),
                "Exit reason": simulation.exit_reason or "",
                "Final PnL %": "" if simulation.pnl_pct is None else round(simulation.pnl_pct, 4),
                "Trade duration": _format_duration(simulation.duration_seconds),
            }
        )
    return rows


def _statistics_workbook(*, snapshot, range_name: str, start_local: date, end_local: date, symbol_query: str, export_rows: list[dict[str, Any]]) -> BytesIO:
    summary_df = pd.DataFrame(
        [
            {"Metric": "Range", "Value": range_name},
            {"Metric": "Start date", "Value": start_local.isoformat()},
            {"Metric": "End date", "Value": (end_local - timedelta(days=1)).isoformat()},
            {"Metric": "Symbol filter", "Value": symbol_query or ""},
            {"Metric": "Total", "Value": snapshot.total},
            {"Metric": "Wins", "Value": snapshot.success},
            {"Metric": "Losses", "Value": snapshot.failed},
            {"Metric": "Pending", "Value": snapshot.pending},
            {"Metric": "Resolved", "Value": snapshot.success + snapshot.failed},
            {"Metric": "Actionable", "Value": snapshot.actionable},
            {"Metric": "Watchlist", "Value": snapshot.watchlist},
            {"Metric": "Win rate %", "Value": round(snapshot.win_rate, 2)},
            {"Metric": "Avg confidence", "Value": round(snapshot.avg_confidence, 2)},
        ]
    )
    rows_df = pd.DataFrame(
        [
            {
                "Symbol": item.symbol,
                "Total": item.total,
                "Wins": item.success,
                "Losses": item.failed,
                "Pending": item.pending,
                "Actionable": item.actionable,
                "Watchlist": item.watchlist,
                "Win rate %": round(item.win_rate, 2),
                "Avg confidence": round(item.avg_confidence, 2),
            }
            for item in snapshot.rows
        ]
    )
    signals_df = pd.DataFrame(export_rows)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        rows_df.to_excel(writer, sheet_name="by_symbol", index=False)
        signals_df.to_excel(writer, sheet_name="signals", index=False)
        for worksheet in writer.book.worksheets:
            for column in worksheet.columns:
                letter = get_column_letter(column[0].column)
                max_length = 0
                for cell in column:
                    if cell.value is not None:
                        max_length = max(max_length, len(str(cell.value)))
                    cell.alignment = Alignment(vertical="top", wrap_text=True)
                worksheet.column_dimensions[letter].width = min(max(max_length + 2, 12), 80)
    buffer.seek(0)
    return buffer


def _statistics_sort_link(
    label: str,
    sort_key: str,
    *,
    range_name: str,
    start_value: str,
    end_value: str,
    query: str,
    current_sort_by: str,
    current_sort_dir: str,
) -> str:
    next_dir = "asc" if current_sort_by != sort_key or current_sort_dir == "desc" else "desc"
    marker = ""
    if current_sort_by == sort_key:
        marker = " ▲" if current_sort_dir == "asc" else " ▼"
    href = (
        f"/statistics?range={quote_plus(range_name)}"
        f"&start={quote_plus(start_value)}"
        f"&end={quote_plus(end_value)}"
        f"&q={quote_plus(query)}"
        f"&sort_by={quote_plus(sort_key)}"
        f"&sort_dir={quote_plus(next_dir)}"
    )
    return f'<a href="{href}">{escape(label)}{marker}</a>'


def _sort_statistics_rows(rows: list, *, sort_by: str, sort_dir: str) -> list:
    reverse = sort_dir == "desc"
    mapping = {
        "symbol": lambda item: item.symbol,
        "total": lambda item: item.total,
        "wins": lambda item: item.success,
        "losses": lambda item: item.failed,
        "winrate": lambda item: item.win_rate,
        "avg_confidence": lambda item: item.avg_confidence,
    }
    key_fn = mapping.get(sort_by, mapping["total"])
    return sorted(rows, key=key_fn, reverse=reverse)


def _empty_block(text: str) -> str:
    return f'<div class="empty">{escape(text)}</div>'


def _format_dt(value: datetime, local_tz: ZoneInfo) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(local_tz).strftime("%d.%m.%Y %H:%M:%S")


def _format_date(value: datetime | None, local_tz: ZoneInfo) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(local_tz).strftime("%Y-%m-%d")


def _format_time(value: datetime | None, local_tz: ZoneInfo) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(local_tz).strftime("%H:%M:%S")


def _format_duration(duration_seconds: int | None) -> str:
    if duration_seconds is None:
        return ""
    hours, remainder = divmod(duration_seconds, 3600)
    minutes = remainder // 60
    return f"{hours}h {minutes}m"


def _format_delivery_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "Очередь пуста"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
