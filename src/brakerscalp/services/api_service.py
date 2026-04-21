from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from brakerscalp.config import Settings
from brakerscalp.storage.repository import Repository


def build_api(repository: Repository, settings: Settings) -> FastAPI:
    app = FastAPI(title="BrakerScalp API")

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
        items = await repository.list_latest_health(limit=50)
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
        return JSONResponse(
            {
                "app_name": settings.app_name,
                "environment": settings.environment,
                "timezone": settings.timezone,
                "enabled_venues": settings.enabled_venues,
                "allowed_chat_ids": settings.allowed_chat_ids,
                "alert_chat_ids": settings.effective_alert_chat_ids,
                "poll_interval_seconds": settings.poll_interval_seconds,
                "engine_interval_seconds": settings.engine_interval_seconds,
                "exchange_book_depth": settings.exchange_book_depth,
                "exchange_trades_limit": settings.exchange_trades_limit,
                "universe_path": str(settings.universe_path),
            }
        )

    return app
