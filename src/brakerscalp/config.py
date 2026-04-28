from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    app_name: str = "brakerscalp"
    environment: str = "dev"
    log_level: str = "INFO"
    timezone: str = "UTC"
    bot_token: str = "replace-me"
    bot_parse_mode: str = "HTML"
    bot_disable_link_preview: bool = True
    bot_polling_timeout_seconds: int = 30
    bot_startup_notifications: bool = True
    bot_shutdown_notifications: bool = False
    allowed_chat_ids: list[int] = Field(default_factory=list)
    alert_chat_ids: list[int] = Field(default_factory=list)
    alert_message_thread_id: int = 475
    database_url: str = ""
    redis_url: str = ""
    postgres_db: str = "brakerscalp"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = ""
    poll_interval_seconds: int = 30
    collector_symbol_concurrency: int = 6
    engine_interval_seconds: int = 20
    order_flow_interval_seconds: int = 5
    signal_duplicate_window_minutes: int = 180
    minimum_alert_confidence: float = 65.0
    risk_usdt: float = 25.0
    strategy_timeframe: str = "5m"
    strategy_minimum_expected_rr: float = 2.0
    strategy_actionable_confidence_threshold: float = 88.0
    strategy_watchlist_confidence_threshold: float = 82.0
    strategy_volume_z_threshold: float = 1.80
    strategy_watchlist_volume_z_threshold: float = 1.05
    strategy_min_touches: int = 3
    strategy_squeeze_threshold: float = 0.72
    strategy_dist_to_level_atr: float = 0.35
    strategy_breakout_distance_atr: float = 0.18
    strategy_body_ratio_threshold: float = 0.58
    strategy_close_to_extreme_threshold: float = 0.22
    strategy_range_expansion_threshold: float = 1.25
    strategy_sl_multiplier: float = 0.22
    strategy_delta_ratio_threshold: float = 0.12
    strategy_watchlist_delta_ratio_threshold: float = 0.04
    strategy_cvd_slope_threshold: float = 0.06
    strategy_delta_divergence_threshold: float = 0.08
    strategy_enable_btc_eth_correlation_filter: bool = True
    strategy_btc_correlation_threshold: float = 0.45
    strategy_enable_liquidation_levels: bool = True
    strategy_enable_round_number_levels: bool = True
    strategy_enable_tick_velocity_alerts: bool = True
    strategy_tick_velocity_alert_multiplier: float = 1.8
    strategy_enable_time_stop_alerts: bool = True
    strategy_time_stop_minutes: int = 3
    strategy_time_stop_min_move_pct: float = 1.0
    strategy_enable_dynamic_breakeven_alerts: bool = True
    strategy_breakeven_trigger_pct: float = 0.5
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    exchange_request_timeout_seconds: float = 10.0
    exchange_book_depth: int = 10
    exchange_trades_limit: int = 50
    enable_binance: bool = True
    enable_bybit: bool = True
    enable_okx: bool = True
    healthcheck_symbol: str = "BTCUSDT"
    signal_dedupe_ttl_seconds: int = 14400
    universe_path: Path = Path("config/universe.json")
    enable_grafana: bool = True
    enable_prometheus: bool = True
    grafana_admin_user: str = "admin"
    grafana_admin_password: str = "admin"
    sentry_dsn: str = ""

    @field_validator("allowed_chat_ids", mode="before")
    @classmethod
    def parse_allowed_chat_ids(cls, value):
        return cls._parse_int_list(value)

    @field_validator("alert_chat_ids", mode="before")
    @classmethod
    def parse_alert_chat_ids(cls, value):
        return cls._parse_int_list(value)

    @classmethod
    def _parse_int_list(cls, value):
        if isinstance(value, list):
            return value
        if value in (None, ""):
            return []
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.startswith("["):
                import json

                return json.loads(cleaned)
            return [int(item.strip()) for item in cleaned.split(",") if item.strip()]
        return value

    @property
    def is_dev(self) -> bool:
        return self.environment.lower() in {"dev", "local", "test"}

    @model_validator(mode="after")
    def build_urls(self) -> "Settings":
        if not self.database_url:
            self.database_url = (
                f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )
        if not self.redis_url:
            auth = f":{self.redis_password}@" if self.redis_password else ""
            self.redis_url = f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return self

    @property
    def effective_alert_chat_ids(self) -> list[int]:
        return self.alert_chat_ids or self.allowed_chat_ids

    @property
    def enabled_venues(self) -> list[str]:
        venues: list[str] = []
        if self.enable_binance:
            venues.append("binance")
        if self.enable_bybit:
            venues.append("bybit")
        if self.enable_okx:
            venues.append("okx")
        return venues

    def default_strategy_config(self) -> dict[str, object]:
        return {
            "timeframe": self.strategy_timeframe,
            "minimum_expected_rr": self.strategy_minimum_expected_rr,
            "actionable_confidence_threshold": self.strategy_actionable_confidence_threshold,
            "watchlist_confidence_threshold": self.strategy_watchlist_confidence_threshold,
            "volume_z_threshold": self.strategy_volume_z_threshold,
            "watchlist_volume_z_threshold": self.strategy_watchlist_volume_z_threshold,
            "min_touches": self.strategy_min_touches,
            "squeeze_threshold": self.strategy_squeeze_threshold,
            "dist_to_level_atr": self.strategy_dist_to_level_atr,
            "breakout_distance_atr": self.strategy_breakout_distance_atr,
            "body_ratio_threshold": self.strategy_body_ratio_threshold,
            "close_to_extreme_threshold": self.strategy_close_to_extreme_threshold,
            "range_expansion_threshold": self.strategy_range_expansion_threshold,
            "sl_multiplier": self.strategy_sl_multiplier,
            "delta_ratio_threshold": self.strategy_delta_ratio_threshold,
            "watchlist_delta_ratio_threshold": self.strategy_watchlist_delta_ratio_threshold,
            "cvd_slope_threshold": self.strategy_cvd_slope_threshold,
            "delta_divergence_threshold": self.strategy_delta_divergence_threshold,
            "enable_btc_eth_correlation_filter": self.strategy_enable_btc_eth_correlation_filter,
            "btc_correlation_threshold": self.strategy_btc_correlation_threshold,
            "enable_liquidation_levels": self.strategy_enable_liquidation_levels,
            "enable_round_number_levels": self.strategy_enable_round_number_levels,
            "enable_tick_velocity_alerts": self.strategy_enable_tick_velocity_alerts,
            "tick_velocity_alert_multiplier": self.strategy_tick_velocity_alert_multiplier,
            "enable_time_stop_alerts": self.strategy_enable_time_stop_alerts,
            "time_stop_minutes": self.strategy_time_stop_minutes,
            "time_stop_min_move_pct": self.strategy_time_stop_min_move_pct,
            "enable_dynamic_breakeven_alerts": self.strategy_enable_dynamic_breakeven_alerts,
            "breakeven_trigger_pct": self.strategy_breakeven_trigger_pct,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
