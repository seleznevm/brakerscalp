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
    engine_interval_seconds: int = 20
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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
