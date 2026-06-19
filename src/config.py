from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "tt-susoft-sync"
    app_env: str = "dev"
    app_auto_create_tables: bool = True

    dashboard_username: str = "admin"
    dashboard_password: str = "change-me"

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/tt_susoft"

    tripletex_base_url: str = "https://tripletex.no/v2"
    tripletex_consumer_token: str = ""
    tripletex_employee_token: str = ""
    tripletex_timezone: str = "Europe/Oslo"

    sync_default_limit: int = 50

    susoft_base_url: str = "https://api.susoft.com:4443"
    susoft_shop_url_key: str = ""
    susoft_username: str = ""
    susoft_password: str = ""

    request_timeout_seconds: int = 30
    webhook_shared_secret: str = ""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
