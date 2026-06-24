from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from src.config import get_settings
from src.models import Base


settings = get_settings()
engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_schema_compatibility()


def _ensure_schema_compatibility() -> None:
    # Backward-compatible startup migration for existing installations.
    # This avoids manual DB migration steps when optional tenant connection fields are added.
    required_tenant_columns = {
        "tripletex_base_url": "VARCHAR(255)",
        "tripletex_consumer_token": "TEXT",
        "tripletex_employee_token": "TEXT",
        "susoft_base_url": "VARCHAR(255)",
        "susoft_shop_url_key": "VARCHAR(255)",
        "susoft_username": "VARCHAR(255)",
        "susoft_password": "TEXT",
        "auto_paid_sync_enabled": "BOOLEAN DEFAULT TRUE",
        "auto_paid_sync_interval_minutes": "INTEGER DEFAULT 1",
        "daily_direct_sales_sync_enabled": "BOOLEAN DEFAULT FALSE",
        "daily_direct_sales_sync_time": "VARCHAR(5) DEFAULT '23:00'",
        "direct_sales_default_income_account": "VARCHAR(20)",
        "direct_sales_settlement_offset_account": "VARCHAR(20) DEFAULT '1900'",
    }

    with engine.begin() as conn:
        inspector = inspect(conn)
        if "tenants" not in inspector.get_table_names():
            return

        def refresh_columns() -> set[str]:
            return {col["name"] for col in inspect(conn).get_columns("tenants")}

        existing_columns = refresh_columns()
        for col_name, col_type in required_tenant_columns.items():
            if col_name in existing_columns:
                continue

            add_attempts = [
                # PostgreSQL and newer engines may support IF NOT EXISTS.
                f"ALTER TABLE tenants ADD COLUMN IF NOT EXISTS {col_name} {col_type}",
                # Generic fallback for SQLite/MySQL/PostgreSQL.
                f"ALTER TABLE tenants ADD COLUMN {col_name} {col_type}",
                # Last-resort fallback without default expression.
                f"ALTER TABLE tenants ADD COLUMN {col_name} {col_type.split(' DEFAULT ')[0]}",
            ]

            for statement in add_attempts:
                try:
                    conn.execute(text(statement))
                except Exception:
                    continue

                existing_columns = refresh_columns()
                if col_name in existing_columns:
                    break

            # If the column exists but default value was not applied, backfill nulls.
            if col_name in existing_columns:
                try:
                    if col_name == "auto_paid_sync_enabled":
                        conn.execute(text("UPDATE tenants SET auto_paid_sync_enabled = TRUE WHERE auto_paid_sync_enabled IS NULL"))
                    elif col_name == "auto_paid_sync_interval_minutes":
                        conn.execute(text("UPDATE tenants SET auto_paid_sync_interval_minutes = 1 WHERE auto_paid_sync_interval_minutes IS NULL"))
                    elif col_name == "daily_direct_sales_sync_enabled":
                        conn.execute(text("UPDATE tenants SET daily_direct_sales_sync_enabled = FALSE WHERE daily_direct_sales_sync_enabled IS NULL"))
                    elif col_name == "daily_direct_sales_sync_time":
                        conn.execute(text("UPDATE tenants SET daily_direct_sales_sync_time = '23:00' WHERE daily_direct_sales_sync_time IS NULL"))
                    elif col_name == "direct_sales_settlement_offset_account":
                        conn.execute(text("UPDATE tenants SET direct_sales_settlement_offset_account = '1900' WHERE direct_sales_settlement_offset_account IS NULL"))
                except Exception:
                    # Keep startup resilient if backfill syntax differs across DB engines.
                    pass


@contextmanager
def db_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def db_health_check() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
