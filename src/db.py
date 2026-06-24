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
    }

    with engine.begin() as conn:
        inspector = inspect(conn)
        if "tenants" not in inspector.get_table_names():
            return

        existing_columns = {col["name"] for col in inspector.get_columns("tenants")}
        for col_name, col_type in required_tenant_columns.items():
            if col_name in existing_columns:
                continue
            try:
                conn.execute(text(f"ALTER TABLE tenants ADD COLUMN {col_name} {col_type}"))
            except Exception:
                # Keep startup resilient across database engines and old SQLite versions.
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
