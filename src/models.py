from __future__ import annotations

from datetime import date
from datetime import UTC, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_key: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    tripletex_base_url: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tripletex_consumer_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    tripletex_employee_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    susoft_base_url: Mapped[str | None] = mapped_column(String(255), nullable=True)
    susoft_shop_url_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    susoft_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    susoft_password: Mapped[str | None] = mapped_column(Text, nullable=True)
    auto_paid_sync_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    auto_paid_sync_interval_minutes: Mapped[int] = mapped_column(Integer, default=1)
    daily_direct_sales_sync_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    daily_direct_sales_sync_time: Mapped[str] = mapped_column(String(5), default="23:00")
    direct_sales_default_income_account: Mapped[str | None] = mapped_column(String(20), nullable=True)
    direct_sales_settlement_offset_account: Mapped[str | None] = mapped_column(String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id"), index=True)
    job_name: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(40), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)


class OrderSync(Base):
    __tablename__ = "order_sync"
    __table_args__ = (UniqueConstraint("tenant_id", "tripletex_order_id", name="uq_order_sync_tenant_tt_order"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id"), index=True)
    tripletex_order_id: Mapped[str] = mapped_column(String(80), index=True)
    status: Mapped[str] = mapped_column(String(50), index=True)
    susoft_uuid: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class SyncEvent(Base):
    __tablename__ = "sync_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id"), index=True)
    job_run_id: Mapped[int | None] = mapped_column(ForeignKey("job_runs.id"), nullable=True, index=True)
    order_sync_id: Mapped[int | None] = mapped_column(ForeignKey("order_sync.id"), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(80), index=True)
    level: Mapped[str] = mapped_column(String(20), default="INFO", index=True)
    message: Mapped[str] = mapped_column(Text)
    details_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)


class ArticleIncomeMapping(Base):
    __tablename__ = "article_income_mapping"
    __table_args__ = (UniqueConstraint("tenant_id", "susoft_product_id", name="uq_article_income_tenant_product"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id"), index=True)
    susoft_product_id: Mapped[str] = mapped_column(String(120), index=True)
    susoft_product_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tripletex_product_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    income_account: Mapped[str | None] = mapped_column(String(20), nullable=True)
    source: Mapped[str] = mapped_column(String(20), default="AUTO")
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class DirectSalesSettlementRun(Base):
    __tablename__ = "direct_sales_settlement_runs"
    __table_args__ = (UniqueConstraint("tenant_id", "settlement_date", name="uq_direct_sales_settlement_tenant_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id"), index=True)
    settlement_date: Mapped[date] = mapped_column(Date, index=True)
    status: Mapped[str] = mapped_column(String(40), index=True)
    direct_sales_gross: Mapped[float] = mapped_column(Float, default=0.0)
    tt_linked_gross: Mapped[float] = mapped_column(Float, default=0.0)
    net_transfer_gross: Mapped[float] = mapped_column(Float, default=0.0)
    lines_count: Mapped[int] = mapped_column(Integer, default=0)
    posted_voucher_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    details_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
