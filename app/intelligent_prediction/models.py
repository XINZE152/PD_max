"""智能预测 ORM（MySQL，表名 pd_ip_*）。"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import BigInteger, Boolean, Date, DateTime, ForeignKey, Numeric, String, Text, func
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class DeliveryRecord(Base):
    """历史送货记录（Excel 导入）。"""

    __tablename__ = "pd_ip_delivery_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    regional_manager: Mapped[str] = mapped_column(String(255), index=True)
    smelter: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    warehouse: Mapped[str] = mapped_column(String(255), index=True)
    warehouse_address: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    smelter_address: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    delivery_date: Mapped[date] = mapped_column(Date, index=True)
    product_variety: Mapped[str] = mapped_column(String(255), index=True)
    weight: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    cn_is_workday: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    cn_calendar_label: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    weather_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    import_weather: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        nullable=False,
    )


class PredictionBatch(Base):
    """批次预测任务。"""

    __tablename__ = "pd_ip_prediction_batches"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    export_file_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    meta: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        nullable=False,
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    results: Mapped[list["PredictionResult"]] = relationship(
        "PredictionResult",
        back_populates="batch",
        cascade="all, delete-orphan",
    )


class PredictionResult(Base):
    """单条预测结果。"""

    __tablename__ = "pd_ip_prediction_results"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    batch_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("pd_ip_prediction_batches.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    regional_manager: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    warehouse: Mapped[str] = mapped_column(String(255), index=True)
    product_variety: Mapped[str] = mapped_column(String(255), index=True)
    smelter: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    target_date: Mapped[date] = mapped_column(Date, index=True)
    predicted_weight: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    confidence: Mapped[str] = mapped_column(String(32), default="medium")
    warnings: Mapped[Optional[list[Any]]] = mapped_column(JSON, nullable=True)
    provider_used: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    latency_ms: Mapped[Optional[float]] = mapped_column(Numeric(12, 4), nullable=True)
    cost_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    raw_response_excerpt: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        nullable=False,
    )

    batch: Mapped[Optional[PredictionBatch]] = relationship("PredictionBatch", back_populates="results")


class OperationAudit(Base):
    """送货量预测模块操作审计（何人、何时、何事）。"""

    __tablename__ = "pd_ip_operation_audit"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    user_label: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    resource: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    detail: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    client_ip: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        nullable=False,
    )
