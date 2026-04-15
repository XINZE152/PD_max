"""Celery 应用（智能预测异步导出）。"""

from __future__ import annotations

from celery import Celery

from app.intelligent_prediction.settings import settings

celery_app = Celery(
    "pd_intelligent_prediction",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)

import app.intelligent_prediction.tasks.export_tasks  # noqa: E402,F401
