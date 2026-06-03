"""Celery：批量预测与 Excel 导出（15 天发货预测 · 豆包方案）。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import tempfile

import pandas as pd

from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.db import get_prediction_session_factory
from app.intelligent_prediction.models import PredictionBatch
from app.intelligent_prediction.schemas.doubao_prediction import DoubaoBatchRequest
from app.intelligent_prediction.services.ai_client import get_ai_client
from app.intelligent_prediction.services.cache_manager import get_cache_manager
from app.intelligent_prediction.services.doubao_prediction_service import (
    DoubaoPredictionService,
    get_doubao_prediction_service,
)
from app.intelligent_prediction.services.doubao_prompt_builder import DoubaoPromptBuilder
from app.intelligent_prediction.tasks.celery_app import celery_app

logger = get_logger(__name__)


async def _run_batch_async(batch_id: str) -> None:
    SessionFactory = get_prediction_session_factory()
    async with SessionFactory() as session:
        batch = await session.get(PredictionBatch, batch_id)
        if batch is None:
            logger.error("prediction batch missing: %s", batch_id)
            return
        batch.status = "processing"
        batch.error_message = None
        await session.commit()
        await session.refresh(batch)
        try:
            meta = batch.meta or {}
            req = DoubaoBatchRequest.model_validate(meta)
            svc: DoubaoPredictionService = get_doubao_prediction_service(
                get_ai_client(), get_cache_manager(), DoubaoPromptBuilder()
            )
            results = await svc.predict_batch(req)
            await svc.persist_sync_results(session, results, batch_id=batch_id)
            rows: list[dict[str, object]] = []
            for pr in results:
                for it in pr.items:
                    rows.append(
                        {
                            "仓库": pr.warehouse,
                            "品类": pr.product_variety or "",
                            "目标日期": it.target_date.isoformat(),
                            "预测发货吨数": float(it.predicted_weight),
                            "发货概率": it.ship_probability,
                            "置信度": it.confidence_level,
                            "主要因素": it.main_factors,
                            "分析报告": pr.analysis_report[:500] if pr.analysis_report else "",
                        }
                    )
            df = pd.DataFrame(rows)
            tmp = Path(tempfile.gettempdir()) / f"prediction_{batch_id}.xlsx"
            df.to_excel(tmp, index=False, engine="openpyxl")
            batch.export_file_path = str(tmp)
            batch.status = "completed"
            batch.completed_at = datetime.now(timezone.utc)
            await session.commit()
        except Exception as e:
            logger.exception("prediction batch task failed batch_id=%s", batch_id)
            batch.status = "failed"
            batch.error_message = str(e)[:2000]
            batch.completed_at = datetime.now(timezone.utc)
            await session.commit()


@celery_app.task(name="intelligent_prediction.run_prediction_batch")
def run_prediction_batch_task(batch_id: str) -> str:
    asyncio.run(_run_batch_async(batch_id))
    return batch_id
