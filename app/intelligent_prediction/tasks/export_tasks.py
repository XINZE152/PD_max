"""Celery：批量预测与 Excel 导出。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import tempfile

import pandas as pd

from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.db import get_prediction_session_factory
from app.intelligent_prediction.models import PredictionBatch
from app.intelligent_prediction.schemas.prediction import BatchPredictionRequest
from app.intelligent_prediction.services.ai_client import get_ai_client
from app.intelligent_prediction.services.cache_manager import get_cache_manager
from app.intelligent_prediction.services.prediction_service import PredictionService
from app.intelligent_prediction.services.prompt_builder import PromptBuilder
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
            req = BatchPredictionRequest.model_validate(meta)
            svc = PredictionService(get_ai_client(), get_cache_manager(), PromptBuilder())
            results = await svc.predict_batch(req)
            await svc.persist_sync_results(session, results, batch_id=batch_id)
            rows: list[dict[str, object]] = []
            for pr in results:
                for it in pr.items:
                    rows.append(
                        {
                            "warehouse": pr.warehouse,
                            "product_variety": pr.product_variety,
                            "smelter": pr.smelter or "",
                            "regional_manager": pr.regional_manager or "",
                            "target_date": it.target_date.isoformat(),
                            "predicted_weight": float(it.predicted_weight),
                            "confidence": str(it.confidence),
                            "warnings": ";".join(it.warnings),
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
