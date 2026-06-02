"""综合预测 v2 HTTP 接口。"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.exceptions import BusinessException, INTERNAL_SERVER_ERROR_MESSAGE
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.api.deps import (
    get_prediction_db_session,
    get_comprehensive_prediction_service_dep,
)
from app.intelligent_prediction.models import PredictionBatch
from app.intelligent_prediction.schemas.prediction import (
    ComprehensiveBatchRequest,
    ComprehensivePredictionResult,
)
from app.intelligent_prediction.services.comprehensive_prediction_service import (
    ComprehensivePredictionService,
)

logger = get_logger(__name__)
router = APIRouter()


@router.post(
    "/comprehensive",
    response_model=list[ComprehensivePredictionResult],
    summary="综合预测（v2）",
    description=(
        "基于历史发货规律（40%）、价格竞争力（30%）、价格敏感度（15%）、"
        "节假日（10%）、天气物流（5%）六大维度进行综合分析，"
        "输出结构化中文分析报告 + 发货概率/预计时间/预计量/置信度。"
    ),
)
async def predict_comprehensive(
    body: ComprehensiveBatchRequest,
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: ComprehensivePredictionService = Depends(get_comprehensive_prediction_service_dep),
) -> list[ComprehensivePredictionResult]:
    """同步批量综合预测并写库。"""
    try:
        results = await svc.predict_batch(body)
        await svc.persist_sync_results(session, results, batch_id=None)
        return results
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("predict_comprehensive failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e
