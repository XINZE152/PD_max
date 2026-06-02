"""定时任务：按送货历史中的仓+品种组合批量执行综合预测（v2）并落库。"""

from __future__ import annotations

import asyncio

from sqlalchemy import select

from app.intelligent_prediction.settings import settings
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.api.audit_deps import AuditActor
from app.intelligent_prediction.db import get_prediction_session_factory
from app.intelligent_prediction.models import DeliveryRecord
from app.intelligent_prediction.schemas.prediction import (
    ComprehensiveBatchRequest,
    PredictionRequest,
)
from app.intelligent_prediction.services.ai_client import get_ai_client
from app.intelligent_prediction.services.audit_service import append_audit
from app.intelligent_prediction.services.cache_manager import get_cache_manager
from app.intelligent_prediction.services.comprehensive_prediction_service import (
    ComprehensivePredictionService,
    get_comprehensive_prediction_service,
)
from app.intelligent_prediction.services.comprehensive_prompt_builder import ComprehensivePromptBuilder

logger = get_logger(__name__)

_SCHEDULER_ACTOR = AuditActor(user_id=None, user_label="scheduler", client_ip=None)


async def _run_scheduled_comprehensive_prediction_async() -> None:
    if not settings.intelligent_prediction_schedule_enabled:
        return
    h = max(1, min(90, settings.intelligent_prediction_schedule_horizon_days))
    limit_n = max(1, min(500, settings.intelligent_prediction_schedule_max_items))

    factory = get_prediction_session_factory()
    async with factory() as session:
        stmt = (
            select(DeliveryRecord.warehouse, DeliveryRecord.product_variety)
            .distinct()
            .limit(limit_n)
        )
        res = await session.execute(stmt)
        pairs = [(r[0], r[1]) for r in res.all()]
        if not pairs:
            logger.info("scheduled comprehensive prediction: no delivery history pairs, skip")
            return

        items = [
            PredictionRequest(
                warehouse=wh,
                product_variety=variety,
                horizon_days=h,
                use_cache=True,
            )
            for wh, variety in pairs
        ]
        body = ComprehensiveBatchRequest(items=items)
        svc = get_comprehensive_prediction_service(
            get_ai_client(), get_cache_manager(), ComprehensivePromptBuilder()
        )
        results = await svc.predict_batch(body)
        await svc.persist_sync_results(session, results, batch_id=None)
        await append_audit(
            session,
            "scheduled_comprehensive_prediction",
            resource="batch",
            detail={
                "horizon_days": h,
                "pairs_requested": len(pairs),
                "results": len(results),
            },
            actor=_SCHEDULER_ACTOR,
        )
        await session.commit()
        logger.info(
            "scheduled comprehensive prediction finished horizon=%s items=%s",
            h,
            len(results),
        )


def run_scheduled_comprehensive_prediction_sync() -> None:
    """供 APScheduler 调用的同步入口（内部 asyncio.run）。"""
    if not settings.intelligent_prediction_schedule_enabled:
        return
    try:
        asyncio.run(_run_scheduled_comprehensive_prediction_async())
    except RuntimeError as e:
        if "未配置智能预测异步数据库" in str(e):
            logger.warning("scheduled comprehensive prediction skipped: %s", e)
            return
        raise
    except Exception:
        logger.exception("scheduled comprehensive prediction failed")
