"""定时任务：按送货历史中的仓+品种组合批量执行15天发货预测并落库。"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from app.intelligent_prediction.settings import settings
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.api.audit_deps import AuditActor
from app.intelligent_prediction.db import get_prediction_session_factory
from app.intelligent_prediction.models import DeliveryRecord, LeadMarketPrice
from app.intelligent_prediction.schemas.doubao_prediction import (
    DoubaoBatchRequest,
    DoubaoHistoryItem,
    DoubaoPredictionRequest,
    SMMPricingItem,
)
from app.intelligent_prediction.services.ai_client import get_ai_client
from app.intelligent_prediction.services.audit_service import append_audit
from app.intelligent_prediction.services.cache_manager import get_cache_manager
from app.intelligent_prediction.services.doubao_prediction_service import (
    DoubaoPredictionService,
    get_doubao_prediction_service,
)
from app.intelligent_prediction.services.doubao_prompt_builder import DoubaoPromptBuilder

logger = get_logger(__name__)

_SCHEDULER_ACTOR = AuditActor(user_id=None, user_label="scheduler", client_ip=None)

# 定时预测固定使用 16 天（day0~day15）
_SCHEDULED_HORIZON = 16


async def _load_history_for_pair(
    session: Any,
    warehouse: str,
    product_variety: str,
    lookback_days: int = 180,
) -> list[DoubaoHistoryItem]:
    """从 DB 加载指定仓+品种的历史送货记录，转为 DoubaoHistoryItem 格式。"""
    cutoff = date.today() - timedelta(days=lookback_days)
    stmt = (
        select(DeliveryRecord)
        .where(DeliveryRecord.warehouse == warehouse)
        .where(DeliveryRecord.product_variety == product_variety)
        .where(DeliveryRecord.delivery_date >= cutoff)
        .order_by(DeliveryRecord.delivery_date)
    )
    res = await session.execute(stmt)
    records = res.scalars().all()

    items: list[DoubaoHistoryItem] = []
    for r in records:
        # 获取天气信息
        weather = r.import_weather
        if not weather and r.weather_json:
            weather = r.weather_json.get("text") or r.weather_json.get("description")
        items.append(
            DoubaoHistoryItem(
                送货日期=r.delivery_date,
                大区经理=r.regional_manager,
                冶炼厂=r.smelter,
                仓库=r.warehouse,
                品类=r.product_variety,
                天气=weather,
                重量吨=r.weight,
            )
        )
    return items


async def _load_smm_prices(session: Any, lookback_days: int = 60) -> list[SMMPricingItem]:
    """从 pd_ip_lead_market_prices 加载近期 SMM 铅价。"""
    cutoff = date.today() - timedelta(days=lookback_days)
    stmt = (
        select(LeadMarketPrice)
        .where(LeadMarketPrice.price_date >= cutoff)
        .order_by(LeadMarketPrice.price_date)
    )
    res = await session.execute(stmt)
    records = res.scalars().all()

    items: list[SMMPricingItem] = []
    for r in records:
        # LeadMarketPrice 只有 lead_price 字段，用作均价
        # 最低价/最高价用均价±50 估算（实际应从 SMM 表获取）
        lead = r.lead_price
        items.append(
            SMMPricingItem(
                定价日期=r.price_date,
                最低价=lead - Decimal("50"),
                最高价=lead + Decimal("50"),
                均价=lead,
            )
        )
    return items


async def _run_scheduled_prediction_async() -> None:
    if not settings.intelligent_prediction_schedule_enabled:
        return
    limit_n = max(1, min(500, settings.intelligent_prediction_schedule_max_items))

    factory = get_prediction_session_factory()
    async with factory() as session:
        # 查询所有仓+品种组合
        stmt = (
            select(DeliveryRecord.warehouse, DeliveryRecord.product_variety)
            .distinct()
            .limit(limit_n)
        )
        res = await session.execute(stmt)
        pairs = [(r[0], r[1]) for r in res.all()]
        if not pairs:
            logger.info("scheduled prediction: no delivery history pairs, skip")
            return

        # 加载 SMM 铅价（全局共享）
        smm_prices = await _load_smm_prices(session)

        # 为每个仓+品种构建 DoubaoPredictionRequest
        items: list[DoubaoPredictionRequest] = []
        for warehouse, variety in pairs:
            history = await _load_history_for_pair(session, warehouse, variety)
            if not history:
                logger.info("scheduled prediction: no history for %s × %s, skip", warehouse, variety)
                continue
            items.append(
                DoubaoPredictionRequest(
                    warehouse=warehouse,
                    product_variety=variety,
                    history=history,
                    smm_prices=smm_prices,
                    use_cache=True,
                )
            )

        if not items:
            logger.info("scheduled prediction: no valid requests, skip")
            return

        body = DoubaoBatchRequest(items=items)
        svc: DoubaoPredictionService = get_doubao_prediction_service(
            get_ai_client(), get_cache_manager(), DoubaoPromptBuilder()
        )
        results = await svc.predict_batch(body)
        await svc.persist_sync_results(session, results, batch_id=None)
        await append_audit(
            session,
            "scheduled_prediction",
            resource="batch",
            detail={
                "pairs_requested": len(pairs),
                "requests_built": len(items),
                "results": len(results),
            },
            actor=_SCHEDULER_ACTOR,
        )
        await session.commit()
        logger.info(
            "scheduled prediction finished items=%s",
            len(results),
        )


def run_scheduled_comprehensive_prediction_sync() -> None:
    """供 APScheduler 调用的同步入口（保持函数名不变以兼容现有注册）。"""
    if not settings.intelligent_prediction_schedule_enabled:
        return
    try:
        asyncio.run(_run_scheduled_prediction_async())
    except RuntimeError as e:
        if "未配置智能预测异步数据库" in str(e):
            logger.warning("scheduled prediction skipped: %s", e)
            return
        raise
    except Exception:
        logger.exception("scheduled prediction failed")
