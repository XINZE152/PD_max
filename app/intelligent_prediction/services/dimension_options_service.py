"""送货历史 / 预测落库结果：大区经理、仓库、冶炼厂去重列表。"""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.models import DeliveryRecord
from app.intelligent_prediction.models import PredictionResult as PredictionResultRow
from app.intelligent_prediction.schemas.dimensions import DimensionListsResponse


def _non_empty_str(col):
    return col.isnot(None) & (func.trim(col) != "")


async def _distinct_ordered(session: AsyncSession, col) -> list[str]:
    stmt = (
        select(col)
        .where(_non_empty_str(col))
        .distinct()
        .order_by(col)
    )
    res = await session.execute(stmt)
    return [str(x) for x in res.scalars().all() if x is not None and str(x).strip()]


async def list_dimensions_from_delivery_history(session: AsyncSession) -> DimensionListsResponse:
    """数据来自 ``pd_ip_delivery_records``（送货历史；PRD 规则预测筛选同源）。"""
    rms = await _distinct_ordered(session, DeliveryRecord.regional_manager)
    whs = await _distinct_ordered(session, DeliveryRecord.warehouse)
    sms = await _distinct_ordered(session, DeliveryRecord.smelter)
    return DimensionListsResponse(
        regional_managers=rms,
        warehouses=whs,
        smelters=sms,
    )


async def list_dimensions_from_prediction_results(session: AsyncSession) -> DimensionListsResponse:
    """数据来自 ``pd_ip_prediction_results``（已落库的智能预测明细）。"""
    rms = await _distinct_ordered(session, PredictionResultRow.regional_manager)
    whs = await _distinct_ordered(session, PredictionResultRow.warehouse)
    sms = await _distinct_ordered(session, PredictionResultRow.smelter)
    return DimensionListsResponse(
        regional_managers=rms,
        warehouses=whs,
        smelters=sms,
    )
