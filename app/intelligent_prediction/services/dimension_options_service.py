"""送货历史 / 预测落库结果：大区经理、仓库、冶炼厂去重列表。

仓库下拉框会合并两个数据源：
1. 送货记录/预测结果中曾出现过的仓库名（pd_ip_delivery_records / pd_ip_prediction_results）
2. 库房字典中所有活跃库房（dict_warehouses，is_active=1）

两者取并集，确保即使某库房尚无送货记录，前端下拉框也能展示。
"""

from __future__ import annotations

from sqlalchemy import func, select, text
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


async def _get_active_warehouse_names(session: AsyncSession) -> list[str]:
    """从库房字典 ``dict_warehouses`` 获取所有活跃库房名称（去重排序）。"""
    stmt = text(
        "SELECT DISTINCT name FROM dict_warehouses "
        "WHERE is_active = 1 AND name IS NOT NULL AND TRIM(name) != '' "
        "ORDER BY name"
    )
    res = await session.execute(stmt)
    return [str(x) for x in res.scalars().all() if x is not None and str(x).strip()]


def _merged_sorted(existing: list[str], supplement: list[str]) -> list[str]:
    """合并两个名称列表，去重后按中文自然排序返回。"""
    merged: dict[str, None] = {}
    for name in existing:
        merged[name] = None
    for name in supplement:
        if name not in merged:
            merged[name] = None
    return sorted(merged.keys())


async def list_dimensions_from_delivery_history(session: AsyncSession) -> DimensionListsResponse:
    """送货历史维度 + 库房字典补充，确保已登记库房不出现在下拉框中。"""
    rms = await _distinct_ordered(session, DeliveryRecord.regional_manager)
    whs = await _distinct_ordered(session, DeliveryRecord.warehouse)
    sms = await _distinct_ordered(session, DeliveryRecord.smelter)

    # 合并库房字典中所有活跃库房，确保无送货记录的库房也能在下拉框中展示
    dict_whs = await _get_active_warehouse_names(session)
    whs = _merged_sorted(whs, dict_whs)

    return DimensionListsResponse(
        regional_managers=rms,
        warehouses=whs,
        smelters=sms,
    )


async def list_dimensions_from_prediction_results(session: AsyncSession) -> DimensionListsResponse:
    """预测结果维度 + 库房字典补充。

    若预测结果为空（尚未执行过 v2 预测），回退到从送货历史 ``pd_ip_delivery_records``
    读取维度，确保前端下拉框始终有候选值。
    """
    rms = await _distinct_ordered(session, PredictionResultRow.regional_manager)
    whs = await _distinct_ordered(session, PredictionResultRow.warehouse)
    sms = await _distinct_ordered(session, PredictionResultRow.smelter)

    # 回退：预测结果无数据时，使用送货历史同源维度
    if not rms and not whs and not sms:
        rms = await _distinct_ordered(session, DeliveryRecord.regional_manager)
        whs = await _distinct_ordered(session, DeliveryRecord.warehouse)
        sms = await _distinct_ordered(session, DeliveryRecord.smelter)

    # 合并库房字典中所有活跃库房
    dict_whs = await _get_active_warehouse_names(session)
    whs = _merged_sorted(whs, dict_whs)

    return DimensionListsResponse(
        regional_managers=rms,
        warehouses=whs,
        smelters=sms,
    )
