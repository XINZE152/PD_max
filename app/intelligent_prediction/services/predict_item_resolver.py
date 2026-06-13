"""单仓预测：读 pd_ip_prediction_results → 无则算模型 → 由 predict_sync 整仓删后写。"""

from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.schemas.doubao_prediction import (
    DoubaoHistoryItem,
    DoubaoPredictionRequest,
    DoubaoPredictionResult,
)
from app.intelligent_prediction.services.doubao_prediction_service import (
    HORIZON,
    DoubaoPredictionService,
)
from app.intelligent_prediction.services.warehouse_prediction_store import (
    load_warehouse_forecast_from_db,
)

_ZERO_REASON = "近30天无发货记录或发货量为0，未调用模型，预测未来15天发货量为0"


async def ensure_history_for_item(
    session: AsyncSession,
    svc: DoubaoPredictionService,
    item: DoubaoPredictionRequest,
) -> DoubaoPredictionRequest:
    if item.history:
        return item
    loaded = await svc._load_history_from_db(
        session, item.warehouse, item.product_variety,
    )
    if not loaded:
        return item
    return item.model_copy(update={"history": loaded})


async def resolve_one_predict_item(
    session: AsyncSession,
    svc: DoubaoPredictionService,
    item: DoubaoPredictionRequest,
) -> tuple[DoubaoPredictionResult, list[DoubaoHistoryItem] | None]:
    """执行一条预测。

    Returns:
        (result, history_for_persist):
        - 库中已有本窗口完整数据 → history_for_persist=None（不写库、不调模型）
        - 否则 → 需写库，history 用于推断大区经理/冶炼厂
    """
    item_checked = await ensure_history_for_item(session, svc, item)
    start = item.prediction_start_date or date.today()
    forecast_dates = [start + timedelta(days=i) for i in range(HORIZON)]

    stored = await load_warehouse_forecast_from_db(session, item)
    if stored is not None:
        return stored, None

    if DoubaoPredictionService._recent_shipment_tonnage(item_checked.history, start) <= 0:
        return (
            DoubaoPredictionService._zero_prediction_result(
                item, forecast_dates, reason=_ZERO_REASON,
            ),
            list(item_checked.history or []),
        )

    result = await svc.predict_single(session, item.model_copy(update={"use_cache": False}))
    hist = list(item_checked.history or [])
    if not hist:
        loaded = await svc._load_history_from_db(
            session, item.warehouse, item.product_variety,
        )
        if loaded:
            hist = loaded
    return result, hist