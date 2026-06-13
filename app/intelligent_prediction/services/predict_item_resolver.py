"""单仓预测解析：与 POST /predict 单条 item 逻辑一致（30 天规则 → daily 库缓存 → 调模型）。"""

from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.schemas.doubao_prediction import (
    DoubaoHistoryItem,
    DoubaoPredictionRequest,
    DoubaoPredictionResult,
)
from app.intelligent_prediction.services.daily_prediction_cache import (
    daily_cache_result_for_request,
)
from app.intelligent_prediction.services.doubao_prediction_service import (
    HORIZON,
    DoubaoPredictionService,
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
    *,
    allow_daily_db_cache: bool = True,
) -> tuple[DoubaoPredictionResult, list[DoubaoHistoryItem] | None]:
    """执行一条预测请求。

    Returns:
        (result, history_for_persist): daily 库命中时 history_for_persist 为 None（已落库）；
        模型/全零短路时返回用于 persist 的 history 列表（可能为空）。
    """
    item_checked = await ensure_history_for_item(session, svc, item)
    start = item.prediction_start_date or date.today()
    forecast_dates = [start + timedelta(days=i) for i in range(HORIZON)]

    if DoubaoPredictionService._recent_shipment_tonnage(item_checked.history, start) <= 0:
        return (
            DoubaoPredictionService._zero_prediction_result(
                item, forecast_dates, reason=_ZERO_REASON,
            ),
            list(item_checked.history or []),
        )

    if allow_daily_db_cache:
        cached = await daily_cache_result_for_request(session, item)
        if cached is not None:
            return cached, None

    result = await svc.predict_single(session, item.model_copy(update={"use_cache": False}))
    hist = list(item_checked.history or [])
    if not hist:
        loaded = await svc._load_history_from_db(
            session, item.warehouse, item.product_variety,
        )
        if loaded:
            hist = loaded
    return result, hist