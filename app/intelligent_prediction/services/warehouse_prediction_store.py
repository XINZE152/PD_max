"""POST /predict 与 pd_ip_prediction_results：按仓库+预测日读库，未命中则整仓替换写入。"""

from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.models import PredictionResult as PredictionResultRow
from app.intelligent_prediction.schemas.doubao_prediction import DoubaoPredictionResult
from app.intelligent_prediction.services.daily_prediction_cache import (
    _DAILY_PREDICTION_HORIZON_DAYS,
    _build_result_from_db_rows,
)

logger = get_logger(__name__)


def forecast_window(start: date) -> tuple[date, date]:
    end = start + timedelta(days=_DAILY_PREDICTION_HORIZON_DAYS - 1)
    return start, end


async def load_warehouse_forecast_from_db(
    session: AsyncSession,
    req,
) -> DoubaoPredictionResult | None:
    """若表中已有该仓库、本预测窗口内完整 16 天 target_date，则直接组装返回。"""
    if not getattr(req, "use_cache", True):
        return None

    start, end = forecast_window(req.prediction_start_date or date.today())
    requested_variety = (req.product_variety or "").strip()

    stmt = (
        select(PredictionResultRow)
        .where(
            PredictionResultRow.warehouse == req.warehouse,
            PredictionResultRow.target_date >= start,
            PredictionResultRow.target_date <= end,
        )
        .order_by(
            PredictionResultRow.created_at.desc(),
            PredictionResultRow.target_date.asc(),
            PredictionResultRow.id.desc(),
        )
    )
    res = await session.execute(stmt)
    all_rows = list(res.scalars().all())
    if not all_rows:
        return None

    latest_created = all_rows[0].created_at
    rows = [r for r in all_rows if r.created_at == latest_created]

    if requested_variety:
        exact = [r for r in rows if (r.product_variety or "").strip() == requested_variety]
        if len({r.target_date for r in exact}) >= _DAILY_PREDICTION_HORIZON_DAYS:
            rows = exact
        else:
            rows = [r for r in rows if (r.product_variety or "").strip() == ""]

    result = _build_result_from_db_rows(
        req,
        rows,
        start=start,
        provider_used="stored_db_cache",
    )
    if result is not None:
        logger.info(
            "predict_cache_hit stored_db warehouse=%s variety=%s window=%s..%s",
            req.warehouse,
            requested_variety or "(全部)",
            start.isoformat(),
            end.isoformat(),
        )
    return result


async def delete_all_prediction_rows_for_warehouse(
    session: AsyncSession,
    warehouse: str,
) -> int:
    """删除该仓库在 pd_ip_prediction_results 中的全部记录（任意 batch_id）。"""
    wh = warehouse.strip()
    if not wh:
        return 0
    stmt = delete(PredictionResultRow).where(PredictionResultRow.warehouse == wh)
    res = await session.execute(stmt)
    deleted = int(res.rowcount or 0)
    if deleted:
        logger.info("predict_replace_warehouse deleted_rows=%d warehouse=%s", deleted, wh)
    return deleted