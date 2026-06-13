"""最新 manual 每日预跑批次 → 读库命中（供 POST /predict 快速返回）。"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.logging_utils import get_logger

logger = get_logger(__name__)

from app.intelligent_prediction.models import PredictionBatch
from app.intelligent_prediction.models import PredictionResult as PredictionResultRow
from app.intelligent_prediction.schemas.doubao_prediction import (
    DailyTonnageItem,
    DoubaoPredictionResult,
)

_DAILY_PREDICTION_TYPE = "manual"
_DAILY_PREDICTION_HORIZON_DAYS = 16


async def latest_daily_prediction_batch_id(session: AsyncSession) -> str | None:
    stmt = (
        select(PredictionBatch.id)
        .where(
            PredictionBatch.prediction_type == _DAILY_PREDICTION_TYPE,
            PredictionBatch.status == "completed",
        )
        .order_by(PredictionBatch.completed_at.desc(), PredictionBatch.created_at.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


def _build_result_from_db_rows(
    req,
    rows: list[PredictionResultRow],
    *,
    start: date,
    provider_used: str,
    batch_id_label: str | None = None,
) -> DoubaoPredictionResult | None:
    """把 pd_ip_prediction_results 多行拼成 DoubaoPredictionResult。"""
    if not rows:
        return None

    requested_product_variety = (req.product_variety or "").strip()
    row_varieties = { (r.product_variety or "").strip() for r in rows }
    has_exact_variety_rows = bool(
        requested_product_variety
        and requested_product_variety in row_varieties
    )

    if has_exact_variety_rows:
        rows = [r for r in rows if (r.product_variety or "").strip() == requested_product_variety]
        by_date: dict[date, PredictionResultRow] = {}
        for row in rows:
            by_date.setdefault(row.target_date, row)
        if len(by_date) < _DAILY_PREDICTION_HORIZON_DAYS:
            return None
        ordered_rows = [
            by_date[start + timedelta(days=i)] for i in range(_DAILY_PREDICTION_HORIZON_DAYS)
        ]
        items = [
            DailyTonnageItem(
                target_date=row.target_date,
                predicted_weight=row.predicted_weight,
                ship_probability=row.ship_probability or "中",
                confidence_level=row.confidence_level or row.confidence or "中",
                main_factors=row.main_factors or "",
            )
            for row in ordered_rows
        ]
        analysis_report = next(
            (
                row.comprehensive_analysis or row.analysis or ""
                for row in ordered_rows
                if row.comprehensive_analysis or row.analysis
            ),
            "",
        )
        pv_out = req.product_variety
    else:
        totals: dict[date, Decimal] = defaultdict(Decimal)
        factors_by_date: dict[date, list[str]] = defaultdict(list)
        for row in rows:
            totals[row.target_date] += Decimal(str(row.predicted_weight or 0))
            if row.main_factors:
                factors_by_date[row.target_date].append(str(row.main_factors))
        if len(totals) < _DAILY_PREDICTION_HORIZON_DAYS:
            return None
        items = []
        for i in range(_DAILY_PREDICTION_HORIZON_DAYS):
            day = start + timedelta(days=i)
            items.append(
                DailyTonnageItem(
                    target_date=day,
                    predicted_weight=totals[day],
                    ship_probability="中",
                    confidence_level="中",
                    main_factors="；".join(factors_by_date.get(day, [])[:3]),
                )
            )
        varieties = sorted({row.product_variety for row in rows if row.product_variety})
        if batch_id_label:
            analysis_report = (
                f"[AI预测缓存] 最新每日预测批次 {batch_id_label}，"
                f"按仓库汇总 {len(varieties)} 个品种。"
            )
        else:
            analysis_report = next(
                (
                    row.comprehensive_analysis or row.analysis or ""
                    for row in rows
                    if row.comprehensive_analysis or row.analysis
                ),
                "",
            )
        pv_out = None if not requested_product_variety else req.product_variety

    return DoubaoPredictionResult(
        warehouse=req.warehouse,
        product_variety=pv_out,
        analysis_report=analysis_report,
        items=items,
        provider_used=provider_used,
        latency_ms=0,
        cost_usd=None,
        cache_hit=True,
        parse_error=None,
    )


async def sync_predict_cache_result_for_request(
    session: AsyncSession,
    req,
) -> DoubaoPredictionResult | None:
    """读最近一次 POST /predict 落库（batch_id IS NULL），同仓+同预测起点+品种维度。"""
    if not getattr(req, "use_cache", True):
        return None

    start = req.prediction_start_date or date.today()
    end = start + timedelta(days=_DAILY_PREDICTION_HORIZON_DAYS - 1)
    requested_variety = (req.product_variety or "").strip()

    stmt = (
        select(PredictionResultRow)
        .where(
            PredictionResultRow.batch_id.is_(None),
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
        if len(exact) >= _DAILY_PREDICTION_HORIZON_DAYS:
            rows = exact
        else:
            rows = [r for r in rows if (r.product_variety or "").strip() == ""]

    result = _build_result_from_db_rows(
        req, rows, start=start, provider_used="sync_predict_cache"
    )
    if result is not None:
        logger.info(
            "predict_cache_hit sync_predict_cache warehouse=%s variety=%s start=%s",
            req.warehouse,
            requested_variety or "(全部)",
            start.isoformat(),
        )
    return result


async def stored_predict_cache_result_for_request(
    session: AsyncSession,
    req,
) -> DoubaoPredictionResult | None:
    """先读上次同步 predict 落库，再读 TL daily manual 批次。"""
    hit = await sync_predict_cache_result_for_request(session, req)
    if hit is not None:
        return hit
    return await daily_cache_result_for_request(session, req)


async def daily_cache_result_for_request(
    session: AsyncSession,
    req,
) -> DoubaoPredictionResult | None:
    """从最新完成的 daily(manual) 批次读取该仓库预测；命中则等同预跑结果，无需再调模型。"""
    if not getattr(req, "use_cache", True):
        return None

    batch_id = await latest_daily_prediction_batch_id(session)
    if not batch_id:
        return None

    start = req.prediction_start_date or date.today()
    end = start + timedelta(days=_DAILY_PREDICTION_HORIZON_DAYS - 1)
    stmt = (
        select(PredictionResultRow)
        .where(
            PredictionResultRow.batch_id == batch_id,
            PredictionResultRow.warehouse == req.warehouse,
            PredictionResultRow.target_date >= start,
            PredictionResultRow.target_date <= end,
        )
        .order_by(
            PredictionResultRow.target_date.asc(),
            PredictionResultRow.created_at.desc(),
            PredictionResultRow.id.desc(),
        )
    )

    async def _fetch_rows(product_variety: str | None) -> list[PredictionResultRow]:
        query = stmt
        if product_variety is not None:
            query = query.where(PredictionResultRow.product_variety == product_variety)
        result = await session.execute(query)
        return list(result.scalars().all())

    requested_product_variety = (req.product_variety or "").strip()
    has_exact_variety_rows = False
    if requested_product_variety:
        rows = await _fetch_rows(requested_product_variety)
        has_exact_variety_rows = bool(rows)
        if not rows:
            rows = await _fetch_rows("")
    else:
        rows = await _fetch_rows(None)
    if not rows:
        return None

    result = _build_result_from_db_rows(
        req,
        rows,
        start=start,
        provider_used="daily_cache",
        batch_id_label=batch_id,
    )
    if result is not None:
        logger.info(
            "predict_cache_hit daily_cache warehouse=%s variety=%s start=%s batch=%s",
            req.warehouse,
            (req.product_variety or "").strip() or "(全部)",
            start.isoformat(),
            batch_id,
        )
    return result