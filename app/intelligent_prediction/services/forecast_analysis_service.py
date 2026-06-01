"""预测依据文案：图表汇总、智能预测明细（与规则预测 explain_prediction 同风格）。"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.models import DeliveryRecord
from app.intelligent_prediction.schemas.forecast import PrdForecastDetailRow
from app.intelligent_prediction.schemas.prediction import PredictionItem, PredictionRequest
from app.intelligent_prediction.services.price_context_service import (
    DailyPriceContext,
    WarehousePriceProfile,
    compute_price_factor,
    estimate_warehouse_price_profile,
    explain_prediction,
    load_daily_price_context,
    resolve_own_factory_id,
)
from app.intelligent_prediction.services.prd_forecast_service import (
    _linear_wma,
    _series_positive_baseline,
    _weekday_coefs,
)
from app.intelligent_prediction.settings import settings


def explain_chart_summary(
    *,
    date_from: date,
    date_to: date,
    dates: list[date],
    total_by_date: list[Decimal],
    detail_rows: list[PrdForecastDetailRow],
) -> str:
    """根据规则预测明细与按日合计生成图表页汇总级预测依据。"""
    if not dates or not detail_rows:
        return (
            f"预测区间 {date_from.isoformat()} 至 {date_to.isoformat()} 内无可用送货历史，"
            "无法生成规则预测汇总；请调整筛选条件或先导入送货历史。"
        )

    n_rows = len(detail_rows)
    n_combos = len({(r.warehouse, r.product_variety, r.smelter or "") for r in detail_rows})
    total_pred = sum((r.predicted_weight for r in detail_rows), Decimal("0")).quantize(Decimal("0.01"))
    day_totals = [Decimal(t) for t in total_by_date]
    avg_daily = (
        (sum(day_totals, Decimal("0")) / Decimal(len(day_totals))).quantize(Decimal("0.01"))
        if day_totals
        else Decimal("0")
    )
    max_day = max(day_totals) if day_totals else Decimal("0")
    min_day = min(day_totals) if day_totals else Decimal("0")

    pf_vals = [float(r.price_factor) for r in detail_rows]
    avg_pf = sum(pf_vals) / len(pf_vals) if pf_vals else 1.0
    advantaged = sum(1 for r in detail_rows if float(r.price_factor) > 1.01)
    disadvantaged = sum(1 for r in detail_rows if float(r.price_factor) < 0.99)
    missing_own = sum(1 for r in detail_rows if r.own_calibration_price is None)

    parts: list[str] = [
        (
            f"预测区间 {date_from.isoformat()} 至 {date_to.isoformat()}，共 {len(dates)} 天、"
            f"{n_combos} 个库房×品种（冶炼厂）组合、{n_rows} 条规则明细。"
        ),
        (
            f"区间预测发货量合计约 {total_pred} 吨，按日汇总日均约 {avg_daily} 吨"
            f"（单日最低 {min_day}、最高 {max_day}）。"
        ),
        (
            f"综合权重：历史规律 {settings.prediction_history_weight:.0%}、"
            f"价格因素 {settings.prediction_price_weight:.0%}；明细平均价格乘数 {avg_pf:.4f}。"
        ),
    ]
    if advantaged > disadvantaged * 1.2:
        parts.append(
            f"约 {advantaged} 条明细价格乘数偏高，整体价格环境有利于发货，预测曲线可能偏上。"
        )
    elif disadvantaged > advantaged * 1.2:
        parts.append(
            f"约 {disadvantaged} 条明细价格乘数偏低，整体价格承压，预测曲线可能偏保守。"
        )
    else:
        parts.append("价格因素在明细间分化不大，曲线主要由历史规律与周系数驱动。")
    if missing_own:
        parts.append(f"其中 {missing_own} 条缺少己方标定价格，已按中性乘数处理。")
    return " ".join(parts)


async def _load_daily_series(
    session: AsyncSession,
    *,
    warehouse: str,
    product_variety: str,
    smelter: Optional[str],
    load_from: date,
    load_to: date,
) -> dict[date, Decimal]:
    conds = [
        DeliveryRecord.warehouse == warehouse,
        DeliveryRecord.product_variety == product_variety,
        DeliveryRecord.delivery_date >= load_from,
        DeliveryRecord.delivery_date <= load_to,
    ]
    if smelter and str(smelter).strip():
        conds.append(DeliveryRecord.smelter == str(smelter).strip())
    stmt = (
        select(DeliveryRecord.delivery_date, func.sum(DeliveryRecord.weight).label("tw"))
        .where(and_(*conds))
        .group_by(DeliveryRecord.delivery_date)
    )
    res = await session.execute(stmt)
    return {d: Decimal(tw) for d, tw in res.all()}


async def _load_warehouse_daily_totals(
    session: AsyncSession,
    *,
    warehouse: str,
    load_from: date,
    load_to: date,
) -> dict[tuple[str, date], Decimal]:
    stmt = (
        select(DeliveryRecord.delivery_date, func.sum(DeliveryRecord.weight).label("tw"))
        .where(
            and_(
                DeliveryRecord.warehouse == warehouse,
                DeliveryRecord.delivery_date >= load_from,
                DeliveryRecord.delivery_date <= load_to,
            )
        )
        .group_by(DeliveryRecord.delivery_date)
    )
    res = await session.execute(stmt)
    return {(warehouse, d): Decimal(tw) for d, tw in res.all()}


async def compute_history_baseline_for_predict(
    session: AsyncSession,
    *,
    warehouse: str,
    product_variety: str,
    smelter: Optional[str],
    target_date: date,
) -> Decimal:
    """与规则预测一致：近30日线性加权 × 仓库周规律系数。"""
    ref_end = target_date - timedelta(days=1)
    if ref_end < date(1970, 1, 1):
        return Decimal("1")
    load_from = ref_end - timedelta(days=149)
    series = await _load_daily_series(
        session,
        warehouse=warehouse,
        product_variety=product_variety,
        smelter=smelter,
        load_from=load_from,
        load_to=ref_end,
    )
    baseline_floor = _series_positive_baseline(series)
    wma = _linear_wma(series, target_date, 30)
    coef_ref_start = ref_end - timedelta(days=119)
    daily_wh = await _load_warehouse_daily_totals(
        session, warehouse=warehouse, load_from=coef_ref_start, load_to=ref_end
    )
    coefs = _weekday_coefs(daily_wh, {warehouse}, coef_ref_start, ref_end)
    c = coefs.get((warehouse, target_date.weekday()), Decimal("1"))
    history_baseline = (wma * c).quantize(Decimal("0.01"))
    if history_baseline <= 0:
        history_baseline = max(baseline_floor, Decimal("0.01")).quantize(Decimal("0.01"))
    return history_baseline


async def build_predict_item_analysis(
    session: AsyncSession,
    *,
    warehouse: str,
    product_variety: str,
    smelter: Optional[str],
    target_date: date,
    predicted_weight: Decimal,
    own_factory_id: Optional[int] = None,
    profile: Optional[WarehousePriceProfile] = None,
    ctx: Optional[DailyPriceContext] = None,
) -> str:
    """生成与规则预测同风格的单条预测依据（大模型预测重量 + 规则侧价格/历史解释）。"""
    fid = own_factory_id
    if fid is None:
        fid = await resolve_own_factory_id(session)
    if profile is None:
        profile = await estimate_warehouse_price_profile(
            session,
            warehouse=warehouse,
            product_variety=product_variety,
            own_factory_id=fid,
        )
    if ctx is None:
        ctx = await load_daily_price_context(
            session,
            as_of=target_date,
            product_variety=product_variety,
            own_factory_id=fid,
        )
    history_baseline = await compute_history_baseline_for_predict(
        session,
        warehouse=warehouse,
        product_variety=product_variety,
        smelter=smelter,
        target_date=target_date,
    )
    price_factor = compute_price_factor(ctx, profile.sensitivity)
    return explain_prediction(
        target_date=target_date,
        history_baseline=history_baseline,
        price_factor=price_factor,
        predicted=predicted_weight.quantize(Decimal("0.01")),
        profile=profile,
        ctx=ctx,
    )


async def enrich_prediction_items_with_analysis(
    session: AsyncSession,
    req: PredictionRequest,
    items: list[PredictionItem],
) -> list[PredictionItem]:
    """为智能预测每条结果附加 analysis 文案。"""
    if not items:
        return items
    smelter = req.smelter
    fid = await resolve_own_factory_id(session)
    profile = await estimate_warehouse_price_profile(
        session,
        warehouse=req.warehouse,
        product_variety=req.product_variety,
        own_factory_id=fid,
    )
    ctx_cache: dict[date, DailyPriceContext] = {}
    out: list[PredictionItem] = []
    for it in sorted(items, key=lambda x: x.target_date):
        ctx = ctx_cache.get(it.target_date)
        if ctx is None:
            ctx = await load_daily_price_context(
                session,
                as_of=it.target_date,
                product_variety=req.product_variety,
                own_factory_id=fid,
            )
            ctx_cache[it.target_date] = ctx
        analysis = await build_predict_item_analysis(
            session,
            warehouse=req.warehouse,
            product_variety=req.product_variety,
            smelter=smelter,
            target_date=it.target_date,
            predicted_weight=it.predicted_weight,
            own_factory_id=fid,
            profile=profile,
            ctx=ctx,
        )
        out.append(it.model_copy(update={"analysis": analysis}))
    return out
