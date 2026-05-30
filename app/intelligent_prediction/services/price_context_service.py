"""预测用价格上下文：铅价/行情、己方标定、竞品报价、库房价格敏感度。"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Literal, Optional

from sqlalchemy import and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.models import DeliveryRecord
from app.intelligent_prediction.services.lead_price_service import get_latest_lead_price
from app.intelligent_prediction.settings import settings

PriceSensitivity = Literal["sensitive", "medium", "stable"]

_OWN_SMELTER_NAMES: tuple[str, ...] = (
    "河南金利金铅集团有限公司",
    "金利",
)


@dataclass(frozen=True)
class DailyPriceContext:
    """某日三类价格快照。"""

    as_of_date: date
    lead_market_price: Optional[Decimal]
    own_calibration_price: Optional[Decimal]
    competitor_price_max: Optional[Decimal]
    competitor_price_avg: Optional[Decimal]

    @property
    def vs_market_ratio(self) -> Optional[float]:
        """己方相对行情的比例偏离：(own-market)/market。"""
        if self.own_calibration_price is None or self.lead_market_price is None:
            return None
        m = float(self.lead_market_price)
        if m <= 0:
            return None
        return (float(self.own_calibration_price) - m) / m

    @property
    def vs_competitor_ratio(self) -> Optional[float]:
        """己方相对竞品最高价的偏离比例。"""
        if self.own_calibration_price is None or self.competitor_price_max is None:
            return None
        c = float(self.competitor_price_max)
        if c <= 0:
            return None
        return (float(self.own_calibration_price) - c) / c


@dataclass(frozen=True)
class WarehousePriceProfile:
    warehouse: str
    sensitivity: PriceSensitivity
    correlation: Optional[float]
    capacity_max: Decimal
    capacity_min: Decimal
    capacity_avg: Decimal


def _quote_unit_sql() -> str:
    return "COALESCE(qd.price_3pct_vat, qd.unit_price, qd.price_13pct_vat)"


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def compute_price_factor(
    ctx: DailyPriceContext,
    sensitivity: PriceSensitivity,
) -> Decimal:
    """根据相对价格优势计算乘数（1.0=中性；>1 价格有利；<1 价格不利）。"""
    raw = 0.0
    n = 0
    if ctx.vs_competitor_ratio is not None:
        raw += 0.6 * ctx.vs_competitor_ratio
        n += 1
    if ctx.vs_market_ratio is not None:
        raw += 0.4 * ctx.vs_market_ratio
        n += 1
    if n == 0:
        return Decimal("1")
    raw = _clamp(raw, -0.35, 0.35)
    factor = 1.0 + raw
    if sensitivity == "stable":
        factor = 1.0 + (factor - 1.0) * 0.35
    elif sensitivity == "medium":
        factor = 1.0 + (factor - 1.0) * 0.65
    return Decimal(str(round(factor, 4)))


def blend_history_and_price(
    history_baseline: Decimal,
    price_factor: Decimal,
) -> Decimal:
    """历史规律 ~20% + 价格因素 ~80%。"""
    hw = settings.prediction_history_weight
    pw = settings.prediction_price_weight
    total = hw + pw
    if total <= 0:
        return history_baseline
    hw, pw = hw / total, pw / total
    blended = float(history_baseline) * (hw + pw * float(price_factor))
    return Decimal(str(round(blended, 4)))


def explain_prediction(
    *,
    target_date: date,
    history_baseline: Decimal,
    price_factor: Decimal,
    predicted: Decimal,
    profile: WarehousePriceProfile,
    ctx: DailyPriceContext,
) -> str:
    """生成可读的预测解释文案。"""
    parts: list[str] = []
    parts.append(
        f"{target_date.isoformat()} 历史基线 {history_baseline}（近30日加权×周规律），"
        f"价格乘数 {price_factor}（历史权重 {settings.prediction_history_weight:.0%}、"
        f"价格权重 {settings.prediction_price_weight:.0%}），预测 {predicted}。"
    )
    sens_label = {"sensitive": "敏感型", "medium": "中等", "stable": "稳定型"}.get(
        profile.sensitivity, profile.sensitivity
    )
    parts.append(
        f"库房能力：最高 {profile.capacity_max}、最低 {profile.capacity_min}、"
        f"平均 {profile.capacity_avg}；价格敏感度 {sens_label}。"
    )
    if ctx.own_calibration_price is not None:
        adv_bits: list[str] = []
        if ctx.lead_market_price is not None:
            adv_bits.append(f"行情 {ctx.lead_market_price}")
        if ctx.competitor_price_max is not None:
            adv_bits.append(f"竞品最高 {ctx.competitor_price_max}")
        adv_bits.append(f"己方 {ctx.own_calibration_price}")
        parts.append("当日价格：" + "，".join(adv_bits) + "。")
        if ctx.vs_competitor_ratio is not None and ctx.vs_competitor_ratio < -0.02:
            parts.append("相对竞品价格偏低，预计发货意愿减弱。")
        elif ctx.vs_competitor_ratio is not None and ctx.vs_competitor_ratio > 0.02:
            parts.append("相对竞品有价格优势，预计发货意愿增强。")
        elif ctx.vs_market_ratio is not None and abs(ctx.vs_market_ratio) <= 0.02:
            parts.append("与行情基本持平。")
    else:
        parts.append("缺少己方标定价格，价格因素按中性处理。")
    return " ".join(parts)


async def resolve_own_factory_id(session: AsyncSession) -> Optional[int]:
    for name in _OWN_SMELTER_NAMES:
        res = await session.execute(
            text(
                "SELECT id FROM dict_factories WHERE TRIM(name) = :n "
                "ORDER BY is_active DESC, id ASC LIMIT 1"
            ),
            {"n": name},
        )
        row = res.first()
        if row:
            return int(row[0])
    return None


async def load_daily_price_context(
    session: AsyncSession,
    *,
    as_of: date,
    product_variety: str,
    own_factory_id: Optional[int] = None,
) -> DailyPriceContext:
    """加载指定日（或之前最近有效日）的三类价格。"""
    fid = own_factory_id if own_factory_id is not None else await resolve_own_factory_id(session)
    lead = await get_latest_lead_price(session, as_of)

    own_price: Optional[Decimal] = None
    if fid is not None:
        res = await session.execute(
            text(
                "SELECT calibration_price FROM pd_smelter_calibration_prices "
                "WHERE factory_id = :fid AND price_date <= :d "
                "ORDER BY price_date DESC, id DESC LIMIT 1"
            ),
            {"fid": fid, "d": as_of},
        )
        row = res.first()
        if row and row[0] is not None:
            own_price = Decimal(str(row[0]))

    comp_max: Optional[Decimal] = None
    comp_avg: Optional[Decimal] = None
    variety = product_variety.strip()
    if variety:
        params: dict[str, object] = {"d": as_of, "v": variety}
        exclude = ""
        if fid is not None:
            exclude = "AND qd.factory_id <> :own_fid"
            params["own_fid"] = fid
        sql = f"""
            SELECT MAX({_quote_unit_sql()}), AVG({_quote_unit_sql()})
            FROM quote_details qd
            WHERE qd.quote_date <= :d
              AND TRIM(qd.category_name) = :v
              {exclude}
              AND {_quote_unit_sql()} IS NOT NULL
              AND qd.quote_date = (
                  SELECT MAX(q2.quote_date) FROM quote_details q2
                  WHERE q2.quote_date <= :d
                    AND TRIM(q2.category_name) = :v
                    {exclude.replace('qd.', 'q2.')}
              )
        """
        res = await session.execute(text(sql), params)
        row = res.first()
        if row and row[0] is not None:
            comp_max = Decimal(str(row[0]))
        if row and row[1] is not None:
            comp_avg = Decimal(str(round(float(row[1]), 4)))

    return DailyPriceContext(
        as_of_date=as_of,
        lead_market_price=lead,
        own_calibration_price=own_price,
        competitor_price_max=comp_max,
        competitor_price_avg=comp_avg,
    )


def _pearson(xs: list[float], ys: list[float]) -> Optional[float]:
    n = len(xs)
    if n < 5 or n != len(ys):
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


async def estimate_warehouse_price_profile(
    session: AsyncSession,
    *,
    warehouse: str,
    product_variety: str,
    own_factory_id: Optional[int] = None,
    lookback_days: int = 120,
) -> WarehousePriceProfile:
    """基于历史发货与价格优势的相关性估计库房价格敏感度。"""
    end = date.today()
    start = end - timedelta(days=lookback_days)
    stmt = (
        select(DeliveryRecord.delivery_date, func.sum(DeliveryRecord.weight).label("tw"))
        .where(
            and_(
                DeliveryRecord.warehouse == warehouse,
                DeliveryRecord.product_variety == product_variety,
                DeliveryRecord.delivery_date >= start,
                DeliveryRecord.delivery_date <= end,
            )
        )
        .group_by(DeliveryRecord.delivery_date)
    )
    res = await session.execute(stmt)
    daily: dict[date, Decimal] = {}
    weights: list[Decimal] = []
    for d, tw in res.all():
        w = Decimal(tw)
        daily[d] = w
        weights.append(w)

    if weights:
        cap_max = max(weights)
        cap_min = min(weights)
        cap_avg = sum(weights) / Decimal(len(weights))
    else:
        cap_max = cap_min = cap_avg = Decimal("0")

    xs: list[float] = []
    ys: list[float] = []
    fid = own_factory_id if own_factory_id is not None else await resolve_own_factory_id(session)
    for d, w in sorted(daily.items()):
        ctx = await load_daily_price_context(
            session, as_of=d, product_variety=product_variety, own_factory_id=fid
        )
        adv = ctx.vs_competitor_ratio if ctx.vs_competitor_ratio is not None else ctx.vs_market_ratio
        if adv is None:
            continue
        xs.append(adv)
        ys.append(float(w))

    corr = _pearson(xs, ys)
    threshold = settings.prediction_price_sensitivity_threshold
    if corr is None:
        sensitivity: PriceSensitivity = "medium"
    elif abs(corr) >= threshold:
        sensitivity = "sensitive"
    elif abs(corr) < threshold * 0.5:
        sensitivity = "stable"
    else:
        sensitivity = "medium"

    return WarehousePriceProfile(
        warehouse=warehouse,
        sensitivity=sensitivity,
        correlation=round(corr, 4) if corr is not None else None,
        capacity_max=cap_max.quantize(Decimal("0.01")),
        capacity_min=cap_min.quantize(Decimal("0.01")),
        capacity_avg=cap_avg.quantize(Decimal("0.01")),
    )


async def load_price_context_for_horizon(
    session: AsyncSession,
    *,
    dates: list[date],
    product_variety: str,
    own_factory_id: Optional[int] = None,
) -> dict[date, DailyPriceContext]:
    """批量加载预测区间内各日价格（未来日沿用最近有效价）。"""
    fid = own_factory_id if own_factory_id is not None else await resolve_own_factory_id(session)
    out: dict[date, DailyPriceContext] = {}
    for d in dates:
        out[d] = await load_daily_price_context(
            session, as_of=d, product_variety=product_variety, own_factory_id=fid
        )
    return out


async def build_intelligent_price_summary(
    session: AsyncSession,
    *,
    warehouse: str,
    product_variety: str,
    forecast_dates: list[date],
) -> dict[str, object]:
    """供大模型 Prompt 使用的价格与库房画像摘要。"""
    profile = await estimate_warehouse_price_profile(
        session, warehouse=warehouse, product_variety=product_variety
    )
    ctx_map = await load_price_context_for_horizon(
        session, dates=forecast_dates, product_variety=product_variety
    )
    latest_ctx = ctx_map.get(forecast_dates[0]) if forecast_dates else None
    return {
        "warehouse_price_profile": {
            "sensitivity": profile.sensitivity,
            "correlation": profile.correlation,
            "capacity_max": float(profile.capacity_max),
            "capacity_min": float(profile.capacity_min),
            "capacity_avg": float(profile.capacity_avg),
        },
        "price_weight": settings.prediction_price_weight,
        "history_weight": settings.prediction_history_weight,
        "latest_prices": {
            "lead_market": float(latest_ctx.lead_market_price)
            if latest_ctx and latest_ctx.lead_market_price
            else None,
            "own_calibration": float(latest_ctx.own_calibration_price)
            if latest_ctx and latest_ctx.own_calibration_price
            else None,
            "competitor_max": float(latest_ctx.competitor_price_max)
            if latest_ctx and latest_ctx.competitor_price_max
            else None,
            "vs_market_ratio": latest_ctx.vs_market_ratio if latest_ctx else None,
            "vs_competitor_ratio": latest_ctx.vs_competitor_ratio if latest_ctx else None,
        },
        "forecast_price_notes": [
            {
                "date": d.isoformat(),
                "lead_market": float(c.lead_market_price) if c.lead_market_price else None,
                "own": float(c.own_calibration_price) if c.own_calibration_price else None,
                "competitor_max": float(c.competitor_price_max) if c.competitor_price_max else None,
            }
            for d, c in sorted(ctx_map.items())
        ],
    }
