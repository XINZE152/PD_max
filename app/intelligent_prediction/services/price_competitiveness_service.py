"""综合预测 v2 — 目标冶炼厂价格竞争力分析。

三优先级：
1. 目标冶炼厂价格 vs 周边竞品冶炼厂价格
2. 目标冶炼厂近期价格变化趋势（3天/7天）
3. SMM 铅价变化趋势

六级评级：A(优势高) → B(优势中) → C(优势低) → D(劣势低) → E(劣势中) → F(劣势高)
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.services.price_context_service import (
    DailyPriceContext,
    load_daily_price_context,
    resolve_own_factory_id,
)
from app.intelligent_prediction.services.lead_price_service import get_latest_lead_price
from app.intelligent_prediction.logging_utils import get_logger

logger = get_logger(__name__)

# 六级评级阈值（相对竞品的偏离比例）
_GRADE_THRESHOLDS = [
    ("A", 0.05, "优势高"),   # 己方价格比竞品高 5% 以上
    ("B", 0.02, "优势中"),   # 己方比竞品高 2%~5%
    ("C", 0.0,  "优势低"),   # 己方比竞品高 0%~2%
    ("D", -0.02, "劣势低"),  # 己方比竞品低 0%~2%
    ("E", -0.05, "劣势中"),  # 己方比竞品低 2%~5%
    ("F", float("-inf"), "劣势高"),  # 己方比竞品低 5% 以上
]


async def analyze_price_competitiveness(
    session: AsyncSession,
    *,
    as_of: date,
    product_variety: str,
    own_factory_id: Optional[int] = None,
    lookback_days: int = 7,
) -> dict[str, Any]:
    """分析目标冶炼厂在当前日期的价格竞争力。

    Returns:
        {
            "grade": "A" | "B" | ... | "F",
            "grade_label": "优势高" | ... | "劣势高",
            "own_price": Decimal | None,
            "competitor_max": Decimal | None,
            "competitor_avg": Decimal | None,
            "vs_competitor_ratio": float | None,  # (own - comp_max) / comp_max
            "vs_market_ratio": float | None,      # (own - market) / market
            "lead_market_price": Decimal | None,
            "own_3d_trend": str,  # 上升/下降/持平
            "own_7d_trend": str,
            "smm_trend": str,
            "analysis_text": str,
        }
    """
    fid = own_factory_id if own_factory_id is not None else await resolve_own_factory_id(session)

    # 获取当日及历史价格上下文
    ctx = await load_daily_price_context(
        session, as_of=as_of, product_variety=product_variety, own_factory_id=fid
    )

    # 获取 SMM 铅价趋势
    smm_current = await get_latest_lead_price(session, as_of)
    smm_3d_ago = await get_latest_lead_price(session, as_of - timedelta(days=3))
    smm_7d_ago = await get_latest_lead_price(session, as_of - timedelta(days=7))

    own_3d_ctx = None
    own_7d_ctx = None
    if fid is not None:
        own_3d_ctx = await load_daily_price_context(
            session, as_of=as_of - timedelta(days=3),
            product_variety=product_variety, own_factory_id=fid,
        )
        own_7d_ctx = await load_daily_price_context(
            session, as_of=as_of - timedelta(days=7),
            product_variety=product_variety, own_factory_id=fid,
        )

    # 计算趋势
    own_3d_trend = _calc_trend(ctx.own_calibration_price, own_3d_ctx.own_calibration_price if own_3d_ctx else None)
    own_7d_trend = _calc_trend(ctx.own_calibration_price, own_7d_ctx.own_calibration_price if own_7d_ctx else None)
    smm_trend = _calc_trend(smm_current, smm_3d_ago)
    smm_7d_trend = _calc_trend(smm_current, smm_7d_ago)

    # 六级评级（基于竞品对比）
    grade, grade_label = _assign_grade(ctx.vs_competitor_ratio, ctx.vs_market_ratio)

    # 生成分析文本
    analysis_text = _build_analysis_text(
        ctx=ctx,
        grade=grade,
        grade_label=grade_label,
        own_3d_trend=own_3d_trend,
        own_7d_trend=own_7d_trend,
        smm_trend=smm_trend,
        smm_7d_trend=smm_7d_trend,
    )

    return {
        "grade": grade,
        "grade_label": grade_label,
        "own_price": ctx.own_calibration_price,
        "competitor_max": ctx.competitor_price_max,
        "competitor_avg": ctx.competitor_price_avg,
        "vs_competitor_ratio": ctx.vs_competitor_ratio,
        "vs_market_ratio": ctx.vs_market_ratio,
        "lead_market_price": ctx.lead_market_price,
        "own_3d_trend": own_3d_trend,
        "own_7d_trend": own_7d_trend,
        "smm_trend": smm_trend,
        "smm_7d_trend": smm_7d_trend,
        "analysis_text": analysis_text,
    }


def _calc_trend(current: Optional[Decimal], previous: Optional[Decimal]) -> str:
    if current is None or previous is None:
        return "无数据"
    if previous == 0:
        return "无数据"
    pct = (float(current) - float(previous)) / float(previous) * 100
    if abs(pct) < 0.5:
        return "持平"
    if pct > 0:
        return f"上升（+{pct:.1f}%）"
    return f"下降（{pct:.1f}%）"


def _assign_grade(
    vs_competitor: Optional[float],
    vs_market: Optional[float],
) -> tuple[str, str]:
    """根据相对价格偏离分配六级评级。"""
    if vs_competitor is not None:
        for grade, threshold, label in _GRADE_THRESHOLDS:
            if vs_competitor >= threshold:
                return grade, label
    # 无竞品数据时退而使用行情
    if vs_market is not None:
        for grade, threshold, label in _GRADE_THRESHOLDS:
            if vs_market >= threshold:
                return grade, label
    return "C", "优势低"


def _build_analysis_text(
    ctx: DailyPriceContext,
    grade: str,
    grade_label: str,
    own_3d_trend: str,
    own_7d_trend: str,
    smm_trend: str,
    smm_7d_trend: str,
) -> str:
    parts: list[str] = []

    # 第一优先级：竞品对比
    if ctx.competitor_price_max is not None and ctx.own_calibration_price is not None:
        diff_pct = ctx.vs_competitor_ratio * 100 if ctx.vs_competitor_ratio is not None else 0
        parts.append(
            f"目标冶炼厂当前价格 {ctx.own_calibration_price}，"
            f"周边竞品最高 {ctx.competitor_price_max}，"
            f"竞品平均 {ctx.competitor_price_avg or '无数据'}。"
        )
        if diff_pct > 0:
            parts.append(f"目标厂价格相对竞品最高价高 {diff_pct:.1f}%，处于价格优势。")
        elif diff_pct < 0:
            parts.append(f"目标厂价格相对竞品最高价低 {abs(diff_pct):.1f}%，处于价格劣势。")
        else:
            parts.append("目标厂价格与竞品最高价持平。")
    elif ctx.own_calibration_price is not None and ctx.lead_market_price is not None:
        market_pct = ctx.vs_market_ratio * 100 if ctx.vs_market_ratio is not None else 0
        parts.append(
            f"无竞品报价数据。目标厂价格 {ctx.own_calibration_price}，"
            f"SMM 铅价行情 {ctx.lead_market_price}，相对行情偏离 {market_pct:+.1f}%。"
        )
    else:
        parts.append("缺少己方标定价格和/或竞品报价，价格竞争力按中性处理。")

    # 第二优先级：目标厂近期趋势
    parts.append(f"目标厂价格近3天趋势：{own_3d_trend}；近7天趋势：{own_7d_trend}。")

    # 第三优先级：SMM 铅价趋势
    parts.append(f"SMM 铅价行情近3天趋势：{smm_trend}；近7天趋势：{smm_7d_trend}。")

    parts.append(f"综合评定：{grade}级（{grade_label}）。")

    return "".join(parts)
