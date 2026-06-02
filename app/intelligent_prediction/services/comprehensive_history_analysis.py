"""综合预测 v2 — 深度历史发货规律分析。

在原有简单统计基础上，增加：
- 发货间隔天数（相邻送货日之差）
- 平均发货间隔、间隔标准差
- 周期稳定性判定
- 当前距离上次发货天数
- 是否接近或超过历史平均发货周期
- 月均发货量、发货频次、单次平均/最大/最小
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Optional

from app.intelligent_prediction.schemas.prediction import PredictionHistoryPoint


def analyze_delivery_pattern(
    history: list[PredictionHistoryPoint],
    *,
    as_of_date: Optional[date] = None,
) -> dict[str, Any]:
    """深度分析历史发货规律，返回结构化字典供 Prompt 使用。

    Args:
        history: 历史送货记录（已按 delivery_date 排序）。
        as_of_date: 当前基准日（默认当天），用于计算距上次发货天数。

    Returns:
        包含月均量、频次、间隔、周期稳定性等的字典。
    """
    if not history:
        return _empty_pattern()

    sorted_pts = sorted(history, key=lambda p: p.delivery_date)
    weights = [float(p.weight) for p in sorted_pts]
    dates = [p.delivery_date for p in sorted_pts]

    now = as_of_date or date.today()

    # 发货间隔
    intervals: list[int] = []
    for i in range(1, len(dates)):
        delta = (dates[i] - dates[i - 1]).days
        if delta > 0:
            intervals.append(delta)

    avg_interval = sum(intervals) / len(intervals) if intervals else 0.0
    interval_std = _std(intervals) if len(intervals) > 1 else 0.0
    # 周期稳定性：变异系数 < 0.3 视为稳定
    period_stable = (avg_interval > 0) and (interval_std / avg_interval < 0.3) if intervals else False

    # 距上次发货天数
    last_date = dates[-1]
    days_since_last = (now - last_date).days

    # 周期 proximity 判断
    cycle_judgment = _judge_cycle_proximity(days_since_last, avg_interval)

    # 月均发货量
    date_range_days = (dates[-1] - dates[0]).days if len(dates) > 1 else 1
    months = max(1, date_range_days / 30.0)
    monthly_avg = sum(weights) / months

    # 发货频次
    frequency = len(dates)

    # 单次统计
    single_avg = sum(weights) / len(weights)
    single_max = max(weights)
    single_min = min(weights)

    # 趋势（与现有逻辑一致）
    first_half = weights[: max(1, len(weights) // 2)]
    second_half = weights[max(1, len(weights) // 2) :]
    trend = "持平"
    if first_half and second_half:
        a = sum(first_half) / len(first_half)
        b = sum(second_half) / len(second_half)
        if b > a * 1.1:
            trend = "上升"
        elif b < a * 0.9:
            trend = "下降"

    # 星期分布
    weekday_counts: dict[str, int] = {}
    for d in dates:
        wd_name = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][d.weekday()]
        weekday_counts[wd_name] = weekday_counts.get(wd_name, 0) + 1

    return {
        "monthly_avg_tons": round(monthly_avg, 2),
        "delivery_frequency": frequency,
        "single_avg_tons": round(single_avg, 4),
        "single_max_tons": round(single_max, 4),
        "single_min_tons": round(single_min, 4),
        "intervals": intervals[:50],  # 最多展示 50 个间隔
        "avg_interval_days": round(avg_interval, 1),
        "interval_std_days": round(interval_std, 1),
        "period_stable": period_stable,
        "last_delivery_date": last_date.isoformat(),
        "days_since_last_delivery": days_since_last,
        "cycle_judgment": cycle_judgment,
        "recent_trend": trend,
        "weekday_distribution": weekday_counts,
        "date_range": f"{dates[0].isoformat()} 至 {dates[-1].isoformat()}",
    }


def format_history_analysis_text(pattern: dict[str, Any]) -> str:
    """将历史分析字典格式化为中文可读文本，供 Prompt 使用。"""
    if pattern.get("delivery_frequency", 0) == 0:
        return "无历史发货数据，无法分析历史规律。"

    parts: list[str] = []
    parts.append(
        f"历史统计区间 {pattern['date_range']}，"
        f"共发货 {pattern['delivery_frequency']} 次，"
        f"月均发货约 {pattern['monthly_avg_tons']} 吨。"
    )
    parts.append(
        f"单次发货：平均 {pattern['single_avg_tons']} 吨，"
        f"最大 {pattern['single_max_tons']} 吨，"
        f"最小 {pattern['single_min_tons']} 吨。"
    )
    if pattern.get("avg_interval_days", 0) > 0:
        parts.append(
            f"发货间隔：平均 {pattern['avg_interval_days']} 天，"
            f"标准差 {pattern['interval_std_days']} 天，"
            f"周期{'稳定' if pattern['period_stable'] else '不稳定'}。"
        )
    parts.append(
        f"当前距离上次发货已过去 {pattern['days_since_last_delivery']} 天（上次：{pattern['last_delivery_date']}）。"
    )
    parts.append(f"近期趋势：{pattern['recent_trend']}。")
    if pattern.get("weekday_distribution"):
        wd = pattern["weekday_distribution"]
        wd_str = "、".join(f"{k}{v}次" for k, v in sorted(wd.items()))
        parts.append(f"星期分布：{wd_str}。")
    parts.append(f"周期判断：{pattern['cycle_judgment']}。")
    return "".join(parts)


def _judge_cycle_proximity(days_since: int, avg_interval: float) -> str:
    """判断当前是否接近/超过历史平均发货周期。"""
    if avg_interval <= 0:
        return "历史数据不足以判断发货周期"
    ratio = days_since / avg_interval if avg_interval > 0 else 0
    if ratio >= 1.2:
        return (
            f"已超过历史平均发货周期（{avg_interval:.1f}天）的 120%，"
            f"当前已 {days_since} 天未发货，**应提高发货概率**。"
        )
    if ratio >= 0.8:
        return (
            f"已接近历史平均发货周期（{avg_interval:.1f}天），"
            f"当前已 {days_since} 天未发货，**可适当提高发货概率**。"
        )
    if ratio >= 0.5:
        return (
            f"处于历史平均发货周期（{avg_interval:.1f}天）的中段，"
            f"当前已 {days_since} 天未发货，**发货概率保持正常**。"
        )
    return (
        f"远未达到历史平均发货周期（{avg_interval:.1f}天），"
        f"当前仅 {days_since} 天未发货，**应降低发货概率**。"
    )


def _empty_pattern() -> dict[str, Any]:
    return {
        "monthly_avg_tons": 0,
        "delivery_frequency": 0,
        "single_avg_tons": 0,
        "single_max_tons": 0,
        "single_min_tons": 0,
        "intervals": [],
        "avg_interval_days": 0,
        "interval_std_days": 0,
        "period_stable": False,
        "last_delivery_date": None,
        "days_since_last_delivery": None,
        "cycle_judgment": "无历史数据，无法判断周期",
        "recent_trend": "无数据",
        "weekday_distribution": {},
        "date_range": "无",
    }


def _std(values: list[int | float]) -> float:
    """计算总体标准差。"""
    if not values:
        return 0.0
    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    return variance ** 0.5
