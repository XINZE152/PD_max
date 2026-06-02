"""综合预测 v2 — 节假日影响分析。

检测春节、国庆、五一、中秋四大节日临近，判断提前发货/推迟发货/暂停发货。
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

import chinese_calendar as cc


# 中国主要节日（固定日期 + 农历中秋需特殊处理）
# 春节：农历正月初一（约1月下旬至2月中旬）
# 五一：5月1日前后
# 国庆：10月1日前后
# 中秋：农历八月十五（约9月至10月）


def analyze_holiday_impact(target_dates: list[date]) -> dict[str, Any]:
    """分析目标日期区间的节假日影响。

    Returns:
        {
            "analysis_text": str,
            "spring_festival_nearby": bool,
            "national_day_nearby": bool,
            "labor_day_nearby": bool,
            "mid_autumn_nearby": bool,
            "impact_summary": str,  # "提前发货" | "推迟发货" | "暂停发货" | "无显著影响"
        }
    """
    if not target_dates:
        return _empty_impact()

    start = min(target_dates)
    end = max(target_dates)

    spring_festival = _check_spring_festival(start, end)
    national_day = _check_national_day(start, end)
    labor_day = _check_labor_day(start, end)
    mid_autumn = _check_mid_autumn(start, end)

    impact = _determine_impact(spring_festival, national_day, labor_day, mid_autumn, start, end)

    analysis_text = _build_analysis_text(
        spring_festival=spring_festival,
        national_day=national_day,
        labor_day=labor_day,
        mid_autumn=mid_autumn,
        impact=impact,
    )

    return {
        "analysis_text": analysis_text,
        "spring_festival_nearby": spring_festival["nearby"],
        "national_day_nearby": national_day["nearby"],
        "labor_day_nearby": labor_day["nearby"],
        "mid_autumn_nearby": mid_autumn["nearby"],
        "impact_summary": impact["summary"],
        "impact_detail": impact["detail"],
    }


def _check_spring_festival(start: date, end: date) -> dict[str, Any]:
    """检查春节期间（1月15日~2月20日宽窗口）。"""
    nearby = False
    phase = None

    for d in _date_range(start, end):
        try:
            is_holiday = cc.is_holiday(d)
        except NotImplementedError:
            is_holiday = False

        # 宽窗口：1月15日至2月20日
        if d.month == 1 and d.day >= 15:
            nearby = True
            phase = "春节前"
        elif d.month == 2 and d.day <= 20:
            nearby = True
            phase = "春节中/后"

        if is_holiday and 1 <= d.month <= 3:
            nearby = True
            phase = "春节假期"

    if nearby:
        # 判断是节前还是节后
        if phase == "春节前":
            return {
                "nearby": True,
                "name": "春节",
                "phase": "节前",
                "impact": "提前发货",
                "detail": f"预测区间包含春节前夕，仓库可能在节前集中发货备货。",
            }
        elif phase == "春节中/后":
            return {
                "nearby": True,
                "name": "春节",
                "phase": "节中/节后",
                "impact": "推迟发货",
                "detail": f"预测区间包含春节假期或节后恢复期，发货可能延迟。",
            }
        else:
            return {
                "nearby": True,
                "name": "春节",
                "phase": "假期中",
                "impact": "暂停发货",
                "detail": f"预测区间包含春节假期，多数仓库暂停发货。",
            }

    return {"nearby": False, "name": "春节", "phase": None, "impact": None, "detail": None}


def _check_national_day(start: date, end: date) -> dict[str, Any]:
    """检查国庆期间（9月25日~10月10日宽窗口）。"""
    nearby = False
    for d in _date_range(start, end):
        if (d.month == 9 and d.day >= 25) or (d.month == 10 and d.day <= 10):
            nearby = True
            break
        try:
            if cc.is_holiday(d) and d.month in (9, 10):
                nearby = True
                break
        except NotImplementedError:
            pass

    if nearby:
        return {
            "nearby": True,
            "name": "国庆",
            "impact": "提前发货/暂停发货",
            "detail": f"预测区间临近国庆长假，部分仓库可能提前发货，节中暂停发货。",
        }
    return {"nearby": False, "name": "国庆", "impact": None, "detail": None}


def _check_labor_day(start: date, end: date) -> dict[str, Any]:
    """检查五一期间（4月25日~5月5日宽窗口）。"""
    nearby = False
    for d in _date_range(start, end):
        if (d.month == 4 and d.day >= 25) or (d.month == 5 and d.day <= 5):
            nearby = True
            break
        try:
            if cc.is_holiday(d) and d.month == 5:
                nearby = True
                break
        except NotImplementedError:
            pass

    if nearby:
        return {
            "nearby": True,
            "name": "五一",
            "impact": "提前发货/暂停发货",
            "detail": f"预测区间临近五一假期，部分仓库可能提前发货，节中发货减少。",
        }
    return {"nearby": False, "name": "五一", "impact": None, "detail": None}


def _check_mid_autumn(start: date, end: date) -> dict[str, Any]:
    """检查中秋期间（9月至10月宽窗口，具体依赖 chinese_calendar）。"""
    nearby = False
    for d in _date_range(start, end):
        try:
            if cc.is_holiday(d) and d.month in (9, 10):
                nearby = True
                break
        except NotImplementedError:
            pass

    if nearby:
        return {
            "nearby": True,
            "name": "中秋",
            "impact": "提前发货/推迟发货",
            "detail": f"预测区间包含或临近中秋节，发货节奏可能受影响。",
        }
    return {"nearby": False, "name": "中秋", "impact": None, "detail": None}


def _determine_impact(
    spring: dict,
    national: dict,
    labor: dict,
    mid_autumn: dict,
    start: date,
    end: date,
) -> dict[str, str]:
    """综合判定节假日影响。"""
    holidays = [spring, national, labor, mid_autumn]
    active = [h for h in holidays if h.get("nearby")]

    if not active:
        return {
            "summary": "无显著影响",
            "detail": f"预测区间（{start.isoformat()} 至 {end.isoformat()}）无重大节假日，发货节奏不受节日影响。",
        }

    # 春节影响最大
    if spring["nearby"]:
        return {"summary": spring["impact"], "detail": spring["detail"]}

    # 合并描述
    impacts = [h["detail"] for h in active if h.get("detail")]
    summary = active[0]["impact"] if active else "无显著影响"
    detail = "；".join(impacts) if impacts else "预测区间包含节假日。"
    return {"summary": summary, "detail": detail}


def _build_analysis_text(
    spring_festival: dict,
    national_day: dict,
    labor_day: dict,
    mid_autumn: dict,
    impact: dict,
) -> str:
    parts: list[str] = []
    parts.append(f"节假日影响判定：{impact['summary']}。")
    parts.append(impact["detail"])

    if spring_festival["nearby"]:
        parts.append(f"春节：{spring_festival.get('phase', '')}，{spring_festival.get('impact', '')}。")
    if national_day["nearby"]:
        parts.append(f"国庆：{national_day.get('impact', '')}。")
    if labor_day["nearby"]:
        parts.append(f"五一：{labor_day.get('impact', '')}。")
    if mid_autumn["nearby"]:
        parts.append(f"中秋：{mid_autumn.get('impact', '')}。")

    return "".join(parts)


def _empty_impact() -> dict[str, Any]:
    return {
        "analysis_text": "无预测日期，无法分析节假日影响。",
        "spring_festival_nearby": False,
        "national_day_nearby": False,
        "labor_day_nearby": False,
        "mid_autumn_nearby": False,
        "impact_summary": "无显著影响",
        "impact_detail": "",
    }


def _date_range(start: date, end: date) -> list[date]:
    from datetime import timedelta
    dates: list[date] = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates
