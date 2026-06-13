"""将 LLM 分析报告中的「预测发货汇总表」与结构化 items 对齐。"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

from app.intelligent_prediction.logging_utils import get_logger

if TYPE_CHECKING:
    from app.intelligent_prediction.schemas.doubao_prediction import DailyTonnageItem

logger = get_logger(__name__)

_SUMMARY_TABLE_ROW = re.compile(
    r"^\|\s*\d+\s*\|\s*day\s*(\d+)\s*\|\s*(\d{4}-\d{2}-\d{2})\s*\|\s*([\d.]+)\s*\|",
    re.MULTILINE | re.IGNORECASE,
)


def extract_summary_table_weights(report_text: str) -> dict[date, Decimal]:
    """从 analysis_report 中解析「预测发货汇总表」里的 (日期 → 日发货量)。"""
    if not report_text:
        return {}
    out: dict[date, Decimal] = {}
    for m in _SUMMARY_TABLE_ROW.finditer(report_text):
        try:
            d = date.fromisoformat(m.group(2))
            w = Decimal(str(m.group(3)))
            if w < 0:
                w = Decimal("0")
            out[d] = w
        except (ValueError, ArithmeticError):
            continue
    return out


def reconcile_items_with_summary_table(
    items: list["DailyTonnageItem"],
    report_text: str,
    forecast_dates: list[date],
) -> list["DailyTonnageItem"]:
    """以汇总表为准覆盖 predicted_weight，保证图表与「预测依据」一致。"""
    from app.intelligent_prediction.schemas.doubao_prediction import DailyTonnageItem

    table = extract_summary_table_weights(report_text)
    if not table:
        return items

    by_date = {it.target_date: it for it in items}
    reconciled: list[DailyTonnageItem] = []
    changed = 0

    for fd in forecast_dates:
        it = by_date.get(fd)
        table_w = table.get(fd)
        if table_w is not None:
            if it is None or it.predicted_weight != table_w:
                changed += 1
            ship = it.ship_probability if it and table_w > 0 else "低"
            conf = it.confidence_level if it and table_w > 0 else "低"
            factors = (it.main_factors if it else "") or "与预测发货汇总表一致"
            reconciled.append(
                DailyTonnageItem(
                    target_date=fd,
                    predicted_weight=table_w,
                    ship_probability=ship if table_w > 0 else "低",
                    confidence_level=conf if table_w > 0 else "低",
                    main_factors=factors[:500],
                )
            )
        elif it is not None:
            reconciled.append(it)
        else:
            reconciled.append(
                DailyTonnageItem(
                    target_date=fd,
                    predicted_weight=Decimal("0"),
                    ship_probability="低",
                    confidence_level="低",
                    main_factors="汇总表未列该日，视为不发货",
                )
            )

    if changed:
        logger.info(
            "prediction_reconcile summary_table=%d rows adjusted=%d forecast_days=%d",
            len(table),
            changed,
            len(forecast_dates),
        )
    return reconciled