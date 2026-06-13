from datetime import date
from decimal import Decimal

from app.intelligent_prediction.schemas.doubao_prediction import DailyTonnageItem
from app.intelligent_prediction.utils.prediction_report_sync import (
    extract_summary_table_weights,
    reconcile_items_with_summary_table,
)


def test_extract_summary_table():
    report = """
--- 预测发货汇总表 ---
| 序号 | 索引 | 日期 | 日发货量(吨) |
|------|------|------|-------------|
| 1 | day2 | 2026-06-15 | 40 |
| 2 | day5 | 2026-06-18 | 70 |
| 3 | day12 | 2026-06-25 | 35 |
--- 汇总表结束 ---
"""
    w = extract_summary_table_weights(report)
    assert w[date(2026, 6, 15)] == Decimal("40")
    assert w[date(2026, 6, 25)] == Decimal("35")


def test_reconcile_fixes_wrong_json_index_mapping():
    """模型 JSON 仅 3 条且按顺序写时，旧逻辑会把 40 吨错配到 day0；校正后应对齐汇总表。"""
    start = date(2026, 6, 13)
    forecast_dates = [start.replace(day=start.day + i) if False else date(2026, 6, 13 + i) for i in range(16)]
    # fix dates properly
    from datetime import timedelta
    forecast_dates = [start + timedelta(days=i) for i in range(16)]

    report = """
| 1 | day2 | 2026-06-15 | 40 |
| 2 | day5 | 2026-06-18 | 70 |
| 3 | day12 | 2026-06-25 | 35 |
"""
    # 模拟错误解析：只有 06-13 有 40 吨（索引对齐 bug）
    wrong_items = [
        DailyTonnageItem(
            target_date=forecast_dates[0],
            predicted_weight=Decimal("40"),
            ship_probability="中",
            confidence_level="中",
            main_factors="错",
        )
    ] + [
        DailyTonnageItem(
            target_date=forecast_dates[i],
            predicted_weight=Decimal("0"),
            ship_probability="低",
            confidence_level="低",
            main_factors="",
        )
        for i in range(1, 16)
    ]
    wrong_items[12] = DailyTonnageItem(
        target_date=forecast_dates[12],
        predicted_weight=Decimal("35"),
        ship_probability="中",
        confidence_level="中",
        main_factors="",
    )

    fixed = reconcile_items_with_summary_table(wrong_items, report, forecast_dates)
    by = {it.target_date: it.predicted_weight for it in fixed}
    assert by[date(2026, 6, 13)] == Decimal("0")
    assert by[date(2026, 6, 15)] == Decimal("40")
    assert by[date(2026, 6, 18)] == Decimal("70")
    assert by[date(2026, 6, 25)] == Decimal("35")