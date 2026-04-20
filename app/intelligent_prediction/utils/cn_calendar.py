"""中国工作日 / 节假日标注（`chinese_calendar` + 节假日中文名）。"""

from __future__ import annotations

from datetime import date


def cn_workday_and_label(d: date) -> tuple[bool, str]:
    """返回 (是否工作日, 简要中文说明)。"""
    import chinese_calendar as cc
    from chinese_calendar.constants import Holiday

    try:
        if cc.is_in_lieu(d):
            return True, "调休工作日"
        if cc.is_workday(d):
            if d.weekday() < 5:
                return True, "工作日"
            return True, "调休上班"
        _, en_name = cc.get_holiday_detail(d)
        if en_name:
            for h in Holiday:
                if h.value == en_name:
                    return False, h.chinese
            return False, str(en_name)[:128]
        if d.weekday() >= 5:
            return False, "周末"
        return False, "休息日"
    except NotImplementedError:
        return True, f"日历库未覆盖{d.year}年"
    except Exception:
        return True, "节假日识别失败"
