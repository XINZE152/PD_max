"""中国工作日推算（`chinese_calendar`）。

送货历史的「节假日」列已改为由导入表填写；本模块可用于其它需按公历推算工作日的场景。
"""

from __future__ import annotations

from datetime import date


def cn_workday_and_label(d: date) -> tuple[bool, str]:
    """返回 (是否中国工作日, 节假日列取值)。

    节假日列**仅**为「是」或「否」：
    - **否**：中国工作日（含调休上班）；
    - **是**：非工作日（法定节假日、周末等）。
    """
    import chinese_calendar as cc

    try:
        wd = bool(cc.is_workday(d))
        return wd, "否" if wd else "是"
    except NotImplementedError:
        return True, "否"
    except Exception:
        return True, "否"
