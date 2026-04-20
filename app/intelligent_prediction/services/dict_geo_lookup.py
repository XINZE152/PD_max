"""从 TL 字典表解析仓库 / 冶炼厂所在「市」（dict_warehouses、dict_factories）。

匹配顺序：精确名称 → 去空白后全等 → 子串模糊（字典名含导入名，或导入名含字典名），
多命中时按规则打分取一条。
"""

from __future__ import annotations

import re
from typing import Literal, Optional

from app.database import get_conn
from app.intelligent_prediction.logging_utils import get_logger

logger = get_logger(__name__)

_TABLE_WH = "dict_warehouses"
_TABLE_DF = "dict_factories"
_DictTable = Literal["dict_warehouses", "dict_factories"]


def _compact(s: str) -> str:
    """去掉首尾与中间空白（含全角空格），用于宽松等值比较。"""
    return re.sub(r"[\s\u3000]+", "", (s or "").strip())


def _escape_like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")


def _rank_match(query: str, dict_name: str) -> tuple[int, int]:
    """分数越小越好；第二项用于同档排序。"""
    q, n = query.strip(), dict_name.strip()
    if not q:
        return (99, 0)
    if n == q:
        return (0, 0)
    qc, nc = _compact(q), _compact(n)
    if qc and nc == qc:
        return (1, 0)
    # 字典名是导入名的子串：如导入「上海宝钢一号库」命中字典「宝钢」
    if n and n in q:
        return (2, -len(n))
    # 导入名是字典名的子串：如导入「宝钢」命中「上海宝钢股份有限公司」
    if q and q in n:
        return (3, len(n))
    return (9, len(n))


def _city_from_row(row: tuple) -> Optional[str]:
    if not row or not row[0]:
        return None
    t = str(row[0]).strip()
    return t or None


def _lookup_city_one_table(cur, table: _DictTable, raw_name: str) -> Optional[str]:
    assert table in (_TABLE_WH, _TABLE_DF)
    name = (raw_name or "").strip()
    if not name:
        return None

    cur.execute(
        f"SELECT city FROM {table} WHERE name = %s AND is_active = 1 LIMIT 1",
        (name,),
    )
    row = cur.fetchone()
    c = _city_from_row(row)
    if c:
        return c

    compact = _compact(name)
    if len(compact) < 2:
        return None

    esc = _escape_like(name)
    like_pat = f"%{esc}%"
    # 与 _compact 一致：去掉空格与全角空格、常见换行
    cur.execute(
        f"""
        SELECT city, name FROM {table}
        WHERE is_active = 1
          AND city IS NOT NULL AND TRIM(city) <> ''
          AND (
            REPLACE(REPLACE(REPLACE(REPLACE(TRIM(name), CHAR(10), ''), CHAR(13), ''), ' ', ''), '　', '') = %s
            OR name LIKE %s ESCAPE '\\\\'
            OR %s LIKE CONCAT('%%', name, '%%')
          )
        LIMIT 80
        """,
        (compact, like_pat, name),
    )
    rows = cur.fetchall()
    if not rows:
        return None

    valid = [(c, n) for c, n in rows if c is not None and n is not None and str(n).strip()]
    if not valid:
        return None

    city_cell, dict_name = min(
        valid,
        key=lambda r: (_rank_match(name, str(r[1])), str(r[1])),
    )
    return _city_from_row((city_cell,))


def lookup_warehouse_factory_cities(
    warehouse_name: str,
    smelter_name: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """返回 (仓库所在市, 冶炼厂所在市)。查不到则为 None。

    匹配规则（每条内按顺序，命中即停；模糊阶段多行取打分最优）：

    1. ``name`` 与导入名**完全一致**（trim），且 ``is_active = 1``；
    2. 去掉空白后 ``name`` 与导入名全等；
    3. ``name LIKE %导入%`` 或 ``导入 LIKE CONCAT('%', name, '%')``；
       多命中时：精确 > 去空白等 > 字典名为导入子串（偏好更长字典名）
       > 导入名为字典子串（偏好更短字典名）。
    """
    wh_city: Optional[str] = None
    sm_city: Optional[str] = None
    wn = (warehouse_name or "").strip()
    sn = (smelter_name or "").strip() or None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if wn:
                    wh_city = _lookup_city_one_table(cur, _TABLE_WH, wn)
                if sn:
                    sm_city = _lookup_city_one_table(cur, _TABLE_DF, sn)
    except Exception:
        logger.exception("dict geo lookup failed warehouse=%s smelter=%s", wn, sn)
        return None, None
    return wh_city, sm_city
