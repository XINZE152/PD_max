"""
「合作库房清单」类 Excel：解析库房名称与整行地址。

供 `POST /tl/import_partner_warehouses_excel` 与离线脚本共用列名推断规则。
"""

from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path
from typing import Union

import pandas as pd

DEFAULT_NAME_CANDIDATES = ("库房名称", "仓库名称", "名称", "name", "Name", "库房名")
DEFAULT_ADDRESS_CANDIDATES = ("库房地址", "仓库地址", "地址", "address", "Address", "详细地址")


class PartnerWarehouseExcelError(ValueError):
    """表头无法识别、工作表不存在等。"""


def _norm_col(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    t = str(s).strip()
    t = re.sub(r"\s+", "", t)
    return t


def _resolve_column(df: pd.DataFrame, explicit: str | None, candidates: tuple[str, ...]) -> str:
    if explicit and explicit.strip():
        c = explicit.strip()
        if c not in df.columns:
            raise PartnerWarehouseExcelError(
                f"列不存在: {c!r}，当前列: {list(df.columns)}"
            )
        return c
    norm_map = {_norm_col(col): col for col in df.columns}
    for cand in candidates:
        key = _norm_col(cand)
        if key in norm_map:
            return norm_map[key]
    raise PartnerWarehouseExcelError(
        "无法自动识别名称或地址列，请传 name_col / address_col。"
        f" 当前列: {list(df.columns)}"
    )


def _cell_str(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v).strip()


def _resolve_sheet_name(xl: pd.ExcelFile, sheet: str | None, sheet_index: int | None) -> str:
    if sheet_index is not None:
        if sheet_index < 0 or sheet_index >= len(xl.sheet_names):
            raise PartnerWarehouseExcelError(
                f"sheet_index 越界: {sheet_index}，共 {len(xl.sheet_names)} 张表"
            )
        return xl.sheet_names[sheet_index]
    if sheet:
        if sheet not in xl.sheet_names:
            raise PartnerWarehouseExcelError(
                f"工作表不存在: {sheet!r}，可选: {xl.sheet_names}"
            )
        return sheet
    preferred = "合作库房清单"
    return preferred if preferred in xl.sheet_names else xl.sheet_names[0]


def parse_partner_warehouse_rows(
    source: Union[Path, bytes],
    *,
    sheet: str | None = None,
    sheet_index: int | None = None,
    name_col: str | None = None,
    address_col: str | None = None,
) -> tuple[str, str, str, list[tuple[str, str]]]:
    """
    :param source: 本地路径、文件字节或类文件对象
    :return: (sheet_name, name_column, address_column, [(仓库名, 整行地址), ...])
    """
    if isinstance(source, Path):
        xl = pd.ExcelFile(source)
        sheet_name = _resolve_sheet_name(xl, sheet, sheet_index)
        df = pd.read_excel(source, sheet_name=sheet_name, dtype=object)
    else:
        raw = source
        bio = BytesIO(raw)
        xl = pd.ExcelFile(bio)
        sheet_name = _resolve_sheet_name(xl, sheet, sheet_index)
        bio.seek(0)
        df = pd.read_excel(bio, sheet_name=sheet_name, dtype=object)

    nc = _resolve_column(df, name_col, DEFAULT_NAME_CANDIDATES)
    ac = _resolve_column(df, address_col, DEFAULT_ADDRESS_CANDIDATES)

    rows: list[tuple[str, str]] = []
    for _, r in df.iterrows():
        name = _cell_str(r.get(nc))
        addr = _cell_str(r.get(ac))
        if not name and not addr:
            continue
        if not name:
            continue
        if not addr:
            continue
        if len(name) > 100:
            continue
        if len(addr) > 500:
            addr = addr[:500]
        rows.append((name, addr))
    return sheet_name, nc, ac, rows


def warehouse_site_fields_from_full_address(full_addr: str) -> tuple[str | None, str | None, str | None, str]:
    """
    将 Excel 整行地址转为 ``TLService.add_warehouse`` 的省/市/区/详址参数。

    与单条完整落库条件一致：省、市、区、详址四项均非空时返回四级字符串；否则退回极简模式
    （省市区为 None，地址为整行截断至 500 字）。
    """
    from app.utils.cn_address_split import split_cn_region_address

    full_addr = (full_addr or "").strip()
    if not full_addr:
        return None, None, None, ""

    p, c, d, detail = split_cn_region_address(full_addr)
    if p or c or d:
        street = ((detail or "").strip() or full_addr)[:500]
        p2 = (p or "")[:64]
        c2 = (c or "")[:64]
        d2 = (d or "")[:64]
        if p2.strip() and c2.strip() and d2.strip() and street.strip():
            return p2, c2, d2, street
    return None, None, None, full_addr[:500]
