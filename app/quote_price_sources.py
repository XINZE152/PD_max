# -*- coding: utf-8 -*-
"""
报价各价格字段来源：「原数据」= OCR/前端直接录入；「换算」= 服务端按税率或公式推算。
入库使用库表列名（英文）作 JSON 键；接口可与前端约定相同键名。
"""
from typing import Any, Dict, Optional

SOURCE_ORIGINAL = "原数据"
SOURCE_DERIVED = "换算"

PRICE_FIELD_KEYS_DB = frozenset(
    {
        "unit_price",
        "price_1pct_vat",
        "price_3pct_vat",
        "price_13pct_vat",
        "price_normal_invoice",
        "price_reverse_invoice",
    }
)

API_KEY_TO_DB: Dict[str, str] = {
    "价格": "unit_price",
    "价格_1pct增值税": "price_1pct_vat",
    "价格_3pct增值税": "price_3pct_vat",
    "价格_13pct增值税": "price_13pct_vat",
    "普通发票价格": "price_normal_invoice",
    "反向发票价格": "price_reverse_invoice",
}

ALLOWED_SOURCE_VALUES = frozenset({SOURCE_ORIGINAL, SOURCE_DERIVED})


def normalize_client_sources(raw: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """校验并统一为 DB 列名 -> 原数据|换算。"""
    if not raw:
        return {}
    out: Dict[str, str] = {}
    for k, v in raw.items():
        dbk = API_KEY_TO_DB.get(k, k)
        if dbk not in PRICE_FIELD_KEYS_DB:
            continue
        if not isinstance(v, str) or v not in ALLOWED_SOURCE_VALUES:
            continue
        out[dbk] = v
    return out


def merge_sources_after_fill(
    item: Dict[str, Any],
    snapshot_before_fill: Dict[str, Any],
    client_sources: Dict[str, str],
) -> Dict[str, str]:
    """
    在服务端补全含税列（如由不含税基准推算）之后生成最终来源表。
    规则：客户端对某列显式传入的来源优先；若该列在补全前为空、补全后有值，则强制为「换算」。
    """
    merged: Dict[str, str] = dict(client_sources)
    for api_k, db_k in API_KEY_TO_DB.items():
        val = item.get(api_k)
        if val is None:
            merged.pop(db_k, None)
            continue
        snap_v = snapshot_before_fill.get(api_k)
        filled_by_server = snap_v is None
        if db_k in merged:
            if filled_by_server:
                merged[db_k] = SOURCE_DERIVED
            continue
        merged[db_k] = SOURCE_DERIVED if filled_by_server else SOURCE_ORIGINAL
    return merged
