# -*- coding: utf-8 -*-
"""
报价含税换算（与 quote_details、比价一致）：

- **unit_price / 接口「价格」**：**不含税基准价**。
- **单列报价**：表内数字按 **备注** 解析为「不含税 / 含1% / 含3% / 含13%」口径；**无备注或无法识别时默认按不含税**。
- 含 1%/3%/13% 价由不含税基准 × (1+税率) 推算；若表中数字本身是含税价，先除以 (1+税率) 还原不含税，再推算其它档。

冶炼厂 `factory_tax_rates` 覆盖默认税率。
"""
import re
from typing import Dict, Literal, Optional, Tuple, Any

PriceBasis = Literal["ex_vat", "incl_1pct", "incl_3pct", "incl_13pct"]

DEFAULT_FACTORY_VAT_RATES: Dict[str, float] = {
    "1pct": 0.01,
    "3pct": 0.03,
    "13pct": 0.13,
}


def merge_factory_rates(db_rates: Optional[Dict[str, float]] = None) -> Dict[str, float]:
    out = dict(DEFAULT_FACTORY_VAT_RATES)
    if db_rates:
        out.update(db_rates)
    return out


def net_from_inclusive(inclusive: float, rate: float) -> float:
    return float(inclusive) / (1 + float(rate))


def inclusive_from_net(net: float, rate: float) -> float:
    return round(float(net) * (1 + float(rate)), 2)


def parse_price_basis_from_remark(remark: str) -> PriceBasis:
    """
    从行备注推断表中报价数字的口径。无备注 → 不含税。
    匹配顺序：显式不含税 → 13% → 3% → 1%/普票 → 泛「含税」→ 默认不含税。
    """
    t = (remark or "").strip()
    if not t:
        return "ex_vat"
    if any(k in t for k in ("不含税", "未税", "税前", "裸价")):
        return "ex_vat"
    if re.search(r"13\s*%|含\s*13|13\s*专|13点|十三点", t):
        return "incl_13pct"
    if re.search(r"3\s*%|含\s*3|3\s*专|三点", t):
        return "incl_3pct"
    if re.search(r"1\s*%|含\s*1|1\s*普|普票", t):
        return "incl_1pct"
    if "含税" in t:
        return "incl_3pct"
    return "ex_vat"


def derive_vat_prices_from_stated_price(
    stated: float,
    basis: PriceBasis,
    rates: Optional[Dict[str, float]] = None,
) -> Tuple[float, float, float, float]:
    """
    stated：表中读出的一列报价数字（按 basis 解释）。
    返回 (不含税基准, 含1%价, 含3%价, 含13%价)。
    """
    r = merge_factory_rates(rates)
    s = float(stated)
    if basis == "ex_vat":
        net = s
        return (
            round(net, 2),
            inclusive_from_net(net, r["1pct"]),
            inclusive_from_net(net, r["3pct"]),
            inclusive_from_net(net, r["13pct"]),
        )
    if basis == "incl_3pct":
        net = net_from_inclusive(s, r["3pct"])
        p3 = round(s, 2)
        return (
            round(net, 4),
            inclusive_from_net(net, r["1pct"]),
            p3,
            inclusive_from_net(net, r["13pct"]),
        )
    if basis == "incl_13pct":
        net = net_from_inclusive(s, r["13pct"])
        p13 = round(s, 2)
        return (
            round(net, 4),
            inclusive_from_net(net, r["1pct"]),
            inclusive_from_net(net, r["3pct"]),
            p13,
        )
    if basis == "incl_1pct":
        net = net_from_inclusive(s, r["1pct"])
        p1 = round(s, 2)
        return (
            round(net, 4),
            p1,
            inclusive_from_net(net, r["3pct"]),
            inclusive_from_net(net, r["13pct"]),
        )
    return derive_vat_prices_from_stated_price(stated, "ex_vat", rates)


def fill_vat_from_exclusive_net(
    net: float, rates: Optional[Dict[str, float]] = None
) -> Tuple[float, float, float]:
    """已知不含税基准，补全含1%/3%/13%价。"""
    r = merge_factory_rates(rates)
    n = float(net)
    return (
        inclusive_from_net(n, r["1pct"]),
        inclusive_from_net(n, r["3pct"]),
        inclusive_from_net(n, r["13pct"]),
    )


def derive_net_and_vat_from_quote_row(
    prices: Dict[str, Any],
    merged_rates: Dict[str, float],
) -> Optional[Tuple[float, float, float, float]]:
    """
    从 quote_details 一行（各列可能只填部分）反推统一的不含税基准与各档含税价。
    返回 (基准不含税, 含1%价, 含3%价, 含13%价)；无法推算时 None。

    优先级：unit_price → 13%/3%/1% 含税列（按厂税率反算不含税）→ 普票/反向发票列（按不含税理解）。
    """
    if prices.get("unit_price") is not None:
        net = float(prices["unit_price"])
        p1, p3, p13 = fill_vat_from_exclusive_net(net, merged_rates)
        return round(net, 2), p1, p3, p13

    for col, tax_key in (
        ("price_13pct_vat", "13pct"),
        ("price_3pct_vat", "3pct"),
        ("price_1pct_vat", "1pct"),
    ):
        v = prices.get(col)
        if v is not None and tax_key in merged_rates:
            net = net_from_inclusive(float(v), merged_rates[tax_key])
            p1, p3, p13 = fill_vat_from_exclusive_net(net, merged_rates)
            return round(net, 4), p1, p3, p13

    for col in ("price_normal_invoice", "price_reverse_invoice"):
        v = prices.get(col)
        if v is not None:
            net = float(v)
            p1, p3, p13 = fill_vat_from_exclusive_net(net, merged_rates)
            return round(net, 2), p1, p3, p13

    return None
