"""垂直库房 AI 定价分析：聚合源库房出边/绑定间价差指标，调用大模型生成三方面建议。"""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from app import config as app_config
from app.database import get_conn
from app.vertical_warehouse_ai.exceptions import VerticalWarehouseAiLLMError
from app.services.tl_dict_geo_crud import (
    CODE_OK,
    warehouse_links_realtime_spread_list,
)

logger = logging.getLogger(__name__)

_NO_BINDINGS_MSG = "该库房没有绑定其他库房，无法预测"

_DEFAULT_COMPETITOR_KEYWORDS: Tuple[str, ...] = ("超威", "豫光")

# 传给大模型前剔除的内部 id 字段（避免回复中出现「ID 62」等数据库编号）
_LLM_STRIP_KEYS: frozenset[str] = frozenset(
    {
        "库房id",
        "绑定库房id",
        "竞品类型id列表",
        "关联id",
        "源库房id",
        "对标库房id",
        "id",
        "类型id",
    }
)

_ID_PAREN_RE = re.compile(r"[（(]\s*ID\s*(\d+)\s*[）)]", re.IGNORECASE)
_ID_LIST_RE = re.compile(r"\bID\s*(\d+(?:\s*[、,，]\s*\d+)*)", re.IGNORECASE)
_ID_KV_RE = re.compile(
    r"(?:库房|类型|关联|对标库房|源库房)?\s*[iI][dD]\s*[=：:\s]\s*(\d+)",
    re.IGNORECASE,
)


def _cell_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    return v


def _competitor_keywords() -> Tuple[str, ...]:
    """内置超威/豫光 + 环境变量配置的类型名称关键词（子串匹配）。"""
    kws: Set[str] = set(_DEFAULT_COMPETITOR_KEYWORDS)
    for name in app_config.VERTICAL_WAREHOUSE_AI_COMPETITOR_TYPE_NAMES:
        s = str(name).strip()
        if s:
            kws.add(s)
    return tuple(sorted(kws))


def resolve_competitor_type_ids() -> Set[int]:
    """合并环境变量 id 与按类型名称关键词（LIKE）查库解析的 id。"""
    ids: Set[int] = set(app_config.VERTICAL_WAREHOUSE_AI_COMPETITOR_TYPE_IDS)
    keywords = _competitor_keywords()
    if not keywords:
        return ids
    with get_conn() as conn:
        with conn.cursor() as cur:
            for kw in keywords:
                cur.execute(
                    "SELECT id FROM dict_warehouse_types WHERE name LIKE %s",
                    (f"%{kw}%",),
                )
                for row in cur.fetchall():
                    ids.add(int(row[0]))
    return ids


def _load_type_names(type_ids: Set[int]) -> List[str]:
    if not type_ids:
        return []
    ph = ",".join(["%s"] * len(type_ids))
    names: List[str] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT name FROM dict_warehouse_types WHERE id IN ({ph}) ORDER BY id",
                tuple(sorted(type_ids)),
            )
            for row in cur.fetchall():
                n = str(row[0] or "").strip()
                if n:
                    names.append(n)
    return names


def _strip_internal_ids_for_llm(obj: Any) -> Any:
    """递归剔除内部 id 字段，供大模型输入使用。"""
    if isinstance(obj, dict):
        return {
            k: _strip_internal_ids_for_llm(v)
            for k, v in obj.items()
            if k not in _LLM_STRIP_KEYS
        }
    if isinstance(obj, list):
        return [_strip_internal_ids_for_llm(x) for x in obj]
    return obj


def _build_id_name_map(context: Dict[str, Any]) -> Dict[int, str]:
    """从完整上下文中收集 id→名称映射，用于净化 AI 文案。"""
    mapping: Dict[int, str] = {}

    def _add(wid: Any, name: Any) -> None:
        if wid is None or name is None:
            return
        try:
            i = int(wid)
        except (TypeError, ValueError):
            return
        n = str(name).strip()
        if i > 0 and n:
            mapping[i] = n

    src = context.get("源库房") or {}
    _add(src.get("库房id"), src.get("库房名称"))
    _add(src.get("绑定库房id"), src.get("绑定库房名称"))

    for list_key in (
        "绑定边列表",
        "绑定间边列表",
        "自有绑定边列表",
        "竞品绑定边列表",
    ):
        for edge in context.get(list_key) or []:
            _add(edge.get("源库房id"), (edge.get("源库房") or {}).get("名称"))
            _add(edge.get("对标库房id"), (edge.get("对标库房") or {}).get("名称"))
            for wh_key in ("源库房", "对标库房"):
                wh = edge.get(wh_key) or {}
                _add(wh.get("id"), wh.get("名称"))
    return mapping


def _name_for_id(id_val: int, id_name_map: Dict[int, str]) -> str:
    return id_name_map.get(id_val, "")


def _replace_id_list_match(match: re.Match[str], id_name_map: Dict[int, str]) -> str:
    raw = match.group(1)
    parts = re.split(r"\s*[、,，]\s*", raw.strip())
    names: List[str] = []
    for p in parts:
        if not p.strip().isdigit():
            continue
        name = _name_for_id(int(p.strip()), id_name_map)
        if name:
            names.append(name)
    # 无法映射的名称不保留 id 数字，避免回复中出现裸 id
    return "、".join(names) if names else ""


def _sanitize_narrative_text(text: Any, id_name_map: Dict[int, str]) -> Any:
    """将 AI 文案中的数据库 id 引用替换为对应名称。"""
    if not isinstance(text, str) or not text.strip() or not id_name_map:
        return text
    out = text

    def _paren_sub(m: re.Match[str]) -> str:
        name = _name_for_id(int(m.group(1)), id_name_map)
        return name if name else ""

    out = _ID_PAREN_RE.sub(_paren_sub, out)

    def _list_sub(m: re.Match[str]) -> str:
        return _replace_id_list_match(m, id_name_map)

    out = _ID_LIST_RE.sub(_list_sub, out)

    def _kv_sub(m: re.Match[str]) -> str:
        name = _name_for_id(int(m.group(1)), id_name_map)
        return name if name else ""

    out = _ID_KV_RE.sub(_kv_sub, out)
    # 清理因替换产生的多余标点/空格
    out = re.sub(r"[、,，]\s*[、,，]+", "、", out)
    out = re.sub(r"\s{2,}", " ", out)
    return out.strip()


def _sanitize_llm_sections(sections: Dict[str, Any], id_name_map: Dict[int, str]) -> Dict[str, Any]:
    text_keys = ("建议原因", "调价建议", "定价总建议")
    for block_key in ("与自己的库房比", "与竞品库房比", "配置价差比"):
        block = sections.get(block_key)
        if not isinstance(block, dict):
            continue
        for tk in text_keys:
            if tk in block and block[tk] is not None:
                block[tk] = _sanitize_narrative_text(block[tk], id_name_map)
    if sections.get("rawText"):
        sections["rawText"] = _sanitize_narrative_text(sections["rawText"], id_name_map)
    return sections


def is_competitor_warehouse(
    type_id: Optional[int],
    type_name: str,
    competitor_type_ids: Set[int],
    competitor_keywords: Tuple[str, ...],
) -> bool:
    if type_id is not None and type_id in competitor_type_ids:
        return True
    tname = (type_name or "").strip()
    if not tname:
        return False
    for kw in competitor_keywords:
        if kw and kw in tname:
            return True
    return False


def _tier_spread_scalar(tier_raw: Any) -> Optional[float]:
    """仅当阶梯价差配置为纯数字时返回数值；JSON 对象/数组返回 None。"""
    if tier_raw is None:
        return None
    if isinstance(tier_raw, bool):
        return None
    if isinstance(tier_raw, Decimal):
        return float(tier_raw)
    if isinstance(tier_raw, (int, float)):
        return float(tier_raw)
    if isinstance(tier_raw, str):
        s = tier_raw.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            try:
                parsed = json.loads(s)
            except json.JSONDecodeError:
                return None
            return _tier_spread_scalar(parsed)
    return None


def _config_spread_ratio(
    realtime: Optional[float],
    tier_scalar: Optional[float],
    *,
    has_tier_config: bool,
) -> Tuple[Optional[float], Optional[str]]:
    if realtime is None:
        return None, "缺少实时价差"
    if tier_scalar is None:
        if has_tier_config:
            return None, "阶梯价差配置非纯数字，请结合配置 JSON 由模型分析"
        return None, "缺少阶梯价差配置"
    if tier_scalar == 0:
        return None, "阶梯价差为 0，无法计算比值"
    return round(realtime / tier_scalar, 4), None


def _load_warehouse_type_map(wh_ids: Set[int]) -> Dict[int, Tuple[Optional[int], str]]:
    if not wh_ids:
        return {}
    ph = ",".join(["%s"] * len(wh_ids))
    out: Dict[int, Tuple[Optional[int], str]] = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT w.id, w.warehouse_type_id, COALESCE(t.name, '')
                FROM dict_warehouses w
                LEFT JOIN dict_warehouse_types t ON t.id = w.warehouse_type_id
                WHERE w.id IN ({ph})
                """,
                tuple(sorted(wh_ids)),
            )
            for wid, tid, tname in cur.fetchall():
                out[int(wid)] = (int(tid) if tid is not None else None, str(tname or ""))
    return out


def _enrich_link_row(
    raw: Dict[str, Any],
    *,
    competitor_type_ids: Set[int],
    competitor_keywords: Tuple[str, ...],
    edge_kind: str,
    type_map: Dict[int, Tuple[Optional[int], str]],
) -> Dict[str, Any]:
    dist = raw.get("distanceKm")
    tier_raw = raw.get("tierPriceSpread")
    tier_scalar = _tier_spread_scalar(tier_raw)
    rt = raw.get("realtimeSpread")
    ratio, ratio_note = _config_spread_ratio(
        float(rt) if rt is not None else None,
        tier_scalar,
        has_tier_config=tier_raw is not None,
    )
    tgt = raw.get("target") or {}
    src = raw.get("source") or {}
    tgt_id = int(tgt.get("id") or raw.get("toWarehouseId") or 0)
    src_id = int(src.get("id") or raw.get("fromWarehouseId") or 0)
    tgt_tid, tgt_tname = type_map.get(tgt_id, (None, ""))
    src_tid, src_tname = type_map.get(src_id, (None, ""))
    if not tgt.get("type") and tgt_tname:
        tgt = {**tgt, "type": tgt_tname}
    if not src.get("type") and src_tname:
        src = {**src, "type": src_tname}
    src_type_label = str(src.get("type") or src_tname or "")
    tgt_type_label = str(tgt.get("type") or tgt_tname or "")
    return {
        "边类型": edge_kind,
        "关联id": raw.get("linkId"),
        "源库房id": raw.get("fromWarehouseId"),
        "对标库房id": raw.get("toWarehouseId"),
        "距离千米": dist,
        "源库房定价": raw.get("fromWarehousePrice"),
        "对标库房定价": raw.get("toWarehousePrice"),
        "实时价差": rt,
        "阶梯价差配置": tier_raw,
        "配置价差比": ratio,
        "配置价差比说明": ratio_note,
        "源库房": {
            "id": src.get("id"),
            "名称": src.get("name"),
            "类型": src_type_label,
            "类型id": src_tid,
            "是否竞品": is_competitor_warehouse(
                src_tid, src_type_label, competitor_type_ids, competitor_keywords
            ),
        },
        "对标库房": {
            "id": tgt.get("id"),
            "名称": tgt.get("name"),
            "类型": tgt_type_label,
            "类型id": tgt_tid,
            "是否竞品": is_competitor_warehouse(
                tgt_tid, tgt_type_label, competitor_type_ids, competitor_keywords
            ),
        },
    }


def _fetch_all_outbound_realtime_links(from_warehouse_id: int) -> List[Dict[str, Any]]:
    page = 1
    size = 200
    all_items: List[Dict[str, Any]] = []
    while True:
        res = warehouse_links_realtime_spread_list(
            page=page,
            size=size,
            from_warehouse_id=from_warehouse_id,
        )
        if res.get("code") != CODE_OK:
            raise ValueError(res.get("msg") or "查询出边实时价差失败")
        payload = res.get("data") or {}
        items = payload.get("list") or []
        all_items.extend(items)
        total = int(payload.get("total") or 0)
        if page * size >= total or not items:
            break
        page += 1
    return all_items


def _fetch_bound_internal_links_efficient(bound_ids: Set[int]) -> List[Dict[str, Any]]:
    """查询绑定库房之间的有向边（含定价与价差）。"""
    if len(bound_ids) < 2:
        return []
    id_list = sorted(bound_ids)
    ph = ",".join(["%s"] * len(id_list))
    params = tuple(id_list + id_list)
    # 复用 realtime list 的单条 SQL 逻辑：直接查库并组装
    from app.services.tl_dict_geo_crud import _haversine_km, _parse_tier_price_spread, _warehouse_row_api, _wh_side_row

    with get_conn() as conn:
        from pymysql.cursors import DictCursor

        with conn.cursor(DictCursor) as cur:
            cur.execute(
                f"""
                SELECT l.id AS link_id, l.from_warehouse_id, l.to_warehouse_id,
                       l.tier_price_spread,
                       wsc_f.warehouse_price AS from_warehouse_price,
                       wsc_t.warehouse_price AS to_warehouse_price,
                       wf.id AS sf_id, wf.name AS sf_name, wf.warehouse_type_id AS sf_warehouse_type_id,
                       wfs.name AS sf_type_name,
                       wf.longitude AS sf_longitude, wf.latitude AS sf_latitude,
                       wt.id AS st_id, wt.name AS st_name, wt.warehouse_type_id AS st_warehouse_type_id,
                       wts.name AS st_type_name,
                       wt.longitude AS st_longitude, wt.latitude AS st_latitude
                FROM dict_warehouse_links l
                INNER JOIN dict_warehouses wf ON wf.id = l.from_warehouse_id
                INNER JOIN dict_warehouses wt ON wt.id = l.to_warehouse_id
                LEFT JOIN dict_warehouse_types wfs ON wf.warehouse_type_id = wfs.id
                LEFT JOIN dict_warehouse_types wts ON wt.warehouse_type_id = wts.id
                LEFT JOIN pd_warehouse_spread_configs wsc_f ON wsc_f.warehouse_id = l.from_warehouse_id
                LEFT JOIN pd_warehouse_spread_configs wsc_t ON wsc_t.warehouse_id = l.to_warehouse_id
                WHERE l.from_warehouse_id IN ({ph}) AND l.to_warehouse_id IN ({ph})
                ORDER BY l.id ASC
                """,
                params,
            )
            rows = cur.fetchall()

    items: List[Dict[str, Any]] = []
    for r in rows:
        dist_km: Optional[float] = None
        try:
            sln, sla = r.get("sf_longitude"), r.get("sf_latitude")
            tln, tla = r.get("st_longitude"), r.get("st_latitude")
            if sln is not None and sla is not None and tln is not None and tla is not None:
                dist_km = round(_haversine_km(float(sln), float(sla), float(tln), float(tla)), 3)
        except (TypeError, ValueError):
            pass
        fp, tp = r.get("from_warehouse_price"), r.get("to_warehouse_price")
        realtime_spread = None
        if fp is not None and tp is not None:
            try:
                realtime_spread = round(float(fp) - float(tp), 4)
            except (TypeError, ValueError):
                pass
        items.append(
            {
                "linkId": int(r["link_id"]),
                "fromWarehouseId": int(r["from_warehouse_id"]),
                "toWarehouseId": int(r["to_warehouse_id"]),
                "distanceKm": dist_km,
                "tierPriceSpread": _parse_tier_price_spread(r.get("tier_price_spread")),
                "fromWarehousePrice": float(fp) if fp is not None else None,
                "toWarehousePrice": float(tp) if tp is not None else None,
                "realtimeSpread": realtime_spread,
                "source": _warehouse_row_api(_wh_side_row(r, "sf"), r.get("sf_type_name")),
                "target": _warehouse_row_api(_wh_side_row(r, "st"), r.get("st_type_name")),
            }
        )
    return items


def build_analysis_context(
    tl_service: Any,
    warehouse_id: int,
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    """聚合源库房、出边、绑定间指标（不含 LLM 建议）。"""
    if warehouse_id < 1:
        raise ValueError("warehouse_id 无效")

    competitor_type_ids = resolve_competitor_type_ids()
    competitor_keywords = _competitor_keywords()
    as_of = as_of_date or tl_service._pricing_calendar_date()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, province, city, district, warehouse_type_id, is_active
                FROM dict_warehouses WHERE id = %s
                """,
                (warehouse_id,),
            )
            wh = cur.fetchone()
            if not wh:
                raise ValueError("库房不存在")
            if int(wh[6] or 0) != 1:
                raise ValueError("库房已停用")
            wh_name = str(wh[1] or "")
            jinli_id = tl_service._jinli_factory_id(cur)
            source_core = tl_service._compute_ai_pricing_core(
                cur,
                warehouse_id=warehouse_id,
                warehouse_name=wh_name,
                province=wh[2],
                city=wh[3],
                district=wh[4],
                jinli_id=jinli_id,
                as_of=as_of,
            )

    outbound_raw = _fetch_all_outbound_realtime_links(warehouse_id)
    if not outbound_raw:
        raise ValueError(_NO_BINDINGS_MSG)

    bound_ids: Set[int] = set()
    for it in outbound_raw:
        tid = it.get("toWarehouseId")
        if tid is not None:
            bound_ids.add(int(tid))

    wh_ids_for_types: Set[int] = {warehouse_id} | bound_ids
    for it in outbound_raw:
        wh_ids_for_types.add(int(it.get("fromWarehouseId") or 0))
        wh_ids_for_types.add(int(it.get("toWarehouseId") or 0))
    type_map = _load_warehouse_type_map({i for i in wh_ids_for_types if i > 0})

    outbound_edges = [
        _enrich_link_row(
            it,
            competitor_type_ids=competitor_type_ids,
            competitor_keywords=competitor_keywords,
            edge_kind="源到绑定",
            type_map=type_map,
        )
        for it in outbound_raw
    ]
    internal_raw = _fetch_bound_internal_links_efficient(bound_ids)
    internal_edges = [
        _enrich_link_row(
            it,
            competitor_type_ids=competitor_type_ids,
            competitor_keywords=competitor_keywords,
            edge_kind="绑定到绑定",
            type_map=type_map,
        )
        for it in internal_raw
    ]

    own_bindings = [e for e in outbound_edges if not e["对标库房"]["是否竞品"]]
    competitor_bindings = [e for e in outbound_edges if e["对标库房"]["是否竞品"]]

    source_summary = {k: _cell_json(v) for k, v in source_core.items()}

    competitor_type_names = _load_type_names(competitor_type_ids)

    return {
        "口径日期": as_of.isoformat(),
        "竞品类型id列表": sorted(competitor_type_ids),
        "竞品类型名称列表": competitor_type_names,
        "竞品类型关键词": list(competitor_keywords),
        "源库房": source_summary,
        "绑定边列表": outbound_edges,
        "绑定间边列表": internal_edges,
        "自有绑定边列表": own_bindings,
        "竞品绑定边列表": competitor_bindings,
    }


def _extract_json_object(text: str) -> Optional[dict]:
    text = (text or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try:
            parsed = json.loads(m.group(0))
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    return None


def _normalize_llm_sections(parsed: Optional[dict], raw_text: str) -> Dict[str, Any]:
    default_own = {
        "定价是否合理": None,
        "建议定价": None,
        "建议原因": "大模型未返回有效 JSON",
    }
    default_comp = {
        "定价是否合理": None,
        "调价建议": None,
        "建议原因": "大模型未返回有效 JSON",
    }
    default_ratio = {
        "定价总建议": None,
        "建议原因": "大模型未返回有效 JSON",
    }
    if not parsed:
        return {
            "与自己的库房比": default_own,
            "与竞品库房比": default_comp,
            "配置价差比": default_ratio,
            "parseError": True,
            "rawText": raw_text,
        }
    own = parsed.get("与自己的库房比") or parsed.get("own_warehouses") or {}
    comp = parsed.get("与竞品库房比") or parsed.get("competitor_warehouses") or {}
    ratio = parsed.get("配置价差比") or parsed.get("spread_ratio_summary") or {}
    return {
        "与自己的库房比": {
            "定价是否合理": own.get("定价是否合理", own.get("reasonable")),
            "建议定价": own.get("建议定价", own.get("suggested_price")),
            "建议原因": own.get("建议原因", own.get("reasons", "")),
        },
        "与竞品库房比": {
            "定价是否合理": comp.get("定价是否合理", comp.get("reasonable")),
            "调价建议": comp.get("调价建议", comp.get("pricing_adjustment")),
            "建议原因": comp.get("建议原因", comp.get("reasons", "")),
        },
        "配置价差比": {
            "定价总建议": ratio.get("定价总建议", ratio.get("overall_suggestion")),
            "建议原因": ratio.get("建议原因", ratio.get("reasons", "")),
        },
        "parseError": False,
        "rawText": raw_text if not isinstance(parsed, dict) else None,
    }


def run_llm_analysis(context: Dict[str, Any]) -> Dict[str, Any]:
    """调用大模型生成三方面 JSON 建议。"""
    if not (app_config.LLM_API_KEY or "").strip():
        raise VerticalWarehouseAiLLMError(
            "未配置文本大模型密钥。请设置 LLM_API_KEY，或与报价图识别共用 VLM_API_KEY。"
        )

    from openai import OpenAI

    client = OpenAI(api_key=app_config.LLM_API_KEY, base_url=app_config.LLM_BASE_URL)
    id_name_map = _build_id_name_map(context)
    llm_context = _strip_internal_ids_for_llm(context)
    data_str = json.dumps(llm_context, ensure_ascii=False, indent=2)
    competitor_type_names = context.get("竞品类型名称列表") or []
    competitor_kws = context.get("竞品类型关键词") or list(_DEFAULT_COMPETITOR_KEYWORDS)
    competitor_desc = (
        "、".join(competitor_type_names)
        if competitor_type_names
        else "、".join(competitor_kws) or "超威、豫光等品牌竞品库"
    )
    prompt = f"""你是废旧电池回收行业的库房定价分析顾问。请根据以下结构化数据，评估「源库房」相对其绑定库房网络的定价是否合理。

业务说明：
- 「实时价差」= 源库房电动车电瓶收货价 − 对标库房同品类收货价（元/吨，可为负）；接口字段「源库房定价」「对标库房定价」数值亦为该品类收货价。
- 「阶梯价差配置」= 关联边上配置的价差（纯数字或 JSON 结构）；请直接依据该配置分析，勿假设按距离分段匹配。
- 「配置价差比」= 实时价差 ÷ 阶梯价差配置数值（仅当配置为纯数字时有效；为 JSON 时该字段为 null，请结合配置 JSON 自行分析）。
- 「自有库房」：对标库房「是否竞品」为 false（非超威、豫光等品牌竞品库）。
- 「竞品库房」：对标库房「是否竞品」为 true（类型为 {competitor_desc}，或类型名称含关键词 {competitor_kws}）。
- 绑定间边为多个绑定库房之间的对比，用于交叉验证价差结构。
- **输出要求**：所有说明文字中禁止出现数据库 id、内部编号（例如 ID 62、库房id=10、类型id=3）；引用库房时必须使用数据中的「库房名称」「名称」字段，引用库房类型时使用「类型」字段。

数据：
{data_str}

请仅输出一个 JSON 对象（不要 markdown 代码块），键名必须为中文，结构如下：
{{
  "与自己的库房比": {{
    "定价是否合理": true或false,
    "建议定价": 数字或null,
    "建议原因": "结合距离、实时价差、阶梯价差、绑定库房定价等说明"
  }},
  "与竞品库房比": {{
    "定价是否合理": true或false,
    "调价建议": "如何调整定价以获取更多收益的简述",
    "建议原因": "结合竞品绑定边、价差比等说明；若无竞品绑定则说明暂无"
  }},
  "配置价差比": {{
    "定价总建议": "综合各边配置价差比与绑定间对比后的总建议",
    "建议原因": "说明主要偏离边与调整方向"
  }}
}}"""

    try:
        resp = client.chat.completions.create(
            model=app_config.LLM_MODEL,
            max_tokens=4096,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text = (resp.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.exception("垂直库房 AI 大模型调用失败")
        err_text = str(exc).lower()
        status = getattr(exc, "status_code", None)
        if status == 403 or ("403" in str(exc) and "forbidden" in err_text):
            raise VerticalWarehouseAiLLMError(
                "大模型服务端拒绝请求（HTTP 403）。请检查 LLM_API_KEY、LLM_BASE_URL、LLM_MODEL。"
            ) from exc
        raise VerticalWarehouseAiLLMError(f"大模型调用失败：{exc}") from exc

    parsed = _extract_json_object(raw_text)
    sections = _normalize_llm_sections(parsed, raw_text)
    sections = _sanitize_llm_sections(sections, id_name_map)
    sections["llm_model"] = app_config.LLM_MODEL
    return sections


def run_full_analysis(
    tl_service: Any,
    warehouse_id: int,
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    context = build_analysis_context(tl_service, warehouse_id, as_of_date=as_of_date)
    ai = run_llm_analysis(context)
    llm_model = ai.pop("llm_model", app_config.LLM_MODEL)
    return {
        "口径日期": context.get("口径日期"),
        "竞品类型id列表": context.get("竞品类型id列表"),
        "竞品类型名称列表": context.get("竞品类型名称列表"),
        "源库房": context.get("源库房"),
        "绑定边列表": context.get("绑定边列表"),
        "绑定间边列表": context.get("绑定间边列表"),
        "自有绑定边列表": context.get("自有绑定边列表"),
        "竞品绑定边列表": context.get("竞品绑定边列表"),
        "ai建议": {
            "与自己的库房比": ai.get("与自己的库房比"),
            "与竞品库房比": ai.get("与竞品库房比"),
            "配置价差比": ai.get("配置价差比"),
        },
        "llm_model": llm_model,
        "llm_parse_error": ai.get("parseError", False),
        "llm_raw_text": ai.get("rawText"),
        "_input_context": context,
        "_llm_result_full": ai,
    }
