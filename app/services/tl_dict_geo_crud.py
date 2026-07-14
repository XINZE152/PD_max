"""
TL 比价使用：仓库/冶炼厂字典表落库 + 天地图地理编码（仅由 TLService 调用，不对外单独暴露 HTTP）。

- 省市区与详细地址齐全时，经度/纬度未手传则调用 maybe_geocode 填充；失败时依配置可存 NULL。
- 冶炼厂不维护颜色配置；库中 color_config 列可留空。
"""
from __future__ import annotations

import json
import logging
import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor

from app.database import get_conn
from app.services.tianditu_geocoder import GeocoderError, maybe_geocode

logger = logging.getLogger(__name__)

_EV_BATTERY_CATEGORY_NAMES: Tuple[str, ...] = ("电动车电瓶", "电动电瓶")


def resolve_ev_battery_category_id(cur) -> Optional[int]:
    """解析电动车电瓶品类 id（用于实时价差=收货价之差）。"""
    for name in _EV_BATTERY_CATEGORY_NAMES:
        cur.execute(
            "SELECT category_id FROM dict_categories WHERE name = %s AND is_active = 1 LIMIT 1",
            (name,),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])
    return None


CODE_OK = 0
CODE_VALIDATION = 1001
CODE_NOT_FOUND = 1002
CODE_DUP_NAME = 1003
CODE_DUP_LINK = 1004
CODE_DB = 2001
CODE_INTERNAL = 5000

_HEX_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")


def _haversine_km(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
    """WGS84 两点球面距离（km）。"""
    r_km = 6371.0088
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r_km * c


def _parse_tier_price_spread(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return None
    if isinstance(raw, (dict, list)):
        return raw
    return raw


def _tier_price_spread_for_db(val: Any) -> Optional[str]:
    """写入 dict_warehouse_links.tier_price_spread（JSON 文本）。"""
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        json.loads(s)
        return s
    return json.dumps(val, ensure_ascii=False)


def _fmt_ts(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, str):
        return v
    return str(v)


def _ok(msg: str, data: Any = None) -> Dict[str, Any]:
    return {"code": CODE_OK, "msg": msg, "data": data}


def _err(code: int, msg: str, data: Any = None) -> Dict[str, Any]:
    return {"code": code, "msg": msg, "data": data}


def _color_to_config_json(color: Optional[str]) -> Optional[str]:
    if not color or not str(color).strip():
        return None
    c = str(color).strip()
    if not _HEX_RE.match(c):
        return None
    return json.dumps({"marker": c}, ensure_ascii=False)


def _hex_from_color_config(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, dict):
        h = val.get("marker") or val.get("hex")
        return str(h) if h else None
    if isinstance(val, (bytes, bytearray)):
        val = val.decode("utf-8")
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            d = json.loads(s)
            if isinstance(d, dict):
                h = d.get("marker") or d.get("hex")
                return str(h) if h else None
        except json.JSONDecodeError:
            return None
    return None


def _norm_cc_db(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        return json.loads(s)
    return None


def _warehouse_row_api(
    row: Dict[str, Any],
    type_name: Optional[str],
) -> Dict[str, Any]:
    cc = _norm_cc_db(row.get("color_config"))
    hw = row.get("hazardous_waste_license_qty")
    mar = row.get("monthly_avg_receipt_ton")
    cit = row.get("current_inventory_ton")
    rpp = row.get("receipt_price_per_ton")
    fa = row.get("freight_amount")
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "type": type_name or "",
        "province": row.get("province") or "",
        "city": row.get("city") or "",
        "district": row.get("district") or "",
        "address": row.get("address") or "",
        "color": _hex_from_color_config(cc),
        "longitude": float(row["longitude"]) if row.get("longitude") is not None else None,
        "latitude": float(row["latitude"]) if row.get("latitude") is not None else None,
        "contactName": row.get("contact_name") or "",
        "contactPhone": row.get("contact_phone") or "",
        "hazardousWasteLicenseQty": float(hw) if hw is not None else None,
        "monthlyAvgReceiptTon": float(mar) if mar is not None else None,
        "currentInventoryTon": float(cit) if cit is not None else None,
        "receiptPricePerTon": float(rpp) if rpp is not None else None,
        "freightAmount": float(fa) if fa is not None else None,
        "status": 1 if int(row.get("is_active", 1)) == 1 else 0,
        "createTime": _fmt_ts(row.get("created_at")),
        "updateTime": _fmt_ts(row.get("updated_at")),
    }


def _factory_row_api(row: Dict[str, Any]) -> Dict[str, Any]:
    """冶炼厂字典行序列化（不含颜色；比价侧不使用冶炼厂标记色）。"""
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "factory_type_id": row.get("factory_type_id"),
        "factory_type_name": row.get("factory_type_name") or "",
        "province": row.get("province") or "",
        "city": row.get("city") or "",
        "district": row.get("district") or "",
        "address": row.get("address") or "",
        "longitude": float(row["longitude"]) if row.get("longitude") is not None else None,
        "latitude": float(row["latitude"]) if row.get("latitude") is not None else None,
        "循融宝发货": bool(int(row.get("use_xunrongbao") or 0)),
        "status": 1 if int(row.get("is_active", 1)) == 1 else 0,
        "createTime": _fmt_ts(row.get("created_at")),
        "updateTime": _fmt_ts(row.get("updated_at")),
    }


def _lookup_warehouse_type_id(cur, type_name: str) -> Optional[int]:
    cur.execute(
        "SELECT id FROM dict_warehouse_types WHERE name = %s AND is_active = 1",
        (type_name.strip(),),
    )
    row = cur.fetchone()
    return int(row["id"]) if row else None


def _lookup_factory_type_id(cur, type_name: str) -> Optional[int]:
    cur.execute(
        "SELECT id FROM dict_factory_types WHERE name = %s AND is_active = 1",
        (type_name.strip(),),
    )
    row = cur.fetchone()
    return int(row["id"]) if row else None


def warehouse_create(payload: Dict[str, Any]) -> Dict[str, Any]:
    """新建仓库（完整行政区划 + 详细地址）：经纬度默认由天地图解析；仅在 payload 同时给出 longitude+latitude 时跳过天地图。"""
    try:
        name = str(payload.get("name") or "").strip()
        type_name = str(payload.get("type") or "").strip()
        province = str(payload.get("province") or "").strip()
        city = str(payload.get("city") or "").strip()
        district = str(payload.get("district") or "").strip()
        address = str(payload.get("address") or "").strip()
        color = payload.get("color")
        lon = payload.get("longitude")
        lat = payload.get("latitude")
        status = payload.get("status")
        if not name:
            return _err(CODE_VALIDATION, "仓库名称不能为空")
        if not type_name:
            return _err(CODE_VALIDATION, "库房类型 type 不能为空")
        if not province or not city or not district or not address:
            return _err(CODE_VALIDATION, "province、city、district、address 均为必填")
        if status is not None and int(status) not in (0, 1):
            return _err(CODE_VALIDATION, "status 须为 0 或 1")

        cc_json = None
        if color is not None and str(color).strip():
            cc_json = _color_to_config_json(str(color).strip())
            if cc_json is None:
                return _err(CODE_VALIDATION, "color 须为六位十六进制，如 #FF5733")

        try:
            lon_f = float(lon) if lon is not None else None
            lat_f = float(lat) if lat is not None else None
        except (TypeError, ValueError):
            return _err(CODE_VALIDATION, "longitude、latitude 格式无效")

        try:
            rx_lon, rx_lat = maybe_geocode(
                province, city, district, address,
                longitude=lon_f,
                latitude=lat_f,
            )
        except GeocoderError as e:
            return _err(CODE_VALIDATION, e.message)

        st = 1 if status is None else int(status)

        def _opt_float_val(v: Any) -> Optional[float]:
            if v is None or v == "":
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        cn = str(payload.get("contact_name") or "").strip() or None
        cp = str(payload.get("contact_phone") or "").strip() or None
        hq = _opt_float_val(payload.get("hazardous_waste_license_qty"))
        mar = _opt_float_val(payload.get("monthly_avg_receipt_ton"))
        cit = _opt_float_val(payload.get("current_inventory_ton"))
        rpp = _opt_float_val(payload.get("receipt_price_per_ton"))
        fa = _opt_float_val(payload.get("freight_amount"))

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                wt_id = _lookup_warehouse_type_id(cur, type_name)
                if wt_id is None:
                    return _err(CODE_VALIDATION, "库房类型不存在或未启用，请先维护库房类型")

                cur.execute(
                    "SELECT id FROM dict_warehouses WHERE name = %s",
                    (name,),
                )
                if cur.fetchone():
                    return _err(CODE_DUP_NAME, "仓库名称已存在")

                cat_id_val = payload.get("category_id")
                if cat_id_val is not None:
                    cat_id_val = int(cat_id_val)

                cur.execute(
                    "INSERT INTO dict_warehouses (name, province, city, district, address, "
                    "warehouse_type_id, category_id, color_config, longitude, latitude, "
                    "contact_name, contact_phone, hazardous_waste_license_qty, "
                    "monthly_avg_receipt_ton, current_inventory_ton, receipt_price_per_ton, "
                    "freight_amount, is_active) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        name,
                        province,
                        city,
                        district,
                        address,
                        wt_id,
                        cat_id_val,
                        cc_json,
                        rx_lon,
                        rx_lat,
                        cn,
                        cp,
                        hq,
                        mar,
                        cit,
                        rpp,
                        fa,
                        st,
                    ),
                )
                wid = cur.lastrowid
                conn.commit()

                cur.execute(
                    "SELECT dw.*, wt.name AS type_name FROM dict_warehouses dw "
                    "LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                    "WHERE dw.id = %s",
                    (wid,),
                )
                row = cur.fetchone()
        data = _warehouse_row_api(row, row.get("type_name"))
        return _ok("创建成功", data=data)
    except pymysql.IntegrityError:
        return _err(CODE_DUP_NAME, "仓库名称已存在")
    except Exception as e:
        logger.exception("创建仓库失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_delete(wh_id: int) -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT id FROM dict_warehouses WHERE id = %s",
                    (wh_id,),
                )
                if not cur.fetchone():
                    return _err(CODE_NOT_FOUND, "仓库不存在")
                cur.execute(
                    "UPDATE dict_warehouses SET is_active = 0 WHERE id = %s",
                    (wh_id,),
                )
                cur.execute(
                    "DELETE FROM dict_warehouse_links WHERE from_warehouse_id = %s "
                    "OR to_warehouse_id = %s",
                    (wh_id, wh_id),
                )
            conn.commit()
        return _ok("删除成功", data=None)
    except Exception as e:
        logger.exception("删除仓库失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_update(wh_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
    """更新仓库；未同时手传经纬度时若改了省/市/区/地址则重新天地图解析坐标。"""
    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT dw.*, wt.name AS type_name FROM dict_warehouses dw "
                    "LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                    "WHERE dw.id = %s",
                    (wh_id,),
                )
                row = cur.fetchone()
                if not row:
                    return _err(CODE_NOT_FOUND, "仓库不存在")

                name = patch.get("name")
                province = patch.get("province")
                city = patch.get("city")
                district = patch.get("district")
                address = patch.get("address")
                color = patch.get("color")
                status = patch.get("status")
                lon_p = patch.get("longitude")
                lat_p = patch.get("latitude")

                n = str(name).strip() if name is not None else row["name"]
                p = str(province).strip() if province is not None else (row.get("province") or "")
                c = str(city).strip() if city is not None else (row.get("city") or "")
                d = str(district).strip() if district is not None else (row.get("district") or "")
                a = str(address).strip() if address is not None else (row.get("address") or "")

                updates: List[str] = []
                params: List[Any] = []

                if name is not None:
                    if not n:
                        return _err(CODE_VALIDATION, "仓库名称不能为空")
                    cur.execute(
                        "SELECT id FROM dict_warehouses WHERE name = %s AND id <> %s",
                        (n, wh_id),
                    )
                    if cur.fetchone():
                        return _err(CODE_DUP_NAME, "仓库名称已存在")
                    updates.append("name = %s")
                    params.append(n)

                if "type" in patch:
                    t_raw = patch.get("type")
                    if t_raw is None or (
                        isinstance(t_raw, str) and not str(t_raw).strip()
                    ):
                        updates.append("warehouse_type_id = NULL")
                    else:
                        tns = str(t_raw).strip()
                        new_wt_id = _lookup_warehouse_type_id(cur, tns)
                        if new_wt_id is None:
                            return _err(CODE_VALIDATION, "库房类型不存在或未启用")
                        updates.append("warehouse_type_id = %s")
                        params.append(new_wt_id)

                if "category_id" in patch:
                    cat_p = patch.get("category_id")
                    if cat_p is None:
                        updates.append("category_id = NULL")
                    else:
                        updates.append("category_id = %s")
                        params.append(int(cat_p))

                if province is not None:
                    updates.append("province = %s")
                    params.append(p)
                if city is not None:
                    updates.append("city = %s")
                    params.append(c)
                if district is not None:
                    updates.append("district = %s")
                    params.append(d)
                if address is not None:
                    updates.append("address = %s")
                    params.append(a)

                if color is not None:
                    if str(color).strip() == "":
                        updates.append("color_config = NULL")
                    else:
                        cj = _color_to_config_json(str(color).strip())
                        if cj is None:
                            return _err(CODE_VALIDATION, "color 须为六位十六进制，如 #FF5733")
                        updates.append("color_config = %s")
                        params.append(cj)

                if status is not None:
                    if int(status) not in (0, 1):
                        return _err(CODE_VALIDATION, "status 须为 0 或 1")
                    updates.append("is_active = %s")
                    params.append(1 if int(status) == 1 else 0)

                if "contact_name" in patch:
                    cv = patch.get("contact_name")
                    if cv is None or str(cv).strip() == "":
                        updates.append("contact_name = NULL")
                    else:
                        updates.append("contact_name = %s")
                        params.append(str(cv).strip())
                if "contact_phone" in patch:
                    cv = patch.get("contact_phone")
                    if cv is None or str(cv).strip() == "":
                        updates.append("contact_phone = NULL")
                    else:
                        updates.append("contact_phone = %s")
                        params.append(str(cv).strip())
                for _k, _col in (
                    ("hazardous_waste_license_qty", "hazardous_waste_license_qty"),
                    ("monthly_avg_receipt_ton", "monthly_avg_receipt_ton"),
                    ("current_inventory_ton", "current_inventory_ton"),
                    ("receipt_price_per_ton", "receipt_price_per_ton"),
                    ("freight_amount", "freight_amount"),
                ):
                    if _k not in patch:
                        continue
                    v = patch.get(_k)
                    if v is None or v == "":
                        updates.append(f"{_col} = NULL")
                    else:
                        try:
                            updates.append(f"{_col} = %s")
                            params.append(float(v))
                        except (TypeError, ValueError):
                            return _err(
                                CODE_VALIDATION,
                                f"{_k} 格式无效",
                            )

                has_lon = "longitude" in patch
                has_lat = "latitude" in patch
                if has_lon or has_lat:
                    if not (has_lon and has_lat):
                        return _err(CODE_VALIDATION, "经度与纬度须同时提供")
                    try:
                        lon_v = float(lon_p)
                        lat_v = float(lat_p)
                    except (TypeError, ValueError):
                        return _err(CODE_VALIDATION, "longitude、latitude 格式无效")
                    if not (-180.0 <= lon_v <= 180.0 and -90.0 <= lat_v <= 90.0):
                        return _err(CODE_VALIDATION, "经纬度超出允许范围")
                    updates.append("longitude = %s")
                    updates.append("latitude = %s")
                    params.extend([lon_v, lat_v])
                elif any(k in patch for k in ("province", "city", "district", "address")):
                    try:
                        rx_lon, rx_lat = maybe_geocode(p, c, d, a, longitude=None, latitude=None)
                    except GeocoderError as e:
                        return _err(CODE_VALIDATION, e.message)
                    updates.append("longitude = %s")
                    updates.append("latitude = %s")
                    params.extend([rx_lon, rx_lat])

                if not updates:
                    cur.execute(
                        "SELECT dw.*, wt.name AS type_name FROM dict_warehouses dw "
                        "LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                        "WHERE dw.id = %s",
                        (wh_id,),
                    )
                    nrow = cur.fetchone()
                    return _ok(
                        "修改成功",
                        data=_warehouse_row_api(nrow, nrow.get("type_name")),
                    )

                params.append(wh_id)
                cur.execute(
                    f"UPDATE dict_warehouses SET {', '.join(updates)} WHERE id = %s",
                    tuple(params),
                )
                conn.commit()

                cur.execute(
                    "SELECT dw.*, wt.name AS type_name FROM dict_warehouses dw "
                    "LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                    "WHERE dw.id = %s",
                    (wh_id,),
                )
                urow = cur.fetchone()
        return _ok("修改成功", data=_warehouse_row_api(urow, urow.get("type_name")))
    except pymysql.IntegrityError:
        return _err(CODE_DUP_NAME, "仓库名称已存在")
    except Exception as e:
        logger.exception("修改仓库失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_list(
    page: int,
    size: int,
    name: Optional[str] = None,
    type_: Optional[str] = None,
    province: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    status: Optional[int] = None,
) -> Dict[str, Any]:
    try:
        page = max(1, page)
        size = min(200, max(1, size))
        offset = (page - 1) * size

        conds: List[str] = ["1=1"]
        params: List[Any] = []

        if name is not None and str(name).strip():
            conds.append("dw.name LIKE %s")
            params.append(f"%{str(name).strip()}%")
        if type_ is not None and str(type_).strip():
            conds.append("wt.name = %s")
            params.append(str(type_).strip())
        if province is not None and str(province).strip():
            conds.append("dw.province = %s")
            params.append(str(province).strip())
        if city is not None and str(city).strip():
            conds.append("dw.city = %s")
            params.append(str(city).strip())
        if district is not None and str(district).strip():
            conds.append("dw.district = %s")
            params.append(str(district).strip())
        if status is not None:
            conds.append("dw.is_active = %s")
            params.append(1 if int(status) == 1 else 0)

        where_sql = " AND ".join(conds)

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    f"SELECT COUNT(*) AS n FROM dict_warehouses dw "
                    f"LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                    f"WHERE {where_sql}",
                    tuple(params),
                )
                total = int(cur.fetchone()["n"])

                cur.execute(
                    f"SELECT dw.*, wt.name AS type_name FROM dict_warehouses dw "
                    f"LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                    f"WHERE {where_sql} ORDER BY dw.id DESC LIMIT %s OFFSET %s",
                    tuple(params + [size, offset]),
                )
                rows = cur.fetchall()

        items = [_warehouse_row_api(r, r.get("type_name")) for r in rows]
        return _ok(
            "查询成功",
            data={"list": items, "total": total, "page": page, "size": size},
        )
    except Exception as e:
        logger.exception("查询仓库列表失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_get(wh_id: int) -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT dw.*, wt.name AS type_name FROM dict_warehouses dw "
                    "LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                    "WHERE dw.id = %s",
                    (wh_id,),
                )
                row = cur.fetchone()
        if not row:
            return _err(CODE_NOT_FOUND, "仓库不存在")
        return _ok("查询成功", data=_warehouse_row_api(row, row.get("type_name")))
    except Exception as e:
        logger.exception("查询仓库详情失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def _link_out_item(
    link_id: int,
    from_id: int,
    to_id: int,
    created_at: Any,
    target_row: Dict[str, Any],
    target_type_name: Optional[str],
    tier_raw: Any = None,
    from_longitude: Any = None,
    from_latitude: Any = None,
) -> Dict[str, Any]:
    """出库关联列表单项：指向对标库房的边（含球面距离 km 与阶梯价差）。"""
    tw = _warehouse_row_api(target_row, target_type_name)
    dist_km: Optional[float] = None
    try:
        fln = float(from_longitude) if from_longitude is not None else None
        fla = float(from_latitude) if from_latitude is not None else None
        tln = tw.get("longitude")
        tla = tw.get("latitude")
        if (
            fln is not None
            and fla is not None
            and tln is not None
            and tla is not None
        ):
            dist_km = round(
                _haversine_km(fln, fla, float(tln), float(tla)),
                3,
            )
    except (TypeError, ValueError):
        dist_km = None
    return {
        "linkId": int(link_id),
        "fromWarehouseId": int(from_id),
        "toWarehouseId": int(to_id),
        "createTime": _fmt_ts(created_at),
        "distanceKm": dist_km,
        "tierPriceSpread": _parse_tier_price_spread(tier_raw),
        "remark": target_row.get("remark") or "",
        "aiAnalysis": target_row.get("ai_analysis") or "",
        "lastAnalysisAt": _fmt_ts(target_row.get("last_analysis_at")),
        "target": tw,
    }


def _link_in_item(
    link_id: int,
    from_id: int,
    to_id: int,
    created_at: Any,
    source_row: Dict[str, Any],
    source_type_name: Optional[str],
    tier_raw: Any = None,
    to_longitude: Any = None,
    to_latitude: Any = None,
) -> Dict[str, Any]:
    """入库关联列表单项：来自源库房的边（含与终点库房间的球面距离与阶梯价差）。"""
    sw = _warehouse_row_api(source_row, source_type_name)
    dist_km: Optional[float] = None
    try:
        sln = sw.get("longitude")
        sla = sw.get("latitude")
        tln = float(to_longitude) if to_longitude is not None else None
        tla = float(to_latitude) if to_latitude is not None else None
        if (
            sln is not None
            and sla is not None
            and tln is not None
            and tla is not None
        ):
            dist_km = round(
                _haversine_km(float(sln), float(sla), tln, tla),
                3,
            )
    except (TypeError, ValueError):
        dist_km = None
    return {
        "linkId": int(link_id),
        "fromWarehouseId": int(from_id),
        "toWarehouseId": int(to_id),
        "createTime": _fmt_ts(created_at),
        "distanceKm": dist_km,
        "tierPriceSpread": _parse_tier_price_spread(tier_raw),
        "remark": source_row.get("remark") or "",
        "aiAnalysis": source_row.get("ai_analysis") or "",
        "lastAnalysisAt": _fmt_ts(source_row.get("last_analysis_at")),
        "source": sw,
    }


def warehouse_link_bind(
    from_wh_id: int,
    to_wh_id: int,
    tier_price_spread: Any = None,
) -> Dict[str, Any]:
    """新增一条有向边 from -> 对标库房 to（幂等重复边返回 CODE_DUP_LINK）。"""
    try:
        if from_wh_id < 1 or to_wh_id < 1:
            return _err(CODE_VALIDATION, "库房 id 无效")
        if from_wh_id == to_wh_id:
            return _err(CODE_VALIDATION, "不能将库房关联到自身")

        try:
            tier_json = _tier_price_spread_for_db(tier_price_spread)
        except (json.JSONDecodeError, TypeError, ValueError):
            return _err(CODE_VALIDATION, "阶梯价差格式无效")

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT id FROM dict_warehouses WHERE id IN (%s,%s)",
                    (from_wh_id, to_wh_id),
                )
                found = {int(r["id"]) for r in cur.fetchall()}
                if from_wh_id not in found:
                    return _err(CODE_NOT_FOUND, "源库房不存在")
                if to_wh_id not in found:
                    return _err(CODE_NOT_FOUND, "对标库房不存在")

                cur.execute(
                    "SELECT id FROM dict_warehouse_links "
                    "WHERE from_warehouse_id = %s AND to_warehouse_id = %s",
                    (from_wh_id, to_wh_id),
                )
                if cur.fetchone():
                    return _err(CODE_DUP_LINK, "该单向关联已存在")

                cur.execute(
                    "INSERT INTO dict_warehouse_links "
                    "(from_warehouse_id, to_warehouse_id, tier_price_spread) "
                    "VALUES (%s,%s,%s)",
                    (from_wh_id, to_wh_id, tier_json),
                )
                lid = cur.lastrowid
                conn.commit()

                cur.execute(
                    "SELECT id, from_warehouse_id, to_warehouse_id, created_at, tier_price_spread "
                    "FROM dict_warehouse_links WHERE id = %s",
                    (lid,),
                )
                lk = cur.fetchone()
        return _ok(
            "绑定成功",
            data={
                "linkId": int(lk["id"]),
                "fromWarehouseId": int(lk["from_warehouse_id"]),
                "toWarehouseId": int(lk["to_warehouse_id"]),
                "createTime": _fmt_ts(lk.get("created_at")),
                "tierPriceSpread": _parse_tier_price_spread(lk.get("tier_price_spread")),
            },
        )
    except pymysql.IntegrityError:
        return _err(CODE_DUP_LINK, "该单向关联已存在")
    except Exception as e:
        logger.exception("库房关联绑定失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_link_unbind(from_wh_id: int, to_wh_id: int) -> Dict[str, Any]:
    """删除有向边 from -> to。"""
    try:
        if from_wh_id < 1 or to_wh_id < 1:
            return _err(CODE_VALIDATION, "库房 id 无效")

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "DELETE FROM dict_warehouse_links WHERE from_warehouse_id = %s "
                    "AND to_warehouse_id = %s",
                    (from_wh_id, to_wh_id),
                )
                deleted = cur.rowcount
                conn.commit()
        if deleted == 0:
            return _err(CODE_NOT_FOUND, "关联不存在")
        return _ok("解绑成功", data=None)
    except Exception as e:
        logger.exception("库房关联解绑失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_link_update_tier_price_spread(
    from_wh_id: int,
    to_wh_id: int,
    tier_price_spread: Any,
) -> Dict[str, Any]:
    """修改已有边上的阶梯价差；传 null 可清空。"""
    try:
        if from_wh_id < 1 or to_wh_id < 1:
            return _err(CODE_VALIDATION, "库房 id 无效")
        try:
            tier_json = _tier_price_spread_for_db(tier_price_spread)
        except (json.JSONDecodeError, TypeError, ValueError):
            return _err(CODE_VALIDATION, "阶梯价差格式无效")

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "UPDATE dict_warehouse_links SET tier_price_spread = %s "
                    "WHERE from_warehouse_id = %s AND to_warehouse_id = %s",
                    (tier_json, from_wh_id, to_wh_id),
                )
                if cur.rowcount == 0:
                    return _err(CODE_NOT_FOUND, "关联不存在")
                conn.commit()
                cur.execute(
                    "SELECT tier_price_spread FROM dict_warehouse_links "
                    "WHERE from_warehouse_id = %s AND to_warehouse_id = %s",
                    (from_wh_id, to_wh_id),
                )
                row = cur.fetchone()
        return _ok(
            "修改成功",
            data={
                "fromWarehouseId": from_wh_id,
                "toWarehouseId": to_wh_id,
                "tierPriceSpread": _parse_tier_price_spread(
                    row.get("tier_price_spread") if row else None
                ),
            },
        )
    except Exception as e:
        logger.exception("修改库房关联阶梯价差失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_link_update_remark(
    from_wh_id: int,
    to_wh_id: int,
    remark: Optional[str],
) -> Dict[str, Any]:
    """修改库房关联边的备注。"""
    try:
        if from_wh_id < 1 or to_wh_id < 1:
            return _err(CODE_VALIDATION, "库房 id 无效")

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "UPDATE dict_warehouse_links SET remark = %s "
                    "WHERE from_warehouse_id = %s AND to_warehouse_id = %s",
                    (remark or None, from_wh_id, to_wh_id),
                )
                if cur.rowcount == 0:
                    return _err(CODE_NOT_FOUND, "关联不存在")
                conn.commit()
        return _ok("备注修改成功", data={"fromWarehouseId": from_wh_id, "toWarehouseId": to_wh_id, "remark": remark or ""})
    except Exception as e:
        logger.exception("修改库房关联备注失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_link_update_ai_analysis(
    link_id: int,
    ai_analysis: str,
) -> Dict[str, Any]:
    """更新单条库房关联边的 AI 分析结果（同时更新分析时间）。"""
    try:
        if link_id < 1:
            return _err(CODE_VALIDATION, "关联 id 无效")

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "UPDATE dict_warehouse_links SET ai_analysis = %s, last_analysis_at = NOW() "
                    "WHERE id = %s",
                    (ai_analysis, link_id),
                )
                if cur.rowcount == 0:
                    return _err(CODE_NOT_FOUND, "关联不存在")
                conn.commit()
        return _ok("AI分析已更新", data={"linkId": link_id})
    except Exception as e:
        logger.exception("更新库房关联AI分析失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_links_all_active() -> List[Dict[str, Any]]:
    """获取全部活跃库房关联边（含源/对标库房摘要），供 AI 分析定时任务使用。"""
    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT l.id AS link_id, l.from_warehouse_id, l.to_warehouse_id, "
                    "l.tier_price_spread, l.remark, l.ai_analysis, l.last_analysis_at, "
                    "wf.id AS sf_id, wf.name AS sf_name, wf.province AS sf_province, "
                    "wf.city AS sf_city, wf.district AS sf_district, "
                    "wf.longitude AS sf_longitude, wf.latitude AS sf_latitude, "
                    "wf.receipt_price_per_ton AS sf_price_per_ton, "
                    "wt.id AS st_id, wt.name AS st_name, wt.province AS st_province, "
                    "wt.city AS st_city, wt.district AS st_district, "
                    "wt.longitude AS st_longitude, wt.latitude AS st_latitude, "
                    "wt.receipt_price_per_ton AS st_price_per_ton "
                    "FROM dict_warehouse_links l "
                    "JOIN dict_warehouses wf ON wf.id = l.from_warehouse_id AND wf.is_active = 1 "
                    "JOIN dict_warehouses wt ON wt.id = l.to_warehouse_id AND wt.is_active = 1 "
                    "ORDER BY l.id"
                )
                return cur.fetchall()
    except Exception as e:
        logger.exception("查询全部库房关联边失败")
        return []


def warehouse_links_outbound(
    warehouse_id: int,
    page: int = 1,
    size: int = 50,
) -> Dict[str, Any]:
    """某库房的所有出边（指向哪些库房）。"""
    try:
        if warehouse_id < 1:
            return _err(CODE_VALIDATION, "库房 id 无效")
        page = max(1, page)
        size = min(200, max(1, size))
        offset = (page - 1) * size

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM dict_warehouse_links WHERE from_warehouse_id = %s",
                    (warehouse_id,),
                )
                total = int(cur.fetchone()["n"])

                cur.execute(
                    "SELECT l.id AS link_id, l.from_warehouse_id, l.to_warehouse_id, "
                    "l.created_at AS link_created_at, l.tier_price_spread AS tier_price_spread, "
                    "l.remark, l.ai_analysis, l.last_analysis_at, "
                    "dwf.longitude AS from_longitude, dwf.latitude AS from_latitude, "
                    "dw.id AS wh_id, dw.name AS wh_name, dw.province, dw.city, dw.district, "
                    "dw.address, dw.warehouse_type_id, dw.color_config, "
                    "dw.longitude, dw.latitude, dw.is_active, dw.created_at, dw.updated_at, "
                    "dw.contact_name, dw.contact_phone, dw.hazardous_waste_license_qty, "
                    "dw.monthly_avg_receipt_ton, dw.freight_amount, "
                    "wt.name AS type_name "
                    "FROM dict_warehouse_links l "
                    "JOIN dict_warehouses dwf ON dwf.id = l.from_warehouse_id "
                    "JOIN dict_warehouses dw ON dw.id = l.to_warehouse_id "
                    "LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                    "WHERE l.from_warehouse_id = %s "
                    "ORDER BY l.id DESC LIMIT %s OFFSET %s",
                    (warehouse_id, size, offset),
                )
                rows = cur.fetchall()

        items = []
        for r in rows:
            tw = {
                "id": r["wh_id"],
                "name": r["wh_name"],
                "province": r.get("province"),
                "city": r.get("city"),
                "district": r.get("district"),
                "address": r.get("address"),
                "warehouse_type_id": r.get("warehouse_type_id"),
                "color_config": r.get("color_config"),
                "longitude": r.get("longitude"),
                "latitude": r.get("latitude"),
                "is_active": r.get("is_active"),
                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
                "contact_name": r.get("contact_name"),
                "contact_phone": r.get("contact_phone"),
                "hazardous_waste_license_qty": r.get("hazardous_waste_license_qty"),
                "monthly_avg_receipt_ton": r.get("monthly_avg_receipt_ton"),
                "freight_amount": r.get("freight_amount"),
                "remark": r.get("remark") or "",
                "ai_analysis": r.get("ai_analysis") or "",
                "last_analysis_at": r.get("last_analysis_at"),
            }
            items.append(
                _link_out_item(
                    int(r["link_id"]),
                    int(r["from_warehouse_id"]),
                    int(r["to_warehouse_id"]),
                    r.get("link_created_at"),
                    tw,
                    r.get("type_name"),
                    tier_raw=r.get("tier_price_spread"),
                    from_longitude=r.get("from_longitude"),
                    from_latitude=r.get("from_latitude"),
                )
            )

        return _ok(
            "查询成功",
            data={"list": items, "total": total, "page": page, "size": size},
        )
    except Exception as e:
        logger.exception("查询库房出边列表失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_links_inbound(
    warehouse_id: int,
    page: int = 1,
    size: int = 50,
) -> Dict[str, Any]:
    """某库房的所有入边（被哪些库房指向）。"""
    try:
        if warehouse_id < 1:
            return _err(CODE_VALIDATION, "库房 id 无效")
        page = max(1, page)
        size = min(200, max(1, size))
        offset = (page - 1) * size

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM dict_warehouse_links WHERE to_warehouse_id = %s",
                    (warehouse_id,),
                )
                total = int(cur.fetchone()["n"])

                cur.execute(
                    "SELECT l.id AS link_id, l.from_warehouse_id, l.to_warehouse_id, "
                    "l.created_at AS link_created_at, l.tier_price_spread AS tier_price_spread, "
                    "l.remark, l.ai_analysis, l.last_analysis_at, "
                    "dw_to.longitude AS to_longitude, dw_to.latitude AS to_latitude, "
                    "dw.id AS wh_id, dw.name AS wh_name, dw.province, dw.city, dw.district, "
                    "dw.address, dw.warehouse_type_id, dw.color_config, "
                    "dw.longitude, dw.latitude, dw.is_active, dw.created_at, dw.updated_at, "
                    "dw.contact_name, dw.contact_phone, dw.hazardous_waste_license_qty, "
                    "dw.monthly_avg_receipt_ton, dw.freight_amount, "
                    "wt.name AS type_name "
                    "FROM dict_warehouse_links l "
                    "JOIN dict_warehouses dw ON dw.id = l.from_warehouse_id "
                    "JOIN dict_warehouses dw_to ON dw_to.id = l.to_warehouse_id "
                    "LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                    "WHERE l.to_warehouse_id = %s "
                    "ORDER BY l.id DESC LIMIT %s OFFSET %s",
                    (warehouse_id, size, offset),
                )
                rows = cur.fetchall()

        items = []
        for r in rows:
            sw = {
                "id": r["wh_id"],
                "name": r["wh_name"],
                "province": r.get("province"),
                "city": r.get("city"),
                "district": r.get("district"),
                "address": r.get("address"),
                "warehouse_type_id": r.get("warehouse_type_id"),
                "color_config": r.get("color_config"),
                "longitude": r.get("longitude"),
                "latitude": r.get("latitude"),
                "is_active": r.get("is_active"),
                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
                "contact_name": r.get("contact_name"),
                "contact_phone": r.get("contact_phone"),
                "hazardous_waste_license_qty": r.get("hazardous_waste_license_qty"),
                "monthly_avg_receipt_ton": r.get("monthly_avg_receipt_ton"),
                "freight_amount": r.get("freight_amount"),
                "remark": r.get("remark") or "",
                "ai_analysis": r.get("ai_analysis") or "",
                "last_analysis_at": r.get("last_analysis_at"),
            }
            items.append(
                _link_in_item(
                    int(r["link_id"]),
                    int(r["from_warehouse_id"]),
                    int(r["to_warehouse_id"]),
                    r.get("link_created_at"),
                    sw,
                    r.get("type_name"),
                    tier_raw=r.get("tier_price_spread"),
                    to_longitude=r.get("to_longitude"),
                    to_latitude=r.get("to_latitude"),
                )
            )
        return _ok(
            "查询成功",
            data={"list": items, "total": total, "page": page, "size": size},
        )
    except Exception as e:
        logger.exception("查询库房入边列表失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def _wh_side_row(r: Dict[str, Any], side: str) -> Dict[str, Any]:
    """side 为 sf（源/from）或 st（目标/to），与 SELECT 别名一致。"""
    return {
        "id": r[f"{side}_id"],
        "name": r[f"{side}_name"],
        "province": r.get(f"{side}_province"),
        "city": r.get(f"{side}_city"),
        "district": r.get(f"{side}_district"),
        "address": r.get(f"{side}_address"),
        "warehouse_type_id": r.get(f"{side}_warehouse_type_id"),
        "color_config": r.get(f"{side}_color_config"),
        "longitude": r.get(f"{side}_longitude"),
        "latitude": r.get(f"{side}_latitude"),
        "is_active": r.get(f"{side}_is_active"),
        "created_at": r.get(f"{side}_created_at"),
        "updated_at": r.get(f"{side}_updated_at"),
        "contact_name": r.get(f"{side}_contact_name"),
        "contact_phone": r.get(f"{side}_contact_phone"),
        "hazardous_waste_license_qty": r.get(f"{side}_hazardous_waste_license_qty"),
        "monthly_avg_receipt_ton": r.get(f"{side}_monthly_avg_receipt_ton"),
        "freight_amount": r.get(f"{side}_freight_amount"),
    }


def warehouse_links_list_all(
    page: int = 1,
    size: int = 50,
    warehouse_id: Optional[int] = None,
    from_warehouse_id: Optional[int] = None,
    to_warehouse_id: Optional[int] = None,
    keyword: Optional[str] = None,
    has_tier_price_spread: Optional[bool] = None,
) -> Dict[str, Any]:
    """全部库房关联（有向边）分页列表；可选筛选涉及库房、精确源/目标、名称模糊、是否已配置阶梯差价。"""
    try:
        page = max(1, page)
        size = min(200, max(1, size))
        offset = (page - 1) * size

        conds: List[str] = ["1=1"]
        params: List[Any] = []

        if has_tier_price_spread is True:
            conds.append("l.tier_price_spread IS NOT NULL")
        elif has_tier_price_spread is False:
            conds.append("l.tier_price_spread IS NULL")

        if warehouse_id is not None and int(warehouse_id) >= 1:
            wid = int(warehouse_id)
            conds.append(
                "(l.from_warehouse_id = %s OR l.to_warehouse_id = %s)"
            )
            params.extend([wid, wid])

        if from_warehouse_id is not None and int(from_warehouse_id) >= 1:
            conds.append("l.from_warehouse_id = %s")
            params.append(int(from_warehouse_id))

        if to_warehouse_id is not None and int(to_warehouse_id) >= 1:
            conds.append("l.to_warehouse_id = %s")
            params.append(int(to_warehouse_id))

        if keyword is not None and str(keyword).strip():
            conds.append("(wf.name LIKE %s OR wt.name LIKE %s)")
            k = f"%{str(keyword).strip()}%"
            params.extend([k, k])

        where_sql = " AND ".join(conds)

        base_from = (
            "FROM dict_warehouse_links l "
            "INNER JOIN dict_warehouses wf ON wf.id = l.from_warehouse_id "
            "INNER JOIN dict_warehouses wt ON wt.id = l.to_warehouse_id "
            "LEFT JOIN dict_warehouse_types wfs ON wf.warehouse_type_id = wfs.id "
            "LEFT JOIN dict_warehouse_types wts ON wt.warehouse_type_id = wts.id "
            f"WHERE {where_sql}"
        )

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(f"SELECT COUNT(*) AS n {base_from}", tuple(params))
                total = int(cur.fetchone()["n"])

                cur.execute(
                    "SELECT l.id AS link_id, l.created_at AS link_created_at, "
                    "l.tier_price_spread AS tier_price_spread, "
                    "l.remark, l.ai_analysis, l.last_analysis_at, "
                    "l.from_warehouse_id, l.to_warehouse_id, "
                    "wf.id AS sf_id, wf.name AS sf_name, wf.province AS sf_province, "
                    "wf.city AS sf_city, wf.district AS sf_district, wf.address AS sf_address, "
                    "wf.warehouse_type_id AS sf_warehouse_type_id, wf.color_config AS sf_color_config, "
                    "wf.longitude AS sf_longitude, wf.latitude AS sf_latitude, wf.is_active AS sf_is_active, "
                    "wf.contact_name AS sf_contact_name, wf.contact_phone AS sf_contact_phone, "
                    "wf.hazardous_waste_license_qty AS sf_hazardous_waste_license_qty, "
                    "wf.monthly_avg_receipt_ton AS sf_monthly_avg_receipt_ton, "
                    "wf.freight_amount AS sf_freight_amount, "
                    "wf.created_at AS sf_created_at, wf.updated_at AS sf_updated_at, "
                    "wfs.name AS sf_type_name, "
                    "wt.id AS st_id, wt.name AS st_name, wt.province AS st_province, "
                    "wt.city AS st_city, wt.district AS st_district, wt.address AS st_address, "
                    "wt.warehouse_type_id AS st_warehouse_type_id, wt.color_config AS st_color_config, "
                    "wt.longitude AS st_longitude, wt.latitude AS st_latitude, wt.is_active AS st_is_active, "
                    "wt.contact_name AS st_contact_name, wt.contact_phone AS st_contact_phone, "
                    "wt.hazardous_waste_license_qty AS st_hazardous_waste_license_qty, "
                    "wt.monthly_avg_receipt_ton AS st_monthly_avg_receipt_ton, "
                    "wt.freight_amount AS st_freight_amount, "
                    "wt.created_at AS st_created_at, wt.updated_at AS st_updated_at, "
                    "wts.name AS st_type_name "
                    f"{base_from} "
                    "ORDER BY l.id DESC LIMIT %s OFFSET %s",
                    tuple(params + [size, offset]),
                )
                rows = cur.fetchall()

        items: List[Dict[str, Any]] = []
        for r in rows:
            dist_km: Optional[float] = None
            try:
                sln = r.get("sf_longitude")
                sla = r.get("sf_latitude")
                tln = r.get("st_longitude")
                tla = r.get("st_latitude")
                if (
                    sln is not None
                    and sla is not None
                    and tln is not None
                    and tla is not None
                ):
                    dist_km = round(
                        _haversine_km(
                            float(sln),
                            float(sla),
                            float(tln),
                            float(tla),
                        ),
                        3,
                    )
            except (TypeError, ValueError):
                dist_km = None
            items.append(
                {
                    "linkId": int(r["link_id"]),
                    "fromWarehouseId": int(r["from_warehouse_id"]),
                    "toWarehouseId": int(r["to_warehouse_id"]),
                    "createTime": _fmt_ts(r.get("link_created_at")),
                    "distanceKm": dist_km,
                    "tierPriceSpread": _parse_tier_price_spread(r.get("tier_price_spread")),
                    "remark": r.get("remark") or "",
                    "aiAnalysis": r.get("ai_analysis") or "",
                    "lastAnalysisAt": _fmt_ts(r.get("last_analysis_at")),
                    "source": _warehouse_row_api(
                        _wh_side_row(r, "sf"),
                        r.get("sf_type_name"),
                    ),
                    "target": _warehouse_row_api(
                        _wh_side_row(r, "st"),
                        r.get("st_type_name"),
                    ),
                }
            )

        return _ok(
            "查询成功",
            data={"list": items, "total": total, "page": page, "size": size},
        )
    except Exception as e:
        logger.exception("查询库房关联列表失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_links_realtime_spread_list(
    page: int = 1,
    size: int = 50,
    warehouse_id: Optional[int] = None,
    from_warehouse_id: Optional[int] = None,
    to_warehouse_id: Optional[int] = None,
    keyword: Optional[str] = None,
    has_realtime_spread: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    库房关联实时价差列表：每条有向边返回源/对标库房电动车电瓶收货价及实时价差（源−对标）。
    价格来自 warehouse_category_receipt_prices（品类=电动车电瓶）。
    """
    try:
        page = max(1, page)
        size = min(200, max(1, size))
        offset = (page - 1) * size

        with get_conn() as conn:
            with conn.cursor() as cur:
                ev_cat_id = resolve_ev_battery_category_id(cur)

        conds: List[str] = ["1=1"]
        params: List[Any] = []

        if has_realtime_spread is True:
            conds.append(
                "wcrp_f.price_per_ton IS NOT NULL AND wcrp_t.price_per_ton IS NOT NULL"
            )
        elif has_realtime_spread is False:
            conds.append(
                "(wcrp_f.price_per_ton IS NULL OR wcrp_t.price_per_ton IS NULL)"
            )

        if warehouse_id is not None and int(warehouse_id) >= 1:
            wid = int(warehouse_id)
            conds.append("(l.from_warehouse_id = %s OR l.to_warehouse_id = %s)")
            params.extend([wid, wid])

        if from_warehouse_id is not None and int(from_warehouse_id) >= 1:
            conds.append("l.from_warehouse_id = %s")
            params.append(int(from_warehouse_id))

        if to_warehouse_id is not None and int(to_warehouse_id) >= 1:
            conds.append("l.to_warehouse_id = %s")
            params.append(int(to_warehouse_id))

        if keyword is not None and str(keyword).strip():
            conds.append("(wf.name LIKE %s OR wt.name LIKE %s)")
            k = f"%{str(keyword).strip()}%"
            params.extend([k, k])

        where_sql = " AND ".join(conds)
        receipt_join = ""
        if ev_cat_id is not None:
            receipt_join = (
                "LEFT JOIN warehouse_category_receipt_prices wcrp_f "
                "ON wcrp_f.warehouse_id = l.from_warehouse_id "
                f"AND wcrp_f.category_id = {int(ev_cat_id)} "
                "LEFT JOIN warehouse_category_receipt_prices wcrp_t "
                "ON wcrp_t.warehouse_id = l.to_warehouse_id "
                f"AND wcrp_t.category_id = {int(ev_cat_id)} "
            )
        else:
            receipt_join = (
                "LEFT JOIN warehouse_category_receipt_prices wcrp_f "
                "ON 1=0 LEFT JOIN warehouse_category_receipt_prices wcrp_t ON 1=0 "
            )
        base_from = (
            "FROM dict_warehouse_links l "
            "INNER JOIN dict_warehouses wf ON wf.id = l.from_warehouse_id "
            "INNER JOIN dict_warehouses wt ON wt.id = l.to_warehouse_id "
            "LEFT JOIN dict_warehouse_types wfs ON wf.warehouse_type_id = wfs.id "
            "LEFT JOIN dict_warehouse_types wts ON wt.warehouse_type_id = wts.id "
            f"{receipt_join}"
            f"WHERE {where_sql}"
        )

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(f"SELECT COUNT(*) AS n {base_from}", tuple(params))
                total = int(cur.fetchone()["n"])

                cur.execute(
                    "SELECT l.id AS link_id, l.created_at AS link_created_at, "
                    "l.tier_price_spread AS tier_price_spread, "
                    "l.remark, l.ai_analysis, l.last_analysis_at, "
                    "l.from_warehouse_id, l.to_warehouse_id, "
                    "wcrp_f.price_per_ton AS from_warehouse_price, "
                    "wcrp_t.price_per_ton AS to_warehouse_price, "
                    "wf.id AS sf_id, wf.name AS sf_name, wf.province AS sf_province, "
                    "wf.city AS sf_city, wf.district AS sf_district, wf.address AS sf_address, "
                    "wf.warehouse_type_id AS sf_warehouse_type_id, wf.color_config AS sf_color_config, "
                    "wf.longitude AS sf_longitude, wf.latitude AS sf_latitude, wf.is_active AS sf_is_active, "
                    "wf.contact_name AS sf_contact_name, wf.contact_phone AS sf_contact_phone, "
                    "wf.hazardous_waste_license_qty AS sf_hazardous_waste_license_qty, "
                    "wf.monthly_avg_receipt_ton AS sf_monthly_avg_receipt_ton, "
                    "wf.freight_amount AS sf_freight_amount, "
                    "wf.created_at AS sf_created_at, wf.updated_at AS sf_updated_at, "
                    "wfs.name AS sf_type_name, "
                    "wt.id AS st_id, wt.name AS st_name, wt.province AS st_province, "
                    "wt.city AS st_city, wt.district AS st_district, wt.address AS st_address, "
                    "wt.warehouse_type_id AS st_warehouse_type_id, wt.color_config AS st_color_config, "
                    "wt.longitude AS st_longitude, wt.latitude AS st_latitude, wt.is_active AS st_is_active, "
                    "wt.contact_name AS st_contact_name, wt.contact_phone AS st_contact_phone, "
                    "wt.hazardous_waste_license_qty AS st_hazardous_waste_license_qty, "
                    "wt.monthly_avg_receipt_ton AS st_monthly_avg_receipt_ton, "
                    "wt.freight_amount AS st_freight_amount, "
                    "wt.created_at AS st_created_at, wt.updated_at AS st_updated_at, "
                    "wts.name AS st_type_name "
                    f"{base_from} "
                    "ORDER BY l.id DESC LIMIT %s OFFSET %s",
                    tuple(params + [size, offset]),
                )
                rows = cur.fetchall()

        items: List[Dict[str, Any]] = []
        for r in rows:
            dist_km: Optional[float] = None
            try:
                sln = r.get("sf_longitude")
                sla = r.get("sf_latitude")
                tln = r.get("st_longitude")
                tla = r.get("st_latitude")
                if (
                    sln is not None
                    and sla is not None
                    and tln is not None
                    and tla is not None
                ):
                    dist_km = round(
                        _haversine_km(float(sln), float(sla), float(tln), float(tla)),
                        3,
                    )
            except (TypeError, ValueError):
                dist_km = None

            fp = r.get("from_warehouse_price")
            tp = r.get("to_warehouse_price")
            realtime_spread = None
            if fp is not None and tp is not None:
                try:
                    realtime_spread = round(float(fp) - float(tp), 4)
                except (TypeError, ValueError):
                    realtime_spread = None

            items.append(
                {
                    "linkId": int(r["link_id"]),
                    "fromWarehouseId": int(r["from_warehouse_id"]),
                    "toWarehouseId": int(r["to_warehouse_id"]),
                    "createTime": _fmt_ts(r.get("link_created_at")),
                    "distanceKm": dist_km,
                    "tierPriceSpread": _parse_tier_price_spread(r.get("tier_price_spread")),
                    "remark": r.get("remark") or "",
                    "aiAnalysis": r.get("ai_analysis") or "",
                    "lastAnalysisAt": _fmt_ts(r.get("last_analysis_at")),
                    "fromWarehousePrice": float(fp) if fp is not None else None,
                    "toWarehousePrice": float(tp) if tp is not None else None,
                    "realtimeSpread": realtime_spread,
                    "source": _warehouse_row_api(
                        _wh_side_row(r, "sf"),
                        r.get("sf_type_name"),
                    ),
                    "target": _warehouse_row_api(
                        _wh_side_row(r, "st"),
                        r.get("st_type_name"),
                    ),
                }
            )

        return _ok(
            "查询成功",
            data={"list": items, "total": total, "page": page, "size": size},
        )
    except Exception as e:
        logger.exception("查询库房关联实时价差列表失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_links_replace_outbound(from_wh_id: int, to_wh_ids: List[int]) -> Dict[str, Any]:
    """将 from 的所有出边替换为指向 to_wh_ids（去重、忽略自环、目标须存在）。"""
    try:
        if from_wh_id < 1:
            return _err(CODE_VALIDATION, "源库房 id 无效")

        uniq: List[int] = []
        seen: Set[int] = set()
        for x in to_wh_ids:
            try:
                tid = int(x)
            except (TypeError, ValueError):
                return _err(CODE_VALIDATION, "目标库房 id 列表无效")
            if tid < 1:
                return _err(CODE_VALIDATION, "目标库房 id 无效")
            if tid == from_wh_id:
                continue
            if tid in seen:
                continue
            seen.add(tid)
            uniq.append(tid)

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT id FROM dict_warehouses WHERE id = %s",
                    (from_wh_id,),
                )
                if not cur.fetchone():
                    return _err(CODE_NOT_FOUND, "源库房不存在")

                if uniq:
                    ph = ",".join(["%s"] * len(uniq))
                    cur.execute(
                        f"SELECT id FROM dict_warehouses WHERE id IN ({ph})",
                        tuple(uniq),
                    )
                    ok_ids = {int(r["id"]) for r in cur.fetchall()}
                    missing = [i for i in uniq if i not in ok_ids]
                    if missing:
                        return _err(
                            CODE_NOT_FOUND,
                            f"目标库房不存在: {missing}",
                        )

                cur.execute(
                    "DELETE FROM dict_warehouse_links WHERE from_warehouse_id = %s",
                    (from_wh_id,),
                )
                for tid in uniq:
                    cur.execute(
                        "INSERT INTO dict_warehouse_links (from_warehouse_id, to_warehouse_id) "
                        "VALUES (%s,%s)",
                        (from_wh_id, tid),
                    )
                conn.commit()

        return _ok(
            "替换成功",
            data={"fromWarehouseId": from_wh_id, "toWarehouseIds": uniq},
        )
    except pymysql.IntegrityError as e:
        logger.warning("替换出边触发约束: %s", e)
        return _err(CODE_DB, f"数据库操作异常: {e}")
    except Exception as e:
        logger.exception("替换库房出边失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def _normalize_outbound_targets(
    from_wh_id: int,
    to_wh_ids: List[Any],
) -> Tuple[Optional[str], List[int]]:
    """解析目标 id 列表：去重、去自环、校验为正整数。返回 (错误信息, 唯一目标 id 列表)。"""
    uniq: List[int] = []
    seen: Set[int] = set()
    for x in to_wh_ids:
        try:
            tid = int(x)
        except (TypeError, ValueError):
            return "目标库房 id 列表无效", []
        if tid < 1:
            return "目标库房 id 无效", []
        if tid == from_wh_id:
            continue
        if tid in seen:
            continue
        seen.add(tid)
        uniq.append(tid)
    return "", uniq


def warehouse_links_batch_bind(from_wh_id: int, to_wh_ids: List[Any]) -> Dict[str, Any]:
    """同一源库房批量新增出边；已存在的边跳过并计入 skippedDuplicate。"""
    try:
        if from_wh_id < 1:
            return _err(CODE_VALIDATION, "源库房 id 无效")

        err_msg, uniq = _normalize_outbound_targets(from_wh_id, to_wh_ids or [])
        if err_msg:
            return _err(CODE_VALIDATION, err_msg)

        if not uniq:
            return _ok(
                "绑定完成",
                data={
                    "fromWarehouseId": from_wh_id,
                    "inserted": 0,
                    "skippedDuplicate": 0,
                },
            )

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT id FROM dict_warehouses WHERE id = %s",
                    (from_wh_id,),
                )
                if not cur.fetchone():
                    return _err(CODE_NOT_FOUND, "源库房不存在")

                ph = ",".join(["%s"] * len(uniq))
                cur.execute(
                    f"SELECT id FROM dict_warehouses WHERE id IN ({ph})",
                    tuple(uniq),
                )
                ok_ids = {int(r["id"]) for r in cur.fetchall()}
                missing = [i for i in uniq if i not in ok_ids]
                if missing:
                    return _err(CODE_NOT_FOUND, f"目标库房不存在: {missing}")

                cur.execute(
                    f"SELECT to_warehouse_id FROM dict_warehouse_links "
                    f"WHERE from_warehouse_id = %s AND to_warehouse_id IN ({ph})",
                    (from_wh_id,) + tuple(uniq),
                )
                already = {int(r["to_warehouse_id"]) for r in cur.fetchall()}
                pending = [t for t in uniq if t not in already]
                skipped_dup = len(uniq) - len(pending)

                for t in pending:
                    cur.execute(
                        "INSERT INTO dict_warehouse_links (from_warehouse_id, to_warehouse_id) "
                        "VALUES (%s,%s)",
                        (from_wh_id, t),
                    )
                conn.commit()

        return _ok(
            "绑定完成",
            data={
                "fromWarehouseId": from_wh_id,
                "inserted": len(pending),
                "skippedDuplicate": skipped_dup,
            },
        )
    except pymysql.IntegrityError as e:
        logger.warning("批量绑定出边触发约束: %s", e)
        return _err(CODE_DB, f"数据库操作异常: {e}")
    except Exception as e:
        logger.exception("批量绑定库房出边失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def warehouse_links_batch_unbind(from_wh_id: int, to_wh_ids: List[Any]) -> Dict[str, Any]:
    """同一源库房批量删除出边；不存在的边不产生错误。"""
    try:
        if from_wh_id < 1:
            return _err(CODE_VALIDATION, "源库房 id 无效")

        err_msg, uniq = _normalize_outbound_targets(from_wh_id, to_wh_ids or [])
        if err_msg:
            return _err(CODE_VALIDATION, err_msg)

        if not uniq:
            return _ok(
                "解绑完成",
                data={"fromWarehouseId": from_wh_id, "deleted": 0},
            )

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT id FROM dict_warehouses WHERE id = %s",
                    (from_wh_id,),
                )
                if not cur.fetchone():
                    return _err(CODE_NOT_FOUND, "源库房不存在")

                ph = ",".join(["%s"] * len(uniq))
                cur.execute(
                    f"DELETE FROM dict_warehouse_links WHERE from_warehouse_id = %s "
                    f"AND to_warehouse_id IN ({ph})",
                    (from_wh_id,) + tuple(uniq),
                )
                deleted = int(cur.rowcount)
                conn.commit()

        return _ok(
            "解绑完成",
            data={"fromWarehouseId": from_wh_id, "deleted": deleted},
        )
    except Exception as e:
        logger.exception("批量解绑库房出边失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


# ---------- 冶炼厂（无 type 字段）----------


def smelter_create(payload: Dict[str, Any]) -> Dict[str, Any]:
    """新建冶炼厂：不写 color_config；经纬度默认不传则由天地图根据地址解析。"""
    try:
        name = str(payload.get("name") or "").strip()
        factory_type_name = str(payload.get("factory_type_name") or "").strip() or None
        factory_type_id = payload.get("factory_type_id")
        province = str(payload.get("province") or "").strip()
        city = str(payload.get("city") or "").strip()
        district = str(payload.get("district") or "").strip()
        address = str(payload.get("address") or "").strip()
        lon = payload.get("longitude")
        lat = payload.get("latitude")
        status = payload.get("status")
        uxb_raw = payload.get("use_xunrongbao")
        use_xrb = False if uxb_raw is None else bool(uxb_raw)

        if not name:
            return _err(CODE_VALIDATION, "冶炼厂名称不能为空")
        if not province or not city or not district or not address:
            return _err(CODE_VALIDATION, "province、city、district、address 均为必填")
        if status is not None and int(status) not in (0, 1):
            return _err(CODE_VALIDATION, "status 须为 0 或 1")

        try:
            lon_f = float(lon) if lon is not None else None
            lat_f = float(lat) if lat is not None else None
        except (TypeError, ValueError):
            return _err(CODE_VALIDATION, "longitude、latitude 格式无效")

        try:
            rx_lon, rx_lat = maybe_geocode(
                province, city, district, address,
                longitude=lon_f,
                latitude=lat_f,
            )
        except GeocoderError as e:
            return _err(CODE_VALIDATION, e.message)

        st = 1 if status is None else int(status)

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT id FROM dict_factories WHERE name = %s",
                    (name,),
                )
                if cur.fetchone():
                    return _err(CODE_DUP_NAME, "冶炼厂名称已存在")

                # 解析类型：优先 ID，其次名称
                ft_id = None
                if factory_type_id is not None and int(factory_type_id) >= 1:
                    ft_id = int(factory_type_id)
                elif factory_type_name:
                    ft_id = _lookup_factory_type_id(cur, factory_type_name)
                    if ft_id is None:
                        return _err(CODE_VALIDATION, "冶炼厂类型不存在或未启用，请先维护冶炼厂类型")

                cur.execute(
                    "INSERT INTO dict_factories (name, factory_type_id, province, city, district, address, "
                    "color_config, longitude, latitude, use_xunrongbao, is_active) "
                    "VALUES (%s,%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s)",
                    (
                        name,
                        ft_id,
                        province,
                        city,
                        district,
                        address,
                        rx_lon,
                        rx_lat,
                        1 if use_xrb else 0,
                        st,
                    ),
                )
                fid = cur.lastrowid
                conn.commit()

                cur.execute(
                    "SELECT df.*, ft.name AS factory_type_name "
                    "FROM dict_factories df "
                    "LEFT JOIN dict_factory_types ft ON df.factory_type_id = ft.id "
                    "WHERE df.id = %s", (fid,))
                row = cur.fetchone()
        return _ok("创建成功", data=_factory_row_api(row))
    except pymysql.IntegrityError:
        return _err(CODE_DUP_NAME, "冶炼厂名称已存在")
    except Exception as e:
        logger.exception("创建冶炼厂失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def smelter_delete(factory_id: int) -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT id FROM dict_factories WHERE id = %s",
                    (factory_id,),
                )
                if not cur.fetchone():
                    return _err(CODE_NOT_FOUND, "冶炼厂不存在")
                cur.execute(
                    "UPDATE dict_factories SET is_active = 0 WHERE id = %s",
                    (factory_id,),
                )
            conn.commit()
        return _ok("删除成功", data=None)
    except Exception as e:
        logger.exception("删除冶炼厂失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def smelter_update(factory_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
    """更新冶炼厂：不支持颜色字段；地址或行政区变更且未同时手传经纬度时重新走天地图。"""
    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT df.*, ft.name AS factory_type_name "
                    "FROM dict_factories df "
                    "LEFT JOIN dict_factory_types ft ON df.factory_type_id = ft.id "
                    "WHERE df.id = %s",
                    (factory_id,),
                )
                row = cur.fetchone()
                if not row:
                    return _err(CODE_NOT_FOUND, "冶炼厂不存在")

                name = patch.get("name")
                factory_type_name = patch.get("factory_type_name")
                factory_type_id = patch.get("factory_type_id")
                province = patch.get("province")
                city = patch.get("city")
                district = patch.get("district")
                address = patch.get("address")
                status = patch.get("status")
                lon_p = patch.get("longitude")
                lat_p = patch.get("latitude")

                n = str(name).strip() if name is not None else row["name"]
                p = str(province).strip() if province is not None else (row.get("province") or "")
                c = str(city).strip() if city is not None else (row.get("city") or "")
                d = str(district).strip() if district is not None else (row.get("district") or "")
                a = str(address).strip() if address is not None else (row.get("address") or "")

                updates: List[str] = []
                params: List[Any] = []

                if name is not None:
                    if not n:
                        return _err(CODE_VALIDATION, "冶炼厂名称不能为空")
                    cur.execute(
                        "SELECT id FROM dict_factories WHERE name = %s AND id <> %s",
                        (n, factory_id),
                    )
                    if cur.fetchone():
                        return _err(CODE_DUP_NAME, "冶炼厂名称已存在")
                    updates.append("name = %s")
                    params.append(n)

                # 冶炼厂类型：优先名称→ID 解析，其次直接传 ID，可传 null 清空
                if "factory_type_name" in patch or "factory_type_id" in patch:
                    if factory_type_name is not None and str(factory_type_name).strip():
                        ft_id = _lookup_factory_type_id(cur, str(factory_type_name).strip())
                        if ft_id is None:
                            return _err(CODE_VALIDATION, "冶炼厂类型不存在或未启用")
                        updates.append("factory_type_id = %s")
                        params.append(ft_id)
                    elif factory_type_id is not None and int(factory_type_id) >= 1:
                        updates.append("factory_type_id = %s")
                        params.append(int(factory_type_id))
                    else:
                        updates.append("factory_type_id = NULL")

                for fld, val, curv in (
                    ("province", province, p),
                    ("city", city, c),
                    ("district", district, d),
                    ("address", address, a),
                ):
                    if fld in patch:
                        updates.append(f"{fld} = %s")
                        params.append(curv)

                if status is not None:
                    if int(status) not in (0, 1):
                        return _err(CODE_VALIDATION, "status 须为 0 或 1")
                    updates.append("is_active = %s")
                    params.append(1 if int(status) == 1 else 0)

                if "use_xunrongbao" in patch and patch["use_xunrongbao"] is not None:
                    uxb = patch["use_xunrongbao"]
                    updates.append("use_xunrongbao = %s")
                    params.append(1 if uxb else 0)

                has_lon = "longitude" in patch
                has_lat = "latitude" in patch
                if has_lon or has_lat:
                    if not (has_lon and has_lat):
                        return _err(CODE_VALIDATION, "经度与纬度须同时提供")
                    try:
                        lon_v = float(lon_p)
                        lat_v = float(lat_p)
                    except (TypeError, ValueError):
                        return _err(CODE_VALIDATION, "longitude、latitude 格式无效")
                    if not (-180.0 <= lon_v <= 180.0 and -90.0 <= lat_v <= 90.0):
                        return _err(CODE_VALIDATION, "经纬度超出允许范围")
                    updates.append("longitude = %s")
                    updates.append("latitude = %s")
                    params.extend([lon_v, lat_v])
                elif any(k in patch for k in ("province", "city", "district", "address")):
                    try:
                        rx_lon, rx_lat = maybe_geocode(p, c, d, a, longitude=None, latitude=None)
                    except GeocoderError as e:
                        return _err(CODE_VALIDATION, e.message)
                    updates.append("longitude = %s")
                    updates.append("latitude = %s")
                    params.extend([rx_lon, rx_lat])

                if not updates:
                    cur.execute(
                        "SELECT df.*, ft.name AS factory_type_name "
                    "FROM dict_factories df "
                    "LEFT JOIN dict_factory_types ft ON df.factory_type_id = ft.id "
                    "WHERE df.id = %s",
                        (factory_id,),
                    )
                    nrow = cur.fetchone()
                    return _ok("修改成功", data=_factory_row_api(nrow))

                params.append(factory_id)
                cur.execute(
                    f"UPDATE dict_factories SET {', '.join(updates)} WHERE id = %s",
                    tuple(params),
                )
                conn.commit()

                cur.execute(
                    "SELECT df.*, ft.name AS factory_type_name "
                    "FROM dict_factories df "
                    "LEFT JOIN dict_factory_types ft ON df.factory_type_id = ft.id "
                    "WHERE df.id = %s",
                    (factory_id,),
                )
                urow = cur.fetchone()
        return _ok("修改成功", data=_factory_row_api(urow))
    except pymysql.IntegrityError:
        return _err(CODE_DUP_NAME, "冶炼厂名称已存在")
    except Exception as e:
        logger.exception("修改冶炼厂失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def smelter_list(
    page: int,
    size: int,
    name: Optional[str] = None,
    province: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    status: Optional[int] = None,
    factory_type: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        page = max(1, page)
        size = min(200, max(1, size))
        offset = (page - 1) * size

        conds: List[str] = ["1=1"]
        params: List[Any] = []

        if name is not None and str(name).strip():
            conds.append("name LIKE %s")
            params.append(f"%{str(name).strip()}%")
        if province is not None and str(province).strip():
            conds.append("province = %s")
            params.append(str(province).strip())
        if city is not None and str(city).strip():
            conds.append("city = %s")
            params.append(str(city).strip())
        if district is not None and str(district).strip():
            conds.append("district = %s")
            params.append(str(district).strip())
        if status is not None:
            conds.append("df.is_active = %s")
            params.append(1 if int(status) == 1 else 0)
        if factory_type is not None and str(factory_type).strip():
            conds.append("df.factory_type_id IS NOT NULL AND ft.name = %s")
            params.append(str(factory_type).strip())

        where_sql = " AND ".join(conds)

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    f"SELECT COUNT(*) AS n FROM dict_factories df "
                    f"LEFT JOIN dict_factory_types ft ON df.factory_type_id = ft.id "
                    f"WHERE {where_sql}",
                    tuple(params),
                )
                total = int(cur.fetchone()["n"])
                cur.execute(
                    f"SELECT df.*, ft.name AS factory_type_name "
                    f"FROM dict_factories df "
                    f"LEFT JOIN dict_factory_types ft ON df.factory_type_id = ft.id "
                    f"WHERE {where_sql} "
                    f"ORDER BY df.id DESC LIMIT %s OFFSET %s",
                    tuple(params + [size, offset]),
                )
                rows = cur.fetchall()

        items = [_factory_row_api(r) for r in rows]
        return _ok(
            "查询成功",
            data={"list": items, "total": total, "page": page, "size": size},
        )
    except Exception as e:
        logger.exception("查询冶炼厂列表失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")


def smelter_get(factory_id: int) -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    "SELECT df.*, ft.name AS factory_type_name "
                    "FROM dict_factories df "
                    "LEFT JOIN dict_factory_types ft ON df.factory_type_id = ft.id "
                    "WHERE df.id = %s",
                    (factory_id,),
                )
                row = cur.fetchone()
        if not row:
            return _err(CODE_NOT_FOUND, "冶炼厂不存在")
        return _ok("查询成功", data=_factory_row_api(row))
    except Exception as e:
        logger.exception("查询冶炼厂详情失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")
