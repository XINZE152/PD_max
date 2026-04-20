"""
TL 比价使用：仓库/冶炼厂字典表落库 + 天地图地理编码（仅由 TLService 调用，不对外单独暴露 HTTP）。

- 省市区与详细地址齐全时，经度/纬度未手传则调用 maybe_geocode 填充；失败时依配置可存 NULL。
- 冶炼厂不维护颜色配置；库中 color_config 列可留空。
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from app.database import get_conn
from app.services.tianditu_geocoder import GeocoderError, maybe_geocode

logger = logging.getLogger(__name__)

CODE_OK = 0
CODE_VALIDATION = 1001
CODE_NOT_FOUND = 1002
CODE_DUP_NAME = 1003
CODE_DB = 2001
CODE_INTERNAL = 5000

_HEX_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")


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
        "status": 1 if int(row.get("is_active", 1)) == 1 else 0,
        "createTime": _fmt_ts(row.get("created_at")),
        "updateTime": _fmt_ts(row.get("updated_at")),
    }


def _factory_row_api(row: Dict[str, Any]) -> Dict[str, Any]:
    """冶炼厂字典行序列化（不含颜色；比价侧不使用冶炼厂标记色）。"""
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "province": row.get("province") or "",
        "city": row.get("city") or "",
        "district": row.get("district") or "",
        "address": row.get("address") or "",
        "longitude": float(row["longitude"]) if row.get("longitude") is not None else None,
        "latitude": float(row["latitude"]) if row.get("latitude") is not None else None,
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

                cur.execute(
                    "INSERT INTO dict_warehouses (name, province, city, district, address, "
                    "warehouse_type_id, color_config, longitude, latitude, is_active) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        name,
                        province,
                        city,
                        district,
                        address,
                        wt_id,
                        cc_json,
                        rx_lon,
                        rx_lat,
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
        size = min(100, max(1, size))
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


# ---------- 冶炼厂（无 type 字段）----------


def smelter_create(payload: Dict[str, Any]) -> Dict[str, Any]:
    """新建冶炼厂：不写 color_config；经纬度默认不传则由天地图根据地址解析。"""
    try:
        name = str(payload.get("name") or "").strip()
        province = str(payload.get("province") or "").strip()
        city = str(payload.get("city") or "").strip()
        district = str(payload.get("district") or "").strip()
        address = str(payload.get("address") or "").strip()
        lon = payload.get("longitude")
        lat = payload.get("latitude")
        status = payload.get("status")

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

                cur.execute(
                    "INSERT INTO dict_factories (name, province, city, district, address, "
                    "color_config, longitude, latitude, is_active) "
                    "VALUES (%s,%s,%s,%s,%s,NULL,%s,%s,%s)",
                    (
                        name,
                        province,
                        city,
                        district,
                        address,
                        rx_lon,
                        rx_lat,
                        st,
                    ),
                )
                fid = cur.lastrowid
                conn.commit()

                cur.execute("SELECT * FROM dict_factories WHERE id = %s", (fid,))
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
                    "SELECT * FROM dict_factories WHERE id = %s",
                    (factory_id,),
                )
                row = cur.fetchone()
                if not row:
                    return _err(CODE_NOT_FOUND, "冶炼厂不存在")

                name = patch.get("name")
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
                        "SELECT * FROM dict_factories WHERE id = %s",
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
                    "SELECT * FROM dict_factories WHERE id = %s",
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
) -> Dict[str, Any]:
    try:
        page = max(1, page)
        size = min(100, max(1, size))
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
            conds.append("is_active = %s")
            params.append(1 if int(status) == 1 else 0)

        where_sql = " AND ".join(conds)

        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    f"SELECT COUNT(*) AS n FROM dict_factories WHERE {where_sql}",
                    tuple(params),
                )
                total = int(cur.fetchone()["n"])
                cur.execute(
                    f"SELECT * FROM dict_factories WHERE {where_sql} "
                    f"ORDER BY id DESC LIMIT %s OFFSET %s",
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
                    "SELECT * FROM dict_factories WHERE id = %s",
                    (factory_id,),
                )
                row = cur.fetchone()
        if not row:
            return _err(CODE_NOT_FOUND, "冶炼厂不存在")
        return _ok("查询成功", data=_factory_row_api(row))
    except Exception as e:
        logger.exception("查询冶炼厂详情失败")
        return _err(CODE_DB, f"数据库操作异常: {e}")
