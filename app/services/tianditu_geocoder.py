"""
天地图地理编码：结构化地址 → 经纬度。
文档：ds 为 JSON 字符串（含 keyWord），tk 为密钥。
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional, Tuple

from app import config

logger = logging.getLogger(__name__)

_ALLOWED_COORD_FALLBACK_MSG = (
    "天地图不可用或未授权（如 HTTP 403 请检查 MAP_API_KEY）；已跳过经纬度落库"
)


class GeocoderError(Exception):
    """地理编码失败（配置缺失、网络、无结果等）"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


def _build_keyword(
    province: str,
    city: str,
    district: str,
    address: str,
) -> str:
    parts = [province or "", city or "", district or "", address or ""]
    return "".join(p.strip() for p in parts if p and str(p).strip())


def _fit_keyword(
    province: str,
    city: str,
    district: str,
    address: str,
    *,
    max_len: int,
) -> str:
    """将省市区+详址压至天地图 keyWord 长度上限（默认 50 字）。

    按优先级尝试缩短；均超长时对完整串做尾部截断以保留门牌号信息。
    """
    full = _build_keyword(province, city, district, address)
    if not full:
        return full
    if len(full) <= max_len:
        return full

    candidates = [
        full,
        _build_keyword("", city, district, address),
        _build_keyword("", "", district, address),
        _build_keyword("", "", "", address),
    ]
    for kw in candidates:
        kw = (kw or "").strip()
        if kw and len(kw) <= max_len:
            if kw != full:
                logger.info(
                    "天地图 keyWord 已缩短: 原长=%d 现长=%d 原=%r 现=%r",
                    len(full),
                    len(kw),
                    full[:80],
                    kw,
                )
            return kw

    truncated = full[-max_len:]
    logger.info(
        "天地图 keyWord 已截断: 原长=%d 现长=%d 原=%r 现=%r",
        len(full),
        len(truncated),
        full[:80],
        truncated,
    )
    return truncated


def geocode_region_address(
    province: str,
    city: str,
    district: str,
    address: str,
    *,
    timeout: Optional[float] = None,
) -> Tuple[float, float]:
    """
    调用天地图 geocoder 接口，返回 (经度, 纬度)。
    timeout：秒；不传则使用 config.MAP_GEOCODER_TIMEOUT（环境变量 MAP_GEOCODER_TIMEOUT，默认 20）。
    """
    effective_timeout = (
        float(timeout) if timeout is not None else float(config.MAP_GEOCODER_TIMEOUT)
    )
    key = (config.MAP_API_KEY or "").strip()
    if not key:
        raise GeocoderError("未配置 MAP_API_KEY，无法调用天地图")

    max_kw_len = int(getattr(config, "MAP_GEOCODER_KEYWORD_MAX_LEN", 50) or 50)
    key_word = _fit_keyword(
        province, city, district, address, max_len=max(1, max_kw_len)
    )
    if not key_word.strip():
        raise GeocoderError("地址关键词为空，无法地理编码")

    base = (config.MAP_GEOCODER_URL or "").strip().rstrip("/")
    if not base:
        base = "http://api.tianditu.gov.cn/geocoder"

    ds_obj = {"keyWord": key_word}
    ds = json.dumps(ds_obj, ensure_ascii=False)
    q = urllib.parse.urlencode({"ds": ds, "tk": key})
    url = f"{base}?{q}"
    # 天地图对异常客户端会 403；补充常见头（部分环境仅 UA 不够）
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; PD_max/1.0; +https://tianditu.gov.cn)",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=effective_timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            pass
        logger.warning(
            "天地图 HTTP 错误: %s keyWord_len=%d keyWord=%r response=%s",
            e,
            len(key_word),
            key_word[:80],
            err_body,
        )
        hint = "天地图服务返回 HTTP 错误"
        code = getattr(e, "code", None)
        if code == 403:
            hint += (
                "（403：多为 tk 与应用类型不匹配——后台须使用天地图控制台申请的「服务端」应用密钥，"
                "浏览器端密钥用于前端脚本，服务端直连常返回 403；也可能是密钥停用或配额用尽）"
            )
        elif code == 400 and err_body:
            hint += f"（{err_body[:200]}）"
        raise GeocoderError(hint) from e
    except urllib.error.URLError as e:
        logger.warning("天地图网络错误: %s", e)
        raise GeocoderError("无法连接天地图服务") from e
    except TimeoutError as e:
        logger.warning(
            "天地图请求超时 timeout=%ss keyWord前80字=%r",
            effective_timeout,
            key_word[:80],
        )
        raise GeocoderError(f"天地图请求超时（{effective_timeout:g}s）") from e
    except Exception as e:
        logger.exception("天地图请求异常")
        raise GeocoderError("地理编码请求失败") from e

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise GeocoderError("天地图返回非 JSON") from e

    status_raw = data.get("status")
    status = str(status_raw).strip() if status_raw is not None else ""
    if status == "101":
        raise GeocoderError("地理编码无结果")
    if status == "404" or status != "0":
        msg = str(data.get("msg", "")) or "地理编码失败"
        raise GeocoderError(msg)

    loc = data.get("location")
    if not isinstance(loc, dict):
        raise GeocoderError("天地图未返回 location")

    lon_raw = loc.get("lon")
    lat_raw = loc.get("lat")
    if lon_raw is None or lat_raw is None:
        raise GeocoderError("天地图未返回有效坐标")

    try:
        lon = float(lon_raw)
        lat = float(lat_raw)
    except (TypeError, ValueError) as e:
        raise GeocoderError("天地图坐标格式无效") from e

    if not (-180.0 <= lon <= 180.0) or not (-90.0 <= lat <= 90.0):
        raise GeocoderError("天地图返回的经纬度超出有效范围")

    return lon, lat


def maybe_geocode(
    province: str,
    city: str,
    district: str,
    address: str,
    *,
    longitude: Optional[float],
    latitude: Optional[float],
) -> Tuple[Optional[float], Optional[float]]:
    """若已提供经纬度则校验后直接返回；否则调用天地图。
    若 MAP_GEOCODE_ALLOW_NULL 为真（默认），天地图失败时返回 (None, None)，业务仍可落库结构化地址。
    """
    if longitude is not None and latitude is not None:
        if not (-180.0 <= longitude <= 180.0):
            raise GeocoderError("经度须在 -180～180 之间")
        if not (-90.0 <= latitude <= 90.0):
            raise GeocoderError("纬度须在 -90～90 之间")
        return longitude, latitude

    if longitude is not None or latitude is not None:
        raise GeocoderError("经度与纬度须同时提供或同时留空由地理编码填充")

    try:
        return geocode_region_address(province, city, district, address)
    except GeocoderError as err:
        if getattr(config, "MAP_GEOCODE_ALLOW_NULL", True):
            logger.warning("%s: %s", _ALLOWED_COORD_FALLBACK_MSG, err.message)
            return None, None
        raise
