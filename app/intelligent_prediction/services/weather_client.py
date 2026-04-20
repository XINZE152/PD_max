"""送货历史导入时的天气：高德地图 Web 服务「天气查询」。

优先使用字典表 `dict_warehouses` / `dict_factories` 中按名称匹配的 `city`（市），
对仓库所在市、冶炼厂所在市分别请求天气（两市相同时只请求一次）。
"""

from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from datetime import date
from typing import Any, Optional

import aiohttp

from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.services.dict_geo_lookup import lookup_warehouse_factory_cities
from app.intelligent_prediction.settings import settings

logger = get_logger(__name__)


def _amap_weather_url(base: str) -> str:
    b = base.strip().rstrip("/")
    if b.endswith("weatherInfo"):
        return b
    return f"{b}/weatherInfo"


def _build_summary_from_amap(data: dict[str, Any], target_date: date) -> str | None:
    """从高德返回 JSON 提取简短中文摘要，供预测 Prompt 使用。"""
    if data.get("status") != "1":
        return None
    ds = target_date.isoformat()
    forecasts = data.get("forecasts")
    if isinstance(forecasts, list) and forecasts:
        casts = forecasts[0].get("casts")
        if isinstance(casts, list):
            for c in casts:
                if not isinstance(c, dict):
                    continue
                if str(c.get("date", ""))[:10] == ds:
                    dayw = c.get("dayweather") or ""
                    dayt = c.get("daytemp") or ""
                    nightw = c.get("nightweather") or ""
                    nightt = c.get("nighttemp") or ""
                    parts = [
                        f"白天{dayw}{dayt}℃" if dayt else f"白天{dayw}",
                        f"夜间{nightw}{nightt}℃" if nightt else f"夜间{nightw}",
                    ]
                    return "，".join(p for p in parts if p.replace("白天", "").replace("夜间", ""))
            if casts and isinstance(casts[0], dict):
                c0 = casts[0]
                return f"{c0.get('date', '')} {c0.get('dayweather', '')} {c0.get('daytemp', '')}℃".strip()
    lives = data.get("lives")
    if isinstance(lives, list) and lives:
        lv = lives[0]
        if isinstance(lv, dict):
            w = lv.get("weather") or ""
            t = lv.get("temperature") or ""
            wd = lv.get("winddirection") or ""
            wp = lv.get("windpower") or ""
            return f"{w} {t}℃ {wd}风{wp}".strip()
    return None


async def _fetch_amap_one_city(
    session: aiohttp.ClientSession,
    url: str,
    key: str,
    target_date: date,
    city: str,
) -> dict[str, Any]:
    params: dict[str, str] = {
        "key": key,
        "city": city[:80],
        "extensions": "all",
        "output": "JSON",
    }
    async with session.get(url, params=params) as resp:
        text = await resp.text()
        http_status = resp.status
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("weather amap: invalid json status=%s head=%s", http_status, text[:200])
        return {"status": "0", "info": "invalid_json", "http_status": http_status}
    return data


async def fetch_weather_for_delivery(
    target_date: date,
    warehouse_name: str,
    smelter_name: Optional[str],
    location_fallback: str,
) -> Optional[dict[str, Any]]:
    """按字典表市名拉取高德天气；两市各查一次，相同市只查一次。

    - 从 `dict_warehouses` / `dict_factories` 用仓库名、冶炼厂名匹配 `city`（市）。
    - 若两市均未配置或均查不到，则用 `location_fallback` 作为高德 `city` 参数发一次请求。
    """
    base = (settings.weather_api_base_url or "").strip()
    key = (settings.weather_api_key or "").strip()
    if not base or not key:
        return None

    url = _amap_weather_url(base)
    wh_city, sm_city = await asyncio.to_thread(
        lookup_warehouse_factory_cities,
        warehouse_name,
        smelter_name,
    )

    city_roles: OrderedDict[str, set[str]] = OrderedDict()

    def _add_city(c: Optional[str], role: str) -> None:
        if not c:
            return
        t = str(c).strip()
        if not t:
            return
        city_roles.setdefault(t, set()).add(role)

    _add_city(wh_city, "仓库")
    _add_city(sm_city, "冶炼厂")

    if not city_roles:
        fb = (location_fallback or "").strip()[:80] or "北京"
        city_roles[fb] = {"地址"}

    timeout = aiohttp.ClientTimeout(total=12)
    by_city: list[dict[str, Any]] = []
    part_summaries: list[str] = []

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for city, roles in city_roles.items():
                data = await _fetch_amap_one_city(session, url, key, target_date, city)
                if data.get("status") != "1":
                    by_city.append(
                        {
                            "city": city,
                            "roles": sorted(roles),
                            "summary": None,
                            "error": data.get("info") or data.get("infocode"),
                        }
                    )
                    continue
                summary = _build_summary_from_amap(data, target_date)
                role_cn = "/".join(sorted(roles))
                part_summaries.append(f"{role_cn}（{city}）：{summary}" if summary else f"{role_cn}（{city}）：无摘要")
                by_city.append(
                    {
                        "city": city,
                        "roles": sorted(roles),
                        "summary": summary,
                        "lives": data.get("lives"),
                        "forecasts": data.get("forecasts"),
                    }
                )
    except (aiohttp.ClientError, TimeoutError, OSError) as e:
        logger.warning("weather amap http failed: %s", e)
        return None
    except Exception:
        logger.exception("weather amap unexpected error")
        return None

    combined = "；".join(part_summaries) if part_summaries else None
    return {
        "provider": "amap",
        "summary": combined,
        "requested_date": target_date.isoformat(),
        "warehouse_city": wh_city,
        "smelter_city": sm_city,
        "by_city": by_city,
    }


def summary_from_weather_json(data: Any) -> Optional[str]:
    """从 `fetch_weather_for_delivery` / `fetch_weather_json` 返回的 dict 取摘要文本。"""
    if not isinstance(data, dict):
        return None
    for k in ("summary", "brief", "text", "description"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()[:500]
    return None


async def fetch_forecast_weather_by_dates(
    dates: list[date],
    warehouse_name: str,
    smelter_name: Optional[str],
    location_fallback: str,
    *,
    default_when_missing: str = "晴",
) -> dict[date, str]:
    """按日期列表拉取当日天气摘要（未配置 API 或失败时用 `default_when_missing`）。"""
    unique = sorted(set(dates))
    if not unique:
        return {}
    tasks = [
        fetch_weather_for_delivery(d, warehouse_name, smelter_name, location_fallback) for d in unique
    ]
    payloads = await asyncio.gather(*tasks)
    out: dict[date, str] = {}
    for d, wj in zip(unique, payloads):
        s = summary_from_weather_json(wj)
        out[d] = ((s or "").strip() or default_when_missing)[:200]
    return out


async def fetch_weather_json(target_date: date, location: str) -> Optional[dict[str, Any]]:
    """兼容旧调用：不按字典解析市名，仅用一段地址/城市文本查询一次。"""
    return await fetch_weather_for_delivery(target_date, "", None, location)
