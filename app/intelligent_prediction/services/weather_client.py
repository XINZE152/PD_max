"""送货历史导入时的天气：高德地图 Web 服务「天气查询」。"""

from __future__ import annotations

import json
from datetime import date
from typing import Any, Optional

import aiohttp

from app.intelligent_prediction.logging_utils import get_logger
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
                    parts = [f"白天{dayw}{dayt}℃" if dayt else f"白天{dayw}", f"夜间{nightw}{nightt}℃" if nightt else f"夜间{nightw}"]
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


async def fetch_weather_json(target_date: date, location: str) -> Optional[dict[str, Any]]:
    """调用高德 `weatherInfo`，写入 `weather_json`。

    需在环境变量配置：
    - `WEATHER_API_BASE_URL`：建议 `https://restapi.amap.com/v3/weather`（不要末尾 `/`）
    - `WEATHER_API_KEY`：高德 Web 服务 Key

    `city` 使用导入行中的地址/仓库等文本（高德支持城市名称、区名、adcode 等，会做匹配）。
    说明：预报为接口返回时的预报数据；与「历史送货日期」完全对齐依赖 casts 中是否有该日。
    """
    base = (settings.weather_api_base_url or "").strip()
    key = (settings.weather_api_key or "").strip()
    if not base or not key:
        return None

    url = _amap_weather_url(base)
    city = (location or "").strip()[:80] or "北京"
    params: dict[str, str] = {
        "key": key,
        "city": city,
        "extensions": "all",
        "output": "JSON",
    }
    timeout = aiohttp.ClientTimeout(total=12)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                text = await resp.text()
                http_status = resp.status
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("weather amap: invalid json status=%s head=%s", http_status, text[:200])
            return None
        if data.get("status") != "1":
            logger.info(
                "weather amap: business error infocode=%s info=%s city=%s",
                data.get("infocode"),
                data.get("info"),
                city[:40],
            )
            return {
                "provider": "amap",
                "summary": None,
                "error": data.get("info") or data.get("infocode"),
                "requested_date": target_date.isoformat(),
                "city": city,
            }
        summary = _build_summary_from_amap(data, target_date)
        out: dict[str, Any] = {
            "provider": "amap",
            "summary": summary,
            "requested_date": target_date.isoformat(),
            "city": city,
            "lives": data.get("lives"),
            "forecasts": data.get("forecasts"),
        }
        return out
    except (aiohttp.ClientError, TimeoutError, OSError) as e:
        logger.warning("weather amap http failed: %s", e)
        return None
    except Exception:
        logger.exception("weather amap unexpected error")
        return None
