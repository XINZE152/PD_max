"""L1 内存缓存与 L2 Redis（redis.asyncio）。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import date
from typing import Any, Optional

import redis.asyncio as aioredis
import redis.exceptions as redis_exc

from app.intelligent_prediction.settings import settings
from app.intelligent_prediction.logging_utils import get_logger

logger = get_logger(__name__)


class MemoryTTLCache:
    """简易内存 TTL 缓存（asyncio.Lock 保护）。"""

    def __init__(self, default_ttl_seconds: int) -> None:
        self._ttl = default_ttl_seconds
        self._data: dict[str, tuple[float, Any]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        """取得未过期值，否则 None。"""
        async with self._lock:
            now = time.monotonic()
            item = self._data.get(key)
            if not item:
                return None
            exp, val = item
            if exp < now:
                del self._data[key]
                return None
            return val

    async def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """写入值并设置过期（单调时钟）。"""
        ttl = ttl_seconds if ttl_seconds is not None else self._ttl
        async with self._lock:
            self._data[key] = (time.monotonic() + ttl, value)


class RedisCache:
    """Redis 异步缓存封装。"""

    def __init__(self, url: str) -> None:
        self._url = url
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        """建立连接（幂等）。"""
        if self._client is None:
            self._client = aioredis.from_url(self._url, decode_responses=True)

    async def close(self) -> None:
        """关闭连接。"""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _reset_client(self) -> None:
        """连接异常后丢弃客户端，便于下次重试。"""
        if self._client is None:
            return
        try:
            await self._client.aclose()
        except Exception:
            pass
        self._client = None

    @property
    def raw(self) -> aioredis.Redis:
        """取得底层客户端（需先 connect）。"""
        if self._client is None:
            raise RuntimeError("Redis not connected")
        return self._client

    async def get_json(self, key: str) -> Any | None:
        """GET 並 json.loads。"""
        try:
            await self.connect()
            raw = await self.raw.get(key)
        except (redis_exc.RedisError, OSError, TimeoutError) as e:
            # RedisError：redis 包装后的连接错误；OSError：如 ConnectionRefusedError 未包装时
            logger.warning("Redis 不可用，跳过预测结果读缓存：%s", e)
            await self._reset_client()
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("redis json decode failed key=%s", key)
            return None

    async def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        """SET JSON 字串並 EX。"""
        try:
            await self.connect()
            await self.raw.set(key, json.dumps(value, ensure_ascii=False, default=str), ex=ttl_seconds)
        except (redis_exc.RedisError, OSError, TimeoutError) as e:
            logger.warning("Redis 不可用，跳过预测结果写缓存：%s", e)
            await self._reset_client()


class CacheManager:
    """统一缓存：Prompt 统计等走 L1；预测结果走 L2。"""

    def __init__(self) -> None:
        self.memory = MemoryTTLCache(settings.prompt_memory_ttl_seconds)
        self.redis = RedisCache(settings.prediction_redis_url)

    @staticmethod
    def forecast_weather_fingerprint(forecast_by_date: dict[date, str]) -> str:
        """预测日天气摘要指纹，用于区分 Prompt/缓存。"""
        if not forecast_by_date:
            return "none"
        payload = {d.isoformat(): v for d, v in sorted(forecast_by_date.items())}
        s = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def prediction_cache_key(
        warehouse: str,
        variety: str,
        horizon: int,
        stats_fingerprint: str,
        smelter: str | None = None,
        forecast_fp: str = "",
    ) -> str:
        """生成预测结果 Redis 键。"""
        sm = smelter or ""
        base = f"{warehouse}|{sm}|{variety}|{horizon}|{stats_fingerprint}|{forecast_fp or 'none'}"
        h = hashlib.sha256(base.encode("utf-8")).hexdigest()[:48]
        return f"pred:v1:{h}"

    @staticmethod
    def stats_fingerprint(stats: dict[str, Any]) -> str:
        """将统计字典压成短指纹。"""
        s = json.dumps(stats, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


_cache_manager: CacheManager | None = None


def get_cache_manager() -> CacheManager:
    """全局单例（进程内）。"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager()
    return _cache_manager
