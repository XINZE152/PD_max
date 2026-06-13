"""多供應商 AI 客戶端：OpenAI 相容 → Azure → Anthropic → 本地規則（PD_max 未內建 Coze，已省略）。"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import aiohttp

from app.intelligent_prediction.settings import settings
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.utils.ai_call_logger import log_ai_call
from app.intelligent_prediction.utils.json_extract import extract_json_object
from app.services.llm_client import build_openai_compatible_body

logger = get_logger(__name__)


class AIModelClient:
    """异步 AI 调用封装，支持故障转移与超时。"""

    def __init__(self) -> None:
        self._timeout = aiohttp.ClientTimeout(total=settings.ai_request_timeout_seconds)

    def _estimate_openai_cost(self, usage: dict[str, Any] | None) -> float | None:
        """按 token 用量粗估 OpenAI 成本（美元）。"""
        if not usage:
            return None
        try:
            inp = int(usage.get("prompt_tokens", 0) or 0)
            out = int(usage.get("completion_tokens", 0) or 0)
        except (TypeError, ValueError):
            return None
        cost = (inp / 1000.0) * settings.openai_input_price_per_1k
        cost += (out / 1000.0) * settings.openai_output_price_per_1k
        return round(cost, 6)

    async def _post_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any],
    ) -> tuple[int, dict[str, Any] | str]:
        """执行 POST 并返回 (status, json|error_text)。

        status==0 且 data 为 str：未拿到有效 HTTP 响应（超时、连接失败等），供上层切换下一供应商。
        """
        try:
            async with session.post(url, headers=headers, json=payload) as resp:
                text = await resp.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    return resp.status, text[:2000]
                if not isinstance(data, dict):
                    return resp.status, text[:2000]
                return resp.status, data
        except asyncio.TimeoutError:
            logger.info("ai http timeout url=%s", url)
            return 0, "timeout"
        except aiohttp.ClientError as e:
            logger.info("ai http client_error url=%s err=%s", url, e)
            return 0, f"client_error:{e}"

    async def _call_openai_compatible(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict[str, str],
        model: str,
        system: str,
        user: str,
        force_json: bool = True,
    ) -> tuple[dict[str, Any] | None, str, float, float | None, str, str]:
        """调用 OpenAI 兼容 Chat Completions。"""
        t0 = time.perf_counter()
        extra: dict[str, Any] = {"temperature": 0.2}
        if force_json:
            extra["response_format"] = {"type": "json_object"}
        body = build_openai_compatible_body(
            model,
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            disable_thinking=force_json,
            **extra,
        )
        status, data = await self._post_json(session, url, headers, body)
        latency_ms = (time.perf_counter() - t0) * 1000.0
        if status == 0:
            err = str(data)
            logger.info(
                "ai_call provider=openai model=%s latency_ms=%.2f cost_usd=None err=%s",
                model,
                latency_ms,
                err[:200],
            )
            return None, "openai", latency_ms, None, "", err
        if status >= 400:
            err = str(data) if isinstance(data, str) else json.dumps(data, ensure_ascii=False)[:500]
            logger.info(
                "ai_call provider=openai model=%s latency_ms=%.2f cost_usd=None err=%s",
                model,
                latency_ms,
                err[:200],
            )
            return None, "openai", latency_ms, None, "", err
        assert isinstance(data, dict)
        choices = data.get("choices") or []
        content = ""
        if choices:
            msg = (choices[0] or {}).get("message") or {}
            content = str(msg.get("content", "") or "")
            # DeepSeek 推理模型把内容放在 reasoning_content 里
            if not content.strip():
                content = str(msg.get("reasoning_content", "") or "")
        usage = data.get("usage") if isinstance(data.get("usage"), dict) else None
        cost = self._estimate_openai_cost(usage)
        logger.info(
            "ai_call provider=openai model=%s latency_ms=%.2f cost_usd=%s",
            model,
            latency_ms,
            cost,
        )
        parsed, perr = extract_json_object(content)
        if parsed is None:
            return None, "openai", latency_ms, cost, content, perr or "parse_failed"
        return parsed, "openai", latency_ms, cost, content, ""

    async def _call_anthropic(
        self,
        session: aiohttp.ClientSession,
        system: str,
        user: str,
    ) -> tuple[dict[str, Any] | None, str, float, float | None, str, str]:
        """调用 Anthropic Messages API。"""
        if not settings.anthropic_api_key:
            return None, "anthropic", 0.0, None, "", "no_api_key"
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": settings.anthropic_api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": settings.anthropic_model,
            "max_tokens": 4096,
            "system": system,
            "messages": [{"role": "user", "content": user}],
        }
        t0 = time.perf_counter()
        status, data = await self._post_json(session, url, headers, payload)
        latency_ms = (time.perf_counter() - t0) * 1000.0
        if status == 0:
            err = str(data)
            logger.info(
                "ai_call provider=anthropic model=%s latency_ms=%.2f cost_usd=None err=%s",
                settings.anthropic_model,
                latency_ms,
                err[:200],
            )
            return None, "anthropic", latency_ms, None, "", err
        if status >= 400:
            err = str(data) if isinstance(data, str) else json.dumps(data, ensure_ascii=False)[:500]
            logger.info(
                "ai_call provider=anthropic model=%s latency_ms=%.2f cost_usd=None err=%s",
                settings.anthropic_model,
                latency_ms,
                err[:200],
            )
            return None, "anthropic", latency_ms, None, "", err
        assert isinstance(data, dict)
        blocks = data.get("content") or []
        text_parts: list[str] = []
        for b in blocks:
            if isinstance(b, dict) and b.get("type") == "text":
                text_parts.append(str(b.get("text", "")))
        content = "\n".join(text_parts)
        logger.info(
            "ai_call provider=anthropic model=%s latency_ms=%.2f cost_usd=None",
            settings.anthropic_model,
            latency_ms,
        )
        parsed, perr = extract_json_object(content)
        if parsed is None:
            return None, "anthropic", latency_ms, None, content, perr or "parse_failed"
        return parsed, "anthropic", latency_ms, None, content, ""

    def _local_rule_json(
        self,
        system: str,
        user: str,
        history_weights: list[Decimal],
        horizon_days: int,
        warehouse: str,
        variety: str,
        start_date: date,
    ) -> dict[str, Any]:
        """本地规则后备 — 当所有远程 AI 供应商不可用时的兜底预测。"""
        _ = system, user

        # 计算历史日发货量均值（基于调用方传入的日聚合数据）
        if history_weights:
            avg = sum(float(w) for w in history_weights) / len(history_weights)
        else:
            avg = 0.0

        items: list[dict[str, Any]] = []
        for i in range(horizon_days):
            # 简单加权：围绕均值做小幅波动，模拟周期性
            factor = 1.0 + 0.05 * ((i % 7) - 3) / 3.0
            w = max(0.0, round(avg * factor, 4))
            d = start_date + timedelta(days=i)
            ship_prob = "低" if w < 10 else ("中" if w < 50 else "高")
            items.append(
                {
                    "target_date": d.isoformat(),
                    "predicted_weight": w,
                    "ship_probability": ship_prob,
                    "confidence_level": "低",
                    "main_factors": "本地规则推算（所有AI供应商不可用，基于历史均值简单估算）",
                }
            )

        first_date = items[0]["target_date"] if items else "N/A"
        last_date = items[-1]["target_date"] if items else "N/A"
        report = (
            f"【本地规则后备预测】\n"
            f"所有远程 AI 供应商当前不可用，使用本地规则基于历史数据（{len(history_weights)} 条记录，"
            f"日均发货 {avg:.1f} 吨）进行简单估算。\n"
            f"预测范围：{first_date} 至 {last_date}，共 {horizon_days} 天。\n"
            f"注意：本地规则仅做均值加权推算，不包含价格/节假日/天气等分析维度，"
            f"预测结果仅供参考。建议稍后重试以获取完整 AI 分析。"
        )
        return {"analysis_report": report, "items": items}

    def _emit_ai_call_log(
        self,
        *,
        system: str,
        user: str,
        warehouse: str,
        product_variety: str,
        start_date: date,
        provider: str,
        model: str | None,
        latency_ms: float,
        cost_usd: float | None,
        errors: list[str],
        raw_response: str,
        parsed: dict[str, Any] | None,
        parse_error: str | None = None,
    ) -> None:
        log_ai_call(
            provider=provider,
            model=model,
            warehouse=warehouse,
            product_variety=product_variety,
            start_date=start_date.isoformat(),
            system_prompt=system,
            user_prompt=user,
            latency_ms=latency_ms,
            cost_usd=cost_usd,
            errors=errors,
            raw_response=raw_response,
            parsed_json=parsed,
            parse_error=parse_error,
        )

    async def complete_with_fallback(
        self,
        system: str,
        user: str,
        *,
        history_weights: list[Decimal],
        horizon_days: int,
        warehouse: str,
        product_variety: str,
        start_date: date,
    ) -> tuple[dict[str, Any], str, float, float | None, str, list[str]]:
        """依次尝试供应商，失败则本地规则。"""
        errors: list[str] = []
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            if settings.openai_api_key:
                url = f"{settings.openai_api_base.rstrip('/')}/chat/completions"
                headers = {
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                }
                parsed, prov, lat, cost, raw, err = await self._call_openai_compatible(
                    session,
                    url,
                    headers,
                    settings.openai_model,
                    system,
                    user,
                    force_json=True,
                )
                if parsed is not None:
                    self._emit_ai_call_log(
                        system=system,
                        user=user,
                        warehouse=warehouse,
                        product_variety=product_variety,
                        start_date=start_date,
                        provider=prov,
                        model=settings.openai_model,
                        latency_ms=lat,
                        cost_usd=cost,
                        errors=errors,
                        raw_response=raw,
                        parsed=parsed,
                    )
                    return parsed, prov, lat, cost, raw[:2000], errors
                errors.append(f"openai:{err}")

            if settings.azure_openai_api_key and settings.azure_openai_endpoint and settings.azure_openai_deployment:
                ep = settings.azure_openai_endpoint.rstrip("/")
                url = (
                    f"{ep}/openai/deployments/{settings.azure_openai_deployment}"
                    f"/chat/completions?api-version={settings.azure_openai_api_version}"
                )
                headers = {
                    "api-key": settings.azure_openai_api_key,
                    "Content-Type": "application/json",
                }
                parsed, prov, lat, cost, raw, err = await self._call_openai_compatible(
                    session,
                    url,
                    headers,
                    settings.azure_openai_deployment,
                    system,
                    user,
                    force_json=True,
                )
                if parsed is not None:
                    self._emit_ai_call_log(
                        system=system,
                        user=user,
                        warehouse=warehouse,
                        product_variety=product_variety,
                        start_date=start_date,
                        provider=prov,
                        model=settings.azure_openai_deployment,
                        latency_ms=lat,
                        cost_usd=cost,
                        errors=errors,
                        raw_response=raw,
                        parsed=parsed,
                    )
                    return parsed, prov, lat, cost, raw[:2000], errors
                errors.append(f"azure:{err}")

            if settings.anthropic_api_key:
                parsed, prov, lat, cost, raw, err = await self._call_anthropic(session, system, user)
                if parsed is not None:
                    self._emit_ai_call_log(
                        system=system,
                        user=user,
                        warehouse=warehouse,
                        product_variety=product_variety,
                        start_date=start_date,
                        provider=prov,
                        model=settings.anthropic_model,
                        latency_ms=lat,
                        cost_usd=cost,
                        errors=errors,
                        raw_response=raw,
                        parsed=parsed,
                    )
                    return parsed, prov, lat, cost, raw[:2000], errors
                errors.append(f"anthropic:{err}")

        logger.warning("ai_client all remote providers failed, using local rule: %s", errors)
        t0 = time.perf_counter()
        parsed = self._local_rule_json(
            system,
            user,
            history_weights,
            horizon_days,
            warehouse,
            product_variety,
            start_date,
        )
        lat = (time.perf_counter() - t0) * 1000.0
        self._emit_ai_call_log(
            system=system,
            user=user,
            warehouse=warehouse,
            product_variety=product_variety,
            start_date=start_date,
            provider="local_rule",
            model=None,
            latency_ms=lat,
            cost_usd=None,
            errors=errors,
            raw_response="",
            parsed=parsed,
            parse_error="remote providers failed, local_rule fallback",
        )
        return parsed, "local_rule", lat, None, "", errors


def get_ai_client() -> AIModelClient:
    """FastAPI 依赖工厂。"""
    return AIModelClient()
