"""文本 LLM 统一客户端：OpenAI SDK 与 HTTP 兼容请求共用 DeepSeek 等扩展参数。"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app import config as app_config

logger = logging.getLogger(__name__)


def create_llm_client():
    """创建 OpenAI 兼容 SDK 客户端。"""
    from openai import OpenAI

    return OpenAI(
        api_key=app_config.LLM_API_KEY,
        base_url=app_config.LLM_BASE_URL,
    )


def merge_llm_completion_kwargs(
    *,
    disable_thinking: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    """合并 DeepSeek 专有参数（reasoning_effort、thinking）到 completion 请求。"""
    out = dict(kwargs)
    if not app_config.is_deepseek_llm():
        return out
    if disable_thinking:
        logger.debug("DeepSeek thinking 已关闭（调用方要求，如 JSON 输出场景）")
        return out
    if app_config.LLM_THINKING_ENABLED:
        effort = (app_config.LLM_REASONING_EFFORT or "high").strip() or "high"
        out["reasoning_effort"] = effort
        extra = dict(out.pop("extra_body", None) or {})
        extra.setdefault("thinking", {"type": "enabled"})
        out["extra_body"] = extra
    return out


def build_openai_compatible_body(
    model: str,
    messages: List[Dict[str, str]],
    *,
    disable_thinking: bool = False,
    **extra: Any,
) -> Dict[str, Any]:
    """构造 OpenAI 兼容 Chat Completions HTTP POST body。"""
    body: Dict[str, Any] = {"model": model, "messages": messages}
    body.update(extra)
    return merge_llm_completion_kwargs(disable_thinking=disable_thinking, **body)


def chat_completions_create(client, *, disable_thinking: bool = False, **kwargs):
    """调用 chat.completions.create，自动注入 DeepSeek 参数。"""
    merged = merge_llm_completion_kwargs(disable_thinking=disable_thinking, **kwargs)
    return client.chat.completions.create(**merged)
