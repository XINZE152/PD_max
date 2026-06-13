"""将每次 AI 调用的输入与输出追加写入文本日志文件。"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.settings import settings

logger = get_logger(__name__)

_lock = threading.Lock()
_SEPARATOR = "=" * 80


def _truncate(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n... [truncated, total {len(text)} chars]"


def _resolve_log_path() -> Path:
    raw = (settings.ai_call_log_path or "").strip()
    if raw:
        p = Path(raw)
        if not p.is_absolute():
            from app.paths import PROJECT_ROOT

            p = PROJECT_ROOT / p
    else:
        from app.paths import PROJECT_ROOT

        p = PROJECT_ROOT / "logs" / "ai_prediction_calls.log"
    return p


def log_ai_call(
    *,
    provider: str,
    model: Optional[str] = None,
    warehouse: str = "",
    product_variety: str = "",
    start_date: Optional[str] = None,
    system_prompt: str,
    user_prompt: str,
    latency_ms: float,
    cost_usd: Optional[float] = None,
    errors: Optional[list[str]] = None,
    raw_response: str = "",
    parsed_json: Optional[dict[str, Any]] = None,
    parse_error: Optional[str] = None,
) -> None:
    """追加一条 AI 调用记录（同步写文件，带锁）。"""
    if not settings.ai_call_log_enabled:
        return

    path = _resolve_log_path()
    max_c = settings.ai_call_log_max_chars
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [
        _SEPARATOR,
        f"timestamp: {ts}",
        f"provider: {provider}",
        f"model: {model or '-'}",
        f"warehouse: {warehouse or '-'}",
        f"product_variety: {product_variety or '-'}",
        f"prediction_start_date: {start_date or '-'}",
        f"latency_ms: {latency_ms:.2f}",
        f"cost_usd: {cost_usd}",
        f"errors_before_success: {errors or []}",
        "",
        "--- INPUT system ---",
        _truncate(system_prompt, max_c),
        "",
        "--- INPUT user ---",
        _truncate(user_prompt, max_c),
        "",
        "--- OUTPUT raw ---",
        _truncate(raw_response or "(empty)", max_c),
        "",
        "--- OUTPUT parsed JSON ---",
        _truncate(
            ""
            if parsed_json is None
            else json.dumps(parsed_json, ensure_ascii=False, indent=2, default=str),
            max_c,
        ),
        f"parse_error: {parse_error or ''}",
        "",
    ]

    body = "\n".join(lines) + "\n"

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with _lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(body)
    except OSError as e:
        logger.warning("ai_call_log_write_failed path=%s err=%s", path, e)