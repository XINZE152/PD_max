"""从非严格 JSON 文本中提取 JSON 对象。"""

from __future__ import annotations

import json
import re
from typing import Any


def extract_json_object(text: str) -> tuple[dict[str, Any] | None, str | None]:
    """尝试从模型输出中解析 JSON 对象。"""
    if not text or not text.strip():
        return None, "empty_response"
    stripped = text.strip()
    try:
        data = json.loads(stripped)
        if isinstance(data, dict):
            return data, None
        return None, "root_not_object"
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", stripped)
    if match:
        chunk = match.group(0)
        try:
            data = json.loads(chunk)
            if isinstance(data, dict):
                return data, None
            return None, "regex_root_not_object"
        except json.JSONDecodeError as e:
            return None, f"json_decode_error:{e.msg}"

    return None, "no_json_object_found"
