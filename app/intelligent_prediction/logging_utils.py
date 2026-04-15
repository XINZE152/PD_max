"""與主專案一致的 logger 取得方式（避免依賴 PD 的 app.core.logging）。"""

from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
