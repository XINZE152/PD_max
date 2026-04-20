"""与主项目一致的 logger 获取方式（避免依赖 PD 的 app.core.logging）。"""

from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
