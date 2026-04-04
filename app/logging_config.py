import logging
import logging.handlers
import os
import sys
from pathlib import Path

from app.paths import PROJECT_ROOT

# 避免用「root 是否已有 handlers」判断：其它库或运行环境可能先挂过 handler（甚至 NullHandler），
# 会导致此处直接 return，应用侧 StreamHandler 从未添加，表现为「没有任何业务日志」。
_handlers_installed = False


def _short_logger_name(name: str) -> str:
    """日志里只保留模块短名，避免 app.services.xxx 占满一行。"""
    n = name[4:] if name.startswith("app.") else name
    return n.rsplit(".", 1)[-1]


class _CompactFormatter(logging.Formatter):
    """asctime 不含毫秒；每条：时间 级别 短logger名 消息。"""

    def format(self, record: logging.LogRecord) -> str:
        record.log_short = _short_logger_name(record.name)
        return super().format(record)


def _parse_log_level(value: str) -> int:
    level_name = (value or "INFO").upper().strip()
    return getattr(logging, level_name, logging.INFO)


def _env_flag(name: str, default: bool = True) -> bool:
    """未设置环境变量时返回 default；设为 0/false/no/off 为关，1/true/yes/on 为开。"""
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    v = str(raw).strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    if v in ("1", "true", "yes", "on"):
        return True
    return default


def _resolve_log_file_path() -> str:
    """
    优先级：LOG_FILE 显式路径 > LOG_ENABLE_FILE 开启时用 LOG_DIR/pd-max.log。
    相对路径一律相对项目根，避免 cwd 不同写到错误目录。
    """
    explicit = os.getenv("LOG_FILE", "").strip()
    if explicit:
        p = Path(explicit)
        return str(p if p.is_absolute() else (PROJECT_ROOT / p))
    if _env_flag("LOG_ENABLE_FILE", default=False):
        log_dir = (os.getenv("LOG_DIR") or "logs").strip() or "logs"
        d = Path(log_dir)
        base = d if d.is_absolute() else (PROJECT_ROOT / d)
        return str(base / "pd-max.log")
    return ""


def setup_logging() -> None:
    """初始化项目日志：handlers 只装一次；LOG_LEVEL 每次生效（便于 reload 后读 .env）。"""
    global _handlers_installed
    root_logger = logging.getLogger()
    level = _parse_log_level(os.getenv("LOG_LEVEL", "INFO"))
    root_logger.setLevel(level)

    if _handlers_installed:
        return

    formatter = _CompactFormatter(
        fmt="%(asctime)s %(levelname)s %(log_short)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if _env_flag("LOG_ENABLE_CONSOLE", default=True):
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    log_file = _resolve_log_file_path()
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if not root_logger.handlers:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    _handlers_installed = True

    # 第三方 HTTP 客户端默认 INFO 会刷屏，一般只需告警以上
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    cfg = logging.getLogger(__name__)
    cfg.info(
        "日志就绪 console=%s file=%s",
        _env_flag("LOG_ENABLE_CONSOLE", default=True),
        log_file or "-",
    )
