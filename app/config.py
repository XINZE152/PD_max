import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from app.paths import PROJECT_ROOT

# 始终加载项目根 .env，不依赖进程当前工作目录（避免 uvicorn、Docker、IDE 启动路径不一致）
load_dotenv(PROJECT_ROOT / ".env")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    # 去掉首尾空白，避免 .env 里误带空格/换行（尤其 MYSQL_PASSWORD）导致 MySQL 1045
    return value.strip()


# 数据库配置
MYSQL_HOST = _require_env("MYSQL_HOST")
MYSQL_PORT = int(_require_env("MYSQL_PORT"))
MYSQL_USER = _require_env("MYSQL_USER")
MYSQL_PASSWORD = _require_env("MYSQL_PASSWORD")
MYSQL_DATABASE = _require_env("MYSQL_DATABASE")
MYSQL_CHARSET = os.getenv("MYSQL_CHARSET", "utf8mb4")

# 文件上传目录（相对路径相对项目根，避免启动目录不同写到别处）
_raw_upload = (os.getenv("UPLOAD_DIR") or "uploads").strip() or "uploads"
_up_path = Path(_raw_upload)
UPLOAD_DIR = (
    str(_up_path.resolve())
    if _up_path.is_absolute()
    else str(PROJECT_ROOT / _raw_upload)
)


# JWT 认证配置
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change_this_to_a_strong_random_secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  # 默认 24 小时

# LLM API 配置（采购建议等文本接口，OpenAI 兼容协议）
# 未单独配置 LLM_API_KEY 时，按顺序复用 DASHSCOPE_API_KEY / QWEN_API_KEY / VLM_API_KEY（与报价图识别同源 key 时可少配一项）
_explicit_llm_key = os.getenv("LLM_API_KEY", "").strip()
LLM_API_KEY = (
    _explicit_llm_key
    or os.getenv("DASHSCOPE_API_KEY", "").strip()
    or os.getenv("QWEN_API_KEY", "").strip()
    or os.getenv("VLM_API_KEY", "").strip()
)
_llm_base_env = os.getenv("LLM_BASE_URL", "").strip()
if _llm_base_env:
    LLM_BASE_URL = _llm_base_env
elif _explicit_llm_key:
    LLM_BASE_URL = "https://api.anthropic.com"
else:
    # 使用兜底 key 时默认走阿里云百炼兼容端点（与 VLM 默认一致）
    LLM_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
_llm_model_env = os.getenv("LLM_MODEL", "").strip()
if _llm_model_env:
    LLM_MODEL = _llm_model_env
elif _explicit_llm_key:
    LLM_MODEL = "claude-sonnet-4-6"
else:
    LLM_MODEL = "qwen-plus"

# VLM API 配置
VLM_API_KEY = os.getenv("VLM_API_KEY", "")
VLM_BASE_URL = os.getenv("VLM_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
VLM_MODEL = os.getenv("VLM_MODEL", "qwen-vl-max-latest")

# 天地图地理编码（MAP_API_KEY 即文档中的 tk）
MAP_API_KEY = os.getenv("MAP_API_KEY", "").strip()
MAP_GEOCODER_URL = (
    os.getenv("MAP_GEOCODER_URL", "http://api.tianditu.gov.cn/geocoder").strip()
    or "http://api.tianditu.gov.cn/geocoder"
)

# 天地图不可用（403/网络/无 KEY）时是否仍写入仓库/冶炼厂，经纬度置 NULL（默认允许，避免阻断业务）
_MAP_ALLOW_NULL_RAW = os.getenv("MAP_GEOCODE_ALLOW_NULL", "1").strip().lower()
MAP_GEOCODE_ALLOW_NULL = _MAP_ALLOW_NULL_RAW in ("1", "true", "yes", "on")


def _optional_positive_int(name: str) -> Optional[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        v = int(raw)
        return v if v > 0 else None
    except ValueError:
        return None


# 上传百炼前将图最长边压到此值（像素），可明显缩短传输与视觉编码时间；不设则不缩放
VLM_IMAGE_MAX_EDGE = _optional_positive_int("VLM_IMAGE_MAX_EDGE")
_vlm_mt = os.getenv("VLM_MAX_TOKENS", "8192").strip()
try:
    VLM_MAX_TOKENS = max(1024, min(32768, int(_vlm_mt)))
except ValueError:
    VLM_MAX_TOKENS = 8192
try:
    VLM_JPEG_QUALITY = max(60, min(100, int(os.getenv("VLM_JPEG_QUALITY", "88"))))
except ValueError:
    VLM_JPEG_QUALITY = 88
try:
    # 调用百炼/兼容 OpenAI 接口的单次 HTTP 读超时（秒），应 ≥ 上游最慢一次 VLM 耗时，且不大于 Nginx proxy_read_timeout
    VLM_REQUEST_TIMEOUT = max(60.0, float(os.getenv("VLM_REQUEST_TIMEOUT", "600")))
except ValueError:
    VLM_REQUEST_TIMEOUT = 600.0


def _env_enabled(name: str, *, default: bool = True) -> bool:
    """环境变量开关：未设置时用 default；0/false/off 为关，1/true/on 为开。"""
    raw = os.getenv(name)
    if raw is None:
        return default
    v = raw.strip().lower()
    if v in ("0", "false", "no", "off", "disabled"):
        return False
    if v in ("1", "true", "yes", "on", "enabled"):
        return True
    return default


# 为 0 时不注册 /ai-detection/*、不启动鉴伪 GC/任务表、不预加载模型（省内存）
AI_DETECTION_ENABLED = _env_enabled("AI_DETECTION_ENABLED", default=True)

# 为 0 时不注册智能预测相关路由、不连 Redis 预热线程、不启动定时预测调度
INTELLIGENT_PREDICTION_ENABLED = _env_enabled("INTELLIGENT_PREDICTION_ENABLED", default=True)
