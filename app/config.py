import os
from pathlib import Path

from dotenv import load_dotenv

from app.paths import PROJECT_ROOT

# 始终加载项目根 .env，不依赖进程当前工作目录（避免 uvicorn、Docker、IDE 启动路径不一致）
load_dotenv(PROJECT_ROOT / ".env")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


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
