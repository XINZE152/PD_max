"""智能预测模块配置：读取环境变量，并与主应用 app.config（JWT、MySQL、LLM）对齐。"""

from __future__ import annotations

import os
import urllib.parse
from dotenv import load_dotenv
from pydantic import BaseModel, Field

from app import config as app_config
from app.paths import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")


def _build_prediction_async_database_url() -> str:
    explicit = os.getenv("PREDICTION_ASYNC_DATABASE_URL")
    if explicit and explicit.strip():
        return explicit.strip()
    host = os.getenv("MYSQL_HOST")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DATABASE")
    port = os.getenv("MYSQL_PORT", "3306")
    charset = os.getenv("MYSQL_CHARSET", "utf8mb4")
    if not host or not user or not database:
        return ""
    u = urllib.parse.quote_plus(user)
    p = urllib.parse.quote_plus(password)
    return f"mysql+aiomysql://{u}:{p}@{host}:{port}/{database}?charset={charset}"


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _openai_key_chain() -> str:
    """与主程序一致：显式 OPENAI_API_KEY 优先，否则沿用 app.config 的 LLM_API_KEY（含百炼等兜底）。"""
    explicit = (os.getenv("OPENAI_API_KEY") or "").strip()
    if explicit:
        return explicit
    return (app_config.LLM_API_KEY or "").strip()


def _openai_base_chain() -> str:
    """显式 OPENAI_API_BASE 优先；仅在配置了专用 OpenAI Key 时默认 openai.com，其余跟随主程序 LLM_BASE_URL。"""
    ob = (os.getenv("OPENAI_API_BASE") or "").strip()
    if ob:
        return ob
    if (os.getenv("OPENAI_API_KEY") or "").strip():
        return "https://api.openai.com/v1"
    return app_config.LLM_BASE_URL


def _openai_model_chain() -> str:
    om = (os.getenv("OPENAI_MODEL") or "").strip()
    if om:
        return om
    if (os.getenv("OPENAI_API_KEY") or "").strip():
        return "gpt-4o-mini"
    return app_config.LLM_MODEL


class IntelligentPredictionSettings(BaseModel):
    prediction_async_db_url: str = Field(default_factory=_build_prediction_async_database_url)
    prediction_redis_url: str = Field(default_factory=lambda: os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    celery_broker_url: str = Field(default_factory=lambda: os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/1"))
    celery_result_backend: str = Field(
        default_factory=lambda: os.getenv("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/2")
    )

    openai_api_key: str = Field(default_factory=_openai_key_chain)
    openai_api_base: str = Field(default_factory=_openai_base_chain)
    openai_model: str = Field(default_factory=_openai_model_chain)
    azure_openai_api_key: str = Field(default_factory=lambda: (os.getenv("AZURE_OPENAI_API_KEY") or "").strip())
    azure_openai_endpoint: str = Field(default_factory=lambda: (os.getenv("AZURE_OPENAI_ENDPOINT") or "").strip())
    azure_openai_deployment: str = Field(default_factory=lambda: (os.getenv("AZURE_OPENAI_DEPLOYMENT") or "").strip())
    azure_openai_api_version: str = Field(
        default_factory=lambda: os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
    )
    anthropic_api_key: str = Field(default_factory=lambda: (os.getenv("ANTHROPIC_API_KEY") or "").strip())
    anthropic_model: str = Field(default_factory=lambda: os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022"))

    ai_call_log_enabled: bool = Field(
        default_factory=lambda: _env_bool("AI_CALL_LOG_ENABLED", True),
        description="是否将每次 AI 预测调用的输入/输出追加写入文本日志",
    )
    ai_call_log_path: str = Field(
        default_factory=lambda: (os.getenv("AI_CALL_LOG_PATH") or "logs/ai_prediction_calls.log").strip(),
        description="AI 调用日志文件路径（相对项目根或绝对路径）",
    )
    ai_call_log_max_chars: int = Field(
        default_factory=lambda: _env_int("AI_CALL_LOG_MAX_CHARS", 500_000),
        description="单段 prompt/响应在日志中最多保留字符数，超出截断",
    )

    ai_request_timeout_seconds: float = Field(default_factory=lambda: _env_float("AI_REQUEST_TIMEOUT_SECONDS", 120.0))
    prediction_redis_ttl_seconds: int = Field(default_factory=lambda: _env_int("PREDICTION_REDIS_TTL_SECONDS", 3600))
    prompt_memory_ttl_seconds: int = Field(default_factory=lambda: _env_int("PROMPT_MEMORY_TTL_SECONDS", 300))
    openai_input_price_per_1k: float = Field(default_factory=lambda: _env_float("OPENAI_INPUT_PRICE_PER_1K", 0.005))
    openai_output_price_per_1k: float = Field(default_factory=lambda: _env_float("OPENAI_OUTPUT_PRICE_PER_1K", 0.015))
    prediction_prometheus_enabled: bool = Field(
        default_factory=lambda: _env_bool("PREDICTION_PROMETHEUS_INSTRUMENTATOR", False)
    )
    enable_manual_db_init: bool = Field(default_factory=lambda: _env_bool("ENABLE_MANUAL_DB_INIT", False))

    intelligent_prediction_schedule_enabled: bool = Field(
        default_factory=lambda: _env_bool("INTELLIGENT_PREDICTION_SCHEDULE_ENABLED", False)
    )
    intelligent_prediction_schedule_horizon_days: int = Field(
        default_factory=lambda: _env_int("INTELLIGENT_PREDICTION_SCHEDULE_HORIZON_DAYS", 30)
    )
    intelligent_prediction_schedule_max_items: int = Field(
        default_factory=lambda: _env_int("INTELLIGENT_PREDICTION_SCHEDULE_MAX_ITEMS", 50)
    )
    intelligent_prediction_schedule_cron_hour: int = Field(
        default_factory=lambda: _env_int("INTELLIGENT_PREDICTION_SCHEDULE_CRON_HOUR", 2)
    )
    intelligent_prediction_schedule_cron_minute: int = Field(
        default_factory=lambda: _env_int("INTELLIGENT_PREDICTION_SCHEDULE_CRON_MINUTE", 30)
    )
    intelligent_prediction_history_purge_secret: str = Field(
        default_factory=lambda: (os.getenv("INTELLIGENT_PREDICTION_HISTORY_PURGE_SECRET") or "").strip()
    )

    prediction_price_weight: float = Field(
        default_factory=lambda: _env_float("PREDICTION_PRICE_WEIGHT", 0.8),
        description="送货量预测中价格因素权重（默认 0.8）",
    )
    prediction_history_weight: float = Field(
        default_factory=lambda: _env_float("PREDICTION_HISTORY_WEIGHT", 0.2),
        description="送货量预测中历史规律权重（默认 0.2）",
    )
    prediction_price_sensitivity_threshold: float = Field(
        default_factory=lambda: _env_float("PREDICTION_PRICE_SENSITIVITY_THRESHOLD", 0.35),
        description="库房价格敏感度判定：|相关系数|>=阈值视为敏感型",
    )
    prediction_default_forecast_days: int = Field(
        default_factory=lambda: _env_int("PREDICTION_DEFAULT_FORECAST_DAYS", 15),
        description="规则预测默认 horizon（天）",
    )

    #: 天气 API 根地址（留空则导入时不请求天气，weather_json 为 NULL）
    weather_api_base_url: str = Field(default_factory=lambda: (os.getenv("WEATHER_API_BASE_URL") or "").strip())
    weather_api_key: str = Field(default_factory=lambda: (os.getenv("WEATHER_API_KEY") or "").strip())

    # 综合预测（v2）权重配置：历史规律 > 价格竞争力 > 价格敏感度 > 节假日 > 天气物流
    prediction_v2_history_weight: float = Field(
        default_factory=lambda: _env_float("PREDICTION_V2_HISTORY_WEIGHT", 0.40),
        description="综合预测中历史规律权重（默认 0.40）",
    )
    prediction_v2_price_weight: float = Field(
        default_factory=lambda: _env_float("PREDICTION_V2_PRICE_WEIGHT", 0.30),
        description="综合预测中价格竞争力权重（默认 0.30）",
    )
    prediction_v2_sensitivity_weight: float = Field(
        default_factory=lambda: _env_float("PREDICTION_V2_SENSITIVITY_WEIGHT", 0.15),
        description="综合预测中仓库价格敏感度权重（默认 0.15）",
    )
    prediction_v2_holiday_weight: float = Field(
        default_factory=lambda: _env_float("PREDICTION_V2_HOLIDAY_WEIGHT", 0.10),
        description="综合预测中节假日权重（默认 0.10）",
    )
    prediction_v2_weather_weight: float = Field(
        default_factory=lambda: _env_float("PREDICTION_V2_WEATHER_WEIGHT", 0.05),
        description="综合预测中天气物流权重（默认 0.05）",
    )


def load_intelligent_prediction_settings() -> IntelligentPredictionSettings:
    return IntelligentPredictionSettings()


settings = load_intelligent_prediction_settings()
