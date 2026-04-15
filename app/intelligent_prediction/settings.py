"""智能預測模組設定：讀環境變數，並與主應用 app.config（JWT、MySQL、LLM）對齊。"""

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
    """與主程式一致：顯式 OPENAI_API_KEY 優先，否則沿用 app.config 的 LLM_API_KEY（含百鍊等兜底）。"""
    explicit = (os.getenv("OPENAI_API_KEY") or "").strip()
    if explicit:
        return explicit
    return (app_config.LLM_API_KEY or "").strip()


def _openai_base_chain() -> str:
    """顯式 OPENAI_API_BASE 優先；僅在設了專用 OpenAI Key 時預設 openai.com，其餘跟主程式 LLM_BASE_URL。"""
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


def load_intelligent_prediction_settings() -> IntelligentPredictionSettings:
    return IntelligentPredictionSettings()


settings = load_intelligent_prediction_settings()
