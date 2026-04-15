"""智能预测 FastAPI 依赖。"""

from __future__ import annotations

from fastapi import Depends

from app.intelligent_prediction.db import get_prediction_db_session
from app.intelligent_prediction.services.ai_client import AIModelClient, get_ai_client
from app.intelligent_prediction.services.cache_manager import CacheManager, get_cache_manager
from app.intelligent_prediction.services.history_service import HistoryService, get_history_service
from app.intelligent_prediction.services.prediction_service import PredictionService, get_prediction_service
from app.intelligent_prediction.services.prompt_builder import PromptBuilder


def get_ai_client_dep() -> AIModelClient:
    return get_ai_client()


def get_cache_manager_dep() -> CacheManager:
    return get_cache_manager()


def get_history_service_dep() -> HistoryService:
    return get_history_service()


def get_prediction_service_dep(
    ai: AIModelClient = Depends(get_ai_client_dep),
    cache: CacheManager = Depends(get_cache_manager_dep),
) -> PredictionService:
    return get_prediction_service(ai, cache, PromptBuilder())


__all__ = [
    "get_prediction_db_session",
    "get_ai_client_dep",
    "get_cache_manager_dep",
    "get_history_service_dep",
    "get_prediction_service_dep",
]
