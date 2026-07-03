import os

from fastapi import APIRouter

from app import config as app_config
from app.api.v1.routes import auth, tl
from app.vertical_warehouse_ai import vertical_warehouse_ai_router

api_router = APIRouter()
api_router.include_router(tl.router, tags=["TL比价模块"])
api_router.include_router(vertical_warehouse_ai_router)
api_router.include_router(auth.router, tags=["用户认证"])
if app_config.AI_DETECTION_ENABLED:
    from app.api.v1.routes import ai_detection

    api_router.include_router(ai_detection.router, tags=["AI鉴伪模块"])
if os.getenv("FORGEGUARD_ENABLED", "").strip().lower() in ("1", "true", "yes"):
    from app.api.v1.routes import ai_detection_forgeguard

    api_router.include_router(ai_detection_forgeguard.router, tags=["ForgeGuard 检测"])
if app_config.INTELLIGENT_PREDICTION_ENABLED:
    from app.intelligent_prediction.api.v1.router import intelligent_prediction_router

    api_router.include_router(intelligent_prediction_router, tags=["智能预测模块"])
