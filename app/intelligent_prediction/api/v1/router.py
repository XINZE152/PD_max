"""汇总智能预测 v1 路由。"""

from __future__ import annotations

from fastapi import APIRouter

from app.intelligent_prediction.api.v1 import forecast, history, knowledge, predict

intelligent_prediction_router = APIRouter()
intelligent_prediction_router.include_router(predict.router, prefix="/predict", tags=["智能预测"])
intelligent_prediction_router.include_router(forecast.router, prefix="/forecast", tags=["规则预测"])
intelligent_prediction_router.include_router(history.router, prefix="/delivery-history", tags=["送货历史"])
intelligent_prediction_router.include_router(
    history.router,
    prefix="/history",
    tags=["送货历史"],
    include_in_schema=False,
)
intelligent_prediction_router.include_router(knowledge.router, prefix="/knowledge", tags=["知识库预留"])
