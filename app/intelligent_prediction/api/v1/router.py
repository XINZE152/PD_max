"""汇总智能预测 v1 路由。"""

from __future__ import annotations

from fastapi import APIRouter

from app.intelligent_prediction.api.v1 import forecast, history, knowledge, predict

intelligent_prediction_router = APIRouter()
intelligent_prediction_router.include_router(predict.router, prefix="/预测", tags=["智能预测"])
intelligent_prediction_router.include_router(forecast.router, prefix="/送货量预测", tags=["规则预测"])
intelligent_prediction_router.include_router(history.router, prefix="/送货历史", tags=["送货历史"])
# 英文路径别名，兼容前端请求 /api/v1/history（与 /api/v1/送货历史 行为一致）
intelligent_prediction_router.include_router(
    history.router,
    prefix="/history",
    tags=["送货历史"],
    include_in_schema=False,
)
intelligent_prediction_router.include_router(knowledge.router, prefix="/知识库", tags=["知识库预留"])
