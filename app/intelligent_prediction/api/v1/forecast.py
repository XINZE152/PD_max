"""PRD 规则预测：图表、明细分页、导出。"""

from __future__ import annotations

import io
from datetime import date, datetime, timedelta

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.exceptions import (
    BusinessException,
    INTERNAL_SERVER_ERROR_MESSAGE,
    ValidationBusinessException,
)
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.api.audit_deps import AuditActor, get_audit_actor
from app.intelligent_prediction.api.deps import get_prediction_db_session
from app.intelligent_prediction.schemas.dimensions import DimensionListsResponse
from app.intelligent_prediction.schemas.forecast import PrdForecastChartResponse, PrdForecastDetailResponse, PrdForecastQuery
from app.intelligent_prediction.services.audit_service import append_audit, write_audit_standalone
from app.intelligent_prediction.services.dimension_options_service import (
    list_dimensions_from_delivery_history,
)
from app.intelligent_prediction.services.prd_forecast_service import PrdForecastService, get_prd_forecast_service

logger = get_logger(__name__)
router = APIRouter()


def _merge_list(primary: list[str], legacy: str | None) -> list[str]:
    out = [x.strip() for x in primary if x and str(x).strip()]
    if legacy and legacy.strip() and legacy.strip() not in out:
        out.append(legacy.strip())
    return out


def _prd_query(
    *,
    date_from: date | None,
    date_to: date | None,
    regional_managers: list[str],
    regional_manager: str | None,
    warehouses: list[str],
    warehouse: str | None,
    product_varieties: list[str],
    product_variety: str | None,
    smelters: list[str],
    smelter: str | None,
    page: int,
    page_size: int,
) -> PrdForecastQuery:
    today = date.today()
    df = date_from or today
    dt = date_to or (today + timedelta(days=14))
    if df > dt:
        df, dt = dt, df
    return PrdForecastQuery(
        date_from=df,
        date_to=dt,
        regional_managers=_merge_list(regional_managers, regional_manager),
        warehouses=_merge_list(warehouses, warehouse),
        product_varieties=_merge_list(product_varieties, product_variety),
        smelters=_merge_list(smelters, smelter),
        page=page,
        page_size=page_size,
    )


@router.get(
    "/chart",
    response_model=PrdForecastChartResponse,
    summary="送货量预测图表数据",
    description="按日期区间与筛选条件返回汇总曲线及按区域经理拆分的序列；支持按冶炼厂筛选。",
)
async def prd_forecast_chart(
    date_from: date | None = Query(None, description="预测区间起点，默认当天"),
    date_to: date | None = Query(None, description="预测区间终点，默认当天+14"),
    regional_manager: str | None = Query(None, description="区域经理（单值，兼容旧参数）"),
    regional_managers: list[str] = Query(default=[], description="区域经理（多值）"),
    warehouse: str | None = Query(None, description="仓库（单值）"),
    warehouses: list[str] = Query(default=[], description="仓库（多值）"),
    product_variety: str | None = Query(None, description="品种（单值）"),
    product_varieties: list[str] = Query(default=[], description="品种（多值）"),
    smelter: str | None = Query(None, description="冶炼厂（单值）"),
    smelters: list[str] = Query(default=[], description="冶炼厂（多值）"),
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: PrdForecastService = Depends(get_prd_forecast_service),
) -> PrdForecastChartResponse:
    q = _prd_query(
        date_from=date_from,
        date_to=date_to,
        regional_managers=regional_managers,
        regional_manager=regional_manager,
        warehouses=warehouses,
        warehouse=warehouse,
        product_varieties=product_varieties,
        product_variety=product_variety,
        smelters=smelters,
        smelter=smelter,
        page=1,
        page_size=1,
    )
    try:
        return await svc.chart_only(session, q)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("prd_forecast_chart failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.get(
    "/details",
    response_model=PrdForecastDetailResponse,
    summary="送货量预测明细分页",
    description="返回规则模型计算的逐日、逐仓、逐品种、逐冶炼厂预测明细。",
)
async def prd_forecast_detail(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(50, ge=1, le=500, description="每页条数"),
    date_from: date | None = Query(None, description="预测区间起点"),
    date_to: date | None = Query(None, description="预测区间终点"),
    regional_manager: str | None = Query(None, description="区域经理（单值）"),
    regional_managers: list[str] = Query(default=[], description="区域经理（多值）"),
    warehouse: str | None = Query(None, description="仓库（单值）"),
    warehouses: list[str] = Query(default=[], description="仓库（多值）"),
    product_variety: str | None = Query(None, description="品种（单值）"),
    product_varieties: list[str] = Query(default=[], description="品种（多值）"),
    smelter: str | None = Query(None, description="冶炼厂（单值）"),
    smelters: list[str] = Query(default=[], description="冶炼厂（多值）"),
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: PrdForecastService = Depends(get_prd_forecast_service),
) -> PrdForecastDetailResponse:
    q = _prd_query(
        date_from=date_from,
        date_to=date_to,
        regional_managers=regional_managers,
        regional_manager=regional_manager,
        warehouses=warehouses,
        warehouse=warehouse,
        product_varieties=product_varieties,
        product_variety=product_variety,
        smelters=smelters,
        smelter=smelter,
        page=page,
        page_size=page_size,
    )
    try:
        return await svc.detail_page(session, q)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("prd_forecast_detail failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.get(
    "/export",
    summary="导出送货量预测 Excel",
    description="按当前筛选条件导出全部明细为 xlsx 流。",
)
async def prd_forecast_export(
    date_from: date | None = Query(None, description="预测区间起点"),
    date_to: date | None = Query(None, description="预测区间终点"),
    regional_manager: str | None = Query(None, description="区域经理（单值）"),
    regional_managers: list[str] = Query(default=[], description="区域经理（多值）"),
    warehouse: str | None = Query(None, description="仓库（单值）"),
    warehouses: list[str] = Query(default=[], description="仓库（多值）"),
    product_variety: str | None = Query(None, description="品种（单值）"),
    product_varieties: list[str] = Query(default=[], description="品种（多值）"),
    smelter: str | None = Query(None, description="冶炼厂（单值）"),
    smelters: list[str] = Query(default=[], description="冶炼厂（多值）"),
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: PrdForecastService = Depends(get_prd_forecast_service),
    actor: AuditActor = Depends(get_audit_actor),
) -> StreamingResponse:
    q = _prd_query(
        date_from=date_from,
        date_to=date_to,
        regional_managers=regional_managers,
        regional_manager=regional_manager,
        warehouses=warehouses,
        warehouse=warehouse,
        product_varieties=product_varieties,
        product_variety=product_variety,
        smelters=smelters,
        smelter=smelter,
        page=1,
        page_size=10**9,
    )
    try:
        rows, _chart = await svc.compute(session, q)
        fn = f"送货量预测_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df = pd.DataFrame([r.model_dump() for r in rows])
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        await append_audit(
            session,
            "prd_forecast_export",
            resource=fn,
            detail={"rows": len(rows), "date_from": str(q.date_from), "date_to": str(q.date_to)},
            actor=actor,
        )
        headers = {"Content-Disposition": f'attachment; filename="{fn}"'}
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("prd_forecast_export failed")
        await write_audit_standalone(
            "prd_forecast_export_failed",
            detail={"error": str(e)},
            actor=actor,
        )
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.get(
    "/dimension-options",
    response_model=DimensionListsResponse,
    summary="规则预测筛选维度列表",
    description=(
        "与送货历史同源：从 ``pd_ip_delivery_records`` 去重返回大区经理、仓库、冶炼厂，"
        "供 ``/forecast/chart``、``/forecast/details`` 等筛选参数候选。"
    ),
)
async def forecast_dimension_options(
    session: AsyncSession = Depends(get_prediction_db_session),
) -> DimensionListsResponse:
    try:
        return await list_dimensions_from_delivery_history(session)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("forecast_dimension_options failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e
