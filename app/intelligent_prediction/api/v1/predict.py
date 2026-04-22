"""预测 HTTP 接口。"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.exceptions import (
    BusinessException,
    INTERNAL_SERVER_ERROR_MESSAGE,
    ServiceUnavailableBusinessException,
)
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.api.deps import get_prediction_db_session, get_prediction_service_dep
from app.intelligent_prediction.models import PredictionBatch
from app.intelligent_prediction.models import PredictionResult as PredictionResultRow
from app.intelligent_prediction.schemas.audit import OperationAuditItem, OperationAuditListResponse
from app.intelligent_prediction.schemas.dict_addresses import (
    TlDictEntityAddress,
    WarehouseSmelterAddressLookupResponse,
)
from app.intelligent_prediction.schemas.dimensions import DimensionListsResponse
from app.intelligent_prediction.schemas.prediction import (
    AsyncPredictionAccepted,
    BatchPredictionRequest,
    BatchStatusResponse,
    PredictionResultSchema,
    StoredPredictionResultItem,
    StoredPredictionResultListResponse,
)
from app.intelligent_prediction.services.audit_service import list_audit_events
from app.intelligent_prediction.services.dimension_options_service import (
    list_dimensions_from_prediction_results,
)
from app.intelligent_prediction.services.dict_geo_lookup import (
    lookup_warehouse_smelter_dict_addresses,
)
from app.intelligent_prediction.services.prediction_service import PredictionService
from app.intelligent_prediction.tasks.export_tasks import run_prediction_batch_task

logger = get_logger(__name__)
router = APIRouter()


@router.get(
    "/dict-addresses",
    response_model=WarehouseSmelterAddressLookupResponse,
    summary="查询仓库与冶炼厂地址（TL 字典）",
    description=(
        "按名称从主库 ``dict_warehouses``、``dict_factories`` 解析省市区、详址与经纬度；"
        "名称匹配规则与送货历史导入时的地理解析一致（精确 → 去空白等 → 模糊择优）。"
    ),
)
def get_warehouse_smelter_dict_addresses(
    warehouse: str = Query(..., min_length=1, description="仓库名称"),
    smelter: str | None = Query(None, description="冶炼厂名称（可选）"),
) -> WarehouseSmelterAddressLookupResponse:
    sn = smelter.strip() if smelter and smelter.strip() else None
    wh_raw, sm_raw = lookup_warehouse_smelter_dict_addresses(warehouse.strip(), sn)
    return WarehouseSmelterAddressLookupResponse(
        warehouse=TlDictEntityAddress.model_validate(wh_raw) if wh_raw else None,
        smelter=TlDictEntityAddress.model_validate(sm_raw) if sm_raw else None,
    )


@router.get(
    "/operation-audit",
    response_model=OperationAuditListResponse,
    summary="分页查询智能预测操作审计",
    description="追溯导入、删除、导出、单条历史修改、定时预测等操作（何人、何时、何事）。",
)
async def list_operation_audit(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=200, description="每页条数"),
    action: str | None = Query(None, description="按动作类型精确筛选，如 history_import"),
    created_from: datetime | None = Query(None, description="创建时间起（含）"),
    created_to: datetime | None = Query(None, description="创建时间止（含）"),
    session: AsyncSession = Depends(get_prediction_db_session),
) -> OperationAuditListResponse:
    try:
        rows, total = await list_audit_events(
            session,
            page=page,
            page_size=page_size,
            action=action,
            created_from=created_from,
            created_to=created_to,
        )
        items = [OperationAuditItem.model_validate(r, from_attributes=True) for r in rows]
        return OperationAuditListResponse(
            total=total, page=page, page_size=page_size, items=items
        )
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("list_operation_audit failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post(
    "",
    response_model=list[PredictionResultSchema],
    summary="同步批量预测",
    description="调用大模型对多笔请求做预测，并将结果写入数据库。",
)
async def predict_sync(
    body: BatchPredictionRequest,
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: PredictionService = Depends(get_prediction_service_dep),
) -> list[PredictionResultSchema]:
    """同步批量预测并写库。"""
    try:
        results = await svc.predict_batch(body)
        await svc.persist_sync_results(session, results, batch_id=None)
        return results
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("predict_sync failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.post(
    "/async",
    response_model=AsyncPredictionAccepted,
    summary="异步批量预测",
    description="创建预测批次并入队 Celery，返回任务编号与批次编号。",
)
async def predict_async(
    body: BatchPredictionRequest,
    session: AsyncSession = Depends(get_prediction_db_session),
) -> AsyncPredictionAccepted:
    """异步预测：入队 Celery。"""
    batch: PredictionBatch | None = None
    try:
        batch = PredictionBatch(
            status="pending",
            meta=body.model_dump(mode="json"),
        )
        session.add(batch)
        await session.flush()
        predict_id_str = batch.id
        try:
            async_result = run_prediction_batch_task.delay(predict_id_str)
        except Exception as enqueue_err:
            logger.exception("predict_async celery enqueue failed batch_id=%s", predict_id_str)
            batch.status = "failed"
            batch.error_message = f"enqueue_failed: {enqueue_err}"[:2000]
            batch.completed_at = datetime.now(timezone.utc)
            await session.commit()
            raise ServiceUnavailableBusinessException(
                "异步预测任务无法入队，请检查 Celery Broker（如 CELERY_BROKER_URL）与 Worker 是否已启动",
            ) from enqueue_err
        batch.celery_task_id = async_result.id
        await session.flush()
        return AsyncPredictionAccepted(
            task_id=async_result.id,
            predict_id=uuid.UUID(predict_id_str),
            status="pending",
        )
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("predict_async failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.get(
    "/results",
    response_model=StoredPredictionResultListResponse,
    summary="分页查询预测结果",
    description="查询已落库的预测明细，支持按仓库、品种、冶炼厂、区域经理、批次、目标日期筛选。",
)
async def list_stored_prediction_results(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(20, ge=1, le=200, description="每页条数"),
    warehouse: str | None = Query(None, description="仓库（精确匹配）"),
    product_variety: str | None = Query(None, description="品种（精确匹配）"),
    smelter: str | None = Query(None, description="冶炼厂（精确匹配）"),
    regional_manager: str | None = Query(None, description="区域经理（精确匹配）"),
    batch_id: uuid.UUID | None = Query(None, description="异步批次 UUID"),
    target_date_from: date | None = Query(None, description="预测目标日期起（含）"),
    target_date_to: date | None = Query(None, description="预测目标日期止（含）"),
    session: AsyncSession = Depends(get_prediction_db_session),
) -> StoredPredictionResultListResponse:
    """分页查询已写入数据库的预测明细（含同步预测 batch_id 为空）。"""
    filters = []
    if warehouse and warehouse.strip():
        filters.append(PredictionResultRow.warehouse == warehouse.strip())
    if product_variety and product_variety.strip():
        filters.append(PredictionResultRow.product_variety == product_variety.strip())
    if smelter and smelter.strip():
        filters.append(PredictionResultRow.smelter == smelter.strip())
    if regional_manager and regional_manager.strip():
        filters.append(PredictionResultRow.regional_manager == regional_manager.strip())
    if batch_id is not None:
        filters.append(PredictionResultRow.batch_id == str(batch_id))
    if target_date_from is not None:
        filters.append(PredictionResultRow.target_date >= target_date_from)
    if target_date_to is not None:
        filters.append(PredictionResultRow.target_date <= target_date_to)

    count_stmt = select(func.count()).select_from(PredictionResultRow)
    stmt = select(PredictionResultRow)
    for f in filters:
        count_stmt = count_stmt.where(f)
        stmt = stmt.where(f)

    try:
        total_res = await session.execute(count_stmt)
        total = int(total_res.scalar_one())
        offset = (page - 1) * page_size
        stmt = stmt.order_by(PredictionResultRow.created_at.desc(), PredictionResultRow.id.desc())
        stmt = stmt.offset(offset).limit(page_size)
        res = await session.execute(stmt)
        rows = res.scalars().all()
        items = [StoredPredictionResultItem.model_validate(r) for r in rows]
        return StoredPredictionResultListResponse(
            total=total, page=page, page_size=page_size, items=items
        )
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("list_stored_prediction_results failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.get(
    "/batches/{predict_id}",
    response_model=BatchStatusResponse,
    summary="查询异步批次状态",
    description="根据批次 UUID 查询处理状态、结果条数、导出文件是否就绪等。",
)
async def get_batch_status(
    predict_id: uuid.UUID,
    session: AsyncSession = Depends(get_prediction_db_session),
) -> BatchStatusResponse:
    predict_id_str = str(predict_id)
    stmt = select(PredictionBatch).where(PredictionBatch.id == predict_id_str)
    res = await session.execute(stmt)
    batch = res.scalar_one_or_none()
    if batch is None:
        raise HTTPException(status_code=404, detail="未找到该预测批次")
    cnt_stmt = select(func.count()).select_from(PredictionResultRow).where(
        PredictionResultRow.batch_id == predict_id_str
    )
    cnt_res = await session.execute(cnt_stmt)
    result_count = int(cnt_res.scalar_one())
    export_ready = bool(batch.export_file_path and Path(batch.export_file_path).is_file())
    return BatchStatusResponse(
        predict_id=predict_id,
        status=batch.status,
        celery_task_id=batch.celery_task_id,
        error_message=batch.error_message,
        created_at=batch.created_at,
        completed_at=batch.completed_at,
        result_count=result_count,
        export_ready=export_ready,
    )


@router.get(
    "/batches/{predict_id}/download",
    summary="下载批次导出 Excel",
    description="异步任务生成导出文件后，通过本接口下载对应 xlsx。",
)
async def download_batch_excel(
    predict_id: uuid.UUID,
    session: AsyncSession = Depends(get_prediction_db_session),
):
    from fastapi.responses import FileResponse

    predict_id_str = str(predict_id)
    stmt = select(PredictionBatch).where(PredictionBatch.id == predict_id_str)
    res = await session.execute(stmt)
    batch = res.scalar_one_or_none()
    if batch is None:
        raise HTTPException(status_code=404, detail="未找到该预测批次")
    path = batch.export_file_path
    if not path or not Path(path).is_file():
        raise HTTPException(status_code=404, detail="导出文件尚未生成或不存在")
    return FileResponse(
        path,
        filename=f"预测导出_{predict_id_str}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get(
    "/dimension-options",
    response_model=DimensionListsResponse,
    summary="已落库预测结果筛选维度列表",
    description=(
        "从 ``pd_ip_prediction_results`` 去重返回大区经理、仓库、冶炼厂，"
        "反映当前库中已写入的智能预测明细里出现过的取值，供 ``/predict/results`` 等筛选下拉使用。"
    ),
)
async def prediction_results_dimension_options(
    session: AsyncSession = Depends(get_prediction_db_session),
) -> DimensionListsResponse:
    try:
        return await list_dimensions_from_prediction_results(session)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("prediction_results_dimension_options failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e
