"""送货历史 HTTP 接口。"""

from __future__ import annotations

import csv
import io
from datetime import date
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.settings import settings
from app.intelligent_prediction.exceptions import (
    BusinessException,
    INTERNAL_SERVER_ERROR_MESSAGE,
    ValidationBusinessException,
)
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.api.audit_deps import AuditActor, get_audit_actor
from app.intelligent_prediction.api.deps import get_history_service_dep, get_prediction_db_session
from app.intelligent_prediction.schemas.dimensions import DimensionListsResponse
from app.intelligent_prediction.schemas.history import (
    DeliveryRecordRead,
    DeliveryRecordUpdate,
    HistoryBatchDeleteRequest,
    HistoryImportResponse,
    HistoryPurgeAllRequest,
    HistoryListResponse,
    HistoryQueryParams,
    HistoryStatsResponse,
    HistoryTemplateFieldsResponse,
)
from app.intelligent_prediction.services.audit_service import append_audit, write_audit_standalone
from app.intelligent_prediction.services.dimension_options_service import (
    list_dimensions_from_delivery_history,
)
from app.intelligent_prediction.services.history_service import HistoryService

logger = get_logger(__name__)
router = APIRouter()


def _parse_history_query_date(name: str, raw: Optional[str]) -> Optional[date]:
    """解析列表查询中的日期：支持 YYYY-MM-DD 或以该格式开头的 ISO 日期时间（前端常带 T00:00:00）。"""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    head = s[:10] if len(s) >= 10 else s
    try:
        return date.fromisoformat(head)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=[
                {
                    "type": "date_parsing",
                    "loc": ["query", name],
                    "msg": "日期须为 YYYY-MM-DD，或为以该日期开头的 ISO 日期时间",
                    "input": raw,
                }
            ],
        ) from None


@router.get(
    "/template/fields",
    response_model=HistoryTemplateFieldsResponse,
    summary="导入模板列定义（JSON）",
    description="返回模板表头顺序及与内部字段映射；导入时「到货日期」列名会映射为送货日期；「节假日」须手填；选填「天气」不填则按晴；与 GET /delivery-history/template 下载的 xlsx 表头一致。",
)
async def history_template_fields() -> HistoryTemplateFieldsResponse:
    return HistoryTemplateFieldsResponse(
        headers=HistoryService.import_template_headers(),
        header_to_field=dict(HistoryService.HEADER_TO_FIELD),
    )


@router.get(
    "/template",
    summary="下载送货历史导入模板",
    description=(
        "返回标准 xlsx：含「导入数据」（表头 + 可跳过示例行）与「使用说明」；"
        "表头含大区经理、冶炼厂、仓库、送货日期、必填节假日（仅「是」/「否」）、品种、重量、选填仓库地址与冶炼厂地址与天气（不填则晴）；"
        "送货日期列亦支持「到货日期」列名；大区经理以「(示例)」开头的行导入时自动跳过。"
    ),
)
async def download_history_template() -> StreamingResponse:
    """标准导入模板（表头与 PRD 一致，含示例与说明工作表）。"""
    body = HistoryService.import_template_xlsx_bytes()
    buf = io.BytesIO(body)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": (
                'attachment; filename="delivery_history_import_template.xlsx"; '
                "filename*=UTF-8''%E9%80%81%E8%B4%A7%E5%8E%86%E5%8F%B2%E5%AF%BC%E5%85%A5%E6%A8%A1%E6%9D%BF.xlsx"
            )
        },
    )


@router.get("/template.csv")
async def download_history_template_csv() -> StreamingResponse:
    """与 xlsx 模板相同表头的 CSV（UTF-8 BOM，便于 Excel 直接打开）。"""
    cols = HistoryService.import_template_headers()
    buf = io.StringIO()
    csv.writer(buf).writerow(cols)
    body = buf.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        io.BytesIO(body),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": (
                'attachment; filename="delivery_history_import_template.csv"; '
                "filename*=UTF-8''%E9%80%81%E8%B4%A7%E5%8E%86%E5%8F%B2%E5%AF%BC%E5%85%A5%E6%A8%A1%E6%9D%BF.csv"
            )
        },
    )


@router.post(
    "/import",
    response_model=HistoryImportResponse,
    summary="导入送货历史 Excel",
    description=(
        "上传 xlsx/csv，校验后批量写入送货历史表；须含「节假日」列且每行仅填「是」或「否」。"
        "校验失败时返回 details.errors：每项含 Excel 行号 row_index、列字母 excel_column、表头 column_header 与 message。"
    ),
)
async def import_history_excel(
    request: Request,
    file: UploadFile = File(..., description="送货历史数据文件：.csv（推荐，纯文本）或 .xlsx"),
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: HistoryService = Depends(get_history_service_dep),
    actor: AuditActor = Depends(get_audit_actor),
) -> HistoryImportResponse:
    fn = file.filename or "upload.xlsx"
    try:
        raw = await file.read()
        result = await svc.import_excel(session, raw, fn)
        await append_audit(
            session,
            "history_import",
            resource=fn,
            detail={"inserted": result.inserted},
            actor=actor,
        )
        return result
    except ValidationBusinessException as e:
        logger.info(
            "history import validation failed file=%s message=%s",
            fn,
            e.message,
        )
        await write_audit_standalone(
            "history_import_failed",
            resource=fn,
            detail={"message": e.message, **(e.details or {})},
            actor=actor,
        )
        raise
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("import failed")
        await write_audit_standalone(
            "history_import_failed",
            resource=fn,
            detail={"error": str(e)},
            actor=actor,
        )
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.get(
    "/statistics",
    response_model=HistoryStatsResponse,
    summary="送货历史统计分析",
    description="在筛选条件下汇总总条数、总重量，并按仓库、品种、大区经理聚合（各最多 200 条，按重量降序）。",
)
async def history_statistics(
    regional_manager: Optional[str] = Query(None, description="区域经理（单值）"),
    regional_managers: list[str] = Query(default=[], description="区域经理（多值）"),
    smelter: Optional[str] = Query(None, description="冶炼厂（单值）"),
    smelters: list[str] = Query(default=[], description="冶炼厂（多值）"),
    warehouse: Optional[str] = Query(None, description="仓库（单值）"),
    warehouses: list[str] = Query(default=[], description="仓库（多值）"),
    product_variety: Optional[str] = Query(None, description="品种（单值）"),
    product_varieties: list[str] = Query(default=[], description="品种（多值）"),
    date_from: Optional[date] = Query(None, description="送货日期起（含）"),
    date_to: Optional[date] = Query(None, description="送货日期止（含）"),
    top_n: int = Query(200, ge=1, le=500, description="各维度聚合返回的最大行数"),
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: HistoryService = Depends(get_history_service_dep),
) -> HistoryStatsResponse:
    q = HistoryQueryParams(
        page=1,
        page_size=1,
        regional_manager=regional_manager,
        warehouse=warehouse,
        product_variety=product_variety,
        regional_managers=regional_managers,
        smelter=smelter,
        smelters=smelters,
        warehouses=warehouses,
        product_varieties=product_varieties,
        date_from=date_from,
        date_to=date_to,
    )
    try:
        return await svc.statistics(session, q, top_n=top_n)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("history_statistics failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put(
    "/{record_id}",
    response_model=DeliveryRecordRead,
    summary="更新单条送货历史",
    description="按主键更新大区经理、冶炼厂、仓库、送货日期、品种、重量、地址及节假日（cn_calendar_label，仅「是」/「否」）等；请求体至少包含一项。",
)
async def update_history_record(
    record_id: int,
    body: DeliveryRecordUpdate,
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: HistoryService = Depends(get_history_service_dep),
    actor: AuditActor = Depends(get_audit_actor),
) -> DeliveryRecordRead:
    try:
        row = await svc.update_record(session, record_id, body)
        if row is None:
            raise HTTPException(status_code=404, detail="记录不存在")
        await append_audit(
            session,
            "history_update",
            resource=str(record_id),
            detail={"fields": list(body.model_dump(exclude_unset=True).keys())},
            actor=actor,
        )
        return row
    except ValidationBusinessException:
        raise
    except BusinessException:
        raise
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("update_history_record failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get(
    "",
    response_model=HistoryListResponse,
    summary="分页查询送货历史",
    description="支持按区域经理、冶炼厂、仓库、品种、送货日期区间筛选。",
)
async def list_history(
    page: int = Query(1, ge=0, le=1_000_000, description="页码（≤0 时按 1 处理）"),
    page_size: int = Query(20, ge=1, le=10_000, description="每页条数（超过 1000 时按 1000 处理）"),
    regional_manager: Optional[str] = Query(None, description="区域经理（单值）"),
    regional_managers: list[str] = Query(default=[], description="区域经理（多值）"),
    smelter: Optional[str] = Query(None, description="冶炼厂（单值）"),
    smelters: list[str] = Query(default=[], description="冶炼厂（多值）"),
    warehouse: Optional[str] = Query(None, description="仓库（单值）"),
    warehouses: list[str] = Query(default=[], description="仓库（多值）"),
    product_variety: Optional[str] = Query(None, description="品种（单值）"),
    product_varieties: list[str] = Query(default=[], description="品种（多值）"),
    date_from: Optional[str] = Query(None, description="送货日期起（含），YYYY-MM-DD 或 ISO 日期时间"),
    date_to: Optional[str] = Query(None, description="送货日期止（含），YYYY-MM-DD 或 ISO 日期时间"),
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: HistoryService = Depends(get_history_service_dep),
) -> HistoryListResponse:
    page_adj = max(1, page)
    page_size_adj = max(1, min(1000, page_size))
    q = HistoryQueryParams(
        page=page_adj,
        page_size=page_size_adj,
        regional_manager=regional_manager,
        warehouse=warehouse,
        product_variety=product_variety,
        regional_managers=regional_managers,
        smelter=smelter,
        smelters=smelters,
        warehouses=warehouses,
        product_varieties=product_varieties,
        date_from=_parse_history_query_date("date_from", date_from),
        date_to=_parse_history_query_date("date_to", date_to),
    )
    try:
        return await svc.list_records(session, q)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("list_history failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.delete(
    "/batch-delete",
    summary="批量删除送货历史",
    description="根据主键 id 列表批量删除记录。",
)
async def batch_delete_history(
    body: HistoryBatchDeleteRequest,
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: HistoryService = Depends(get_history_service_dep),
    actor: AuditActor = Depends(get_audit_actor),
) -> dict[str, int]:
    try:
        deleted = await svc.batch_delete(session, body.ids)
        await append_audit(
            session,
            "history_batch_delete",
            detail={"deleted": deleted, "ids_sample": body.ids[:50]},
            actor=actor,
        )
        return {"deleted": deleted}
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("batch_delete failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.post(
    "/purge-all",
    summary="一键清除全部送货历史",
    description=(
        "删除表 pd_ip_delivery_records 全部行。须先在环境变量配置 INTELLIGENT_PREDICTION_HISTORY_PURGE_SECRET；"
        "请求头 X-Purge-Delivery-History-Secret 与该值完全一致，且 JSON 体 {\"confirm\": true}。"
    ),
)
async def purge_all_delivery_history(
    body: HistoryPurgeAllRequest,
    session: AsyncSession = Depends(get_prediction_db_session),
    svc: HistoryService = Depends(get_history_service_dep),
    actor: AuditActor = Depends(get_audit_actor),
    x_purge_delivery_history_secret: Annotated[
        Optional[str],
        Header(alias="X-Purge-Delivery-History-Secret"),
    ] = None,
) -> dict[str, int]:
    configured = (settings.intelligent_prediction_history_purge_secret or "").strip()
    if not configured:
        raise BusinessException(
            "未配置 INTELLIGENT_PREDICTION_HISTORY_PURGE_SECRET，一键清除接口不可用",
            code="FEATURE_DISABLED",
            status_code=503,
        )
    if (x_purge_delivery_history_secret or "").strip() != configured:
        raise BusinessException("清除密钥无效", code="FORBIDDEN", status_code=403)
    try:
        deleted = await svc.purge_all_delivery_records(session)
        await append_audit(
            session,
            "history_purge_all",
            resource="pd_ip_delivery_records",
            detail={"deleted": deleted},
            actor=actor,
        )
        logger.warning(
            "history purge_all completed deleted=%s actor=%s",
            deleted,
            actor.user_label,
        )
        return {"deleted": deleted}
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("purge_all failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.get(
    "/dimension-options",
    response_model=DimensionListsResponse,
    summary="送货历史筛选维度列表",
    description=(
        "从送货历史表 ``pd_ip_delivery_records`` 去重返回大区经理、仓库、冶炼厂名称，"
        "供列表/统计等筛选下拉使用（与规则预测 ``/forecast`` 筛选维度同源）。"
    ),
)
async def delivery_history_dimension_options(
    session: AsyncSession = Depends(get_prediction_db_session),
) -> DimensionListsResponse:
    try:
        return await list_dimensions_from_delivery_history(session)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("delivery_history_dimension_options failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e
