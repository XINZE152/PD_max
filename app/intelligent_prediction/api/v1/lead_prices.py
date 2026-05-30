"""铅价/行情价 HTTP 接口。"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.api.audit_deps import AuditActor, get_audit_actor
from app.intelligent_prediction.api.deps import get_prediction_db_session
from app.intelligent_prediction.exceptions import (
    BusinessException,
    INTERNAL_SERVER_ERROR_MESSAGE,
)
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.schemas.lead_price import (
    LeadMarketPriceCreate,
    LeadMarketPriceItem,
    LeadMarketPriceListResponse,
    LeadMarketPriceUpdate,
)
from app.intelligent_prediction.services.audit_service import append_audit
from app.intelligent_prediction.services.lead_price_service import (
    create_lead_market_price,
    delete_lead_market_price,
    list_lead_market_prices,
    update_lead_market_price,
)

logger = get_logger(__name__)
router = APIRouter()


@router.get(
    "",
    response_model=LeadMarketPriceListResponse,
    summary="分页查询铅价/行情价",
)
async def list_lead_prices(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    date_from: date | None = Query(None, description="日期起（含）"),
    date_to: date | None = Query(None, description="日期止（含）"),
    session: AsyncSession = Depends(get_prediction_db_session),
) -> LeadMarketPriceListResponse:
    try:
        rows, total = await list_lead_market_prices(
            session, page=page, page_size=page_size, date_from=date_from, date_to=date_to
        )
        items = [LeadMarketPriceItem.model_validate(r) for r in rows]
        return LeadMarketPriceListResponse(total=total, page=page, page_size=page_size, items=items)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("list_lead_prices failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.post(
    "",
    response_model=LeadMarketPriceItem,
    summary="新增铅价/行情价",
)
async def create_lead_price(
    body: LeadMarketPriceCreate,
    session: AsyncSession = Depends(get_prediction_db_session),
    actor: AuditActor = Depends(get_audit_actor),
) -> LeadMarketPriceItem:
    try:
        row = await create_lead_market_price(session, body)
        await append_audit(
            session,
            "lead_market_price_create",
            resource=str(row.id),
            detail={"price_date": str(row.price_date), "lead_price": str(row.lead_price)},
            actor=actor,
        )
        return LeadMarketPriceItem.model_validate(row)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("create_lead_price failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.put(
    "/{price_id}",
    response_model=LeadMarketPriceItem,
    summary="更新铅价/行情价",
)
async def update_lead_price(
    price_id: int,
    body: LeadMarketPriceUpdate,
    session: AsyncSession = Depends(get_prediction_db_session),
    actor: AuditActor = Depends(get_audit_actor),
) -> LeadMarketPriceItem:
    try:
        row = await update_lead_market_price(session, price_id, body)
        await append_audit(
            session,
            "lead_market_price_update",
            resource=str(price_id),
            detail=body.model_dump(exclude_none=True),
            actor=actor,
        )
        return LeadMarketPriceItem.model_validate(row)
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("update_lead_price failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e


@router.delete(
    "/{price_id}",
    summary="删除铅价/行情价",
)
async def delete_lead_price(
    price_id: int,
    session: AsyncSession = Depends(get_prediction_db_session),
    actor: AuditActor = Depends(get_audit_actor),
) -> dict[str, str]:
    try:
        await delete_lead_market_price(session, price_id)
        await append_audit(
            session,
            "lead_market_price_delete",
            resource=str(price_id),
            actor=actor,
        )
        return {"status": "ok"}
    except BusinessException:
        raise
    except Exception as e:
        logger.exception("delete_lead_price failed")
        raise HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_MESSAGE) from e
