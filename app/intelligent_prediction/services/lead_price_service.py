"""铅价/行情价维护。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.exceptions import NotFoundBusinessException, ValidationBusinessException
from app.intelligent_prediction.models import LeadMarketPrice
from app.intelligent_prediction.schemas.lead_price import (
    LeadMarketPriceCreate,
    LeadMarketPriceUpdate,
)


async def list_lead_market_prices(
    session: AsyncSession,
    *,
    page: int = 1,
    page_size: int = 50,
    date_from: date | None = None,
    date_to: date | None = None,
) -> tuple[list[LeadMarketPrice], int]:
    filters = []
    if date_from is not None:
        filters.append(LeadMarketPrice.price_date >= date_from)
    if date_to is not None:
        filters.append(LeadMarketPrice.price_date <= date_to)
    count_stmt = select(func.count()).select_from(LeadMarketPrice)
    stmt = select(LeadMarketPrice)
    for f in filters:
        count_stmt = count_stmt.where(f)
        stmt = stmt.where(f)
    total = int((await session.execute(count_stmt)).scalar_one())
    offset = (page - 1) * page_size
    stmt = stmt.order_by(LeadMarketPrice.price_date.desc(), LeadMarketPrice.id.desc())
    stmt = stmt.offset(offset).limit(page_size)
    rows = list((await session.execute(stmt)).scalars().all())
    return rows, total


async def create_lead_market_price(
    session: AsyncSession,
    body: LeadMarketPriceCreate,
) -> LeadMarketPrice:
    exists = await session.execute(
        select(LeadMarketPrice.id).where(LeadMarketPrice.price_date == body.price_date)
    )
    if exists.scalar_one_or_none() is not None:
        raise ValidationBusinessException(f"日期 {body.price_date} 的铅价已存在，请使用更新接口")
    row = LeadMarketPrice(
        price_date=body.price_date,
        lead_price=body.lead_price,
        remark=(body.remark or "").strip() or None,
    )
    session.add(row)
    await session.flush()
    return row


async def update_lead_market_price(
    session: AsyncSession,
    price_id: int,
    body: LeadMarketPriceUpdate,
) -> LeadMarketPrice:
    row = await session.get(LeadMarketPrice, price_id)
    if row is None:
        raise NotFoundBusinessException("未找到该铅价记录")
    if body.lead_price is not None:
        row.lead_price = body.lead_price
    if body.remark is not None:
        row.remark = body.remark.strip() or None
    await session.flush()
    return row


async def delete_lead_market_price(session: AsyncSession, price_id: int) -> None:
    row = await session.get(LeadMarketPrice, price_id)
    if row is None:
        raise NotFoundBusinessException("未找到该铅价记录")
    await session.delete(row)


async def get_latest_lead_price(
    session: AsyncSession,
    as_of: date,
) -> Decimal | None:
    stmt = (
        select(LeadMarketPrice.lead_price)
        .where(LeadMarketPrice.price_date <= as_of)
        .order_by(LeadMarketPrice.price_date.desc(), LeadMarketPrice.id.desc())
        .limit(1)
    )
    val = (await session.execute(stmt)).scalar_one_or_none()
    if val is None:
        return None
    return Decimal(val)
