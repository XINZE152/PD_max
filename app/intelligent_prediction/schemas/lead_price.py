"""铅价/行情价 CRUD 结构。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LeadMarketPriceCreate(BaseModel):
    price_date: date = Field(..., description="铅价日期")
    lead_price: Decimal = Field(..., gt=0, description="铅价（元/吨）")
    remark: Optional[str] = Field(default=None, max_length=255, description="备注或来源")

    @field_validator("lead_price", mode="before")
    @classmethod
    def coerce_price(cls, v: object) -> object:
        if v is None:
            raise ValueError("lead_price 不可为空")
        return v


class LeadMarketPriceUpdate(BaseModel):
    lead_price: Optional[Decimal] = Field(default=None, gt=0, description="铅价（元/吨）")
    remark: Optional[str] = Field(default=None, max_length=255, description="备注或来源")


class LeadMarketPriceItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    price_date: date
    lead_price: Decimal
    remark: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class LeadMarketPriceListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[LeadMarketPriceItem]
