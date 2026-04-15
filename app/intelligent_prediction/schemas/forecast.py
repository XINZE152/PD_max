"""PRD 送货量预测（线性加权移动平均 + 周规律）API 结构。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class PrdForecastQuery(BaseModel):
    """查询体（服务层使用）。"""

    date_from: date
    date_to: date
    regional_managers: list[str] = Field(default_factory=list)
    warehouses: list[str] = Field(default_factory=list)
    product_varieties: list[str] = Field(default_factory=list)
    smelters: list[str] = Field(default_factory=list, description="冶炼厂（多值精确筛选，空表示不限）")
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)


class PrdForecastDetailRow(BaseModel):
    target_date: date
    regional_manager: str
    warehouse: str
    product_variety: str
    smelter: Optional[str] = Field(default=None, description="冶炼厂（历史无则 null）")
    wma_base: Decimal = Field(description="周规律调整前（线性加权30日均）")
    week_coef: Decimal = Field(description="仓库周规律系数")
    predicted_weight: Decimal


class PrdForecastByRmSeries(BaseModel):
    regional_manager: str
    totals: list[Decimal] = Field(description="与 chart.dates 对齐的按日合计")


class PrdForecastChartResponse(BaseModel):
    dates: list[date]
    total_by_date: list[Decimal]
    by_regional_manager: list[PrdForecastByRmSeries]


class PrdForecastDetailResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[PrdForecastDetailRow]
