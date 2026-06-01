"""PRD 送货量预测（历史规律 + 价格因素）API 结构。"""

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
    history_baseline: Decimal = Field(description="历史规律基线 = wma_base × week_coef")
    price_factor: Decimal = Field(default=Decimal("1"), description="价格乘数（相对行情/竞品）")
    lead_market_price: Optional[Decimal] = Field(default=None, description="铅价/行情价")
    own_calibration_price: Optional[Decimal] = Field(default=None, description="己方标定价格（金利）")
    competitor_price_max: Optional[Decimal] = Field(default=None, description="竞品最高价")
    price_sensitivity: Optional[str] = Field(default=None, description="库房价格敏感度")
    analysis: Optional[str] = Field(default=None, description="预测解释文案")
    predicted_weight: Decimal


class PrdForecastWarehouseProfile(BaseModel):
    warehouse: str
    product_variety: str
    price_sensitivity: str
    price_correlation: Optional[float] = None
    capacity_max: Decimal
    capacity_min: Decimal
    capacity_avg: Decimal


class PrdForecastByRmSeries(BaseModel):
    regional_manager: str
    totals: list[Decimal] = Field(description="与 chart.dates 对齐的按日合计")


class PrdForecastChartResponse(BaseModel):
    dates: list[date]
    total_by_date: list[Decimal]
    by_regional_manager: list[PrdForecastByRmSeries]
    warehouse_profiles: list[PrdForecastWarehouseProfile] = Field(
        default_factory=list,
        description="各库房价格敏感度与能力基线",
    )
    summary_analysis: Optional[str] = Field(
        default=None,
        description="区间汇总级预测依据（规则模型解释）",
    )


class PrdForecastDetailResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[PrdForecastDetailRow]
