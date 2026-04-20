"""历史数据 API 结构。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

HolidayYesNo = Literal["是", "否"]


class DeliveryRecordRead(BaseModel):
    """单条送货记录（读取）。"""

    model_config = ConfigDict(from_attributes=True)

    id: int
    regional_manager: str
    smelter: Optional[str] = None
    warehouse: str
    warehouse_address: Optional[str] = None
    smelter_address: Optional[str] = None
    delivery_date: date
    product_variety: str
    weight: Decimal
    cn_is_workday: Optional[bool] = Field(
        None,
        description="是否中国工作日：与导入「节假日」列一致，「否」为工作日，「是」为非工作日",
    )
    cn_calendar_label: Optional[str] = Field(
        None,
        description="导入「节假日」列：仅「是」（非工作日）或「否」（工作日）",
    )
    weather_json: Optional[dict[str, Any]] = Field(None, description="天气 API 返回摘要（未配置 API 时为空）")
    import_weather: Optional[str] = Field(
        None,
        description="导入「天气」列简述；未填时按「晴」入库",
    )
    created_at: datetime
    updated_at: Optional[datetime] = None


class HistoryListResponse(BaseModel):
    """分页列表。"""

    total: int
    page: int
    page_size: int
    items: list[DeliveryRecordRead]


class HistoryImportRowError(BaseModel):
    """导入错误行信息。"""

    row_index: int = Field(
        ...,
        description="Excel 行号（1-based；第 1 行为表头，数据从第 2 行起）",
    )
    excel_column: Optional[str] = Field(
        None,
        description="与导入模板一致的列字母（如 A、D），便于在表格中定位",
    )
    column_header: Optional[str] = Field(
        None,
        description="表头中文列名（与模板一致），与 excel_column 对应",
    )
    field: Optional[str] = Field(
        None,
        description="内部字段名（如 regional_manager），可选；新错误以 excel_column 为准",
    )
    message: str = Field(..., description="该列/单元格的具体错误说明")


class HistoryImportResponse(BaseModel):
    """导入结果。"""

    inserted: int
    skipped: int
    errors: list[HistoryImportRowError]


class HistoryTemplateFieldsResponse(BaseModel):
    """导入模板列说明（与下载的 xlsx/csv 表头一致）。"""

    headers: list[str] = Field(..., description="表头中文名，顺序与模板文件一致")
    header_to_field: dict[str, str] = Field(..., description="表头到内部字段名（如 smelter）")


class DeliveryRecordUpdate(BaseModel):
    """单条送货历史更新（全部可选，至少改一项）。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    regional_manager: Optional[str] = Field(default=None, max_length=255)
    smelter: Optional[str] = Field(default=None, max_length=100)
    warehouse: Optional[str] = Field(default=None, min_length=1, max_length=255)
    warehouse_address: Optional[str] = Field(default=None, max_length=512)
    smelter_address: Optional[str] = Field(default=None, max_length=512)
    delivery_date: Optional[date] = None
    product_variety: Optional[str] = Field(default=None, min_length=1, max_length=255)
    weight: Optional[Decimal] = Field(default=None, ge=0)
    cn_calendar_label: Optional[HolidayYesNo] = Field(
        default=None,
        description="节假日：仅「是」或「否」；更新时与 cn_is_workday 联动",
    )
    import_weather: Optional[str] = Field(default=None, max_length=64)

    @field_validator("smelter", mode="before")
    @classmethod
    def empty_smelter_none(cls, v: Any) -> Any:
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    @field_validator("warehouse_address", "smelter_address", mode="before")
    @classmethod
    def empty_address_none(cls, v: Any) -> Any:
        if v is None:
            return None
        s = str(v).strip()
        return s or None


class HistoryBatchDeleteRequest(BaseModel):
    """批量删除请求。"""

    ids: list[int] = Field(..., min_length=1, max_length=2000)

    @field_validator("ids")
    @classmethod
    def unique_ids(cls, v: list[int]) -> list[int]:
        """去重。"""
        return list(dict.fromkeys(v))


class HistoryPurgeAllRequest(BaseModel):
    """一键清除全部送货历史：须显式确认。"""

    confirm: Literal[True] = Field(..., description="必须为 JSON 布尔 true")


class HistoryQueryParams(BaseModel):
    """查询参数（由服务层组装）。"""

    page: int = Field(default=1, ge=1, le=1_000_000)
    page_size: int = Field(default=20, ge=1, le=1000)
    regional_manager: Optional[str] = None
    warehouse: Optional[str] = None
    product_variety: Optional[str] = None
    regional_managers: list[str] = Field(default_factory=list)
    warehouses: list[str] = Field(default_factory=list)
    product_varieties: list[str] = Field(default_factory=list)
    smelter: Optional[str] = None
    smelters: list[str] = Field(default_factory=list)
    date_from: Optional[date] = None
    date_to: Optional[date] = None

    @field_validator("regional_manager", "warehouse", "product_variety", "smelter", mode="before")
    @classmethod
    def empty_to_none(cls, v: Any) -> Any:
        """空字符串视为未筛选。"""
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v.strip() if isinstance(v, str) else v

    @field_validator("regional_managers", "warehouses", "product_varieties", "smelters", mode="before")
    @classmethod
    def normalize_str_lists(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",") if p.strip()]
            return parts
        if isinstance(v, (list, tuple)):
            out: list[str] = []
            for x in v:
                if x is None:
                    continue
                s = str(x).strip()
                if s:
                    out.append(s)
            return out
        return []


class HistoryStatsBucket(BaseModel):
    """按维度聚合的一行。"""

    key: str = Field(..., description="维度取值，如仓库名")
    record_count: int = Field(..., ge=0)
    total_weight: Decimal = Field(..., ge=0)


class HistoryStatsResponse(BaseModel):
    """送货历史统计分析（在筛选范围内）。"""

    total_records: int = Field(..., ge=0)
    total_weight: Decimal = Field(..., ge=0)
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    by_warehouse: list[HistoryStatsBucket] = Field(default_factory=list)
    by_product_variety: list[HistoryStatsBucket] = Field(default_factory=list)
    by_regional_manager: list[HistoryStatsBucket] = Field(default_factory=list)
