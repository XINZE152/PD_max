"""预测相关 Pydantic 模型与校验器。"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _snake_to_camel(name: str) -> str:
    """JSON 驼峰别名（与前端 Vue/TS 常见命名一致）。"""
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:] if p)


class ConfidenceLevel(str, Enum):
    """信心等级枚举。"""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class HorizonPreset(str, Enum):
    """预测跨度预设：填写时覆盖 `horizon_days`（一周 / 一月 / 三月）。"""

    ONE_WEEK = "one_week"
    ONE_MONTH = "one_month"
    THREE_MONTHS = "three_months"


class PredictionItem(BaseModel):
    """单日预测项。"""

    model_config = ConfigDict(from_attributes=True)

    target_date: date = Field(..., description="预测目标日期")
    predicted_weight: Decimal = Field(..., ge=0, description="预测重量（非负）")
    confidence: ConfidenceLevel | str = Field(default=ConfidenceLevel.MEDIUM, description="信心等级")
    warnings: list[str] = Field(default_factory=list, description="警告信息")

    @field_validator("predicted_weight", mode="before")
    @classmethod
    def coerce_decimal(cls, v: Any) -> Any:
        """将数值转为 Decimal 可解析类型。"""
        if v is None:
            raise ValueError("predicted_weight 不可为空")
        return v

    @field_validator("confidence", mode="before")
    @classmethod
    def normalize_confidence(cls, v: Any) -> str:
        """规范化信心字符串。"""
        if v is None:
            return ConfidenceLevel.MEDIUM.value
        s = str(v).strip().lower()
        if s in ("高", "high", "h"):
            return ConfidenceLevel.HIGH.value
        if s in ("中", "medium", "m"):
            return ConfidenceLevel.MEDIUM.value
        if s in ("低", "low", "l"):
            return ConfidenceLevel.LOW.value
        return s


class PredictionHistoryPoint(BaseModel):
    """用于提示的历史点。"""

    model_config = ConfigDict(populate_by_name=True, alias_generator=_snake_to_camel)

    delivery_date: date
    weight: Decimal = Field(..., ge=0)
    cn_calendar_label: Optional[str] = Field(
        default=None,
        description="节假日列：仅「是」（非工作日）或「否」（工作日），来自历史导入",
    )
    weather_summary: Optional[str] = Field(
        default=None,
        description="天气展示：导入「天气」列优先，否则高德摘要，否则按「晴」",
    )


class PredictionRequest(BaseModel):
    """同步或批量预测请求。"""

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        alias_generator=_snake_to_camel,
    )

    regional_manager: Optional[str] = Field(default=None, max_length=255)
    smelter: Optional[str] = Field(
        default=None,
        max_length=100,
        description="冶炼厂（可选；填写时从数据库加载历史将按冶炼厂筛选）",
    )
    warehouse: str = Field(..., min_length=1, max_length=255, description="仓库")
    product_variety: str = Field(..., min_length=1, max_length=255, description="品种")
    horizon_days: int = Field(default=7, ge=1, le=90, description="预测天数")
    horizon_preset: Optional[HorizonPreset] = Field(
        default=None,
        description="可选：one_week=7 天、one_month=30 天、three_months=90 天，覆盖 horizon_days",
    )
    prediction_start_date: Optional[date] = Field(
        default=None,
        description="预测起始日（含）；未填则使用当日 UTC 日期",
    )
    history: list[PredictionHistoryPoint] = Field(
        default_factory=list,
        description="若为空则由服务从数据库加载",
    )
    use_cache: bool = Field(default=True, description="是否使用缓存")
    client_request_id: Optional[str] = Field(default=None, max_length=128)

    @field_validator("warehouse", "product_variety")
    @classmethod
    def strip_strings(cls, v: str) -> str:
        """去除首尾空白。"""
        return v.strip()

    @field_validator("smelter", mode="before")
    @classmethod
    def smelter_optional_strip(cls, v: Any) -> Any:
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    @model_validator(mode="after")
    def check_history_or_db(self) -> PredictionRequest:
        """历史可为空（由服务补齐）；可选预设覆盖 horizon_days。"""
        if self.horizon_preset is not None:
            preset_days = {
                HorizonPreset.ONE_WEEK: 7,
                HorizonPreset.ONE_MONTH: 30,
                HorizonPreset.THREE_MONTHS: 90,
            }
            object.__setattr__(self, "horizon_days", preset_days[self.horizon_preset])
        if self.horizon_days < 1:
            raise ValueError("horizon_days 至少为 1")
        return self


class BatchPredictionRequest(BaseModel):
    """多笔预测请求（批量）。"""

    model_config = ConfigDict(populate_by_name=True, alias_generator=_snake_to_camel)

    items: list[PredictionRequest] = Field(..., min_length=1, max_length=500)

    @field_validator("items")
    @classmethod
    def non_empty_items(cls, v: list[PredictionRequest]) -> list[PredictionRequest]:
        """确保批量非空。"""
        if not v:
            raise ValueError("items 不可为空")
        return v


class PredictionResultSchema(BaseModel):
    """API 返回的预测结果包装。"""

    model_config = ConfigDict(from_attributes=True)

    warehouse: str
    product_variety: str
    smelter: Optional[str] = Field(default=None, description="冶炼厂（与请求或历史推断一致）")
    regional_manager: Optional[str] = None
    items: list[PredictionItem]
    provider_used: str = Field(default="unknown", description="实际使用的供应商")
    latency_ms: float = Field(default=0.0, ge=0)
    cost_usd: Optional[float] = Field(default=None, ge=0)
    cache_hit: bool = False
    parse_error: Optional[str] = Field(default=None, description="若 AI 返回非 JSON 的说明")


class AsyncPredictionAccepted(BaseModel):
    """异步预测已接受。"""

    task_id: str = Field(..., description="Celery 任务编号")
    predict_id: uuid.UUID = Field(..., description="预测批次 UUID")
    status: str = Field(default="pending", description="批次状态，如 pending")


class BatchStatusResponse(BaseModel):
    """批次状态查询。"""

    predict_id: uuid.UUID
    status: str
    celery_task_id: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    result_count: int = 0
    export_ready: bool = False


class ErrorResponse(BaseModel):
    """统一错误结构。"""

    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class StoredPredictionResultItem(BaseModel):
    """已落库的预测明细（供分页查询）。"""

    model_config = ConfigDict(from_attributes=True)

    id: int
    batch_id: Optional[str] = None
    regional_manager: Optional[str] = None
    warehouse: str
    product_variety: str
    smelter: Optional[str] = None
    target_date: date
    predicted_weight: Decimal
    confidence: str
    warnings: list[str] = Field(default_factory=list)
    provider_used: Optional[str] = None
    latency_ms: Optional[float] = None
    cost_usd: Optional[float] = None
    raw_response_excerpt: Optional[str] = None
    created_at: datetime

    @field_validator("warnings", mode="before")
    @classmethod
    def coerce_warnings(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x) for x in v]
        return [str(v)]

    @field_validator("cost_usd", mode="before")
    @classmethod
    def coerce_cost_usd(cls, v: Any) -> Any:
        if v is None:
            return None
        return float(v)


class StoredPredictionResultListResponse(BaseModel):
    """预测结果分页列表。"""

    total: int
    page: int
    page_size: int
    items: list[StoredPredictionResultItem]
