"""15 天仓库发货预测 — 请求 / 响应模型（豆包方案）。

字段名与前端传入的中文 JSON 键保持一致，便于直接对接。
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _snake_to_camel(name: str) -> str:
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:] if p)


# ---------------------------------------------------------------------------
# 请求侧：三组数据数组
# ---------------------------------------------------------------------------


class DoubaoHistoryItem(BaseModel):
    """仓库历史送货记录（对应 Excel 导出的中文字段）。"""

    model_config = ConfigDict(populate_by_name=True)

    送货日期: date
    大区经理: Optional[str] = None
    冶炼厂: Optional[str] = None
    仓库: str
    品类: str
    天气: Optional[str] = None
    重量吨: Decimal = Field(alias="重量(吨)")

    @field_validator("重量吨", mode="before")
    @classmethod
    def coerce_weight(cls, v: Any) -> Any:
        if v is None:
            raise ValueError("重量(吨) 不可为空")
        return v


class SmelterPriceItem(BaseModel):
    """冶炼厂基准价记录。"""

    model_config = ConfigDict(populate_by_name=True)

    日期: date
    冶炼厂: str
    品种: str
    基准价: Decimal


class SMMPricingItem(BaseModel):
    """SMM 1# 铅锭价格。"""

    model_config = ConfigDict(populate_by_name=True)

    定价日期: date
    最低价: Decimal
    最高价: Decimal
    均价: Decimal


# ---------------------------------------------------------------------------
# 请求体
# ---------------------------------------------------------------------------


class DoubaoPredictionRequest(BaseModel):
    """单条 15 天发货预测请求。"""

    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=_snake_to_camel,
    )

    warehouse: str = Field(..., min_length=1, max_length=255, description="仓库名称")
    product_variety: Optional[str] = Field(
        default=None, max_length=255, description="品类（可选，用于筛选历史）"
    )
    prediction_start_date: Optional[date] = Field(
        default=None, description="预测起始日（含）；未填则使用当日"
    )
    history: List[DoubaoHistoryItem] = Field(
        default_factory=list, description="仓库历史送货数据"
    )
    smelter_prices: List[SmelterPriceItem] = Field(
        default_factory=list, description="冶炼厂品类收货价格（含竞品）"
    )
    smm_prices: List[SMMPricingItem] = Field(
        default_factory=list, description="SMM 1# 铅锭价格"
    )
    use_cache: bool = Field(default=True, description="是否使用缓存")

    @field_validator("warehouse")
    @classmethod
    def strip_warehouse(cls, v: str) -> str:
        return v.strip()


class DoubaoBatchRequest(BaseModel):
    """批量 15 天发货预测请求。"""

    model_config = ConfigDict(populate_by_name=True, alias_generator=_snake_to_camel)

    items: List[DoubaoPredictionRequest] = Field(
        ..., min_length=1, max_length=500, description="预测请求列表"
    )


# ---------------------------------------------------------------------------
# 响应侧
# ---------------------------------------------------------------------------


class ShipProbability(str, Enum):
    """发货概率枚举。"""

    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"


class ConfidenceLevel(str, Enum):
    """置信度枚举。"""

    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"


class DailyTonnageItem(BaseModel):
    """单日发货吨数预测。"""

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        alias_generator=_snake_to_camel,
    )

    target_date: date = Field(..., description="预测目标日期")
    predicted_weight: Decimal = Field(..., ge=0, description="预测发货吨数")
    ship_probability: str = Field(default="中", description="发货概率：高/中/低")
    confidence_level: str = Field(default="中", description="置信度：高/中/低")
    main_factors: str = Field(default="", description="影响判断的主要因素")

    @field_validator("predicted_weight", mode="before")
    @classmethod
    def coerce_weight(cls, v: Any) -> Any:
        if v is None:
            return Decimal("0")
        return v

    @field_validator("ship_probability", mode="before")
    @classmethod
    def normalize_ship_prob(cls, v: Any) -> str:
        if v is None:
            return "中"
        s = str(v).strip()
        mapping = {"高": "高", "high": "高", "h": "高",
                    "中": "中", "medium": "中", "m": "中",
                    "低": "低", "low": "低", "l": "低"}
        return mapping.get(s.lower(), s)

    @field_validator("confidence_level", mode="before")
    @classmethod
    def normalize_confidence(cls, v: Any) -> str:
        if v is None:
            return "中"
        s = str(v).strip()
        mapping = {"高": "高", "high": "高", "h": "高",
                    "中": "中", "medium": "中", "m": "中",
                    "低": "低", "low": "低", "l": "低"}
        return mapping.get(s.lower(), s)


class DoubaoPredictionResult(BaseModel):
    """15 天预测完整结果。"""

    model_config = ConfigDict(from_attributes=True)

    warehouse: str = Field(..., description="仓库名称")
    product_variety: Optional[str] = Field(default=None, description="品类")
    analysis_report: str = Field(default="", description="LLM 输出的完整分析报告文本")
    items: List[DailyTonnageItem] = Field(default_factory=list, description="day0 ~ day15 共 16 条")
    provider_used: str = Field(default="unknown", description="实际使用的供应商")
    latency_ms: float = Field(default=0.0, ge=0, description="LLM 调用耗时（毫秒）")
    cost_usd: Optional[float] = Field(default=None, ge=0, description="调用费用（美元）")
    cache_hit: bool = Field(default=False, description="是否命中缓存")
    parse_error: Optional[str] = Field(default=None, description="若 AI 返回异常的说明")
