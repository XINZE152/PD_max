from typing import List, Optional

from pydantic import BaseModel, Field


class ComparisonRequest(BaseModel):
    """接口4 请求体"""
    选中仓库id列表: List[int] = Field(..., description="选中的仓库ID列表")
    冶炼厂id列表: List[int] = Field(..., description="冶炼厂ID列表")
    品类id列表: List[int] = Field(..., description="品类ID列表")


class AddWarehouseRequest(BaseModel):
    """添加仓库请求体"""
    仓库名: str = Field(..., description="仓库名称")


class UploadFreightRequest(BaseModel):
    """接口6 请求体（单条）"""
    仓库: str = Field(..., description="仓库名称，如 北京仓")
    冶炼厂: str = Field(..., description="冶炼厂名称，如 华北冶炼厂")
    运费: float = Field(..., description="运费金额（元/吨）")


class CategoryMappingItem(BaseModel):
    """接口7 单条品类映射"""
    品类id: int = Field(..., description="品类分组ID")
    品类名称: List[str] = Field(..., description="品类名称列表，第一个为主名称")


class UpdateCategoryMappingRequest(BaseModel):
    """接口7 请求体"""
    品类id: int = Field(..., description="品类分组ID")
    品类名称: List[str] = Field(..., description="品类名称列表，第一个为主名称")


class ConfirmPriceTableItem(BaseModel):
    """确认价格表 - 单条明细"""
    冶炼厂名: str = Field(..., description="冶炼厂名称（OCR识别或前端修改后）")
    冶炼厂id: Optional[int] = Field(None, description="冶炼厂ID，null则自动新建")
    品类名: str = Field(..., description="品类名称（OCR识别或前端修改后）")
    品类id: Optional[int] = Field(None, description="品类分组ID，null则自动新建")
    价格: float = Field(..., description="单价（元/吨）")


class ConfirmPriceTableRequest(BaseModel):
    """接口5b 请求体 - 确认写入报价数据"""
    报价日期: str = Field(..., description="报价日期，格式 YYYY-MM-DD")
    数据: List[ConfirmPriceTableItem] = Field(..., description="报价明细列表")


class DemandItem(BaseModel):
    """A7 单条需求"""
    smelter_id: int = Field(..., description="冶炼厂ID")
    category_id: int = Field(..., description="品类分组ID")
    demand: float = Field(..., description="需求吨数")


class PurchaseSuggestionRequest(BaseModel):
    """A7 采购建议请求体"""
    warehouse_ids: List[int] = Field(..., description="仓库ID列表")
    demands: List[DemandItem] = Field(..., description="需求列表")
