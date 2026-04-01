from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ComparisonRequest(BaseModel):
    """接口4 请求体"""
    选中仓库id列表: List[int] = Field(..., description="选中的仓库ID列表")
    冶炼厂id列表: List[int] = Field(..., description="冶炼厂ID列表")
    品类id列表: List[int] = Field(..., description="品类ID列表")
    price_type: Optional[str] = Field(
        None,
        description="目标税率类型：null=普通价、1pct=1%增值税、3pct=3%增值税、13pct=13%增值税、normal_invoice=普通发票、reverse_invoice=反向发票"
    )


class AddWarehouseRequest(BaseModel):
    """添加仓库请求体"""
    仓库名: str = Field(..., description="仓库名称")


class UpdateWarehouseRequest(BaseModel):
    """修改仓库请求体"""
    仓库id: int = Field(..., description="仓库ID")
    仓库名: Optional[str] = Field(None, description="仓库名称（可选）")
    is_active: Optional[bool] = Field(None, description="是否启用（可选）")


class AddSmelterRequest(BaseModel):
    """新建冶炼厂请求体"""
    冶炼厂名: str = Field(..., description="冶炼厂名称")


class UpdateSmelterRequest(BaseModel):
    """修改冶炼厂请求体"""
    冶炼厂id: int = Field(..., description="冶炼厂ID")
    冶炼厂名: Optional[str] = Field(None, description="冶炼厂名称（可选）")
    is_active: Optional[bool] = Field(None, description="是否启用（可选）")


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


class VlmPriceRow(BaseModel):
    """VLM提取的单行数据（供前端编辑）"""
    index: Optional[int] = Field(None, description="序号")
    category: str = Field("", description="品类名称")
    is_category_start: bool = Field(False, description="是否为合并单元格首行")
    price_1pct_vat: Optional[int] = Field(None, description="1%增值税价格")
    price_3pct_vat: Optional[int] = Field(None, description="3%增值税价格")
    price_13pct_vat: Optional[int] = Field(None, description="13%增值税价格")
    price_normal_invoice: Optional[int] = Field(None, description="普通发票价格")
    price_reverse_invoice: Optional[int] = Field(None, description="反向发票价格")
    price_general: Optional[int] = Field(None, description="通用单价")
    unit: str = Field("元/吨", description="单位")
    remark: str = Field("", description="备注")


class VlmFullData(BaseModel):
    """VLM提取的完整报价表数据（upload接口返回，confirm接口回传）"""
    image_path: str = Field("", description="图片路径")
    file_name: str = Field("", description="文件名")
    company_name: str = Field("", description="公司名称")
    doc_title: str = Field("", description="文档标题")
    subtitle: str = Field("", description="副标题")
    quote_date: str = Field("", description="报价日期")
    execution_date: str = Field("", description="执行日期")
    valid_period: str = Field("", description="有效期")
    price_unit: str = Field("元/吨", description="价格单位")
    price_column_type: str = Field("unknown", description="价格列类型")
    has_merged_cells: bool = Field(False, description="是否有合并单元格")
    vat_columns_detected: List[str] = Field(default_factory=list, description="检测到的VAT列")
    headers: List[str] = Field(default_factory=list, description="表头")
    rows: List[VlmPriceRow] = Field(default_factory=list, description="数据行")
    policies: Dict[str, Any] = Field(default_factory=dict, description="政策信息")
    footer_notes: List[str] = Field(default_factory=list, description="页脚备注")
    footer_notes_raw: str = Field("", description="页脚备注原始文本")
    brand_specifications: str = Field("", description="品牌规格说明")
    raw_full_text: str = Field("", description="原始完整识别文本")
    markdown_table: str = Field("", description="Markdown表格")
    elapsed_time: float = Field(0.0, description="处理耗时（秒）")
    source_image: str = Field("", description="来源图片文件名")


class ConfirmPriceTableItem(BaseModel):
    """确认价格表 - 单条明细"""
    冶炼厂名: str = Field(..., description="冶炼厂名称（OCR识别或前端修改后）")
    冶炼厂id: Optional[int] = Field(None, description="冶炼厂ID，null则自动新建")
    品类名: str = Field(..., description="品类名称（OCR识别或前端修改后）")
    品类id: Optional[int] = Field(None, description="品类分组ID，null则自动新建")
    价格: Optional[float] = Field(None, description="普通价单价（元/吨）")
    价格_1pct增值税: Optional[float] = Field(None, description="1%增值税价格（元/吨）")
    价格_3pct增值税: Optional[float] = Field(None, description="3%增值税价格（元/吨）")
    价格_13pct增值税: Optional[float] = Field(None, description="13%增值税价格（元/吨）")
    普通发票价格: Optional[float] = Field(None, description="普通发票价格（元/吨）")
    反向发票价格: Optional[float] = Field(None, description="反向发票价格（元/吨）")


class ConfirmPriceTableRequest(BaseModel):
    """接口5b 请求体 - 确认写入报价数据"""
    报价日期: str = Field(..., description="报价日期，格式 YYYY-MM-DD")
    full_data: Optional[VlmFullData] = Field(None, description="VLM提取的完整原始数据，存入元数据表")
    数据: List[ConfirmPriceTableItem] = Field(..., description="报价明细列表（前端确认/修改后）")


class DemandItem(BaseModel):
    """A7 单条需求（冶炼厂由后端默认取全部启用冶炼厂，前端不传）"""
    category_id: int = Field(..., description="品类分组ID")
    demand: float = Field(..., description="需求吨数")


class PurchaseSuggestionRequest(BaseModel):
    """A7 采购建议请求体"""
    warehouse_ids: List[int] = Field(..., description="仓库ID列表")
    demands: List[DemandItem] = Field(..., description="需求列表（仅品类与吨数）")
    price_type: Optional[str] = Field(None, description="价格类型：None=普通价, 1pct/3pct/13pct/normal_invoice/reverse_invoice")


# ==================== 税率表 ====================

VALID_TAX_TYPES = {"1pct", "3pct", "13pct"}


class TaxRateItem(BaseModel):
    """单条税率记录"""
    factory_id: int = Field(..., description="冶炼厂ID")
    tax_type: str = Field(..., description="税率类型：1pct/3pct/13pct")
    tax_rate: float = Field(..., ge=0, le=1, description="税率值，如 0.03 表示3%")


class TaxRateUpsertRequest(BaseModel):
    """批量设置税率（upsert）"""
    items: List[TaxRateItem] = Field(..., description="税率列表")
