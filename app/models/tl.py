from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ComparisonRequest(BaseModel):
    """接口4 请求体"""
    选中仓库id列表: List[int] = Field(..., description="选中的仓库ID列表")
    冶炼厂id列表: List[int] = Field(..., description="冶炼厂ID列表")
    品类id列表: List[int] = Field(..., description="品类ID列表")
    price_type: Optional[str] = Field(
        None,
        description=(
            "比价取价口径：null=普通价(不含税)、1pct/3pct/13pct=对应含税列（会折合为不含税参与展示与利润）、"
            "normal_invoice/reverse_invoice=表中数值按不含税使用"
        ),
    )
    吨数: float = Field(
        1.0,
        gt=0,
        description="吨数；按吨计费时总运费=每吨运费×吨数；按车计费时车数=向上取整(吨数/每车吨数)（至少1车），总运费=每车运费×车数",
    )
    运费计价方式: Literal["per_ton", "per_truck"] = Field(
        "per_ton",
        description="per_ton：运费字段为每吨单价（元/吨）；per_truck：运费字段为每车单价（元/车），默认每车35吨",
    )
    每车吨数: float = Field(
        35.0,
        gt=0,
        description="按车计费时一车折合吨数；车数=向上取整(吨数/每车吨数)，至少1车",
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


class UploadVarietyRequest(BaseModel):
    """上传品种（单条，与上传运费相同可传列表批量）"""
    品种名: str = Field(..., description="品种名称，写入 dict_categories")


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


class UpdateFreightRequest(BaseModel):
    """接口6c 编辑运费（按列表返回的 id）"""
    运费id: int = Field(..., description="freight_rates 主键，见 get_freight_list 返回的 id")
    运费: float = Field(..., ge=0, description="每吨运费（元）")
    生效日期: Optional[str] = Field(
        None,
        description="YYYY-MM-DD；不传则保持原生效日期；若修改，同一仓库+冶炼厂下该日期不能已有其它记录",
    )


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
    price_basis: str = Field("ex_vat", description="价格口径：ex_vat不含税/incl_1pct/incl_3pct/incl_13pct")
    exclusive_net: Optional[int] = Field(None, description="推算的不含税基准（元/吨）")


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
    价格: Optional[float] = Field(None, description="不含税基准价（元/吨）")
    价格口径: Optional[str] = Field(
        None,
        description="表中报价含义：ex_vat不含税、incl_1pct、incl_3pct、incl_13pct；确认时可不传，将按备注推断",
    )
    备注: Optional[str] = Field(None, description="行备注（识别或手工维护，用于推断价格口径）")
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
