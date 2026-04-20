from typing import Any, Dict, List, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

OPTIMAL_PRICE_BASIS_ALLOWED = frozenset(
    {"base", "1pct", "3pct", "13pct", "normal_invoice", "reverse_invoice"}
)

def _normalize_comparison_price_type(v: Any) -> Any:
    """将前端常用中文/别名规范为接口约定：null、1pct、3pct、13pct、normal_invoice、reverse_invoice。"""
    if v is None:
        return None
    if isinstance(v, bool):
        raise ValueError(f"price_type 不能为布尔值: {v!r}")
    s = str(v).strip()
    if not s:
        return None
    compact = s.replace(" ", "").replace("（", "(").replace("）", ")")
    low = compact.lower()
    aliases = {
        "普通价": None,
        "不含税": None,
        "基准价": None,
        "基准": None,
        "null": None,
        "none": None,
        "1pct": "1pct",
        "1%增值税": "1pct",
        "1%含税": "1pct",
        "含1%增值税": "1pct",
        "3pct": "3pct",
        "3%增值税": "3pct",
        "3%含税": "3pct",
        "含3%增值税": "3pct",
        "13pct": "13pct",
        "13%增值税": "13pct",
        "13%含税": "13pct",
        "含13%增值税": "13pct",
        "normal_invoice": "normal_invoice",
        "普通发票": "normal_invoice",
        "普票": "normal_invoice",
        "reverse_invoice": "reverse_invoice",
        "反向发票": "reverse_invoice",
    }
    if low in aliases:
        return aliases[low]
    if compact in aliases:
        return aliases[compact]
    if low in ("1pct", "3pct", "13pct", "normal_invoice", "reverse_invoice"):
        return low
    raise ValueError(
        f"不支持的 price_type: {v!r}，请使用 null/1pct/3pct/13pct/normal_invoice/reverse_invoice，"
        f"或中文如 3%增值税、普通价 等"
    )


def _normalize_optimal_basis_token(x: Any) -> str:
    """最优价计税口径：中文/英文 → base、1pct、3pct 等。"""
    s = str(x).strip()
    if not s:
        raise ValueError("最优价计税口径不能为空")
    compact = s.replace(" ", "").replace("（", "(").replace("）", ")")
    low = compact.lower()
    aliases = {
        "base": "base",
        "基准价": "base",
        "基准": "base",
        "不含税基准": "base",
        "普通价": "base",
        "1pct": "1pct",
        "1%增值税": "1pct",
        "1%含税": "1pct",
        "含1%增值税": "1pct",
        "3pct": "3pct",
        "3%增值税": "3pct",
        "3%含税": "3pct",
        "含3%增值税": "3pct",
        "13pct": "13pct",
        "13%增值税": "13pct",
        "13%含税": "13pct",
        "含13%增值税": "13pct",
        "normal_invoice": "normal_invoice",
        "普通发票": "normal_invoice",
        "普票": "normal_invoice",
        "reverse_invoice": "reverse_invoice",
        "反向发票": "reverse_invoice",
    }
    if low in aliases:
        return aliases[low]
    if compact in aliases:
        return aliases[compact]
    if low in OPTIMAL_PRICE_BASIS_ALLOWED:
        return low
    raise ValueError(
        f"不支持的最优价计税口径: {x!r}，允许：{sorted(OPTIMAL_PRICE_BASIS_ALLOWED)} 或中文如 基准价、3%增值税 等"
    )


class ComparisonRequest(BaseModel):
    """接口4 请求体"""

    model_config = ConfigDict(extra="ignore")

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
        description="货物吨数；总运费=freight_rates 每吨运费（元/吨）×吨数",
    )
    最优价计税口径列表: List[str] = Field(
        default_factory=lambda: ["3pct"],
        description=(
            "最优价（单价×吨数−总运费）按哪些口径各算一份。"
            "base=不含税基准；1pct/3pct/13pct=对应含税单价；"
            "normal_invoice/reverse_invoice=表中对应列单价（元/吨）。可多选，重复项会去重保序。"
        ),
    )
    最优价排序口径: Optional[str] = Field(
        None,
        description="明细与冶炼厂排行按该口径下的利润从高到低排序；须出现在最优价计税口径列表中；省略则用列表第一项",
    )
    报价日期: Optional[str] = Field(
        None,
        description=(
            "YYYY-MM-DD，可选。指定则只使用该日期的 quote_details；"
            "省略则对每个冶炼厂+品种名取「比价日历日及以前」中 quote_date 最近的一条（见接口文档 QUOTE_COMPARISON_TZ）"
        ),
    )

    @field_validator("price_type", mode="before")
    @classmethod
    def _normalize_price_type(cls, v: Any) -> Any:
        if v is None or (isinstance(v, str) and not str(v).strip()):
            return None
        return _normalize_comparison_price_type(v)

    @field_validator("最优价计税口径列表", mode="before")
    @classmethod
    def _normalize_optimal_basis_list(cls, v: Any) -> Any:
        if v is None:
            return ["3pct"]
        if not isinstance(v, list):
            raise ValueError("最优价计税口径列表须为数组")
        out: List[str] = []
        for x in v:
            out.append(_normalize_optimal_basis_token(x))
        return out if out else ["3pct"]

    @field_validator("最优价计税口径列表", mode="after")
    @classmethod
    def _dedupe_optimal_basis_list(cls, v: List[str]) -> List[str]:
        order_unique: List[str] = []
        seen = set()
        for x in v:
            if x not in OPTIMAL_PRICE_BASIS_ALLOWED:
                raise ValueError(
                    f"不支持的最优价计税口径: {x!r}，允许：{sorted(OPTIMAL_PRICE_BASIS_ALLOWED)}"
                )
            if x not in seen:
                seen.add(x)
                order_unique.append(x)
        return order_unique if order_unique else ["3pct"]

    @field_validator("最优价排序口径", mode="before")
    @classmethod
    def _normalize_optimal_sort_basis(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return _normalize_optimal_basis_token(v)

    @model_validator(mode="after")
    def _validate_optimal_sort_basis(self) -> "ComparisonRequest":
        sk = self.最优价排序口径
        if sk is not None and sk not in self.最优价计税口径列表:
            raise ValueError(
                f"最优价排序口径 {sk!r} 须为最优价计税口径列表中的一项，当前列表={self.最优价计税口径列表}"
            )
        return self


class AddWarehouseRequest(BaseModel):
    """添加仓库请求体：库房类型带出类型颜色，仓库可另有独立颜色"""
    仓库名: str = Field(..., description="仓库名称")
    地址: Optional[str] = Field(None, description="地址（可选）")
    仓库类型id: Optional[int] = Field(
        None,
        description="库房类型 ID（可选）；类型颜色来自类型表的「颜色配置」",
    )
    库房类型名: Optional[str] = Field(
        None,
        description="库房类型名称（可选）；若与省市区详址一并提供则走完整落库（含天地图经纬度），与仓库类型id二选一或同时传时优先名称",
    )
    省: Optional[str] = Field(None, description="省（与市、区、详细地址一并传时启用完整落库）")
    市: Optional[str] = Field(None, description="市")
    区: Optional[str] = Field(None, description="区/县")
    经度: Optional[float] = Field(
        None,
        ge=-180,
        le=180,
        description="默认不传：完整地址模式下由天地图解析；仅当与纬度同时传入时才跳过天地图并手写坐标",
    )
    纬度: Optional[float] = Field(
        None,
        ge=-90,
        le=90,
        description="默认不传：完整地址模式下由天地图解析；须与经度成对传入才可手写坐标",
    )
    仓库颜色配置: Optional[Any] = Field(
        None,
        description="仓库独立颜色（JSON，可选），与库房类型颜色并存；未传则不写入",
    )


class UpdateWarehouseRequest(BaseModel):
    """修改仓库请求体"""
    仓库id: int = Field(..., description="仓库ID")
    仓库名: Optional[str] = Field(None, description="仓库名称（可选）")
    is_active: Optional[bool] = Field(None, description="是否启用（可选）")
    地址: Optional[str] = Field(None, description="地址（可选）")
    仓库类型id: Optional[int] = Field(
        None,
        description="库房类型 ID（可选）；传 null 可取消类型关联（库房类型颜色随之不可用）",
    )
    库房类型名: Optional[str] = Field(
        None,
        description="库房类型名称；与省市区等组合修改时写入；传空字符串可取消类型关联",
    )
    省: Optional[str] = None
    市: Optional[str] = None
    区: Optional[str] = None
    经度: Optional[float] = Field(
        None,
        ge=-180,
        le=180,
        description="与纬度成对传则只改坐标；单改省市区/地址且未传经纬度时由天地图重算",
    )
    纬度: Optional[float] = Field(
        None,
        ge=-90,
        le=90,
        description="与经度成对传则只改坐标；否则随行政区/地址变更触发天地图",
    )
    仓库颜色配置: Optional[Any] = Field(
        None,
        description="仓库独立颜色（可选）；传 null 可清空；不传则不修改",
    )


class AddWarehouseTypeRequest(BaseModel):
    """新增库房类型（类型与颜色一对一）"""
    类型名: str = Field(..., description="类型名称，唯一")
    颜色配置: Optional[Any] = Field(
        None,
        description="颜色配置（JSON），如 {\"marker\": \"#3388ff\"} 或主色字段名与色值",
    )


class UpdateWarehouseTypeRequest(BaseModel):
    """修改库房类型"""
    类型id: int = Field(..., description="dict_warehouse_types.id")
    类型名: Optional[str] = Field(None, description="类型名称（可选）")
    颜色配置: Optional[Any] = Field(None, description="颜色配置（可选）；传 null 可清空")
    is_active: Optional[bool] = Field(None, description="是否启用（可选）")


class AddSmelterRequest(BaseModel):
    """新建冶炼厂（比价侧不设标记颜色；经纬度默认由天地图根据地址解析）"""
    冶炼厂名: str = Field(..., description="冶炼厂名称")
    地址: Optional[str] = Field(None, description="详细地址（与省市区一并传时走完整落库）")
    省: Optional[str] = None
    市: Optional[str] = None
    区: Optional[str] = None
    经度: Optional[float] = Field(
        None,
        ge=-180,
        le=180,
        description="默认不传；与纬度同时传则跳过天地图手写坐标",
    )
    纬度: Optional[float] = Field(
        None,
        ge=-90,
        le=90,
        description="默认不传；须与经度同时传才可手写坐标",
    )


class UploadVarietyRequest(BaseModel):
    """上传品种（单条，与上传运费相同可传列表批量）"""

    model_config = ConfigDict(populate_by_name=True)

    品种名: str = Field(
        ...,
        validation_alias=AliasChoices("品种名", "name", "varietyName", "categoryName"),
        description="品种名称，写入 dict_categories",
    )


class UpdateSmelterRequest(BaseModel):
    """修改冶炼厂（无颜色字段；改行政区/地址且未手传经纬度时重新天地图）"""
    冶炼厂id: int = Field(..., description="冶炼厂ID")
    冶炼厂名: Optional[str] = Field(None, description="冶炼厂名称（可选）")
    is_active: Optional[bool] = Field(None, description="是否启用（可选）")
    地址: Optional[str] = Field(None, description="冶炼厂地址（可选）；传 null 可清空")
    省: Optional[str] = None
    市: Optional[str] = None
    区: Optional[str] = None
    经度: Optional[float] = Field(
        None,
        ge=-180,
        le=180,
        description="与纬度成对传则直接改库中坐标",
    )
    纬度: Optional[float] = Field(
        None,
        ge=-90,
        le=90,
        description="与经度成对传则直接改库中坐标",
    )


class DownloadFreightTemplateRequest(BaseModel):
    """下载运费导入模板：首列为所选库房名称，表头为全部启用冶炼厂（其余格为空）。"""

    model_config = ConfigDict(extra="ignore")

    库房id列表: List[int] = Field(..., min_length=1, description="库房（仓库）ID 列表，顺序即模板首列自上而下顺序")


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

    model_config = ConfigDict(extra="ignore")

    品类id: int = Field(
        ...,
        validation_alias=AliasChoices("品类id", "品类ID", "category_id"),
        description="品类分组ID",
    )
    品类名称: List[str] = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("品类名称", "names", "aliasNames"),
        description=(
            "名称列表：默认（整组替换）时第一项为主名称，其余为别名；"
            "当 仅追加别名=true 时只填待追加的别名即可，勿再传主名称（主名称沿用库中已有）；"
            "也可传单个字符串，会自动当作单元素列表"
        ),
    )
    仅追加别名: bool = Field(
        False,
        validation_alias=AliasChoices(
            "仅追加别名",
            "append_only",
            "appendOnly",
            "append_aliases",
            "appendAliases",
        ),
        description=(
            "false（默认）：提交列表为该分组最终别名集，未出现在列表中的原启用别名将软删除；"
            "true：在保留该分组已有启用别名的前提下合并列表中的名称（去重）；"
            "此时 品类名称 只写要追加的别名；若分组已有启用行，新插入的名称不会成为主名称"
        ),
    )

    @field_validator("品类名称", mode="before")
    @classmethod
    def _coerce_category_names(cls, v: Any) -> Any:
        if isinstance(v, str):
            t = v.strip()
            return [t] if t else []
        if isinstance(v, (int, float)):
            return [str(v).strip()]
        return v


class UpdateCategoryMappingRequest(BaseModel):
    """接口7 请求体（与 CategoryMappingItem 字段一致，单条 JSON 时使用）"""

    model_config = ConfigDict(extra="ignore")

    品类id: int = Field(
        ...,
        validation_alias=AliasChoices("品类id", "品类ID", "category_id"),
    )
    品类名称: List[str] = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("品类名称", "names", "aliasNames"),
    )
    仅追加别名: bool = Field(
        False,
        validation_alias=AliasChoices(
            "仅追加别名",
            "append_only",
            "appendOnly",
            "append_aliases",
            "appendAliases",
        ),
    )

    @field_validator("品类名称", mode="before")
    @classmethod
    def _coerce_category_names_single(cls, v: Any) -> Any:
        if isinstance(v, str):
            t = v.strip()
            return [t] if t else []
        if isinstance(v, (int, float)):
            return [str(v).strip()]
        return v


class UpdateCategoryRowRequest(BaseModel):
    """按 dict_categories.row_id 修改单条别名（名称或主名称）"""

    model_config = ConfigDict(extra="ignore")

    行id: int = Field(..., ge=1, description="dict_categories 主键 row_id，见 get_category_mapping 别名行")
    品种名: Optional[str] = Field(
        None,
        description="新名称；传入则改名，并同步 quote_details 中历史 category_name",
    )
    设为主名称: Optional[bool] = Field(
        None,
        description="传 true 将该别名设为该品类组的主名称（is_main=1，同组其余为0）",
    )

    @model_validator(mode="after")
    def _at_least_one_field(self) -> "UpdateCategoryRowRequest":
        has_name = self.品种名 is not None and str(self.品种名).strip() != ""
        if has_name or self.设为主名称 is True:
            return self
        raise ValueError("至少需要提供非空的 品种名，或将 设为主名称 设为 true")


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
    价格字段来源: Optional[Dict[str, str]] = Field(
        None,
        description=(
            "各价格列来源：键为库列名（unit_price, price_1pct_vat, price_3pct_vat, price_13pct_vat, "
            "price_normal_invoice, price_reverse_invoice）或与上传接口一致的中文键（价格、价格_1pct增值税 等）；"
            "值为「原数据」或「换算」。上传识别返回的 items 中若带此字段可原样回传；确认写入时含1%/3%/13%价会按冶炼厂税率表重算并标为「换算」（不含税列按是否本次提交原值标原数据/换算）。"
        ),
    )


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
    tax_rate: float = Field(
        ...,
        description="税率：0~1 小数（如 0.03），或 1~100 表示百分比（如 3 表示 3%）",
    )

    @field_validator("tax_rate", mode="before")
    @classmethod
    def _coerce_tax_rate(cls, v: Any) -> Any:
        if v is None:
            raise ValueError("tax_rate 不能为空")
        return float(v)

    @field_validator("tax_rate", mode="after")
    @classmethod
    def _percent_to_fraction(cls, v: float) -> float:
        if v > 1.0:
            if v <= 100.0:
                return round(v / 100.0, 6)
            raise ValueError(f"百分比税率须在 1~100 之间，收到：{v}")
        if not (0 <= v <= 1):
            raise ValueError(f"税率须在 0~1 之间（或填写 1~100 表示百分比），收到：{v}")
        return v


class TaxRateUpsertRequest(BaseModel):
    """批量设置税率（upsert）"""
    items: List[TaxRateItem] = Field(..., description="税率列表")


class QuoteDetailsFilterRequest(BaseModel):
    """报价明细分页/导出共用的筛选条件（与 Query 参数一致，供 POST 导出使用）"""

    model_config = ConfigDict(extra="ignore")

    factory_id: Optional[int] = None
    quote_date: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    category_name: Optional[str] = None
    variety: Optional[str] = None
    category_exact: bool = False


class BatchWarehouseIdsRequest(BaseModel):
    """批量停用仓库（软删除）"""

    仓库id列表: List[int] = Field(..., min_length=1, description="仓库主键列表")


class BatchSmelterIdsRequest(BaseModel):
    """批量停用冶炼厂（软删除）"""

    冶炼厂id列表: List[int] = Field(..., min_length=1, description="冶炼厂主键列表")
