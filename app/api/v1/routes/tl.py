"""
TL比价模块路由
接口前缀：/tl
包含接口：
  0. POST /tl/add_warehouse            - 添加仓库（不存在则新建）
  1. GET  /tl/get_warehouses           - 获取仓库列表
  1b.POST /tl/update_warehouse         - 修改仓库信息
  1c.DELETE /tl/delete_warehouse        - 删除仓库（软删除）
  1d.POST /tl/add_smelter              - 新建冶炼厂
  2. GET  /tl/get_smelters             - 获取冶炼厂列表
  3. GET  /tl/get_categories           - 获取品类列表
  3b.POST /tl/upload_variety           - 上传品种（批量写入 dict_categories）
  4. POST /tl/get_comparison           - 获取比价表
  5. POST /tl/upload_price_table       - 上传价格表（OCR识别，返回原始识别结果）
  5b.POST /tl/confirm_price_table      - 确认写入报价数据（自动新建缺失冶炼厂/品类）
  5c.GET  /tl/get_quote_details_list   - 报价数据列表（分页、筛选）
  5d.GET  /tl/export_quote_details_excel - 导出报价数据 Excel（与查询条件一致）
  6. POST /tl/upload_freight           - 上传运费
  6a.POST /tl/download_freight_template_excel - 下载运费导入模板（Excel）
  6a2.POST /tl/import_freight_excel     - 导入运费配置（Excel，写入 freight_rates）
  6b.GET  /tl/get_freight_list         - 运费列表（分页、筛选）
  6c.POST /tl/update_freight           - 编辑运费（按 id）
  6d.DELETE /tl/delete_freight         - 删除运费（按 id）
  7a.GET  /tl/get_category_mapping     - 获取品类映射表
  7. POST /tl/update_category_mapping  - 更新品类映射表
  7b.POST /tl/update_category_row      - 按行修改品类别名（改名/设主名称）
  7c.DELETE /tl/delete_category        - 删除品类分组（软删除）
  7d.DELETE /tl/delete_category_row    - 删除单条品类别名（软删除）
"""
import io
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse

from app.models.tl import (
    ComparisonRequest,
    UploadFreightRequest,
    DownloadFreightTemplateRequest,
    UpdateFreightRequest,
    CategoryMappingItem,
    UpdateCategoryRowRequest,
    ConfirmPriceTableRequest,
    AddWarehouseRequest,
    UpdateWarehouseRequest,
    AddSmelterRequest,
    UploadVarietyRequest,
    UpdateSmelterRequest,
    PurchaseSuggestionRequest,
    VlmFullData,
    TaxRateItem,
    TaxRateUpsertRequest,
)
from app.services.tl_service import PurchaseSuggestionLLMError, TLService, get_tl_service

router = APIRouter(prefix="/tl", tags=["TL比价模块"])


def _merge_quote_list_filters(
    date_from: Optional[str],
    date_to: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    category_name: Optional[str],
    variety: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """与「查询条件」对齐：start_date/end_date 同 date_from/date_to；variety 优先于 category_name。"""
    d_from = date_from or start_date
    d_to = date_to or end_date
    cat: Optional[str] = None
    if variety is not None and str(variety).strip():
        cat = str(variety).strip()
    elif category_name is not None and str(category_name).strip():
        cat = str(category_name).strip()
    return d_from, d_to, cat


# ===================== 接口0：添加仓库 =====================

@router.post("/add_warehouse", summary="添加仓库")
def add_warehouse(
    body: AddWarehouseRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.add_warehouse(name=body.仓库名)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口1：获取仓库列表 =====================

@router.get("/get_warehouses", summary="获取仓库列表")
def get_warehouses(
    keyword: Optional[str] = Query(
        None,
        description="仓库名模糊搜索（可选）；不传则返回全部启用仓库",
    ),
    service: TLService = Depends(get_tl_service),
):
    try:
        data = service.get_warehouses(keyword=keyword)
        return {"code": 200, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口1b：修改仓库 =====================

@router.post("/update_warehouse", summary="修改仓库信息")
def update_warehouse(
    body: UpdateWarehouseRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.update_warehouse(
            warehouse_id=body.仓库id,
            name=body.仓库名,
            is_active=body.is_active,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口1c：删除仓库 =====================

@router.delete("/delete_warehouse", summary="删除仓库（软删除）")
def delete_warehouse(
    warehouse_id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_warehouse(warehouse_id=warehouse_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口1d：新建冶炼厂 =====================

@router.post("/add_smelter", summary="新建冶炼厂")
def add_smelter(
    body: AddSmelterRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.add_smelter(name=body.冶炼厂名)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口2：获取冶炼厂列表 =====================

@router.get("/get_smelters", summary="获取冶炼厂列表")
def get_smelters(service: TLService = Depends(get_tl_service)):
    try:
        data = service.get_smelters()
        return {"code": 200, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口2b：修改冶炼厂 =====================

@router.post("/update_smelter", summary="修改冶炼厂信息")
def update_smelter(
    body: UpdateSmelterRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.update_smelter(
            smelter_id=body.冶炼厂id,
            name=body.冶炼厂名,
            is_active=body.is_active,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口2c：删除冶炼厂 =====================

@router.delete("/delete_smelter", summary="删除冶炼厂（软删除）")
def delete_smelter(
    smelter_id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_smelter(smelter_id=smelter_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口3：获取品类列表 =====================

@router.get("/get_categories", summary="获取品类列表")
def get_categories(service: TLService = Depends(get_tl_service)):
    try:
        data = service.get_categories()
        return {"code": 200, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口3b：上传品种 =====================

@router.post("/upload_variety", summary="上传品种")
def upload_variety(
    body: List[UploadVarietyRequest],
    service: TLService = Depends(get_tl_service),
):
    """批量提交品种名：新建品类分组、已存在则跳过、停用则恢复启用（与报价确认时新建品类规则一致）。"""
    try:
        items = [item.model_dump() for item in body]
        return service.upload_variety(items)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口4：获取比价表 =====================

@router.post("/get_comparison", summary="获取比价表")
def get_comparison(
    body: ComparisonRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        out = service.get_comparison(
            warehouse_ids=body.选中仓库id列表,
            smelter_ids=body.冶炼厂id列表,
            category_ids=body.品类id列表,
            price_type=body.price_type,
            tons=body.吨数,
            optimal_basis_list=body.最优价计税口径列表,
            optimal_sort_basis=body.最优价排序口径,
            quote_date_str=body.报价日期,
        )
        return {
            "code": 200,
            "data": out["明细"],
            "冶炼厂利润排行": out["冶炼厂利润排行"],
            "最优价排序口径": out["最优价排序口径"],
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口5：上传价格表 =====================

@router.post("/upload_price_table", summary="上传价格表")
def upload_price_table(
    file: List[UploadFile] = File(..., description="价格表图片，支持批量上传"),
    service: TLService = Depends(get_tl_service),
):
    allowed_types = {"image/jpeg", "image/jpg", "image/png", "image/bmp", "image/webp"}
    for f in file:
        if f.content_type not in allowed_types:
            raise HTTPException(
                status_code=400,
                detail=f"文件 '{f.filename}' 格式不支持，仅允许 jpg/png/bmp/webp",
            )
    try:
        return service.upload_price_table(file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口5b：确认价格表写入 =====================

@router.post("/confirm_price_table", summary="确认并写入报价数据")
def confirm_price_table(
    body: ConfirmPriceTableRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        items = [item.model_dump() for item in body.数据]
        full_data = body.full_data.model_dump() if body.full_data else None
        return service.confirm_price_table(
            quote_date_str=body.报价日期,
            items=items,
            full_data=full_data,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口5c：报价数据列表 =====================

@router.get("/get_quote_details_list", summary="报价数据列表")
def get_quote_details_list(
    factory_id: Optional[int] = None,
    quote_date: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category_name: Optional[str] = None,
    variety: Optional[str] = None,
    category_exact: bool = Query(
        False,
        description="品种为下拉精确选中时传 true；false 为模糊匹配（默认）",
    ),
    page: int = 1,
    page_size: int = 50,
    response_format: str = Query(
        "full",
        description='返回字段：`full`=库表全量列；`table`=与「报价数据列表」页表格列一致（日期/冶炼厂/品种/基准价/3%含税价/13%含税价）',
    ),
    service: TLService = Depends(get_tl_service),
):
    """报价明细分页；查询条件区可用 start_date/end_date、variety；冶炼厂用 factory_id。"""
    eff_from, eff_to, eff_cat = _merge_quote_list_filters(
        date_from, date_to, start_date, end_date, category_name, variety
    )
    try:
        return service.get_quote_details_list(
            factory_id=factory_id,
            quote_date=quote_date,
            date_from=eff_from,
            date_to=eff_to,
            category_name=eff_cat,
            category_exact=category_exact,
            page=page,
            page_size=page_size,
            response_format=response_format,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口5d：导出报价数据 Excel =====================

@router.get("/export_quote_details_excel", summary="导出报价数据 Excel")
def export_quote_details_excel(
    factory_id: Optional[int] = None,
    quote_date: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category_name: Optional[str] = None,
    variety: Optional[str] = None,
    category_exact: bool = Query(
        False,
        description="与列表接口一致：下拉选品种建议 true",
    ),
    service: TLService = Depends(get_tl_service),
):
    """筛选条件与 get_quote_details_list 相同，表头为：日期、冶炼厂、品种、基准价、3%含税价、13%含税价。"""
    eff_from, eff_to, eff_cat = _merge_quote_list_filters(
        date_from, date_to, start_date, end_date, category_name, variety
    )
    try:
        data = service.export_quote_details_excel(
            factory_id=factory_id,
            quote_date=quote_date,
            date_from=eff_from,
            date_to=eff_to,
            category_name=eff_cat,
            category_exact=category_exact,
        )
        filename = "报价数据导出.xlsx"
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}",
            },
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口6：上传运费 =====================

@router.post("/upload_freight", summary="上传运费")
def upload_freight(
    body: List[UploadFreightRequest],
    service: TLService = Depends(get_tl_service),
):
    try:
        freight_list = [item.model_dump() for item in body]
        return service.upload_freight(freight_list)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口6a：下载运费导入模板 Excel =====================


@router.post("/download_freight_template_excel", summary="下载运费导入模板（Excel）")
def download_freight_template_excel(
    body: DownloadFreightTemplateRequest,
    service: TLService = Depends(get_tl_service),
):
    """表头为「库房」及全部启用冶炼厂；首列为请求中的库房名称（按 id 顺序），其余单元格为空，供填写后走 import_freight_excel 导入。"""
    try:
        data = service.build_freight_template_excel(warehouse_ids=body.库房id列表)
        filename = "运费导入模板.xlsx"
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}",
            },
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口6a2：导入运费配置 Excel =====================


@router.post("/import_freight_excel", summary="导入运费配置（Excel）")
async def import_freight_excel(
    file: UploadFile = File(..., description="由 download_freight_template_excel 生成并填写后的 xlsx"),
    service: TLService = Depends(get_tl_service),
):
    """识别首列库房、表头冶炼厂与单元格数值，写入 freight_rates（当日生效）；字典中不存在的库房/冶炼厂名称会自动新建（已停用则恢复启用）。结果可在 get_freight_list 中查询。"""
    try:
        raw = await file.read()
        return service.import_freight_excel(raw)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口6b：运费列表 =====================

@router.get("/get_freight_list", summary="运费列表")
def get_freight_list(
    warehouse_id: Optional[int] = None,
    factory_id: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    include_latest_quotes: bool = Query(
        False,
        description=(
            "为 true 时，在 data 中附带「冶炼厂各品种最新报价」：按冶炼厂+品种名称取 quote_details 最新日期；"
            "无报价记录则各价格字段为 null（与比价取价一致）"
        ),
    ),
    service: TLService = Depends(get_tl_service),
):
    """按仓库/冶炼厂/生效日期区间筛选，默认按生效日期倒序分页。"""
    try:
        return service.get_freight_list(
            warehouse_id=warehouse_id,
            factory_id=factory_id,
            date_from=date_from,
            date_to=date_to,
            page=page,
            page_size=page_size,
            include_latest_quotes=include_latest_quotes,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口6c：编辑运费 =====================

@router.post("/update_freight", summary="编辑运费")
def update_freight(
    body: UpdateFreightRequest,
    service: TLService = Depends(get_tl_service),
):
    """按 `get_freight_list` 返回的 `id` 更新单价；可选修改生效日期（不可与同仓库+冶炼厂下其它记录日期冲突）。"""
    try:
        return service.update_freight(
            freight_id=body.运费id,
            price_per_ton=body.运费,
            effective_date_str=body.生效日期,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口6d：删除运费 =====================

@router.delete("/delete_freight", summary="删除运费")
def delete_freight(
    freight_id: int = Query(..., description="freight_rates 主键，与 get_freight_list 返回的 id 一致"),
    service: TLService = Depends(get_tl_service),
):
    """物理删除一条运费配置；删除后同仓库+冶炼厂可重新上传该生效日期的运费。"""
    try:
        return service.delete_freight(freight_id=freight_id)
    except ValueError as e:
        msg = str(e)
        code = 404 if "运费记录不存在" in msg else 400
        raise HTTPException(status_code=code, detail=msg)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口7a：获取品类映射表 =====================

@router.get("/get_category_mapping", summary="获取品类映射表")
def get_category_mapping(service: TLService = Depends(get_tl_service)):
    try:
        data = service.get_category_mapping()
        return {"code": 200, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# ===================== 接口A7：采购建议 =====================

@router.post("/get_purchase_suggestion", summary="采购建议")
def get_purchase_suggestion(
    body: PurchaseSuggestionRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        demands = [d.model_dump() for d in body.demands]
        return service.get_purchase_suggestion(
            warehouse_ids=body.warehouse_ids,
            demands=demands,
            price_type=body.price_type,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PurchaseSuggestionLLMError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 税率表接口 =====================

@router.get("/get_tax_rates", summary="获取税率表")
def get_tax_rates(
    factory_ids: Optional[str] = None,
    service: TLService = Depends(get_tl_service),
):
    """factory_ids: 逗号分隔的冶炼厂ID，不传则返回全部"""
    try:
        ids = [int(x) for x in factory_ids.split(",")] if factory_ids else None
        data = service.get_tax_rates(factory_ids=ids)
        return {"code": 200, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upsert_tax_rates", summary="批量设置税率")
def upsert_tax_rates(
    body: TaxRateUpsertRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        items = [item.model_dump() for item in body.items]
        return service.upsert_tax_rates(items)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_tax_rate", summary="删除某冶炼厂的某税率记录")
def delete_tax_rate(
    factory_id: int,
    tax_type: str,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_tax_rate(factory_id=factory_id, tax_type=tax_type)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update_category_mapping", summary="更新品类映射表")
def update_category_mapping(
    body: List[CategoryMappingItem],
    service: TLService = Depends(get_tl_service),
):
    try:
        batch = [(it.品类id, it.品类名称, it.仅追加别名) for it in body]
        return service.update_category_mapping_batch(batch)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口7b：按行修改品类别名 =====================

@router.post("/update_category_row", summary="按行修改品类别名")
def update_category_row(
    body: UpdateCategoryRowRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.update_category_row(
            row_id=body.行id,
            new_name=body.品种名,
            set_main=body.设为主名称,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口7c：删除品类分组 =====================

@router.delete("/delete_category", summary="删除品类分组（软删除）")
def delete_category(
    品类id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_category(category_id=品类id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口7d：删除单条品类别名 =====================

@router.delete("/delete_category_row", summary="删除单条品类别名（软删除）")
def delete_category_row(
    行id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_category_row(row_id=行id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
