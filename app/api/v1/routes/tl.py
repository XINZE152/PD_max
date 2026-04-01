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
  6b.GET  /tl/get_freight_list         - 运费列表（分页、筛选）
  7a.GET  /tl/get_category_mapping     - 获取品类映射表
  7. POST /tl/update_category_mapping  - 更新品类映射表
"""
import io
from typing import List, Optional, Tuple
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse

from app.models.tl import (
    ComparisonRequest,
    UploadFreightRequest,
    CategoryMappingItem,
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
from app.services.tl_service import TLService, get_tl_service

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
def get_warehouses(service: TLService = Depends(get_tl_service)):
    try:
        data = service.get_warehouses()
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
        data = service.get_comparison(
            warehouse_ids=body.选中仓库id列表,
            smelter_ids=body.冶炼厂id列表,
            category_ids=body.品类id列表,
            price_type=body.price_type,
        )
        return {"code": 200, "data": data}
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


# ===================== 接口6b：运费列表 =====================

@router.get("/get_freight_list", summary="运费列表")
def get_freight_list(
    warehouse_id: Optional[int] = None,
    factory_id: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
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
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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

def update_category_mapping(
    body: List[CategoryMappingItem],
    service: TLService = Depends(get_tl_service),
):
    try:
        for item in body:
            service.update_category_mapping(
                category_id=item.品类id,
                names=item.品类名称,
            )
        return {"code": 200, "msg": "品类映射表更新成功，数据已存入数据库"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
