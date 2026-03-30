"""
TL比价模块路由
接口前缀：/tl
包含接口：
  0. POST /tl/add_warehouse            - 添加仓库（不存在则新建）
  1. GET  /tl/get_warehouses           - 获取仓库列表
  2. GET  /tl/get_smelters             - 获取冶炼厂列表
  3. GET  /tl/get_categories           - 获取品类列表
  4. POST /tl/get_comparison           - 获取比价表
  5. POST /tl/upload_price_table       - 上传价格表（OCR识别，返回原始识别结果）
  5b.POST /tl/confirm_price_table      - 确认写入报价数据（自动新建缺失冶炼厂/品类）
  6. POST /tl/upload_freight           - 上传运费
  7a.GET  /tl/get_category_mapping     - 获取品类映射表
  7. POST /tl/update_category_mapping  - 更新品类映射表
"""
from typing import List

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File

from app.models.tl import (
    ComparisonRequest,
    UploadFreightRequest,
    CategoryMappingItem,
    ConfirmPriceTableRequest,
    AddWarehouseRequest,
    PurchaseSuggestionRequest,
)
from app.services.tl_service import TLService, get_tl_service

router = APIRouter(prefix="/tl", tags=["TL比价模块"])


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


# ===================== 接口2：获取冶炼厂列表 =====================

@router.get("/get_smelters", summary="获取冶炼厂列表")
def get_smelters(service: TLService = Depends(get_tl_service)):
    try:
        data = service.get_smelters()
        return {"code": 200, "data": data}
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
            tax_type=body.税率类型,
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
        return service.confirm_price_table(
            quote_date_str=body.报价日期,
            items=items,
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
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update_category_mapping", summary="更新品类映射表")
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
