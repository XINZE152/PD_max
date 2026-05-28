"""
TL比价模块路由
接口前缀：/tl
仓库/冶炼厂仅通过本模块 /tl/* 维护（无独立 /warehouse、/smelter 路由）；地理编码见 tl_dict_geo_crud + tianditu_geocoder。
包含接口：
  0. POST /tl/add_warehouse            - 添加仓库（省市区+详址齐全时经纬度默认由天地图解析）
  1. GET  /tl/get_warehouses           - 获取仓库列表（keyword；可选 page；size 最大 200）
  1a.  GET/POST/DELETE  /tl/get_warehouse_types, /add_warehouse_type, /update_warehouse_type, /delete_warehouse_type  - 库房类型与颜色
  1b.POST /tl/update_warehouse         - 修改仓库信息
  1c.DELETE /tl/delete_warehouse        - 删除仓库（软删除）
  1c2.DELETE /tl/purge_warehouse        - 永久删除仓库（硬删除）
  1d.库房单向关联（有向图）：POST /tl/bind_warehouse_link、DELETE /tl/unbind_warehouse_link、
      POST /tl/batch_bind_warehouse_links、POST /tl/batch_unbind_warehouse_links、
      GET /tl/get_warehouse_links_list、GET /tl/get_link_realtime_spread_list（实时价差列表）、
      GET /tl/get_tier_price_spread_list（阶梯差价列表）、
      GET /tl/get_warehouse_links_outbound、GET /tl/get_warehouse_links_inbound、
      PUT /tl/replace_warehouse_links_outbound、PUT /tl/update_warehouse_link_tier
  1e.POST /tl/add_smelter              - 新建冶炼厂（可选循融宝发货，默认否）
  2. GET  /tl/get_smelters             - 获取冶炼厂列表（size 最大 200；含循融宝发货）
  2a. GET  /tl/get_smelter              - 获取单个冶炼厂详情（含循融宝、is_active）
  2a1.GET  /tl/list_smelter_xunrongbao  - 查询全部冶炼厂循融宝状态及加价元/吨
  2b1.POST /tl/set_smelter_xunrongbao   - 设置单个冶炼厂是否循融宝发货（改）
  2b2.DELETE /tl/smelter_xunrongbao/{id} - 关闭循融宝发货（删开关，不删厂）
  2b3.POST /tl/batch_set_smelters_xunrongbao - 批量设置循融宝发货
  2c2.DELETE /tl/purge_smelter         - 永久删除冶炼厂（硬删除；默认级联删运费/报价等；?cascade=false 为严格仅删厂）
  2d.GET  /tl/calculate_distance       - 计算两组经纬度的球面直线距离（km）
  3. GET  /tl/get_categories           - 获取品类列表
  3b.POST /tl/upload_variety           - 上传品种（批量写入 dict_categories）
  4. POST /tl/get_comparison           - 获取比价表
  5. POST /tl/upload_price_table       - 上传价格表（OCR识别，返回原始识别结果）
  5a1.GET /tl/download_quote_list_template_excel - 下载报价列表导入模板（xlsx）
  5a2.POST /tl/upload_price_table_excel - 上传报价列表（xlsx 解析，返回 items/full_data 供确认写入）
  5b.POST /tl/confirm_price_table      - 确认写入报价数据（冶炼厂须字典名称精确匹配；品类缺失仍可自动新建）
  5b2.POST /tl/manual_quote            - 手写录入报价（无 OCR；请求体与 confirm 相同，full_data 可省略）
  5b3.POST /tl/update_quote_detail     - 按明细 id 修改报价（改价后按冶炼厂税率重算各档含税价）
  5b4.DELETE /tl/quote_detail/{detail_id} - 按 id 删除单条报价明细（报价查询纠错）
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
  8. 对标定价 / 标定价格 / 库房差额 / AI 分析快照：
      GET/POST/PUT/DELETE /tl/province_benchmark_prices — 省份对标城市定价历史
      GET/POST/PUT/DELETE /tl/smelter_calibration_prices — 冶炼厂标定价格历史
      GET/POST/PUT/DELETE /tl/warehouse_spread_configs — 库房对标差额与毛利配置
      POST /tl/import_warehouse_spread_excel — 导入库房差额与毛利（xlsx，读取全部工作表）
      GET /tl/ai_pricing_analysis — 实时聚合分析（公式：库房定价=对标城市定价+差额；毛利计算版=标定−运费−库房定价）
      GET/POST /tl/ai_pricing_snapshots、GET/PUT/DELETE /tl/ai_pricing_snapshots/{id} — 快照 CRUD
      PUT/DELETE /tl/ai_pricing_snapshots/{id}/items/{item_id} — 明细备注/删除
"""
import asyncio
import io
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse

from app.models.tl import (
    AiPricingSnapshotCreate,
    AiPricingSnapshotItemRemarkBody,
    AiPricingSnapshotUpdate,
    BatchSmelterIdsRequest,
    BatchWarehouseIdsRequest,
    ComparisonRequest,
    UploadFreightRequest,
    DownloadFreightTemplateRequest,
    UpdateFreightRequest,
    CategoryMappingItem,
    UpdateCategoryRowRequest,
    ConfirmPriceTableRequest,
    ManualQuoteRequest,
    UpdateQuoteDetailRequest,
    AddWarehouseRequest,
    AddWarehouseTypeRequest,
    UpdateWarehouseRequest,
    WarehouseLinkBindRequest,
    WarehouseLinkUpdateTierRequest,
    WarehouseLinksBatchOutboundRequest,
    WarehouseLinksReplaceOutboundRequest,
    UpdateWarehouseTypeRequest,
    AddSmelterRequest,
    UploadVarietyRequest,
    UpdateSmelterRequest,
    SmelterXunrongbaoItem,
    BatchSetSmeltersXunrongbaoRequest,
    PurchaseSuggestionRequest,
    VlmFullData,
    TaxRateItem,
    TaxRateUpsertRequest,
    ProvinceBenchmarkPriceCreate,
    ProvinceBenchmarkPriceUpdate,
    QuoteDetailsFilterRequest,
    SmelterCalibrationPriceCreate,
    SmelterCalibrationPriceUpdate,
    WarehouseSpreadConfigCreate,
    WarehouseSpreadConfigUpdate,
)
from app.services.partner_warehouse_excel import (
    PartnerWarehouseExcelError,
    parse_partner_warehouse_rows,
    warehouse_site_fields_from_full_address,
)
from app.services.tl_service import PurchaseSuggestionLLMError, TLService, get_tl_service

router = APIRouter(prefix="/tl", tags=["TL比价模块"])


def _default_warehouse_import_concurrency() -> int:
    """环境变量 TL_IMPORT_WAREHOUSE_CONCURRENCY（1–20），缺省 3。"""
    raw = (os.getenv("TL_IMPORT_WAREHOUSE_CONCURRENCY") or "3").strip()
    try:
        return max(1, min(20, int(raw)))
    except ValueError:
        return 3


_DEFAULT_WAREHOUSE_IMPORT_CONCURRENCY = _default_warehouse_import_concurrency()


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


def _validate_coordinate(longitude: float, latitude: float, label: str) -> None:
    if not -180 <= longitude <= 180:
        raise ValueError(f"{label}经度必须在 -180 到 180 之间")
    if not -90 <= latitude <= 90:
        raise ValueError(f"{label}纬度必须在 -90 到 90 之间")


def _haversine_distance_km(
    lng1: float,
    lat1: float,
    lng2: float,
    lat2: float,
) -> float:
    """计算两个 WGS84 经纬度点的球面直线距离，单位 km。"""
    radius_km = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lng2 - lng1)

    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


def _quote_details_excel_response(data: bytes) -> StreamingResponse:
    filename = "报价数据导出.xlsx"
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}",
        },
    )


# ===================== 接口0：添加仓库 =====================

@router.post("/add_warehouse", summary="添加仓库")
def add_warehouse(
    body: AddWarehouseRequest,
    service: TLService = Depends(get_tl_service),
):
    """省、市、区与详细地址齐全时写入完整记录，经度/纬度未同时传则走天地图；否则走极简 name+地址+类型。"""
    try:
        return service.add_warehouse(
            name=body.仓库名,
            address=body.地址,
            warehouse_type_id=body.仓库类型id,
            warehouse_color_config=body.仓库颜色配置,
            province=body.省,
            city=body.市,
            district=body.区,
            longitude=body.经度,
            latitude=body.纬度,
            warehouse_type_name=body.库房类型名,
            contact_name=body.库房联系人,
            contact_phone=body.电话,
            hazardous_waste_license_qty=body.危废经营许可数量,
            monthly_avg_receipt_ton=body.月均收货,
            current_inventory_ton=body.当前库存,
            receipt_price_per_ton=body.收货价格,
            freight_amount=body.运费,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _import_partner_row_async(
    sem: asyncio.Semaphore,
    service: TLService,
    idx: int,
    wh_name: str,
    full_addr: str,
    wt_name: Optional[str],
    wt_id: Optional[int],
    geocode_sleep: float,
) -> Dict[str, Any]:
    """单行：线程池执行 add_warehouse，信号量限流；sleep 放在锁外以免占满并发槽。"""
    pv, cv, dv, addr = warehouse_site_fields_from_full_address(full_addr)
    try:
        async with sem:

            def _call() -> Dict[str, Any]:
                return service.add_warehouse(
                    name=wh_name,
                    address=addr,
                    province=pv,
                    city=cv,
                    district=dv,
                    warehouse_type_name=wt_name,
                    warehouse_type_id=wt_id,
                )

            out = await asyncio.to_thread(_call)
        if geocode_sleep > 0:
            await asyncio.sleep(float(geocode_sleep))
        return {
            "index": idx,
            "仓库名": wh_name,
            "省": pv,
            "市": cv,
            "区": dv,
            "详址": addr,
            "response": out,
        }
    except ValueError as e:
        if geocode_sleep > 0:
            await asyncio.sleep(float(geocode_sleep))
        return {"index": idx, "仓库名": wh_name, "error": str(e)}


def _rollup_partner_import_summary(rows_out: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {"新建": 0, "已存在": 0, "失败": 0, "其它": 0}
    for r in rows_out:
        if "error" in r:
            summary["失败"] += 1
            continue
        out = r.get("response") or {}
        code = int(out.get("code", 0))
        is_new = out.get("新建")
        if code == 200 and is_new is True:
            summary["新建"] += 1
        elif code == 200 and is_new is False:
            summary["已存在"] += 1
        elif code == 200:
            summary["其它"] += 1
        else:
            summary["失败"] += 1
    return summary


@router.post("/import_partner_warehouses_excel", summary="批量导入合作库房清单 Excel")
async def import_partner_warehouses_excel(
    file: UploadFile = File(..., description="xlsx，需含库房名称、库房地址列（规则同离线脚本）"),
    库房类型名: Optional[str] = Form(
        None,
        description="与单条 add_warehouse 一致：完整省市区详址落库时须能解析出类型（名称或 id 至少其一）",
    ),
    仓库类型id: Optional[int] = Form(None, description="可选，与 库房类型名 二选一或并存（服务内优先名称）"),
    sheet: Optional[str] = Form(None, description="工作表名；不传则优先「合作库房清单」"),
    sheet_index: Optional[int] = Form(None, description="工作表索引（0 起）；与 sheet 二选一"),
    geocode_sleep: float = Form(
        0.2,
        ge=0.0,
        le=10.0,
        description="每行处理完成后的休眠秒数（与并发并存，略降天地图压力）；设为 0 关闭",
    ),
    limit: Optional[int] = Form(
        None,
        ge=1,
        le=5000,
        description="仅处理前 N 条有效行（不传则处理全部）",
    ),
    concurrency: int = Form(
        _DEFAULT_WAREHOUSE_IMPORT_CONCURRENCY,
        ge=1,
        le=20,
        description="同时处理的最大行数（1–20）；过大易触发天地图限流，可用环境变量 TL_IMPORT_WAREHOUSE_CONCURRENCY 改默认",
    ),
    service: TLService = Depends(get_tl_service),
):
    """
    读取上传文件，按行解析后与 ``POST /tl/add_warehouse`` 相同调用 ``TLService.add_warehouse``：
    整行地址经 ``cn_address_split`` 拆省市区详址；四级齐全则走完整落库（天地图经纬度），否则走 name+地址极简落库。
    DB/天地图在默认线程池执行，事件循环可继续响应其它请求；并发由 ``concurrency`` 限制，避免拖死进程。
    """
    fn = (file.filename or "").lower()
    if not fn.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="请上传 .xlsx 或 .xls 文件")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="空文件")

    try:
        sheet_used, name_col, addr_col, rows = parse_partner_warehouse_rows(
            raw,
            sheet=sheet,
            sheet_index=sheet_index,
        )
    except PartnerWarehouseExcelError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    if limit is not None:
        rows = rows[: int(limit)]

    wt_name = (库房类型名 or "").strip() or None
    wt_id = 仓库类型id

    sem = asyncio.Semaphore(max(1, min(20, int(concurrency))))
    tasks = [
        _import_partner_row_async(
            sem,
            service,
            idx,
            wh_name,
            full_addr,
            wt_name,
            wt_id,
            float(geocode_sleep),
        )
        for idx, (wh_name, full_addr) in enumerate(rows, start=1)
    ]
    results = await asyncio.gather(*tasks)
    results = sorted(results, key=lambda r: int(r["index"]))
    summary = _rollup_partner_import_summary(results)

    return {
        "code": 200,
        "data": {
            "sheet": sheet_used,
            "name_column": name_col,
            "address_column": addr_col,
            "total_rows": len(rows),
            "summary": summary,
            "rows": results,
        },
    }


# ===================== 接口1：获取仓库列表 =====================

@router.get("/get_warehouses", summary="获取仓库列表")
def get_warehouses(
    keyword: Optional[str] = Query(
        None,
        description="仓库名模糊搜索（可选）；不传则返回全部启用仓库",
    ),
    page: Optional[int] = Query(
        None,
        ge=1,
        description="分页页码；传入则返回 data 为 { list, total, page, size }，并与省/市/区/status 筛选联用",
    ),
    size: Optional[int] = Query(
        None,
        ge=1,
        le=200,
        description="分页大小（默认 20，单页最多 200）；须与 page 同用",
    ),
    province: Optional[str] = Query(None, description="省（精确，仅分页模式）"),
    city: Optional[str] = Query(None, description="市（精确，仅分页模式）"),
    district: Optional[str] = Query(None, description="区（精确，仅分页模式）"),
    status: Optional[int] = Query(
        None,
        description="1 启用 0 停用；分页时省略则默认仅启用",
    ),
    service: TLService = Depends(get_tl_service),
):
    """未传 page 时返回全部启用仓库（含省市区与经纬度列）；传 page 时分页并支持省/市/区/status 筛选。"""
    try:
        data = service.get_warehouses(
            keyword=keyword,
            page=page,
            size=size,
            province=province,
            city=city,
            district=district,
            status=status,
        )
        return {"code": 200, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口1a：库房类型（类型-颜色）维护 =====================

@router.get("/get_warehouse_types", summary="库房类型列表")
def get_warehouse_types(
    keyword: Optional[str] = Query(None, description="类型名模糊搜索（可选）"),
    include_inactive: bool = Query(
        False,
        description="是否包含已停用的类型",
    ),
    service: TLService = Depends(get_tl_service),
):
    try:
        data = service.get_warehouse_types(
            keyword=keyword,
            include_inactive=include_inactive,
        )
        return {"code": 200, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_warehouse_type", summary="新增库房类型")
def add_warehouse_type(
    body: AddWarehouseTypeRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.add_warehouse_type(
            name=body.类型名,
            color_config=body.颜色配置,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update_warehouse_type", summary="修改库房类型")
def update_warehouse_type(
    body: UpdateWarehouseTypeRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        patch = body.model_dump(exclude_unset=True)
        type_id = patch.pop("类型id")
        return service.update_warehouse_type(type_id=type_id, patch=patch)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_warehouse_type", summary="删除库房类型（软删除）")
def delete_warehouse_type(
    type_id: int = Query(..., description="库房类型 id"),
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_warehouse_type(type_id=type_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口1b：修改仓库 =====================

@router.post("/update_warehouse", summary="修改仓库信息")
def update_warehouse(
    body: UpdateWarehouseRequest,
    service: TLService = Depends(get_tl_service),
):
    """含省/市/区/经纬度/库房类型名等字段时走地理落库逻辑；单改名称/类型/颜色等仍支持。"""
    try:
        patch = body.model_dump(exclude_unset=True)
        warehouse_id = patch.pop("仓库id")
        return service.update_warehouse(warehouse_id=warehouse_id, patch=patch)
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


@router.delete("/purge_warehouse", summary="永久删除仓库（硬删除）")
def purge_warehouse(
    warehouse_id: int,
    service: TLService = Depends(get_tl_service),
):
    """物理删除 dict_warehouses 行；若运费等仍引用该仓库则返回 409。"""
    try:
        return service.purge_warehouse(warehouse_id=warehouse_id)
    except ValueError as e:
        detail = str(e)
        if "不存在" in detail or detail == "仓库 id 无效":
            raise HTTPException(status_code=404, detail=detail)
        raise HTTPException(status_code=409, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _tl_value_error_http(e: ValueError) -> HTTPException:
    detail = str(e)
    if "不存在" in detail or "关联不存在" in detail:
        return HTTPException(status_code=404, detail=detail)
    return HTTPException(status_code=400, detail=detail)


# ===================== 接口1d：库房单向关联（有向图）====================

@router.post("/bind_warehouse_link", summary="绑定库房单向关联（新增出边）")
def bind_warehouse_link(
    body: WarehouseLinkBindRequest,
    service: TLService = Depends(get_tl_service),
):
    """一条有向边：源库房 → 对标库房；可附带阶梯价差 JSON；重复绑定返回错误。"""
    try:
        return service.bind_warehouse_link(
            body.源库房id,
            body.目标库房id,
            tier_price_spread=body.阶梯价差,
        )
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/unbind_warehouse_link", summary="解绑库房单向关联（删除出边）")
def unbind_warehouse_link(
    from_warehouse_id: int = Query(..., ge=1, description="源库房 id"),
    to_warehouse_id: int = Query(..., ge=1, description="对标库房 id"),
    service: TLService = Depends(get_tl_service),
):
    """删除 ``from_warehouse_id → to_warehouse_id`` 这一条边。"""
    try:
        return service.unbind_warehouse_link(from_warehouse_id, to_warehouse_id)
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch_bind_warehouse_links", summary="批量绑定库房单向关联（一次新增多条出边）")
def batch_bind_warehouse_links(
    body: WarehouseLinksBatchOutboundRequest,
    service: TLService = Depends(get_tl_service),
):
    """同一源库房对列表内多个目标依次绑定；已存在的边跳过并计入 skipped。"""
    try:
        return service.batch_bind_warehouse_links(body.源库房id, body.目标库房id列表)
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch_unbind_warehouse_links", summary="批量解绑库房单向关联（一次删除多条出边）")
def batch_unbind_warehouse_links(
    body: WarehouseLinksBatchOutboundRequest,
    service: TLService = Depends(get_tl_service),
):
    """同一源库房对列表内多个目标依次解绑；本来不存在的边不计入删除条数。"""
    try:
        return service.batch_unbind_warehouse_links(body.源库房id, body.目标库房id列表)
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_warehouse_links_list", summary="库房关联列表（全部有向边分页）")
def get_warehouse_links_list(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    warehouse_id: Optional[int] = Query(
        None,
        description="涉及该库房 id 的关联（作为源或目标）；不传则不限定",
    ),
    from_warehouse_id: Optional[int] = Query(
        None,
        ge=1,
        description="仅源库房 id 等于该值的边",
    ),
    to_warehouse_id: Optional[int] = Query(
        None,
        ge=1,
        description="仅对标库房 id 等于该值的边",
    ),
    keyword: Optional[str] = Query(
        None,
        description="源或目标库房名称模糊匹配（可选）",
    ),
    has_tier_price_spread: Optional[bool] = Query(
        None,
        description="true=仅已配置阶梯差价；false=仅未配置；不传=全部",
    ),
    service: TLService = Depends(get_tl_service),
):
    """每条记录包含源库房、对标库房摘要及关联 id、距离千米、阶梯价差；可与出边/入边列表配合。"""
    try:
        return service.get_warehouse_links_list(
            page=page,
            size=size,
            warehouse_id=warehouse_id,
            from_warehouse_id=from_warehouse_id,
            to_warehouse_id=to_warehouse_id,
            keyword=keyword,
            has_tier_price_spread=has_tier_price_spread,
        )
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_link_realtime_spread_list", summary="查询库房关联实时价差列表")
def get_link_realtime_spread_list(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    warehouse_id: Optional[int] = Query(
        None,
        description="涉及该库房 id 的关联（作为源或对标）；不传则不限定",
    ),
    from_warehouse_id: Optional[int] = Query(
        None,
        ge=1,
        description="仅源库房 id 等于该值的边",
    ),
    to_warehouse_id: Optional[int] = Query(
        None,
        ge=1,
        description="仅对标库房 id 等于该值的边",
    ),
    keyword: Optional[str] = Query(
        None,
        description="源或对标库房名称模糊匹配（可选）",
    ),
    has_realtime_spread: Optional[bool] = Query(
        None,
        description="true=源/对标库房均已配置库房定价并可计算价差；false=至少一方未配置定价；不传=全部",
    ),
    service: TLService = Depends(get_tl_service),
):
    """
    分页返回各关联边上的源库房定价、对标库房定价及实时价差（源定价−对标定价）。
    筛选条件与 get_warehouse_links_list / get_tier_price_spread_list 一致。
    """
    try:
        return service.get_link_realtime_spread_list(
            page=page,
            size=size,
            warehouse_id=warehouse_id,
            from_warehouse_id=from_warehouse_id,
            to_warehouse_id=to_warehouse_id,
            keyword=keyword,
            has_realtime_spread=has_realtime_spread,
        )
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_tier_price_spread_list", summary="查询阶梯差价列表")
def get_tier_price_spread_list(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    warehouse_id: Optional[int] = Query(
        None,
        description="涉及该库房 id 的关联（作为源或对标）；不传则不限定",
    ),
    from_warehouse_id: Optional[int] = Query(
        None,
        ge=1,
        description="仅源库房 id 等于该值的边",
    ),
    to_warehouse_id: Optional[int] = Query(
        None,
        ge=1,
        description="仅对标库房 id 等于该值的边",
    ),
    keyword: Optional[str] = Query(
        None,
        description="源或对标库房名称模糊匹配（可选）",
    ),
    has_tier_price_spread: Optional[bool] = Query(
        None,
        description="true=仅已配置阶梯差价；false=仅未配置；不传=全部",
    ),
    service: TLService = Depends(get_tl_service),
):
    """分页返回各边上的源库房、对标库房、距离千米、阶梯价差；筛选与 get_warehouse_links_list 一致。"""
    try:
        return service.get_tier_price_spread_list(
            page=page,
            size=size,
            warehouse_id=warehouse_id,
            from_warehouse_id=from_warehouse_id,
            to_warehouse_id=to_warehouse_id,
            keyword=keyword,
            has_tier_price_spread=has_tier_price_spread,
        )
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_warehouse_links_outbound", summary="列出库房出边（指向哪些库房）")
def get_warehouse_links_outbound(
    warehouse_id: int = Query(..., ge=1, description="库房 id"),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    service: TLService = Depends(get_tl_service),
):
    """分页返回该库房作为起点的全部有向边及目标库房摘要。"""
    try:
        return service.get_warehouse_links_outbound(warehouse_id, page=page, size=size)
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_warehouse_links_inbound", summary="列出库房入边（被哪些库房指向）")
def get_warehouse_links_inbound(
    warehouse_id: int = Query(..., ge=1, description="库房 id"),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    service: TLService = Depends(get_tl_service),
):
    """分页返回以该库房为终点的全部有向边及源库房摘要。"""
    try:
        return service.get_warehouse_links_inbound(warehouse_id, page=page, size=size)
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/update_warehouse_link_tier", summary="修改库房关联边上的阶梯价差")
def update_warehouse_link_tier(
    body: WarehouseLinkUpdateTierRequest,
    service: TLService = Depends(get_tl_service),
):
    """修改源库房→对标库房边上的阶梯价差 JSON；传 null 清空。"""
    try:
        return service.update_warehouse_link_tier_price_spread(
            body.源库房id,
            body.对标库房id,
            body.阶梯价差,
        )
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/replace_warehouse_links_outbound", summary="替换库房全部出边（覆盖式修改）")
def replace_warehouse_links_outbound(
    body: WarehouseLinksReplaceOutboundRequest,
    service: TLService = Depends(get_tl_service),
):
    """删除该源库房的全部出边后，按列表重建；空列表等价于清空出边。"""
    try:
        return service.replace_warehouse_links_outbound(
            body.源库房id,
            body.目标库房id列表,
        )
    except ValueError as e:
        raise _tl_value_error_http(e)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口1e：新建冶炼厂 =====================

@router.post("/add_smelter", summary="新建冶炼厂")
def add_smelter(
    body: AddSmelterRequest,
    service: TLService = Depends(get_tl_service),
):
    """省市区+详址齐全时落库并无标记色；经度/纬度默认不传，由天地图解析（若同时传经度+纬度则用手写值）。可选传循融宝发货。"""
    try:
        return service.add_smelter(
            name=body.冶炼厂名,
            address=body.地址,
            province=body.省,
            city=body.市,
            district=body.区,
            longitude=body.经度,
            latitude=body.纬度,
            use_xunrongbao=body.循融宝发货,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口2：获取冶炼厂列表 =====================

@router.get("/get_smelter", summary="获取单个冶炼厂详情")
def get_smelter(
    冶炼厂id: int = Query(..., description="冶炼厂 id"),
    service: TLService = Depends(get_tl_service),
):
    """含地址、经纬度、循融宝发货、is_active 等；用于循融宝与其它字段联查。"""
    try:
        return {"code": 200, "data": service.get_smelter(冶炼厂id)}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list_smelter_xunrongbao", summary="列出冶炼厂循融宝发货状态")
def list_smelter_xunrongbao(
    include_inactive: bool = Query(
        False,
        description="为 true 时包含已停用冶炼厂；默认仅启用",
    ),
    service: TLService = Depends(get_tl_service),
):
    """返回系统加价元/吨与各冶炼厂当前开关，便于配置页一次性加载。"""
    try:
        return {"code": 200, "data": service.list_smelter_xunrongbao(include_inactive)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_smelters", summary="获取冶炼厂列表")
def get_smelters(
    keyword: Optional[str] = Query(
        None,
        description="冶炼厂名称模糊搜索（可选），与库房列表 keyword 用法一致",
    ),
    page: Optional[int] = Query(
        None,
        ge=1,
        description="分页页码；传入则 data 为 { list, total, page, size }",
    ),
    size: Optional[int] = Query(
        None, ge=1, le=200, description="分页大小（默认 20，单页最多 200）"
    ),
    province: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    district: Optional[str] = Query(None),
    status: Optional[int] = Query(None, description="1 启用 0 停用；分页时省略则默认仅启用"),
    service: TLService = Depends(get_tl_service),
):
    """列表不含冶炼厂颜色字段；未传 page 为简易列表，传 page 为分页结构。"""
    try:
        data = service.get_smelters(
            keyword=keyword,
            page=page,
            size=size,
            province=province,
            city=city,
            district=district,
            status=status,
        )
        return {"code": 200, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_missing_geo_info", summary="地址经纬度缺失列表")
def get_missing_geo_info(
    service: TLService = Depends(get_tl_service),
):
    """返回启用中且缺少经度或纬度的仓库、冶炼厂，用于前端集中补全坐标。"""
    try:
        return {"code": 200, "data": service.get_missing_geo_info()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口2d：计算两点直线距离 =====================

@router.get("/calculate_distance", summary="计算两组经纬度的直线距离")
def calculate_distance(
    lng1: float = Query(..., ge=-180, le=180, description="起点经度"),
    lat1: float = Query(..., ge=-90, le=90, description="起点纬度"),
    lng2: float = Query(..., ge=-180, le=180, description="终点经度"),
    lat2: float = Query(..., ge=-90, le=90, description="终点纬度"),
):
    """使用 Haversine 公式计算 WGS84 经纬度两点球面直线距离；不是驾车路线距离。"""
    try:
        _validate_coordinate(lng1, lat1, "起点")
        _validate_coordinate(lng2, lat2, "终点")
        distance_km = _haversine_distance_km(lng1, lat1, lng2, lat2)
        return {
            "code": 200,
            "data": {
                "distance_km": round(distance_km, 3),
                "distance_m": round(distance_km * 1000, 2),
            },
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ===================== 接口2b：修改冶炼厂 =====================

@router.post("/update_smelter", summary="修改冶炼厂信息")
def update_smelter(
    body: UpdateSmelterRequest,
    service: TLService = Depends(get_tl_service),
):
    """变更行政区或地址且未同时传经纬度时由服务端调用天地图刷新坐标；不支持颜色字段。"""
    try:
        patch = body.model_dump(exclude_unset=True)
        smelter_id = patch.pop("冶炼厂id")
        return service.update_smelter(smelter_id=smelter_id, patch=patch)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/set_smelter_xunrongbao", summary="设置单个冶炼厂循融宝发货")
def set_smelter_xunrongbao(
    body: SmelterXunrongbaoItem,
    service: TLService = Depends(get_tl_service),
):
    """增/改循融宝开关：传 true 为启用循融宝发货，false 为关闭。"""
    try:
        return service.set_smelter_xunrongbao(
            smelter_id=body.冶炼厂id,
            enabled=body.循融宝发货,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/smelter_xunrongbao/{smelter_id}", summary="关闭冶炼厂循融宝发货")
def clear_smelter_xunrongbao(
    smelter_id: int,
    service: TLService = Depends(get_tl_service),
):
    """仅将循融宝发货置为否，不删除冶炼厂、不做软删。"""
    try:
        return service.clear_smelter_xunrongbao(smelter_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch_set_smelters_xunrongbao", summary="批量设置冶炼厂循融宝发货")
def batch_set_smelters_xunrongbao(
    body: BatchSetSmeltersXunrongbaoRequest,
    service: TLService = Depends(get_tl_service),
):
    """同一请求可提交多条，分别指定各冶炼厂是否循融宝发货。"""
    try:
        items = [x.model_dump() for x in body.列表]
        return service.batch_set_smelters_xunrongbao(items)
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


@router.delete("/purge_smelter", summary="永久删除冶炼厂（硬删除）")
def purge_smelter(
    smelter_id: int,
    cascade: bool = Query(
        True,
        description=(
            "默认 true：同一事务内级联删除该厂的需求/报价/运费等后再删冶炼厂。"
            "传 false 时仅当无任何子表引用才删厂，否则 409（严格校验）。"
        ),
    ),
    service: TLService = Depends(get_tl_service),
):
    """物理删除冶炼厂；默认级联清除本厂关联业务数据。"""
    try:
        return service.purge_smelter(smelter_id=smelter_id, cascade=cascade)
    except ValueError as e:
        detail = str(e)
        if "不存在" in detail or detail == "冶炼厂 id 无效":
            raise HTTPException(status_code=404, detail=detail)
        raise HTTPException(status_code=409, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch_delete_warehouses", summary="批量停用仓库（软删除）")
def batch_delete_warehouses(
    body: BatchWarehouseIdsRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.batch_delete_warehouses(body.仓库id列表)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch_delete_smelters", summary="批量停用冶炼厂（软删除）")
def batch_delete_smelters(
    body: BatchSmelterIdsRequest,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.batch_delete_smelters(body.冶炼厂id列表)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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
    body: Any = Body(...),
    service: TLService = Depends(get_tl_service),
):
    """批量提交品种名：新建品类分组、已存在则跳过、停用则恢复启用。
    请求体可为 **单对象** `{ \"品种名\": \"…\" }` 或 **数组** `[{ \"品种名\": \"…\" }, …]`。
    """
    try:
        if isinstance(body, list):
            parsed = [UploadVarietyRequest.model_validate(x) for x in body]
        elif isinstance(body, dict):
            parsed = [UploadVarietyRequest.model_validate(body)]
        else:
            raise HTTPException(
                status_code=400,
                detail='请求体须为 JSON 对象或数组，例如 {"品种名":"电动车电池"} 或 [{"品种名":"..."}]',
            )
        items = [item.model_dump() for item in parsed]
        return service.upload_variety(items)
    except HTTPException:
        raise
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
    """明细含单价/总价/运费及报价等；利润=总价−运费。循融宝厂另含不含/含循融宝分支与加价元/吨。"""
    try:
        tons_by = None
        if body.品类吨数列表:
            tons_by = {int(x.品类id): float(x.吨数) for x in body.品类吨数列表}
        out = service.get_comparison(
            warehouse_ids=body.选中仓库id列表,
            smelter_ids=body.冶炼厂id列表,
            category_ids=body.品类id列表,
            price_type=body.price_type,
            tons=body.吨数,
            tons_by_category=tons_by,
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


@router.get("/get_comparison_options", summary="智能比价取价/最优价口径选项")
def get_comparison_options():
    """供前端下拉使用：取价口径与最优价计税口径（与 get_comparison 中 price_type / 最优价计税口径列表 对应）。"""
    return {
        "code": 200,
        "data": {
            "price_type": [
                {"value": None, "label": "普通价（不含税）"},
                {"value": "1pct", "label": "1%增值税"},
                {"value": "3pct", "label": "3%增值税"},
                {"value": "13pct", "label": "13%增值税"},
                {"value": "normal_invoice", "label": "普通发票价格"},
                {"value": "reverse_invoice", "label": "反向发票价格"},
            ],
            "optimal_basis": [
                {"value": "base", "label": "不含税基准价"},
                {"value": "1pct", "label": "1%增值税价"},
                {"value": "3pct", "label": "3%增值税价"},
                {"value": "13pct", "label": "13%增值税价"},
                {"value": "normal_invoice", "label": "普通发票价"},
                {"value": "reverse_invoice", "label": "反向发票价"},
            ],
        },
    }


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


@router.get("/download_quote_list_template_excel", summary="下载报价列表导入模板（xlsx）")
def download_quote_list_template_excel(
    service: TLService = Depends(get_tl_service),
):
    """表头与「报价数据导出」及 upload_price_table_excel 解析规则对齐；含「填写说明」工作表。"""
    try:
        data = service.build_quote_list_import_template_excel()
        fn = "报价列表导入模板.xlsx"
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(fn)}",
            },
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload_price_table_excel", summary="上传报价列表（Excel xlsx）")
def upload_price_table_excel(
    file: List[UploadFile] = File(
        ...,
        description="报价列表 .xlsx，可多文件；读取首工作表「导入数据」或同结构表。可由 GET /tl/download_quote_list_template_excel 下载模板填写",
    ),
    service: TLService = Depends(get_tl_service),
):
    """
    解析为与 OCR 上传类似的 ``data.details``：每项 ``success``、``items``、``full_data``；
    成功项用 ``file`` 表示文件名；若「日期」列全日相同则附带 ``suggested_quote_date``。
    前端确认时仍调用 ``POST /tl/confirm_price_table``，传入 ``报价日期`` 与 ``items``（及可选 ``full_data``）。
    """
    for f in file:
        fn = (f.filename or "").lower()
        if not fn.endswith(".xlsx"):
            raise HTTPException(
                status_code=400,
                detail=f"文件 '{f.filename}' 格式不支持，仅允许 .xlsx",
            )
    try:
        return service.upload_price_table_excel(file)
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
            replace_factory_quotes_on_date=body.同冶炼厂当日整表覆盖,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/manual_quote", summary="手写录入报价（无 OCR）")
def manual_quote(
    body: ManualQuoteRequest,
    service: TLService = Depends(get_tl_service),
):
    """与 confirm_price_table 相同落库逻辑；可不传 full_data，用于表格/手工维护。"""
    try:
        items = [item.model_dump() for item in body.数据]
        full_data = body.full_data.model_dump() if body.full_data else None
        return service.manual_quote_entry(
            quote_date_str=body.报价日期,
            items=items,
            full_data=full_data,
            replace_factory_quotes_on_date=body.同冶炼厂当日整表覆盖,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update_quote_detail", summary="按 id 修改单条报价明细")
def update_quote_detail(
    body: UpdateQuoteDetailRequest,
    service: TLService = Depends(get_tl_service),
):
    """修改后按冶炼厂税率重算 1%/3%/13% 含税列与不含税基准（锚点为本次请求中实际提交的价格列）。"""
    try:
        return service.update_quote_detail(body)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/quote_detail/{detail_id}",
    summary="按 id 删除单条报价明细",
)
def delete_quote_detail(
    detail_id: int,
    service: TLService = Depends(get_tl_service),
):
    """删除 quote_details 一行；用于报价数据查询中清理错误/重复数据。"""
    try:
        return service.delete_quote_detail(detail_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===================== 接口5c：报价数据列表 =====================

@router.get("/get_quote_details_list", summary="报价数据列表")
def get_quote_details_list(
    factory_id: Optional[int] = None,
    category_id: Optional[int] = None,
    品类id: Optional[int] = Query(None, description="品类分组 id；兼容旧前端中文参数"),
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
            category_id=category_id or 品类id,
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
    category_id: Optional[int] = None,
    品类id: Optional[int] = Query(None, description="品类分组 id；兼容旧前端中文参数"),
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
            category_id=category_id or 品类id,
            quote_date=quote_date,
            date_from=eff_from,
            date_to=eff_to,
            category_name=eff_cat,
            category_exact=category_exact,
        )
        return _quote_details_excel_response(data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export_quote_details_excel", summary="导出报价数据 Excel（POST，筛选与列表一致）")
def export_quote_details_excel_post(
    body: QuoteDetailsFilterRequest,
    service: TLService = Depends(get_tl_service),
):
    """与 GET 导出相同，请求体携带筛选条件，避免查询串过长或编码不一致导致导出为空。"""
    eff_from, eff_to, eff_cat = _merge_quote_list_filters(
        body.date_from,
        body.date_to,
        body.start_date,
        body.end_date,
        body.category_name,
        body.variety,
    )
    try:
        data = service.export_quote_details_excel(
            factory_id=body.factory_id,
            category_id=body.category_id,
            quote_date=body.quote_date,
            date_from=eff_from,
            date_to=eff_to,
            category_name=eff_cat,
            category_exact=body.category_exact,
        )
        return _quote_details_excel_response(data)
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
    """按表头识别「库房/仓库」列与各冶炼厂列（支持表头不在第 1 行、库房列不在第 A 列），解析矩阵单元格数值写入 freight_rates（当日生效）；字典中不存在的库房/冶炼厂名称会自动新建（已停用则恢复启用）。结果可在 get_freight_list 中查询。"""
    try:
        raw = await file.read()
        return await asyncio.to_thread(service.import_freight_excel, raw)
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


# ===================== 对标定价 / 标定价格 / 库房差额 / AI 分析快照 =====================


@router.get("/province_benchmark_prices", summary="省份对标城市定价列表（含历史）")
def province_benchmark_prices(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    province: Optional[str] = Query(None, description="省份精确匹配（TRIM）"),
    province_keyword: Optional[str] = Query(None, description="省份模糊（LIKE）"),
    benchmark_city: Optional[str] = Query(None, description="对标城市精确匹配（TRIM）"),
    benchmark_city_keyword: Optional[str] = Query(None, description="对标城市模糊（LIKE）"),
    keyword: Optional[str] = Query(
        None,
        description="省份或对标城市模糊（OR，LIKE）",
    ),
    row_id: Optional[int] = Query(
        None,
        ge=1,
        description="主键 id 精确筛选",
        alias="id",
    ),
    date_from: Optional[str] = Query(None, description="定价日起 YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="定价日止 YYYY-MM-DD"),
    created_from: Optional[str] = Query(None, description="上传日期起 YYYY-MM-DD（按 DATE(created_at)）"),
    created_to: Optional[str] = Query(None, description="上传日期止 YYYY-MM-DD"),
    benchmark_price_min: Optional[float] = Query(None, description="对标城市定价下限（含）"),
    benchmark_price_max: Optional[float] = Query(None, description="对标城市定价上限（含）"),
    only_latest: bool = Query(
        False,
        description="为 true 时每省仅保留当前有效的一条（price_date 最大，同日 id 最大）",
    ),
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.list_province_benchmark_prices(
            page=page,
            page_size=page_size,
            province=province,
            province_keyword=province_keyword,
            benchmark_city=benchmark_city,
            benchmark_city_keyword=benchmark_city_keyword,
            keyword=keyword,
            price_id=row_id,
            date_from=date_from,
            date_to=date_to,
            created_from=created_from,
            created_to=created_to,
            benchmark_price_min=benchmark_price_min,
            benchmark_price_max=benchmark_price_max,
            only_latest=only_latest,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/province_benchmark_prices", summary="新增省份对标城市定价")
def province_benchmark_prices_create(
    body: ProvinceBenchmarkPriceCreate,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.create_province_benchmark_price(
            province=body.省份,
            benchmark_city=body.对标城市,
            benchmark_price=body.对标城市定价,
            price_date=body.定价日期,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put(
    "/province_benchmark_prices/{price_id}",
    summary="修订省份对标定价历史行（保留源记录，新增一条历史）",
)
def province_benchmark_prices_update(
    price_id: int,
    body: ProvinceBenchmarkPriceUpdate,
    service: TLService = Depends(get_tl_service),
):
    """按 `price_id` 取源行，与请求体合并后**插入**新历史行；源行不修改。同日多条时 id 最大者为当前有效。"""
    try:
        patch = body.model_dump(exclude_unset=True)
        return service.update_province_benchmark_price(
            price_id,
            province=patch.get("省份"),
            benchmark_city=patch.get("对标城市"),
            benchmark_price=patch.get("对标城市定价"),
            price_date=patch.get("定价日期"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/province_benchmark_prices/{price_id}", summary="删除省份对标定价历史行")
def province_benchmark_prices_delete(
    price_id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_province_benchmark_price(price_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/smelter_calibration_prices", summary="冶炼厂标定价格列表")
def smelter_calibration_prices(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    factory_id: Optional[int] = Query(None, description="冶炼厂 id"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.list_smelter_calibration_prices(
            page=page,
            page_size=page_size,
            factory_id=factory_id,
            date_from=date_from,
            date_to=date_to,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/smelter_calibration_prices", summary="新增冶炼厂标定价格")
def smelter_calibration_prices_create(
    body: SmelterCalibrationPriceCreate,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.create_smelter_calibration_price(
            factory_id=body.冶炼厂id,
            calibration_price=body.标定价格,
            price_date=body.定价日期,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put(
    "/smelter_calibration_prices/{price_id}",
    summary="修订冶炼厂标定价格历史行（保留源记录，新增一条历史）",
)
def smelter_calibration_prices_update(
    price_id: int,
    body: SmelterCalibrationPriceUpdate,
    service: TLService = Depends(get_tl_service),
):
    """按 `price_id` 取源行，与请求体合并后**插入**新历史行；源行不修改。同日多条时 id 最大者为当前有效。"""
    try:
        patch = body.model_dump(exclude_unset=True)
        return service.update_smelter_calibration_price(
            price_id,
            factory_id=patch.get("冶炼厂id"),
            calibration_price=patch.get("标定价格"),
            price_date=patch.get("定价日期"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/smelter_calibration_prices/{price_id}", summary="删除冶炼厂标定价格历史行")
def smelter_calibration_prices_delete(
    price_id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_smelter_calibration_price(price_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/warehouse_spread_configs", summary="库房对标差额与毛利配置列表")
def warehouse_spread_configs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    config_row_id: Optional[int] = Query(
        None,
        ge=1,
        description="配置主键 wsc.id",
        alias="id",
    ),
    warehouse_id: Optional[int] = Query(None, ge=1, description="库房 id 精确"),
    warehouse_ids: Optional[List[int]] = Query(
        None,
        description="库房 id 列表（可重复 query 参数，如 warehouse_ids=1&warehouse_ids=2）",
    ),
    province: Optional[str] = Query(None, description="库房所在省精确（TRIM）"),
    province_keyword: Optional[str] = Query(None, description="省模糊（LIKE）"),
    city: Optional[str] = Query(None, description="库房所在市精确（TRIM）"),
    city_keyword: Optional[str] = Query(None, description="市模糊（LIKE）"),
    district: Optional[str] = Query(None, description="库房所在区县精确（TRIM）"),
    district_keyword: Optional[str] = Query(None, description="区县模糊（LIKE）"),
    warehouse_name: Optional[str] = Query(None, description="库房名称精确（TRIM）"),
    warehouse_name_keyword: Optional[str] = Query(None, description="库房名称模糊（LIKE）"),
    warehouse_type_id: Optional[int] = Query(None, ge=1, description="库房类型 id"),
    is_active: Optional[int] = Query(
        None,
        ge=0,
        le=1,
        description="库房启用：1 启用 0 停用；不传不限定",
    ),
    benchmark_city: Optional[str] = Query(None, description="配置对标城市精确（TRIM）"),
    benchmark_city_keyword: Optional[str] = Query(None, description="对标城市模糊（LIKE）"),
    keyword: Optional[str] = Query(
        None,
        description="库房名/省/市/区/对标城市 合一模糊（OR，LIKE）",
    ),
    city_spread_min: Optional[float] = Query(None, description="对标城市差额下限（含）"),
    city_spread_max: Optional[float] = Query(None, description="对标城市差额上限（含）"),
    gross_margin_min: Optional[float] = Query(None, description="毛利（配置版）下限（含）"),
    gross_margin_max: Optional[float] = Query(None, description="毛利（配置版）上限（含）"),
    has_gross_margin: Optional[bool] = Query(
        None,
        description="true=仅已填毛利配置；false=仅未填；不传不限定",
    ),
    created_from: Optional[str] = Query(None, description="配置创建日起 YYYY-MM-DD"),
    created_to: Optional[str] = Query(None, description="配置创建日止 YYYY-MM-DD"),
    updated_from: Optional[str] = Query(None, description="配置更新日起 YYYY-MM-DD"),
    updated_to: Optional[str] = Query(None, description="配置更新日止 YYYY-MM-DD"),
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.list_warehouse_spread_configs(
            page=page,
            page_size=page_size,
            config_id=config_row_id,
            warehouse_id=warehouse_id,
            warehouse_ids=warehouse_ids,
            province=province,
            province_keyword=province_keyword,
            city=city,
            city_keyword=city_keyword,
            district=district,
            district_keyword=district_keyword,
            warehouse_name=warehouse_name,
            warehouse_name_keyword=warehouse_name_keyword,
            warehouse_type_id=warehouse_type_id,
            is_active=is_active,
            benchmark_city=benchmark_city,
            benchmark_city_keyword=benchmark_city_keyword,
            keyword=keyword,
            city_spread_min=city_spread_min,
            city_spread_max=city_spread_max,
            gross_margin_min=gross_margin_min,
            gross_margin_max=gross_margin_max,
            has_gross_margin=has_gross_margin,
            created_from=created_from,
            created_to=created_to,
            updated_from=updated_from,
            updated_to=updated_to,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/warehouse_spread_configs", summary="新增库房对标差额配置")
def warehouse_spread_configs_create(
    body: WarehouseSpreadConfigCreate,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.create_warehouse_spread_config(
            warehouse_id=body.库房id,
            benchmark_city=body.对标城市,
            city_spread=body.对标城市差额,
            gross_margin_config=body.毛利配置版,
            warehouse_price=body.库房定价,
        )
    except ValueError as e:
        if "已有配置" in str(e):
            raise HTTPException(status_code=409, detail=str(e)) from e
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/warehouse_spread_configs/{config_id}", summary="修改库房对标差额配置")
def warehouse_spread_configs_update(
    config_id: int,
    body: WarehouseSpreadConfigUpdate,
    service: TLService = Depends(get_tl_service),
):
    try:
        patch = body.model_dump(exclude_unset=True)
        return service.update_warehouse_spread_config(
            config_id,
            benchmark_city=patch.get("对标城市"),
            city_spread=patch.get("对标城市差额"),
            gross_margin_config=patch.get("毛利配置版"),
            warehouse_price=patch.get("库房定价"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/warehouse_spread_configs/{config_id}", summary="删除库房对标差额配置")
def warehouse_spread_configs_delete(
    config_id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_warehouse_spread_config(config_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/import_warehouse_spread_excel", summary="导入库房对标差额与毛利（Excel xlsx）")
async def import_warehouse_spread_excel(
    file: UploadFile = File(..., description="库房分析类 xlsx；读取工作簿内全部工作表"),
    overwrite: bool = Form(
        True,
        description="true=已存在配置则按 Excel 非空字段更新；false=跳过已有配置的库房",
    ),
    service: TLService = Depends(get_tl_service),
):
    """
    解析「库房分析」类 Excel（含多个工作表），写入 ``pd_warehouse_spread_configs``。

    列识别：库房名称、毛利/保底毛利、定价/库房报价、对比/对标差额列；差额缺省时可由定价减省份对标价推算。
    库房按名称匹配 ``dict_warehouses``（不自动新建）。
    """
    fn = (file.filename or "").lower()
    if not fn.endswith((".xlsx", ".xlsm")):
        raise HTTPException(status_code=400, detail="请上传 .xlsx 或 .xlsm 文件")
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="文件内容为空")
    try:
        return await asyncio.to_thread(
            service.import_warehouse_spread_excel,
            raw,
            overwrite=overwrite,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ai_pricing_analysis", summary="库房 AI 定价对标分析（实时计算，不落库）")
def ai_pricing_analysis(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    as_of_date: Optional[str] = Query(
        None,
        description="口径日期 YYYY-MM-DD；解析对标价、标定价、运费时取该日及以前最新一条",
    ),
    warehouse_id: Optional[int] = Query(None, ge=1, description="库房 id 精确"),
    warehouse_ids: Optional[List[int]] = Query(
        None,
        description="库房 id 列表（可重复 query 参数，如 warehouse_ids=1&warehouse_ids=2）",
    ),
    province: Optional[str] = Query(None, description="库房所在省精确（TRIM）"),
    province_keyword: Optional[str] = Query(None, description="省模糊（LIKE）"),
    city: Optional[str] = Query(None, description="库房所在市精确（TRIM）"),
    city_keyword: Optional[str] = Query(None, description="市模糊（LIKE）"),
    district: Optional[str] = Query(None, description="库房所在区县精确（TRIM）"),
    district_keyword: Optional[str] = Query(None, description="区县模糊（LIKE）"),
    warehouse_name: Optional[str] = Query(None, description="库房名称精确（TRIM）"),
    warehouse_name_keyword: Optional[str] = Query(None, description="库房名称模糊（LIKE）"),
    warehouse_type_id: Optional[int] = Query(None, ge=1, description="库房类型 id"),
    is_active: Optional[int] = Query(
        None,
        ge=0,
        le=1,
        description="库房启用：1 启用 0 停用；不传则默认仅启用库房",
    ),
    benchmark_city: Optional[str] = Query(None, description="配置对标城市精确（TRIM）"),
    benchmark_city_keyword: Optional[str] = Query(None, description="对标城市模糊（LIKE）"),
    keyword: Optional[str] = Query(
        None,
        description="库房名/省/市/区/对标城市 合一模糊（OR，LIKE）",
    ),
    city_spread_min: Optional[float] = Query(None, description="对标城市差额下限（含）"),
    city_spread_max: Optional[float] = Query(None, description="对标城市差额上限（含）"),
    gross_margin_min: Optional[float] = Query(None, description="毛利（配置版）下限（含）"),
    gross_margin_max: Optional[float] = Query(None, description="毛利（配置版）上限（含）"),
    has_gross_margin: Optional[bool] = Query(
        None,
        description="true=仅已填毛利配置；false=仅未填；不传不限定",
    ),
    has_spread_config: Optional[bool] = Query(
        None,
        description="true=仅已有差额配置；false=仅未配置；不传不限定",
    ),
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.get_ai_pricing_analysis(
            page=page,
            page_size=page_size,
            warehouse_id=warehouse_id,
            warehouse_ids=warehouse_ids,
            province=province,
            province_keyword=province_keyword,
            city=city,
            city_keyword=city_keyword,
            district=district,
            district_keyword=district_keyword,
            warehouse_name=warehouse_name,
            warehouse_name_keyword=warehouse_name_keyword,
            warehouse_type_id=warehouse_type_id,
            is_active=is_active,
            benchmark_city=benchmark_city,
            benchmark_city_keyword=benchmark_city_keyword,
            keyword=keyword,
            city_spread_min=city_spread_min,
            city_spread_max=city_spread_max,
            gross_margin_min=gross_margin_min,
            gross_margin_max=gross_margin_max,
            has_gross_margin=has_gross_margin,
            has_spread_config=has_spread_config,
            as_of_date=as_of_date,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ai_pricing_snapshots", summary="生成 AI 定价对标分析快照")
def ai_pricing_snapshots_create(
    body: AiPricingSnapshotCreate,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.create_ai_pricing_snapshot(
            title=body.标题,
            as_of_date=body.口径日期,
            warehouse_ids=body.库房id列表,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ai_pricing_snapshots", summary="AI 定价对标分析快照列表")
def ai_pricing_snapshots_list(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.list_ai_pricing_snapshots(page=page, page_size=page_size)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ai_pricing_snapshots/{snapshot_id}", summary="快照详情（含明细）")
def ai_pricing_snapshots_detail(
    snapshot_id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.get_ai_pricing_snapshot_detail(snapshot_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/ai_pricing_snapshots/{snapshot_id}", summary="更新快照元数据")
def ai_pricing_snapshots_update_meta(
    snapshot_id: int,
    body: AiPricingSnapshotUpdate,
    service: TLService = Depends(get_tl_service),
):
    try:
        patch = body.model_dump(exclude_unset=True)
        return service.update_ai_pricing_snapshot(
            snapshot_id,
            title=patch.get("标题"),
            as_of_date=patch.get("口径日期"),
            _set_title=("标题" in patch),
            _set_as_of_date=("口径日期" in patch),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/ai_pricing_snapshots/{snapshot_id}", summary="删除快照（级联删明细）")
def ai_pricing_snapshots_delete(
    snapshot_id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_ai_pricing_snapshot(snapshot_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put(
    "/ai_pricing_snapshots/{snapshot_id}/items/{item_id}",
    summary="修改快照明细备注",
)
def ai_pricing_snapshots_item_update_remark(
    snapshot_id: int,
    item_id: int,
    body: AiPricingSnapshotItemRemarkBody,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.update_ai_pricing_snapshot_item_remark(
            snapshot_id, item_id, body.备注
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/ai_pricing_snapshots/{snapshot_id}/items/{item_id}",
    summary="删除快照明细行",
)
def ai_pricing_snapshots_item_delete(
    snapshot_id: int,
    item_id: int,
    service: TLService = Depends(get_tl_service),
):
    try:
        return service.delete_ai_pricing_snapshot_item(snapshot_id, item_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
