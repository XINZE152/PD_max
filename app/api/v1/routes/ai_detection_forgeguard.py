# -*- coding: utf-8 -*-
"""ForgeGuard 外部检测引擎接口（新增，不影响原有规则检测）。"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from app.ai_detection.services.forgeguard_client import (
    FORGEGUARD_API_KEY,
    FORGEGUARD_BASE_URL,
    forgeguard_detect,
    forgeguard_health,
    forgeguard_verify,
    _normalize_detect_result,
    _normalize_verify_result,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ai-detection",
    tags=["ForgeGuard 检测"],
)


def _parse_bbox(raw: Optional[str]) -> List[int]:
    """解析 "[x1,y1,x2,y2]" 或 "x1,y1,x2,y2" 格式。"""
    if not raw:
        raise HTTPException(status_code=400, detail="bbox 参数不能为空")
    s = raw.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    parts = [int(float(x.strip())) for x in s.split(",") if x.strip()]
    if len(parts) != 4:
        raise HTTPException(status_code=400, detail="bbox 格式无效，需要 [x1,y1,x2,y2]")
    return parts


def _parse_bboxes(raw: Optional[str]) -> List[List[int]]:
    """解析 "[[x1,y1,x2,y2], ...]" 格式。"""
    if not raw:
        raise HTTPException(status_code=400, detail="bboxes 参数不能为空")
    try:
        parsed = json.loads(raw.strip())
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="bboxes 格式无效，请使用 JSON 数组")
    if not isinstance(parsed, list) or not all(isinstance(x, list) and len(x) == 4 for x in parsed):
        raise HTTPException(status_code=400, detail="bboxes 格式无效，请使用 [[x1,y1,x2,y2], ...]")
    return [[int(v) for v in x] for x in parsed]


# ---------------------------------------------------------------------------
# 健康检查
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/forgeguard/health",
    summary="ForgeGuard 服务健康检查",
)
async def forgeguard_health_check():
    """代理 ForgeGuard 的 /health 端点，检查外部引擎是否可用。"""
    try:
        data = await run_in_threadpool(forgeguard_health)
        return {"status": "success", "forgeguard": data}
    except requests.ConnectionError:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": f"无法连接 ForgeGuard 服务 ({FORGEGUARD_BASE_URL})"},
        )
    except requests.Timeout:
        return JSONResponse(
            status_code=504,
            content={"status": "error", "message": "ForgeGuard 服务响应超时"},
        )
    except Exception as exc:
        logger.exception("forgeguard health check failed")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(exc)},
        )


# ---------------------------------------------------------------------------
# GET /api/v1/forgeguard/status — 诊断 / 可用性检查
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/forgeguard/status",
    summary="ForgeGuard 诊断检查",
    description=(
        "综合检查 ForgeGuard 连通性、模型状态、替换模式是否开启。适合用于排查集成问题。"
    ),
)
async def forgeguard_status_check():
    import os as _os

    result: Dict[str, Any] = {
        "forgeguard_base_url": FORGEGUARD_BASE_URL,
        "forgeguard_api_key_configured": bool(FORGEGUARD_API_KEY),
        "replacement_mode": _os.getenv("FORGEGUARD_REPLACE_RULE_CHECKS", "").strip().lower() in ("1", "true", "yes"),
        "connectivity": "unknown",
        "model_loaded": False,
        "build": None,
    }
    try:
        data = await run_in_threadpool(forgeguard_health)
        result["connectivity"] = "ok"
        result["model_loaded"] = bool(data.get("dl_model_loaded"))
        result["build"] = data.get("build")
        result["auth_enabled"] = bool(data.get("auth_enabled"))
        result["rate_limit_rpm"] = data.get("rate_limit_rpm")
        result["max_concurrent"] = data.get("max_concurrent")
    except requests.ConnectionError:
        result["connectivity"] = "unreachable"
    except requests.Timeout:
        result["connectivity"] = "timeout"
    except Exception as exc:
        result["connectivity"] = "error"
        result["error"] = str(exc)

    result["available"] = result["connectivity"] == "ok" and result["model_loaded"]
    return {"status": "success", "diagnosis": result}


# ---------------------------------------------------------------------------
# POST /api/v1/forgeguard/detect — 整图篡改检测
# ---------------------------------------------------------------------------


@router.post(
    "/api/v1/forgeguard/detect",
    summary="ForgeGuard 整图篡改检测",
    description=(
        "上传整张图片，由 ForgeGuard 三引擎（CV + DL + DF）并行分析，"
        "返回篡改/正常/可疑结论、置信度、可疑区域。\n\n"
        "**请求方式**：`multipart/form-data`\n\n"
        "- **file**：图片文件（必填），JPG/PNG/WEBP，最大 20MB\n"
        "- **technique**：引擎选择 `auto`（默认）/ `cv` / `dl` / `df`\n\n"
        "**注意**：本接口直接调用内网 ForgeGuard 服务，不经过本系统原有 AI 鉴伪引擎。"
    ),
    response_class=JSONResponse,
)
async def forgeguard_detect_endpoint(
    file: UploadFile = File(..., description="待检测图片文件"),
    technique: str = Form("auto", description="引擎选择：auto / cv / dl / df"),
):
    if technique not in ("auto", "cv", "dl", "df"):
        raise HTTPException(status_code=400, detail="technique 须为 auto / cv / dl / df")
    try:
        image_bytes = await file.read()
        if not image_bytes:
            raise HTTPException(status_code=400, detail="上传文件为空")
    except Exception:
        raise HTTPException(status_code=400, detail="无法读取上传文件")
    filename = file.filename or "image.jpg"

    try:
        raw = await run_in_threadpool(
            forgeguard_detect, image_bytes, filename=filename, technique=technique
        )
        data = _normalize_detect_result(raw)
        return {"status": "success", "data": data}
    except requests.ConnectionError:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": f"无法连接 ForgeGuard 服务 ({FORGEGUARD_BASE_URL})"},
        )
    except requests.Timeout:
        return JSONResponse(
            status_code=504,
            content={"status": "error", "message": "ForgeGuard 服务响应超时"},
        )
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 500
        detail = str(exc)
        try:
            detail = str(exc.response.json() if exc.response is not None else str(exc))
        except Exception:
            pass
        return JSONResponse(status_code=status_code, content={"status": "error", "message": detail})
    except Exception as exc:
        logger.exception("forgeguard detect failed")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(exc)})


# ---------------------------------------------------------------------------
# POST /api/v1/forgeguard/verify — 区域验证 + 重叠分析
# ---------------------------------------------------------------------------


@router.post(
    "/api/v1/forgeguard/verify",
    summary="ForgeGuard 区域验证 + 重叠分析",
    description=(
        "裁剪图片指定 ROI 进行检测，同时分析 OCR 候选框之间的重叠情况。\n\n"
        "**请求方式**：`multipart/form-data`\n\n"
        "- **file**：图片文件（必填），JPG/PNG，最大 20MB\n"
        "- **bbox**：检测 ROI `[x1,y1,x2,y2]`，xyxy 像素坐标（与 bboxes 二选一）\n"
        "- **bboxes**：OCR 候选框列表 `[[x1,y1,x2,y2], ...]`（与 bbox 二选一，用于 IoU 重叠分析）\n\n"
        "**注意**：本接口直接调用内网 ForgeGuard 服务，不经过本系统原有规则检测引擎。"
    ),
    response_class=JSONResponse,
)
async def forgeguard_verify_endpoint(
    file: UploadFile = File(..., description="待检测图片文件"),
    bbox: Optional[str] = Form(
        None,
        description="检测 ROI：[x1,y1,x2,y2]（与 bboxes 二选一）",
        examples=["[120,80,400,340]"],
    ),
    bboxes: Optional[str] = Form(
        None,
        description="OCR 候选框：[[x1,y1,x2,y2], ...]（与 bbox 二选一）",
        examples=["[[10,20,100,80],[95,25,200,90]]"],
    ),
):
    roi_bbox: Optional[List[int]] = None
    detection_bboxes: Optional[List[List[int]]] = None

    if bboxes:
        detection_bboxes = _parse_bboxes(bboxes)
    if bbox:
        roi_bbox = _parse_bbox(bbox)
    if roi_bbox is None and detection_bboxes is None:
        raise HTTPException(status_code=400, detail="请提供 bbox 或 bboxes 参数")
    if roi_bbox is None and detection_bboxes:
        # 无单独 ROI 时用第一个候选框作为 ROI
        roi_bbox = detection_bboxes[0]

    try:
        image_bytes = await file.read()
        if not image_bytes:
            raise HTTPException(status_code=400, detail="上传文件为空")
    except Exception:
        raise HTTPException(status_code=400, detail="无法读取上传文件")
    filename = file.filename or "image.jpg"

    try:
        raw = await run_in_threadpool(
            forgeguard_verify,
            image_bytes,
            roi_bbox=roi_bbox,  # type: ignore[arg-type]
            detection_bboxes=detection_bboxes,
            filename=filename,
        )
        data = _normalize_verify_result(raw)
        return {"status": "success", "data": data}
    except requests.ConnectionError:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": f"无法连接 ForgeGuard 服务 ({FORGEGUARD_BASE_URL})"},
        )
    except requests.Timeout:
        return JSONResponse(
            status_code=504,
            content={"status": "error", "message": "ForgeGuard 服务响应超时"},
        )
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 500
        detail = str(exc)
        try:
            detail = str(exc.response.json() if exc.response is not None else str(exc))
        except Exception:
            pass
        return JSONResponse(status_code=status_code, content={"status": "error", "message": detail})
    except Exception as exc:
        logger.exception("forgeguard verify failed")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(exc)})
