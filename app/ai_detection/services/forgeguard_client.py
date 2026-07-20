# -*- coding: utf-8 -*-
"""ForgeGuard 外部检测引擎 HTTP 客户端（内网 http://127.0.0.1:8030）。"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

FORGEGUARD_BASE_URL = os.getenv("FORGEGUARD_BASE_URL", "http://127.0.0.1:8030").rstrip("/")
FORGEGUARD_API_KEY = os.getenv("FORGEGUARD_API_KEY")
FORGEGUARD_TIMEOUT = int(os.getenv("FORGEGUARD_TIMEOUT", "300"))

_PREDICTION_MAP = {
    "forged": "篡改",
    "uncertain": "可疑",
    "authentic": "正常",
}


def _headers() -> Dict[str, str]:
    h: Dict[str, str] = {}
    if FORGEGUARD_API_KEY:
        h["X-API-Key"] = FORGEGUARD_API_KEY
    return h


def forgeguard_health() -> Dict[str, Any]:
    resp = requests.get(
        f"{FORGEGUARD_BASE_URL}/health",
        headers=_headers(),
        timeout=FORGEGUARD_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def forgeguard_detect(
    image_bytes: bytes,
    filename: str = "image.jpg",
    technique: str = "auto",
) -> Dict[str, Any]:
    """整图篡改检测，返回 ForgeGuard 原始响应。"""
    resp = requests.post(
        f"{FORGEGUARD_BASE_URL}/detect",
        files={"file": (filename, image_bytes, "image/jpeg")},
        data={"technique": technique},
        headers=_headers(),
        timeout=FORGEGUARD_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def forgeguard_verify(
    image_bytes: bytes,
    *,
    roi_bbox: List[int],
    detection_bboxes: Optional[List[List[int]]] = None,
    filename: str = "image.jpg",
) -> Dict[str, Any]:
    """区域验证 + 重叠分析，返回 ForgeGuard 原始响应。"""
    data: Dict[str, str] = {
        "roi_bbox": str(roi_bbox),
    }
    if detection_bboxes:
        data["detection_bboxes"] = str(detection_bboxes)
    resp = requests.post(
        f"{FORGEGUARD_BASE_URL}/verify",
        files={"image": (filename, image_bytes, "image/jpeg")},
        data=data,
        headers=_headers(),
        timeout=FORGEGUARD_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _normalize_detect_result(raw: Dict[str, Any]) -> Dict[str, Any]:
    """将 ForgeGuard /detect 响应转换为本系统统一结构。"""
    prediction = raw.get("prediction", "authentic")
    return {
        "prediction": prediction,
        "prediction_label": _PREDICTION_MAP.get(prediction, "正常"),
        "confidence": raw.get("confidence"),
        "technique": raw.get("technique"),
        "filename": raw.get("filename"),
        "votes_forged": raw.get("votes_forged"),
        "detectors": raw.get("detectors"),
        "forgery_regions": raw.get("forgery_regions") or [],
        "model": raw.get("model"),
    }


def _normalize_verify_result(raw: Dict[str, Any]) -> Dict[str, Any]:
    """将 ForgeGuard /verify 响应转换为本系统统一结构。"""
    inner = raw.get("data") or raw
    return {
        "result": inner.get("result"),
        "confidence": inner.get("confidence"),
        "bbox": inner.get("bbox"),
        "reason": inner.get("reason"),
        "bbox_overlap_check": inner.get("bbox_overlap_check"),
        "hard_tamper_flags": inner.get("hard_tamper_flags") or {},
    }
