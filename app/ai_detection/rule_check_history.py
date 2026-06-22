# -*- coding: utf-8 -*-
"""规则检测（像素重叠 / 时间戳）历史落库：outcome 组装与写入 ai_detection_history。"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.ai_detection.history_db import insert_ai_detection_history

MODE_RULE_CHECKS = "rule_checks"
MODE_RULE_PIXEL_OVERLAP = "rule_pixel_overlap"
MODE_RULE_TIMESTAMP = "rule_timestamp"

RULE_CHECK_MODES = frozenset({
    MODE_RULE_CHECKS,
    MODE_RULE_PIXEL_OVERLAP,
    MODE_RULE_TIMESTAMP,
})


def _summary_from_pixel_overlap(data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not data:
        return {
            "pixel_overlap_score": None,
            "pixel_alert": False,
            "pixel_hard_tamper": False,
        }
    return {
        "pixel_overlap_score": data.get("pixel_overlap_score"),
        "pixel_alert": bool(data.get("alert")),
        "pixel_hard_tamper": bool(data.get("hard_tamper")),
    }


def _summary_from_timestamp(data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not data:
        return {
            "timestamp_risk": None,
            "timestamp_hard_tamper": False,
            "business_mismatch": False,
            "anomaly_codes": [],
        }
    return {
        "timestamp_risk": data.get("risk"),
        "timestamp_hard_tamper": bool(data.get("hard_tamper")),
        "business_mismatch": bool(data.get("business_mismatch")),
        "anomaly_codes": list(data.get("anomalies") or []),
    }


def build_rule_checks_outcome(
    data: Dict[str, Any],
    *,
    bbox: Optional[List[int]] = None,
    bboxes: Optional[List[List[int]]] = None,
    document_time: Optional[str] = None,
) -> Dict[str, Any]:
    pixel = data.get("pixel_overlap")
    timestamp = data.get("timestamp")
    flags = data.get("hard_tamper_flags") or {}
    pixel_summary = _summary_from_pixel_overlap(pixel if isinstance(pixel, dict) else None)
    ts_summary = _summary_from_timestamp(timestamp if isinstance(timestamp, dict) else None)
    any_hard = (
        bool(flags.get("pixel_overlap"))
        or bool(flags.get("timestamp"))
    )
    request: Dict[str, Any] = {"document_time": document_time}
    if bboxes:
        request["bboxes"] = bboxes
    elif bbox is not None:
        request["bbox"] = bbox
    return {
        "api_version": "v1",
        "check_type": MODE_RULE_CHECKS,
        "request": request,
        "pixel_overlap": pixel,
        "pixel_overlap_source": data.get("pixel_overlap_source"),
        "suggested_rois": data.get("suggested_rois"),
        "timestamp": timestamp,
        "hard_tamper_flags": flags,
        "reason": data.get("reason"),
        "summary": {
            **pixel_summary,
            **ts_summary,
            "any_hard_tamper": any_hard,
        },
    }


def build_pixel_overlap_outcome(
    data: Dict[str, Any],
    *,
    bbox: List[int],
    bboxes: Optional[List[List[int]]] = None,
) -> Dict[str, Any]:
    pixel_summary = _summary_from_pixel_overlap(data)
    request: Dict[str, Any] = {}
    if bboxes:
        request["bboxes"] = bboxes
    else:
        request["bbox"] = bbox
    return {
        "api_version": "v1",
        "check_type": MODE_RULE_PIXEL_OVERLAP,
        "request": request,
        "result": data,
        "summary": {
            **pixel_summary,
            "any_hard_tamper": pixel_summary["pixel_hard_tamper"],
        },
    }


def build_timestamp_outcome(
    data: Dict[str, Any],
    *,
    document_time: Optional[str] = None,
) -> Dict[str, Any]:
    ts_summary = _summary_from_timestamp(data)
    return {
        "api_version": "v1",
        "check_type": MODE_RULE_TIMESTAMP,
        "request": {"document_time": document_time},
        "result": data,
        "summary": {
            **ts_summary,
            "any_hard_tamper": ts_summary["timestamp_hard_tamper"],
        },
    }


def build_rule_check_failed_outcome(
    check_type: str,
    error_msg: str,
    *,
    bbox: Optional[List[int]] = None,
    document_time: Optional[str] = None,
) -> Dict[str, Any]:
    request: Dict[str, Any] = {}
    if bbox is not None:
        request["bbox"] = bbox
    if document_time is not None:
        request["document_time"] = document_time
    return {
        "api_version": "v1",
        "check_type": check_type,
        "request": request or None,
        "error_msg": error_msg,
    }


def persist_rule_check_history(
    *,
    mode: str,
    original_filename: Optional[str],
    bbox: Optional[List[int]],
    bboxes: Optional[List[List[int]]] = None,
    status: str,
    outcome: Dict[str, Any],
    source_image_path: Optional[str] = None,
    task_id: Optional[str] = None,
    image_created_at: Optional[str] = None,
) -> None:
    """写入 ai_detection_history；失败仅打日志，不抛出。

    bbox: 单框 [x1,y1,x2,y2]（向后兼容）；bboxes: 多框 [[x1,y1,x2,y2], ...]。
    多框时优先使用 bboxes 写入 bbox 列（JSON 数组形式）。"""
    stored_bbox = bboxes if bboxes else bbox
    insert_ai_detection_history(
        mode=mode,
        task_id=task_id,
        original_filename=original_filename,
        bbox=stored_bbox,
        status=status,
        outcome=outcome,
        source_image_path=source_image_path,
        image_created_at=image_created_at,
    )
