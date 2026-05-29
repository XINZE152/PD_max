# -*- coding: utf-8 -*-
"""检测框 IoU 重叠分析：识别 OCR 多区域高度重叠（疑似重复贴图/复制区域）。"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple


def bbox_iou_xyxy(a: Sequence[int], b: Sequence[int]) -> float:
    ax1, ay1, ax2, ay2 = [int(v) for v in a[:4]]
    bx1, by1, bx2, by2 = [int(v) for v in b[:4]]
    inter_x1 = max(ax1, bx1)
    inter_y1 = max(ay1, by1)
    inter_x2 = min(ax2, bx2)
    inter_y2 = min(ay2, by2)
    inter_w = max(0, inter_x2 - inter_x1)
    inter_h = max(0, inter_y2 - inter_y1)
    inter_area = inter_w * inter_h
    if inter_area == 0:
        return 0.0
    area_a = max(1, (ax2 - ax1) * (ay2 - ay1))
    area_b = max(1, (bx2 - bx1) * (by2 - by1))
    return inter_area / max(1, area_a + area_b - inter_area)


def _normalize_bbox_xyxy(bbox: Sequence[int]) -> Tuple[int, int, int, int]:
    x1, y1, x2, y2 = [int(v) for v in bbox[:4]]
    if x2 < x1:
        x1, x2 = x2, x1
    if y2 < y1:
        y1, y2 = y2, y1
    return x1, y1, max(x1 + 1, x2), max(y1 + 1, y2)


def _is_same_row_ocr_fragment(
    a: Tuple[int, int, int, int],
    b: Tuple[int, int, int, int],
    iou: float,
    hard_iou: float,
) -> bool:
    """同一行相邻 OCR 分片会产生中等 IoU，不应视为复制贴图。"""
    if iou >= hard_iou:
        return False
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    row_tol = 10
    if abs(ay1 - by1) > row_tol or abs(ay2 - by2) > row_tol:
        return False
    vertical_overlap = max(0, min(ay2, by2) - max(ay1, by1))
    min_h = min(ay2 - ay1, by2 - by1)
    return min_h > 0 and (vertical_overlap / min_h) >= 0.7


def analyze_bbox_iou_overlaps(
    bboxes: Sequence[Sequence[int]],
    *,
    roi_bbox_xyxy: Optional[Sequence[int]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
  分析检测框两两 IoU，输出最大重叠分、重叠对列表与风险分。
  用于鉴伪：高度重叠的多个金额/数字区域可能为复制粘贴伪造。
    """
    thresh = thresholds or {}
    alert_iou = float(thresh.get("bbox_iou_alert", 0.35))
    hard_iou = float(thresh.get("bbox_iou_hard_tamper", 0.70))
    iou_risk = float(thresh.get("bbox_iou_risk", 0.62))

    normalized: List[Tuple[int, int, int, int]] = []
    for bbox in bboxes or []:
        if len(bbox) < 4:
            continue
        normalized.append(_normalize_bbox_xyxy(bbox))

    if roi_bbox_xyxy is not None and len(roi_bbox_xyxy) >= 4:
        roi = _normalize_bbox_xyxy(roi_bbox_xyxy)
        if roi not in normalized:
            normalized.append(roi)

    overlapping_pairs: List[Dict[str, Any]] = []
    max_iou = 0.0
    risk_max_iou = 0.0

    for i in range(len(normalized)):
        for j in range(i + 1, len(normalized)):
            iou = bbox_iou_xyxy(normalized[i], normalized[j])
            max_iou = max(max_iou, iou)
            same_row_fragment = _is_same_row_ocr_fragment(normalized[i], normalized[j], iou, hard_iou)
            if not same_row_fragment:
                risk_max_iou = max(risk_max_iou, iou)
            if iou >= alert_iou:
                overlapping_pairs.append(
                    {
                        "bbox_a": list(normalized[i]),
                        "bbox_b": list(normalized[j]),
                        "iou": round(float(iou), 4),
                        "same_row_fragment": same_row_fragment,
                    }
                )

    overlapping_pairs.sort(key=lambda item: item["iou"], reverse=True)
    hard_tamper = risk_max_iou >= hard_iou
    risk = iou_risk if risk_max_iou >= alert_iou else 0.0
    if hard_tamper:
        risk = max(risk, float(thresh.get("bbox_iou_hard_risk", 0.82)))

    reasons: List[str] = []
    if hard_tamper:
        reasons.append("检测到多个高度重叠的疑似数字区域(疑似复制贴图)")
    elif risk_max_iou >= alert_iou:
        reasons.append("检测到部分重叠的疑似数字区域")

    return {
        "bbox_overlap_check": {
            "max_iou": round(float(max_iou), 4),
            "risk_max_iou": round(float(risk_max_iou), 4),
            "overlapping_pairs": overlapping_pairs,
            "box_count": len(normalized),
            "anomalies": (
                ["bbox_iou_hard_overlap"]
                if hard_tamper
                else (["bbox_iou_partial_overlap"] if risk_max_iou >= alert_iou else [])
            ),
        },
        "risk": float(min(1.0, risk)),
        "reasons": reasons,
        "hard_tamper": hard_tamper,
        "max_iou": float(max_iou),
    }
