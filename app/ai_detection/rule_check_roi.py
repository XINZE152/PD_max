# -*- coding: utf-8 -*-
"""规则检测高风险 ROI 自动定位（与 AI 鉴伪共用金额候选逻辑）。"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.ai_detection.amount_candidates import (
    AmountCandidate,
    OCRToken,
    bbox_iou,
    build_amount_candidates,
)
from app.ai_detection.semantic_checker import find_labeled_field_bbox

# 账单/回单中易被 P 图替换的字段（值区域做像素拼接检测）
HIGH_RISK_FIELD_LABELS = (
    "收款账号",
    "付款账号",
    "收款方账户",
    "付款方账户",
    "对方账户",
    "转账金额",
    "交易金额",
    "金额",
    "小写",
)


def _dedupe_rois(
    rois: List[Dict[str, Any]],
    *,
    iou_threshold: float = 0.72,
) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    for item in sorted(rois, key=lambda row: float(row.get("risk_score") or 0.0), reverse=True):
        bbox = item.get("bbox")
        if not bbox or len(bbox) != 4:
            continue
        if any(
            bbox_iou(tuple(bbox), tuple(existing["bbox"])) >= iou_threshold
            for existing in kept
            if existing.get("bbox")
        ):
            continue
        kept.append(item)
    return kept


def find_high_risk_pixel_rois(
    tokens: Sequence[OCRToken],
    image_shape: Tuple[int, int, int],
    *,
    business_rules: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    在未手动框选时，定位 P 图高风险区域（大号金额、账户、带金额标签的行等）。
    与 ``build_detection_bboxes_from_tokens`` 同源，供规则像素拼接二次扫描。
    """
    rules = business_rules or {}
    min_amount_score = float(rules.get("auto_pixel_rescan_min_amount_score", 0.35))
    max_rois = max(1, int(rules.get("auto_pixel_rescan_max_rois", 5)))

    rois: List[Dict[str, Any]] = []

    for label in HIGH_RISK_FIELD_LABELS:
        bbox = find_labeled_field_bbox(tokens, label)
        if bbox is None:
            continue
        rois.append(
            {
                "bbox": [int(v) for v in bbox],
                "source": "ocr_labeled_field",
                "label": label,
                "risk_score": 0.82 if label in ("对方账户", "收款账号", "收款方账户") else 0.75,
            }
        )

    amount_candidates: List[AmountCandidate] = build_amount_candidates(tokens, image_shape)
    for candidate in amount_candidates:
        if float(candidate.amount_score) < min_amount_score:
            continue
        rois.append(
            {
                "bbox": [int(v) for v in candidate.bbox],
                "source": f"amount_{candidate.source}",
                "label": candidate.clean_text[:32] or "金额区域",
                "risk_score": min(1.0, 0.55 + float(candidate.amount_score) * 0.35),
                "amount_score": float(candidate.amount_score),
                "match_flags": candidate.match_flags,
            }
        )

    return _dedupe_rois(rois)[:max_rois]


def should_auto_scan_high_risk_pixel_rois(
    *,
    manual_bbox: Optional[Sequence[int]],
    business_rules: Optional[Dict[str, Any]] = None,
) -> bool:
    """未手动框选时，是否对金额/余额/账户等多 ROI 做像素拼接扫描。"""
    if manual_bbox is not None:
        return False
    rules = business_rules or {}
    return bool(rules.get("auto_detect_high_risk_rois", True))


def rule_checks_need_auto_pixel_rescan(
    *,
    manual_bbox: Optional[Sequence[int]],
    semantic: Dict[str, Any],
    timestamp: Dict[str, Any],
    pixel_overlap: Optional[Dict[str, Any]],
    business_rules: Optional[Dict[str, Any]] = None,
) -> bool:
    """兼容旧调用；语义/像素已有结论时仍继续扫金额等区域。"""
    _ = semantic, timestamp, pixel_overlap
    return should_auto_scan_high_risk_pixel_rois(
        manual_bbox=manual_bbox,
        business_rules=business_rules,
    )
