# -*- coding: utf-8 -*-
"""规则检测高风险 ROI 自动定位（与 AI 鉴伪共用金额候选逻辑）。"""
from __future__ import annotations

import re
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

# 建议检测区域分类定义：(标签关键词元组, 分类名, 展示优先级)
SUGGESTED_ROI_CATEGORIES = [
    (("转账金额", "交易金额", "金额", "小写", "大写"), "金额", 1),
    (("收款账号", "付款账号", "收款方账户", "付款方账户", "对方账户"), "账号", 2),
    (("申请时间", "交易时间", "转账时间", "收款时间", "交易日期"), "时间", 3),
    (("转账单号", "订单号", "交易单号", "电子凭证号", "业务单号"), "单号", 4),
    (("收款人", "付款人", "收款方", "付款方", "姓名", "微信昵称", "微信号"), "姓名/昵称", 5),
]


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


def find_suggested_rois(
    tokens: Sequence[OCRToken],
    image_shape: Tuple[int, int, int],
    *,
    business_rules: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    未传 bbox 时，用 OCR 定位建议检测区域（金额/账号/时间/单号/姓名），
    供前端展示让用户勾选后再做像素重叠检测。

    返回按 priority 升序排列的 ROI 列表，每项含 bbox/分类标签/OCR 文本。
    """
    from app.ai_detection.semantic_checker import find_labeled_field_bbox

    rules = business_rules or {}
    max_rois = max(1, int(rules.get("suggested_rois_max", 12)))
    min_amount_score = float(rules.get("auto_pixel_rescan_min_amount_score", 0.35))

    rois: List[Dict[str, Any]] = []
    seen_bboxes: List[Tuple[int, int, int, int]] = []

    def _add_roi(bbox_xyxy: List[int], label: str, category: str, priority: int, source: str = "ocr_label") -> None:
        bbox_tuple = tuple(int(v) for v in bbox_xyxy[:4])
        # 去重：与已添加区域 IoU >= 0.8 则跳过
        if any(bbox_iou(bbox_tuple, seen) >= 0.80 for seen in seen_bboxes):
            return
        seen_bboxes.append(bbox_tuple)
        rois.append({
            "bbox": [int(v) for v in bbox_xyxy[:4]],
            "label": label,
            "category": category,
            "priority": priority,
            "source": source,
        })

    # 1. 按标签定位字段值区域（金额/账号/时间/单号/姓名）
    for labels, category, priority in SUGGESTED_ROI_CATEGORIES:
        for label_text in labels:
            bbox = find_labeled_field_bbox(tokens, label_text)
            if bbox is not None:
                _add_roi(bbox, label_text, category, priority, source="ocr_labeled_field")

    # 2. 自动检测的数字/金额候选区域
    amount_candidates: List[AmountCandidate] = build_amount_candidates(tokens, image_shape)
    image_h, image_w = image_shape[:2]
    for candidate in amount_candidates:
        if float(candidate.amount_score) < min_amount_score:
            continue
        text = (candidate.clean_text or "").strip()
        if text:
            digit_count = len(re.findall(r'\d', text))
            # 无数字或数字占比过低 → 非金额
            if digit_count == 0 or digit_count / max(len(text), 1) < 0.20:
                continue
            # 含中文 → OCR 粘连，金额区域不应有中文
            if re.search(r'[一-鿿]', text):
                continue
            # 含过多异常标点（..、+、连续符号）→ OCR 噪声
            noisy = len(re.findall(r'\.{2,}|\+|[*@#]', text))
            if noisy > 0 and noisy / max(len(text), 1) > 0.05:
                continue
            # 状态栏：顶部 7% 区域无金额关键词的数字直接排除
            if candidate.bbox[1] < image_h * 0.07:
                continue
            # 纯4位数字（无逗号/符号/小数点）→ 极可能是账号碎片或验证码
            if re.fullmatch(r'\d{4}', text):
                continue
        _add_roi(
            [int(v) for v in candidate.bbox],
            candidate.clean_text[:32] or "数字区域",
            "金额候选",
            6,
            source=f"amount_{candidate.source}",
        )

    # 按 priority 排序
    rois.sort(key=lambda r: (r["priority"], r.get("label", "")))
    return rois[:max_rois]
