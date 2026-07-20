# -*- coding: utf-8 -*-
"""规则检测高风险 ROI 自动定位（与 AI 鉴伪共用金额候选逻辑）。"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.ai_detection.core.amount_candidates import (
    AmountCandidate,
    DATE_PATTERN,
    MASKED_ACCOUNT_PATTERN,
    OCRToken,
    ORDER_PATTERN,
    TIME_PATTERN,
    bbox_iou,
    build_amount_candidates,
    looks_like_clock_time,
)
from app.ai_detection.core.semantic_checker import find_labeled_field_bbox

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
    (("收款人", "付款人", "收款方", "付款方", "姓名", "微信昵称", "微信号"), "姓名", 2),
    (("申请时间", "交易时间", "转账时间", "收款时间", "交易日期"), "时间", 3),
]

KEY_FIELD_ROI_CATEGORIES = [
    (("转账金额", "交易金额", "金额", "小写", "大写"), "amount", "金额", 1),
    (("收款人", "付款人", "收款方", "付款方", "姓名", "微信昵称", "微信号"), "name", "姓名", 2),
    (("申请时间", "交易时间", "转账时间", "收款时间", "交易日期"), "time", "时间", 3),
]

_DIRECT_DATETIME_PATTERN = re.compile(
    r"20\d{2}(?:(?:[-./]?(?:0?[1-9]|1[0-2]))[-./](?:0?[1-9]|[12]\d|3[01])"
    r"|年(?:0?[1-9]|1[0-2])月(?:0?[1-9]|[12]\d|3[01])日)"
    r"(?:[-./ T]?(?:[01]?\d|2[0-3])[:.]?[0-5]\d(?:[:.]?[0-5]\d)?)?"
)
_MINOR_UNIT_AMOUNT_PATTERN = re.compile(
    r"小(?:写)?[：:.]?[+\-]?(?:[¥￥])?\d[\d,]*(?:[.:]\d{2})?元$"
)
_DECIMAL_AMOUNT_WITH_TRAILING_PUNCTUATION = re.compile(
    r"(?:币)?[+\-]?(?:[¥￥])?\d[\d,]*[.:]\d{2}(?:元)?[，,。.]$"
)
_LABELED_CURRENCY_AMOUNT_PATTERN = re.compile(
    r"(?:转账金额|交易金额|收款金额|付款金额|金额|小写).*[+\-]?\d[\d,]*(?:[.:]\d{2})?元"
)
_CURRENCY_AMOUNT_VALUE_PATTERN = re.compile(
    r"[+\-]?(?:[¥￥])?\d[\d,]*(?:[.:]\d{2})?元"
)
_TRANSFER_CONTEXT_PATTERN = re.compile(r"(?:转给|转账|收款|付款|金额)")


def _is_direct_datetime_candidate(text: str) -> bool:
    """Accept a fully formatted date/time value, never an unstructured ID number."""
    compact = (text or "").strip()
    if not compact or not re.search(r"[-./年]", compact):
        return False

    # EasyOCR often reads 1 as I or l in otherwise well-formed dates.
    normalized = compact.replace("I", "1").replace("l", "1").replace("O", "0")
    return bool(_DIRECT_DATETIME_PATTERN.fullmatch(normalized))


def _is_reliable_amount_candidate(candidate: AmountCandidate) -> bool:
    """过滤普通数字、账号和单号，只保留有明确金额证据的候选。"""
    text = (candidate.clean_text or "").strip()
    if not text:
        return False

    compact_digits = text.replace(",", "")
    flags = set(str(candidate.match_flags or "").split("|"))
    has_money_regex = "money_regex" in flags
    has_target_keyword = "target_keyword" in flags
    has_currency_hint = "currency_hint" in flags or any(ch in text for ch in ("¥", "￥", "元"))
    has_signed_amount = "signed_or_currency" in flags or text.startswith(("+", "-", "¥", "￥"))
    has_decimal_amount = bool(re.search(r"\d[\d,]*[.:]\d{2}(?:元)?$", text))
    has_grouped_amount = bool(re.fullmatch(r"[+\-]?(?:[¥￥])?\d{1,3}(?:,\d{3})+(?:[.:]\d{2})?(?:元)?", text))
    has_minor_unit_prefix = bool(_MINOR_UNIT_AMOUNT_PATTERN.search(text))
    has_decimal_trailing_punctuation = bool(
        _DECIMAL_AMOUNT_WITH_TRAILING_PUNCTUATION.fullmatch(text)
    )
    amount_evidence = (
        has_target_keyword
        or has_currency_hint
        or has_signed_amount
        or has_decimal_amount
        or has_grouped_amount
        or has_minor_unit_prefix
        or (has_money_regex and has_decimal_trailing_punctuation)
    )

    if _is_direct_datetime_candidate(text) or DATE_PATTERN.search(text) or TIME_PATTERN.fullmatch(text) or looks_like_clock_time(text):
        return False
    if MASKED_ACCOUNT_PATTERN.search(text):
        return False
    if ORDER_PATTERN.fullmatch(compact_digits) and not amount_evidence:
        return False
    if re.fullmatch(r"\d{5,}", compact_digits) and not amount_evidence:
        return False

    return amount_evidence


def _nearby_labeled_currency_amount_tokens(tokens: Sequence[OCRToken]) -> List[OCRToken]:
    """Find an amount value wrapped onto the line below its explicit amount label."""
    values: List[OCRToken] = []
    labels = [
        token
        for token in tokens
        if any(label in token.clean_text for label in ("转账金额", "交易金额", "收款金额", "付款金额", "金额", "小写"))
    ]
    for label in labels:
        for value in tokens:
            if value is label or not _CURRENCY_AMOUNT_VALUE_PATTERN.search(value.clean_text):
                continue
            vertical_gap = abs(value.center_y - label.center_y)
            maximum_gap = max(label.height, value.height) * 3.0
            if vertical_gap <= maximum_gap:
                values.append(value)
    return values


def _nearby_transfer_amount_tokens(
    tokens: Sequence[OCRToken],
    image_shape: Tuple[int, int, int],
) -> List[OCRToken]:
    image_h, image_w = image_shape[:2]
    values: List[OCRToken] = []
    for value in tokens:
        digit_count = len(re.findall(r"\d", value.clean_text))
        if not 4 <= digit_count <= 9:
            continue
        if (
            DATE_PATTERN.search(value.clean_text)
            or TIME_PATTERN.search(value.clean_text)
            or MASKED_ACCOUNT_PATTERN.search(value.clean_text)
            or ORDER_PATTERN.fullmatch(value.clean_text)
        ):
            continue
        if not (image_h * 0.08 <= value.center_y <= image_h * 0.42):
            continue
        if abs(value.bbox[0] + value.bbox[2] - image_w) > image_w * 0.45:
            continue
        for context in tokens:
            if context is value or not _TRANSFER_CONTEXT_PATTERN.search(context.clean_text):
                continue
            vertical_gap = max(0, value.bbox[1] - context.bbox[3])
            maximum_gap = max(value.height, context.height) * 2.5
            if vertical_gap <= maximum_gap:
                values.append(value)
                break
    return values


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


def _add_key_field_roi(
    rois: List[Dict[str, Any]],
    seen_bboxes: List[Tuple[int, int, int, int]],
    bbox_xyxy: Sequence[int],
    *,
    field_type: str,
    field_label: str,
    priority: int,
    text: str,
    source: str,
    iou_threshold: float = 0.80,
) -> None:
    if len(bbox_xyxy) < 4:
        return
    bbox_tuple = tuple(int(v) for v in bbox_xyxy[:4])
    if bbox_tuple[2] <= bbox_tuple[0] or bbox_tuple[3] <= bbox_tuple[1]:
        return
    if any(bbox_iou(bbox_tuple, seen) >= iou_threshold for seen in seen_bboxes):
        return
    seen_bboxes.append(bbox_tuple)
    rois.append(
        {
            "bbox": [int(v) for v in bbox_tuple],
            "field_type": field_type,
            "field_label": field_label,
            "label": text or field_label,
            "category": field_label,
            "priority": priority,
            "source": source,
        }
    )


def find_key_field_rois(
    tokens: Sequence[OCRToken],
    image_shape: Tuple[int, int, int],
    *,
    business_rules: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """定位 v3 自动鉴伪使用的关键字段区域：金额、姓名、时间。"""
    rules = business_rules or {}
    max_rois = max(1, int(rules.get("key_field_rois_max", 24)))
    min_amount_score = float(rules.get("auto_pixel_rescan_min_amount_score", 0.35))
    image_h = image_shape[0] if image_shape else 0

    rois: List[Dict[str, Any]] = []
    seen_bboxes: List[Tuple[int, int, int, int]] = []

    for labels, field_type, field_label, priority in KEY_FIELD_ROI_CATEGORIES:
        for label_text in labels:
            bbox = find_labeled_field_bbox(tokens, label_text)
            if bbox is None:
                continue
            _add_key_field_roi(
                rois,
                seen_bboxes,
                bbox,
                field_type=field_type,
                field_label=field_label,
                priority=priority,
                text=label_text,
                source="ocr_labeled_field",
            )

    for token in tokens:
        if not _is_direct_datetime_candidate(token.clean_text):
            continue
        _add_key_field_roi(
            rois,
            seen_bboxes,
            token.bbox,
            field_type="time",
            field_label="时间",
            priority=5,
            text=token.clean_text[:32],
            source="ocr_datetime_value",
        )

    for token in tokens:
        if not _LABELED_CURRENCY_AMOUNT_PATTERN.search(token.clean_text):
            continue
        _add_key_field_roi(
            rois,
            seen_bboxes,
            token.bbox,
            field_type="amount",
            field_label="金额",
            priority=6,
            text=token.clean_text[:32],
            source="ocr_labeled_currency_amount",
        )

    for token in tokens:
        if not _CURRENCY_AMOUNT_VALUE_PATTERN.search(token.clean_text):
            continue
        _add_key_field_roi(
            rois,
            seen_bboxes,
            token.bbox,
            field_type="amount",
            field_label="金额",
            priority=6,
            text=token.clean_text[:32],
            source="ocr_currency_amount_value",
        )

    for token in _nearby_labeled_currency_amount_tokens(tokens):
        _add_key_field_roi(
            rois,
            seen_bboxes,
            token.bbox,
            field_type="amount",
            field_label="金额",
            priority=6,
            text=token.clean_text[:32],
            source="ocr_labeled_currency_value",
        )

    for token in _nearby_transfer_amount_tokens(tokens, image_shape):
        _add_key_field_roi(
            rois,
            seen_bboxes,
            token.bbox,
            field_type="amount",
            field_label="金额",
            priority=6,
            text=token.clean_text[:32],
            source="ocr_transfer_context_amount",
        )

    for candidate in build_amount_candidates(tokens, image_shape):
        if float(candidate.amount_score) < min_amount_score:
            continue
        if not _is_reliable_amount_candidate(candidate):
            continue
        text = (candidate.clean_text or "").strip()
        if text:
            digit_count = len(re.findall(r"\d", text))
            if digit_count == 0 or digit_count / max(len(text), 1) < 0.20:
                continue
            chinese_chars = set(re.findall(r"[\u4e00-\u9fff]", text))
            has_labeled_currency_amount = bool(
                _LABELED_CURRENCY_AMOUNT_PATTERN.search(text)
            )
            if chinese_chars - set("人民币币元圆小写") and not has_labeled_currency_amount:
                continue
            if not re.search(r"\d", text):
                continue
            if (
                not has_labeled_currency_amount
                and not _MINOR_UNIT_AMOUNT_PATTERN.search(text)
                and not re.search(r"\d[\d,]*[.:]\d{2}", text)
            ):
                continue
            if candidate.bbox[1] < image_h * 0.07:
                continue
            if re.fullmatch(r"\d{4}", text):
                continue
        _add_key_field_roi(
            rois,
            seen_bboxes,
            candidate.bbox,
            field_type="amount",
            field_label="金额",
            priority=6,
            text=candidate.clean_text[:32] or "金额",
            source=f"amount_{candidate.source}",
        )

    rois.sort(key=lambda row: (int(row.get("priority") or 99), row.get("bbox", [0, 0, 0, 0])[1], row.get("bbox", [0, 0, 0, 0])[0]))
    return rois[:max_rois]


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
    未传 bbox 时，用 OCR 定位建议检测区域（金额/姓名/时间），
    供前端展示让用户勾选后再做像素重叠检测。

    返回按 priority 升序排列的 ROI 列表，每项含 bbox/分类标签/OCR 文本。
    """
    from app.ai_detection.core.semantic_checker import find_labeled_field_bbox

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

    # 1. 按标签定位字段值区域（金额/姓名/时间）
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
        if not _is_reliable_amount_candidate(candidate):
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
            candidate.clean_text[:32] or "金额候选",
            "金额候选",
            6,
            source=f"amount_{candidate.source}",
        )

    # 按 priority 排序
    rois.sort(key=lambda r: (r["priority"], r.get("label", "")))
    return rois[:max_rois]
