# -*- coding: utf-8 -*-
"""单据语义规则：金额格式、明细行排版、无 EXIF 合成图信号。"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import cv2
import numpy as np

from app.ai_detection.core.amount_candidates import (
    MASKED_ACCOUNT_PATTERN,
    OCRToken,
    group_tokens_by_line,
    normalize_text,
)
from app.ai_detection.core.detectors import OriginalityChecker
from app.ai_detection.core.timestamp_checker import parse_exif_timestamps

AMOUNT_DECIMAL_PATTERN = re.compile(
    r"(?<!\d)[+\-]?\s*(?:[¥￥])?\d[\d,]*\.\d{2}(?:元)?(?!\d)"
)
DETAIL_FIELD_LABELS = (
    "收款账号",
    "付款账号",
    "收款方账户",
    "付款方账户",
    "收款银行",
    "付款银行",
    "交易时间",
    "交易渠道",
    "汇款附言",
    "转账金额",
)
ACCOUNT_FIELD_LABELS = (
    "收款账号",
    "付款账号",
    "收款方账户",
    "付款方账户",
    "对方账户",
)

DEFAULT_HARD_SEMANTIC_ANOMALIES = frozenset({
    "invalid_amount_format",
    "detail_field_typography_anomaly",
    "account_mask_pattern_inconsistent",
    "synthetic_image_signals",
})


def _resolve_hard_semantic_anomalies(thresholds: Dict[str, Any]) -> frozenset[str]:
    configured = thresholds.get("hard_semantic_anomalies")
    if configured is None:
        return DEFAULT_HARD_SEMANTIC_ANOMALIES
    return frozenset(str(item) for item in configured)


def is_invalid_amount_thousand_separator(text: str) -> bool:
    """检测千分位格式错误，如 3,2500.00（逗号后应为 3 位）。"""
    clean = normalize_text(text)
    match = AMOUNT_DECIMAL_PATTERN.search(clean)
    if not match:
        return False

    amount_text = match.group()
    integer_part = amount_text.split(".")[0]
    integer_part = re.sub(r"^[+\-]?\s*(?:[¥￥])?", "", integer_part)
    if "," not in integer_part:
        return False

    groups = integer_part.split(",")
    if len(groups) < 2:
        return False

    for index, group in enumerate(groups[1:], start=1):
        if len(group) != 3:
            return True

    first_group = groups[0]
    if len(first_group) > 3:
        return True
    return False


def _find_label_value_tokens(line: Sequence[OCRToken], label: str) -> List[OCRToken]:
    label_index: Optional[int] = None
    for index, token in enumerate(line):
        if label in token.clean_text:
            label_index = index
            break
    if label_index is None:
        return []

    value_tokens = [
        token
        for token in line[label_index + 1 :]
        if token.clean_text and label not in token.clean_text
    ]
    if value_tokens:
        return value_tokens

    for token in line:
        if label in token.clean_text:
            continue
        if MASKED_ACCOUNT_PATTERN.search(token.clean_text):
            return [token]
    return []


def _merge_token_bbox(tokens: Sequence[OCRToken]) -> Optional[List[int]]:
    if not tokens:
        return None
    return [
        min(token.bbox[0] for token in tokens),
        min(token.bbox[1] for token in tokens),
        max(token.bbox[2] for token in tokens),
        max(token.bbox[3] for token in tokens),
    ]


def _account_mask_pattern(text: str) -> Optional[tuple[int, ...]]:
    """提取账号脱敏星号分组长度，如 6213 **** **** 3191 -> (4, 4)。"""
    clean = normalize_text(text)
    if not re.search(r"\*", clean):
        return None
    groups = re.findall(r"\*+", clean)
    if not groups:
        return None
    return tuple(len(group) for group in groups)


def check_account_mask_consistency(tokens: Sequence[OCRToken]) -> Dict[str, Any]:
    """同一回单内多个账号的星号分组格式应一致（GPT 重绘常出现 *** 与 **** 混用）。"""
    patterns: Dict[str, tuple[int, ...]] = {}
    for token in tokens:
        pattern = _account_mask_pattern(token.clean_text)
        if pattern is None:
            continue
        label_hint = token.clean_text[:16]
        patterns[label_hint] = pattern

    for line in group_tokens_by_line(tokens):
        line_patterns: List[tuple[int, ...]] = []
        line_labels: List[str] = []
        for label in ACCOUNT_FIELD_LABELS:
            value_tokens = _find_label_value_tokens(line, label)
            for token in value_tokens:
                pattern = _account_mask_pattern(token.clean_text)
                if pattern is not None:
                    line_patterns.append(pattern)
                    line_labels.append(label)

        unique_patterns = set(line_patterns)
        if len(unique_patterns) > 1:
            return {
                "anomaly": True,
                "patterns": {label: list(pattern) for label, pattern in zip(line_labels, line_patterns)},
                "message": "同一回单内账号脱敏格式不一致",
            }

    all_patterns = list(patterns.values())
    if len(set(all_patterns)) > 1 and len(all_patterns) >= 2:
        return {
            "anomaly": True,
            "patterns": {key: list(value) for key, value in patterns.items()},
            "message": "多个账号脱敏星号分组不一致",
        }

    return {"anomaly": False, "patterns": patterns}


def is_large_amount_missing_separator(text: str, *, min_integer_digits: int = 5) -> bool:
    """大额数字未使用千分位（如 73929.50），常见于 AI 重绘回单。"""
    clean = normalize_text(text)
    match = AMOUNT_DECIMAL_PATTERN.search(clean)
    if not match:
        return False
    amount_text = match.group()
    if "," in amount_text:
        return False
    integer_part = re.sub(r"^[+\-]?\s*(?:[¥￥])?", "", amount_text.split(".")[0])
    digits = re.sub(r"\D", "", integer_part)
    return len(digits) >= min_integer_digits


def find_account_field_bbox(
    tokens: Sequence[OCRToken],
) -> Optional[List[int]]:
    """优先定位收款侧账号字段 bbox（xyxy）。"""
    for label in ACCOUNT_FIELD_LABELS:
        bbox = find_labeled_field_bbox(tokens, label)
        if bbox is not None:
            return bbox
    return None


def find_labeled_field_bbox(
    tokens: Sequence[OCRToken],
    label: str = "收款账号",
) -> Optional[List[int]]:
    """从 OCR 结果定位指定标签对应值的 bbox（xyxy）。"""
    for line in group_tokens_by_line(tokens):
        value_tokens = _find_label_value_tokens(line, label)
        if value_tokens:
            merged = _merge_token_bbox(value_tokens)
            if merged is not None:
                return merged
    return None


def check_detail_field_typography(tokens: Sequence[OCRToken]) -> Dict[str, Any]:
    """
    对比明细行（收款账号/付款账号等）右侧值的字高与基线。
    单行替换文字时常出现高度或基线偏移。
    """
    labeled_rows: List[Dict[str, Any]] = []
    typography_labels = (
        "收款账号",
        "付款账号",
        "收款方账户",
        "付款方账户",
        "交易时间",
        "转账金额",
    )
    for line in group_tokens_by_line(tokens):
        for label in typography_labels:
            value_tokens = _find_label_value_tokens(line, label)
            if not value_tokens:
                continue
            primary = max(value_tokens, key=lambda item: item.width)
            labeled_rows.append(
                {
                    "label": label,
                    "height": float(primary.height),
                    "center_y": float(primary.center_y),
                    "text": primary.clean_text,
                }
            )

    if len(labeled_rows) < 2:
        return {"anomaly": False, "rows": labeled_rows}

    heights = [row["height"] for row in labeled_rows]
    center_ys = [row["center_y"] for row in labeled_rows]
    median_height = float(np.median(heights))
    median_center_y = float(np.median(center_ys))

    outliers: List[str] = []
    for row in labeled_rows:
        if median_height > 0 and abs(row["height"] - median_height) / median_height > 0.22:
            outliers.append(f"{row['label']}字高异常")
        if abs(row["center_y"] - median_center_y) > max(6.0, median_height * 0.45):
            outliers.append(f"{row['label']}基线偏移")

    return {
        "anomaly": bool(outliers),
        "rows": labeled_rows,
        "outliers": outliers,
        "median_height": median_height,
        "median_center_y": median_center_y,
    }


def _background_variance(gray: np.ndarray) -> float:
    h, w = gray.shape
    if h < 20 or w < 20:
        return float(np.var(gray))

    samples = [
        gray[: max(4, h // 10), : max(4, w // 5)],
        gray[: max(4, h // 10), -max(4, w // 5) :],
        gray[-max(4, h // 10) :, : max(4, w // 5)],
    ]
    values = [float(np.var(sample)) for sample in samples if sample.size > 0]
    return float(np.mean(values)) if values else float(np.var(gray))


def check_synthetic_image_signals(
    image_path: str,
    *,
    ocr_tokens: Optional[Sequence[OCRToken]] = None,
    semantic_anomalies: Optional[Sequence[str]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    无 EXIF / AI 重绘启发式：平滑背景 + 低噪声 + 语义异常组合。
    针对 GPT 等清除元数据的全图重绘。
    """
    thresh = thresholds or {}
    exif_info = parse_exif_timestamps(image_path)
    feats, _, _ = OriginalityChecker.extract_features(image_path)
    feats = feats or {}

    signals: List[str] = []
    no_exif = not exif_info.get("has_exif")
    if no_exif:
        signals.append("no_exif")

    bg_var = None
    try:
        img = cv2.imdecode(np.fromfile(image_path, dtype=np.uint8), cv2.IMREAD_GRAYSCALE)
        if img is not None:
            bg_var = _background_variance(img)
            smooth_bg = bg_var < float(thresh.get("synthetic_bg_var_max", 0.08))
            if smooth_bg:
                signals.append("smooth_background")
    except Exception:
        pass

    noise_std = float(feats.get("noise_std", 0.0))
    low_noise = noise_std < float(thresh.get("synthetic_noise_std_max", 55.0))
    if low_noise and no_exif:
        signals.append("low_noise")

    has_semantic = bool(semantic_anomalies)
    if has_semantic:
        signals.append("semantic_anomaly")

    min_signals = int(thresh.get("synthetic_min_signals", 3))
    receipt_hint = any(
        any(keyword in token.clean_text for keyword in ("回单", "电子回单", "交易成功", "转账金额"))
        for token in (ocr_tokens or [])
    )
    suspicious = len(set(signals)) >= min_signals or (no_exif and has_semantic and "smooth_background" in signals)
    if no_exif and receipt_hint and low_noise and "smooth_background" in signals:
        suspicious = True

    return {
        "suspicious": suspicious,
        "signals": signals,
        "no_exif": no_exif,
        "background_variance": bg_var,
        "noise_std": noise_std,
    }


def check_receipt_semantics(
    image_path: str,
    *,
    ocr_tokens: Optional[Sequence[OCRToken]] = None,
    image_shape: Optional[Tuple[int, int, int]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """综合语义规则检测，供 rule-checks 调用。"""
    thresh = thresholds or {}
    hard_set = _resolve_hard_semantic_anomalies(thresh)
    tokens = list(ocr_tokens or [])

    anomalies: List[str] = []
    reasons: List[str] = []
    risk = 0.0
    details: Dict[str, Any] = {}

    invalid_amounts: List[str] = []
    for token in tokens:
        if is_invalid_amount_thousand_separator(token.clean_text):
            invalid_amounts.append(token.clean_text)

    for line in group_tokens_by_line(tokens):
        merged = normalize_text("".join(item.clean_text for item in line))
        if is_invalid_amount_thousand_separator(merged):
            invalid_amounts.append(merged)

    if invalid_amounts:
        anomalies.append("invalid_amount_format")
        sample = invalid_amounts[0]
        reasons.append(f"金额千分位格式异常（如 {sample}）")
        risk = max(risk, float(thresh.get("semantic_amount_format_risk", 0.78)))

    mask_check = check_account_mask_consistency(tokens)
    details["account_masks"] = mask_check
    if mask_check.get("anomaly"):
        anomalies.append("account_mask_pattern_inconsistent")
        reasons.append("账号脱敏星号格式不一致（疑似非原生回单）")
        risk = max(risk, float(thresh.get("semantic_account_mask_risk", 0.76)))

    typography = check_detail_field_typography(tokens)
    details["typography"] = typography
    if typography.get("anomaly"):
        anomalies.append("detail_field_typography_anomaly")
        outlier_text = "、".join(typography.get("outliers") or [])
        reasons.append(f"明细行排版不一致（{outlier_text}）")
        risk = max(risk, float(thresh.get("semantic_typography_risk", 0.72)))

    synthetic = check_synthetic_image_signals(
        image_path,
        ocr_tokens=tokens,
        semantic_anomalies=anomalies,
        thresholds=thresh,
    )
    details["synthetic"] = synthetic
    if synthetic.get("suspicious"):
        anomalies.append("synthetic_image_signals")
        signal_text = "、".join(synthetic.get("signals") or [])
        reasons.append(f"疑似 AI 重绘/合成图（{signal_text}）")
        risk = max(risk, float(thresh.get("semantic_synthetic_risk", 0.75)))

    account_bbox = find_account_field_bbox(tokens)
    if account_bbox is not None:
        details["account_field_bbox"] = account_bbox

    hard_anomalies = set(hard_set).intersection(anomalies)
    return {
        "semantic_check": details,
        "anomalies": list(dict.fromkeys(anomalies)),
        "reasons": reasons,
        "risk": float(min(1.0, risk)),
        "hard_tamper": bool(hard_anomalies),
        "account_field_bbox": account_bbox,
    }
