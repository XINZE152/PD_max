# -*- coding: utf-8 -*-
"""规则类鉴伪检测：像素重叠与时间戳（与 AI 模型鉴伪解耦，供独立接口与引擎复用）。"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import cv2
import yaml

from app.ai_detection.core.detectors import PixelLevelDetector
from app.ai_detection.core.utils import safe_read_image
from app.ai_detection.semantic_checker import check_receipt_semantics, find_labeled_field_bbox
from app.ai_detection.timestamp_checker import check_image_timestamps


def load_rule_check_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = (Path(__file__).resolve().parent / config_file).resolve()
    with open(config_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def clip_bbox_xyxy(bbox_xyxy: Sequence[int], img_w: int, img_h: int) -> Tuple[int, int, int, int]:
    x1, y1, x2, y2 = [int(v) for v in bbox_xyxy[:4]]
    x1 = max(0, min(x1, img_w - 1))
    y1 = max(0, min(y1, img_h - 1))
    x2 = max(x1 + 1, min(x2, img_w))
    y2 = max(y1 + 1, min(y2, img_h))
    return x1, y1, x2, y2


def normalize_roi_bbox(
    roi_bbox: Sequence[int],
    img_w: int,
    img_h: int,
    bbox_format: str = "xyxy",
) -> Tuple[int, int, int, int]:
    if len(roi_bbox) != 4:
        raise ValueError("ROI bbox must contain exactly four integers.")

    x1, y1, third, fourth = [int(v) for v in roi_bbox]
    format_name = (bbox_format or "xyxy").lower()

    if format_name == "xyxy":
        return clip_bbox_xyxy([x1, y1, third, fourth], img_w, img_h)

    if format_name == "xywh":
        return clip_bbox_xyxy([x1, y1, x1 + third, y1 + fourth], img_w, img_h)

    if format_name == "auto":
        looks_like_xyxy = third > x1 and fourth > y1 and third <= img_w and fourth <= img_h
        if looks_like_xyxy:
            return clip_bbox_xyxy([x1, y1, third, fourth], img_w, img_h)
        return clip_bbox_xyxy([x1, y1, x1 + third, y1 + fourth], img_w, img_h)

    looks_like_xyxy = third > x1 and fourth > y1 and third <= img_w and fourth <= img_h
    if looks_like_xyxy:
        return clip_bbox_xyxy([x1, y1, third, fourth], img_w, img_h)

    return clip_bbox_xyxy([x1, y1, x1 + third, y1 + fourth], img_w, img_h)


def crop_expanded_roi(
    img: Any,
    bbox_xyxy: Sequence[int],
    margin: int,
) -> Tuple[Any, List[int]]:
    """返回外扩 ROI 图像与 [x, y, w, h]（引擎实际使用的 ROI 格式）。"""
    img_h, img_w = img.shape[:2]
    x1, y1, x2, y2 = clip_bbox_xyxy(bbox_xyxy, img_w, img_h)
    x, y = x1, y1
    w, h = x2 - x1, y2 - y1

    x_exp = max(0, x - margin)
    y_exp = max(0, y - margin)
    w_exp = min(img_w - x_exp, w + 2 * margin)
    h_exp = min(img_h - y_exp, h + 2 * margin)
    roi_expanded = img[y_exp : y_exp + h_exp, x_exp : x_exp + w_exp]
    return roi_expanded, [x, y, w, h]


def evaluate_pixel_overlap_alert(
    metrics: Dict[str, float],
    thresholds: Dict[str, Any],
) -> bool:
    """
    分层告警：blend 达阈即报；或 structural 高且伴随双重边缘；
    或 ELA/噪声条带检测到文字级无痕替换。
    """
    structural = float(metrics.get("structural_score", 0.0))
    blend = float(metrics.get("blend_score", 0.0))
    double_edge = float(metrics.get("double_edge_ratio", 0.0))
    ela = float(metrics.get("ela_score", 0.0))
    text_splice = float(metrics.get("text_splice_score", 0.0))

    blend_alert = float(thresholds.get("pixel_overlap_blend_alert", 0.55))
    structural_alert = float(thresholds.get("pixel_overlap_structural_alert", 0.79))
    structural_de_min = float(thresholds.get("pixel_overlap_structural_de_min", 0.018))
    text_splice_alert = float(thresholds.get("pixel_overlap_text_splice_alert", 0.38))
    ela_corroboration_min = float(thresholds.get("pixel_overlap_ela_corroboration_min", 0.22))
    structural_text_min = float(thresholds.get("pixel_overlap_structural_text_min", 0.52))

    if blend >= blend_alert:
        return True
    if text_splice >= text_splice_alert:
        return True
    if ela >= ela_corroboration_min and structural >= structural_text_min:
        return True
    return structural >= structural_alert and double_edge >= structural_de_min


def _pixel_overlap_has_blend_corroboration(
    metrics: Dict[str, float],
    thresholds: Dict[str, Any],
) -> bool:
    """hard 判需羽化/双重边缘佐证，避免纯 structural 高分误 hard。"""
    blend = float(metrics.get("blend_score", 0.0))
    double_edge = float(metrics.get("double_edge_ratio", 0.0))
    blend_min = float(thresholds.get("pixel_overlap_hard_blend_min", 0.55))
    de_min = float(thresholds.get("pixel_overlap_hard_de_min", 0.045))
    return blend >= blend_min or double_edge >= de_min


def _pixel_overlap_has_text_splice_corroboration(
    metrics: Dict[str, float],
    thresholds: Dict[str, Any],
) -> bool:
    text_splice = float(metrics.get("text_splice_score", 0.0))
    ela = float(metrics.get("ela_score", 0.0))
    text_min = float(thresholds.get("pixel_overlap_text_splice_hard_min", 0.42))
    ela_min = float(thresholds.get("pixel_overlap_ela_hard_min", 0.28))
    return text_splice >= text_min or ela >= ela_min


def evaluate_pixel_overlap_hard_tamper(
    metrics: Dict[str, float],
    thresholds: Dict[str, Any],
    *,
    corroboration_signals: Optional[Dict[str, bool]] = None,
) -> bool:
    """根据阈值判定像素重叠是否硬判篡改；独立接口需 blend/双重边缘或 ELA 佐证。"""
    score = float(metrics.get("pixel_overlap_score", 0.0))
    thresh_overlap_hard = float(thresholds.get("pixel_overlap_hard_tamper", 0.72))
    thresh_overlap_absolute = float(thresholds.get("pixel_overlap_hard_tamper_absolute", 0.82))
    requires_corroboration = bool(
        thresholds.get("pixel_overlap_hard_tamper_requires_corroboration", True)
    )
    has_blend_signal = _pixel_overlap_has_blend_corroboration(metrics, thresholds)
    has_text_splice_signal = _pixel_overlap_has_text_splice_corroboration(metrics, thresholds)

    if score >= thresh_overlap_absolute and (has_blend_signal or has_text_splice_signal):
        return True
    if score < thresh_overlap_hard:
        if has_text_splice_signal and score >= float(thresholds.get("pixel_overlap_text_hard_score", 0.48)):
            signals = corroboration_signals or {}
            if not requires_corroboration:
                return True
            return bool(
                signals.get("global_fake")
                or signals.get("pixel_anomaly")
                or signals.get("font_anomaly")
                or signals.get("semantic_anomaly")
            )
        return False
    if not has_blend_signal and not has_text_splice_signal:
        return False
    if not requires_corroboration:
        return True

    signals = corroboration_signals or {}
    return bool(
        signals.get("global_fake")
        or signals.get("pixel_anomaly")
        or signals.get("font_anomaly")
        or signals.get("semantic_anomaly")
    )


def run_pixel_overlap_check(
    image_path: str,
    bbox_xyxy: Sequence[int],
    pixel_detector: PixelLevelDetector,
    *,
    thresholds: Optional[Dict[str, Any]] = None,
    margin: int = 15,
    bbox_format: str = "xyxy",
    corroboration_signals: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    """对指定 ROI 执行像素重叠规则检测。"""
    thresh = thresholds or {}

    img = safe_read_image(image_path)
    if img is None:
        raise ValueError("无法读取图片或路径不存在")

    img_h, img_w = img.shape[:2]
    x1, y1, x2, y2 = normalize_roi_bbox(bbox_xyxy, img_w, img_h, bbox_format)
    roi_expanded, bbox_xywh = crop_expanded_roi(img, [x1, y1, x2, y2], margin)

    raw_metrics = pixel_detector.overlap_metrics(roi_expanded)
    overlap_metrics = {
        "structural_score": raw_metrics["structural_score"],
        "blend_score": raw_metrics["blend_score"],
        "double_edge_ratio": raw_metrics["double_edge_ratio"],
        "long_gradient_ratio": raw_metrics["long_gradient_ratio"],
        "ela_score": raw_metrics.get("ela_score", 0.0),
        "noise_inconsistency_score": raw_metrics.get("noise_inconsistency_score", 0.0),
        "text_splice_score": raw_metrics.get("text_splice_score", 0.0),
        "pixel_overlap_score": raw_metrics["pixel_overlap_score"],
    }
    score = float(overlap_metrics["pixel_overlap_score"])
    alert = evaluate_pixel_overlap_alert(overlap_metrics, thresh)
    hard_tamper = evaluate_pixel_overlap_hard_tamper(
        overlap_metrics,
        thresh,
        corroboration_signals=corroboration_signals,
    )

    reasons: List[str] = []
    if alert:
        reasons.append("检测到疑似像素重叠/拼接痕迹")

    return {
        "pixel_overlap_score": round(score, 4),
        "overlap_metrics": {
            "structural_score": overlap_metrics["structural_score"],
            "blend_score": overlap_metrics["blend_score"],
            "double_edge_ratio": overlap_metrics["double_edge_ratio"],
            "long_gradient_ratio": overlap_metrics["long_gradient_ratio"],
            "ela_score": overlap_metrics["ela_score"],
            "noise_inconsistency_score": overlap_metrics["noise_inconsistency_score"],
            "text_splice_score": overlap_metrics["text_splice_score"],
        },
        "bbox": [int(v) for v in bbox_xywh],
        "bbox_xyxy": [x1, y1, x2, y2],
        "alert": alert,
        "hard_tamper": hard_tamper,
        "reasons": reasons,
    }


def run_timestamp_check(
    image_path: str,
    *,
    ocr_tokens: Optional[Sequence[Any]] = None,
    image_shape: Optional[Tuple[int, int, int]] = None,
    business_datetime: Optional[str] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """对图片执行时间戳规则检测（OCR 可见时间 + EXIF + 业务单据时间）。"""
    result = check_image_timestamps(
        image_path,
        ocr_tokens=ocr_tokens,
        image_shape=image_shape,
        business_datetime=business_datetime,
        thresholds=thresholds or {},
    )
    return {
        "timestamp_check": result.get("timestamp_check"),
        "risk": float(result.get("risk", 0.0)),
        "reasons": list(result.get("reasons") or []),
        "anomalies": list(result.get("anomalies") or []),
        "hard_tamper": bool(result.get("hard_tamper")),
        "business_mismatch": bool(result.get("business_mismatch")),
    }


def run_semantic_check(
    image_path: str,
    *,
    ocr_tokens: Optional[Sequence[Any]] = None,
    image_shape: Optional[Tuple[int, int, int]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """单据语义规则：金额格式、明细排版、无 EXIF 合成图信号。"""
    result = check_receipt_semantics(
        image_path,
        ocr_tokens=ocr_tokens,
        image_shape=image_shape,
        thresholds=thresholds or {},
    )
    return {
        "semantic_check": result.get("semantic_check"),
        "anomalies": list(result.get("anomalies") or []),
        "reasons": list(result.get("reasons") or []),
        "risk": float(result.get("risk", 0.0)),
        "hard_tamper": bool(result.get("hard_tamper")),
        "account_field_bbox": result.get("account_field_bbox"),
    }


def _resolve_pixel_overlap_bbox(
    bbox_xyxy: Optional[Sequence[int]],
    ocr_tokens: Optional[Sequence[Any]],
    *,
    auto_detect_account_field: bool,
) -> Optional[List[int]]:
    if bbox_xyxy is not None:
        return [int(v) for v in bbox_xyxy[:4]]
    if auto_detect_account_field and ocr_tokens:
        detected = find_labeled_field_bbox(ocr_tokens, "收款账号")
        if detected is not None:
            return detected
    return None


def run_rule_checks(
    image_path: str,
    pixel_detector: PixelLevelDetector,
    *,
    bbox_xyxy: Optional[Sequence[int]] = None,
    business_datetime: Optional[str] = None,
    ocr_tokens: Optional[Sequence[Any]] = None,
    image_shape: Optional[Tuple[int, int, int]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
    business_rules: Optional[Dict[str, Any]] = None,
    bbox_format: str = "xyxy",
    corroboration_signals: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    """聚合规则检测：像素重叠（bbox 或 OCR 自动定位）+ 语义 + 时间戳。"""
    rules = business_rules or {}
    margin = int(rules.get("roi_expand_margin", 15))
    thresh = thresholds or {}
    auto_detect = bool(rules.get("auto_detect_account_field", True))

    semantic = run_semantic_check(
        image_path,
        ocr_tokens=ocr_tokens,
        image_shape=image_shape,
        thresholds=thresh,
    )
    merged_signals = dict(corroboration_signals or {})
    if semantic.get("anomalies"):
        merged_signals["semantic_anomaly"] = True

    effective_bbox = _resolve_pixel_overlap_bbox(
        bbox_xyxy,
        ocr_tokens,
        auto_detect_account_field=auto_detect,
    )
    pixel_overlap_source = "manual_bbox" if bbox_xyxy is not None else (
        "ocr_account_field" if effective_bbox is not None else None
    )

    pixel_overlap: Optional[Dict[str, Any]] = None
    if effective_bbox is not None:
        pixel_overlap = run_pixel_overlap_check(
            image_path,
            effective_bbox,
            pixel_detector,
            thresholds=thresh,
            margin=margin,
            bbox_format=bbox_format,
            corroboration_signals=merged_signals,
        )
        if pixel_overlap_source == "ocr_account_field":
            pixel_overlap["auto_detected"] = True
            pixel_overlap["auto_detect_source"] = "收款账号"

    timestamp = run_timestamp_check(
        image_path,
        ocr_tokens=ocr_tokens,
        image_shape=image_shape,
        business_datetime=business_datetime,
        thresholds=thresh,
    )

    reasons: List[str] = []
    if pixel_overlap and pixel_overlap.get("reasons"):
        reasons.extend(pixel_overlap["reasons"])
    if semantic.get("reasons"):
        reasons.extend(semantic["reasons"])
    if timestamp.get("reasons"):
        reasons.extend(timestamp["reasons"])

    hard_tamper_flags = {
        "pixel_overlap": bool(pixel_overlap and pixel_overlap.get("hard_tamper")),
        "timestamp": bool(timestamp.get("hard_tamper")),
        "semantic": bool(semantic.get("hard_tamper")),
    }

    return {
        "pixel_overlap": pixel_overlap,
        "pixel_overlap_source": pixel_overlap_source,
        "semantic": semantic,
        "timestamp": timestamp,
        "hard_tamper_flags": hard_tamper_flags,
        "reason": "；".join(dict.fromkeys(reasons)) if reasons else "未检出明显规则类异常",
    }
