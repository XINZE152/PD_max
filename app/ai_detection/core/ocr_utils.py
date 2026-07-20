# -*- coding: utf-8 -*-
"""全图 OCR 工具：供同步/异步鉴伪共用，抽取时间戳等。"""
from __future__ import annotations

import os
from typing import Any, List, Optional, Sequence, Tuple

import cv2
import numpy as np

from app.ai_detection.core.amount_candidates import OCRToken, build_amount_candidates, tokenize_ocr_results
from app.ai_detection.core.rule_check_roi import find_key_field_rois

OCR_MAX_SIDE = max(1, int(os.getenv("AI_OCR_MAX_SIDE", "2200") or "2200"))
OCR_MAX_PIXELS = max(1, int(os.getenv("AI_OCR_MAX_PIXELS", "4000000") or "4000000"))
OCR_MAG_RATIO = max(1.0, float(os.getenv("AI_OCR_MAG_RATIO", "1.5") or "1.5"))
OCR_MIN_SHORT_SIDE = max(0, int(os.getenv("AI_OCR_MIN_SHORT_SIDE", "1100") or "1100"))


def _resize_for_ocr(
    img_cv2: np.ndarray,
    *,
    max_side: int = OCR_MAX_SIDE,
    max_pixels: int = OCR_MAX_PIXELS,
    min_short_side: int = 0,
) -> Tuple[np.ndarray, float]:
    """Scale images within OCR memory limits, optionally enlarging small text."""
    h, w = img_cv2.shape[:2]
    if h <= 0 or w <= 0:
        return img_cv2, 1.0

    scale = 1.0
    shortest = min(h, w)
    if min_short_side:
        if shortest < min_short_side:
            scale = min_short_side / float(shortest)

    longest = max(h, w)
    pixels = h * w
    max_safe_scale = min(
        max_side / float(longest),
        (max_pixels / float(pixels)) ** 0.5,
    )
    scale = min(scale, max_safe_scale)

    if abs(scale - 1.0) < 0.001:
        return img_cv2, 1.0

    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    interpolation = cv2.INTER_CUBIC if scale > 1.0 else cv2.INTER_AREA
    return cv2.resize(img_cv2, (new_w, new_h), interpolation=interpolation), scale


def _scale_ocr_results_to_original(
    ocr_results: Sequence[Tuple[Sequence[Sequence[float]], str, float]],
    *,
    scale: float,
    original_shape: Tuple[int, int, int],
) -> List[Tuple[List[List[float]], str, float]]:
    if abs(scale - 1.0) < 0.001:
        return [(list(map(list, bbox)), text, conf) for bbox, text, conf in ocr_results]

    h, w = original_shape[:2]
    inv_scale = 1.0 / max(scale, 1e-6)
    scaled: List[Tuple[List[List[float]], str, float]] = []
    for bbox, text, conf in ocr_results:
        points: List[List[float]] = []
        for point in bbox:
            if len(point) < 2:
                continue
            x = min(max(float(point[0]) * inv_scale, 0.0), max(float(w - 1), 0.0))
            y = min(max(float(point[1]) * inv_scale, 0.0), max(float(h - 1), 0.0))
            points.append([x, y])
        if points:
            scaled.append((points, text, conf))
    return scaled


def run_full_image_ocr(
    image_path: str,
    ocr_reader: Any,
) -> Tuple[Optional[np.ndarray], List[OCRToken]]:
    """对整张图片执行一次 OCR，返回 (BGR 图像, token 列表)。"""
    img_cv2 = cv2.imdecode(np.fromfile(image_path, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img_cv2 is None:
        return None, []

    ocr_img, scale = _resize_for_ocr(
        img_cv2,
        min_short_side=OCR_MIN_SHORT_SIDE,
    )
    gray = cv2.cvtColor(ocr_img, cv2.COLOR_BGR2GRAY)
    blurred = cv2.medianBlur(gray, 3)
    ocr_results = ocr_reader.readtext(
        blurred,
        adjust_contrast=0.5,
        mag_ratio=OCR_MAG_RATIO,
        text_threshold=0.25,
    )
    original_results = _scale_ocr_results_to_original(
        ocr_results,
        scale=scale,
        original_shape=img_cv2.shape,
    )
    return img_cv2, tokenize_ocr_results(original_results)


def build_detection_bboxes_from_tokens(
    tokens: Sequence[OCRToken],
    image_shape: Tuple[int, int, int],
) -> List[List[int]]:
    """从 OCR token 构建金额/数字候选框列表，供 IoU 重叠鉴伪使用。"""
    return [list(candidate.bbox) for candidate in build_amount_candidates(tokens, image_shape)]


def build_key_field_rois_from_tokens(
    tokens: Sequence[OCRToken],
    image_shape: Tuple[int, int, int],
) -> List[dict]:
    """从 OCR token 构建 v3 自动检测框：金额、姓名、时间。"""
    return find_key_field_rois(tokens, image_shape)
