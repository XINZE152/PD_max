# -*- coding: utf-8 -*-
"""全图 OCR 工具：供同步/异步鉴伪共用，抽取时间戳等。"""
from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

import cv2
import numpy as np

from app.ai_detection.amount_candidates import OCRToken, build_amount_candidates, tokenize_ocr_results
from app.ai_detection.rule_check_roi import find_key_field_rois


def run_full_image_ocr(
    image_path: str,
    ocr_reader: Any,
) -> Tuple[Optional[np.ndarray], List[OCRToken]]:
    """对整张图片执行一次 OCR，返回 (BGR 图像, token 列表)。"""
    img_cv2 = cv2.imdecode(np.fromfile(image_path, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img_cv2 is None:
        return None, []

    gray = cv2.cvtColor(img_cv2, cv2.COLOR_BGR2GRAY)
    blurred = cv2.medianBlur(gray, 3)
    ocr_results = ocr_reader.readtext(
        blurred,
        adjust_contrast=0.5,
        mag_ratio=2.0,
        text_threshold=0.25,
    )
    return img_cv2, tokenize_ocr_results(ocr_results)


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
