import cv2
import json
import yaml
import numpy as np
import logging
import os
import joblib
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.ai_detection.bbox_overlap_checker import analyze_bbox_iou_overlaps
from app.ai_detection.core.extractors import FeatureExtractor, FontFeatureLibrary, TamperAnalyzer
from app.ai_detection.core.detectors import PixelLevelDetector
from app.ai_detection.core.utils import NumpyEncoder, safe_read_image
from app.ai_detection.rule_check_service import crop_expanded_roi, normalize_roi_bbox

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InferenceEngineAPI:
    def __init__(self, config_path="config.yaml", shared_ocr_reader: Optional[Any] = None):
        """
        :param shared_ocr_reader: 与路由层共用的 easyocr.Reader；传入则 FeatureExtractor 不再单独 new 一份（显著降低内存）。
        """
        config_file = Path(config_path)
        if not config_file.is_absolute():
            config_file = (Path(__file__).resolve().parent / config_file).resolve()

        with open(config_file, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.base_dir = config_file.parent

        self.extractor = FeatureExtractor(reader=shared_ocr_reader)
        self.font_lib = FontFeatureLibrary()
        font_lib_path = self._resolve_path(self.config['paths']['font_lib_path'])
        self.font_lib.load(font_lib_path)

        xgb_path = self.config.get('paths', {}).get('xgb_model_path', "models/global_layout_model.pkl")
        self.global_model = joblib.load(self._resolve_path(xgb_path))
        self.pixel_detector = PixelLevelDetector()

    def _resolve_path(self, path_str: str) -> str:
        path = Path(path_str)
        if path.is_absolute():
            return str(path)
        return str((self.base_dir / path).resolve())

    @staticmethod
    def _profile_numeric_text(extracted_text: str, max_len: int) -> Dict[str, float]:
        text_clean = extracted_text.replace(" ", "")
        total_len = len(text_clean)
        digit_count = len(re.findall(r"\d", text_clean))
        digit_ratio = (digit_count / total_len) if total_len else 0.0

        amount_pattern = re.search(r"\d[\d,]*[.:]\d{1,2}", text_clean)
        currency_hint = re.search(r"(小写|金额|元|¥|￥|人民币)", text_clean)
        order_hint = re.search(r"(单号|订单|流水|凭证|参考号)", text_clean)

        is_core_candidate = digit_count >= 3 and (
            digit_ratio >= 0.35 or amount_pattern is not None or currency_hint is not None or order_hint is not None
        )
        should_use_font_signal = is_core_candidate or (digit_count >= 3 and total_len <= max_len * 2)

        return {
            "digit_count": digit_count,
            "digit_ratio": digit_ratio,
            "total_len": total_len,
            "is_core_candidate": float(is_core_candidate),
            "should_use_font_signal": float(should_use_font_signal),
        }

    def predict(
        self,
        full_image_path: str,
        roi_bbox: List[int],
        bbox_format: str = "auto",
        detection_bboxes: Optional[List[List[int]]] = None,
    ) -> str:
        try:
            reasons = []
            result_status = "正常"

            img = safe_read_image(full_image_path)
            if img is None:
                return json.dumps({"result": "错误", "reason": "无法读取图片或路径不存在"}, ensure_ascii=False)

            img_h, img_w = img.shape[:2]

            rules = self.config.get('business_rules', {})
            weights = self.config.get('weights', {})
            thresh = self.config.get('thresholds', {})

            margin = rules.get('roi_expand_margin', 15)
            max_len = rules.get('max_core_text_length', 15)

            thresh_global = thresh.get('global_fake', 0.65)
            thresh_pixel_alert = thresh.get('pixel_anomaly_alert', 0.60)
            thresh_exempt = thresh.get('exempt_pixel_safe', 0.40)
            thresh_high = thresh.get('suspect_high', 0.65)
            thresh_low = thresh.get('suspect_low', 0.50)

            x1, y1, x2, y2 = normalize_roi_bbox(roi_bbox, img_w, img_h, bbox_format)
            x, y = x1, y1
            w, h = x2 - x1, y2 - y1

            global_feat = self.extractor.extract_global_feature(img)
            global_fake_prob = float(self.global_model.predict_proba(np.array([global_feat]))[0][1])

            roi_img = img[y:y + h, x:x + w]
            roi_img_expanded, _bbox_xywh = crop_expanded_roi(img, [x1, y1, x2, y2], margin)

            roi_rgb = cv2.cvtColor(roi_img, cv2.COLOR_BGR2RGB)
            feats, stats = self.extractor.extract_from_roi(roi_rgb)

            feature_texts = [s['text'] for s in stats if s.get('is_core_number')]
            extracted_text = "".join(feature_texts) if feature_texts else "".join([s['text'] for s in stats])
            text_profile = self._profile_numeric_text(extracted_text, max_len)
            should_use_font_signal = bool(text_profile["should_use_font_signal"])

            font_sim = np.mean([self.font_lib.search_similarity(f) for f in feats]) if feats else 0.5
            font_anomaly = max(0.0, 1.0 - font_sim)

            pixel_anomaly = self.pixel_detector.detect(roi_img_expanded)
            geo_reasons, geo_penalty = TamperAnalyzer.check_internal_consistency(stats)

            bbox_overlap_result = analyze_bbox_iou_overlaps(
                detection_bboxes or [],
                roi_bbox_xyxy=[x1, y1, x2, y2],
                thresholds=thresh,
            )
            bbox_iou_risk = float(bbox_overlap_result.get("risk", 0.0))
            bbox_iou_hard_tamper = bool(bbox_overlap_result.get("hard_tamper"))

            if should_use_font_signal and len(extracted_text) > 0:
                local_tamper_prob = (
                    pixel_anomaly * weights.get('core_pixel', 0.6)
                ) + (
                    font_anomaly * weights.get('core_font', 0.4)
                ) + geo_penalty

                if text_profile["digit_count"] >= 8 and font_anomaly > 0.75:
                    local_tamper_prob = max(local_tamper_prob, thresh_low + 0.02)
            else:
                local_tamper_prob = (pixel_anomaly * weights.get('non_core_pixel', 0.8)) + geo_penalty
                if pixel_anomaly < thresh_exempt and geo_penalty == 0:
                    local_tamper_prob = 0.0

            final_risk = max(global_fake_prob, local_tamper_prob, bbox_iou_risk)
            final_risk = max(0.0, min(1.0, float(final_risk)))

            if global_fake_prob > thresh_global:
                reasons.append("全局UI布局异常")
            if pixel_anomaly > thresh_pixel_alert:
                reasons.append("存在局部边缘拼接/像素涂抹痕迹")
            if bbox_overlap_result.get("reasons"):
                reasons.extend(bbox_overlap_result["reasons"])
            if should_use_font_signal and font_anomaly > 0.55:
                reasons.append("局部字体风格异常")
            if geo_penalty > 0:
                reasons.extend(geo_reasons)

            if bbox_iou_hard_tamper:
                result_status = "篡改"
                final_risk = max(final_risk, float(thresh_high) + 0.05)
            elif final_risk > thresh_high:
                result_status = "篡改"
            elif final_risk > thresh_low:
                result_status = "可疑"
            else:
                if not reasons:
                    reasons.append("未检出明显篡改痕迹")

            output = {
                "result": result_status,
                "confidence": final_risk,
                "bbox": [int(i) for i in [x, y, w, h]],
                "reason": "；".join(dict.fromkeys(reasons)),
                "bbox_overlap_check": bbox_overlap_result.get("bbox_overlap_check"),
                "hard_tamper_flags": {
                    "bbox_iou": bbox_iou_hard_tamper,
                },
            }
            return json.dumps(output, ensure_ascii=False, indent=4, cls=NumpyEncoder)

        except Exception as e:
            logger.error(f"引擎推理引发未捕获异常: {e}", exc_info=True)
            error_output = {
                "result": "错误",
                "confidence": 0.0,
                "bbox": roi_bbox,
                "reason": f"引擎内部解析失败: {str(e)}"
            }
            return json.dumps(error_output, ensure_ascii=False, indent=4, cls=NumpyEncoder)


if __name__ == "__main__":
    import time

    logger.info("启动单图推理本地测试 (Inference API)")

    try:
        engine = InferenceEngineAPI(config_path=str(Path(__file__).resolve().parent / "config.yaml"))
        logger.info("引擎初始化成功")
    except Exception as e:
        logger.error(f"引擎初始化失败: {e}", exc_info=True)
        exit(1)

    test_image_path = "pptest/111.png"
    test_bbox = [150, 200, 180, 45]

    if not os.path.exists(test_image_path):
        logger.warning(f"找不到测试图片: {test_image_path}，请修改路径后重试。")
    else:
        logger.info(f"目标图片: {test_image_path} | BBox: {test_bbox}")
        start_time = time.time()
        try:
            result_json = engine.predict(full_image_path=test_image_path, roi_bbox=test_bbox)
            cost_time = time.time() - start_time
            logger.info(f"推理耗时: {cost_time:.3f} 秒")
            logger.info(f"返回结果:\n{result_json}")
        except Exception as e:
            logger.error(f"推理过程中发生错误: {e}", exc_info=True)
