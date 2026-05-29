import json
import unittest
from unittest.mock import patch

import numpy as np

from app.ai_detection.inference_api import InferenceEngineAPI


class _DummyExtractor:
    def __init__(self, stats=None):
        self._stats = stats or [
            {
                "text": "12345678",
                "bbox": [0, 0, 10, 10],
                "conf": 0.99,
                "is_core_number": True,
            }
        ]

    def extract_global_feature(self, img_np):
        return np.zeros(512, dtype=np.float32)

    def extract_from_roi(self, roi_rgb):
        return [np.zeros(512, dtype=np.float32)], list(self._stats)


class _DummyGlobalModel:
    def __init__(self, tamper_prob=0.1):
        self.tamper_prob = tamper_prob

    def predict_proba(self, values):
        return np.array([[1.0 - self.tamper_prob, self.tamper_prob]], dtype=float)


class _DummyFontLib:
    def __init__(self, similarity=0.2):
        self.similarity = similarity

    def search_similarity(self, query_feat):
        return self.similarity


class _DummyPixelDetector:
    def __init__(self, score=0.1, overlap_score=0.1):
        self.score = score
        self.overlap_score = overlap_score

    def detect(self, cropped_img_np, quality=85):
        return self.score

    def detect_overlap(self, cropped_img_np, band_ratio=0.08, min_band=4):
        return self.overlap_score


class InferenceEngineApiTests(unittest.TestCase):
    def _build_engine(self):
        engine = InferenceEngineAPI.__new__(InferenceEngineAPI)
        engine.config = {
            "business_rules": {
                "roi_expand_margin": 15,
                "max_core_text_length": 15,
            },
            "weights": {
                "core_pixel": 0.60,
                "core_font": 0.40,
                "non_core_pixel": 0.80,
                "pixel_overlap": 0.30,
            },
            "thresholds": {
                "global_fake": 0.65,
                "pixel_anomaly_alert": 0.60,
                "pixel_overlap_alert": 0.55,
                "exempt_pixel_safe": 0.40,
                "suspect_high": 0.65,
                "suspect_low": 0.50,
                "pixel_overlap_hard_tamper": 0.72,
                "pixel_overlap_hard_tamper_absolute": 0.82,
                "pixel_overlap_hard_tamper_requires_corroboration": True,
            },
        }
        engine.extractor = _DummyExtractor()
        engine.font_lib = _DummyFontLib(similarity=0.2)
        engine.global_model = _DummyGlobalModel(tamper_prob=0.1)
        engine.pixel_detector = _DummyPixelDetector(score=0.1)
        return engine

    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")
    @patch("app.ai_detection.inference_api.check_image_timestamps")
    @patch("app.ai_detection.inference_api.safe_read_image")
    def test_predict_respects_xyxy_bbox_format(self, mock_safe_read_image, mock_timestamp_check, mock_bbox_overlap):
        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        mock_timestamp_check.return_value = {
            "timestamp_check": {"anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "anomalies": [],
            "hard_tamper": False,
        }
        mock_bbox_overlap.return_value = {
            "bbox_overlap_check": {"max_iou": 0.0, "overlapping_pairs": [], "box_count": 0, "anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "hard_tamper": False,
            "max_iou": 0.0,
        }
        engine = self._build_engine()

        result = json.loads(engine.predict("/tmp/mock.jpg", [10, 20, 40, 50], bbox_format="xyxy"))

        self.assertEqual(result["bbox"], [10, 20, 30, 30])
        self.assertIn("pixel_overlap_score", result)
        self.assertIn("timestamp_check", result)
        self.assertIn("hard_tamper_flags", result)

        self.assertIn("bbox_overlap_check", result)

    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")
    @patch("app.ai_detection.inference_api.check_image_timestamps")
    @patch("app.ai_detection.inference_api.safe_read_image")
    def test_predict_hard_tamper_on_timestamp(self, mock_safe_read_image, mock_timestamp_check, mock_bbox_overlap):
        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        mock_timestamp_check.return_value = {
            "timestamp_check": {"anomalies": ["status_transaction_time_mismatch"]},
            "risk": 0.58,
            "reasons": ["状态栏时间与交易时间不一致"],
            "anomalies": ["status_transaction_time_mismatch"],
            "hard_tamper": True,
        }
        mock_bbox_overlap.return_value = {
            "bbox_overlap_check": {"max_iou": 0.0, "overlapping_pairs": [], "box_count": 0, "anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "hard_tamper": False,
            "max_iou": 0.0,
        }
        engine = self._build_engine()
        engine.pixel_detector = _DummyPixelDetector(score=0.1, overlap_score=0.1)

        result = json.loads(engine.predict("/tmp/mock.jpg", [10, 20, 40, 50], bbox_format="xyxy"))

        self.assertEqual(result["result"], "篡改")
        self.assertTrue(result["hard_tamper_flags"]["timestamp"])

    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")
    @patch("app.ai_detection.inference_api.check_image_timestamps")
    @patch("app.ai_detection.inference_api.safe_read_image")
    def test_predict_business_timestamp_mismatch_is_suspect_not_tamper(
        self, mock_safe_read_image, mock_timestamp_check, mock_bbox_overlap
    ):
        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        mock_timestamp_check.return_value = {
            "timestamp_check": {
                "anomalies": ["business_visible_datetime_mismatch"],
                "business_mismatch": True,
            },
            "risk": 0.58,
            "reasons": ["业务单据时间与图片可见交易时间不一致"],
            "anomalies": ["business_visible_datetime_mismatch"],
            "hard_tamper": False,
            "business_mismatch": True,
        }
        mock_bbox_overlap.return_value = {
            "bbox_overlap_check": {"max_iou": 0.0, "overlapping_pairs": [], "box_count": 0, "anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "hard_tamper": False,
            "max_iou": 0.0,
        }
        engine = self._build_engine()
        engine.pixel_detector = _DummyPixelDetector(score=0.1, overlap_score=0.1)

        result = json.loads(
            engine.predict(
                "/tmp/mock.jpg",
                [10, 20, 40, 50],
                bbox_format="xyxy",
                business_datetime="2026-05-28 11:32:00",
            )
        )

        self.assertEqual(result["result"], "可疑")
        self.assertFalse(result["hard_tamper_flags"]["timestamp"])
        self.assertTrue(result["timestamp_check"]["business_mismatch"])

    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")
    @patch("app.ai_detection.inference_api.check_image_timestamps")
    @patch("app.ai_detection.inference_api.safe_read_image")
    def test_predict_pixel_overlap_requires_corroboration(self, mock_safe_read_image, mock_timestamp_check, mock_bbox_overlap):
        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        mock_timestamp_check.return_value = {
            "timestamp_check": {"anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "anomalies": [],
            "hard_tamper": False,
        }
        mock_bbox_overlap.return_value = {
            "bbox_overlap_check": {"max_iou": 0.0, "overlapping_pairs": [], "box_count": 0, "anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "hard_tamper": False,
            "max_iou": 0.0,
        }
        engine = self._build_engine()
        engine.global_model = _DummyGlobalModel(tamper_prob=0.1)
        engine.font_lib = _DummyFontLib(similarity=0.95)
        engine.pixel_detector = _DummyPixelDetector(score=0.1, overlap_score=0.76)

        result = json.loads(engine.predict("/tmp/mock.jpg", [10, 20, 40, 50], bbox_format="xyxy"))

        self.assertFalse(result["hard_tamper_flags"]["pixel_overlap"])
        self.assertNotEqual(result["result"], "篡改")

    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")
    @patch("app.ai_detection.inference_api.check_image_timestamps")
    @patch("app.ai_detection.inference_api.safe_read_image")
    def test_predict_pixel_overlap_with_global_fake_still_hard_tamper(
        self, mock_safe_read_image, mock_timestamp_check, mock_bbox_overlap
    ):
        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        mock_timestamp_check.return_value = {
            "timestamp_check": {"anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "anomalies": [],
            "hard_tamper": False,
        }
        mock_bbox_overlap.return_value = {
            "bbox_overlap_check": {"max_iou": 0.0, "overlapping_pairs": [], "box_count": 0, "anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "hard_tamper": False,
            "max_iou": 0.0,
        }
        engine = self._build_engine()
        engine.global_model = _DummyGlobalModel(tamper_prob=0.9)
        engine.pixel_detector = _DummyPixelDetector(score=0.1, overlap_score=0.76)

        result = json.loads(engine.predict("/tmp/mock.jpg", [10, 20, 40, 50], bbox_format="xyxy"))

        self.assertTrue(result["hard_tamper_flags"]["pixel_overlap"])
        self.assertEqual(result["result"], "篡改")

    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")
    @patch("app.ai_detection.inference_api.check_image_timestamps")
    @patch("app.ai_detection.inference_api.safe_read_image")
    def test_predict_hard_tamper_on_bbox_iou(self, mock_safe_read_image, mock_timestamp_check, mock_bbox_overlap):
        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        mock_timestamp_check.return_value = {
            "timestamp_check": {"anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "anomalies": [],
            "hard_tamper": False,
        }
        mock_bbox_overlap.return_value = {
            "bbox_overlap_check": {"max_iou": 0.85, "overlapping_pairs": [], "box_count": 2, "anomalies": ["bbox_iou_hard_overlap"]},
            "risk": 0.82,
            "reasons": ["检测到多个高度重叠的疑似数字区域(疑似复制贴图)"],
            "hard_tamper": True,
            "max_iou": 0.85,
        }
        engine = self._build_engine()

        result = json.loads(engine.predict("/tmp/mock.jpg", [10, 20, 40, 50], bbox_format="xyxy"))

        self.assertEqual(result["result"], "篡改")
        self.assertTrue(result["hard_tamper_flags"]["bbox_iou"])

    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")
    @patch("app.ai_detection.inference_api.check_image_timestamps")
    @patch("app.ai_detection.inference_api.safe_read_image")
    def test_predict_restores_font_signal_for_numeric_core_text(self, mock_safe_read_image, mock_timestamp_check, mock_bbox_overlap):
        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        mock_timestamp_check.return_value = {
            "timestamp_check": {"anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "anomalies": [],
            "hard_tamper": False,
        }
        mock_bbox_overlap.return_value = {
            "bbox_overlap_check": {"max_iou": 0.0, "overlapping_pairs": [], "box_count": 0, "anomalies": []},
            "risk": 0.0,
            "reasons": [],
            "hard_tamper": False,
            "max_iou": 0.0,
        }
        engine = self._build_engine()

        result = json.loads(engine.predict("/tmp/mock.jpg", [10, 20, 40, 50], bbox_format="xyxy"))

        self.assertEqual(result["result"], "可疑")
        self.assertIn("局部字体风格异常", result["reason"])


if __name__ == "__main__":
    unittest.main()
