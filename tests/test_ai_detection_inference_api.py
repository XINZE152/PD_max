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
        self.is_ready = True



    def search_similarity(self, query_feat):

        return self.similarity

    def search_similarity_batch(self, query_feats):

        return [self.similarity for _ in query_feats]





class _DummyPixelDetector:

    def __init__(self, score=0.1):

        self.score = score



    def detect(self, cropped_img_np, quality=85, **kwargs):

        return self.score





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

            },

            "thresholds": {

                "global_fake": 0.65,

                "pixel_anomaly_alert": 0.60,

                "exempt_pixel_safe": 0.40,

                "suspect_high": 0.65,

                "suspect_low": 0.50,

            },

        }

        engine.extractor = _DummyExtractor()

        engine.font_lib = _DummyFontLib(similarity=0.2)

        engine.global_model = _DummyGlobalModel(tamper_prob=0.1)

        engine.pixel_detector = _DummyPixelDetector(score=0.1)

        engine._origin_enabled = False

        engine._calibration_temp = 1.0

        engine._metrics = {
            "total_predictions": 0,
            "tampered_count": 0,
            "suspicious_count": 0,
            "normal_count": 0,
            "error_count": 0,
            "total_inference_time_ms": 0.0,
            "inference_times_ms": [],
        }

        return engine



    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")

    @patch("app.ai_detection.inference_api.safe_read_image")

    def test_predict_respects_xyxy_bbox_format(self, mock_safe_read_image, mock_bbox_overlap):

        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)

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

        self.assertNotIn("pixel_overlap_score", result)

        self.assertNotIn("timestamp_check", result)

        self.assertIn("bbox_overlap_check", result)

        self.assertIn("hard_tamper_flags", result)

        self.assertIn("bbox_iou", result["hard_tamper_flags"])



    @patch("app.ai_detection.inference_api.analyze_bbox_iou_overlaps")

    @patch("app.ai_detection.inference_api.safe_read_image")

    def test_predict_hard_tamper_on_bbox_iou(self, mock_safe_read_image, mock_bbox_overlap):

        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)

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

    @patch("app.ai_detection.inference_api.safe_read_image")

    def test_predict_restores_font_signal_for_numeric_core_text(self, mock_safe_read_image, mock_bbox_overlap):

        mock_safe_read_image.return_value = np.zeros((100, 100, 3), dtype=np.uint8)

        mock_bbox_overlap.return_value = {

            "bbox_overlap_check": {"max_iou": 0.0, "overlapping_pairs": [], "box_count": 0, "anomalies": []},

            "risk": 0.0,

            "reasons": [],

            "hard_tamper": False,

            "max_iou": 0.0,

        }

        engine = self._build_engine()
        engine.font_lib = _DummyFontLib(similarity=0.3)
        engine.global_model = _DummyGlobalModel(tamper_prob=0.7)
        engine.pixel_detector = _DummyPixelDetector(score=0.8)



        result = json.loads(engine.predict("/tmp/mock.jpg", [10, 20, 40, 50], bbox_format="xyxy"))



        self.assertEqual(result["result"], "可疑")

        self.assertIn("局部字体风格异常", result["reason"])

    def test_file_size_metadata_is_hard_tamper_evidence(self):
        engine = self._build_engine()

        self.assertTrue(
            engine._has_hard_metadata_evidence(
                0.50,
                ["文件体积/像素比异常(疑似工具导出)"],
            )
        )

    def test_structural_metadata_is_hard_tamper_evidence(self):
        engine = self._build_engine()

        self.assertTrue(
            engine._has_hard_metadata_evidence(
                0.55,
                ["缺少EXIF且图像结构异常(疑似生成图)", "色彩分布过于均匀(疑似生成图或纯色背景)"],
            )
        )

    def test_color_uniformity_alone_is_not_hard_tamper_evidence(self):
        engine = self._build_engine()

        self.assertFalse(
            engine._has_hard_metadata_evidence(
                0.55,
                ["色彩分布过于均匀(疑似生成图或纯色背景)"],
            )
        )





if __name__ == "__main__":

    unittest.main()
