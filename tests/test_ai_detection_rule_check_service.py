import tempfile
import unittest
from unittest.mock import MagicMock, patch

import numpy as np

from app.ai_detection.core.detectors import PixelLevelDetector
from app.ai_detection.rule_check_service import (
    crop_expanded_roi,
    evaluate_pixel_overlap_hard_tamper,
    normalize_roi_bbox,
    run_pixel_overlap_check,
    run_rule_checks,
    run_timestamp_check,
)


class RuleCheckServiceTests(unittest.TestCase):
    def test_normalize_roi_bbox_xyxy(self):
        x1, y1, x2, y2 = normalize_roi_bbox([10, 20, 110, 80], 200, 200, "xyxy")
        self.assertEqual([x1, y1, x2, y2], [10, 20, 110, 80])

    def test_crop_expanded_roi_respects_margin(self):
        img = np.zeros((100, 100, 3), dtype=np.uint8)
        roi, bbox_xywh = crop_expanded_roi(img, [20, 20, 60, 60], margin=10)
        self.assertEqual(bbox_xywh, [20, 20, 40, 40])
        self.assertEqual(roi.shape[0], 60)
        self.assertEqual(roi.shape[1], 60)

    def test_evaluate_pixel_overlap_hard_tamper_absolute(self):
        hard = evaluate_pixel_overlap_hard_tamper(
            0.90,
            {
                "pixel_overlap_hard_tamper": 0.72,
                "pixel_overlap_hard_tamper_absolute": 0.82,
                "pixel_overlap_hard_tamper_requires_corroboration": True,
            },
        )
        self.assertTrue(hard)

    def test_evaluate_pixel_overlap_requires_corroboration(self):
        thresholds = {
            "pixel_overlap_hard_tamper": 0.72,
            "pixel_overlap_hard_tamper_absolute": 0.82,
            "pixel_overlap_hard_tamper_requires_corroboration": True,
        }
        self.assertFalse(
            evaluate_pixel_overlap_hard_tamper(0.75, thresholds, corroboration_signals={})
        )
        self.assertTrue(
            evaluate_pixel_overlap_hard_tamper(
                0.75,
                thresholds,
                corroboration_signals={"pixel_anomaly": True},
            )
        )

    @patch("app.ai_detection.rule_check_service.safe_read_image")
    def test_run_pixel_overlap_check_returns_score(self, mock_read):
        mock_read.return_value = np.full((80, 120, 3), 180, dtype=np.uint8)
        detector = PixelLevelDetector()
        result = run_pixel_overlap_check(
            "dummy.jpg",
            [10, 10, 70, 50],
            detector,
            thresholds={"pixel_overlap_alert": 0.55},
            margin=5,
        )
        self.assertIn("pixel_overlap_score", result)
        self.assertEqual(len(result["bbox"]), 4)
        self.assertIn("alert", result)

    @patch("app.ai_detection.rule_check_service.check_image_timestamps")
    def test_run_timestamp_check_wraps_checker(self, mock_check):
        mock_check.return_value = {
            "timestamp_check": {"status_bar_time": "11:32", "anomalies": []},
            "risk": 0.1,
            "reasons": [],
            "anomalies": [],
            "hard_tamper": False,
            "business_mismatch": False,
        }
        result = run_timestamp_check("dummy.jpg", ocr_tokens=[], image_shape=(100, 100, 3))
        self.assertEqual(result["risk"], 0.1)
        self.assertFalse(result["hard_tamper"])
        mock_check.assert_called_once()

    @patch("app.ai_detection.rule_check_service.run_timestamp_check")
    @patch("app.ai_detection.rule_check_service.run_pixel_overlap_check")
    def test_run_rule_checks_aggregates(self, mock_pixel, mock_ts):
        mock_pixel.return_value = {
            "pixel_overlap_score": 0.2,
            "reasons": [],
            "hard_tamper": False,
        }
        mock_ts.return_value = {
            "timestamp_check": {},
            "risk": 0.0,
            "reasons": [],
            "anomalies": [],
            "hard_tamper": False,
            "business_mismatch": False,
        }
        detector = MagicMock()
        result = run_rule_checks(
            "dummy.jpg",
            detector,
            bbox_xyxy=[0, 0, 10, 10],
            ocr_tokens=[],
            image_shape=(10, 10, 3),
        )
        self.assertIsNotNone(result["pixel_overlap"])
        self.assertIsNotNone(result["timestamp"])
        self.assertIn("hard_tamper_flags", result)

    @patch("app.ai_detection.rule_check_service.run_timestamp_check")
    def test_run_rule_checks_without_bbox(self, mock_ts):
        mock_ts.return_value = {
            "timestamp_check": {},
            "risk": 0.0,
            "reasons": [],
            "anomalies": [],
            "hard_tamper": False,
            "business_mismatch": False,
        }
        result = run_rule_checks("dummy.jpg", MagicMock(), ocr_tokens=[], image_shape=(10, 10, 3))
        self.assertIsNone(result["pixel_overlap"])
        mock_ts.assert_called_once()


if __name__ == "__main__":
    unittest.main()
