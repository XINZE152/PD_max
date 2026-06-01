import os
import unittest

import cv2
import numpy as np

from app.ai_detection.core.detectors import PixelLevelDetector
from app.ai_detection.rule_check_service import crop_expanded_roi, run_pixel_overlap_check


class PixelOverlapDetectorTests(unittest.TestCase):
    def test_detect_overlap_on_edge_band_difference(self):
        detector = PixelLevelDetector()
        uniform = np.full((120, 160, 3), 200, dtype=np.uint8)
        edged = uniform.copy()
        edged[:, :18] = 40
        edged[:, -18:] = 40

        uniform_score = detector.detect_overlap(uniform)
        edged_score = detector.detect_overlap(edged)

        self.assertGreater(edged_score, uniform_score)
        self.assertGreaterEqual(uniform_score, 0.0)
        self.assertLessEqual(edged_score, 1.0)

    def test_alpha_blend_overlap_score_formula(self):
        detector = PixelLevelDetector()
        tamper_like = detector._alpha_blend_overlap_score(0.148, 0.189)
        normal_like = detector._alpha_blend_overlap_score(0.034, 0.135)
        self.assertGreater(tamper_like, 0.55)
        self.assertLess(normal_like, 0.15)

    def test_overlap_metrics_keys(self):
        detector = PixelLevelDetector()
        img = np.random.randint(0, 255, (80, 120, 3), dtype=np.uint8)
        metrics = detector.overlap_metrics(img)
        self.assertIn("structural_score", metrics)
        self.assertIn("blend_score", metrics)
        self.assertIn("double_edge_ratio", metrics)
        self.assertIn("text_splice_score", metrics)
        self.assertIn("ela_score", metrics)
        self.assertIn("pixel_overlap_score", metrics)


class PixelOverlapIntegrationTests(unittest.TestCase):
    """在三张业务/测试图上回归（资产存在时运行）。"""

    _ASSET_DIR = (
        r"C:\Users\HP\.cursor\projects\e-PD-MAX-PD-max\assets"
    )

    @classmethod
    def setUpClass(cls):
        cls.cases = []
        mapping = [
            ("remit", "d9e387645a2d46d4964aeb8eff2d98c5", [30, 200, 430, 380], False),
            ("receipt", "de9ba710bd3e4182b347e9ce05fe6806", [200, 350, 460, 580], True),
            ("tamper", "image-682db522-5774-4206-b47e-11364363c318", [122, 112, 528, 418], True),
        ]
        for name, token, bbox, should_alert in mapping:
            path = None
            if os.path.isdir(cls._ASSET_DIR):
                for fname in os.listdir(cls._ASSET_DIR):
                    if token in fname and fname.lower().endswith((".png", ".jpg", ".jpeg")):
                        path = os.path.join(cls._ASSET_DIR, fname)
                        break
            if path and os.path.isfile(path):
                cls.cases.append((name, path, bbox, should_alert))

    def test_three_image_regression(self):
        if len(self.cases) < 3:
            self.skipTest("测试图片资产不存在，跳过集成回归")

        detector = PixelLevelDetector()
        for name, path, bbox, should_alert in self.cases:
            result = run_pixel_overlap_check(path, bbox, detector)
            self.assertIn("overlap_metrics", result, msg=name)
            if should_alert:
                self.assertTrue(
                    result["alert"],
                    msg=f"{name} score={result['pixel_overlap_score']} metrics={result['overlap_metrics']}",
                )
            else:
                self.assertFalse(
                    result["alert"],
                    msg=f"{name} score={result['pixel_overlap_score']} metrics={result['overlap_metrics']}",
                )


if __name__ == "__main__":
    unittest.main()
