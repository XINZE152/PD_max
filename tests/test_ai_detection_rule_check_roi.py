# -*- coding: utf-8 -*-
import unittest

from app.ai_detection.amount_candidates import OCRToken
from app.ai_detection.rule_check_roi import (
    find_high_risk_pixel_rois,
    rule_checks_need_auto_pixel_rescan,
)
from app.ai_detection.rule_check_service import merge_pixel_overlap_results


class RuleCheckRoiTests(unittest.TestCase):
    def test_rule_checks_need_auto_pixel_rescan_when_no_findings(self):
        self.assertTrue(
            rule_checks_need_auto_pixel_rescan(
                manual_bbox=None,
                semantic={"hard_tamper": False},
                timestamp={"hard_tamper": False, "anomalies": ["status_transaction_time_mismatch"]},
                pixel_overlap=None,
                business_rules={"auto_detect_high_risk_rois": True},
            )
        )
        self.assertFalse(
            rule_checks_need_auto_pixel_rescan(
                manual_bbox=[1, 2, 3, 4],
                semantic={"hard_tamper": False},
                timestamp={"hard_tamper": False},
                pixel_overlap=None,
            )
        )
        self.assertFalse(
            rule_checks_need_auto_pixel_rescan(
                manual_bbox=None,
                semantic={"hard_tamper": False},
                timestamp={"hard_tamper": False},
                pixel_overlap={"alert": True},
            )
        )

    def test_find_high_risk_pixel_rois_includes_center_amount(self):
        image_shape = (2400, 1080, 3)
        tokens = [
            OCRToken(
                text="6,000.00",
                clean_text="6,000.00",
                bbox=(120, 520, 960, 620),
                conf=0.95,
                width=840,
                height=100,
                center_y=570.0,
            ),
            OCRToken(
                text="对方账户",
                clean_text="对方账户",
                bbox=(80, 900, 220, 940),
                conf=0.9,
                width=140,
                height=40,
                center_y=920.0,
            ),
            OCRToken(
                text="156******27",
                clean_text="156******27",
                bbox=(700, 900, 980, 940),
                conf=0.88,
                width=280,
                height=40,
                center_y=920.0,
            ),
        ]
        rois = find_high_risk_pixel_rois(tokens, image_shape)
        sources = {item["source"] for item in rois}
        self.assertTrue(any(src.startswith("amount_") for src in sources))

    def test_merge_pixel_overlap_results_takes_max_alert(self):
        merged = merge_pixel_overlap_results(
            {"pixel_overlap_score": 0.2, "alert": False, "bbox_xyxy": [0, 0, 10, 10]},
            [
                {
                    "pixel_overlap_score": 0.61,
                    "alert": True,
                    "hard_tamper": False,
                    "bbox_xyxy": [1, 2, 3, 4],
                    "reasons": ["检测到疑似像素重叠/拼接痕迹"],
                    "auto_label": "6,000.00",
                }
            ],
        )
        self.assertTrue(merged["alert"])
        self.assertEqual(merged["pixel_overlap_score"], 0.61)
        self.assertEqual(len(merged["auto_scan_regions"]), 2)


if __name__ == "__main__":
    unittest.main()
