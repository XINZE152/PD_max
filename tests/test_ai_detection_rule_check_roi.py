# -*- coding: utf-8 -*-
import unittest

from app.ai_detection.amount_candidates import OCRToken
from app.ai_detection.rule_check_roi import (
    find_key_field_rois,
    find_high_risk_pixel_rois,
    find_suggested_rois,
    rule_checks_need_auto_pixel_rescan,
)
from app.ai_detection.rule_check_service import merge_pixel_overlap_results


class RuleCheckRoiTests(unittest.TestCase):
    def test_find_key_field_rois_only_amount_name_time(self):
        image_shape = (1200, 800, 3)
        tokens = [
            OCRToken("转账金额", "转账金额", (40, 100, 140, 130), 0.96, 100, 30, 115.0),
            OCRToken("6,000.00", "6,000.00", (300, 100, 430, 130), 0.95, 130, 30, 115.0),
            OCRToken("收款人", "收款人", (40, 180, 120, 210), 0.96, 80, 30, 195.0),
            OCRToken("张三", "张三", (300, 180, 360, 210), 0.95, 60, 30, 195.0),
            OCRToken("交易时间", "交易时间", (40, 260, 140, 290), 0.96, 100, 30, 275.0),
            OCRToken("2026-06-01 12:30:00", "2026-06-0112:30:00", (300, 260, 520, 290), 0.95, 220, 30, 275.0),
            OCRToken("订单号", "订单号", (40, 340, 120, 370), 0.96, 80, 30, 355.0),
            OCRToken("NO123456", "NO123456", (300, 340, 420, 370), 0.95, 120, 30, 355.0),
            OCRToken("收款账号", "收款账号", (40, 420, 140, 450), 0.96, 100, 30, 435.0),
            OCRToken("6222****8888", "6222****8888", (300, 420, 460, 450), 0.95, 160, 30, 435.0),
        ]

        rois = find_key_field_rois(tokens, image_shape)

        self.assertEqual([roi["field_type"] for roi in rois[:3]], ["amount", "name", "time"])
        self.assertNotIn("账号", {roi.get("field_label") for roi in rois})
        self.assertNotIn("单号", {roi.get("field_label") for roi in rois})

    def test_find_key_field_rois_keeps_multiple_same_field_candidates(self):
        image_shape = (1200, 800, 3)
        tokens = [
            OCRToken("转账金额", "转账金额", (40, 100, 140, 130), 0.96, 100, 30, 115.0),
            OCRToken("6,000.00", "6,000.00", (300, 100, 430, 130), 0.95, 130, 30, 115.0),
            OCRToken("交易金额", "交易金额", (40, 180, 140, 210), 0.96, 100, 30, 195.0),
            OCRToken("8,000.00", "8,000.00", (300, 180, 430, 210), 0.95, 130, 30, 195.0),
        ]

        rois = find_key_field_rois(tokens, image_shape)

        amount_rois = [roi for roi in rois if roi.get("field_type") == "amount"]
        self.assertGreaterEqual(len(amount_rois), 2)

    def test_find_key_field_rois_excludes_plain_digits_accounts_and_orders(self):
        image_shape = (1200, 800, 3)
        tokens = [
            OCRToken("普通数字", "普通数字", (40, 80, 140, 110), 0.96, 100, 30, 95.0),
            OCRToken("123456", "123456", (320, 80, 430, 110), 0.95, 110, 30, 95.0),
            OCRToken("订单号", "订单号", (40, 160, 120, 190), 0.96, 80, 30, 175.0),
            OCRToken("20260601123000", "20260601123000", (320, 160, 520, 190), 0.95, 200, 30, 175.0),
            OCRToken("收款账号", "收款账号", (40, 240, 140, 270), 0.96, 100, 30, 255.0),
            OCRToken("6222****8888", "6222****8888", (320, 240, 500, 270), 0.95, 180, 30, 255.0),
        ]

        rois = find_key_field_rois(tokens, image_shape)

        self.assertEqual(rois, [])

    def test_find_key_field_rois_keeps_multiple_time_candidates(self):
        image_shape = (1200, 800, 3)
        tokens = [
            OCRToken("申请时间", "申请时间", (40, 100, 140, 130), 0.96, 100, 30, 115.0),
            OCRToken("2026-06-01 12:30:00", "2026-06-0112:30:00", (300, 100, 520, 130), 0.95, 220, 30, 115.0),
            OCRToken("交易时间", "交易时间", (40, 180, 140, 210), 0.96, 100, 30, 195.0),
            OCRToken("2026-06-01 12:31:00", "2026-06-0112:31:00", (300, 180, 520, 210), 0.95, 220, 30, 195.0),
        ]

        rois = find_key_field_rois(tokens, image_shape)

        time_rois = [roi for roi in rois if roi.get("field_type") == "time"]
        self.assertEqual(len(time_rois), 2)

    def test_find_suggested_rois_only_amount_name_time(self):
        image_shape = (1200, 800, 3)
        tokens = [
            OCRToken("转账金额", "转账金额", (40, 100, 140, 130), 0.96, 100, 30, 115.0),
            OCRToken("6,000.00", "6,000.00", (300, 100, 430, 130), 0.95, 130, 30, 115.0),
            OCRToken("收款人", "收款人", (40, 180, 120, 210), 0.96, 80, 30, 195.0),
            OCRToken("张三", "张三", (300, 180, 360, 210), 0.95, 60, 30, 195.0),
            OCRToken("交易时间", "交易时间", (40, 260, 140, 290), 0.96, 100, 30, 275.0),
            OCRToken("2026-06-01 12:30:00", "2026-06-0112:30:00", (300, 260, 520, 290), 0.95, 220, 30, 275.0),
            OCRToken("订单号", "订单号", (40, 340, 120, 370), 0.96, 80, 30, 355.0),
            OCRToken("NO123456", "NO123456", (300, 340, 420, 370), 0.95, 120, 30, 355.0),
            OCRToken("收款账号", "收款账号", (40, 420, 140, 450), 0.96, 100, 30, 435.0),
            OCRToken("6222****8888", "6222****8888", (300, 420, 460, 450), 0.95, 160, 30, 435.0),
        ]

        rois = find_suggested_rois(tokens, image_shape)

        categories = {roi.get("category") for roi in rois}
        self.assertIn("金额", categories)
        self.assertIn("姓名", categories)
        self.assertIn("时间", categories)
        self.assertNotIn("账号", categories)
        self.assertNotIn("单号", categories)

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
        self.assertTrue(
            rule_checks_need_auto_pixel_rescan(
                manual_bbox=None,
                semantic={"hard_tamper": True},
                timestamp={"hard_tamper": False},
                pixel_overlap={"alert": True, "hard_tamper": True},
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
                business_rules={"auto_detect_high_risk_rois": False},
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
