import unittest

from app.ai_detection.amount_candidates import OCRToken
from app.ai_detection.rule_check_service import evaluate_pixel_overlap_alert
from app.ai_detection.semantic_checker import (
    check_account_mask_consistency,
    check_detail_field_typography,
    find_labeled_field_bbox,
    is_invalid_amount_thousand_separator,
)


class SemanticCheckerTests(unittest.TestCase):
    def test_invalid_amount_thousand_separator(self):
        self.assertTrue(is_invalid_amount_thousand_separator("- 3,2500.00"))
        self.assertTrue(is_invalid_amount_thousand_separator("¥3,2500.00"))
        self.assertFalse(is_invalid_amount_thousand_separator("- 99,990.00"))
        self.assertFalse(is_invalid_amount_thousand_separator("32500.00"))
        self.assertFalse(is_invalid_amount_thousand_separator("- 3,250.00"))

    def test_detail_field_typography_detects_outlier(self):
        tokens = [
            OCRToken("收款账号", "收款账号", (40, 300, 140, 330), 0.9, 100, 30, 315.0),
            OCRToken("6230****8852", "6230****8852", (360, 298, 520, 340), 0.88, 160, 42, 319.0),
            OCRToken("付款账号", "付款账号", (40, 360, 140, 390), 0.9, 100, 30, 375.0),
            OCRToken("6212****6628", "6212****6628", (360, 361, 520, 387), 0.9, 160, 26, 374.0),
            OCRToken("交易时间", "交易时间", (40, 420, 140, 450), 0.9, 100, 30, 435.0),
            OCRToken("2026-01-05 18:15:58", "2026-01-05 18:15:58", (360, 421, 620, 447), 0.9, 260, 26, 434.0),
        ]
        result = check_detail_field_typography(tokens)
        self.assertTrue(result["anomaly"])
        self.assertTrue(any("收款账号" in item for item in result["outliers"]))

    def test_find_labeled_field_bbox(self):
        tokens = [
            OCRToken("收款账号", "收款账号", (40, 300, 140, 330), 0.9, 100, 30, 315.0),
            OCRToken("6230****8852", "6230****8852", (360, 302, 520, 328), 0.88, 160, 26, 315.0),
        ]
        bbox = find_labeled_field_bbox(tokens, "收款账号")
        self.assertEqual(bbox, [360, 302, 520, 328])

    def test_account_mask_inconsistency(self):
        tokens = [
            OCRToken("收款方账户", "收款方账户", (40, 300, 160, 330), 0.9, 120, 30, 315.0),
            OCRToken("6213 **** **** 3191", "6213 **** **** 3191", (360, 302, 620, 328), 0.88, 260, 26, 315.0),
            OCRToken("付款方账户", "付款方账户", (40, 360, 160, 390), 0.9, 120, 30, 375.0),
            OCRToken("6230 *** **** 7763", "6230 *** **** 7763", (360, 361, 620, 387), 0.9, 260, 26, 374.0),
        ]
        result = check_account_mask_consistency(tokens)
        self.assertTrue(result["anomaly"])


class RuleCheckDisplayTests(unittest.TestCase):
    def test_derive_status_from_semantic_hard_tamper(self):
        from app.ai_detection.rule_check_display import build_rule_check_public_summary, derive_rule_check_status

        payload = {
            "reason": "金额千分位格式异常（如 3,2500.00）",
            "hard_tamper_flags": {"semantic": True, "pixel_overlap": False, "timestamp": False},
            "semantic": {"hard_tamper": True, "anomalies": ["invalid_amount_format"], "reasons": ["金额千分位格式异常"]},
            "pixel_overlap": {"alert": True},
            "timestamp": {"anomalies": []},
        }
        self.assertEqual(derive_rule_check_status(payload), "篡改")
        summary = build_rule_check_public_summary(payload)
        self.assertTrue(summary["available"])
        self.assertFalse(summary["pixel_overlap"]["passed"])


class PixelOverlapAlertTests(unittest.TestCase):
    def test_text_splice_alert(self):
        thresholds = {
            "pixel_overlap_blend_alert": 0.55,
            "pixel_overlap_structural_alert": 0.79,
            "pixel_overlap_structural_de_min": 0.018,
            "pixel_overlap_text_splice_alert": 0.38,
            "pixel_overlap_ela_corroboration_min": 0.22,
            "pixel_overlap_structural_text_min": 0.52,
        }
        self.assertTrue(
            evaluate_pixel_overlap_alert(
                {
                    "blend_score": 0.0,
                    "structural_score": 0.30,
                    "double_edge_ratio": 0.01,
                    "text_splice_score": 0.45,
                    "ela_score": 0.40,
                },
                thresholds,
            )
        )
        self.assertTrue(
            evaluate_pixel_overlap_alert(
                {
                    "blend_score": 0.0,
                    "structural_score": 0.60,
                    "double_edge_ratio": 0.01,
                    "text_splice_score": 0.10,
                    "ela_score": 0.30,
                },
                thresholds,
            )
        )


if __name__ == "__main__":
    unittest.main()
