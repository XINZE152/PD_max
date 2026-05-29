import unittest

from app.ai_detection.bbox_overlap_checker import analyze_bbox_iou_overlaps, bbox_iou_xyxy


class BboxOverlapCheckerTests(unittest.TestCase):
    def test_bbox_iou_identical_boxes(self):
        box = [10, 20, 110, 80]
        self.assertAlmostEqual(bbox_iou_xyxy(box, box), 1.0)

    def test_detects_high_iou_overlap(self):
        result = analyze_bbox_iou_overlaps(
            [
                [100, 200, 400, 280],
                [110, 205, 390, 275],
            ],
            thresholds={
                "bbox_iou_alert": 0.35,
                "bbox_iou_hard_tamper": 0.70,
            },
        )

        self.assertGreaterEqual(result["max_iou"], 0.70)
        self.assertTrue(result["hard_tamper"])
        self.assertGreater(len(result["bbox_overlap_check"]["overlapping_pairs"]), 0)

    def test_ignores_same_row_fragment_overlap_for_risk(self):
        result = analyze_bbox_iou_overlaps(
            [
                [185, 1000, 488, 1021],
                [314, 1000, 488, 1021],
            ],
            thresholds={
                "bbox_iou_alert": 0.35,
                "bbox_iou_hard_tamper": 0.70,
            },
        )

        self.assertGreaterEqual(result["max_iou"], 0.35)
        self.assertEqual(result["bbox_overlap_check"]["risk_max_iou"], 0.0)
        self.assertFalse(result["hard_tamper"])
        self.assertEqual(result["risk"], 0.0)

    def test_includes_roi_in_overlap_scan(self):
        result = analyze_bbox_iou_overlaps(
            [[100, 200, 400, 280]],
            roi_bbox_xyxy=[105, 205, 395, 275],
            thresholds={"bbox_iou_alert": 0.35, "bbox_iou_hard_tamper": 0.70},
        )

        self.assertGreaterEqual(result["max_iou"], 0.70)


if __name__ == "__main__":
    unittest.main()
