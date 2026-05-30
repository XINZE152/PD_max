import unittest
from datetime import datetime
from unittest.mock import patch

from app.ai_detection.amount_candidates import OCRToken
from app.ai_detection.timestamp_checker import (
    check_image_timestamps,
    extract_timestamps_from_tokens,
    parse_exif_timestamps,
    parse_loose_datetime,
)


class TimestampCheckerTests(unittest.TestCase):
    def test_extract_status_bar_and_transaction_time(self):
        tokens = [
            OCRToken(
                text="11:32",
                clean_text="11:32",
                bbox=(10, 10, 90, 40),
                conf=0.99,
                width=80,
                height=30,
                center_y=25.0,
            ),
            OCRToken(
                text="2026-05-28 11:32:00",
                clean_text="2026-05-28 11:32:00",
                bbox=(100, 420, 620, 470),
                conf=0.98,
                width=520,
                height=50,
                center_y=445.0,
            ),
        ]

        info = extract_timestamps_from_tokens(tokens, (1000, 800, 3))

        self.assertEqual(info["status_bar_time"], "11:32")
        self.assertEqual(info["transaction_time"], "2026-05-28 11:32:00")
        self.assertEqual(info["transaction_datetime"], "2026-05-28 11:32:00")

    def test_detects_status_transaction_mismatch(self):
        tokens = [
            OCRToken(
                text="09:15",
                clean_text="09:15",
                bbox=(10, 10, 90, 40),
                conf=0.99,
                width=80,
                height=30,
                center_y=25.0,
            ),
            OCRToken(
                text="2026-05-28 18:40:00",
                clean_text="2026-05-28 18:40:00",
                bbox=(100, 420, 620, 470),
                conf=0.98,
                width=520,
                height=50,
                center_y=445.0,
            ),
        ]

        with patch("app.ai_detection.timestamp_checker.parse_exif_timestamps", return_value={"has_exif": False}):
            result = check_image_timestamps(
                "/tmp/mock.jpg",
                ocr_tokens=tokens,
                image_shape=(1000, 800, 3),
            )

        self.assertIn("status_transaction_time_mismatch", result["anomalies"])
        self.assertGreater(result["risk"], 0.5)
        self.assertTrue(result.get("hard_tamper"))
        self.assertTrue(any("状态栏时间" in reason for reason in result["reasons"]))

    def test_parse_loose_datetime_glued_ocr_text(self):
        parsed = parse_loose_datetime("2026-01-2615.53.12")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(sep=" ", timespec="seconds"), "2026-01-26 15:53:12")

    def test_parse_mangled_datetime_with_ocr_noise(self):
        parsed = parse_loose_datetime("2026.0..22.4.20.37")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(sep=" ", timespec="seconds"), "2026-01-22 04:20:37")

    def test_align_hour_with_status_bar(self):
        from app.ai_detection.timestamp_checker import _align_hour_with_status_bar

        parsed = parse_loose_datetime("2026.0..22.4.20.37")
        aligned = _align_hour_with_status_bar(parsed, "14:21")
        self.assertEqual(aligned.isoformat(sep=" ", timespec="seconds"), "2026-01-22 14:20:37")

    def test_extract_glued_transaction_time(self):
        tokens = [
            OCRToken(
                text="15:55",
                clean_text="15:55",
                bbox=(10, 10, 90, 40),
                conf=0.99,
                width=80,
                height=30,
                center_y=25.0,
            ),
            OCRToken(
                text="2026-01-2615.53.12",
                clean_text="2026-01-2615.53.12",
                bbox=(100, 420, 620, 470),
                conf=0.98,
                width=520,
                height=50,
                center_y=445.0,
            ),
        ]

        info = extract_timestamps_from_tokens(tokens, (1000, 800, 3))

        self.assertEqual(info["transaction_datetime"], "2026-01-26 15:53:12")

    def test_business_status_bar_not_compared_by_default(self):
        tokens = [
            OCRToken(
                text="09:15",
                clean_text="09:15",
                bbox=(10, 10, 90, 40),
                conf=0.99,
                width=80,
                height=30,
                center_y=25.0,
            ),
        ]

        with patch("app.ai_detection.timestamp_checker.parse_exif_timestamps", return_value={"has_exif": False}):
            result = check_image_timestamps(
                "/tmp/mock.jpg",
                ocr_tokens=tokens,
                image_shape=(1000, 800, 3),
                business_datetime="2026-05-28 18:40:00",
            )

        self.assertNotIn("business_status_bar_time_mismatch", result["anomalies"])
        self.assertFalse(result.get("business_mismatch"))

    def test_detects_business_document_time_mismatch(self):
        tokens = [
            OCRToken(
                text="2026-05-28 11:32:00",
                clean_text="2026-05-28 11:32:00",
                bbox=(100, 420, 620, 470),
                conf=0.98,
                width=520,
                height=50,
                center_y=445.0,
            ),
        ]

        with patch("app.ai_detection.timestamp_checker.parse_exif_timestamps", return_value={"has_exif": False}):
            result = check_image_timestamps(
                "/tmp/mock.jpg",
                ocr_tokens=tokens,
                image_shape=(1000, 800, 3),
                business_datetime="2026-05-28 18:40:00",
            )

        self.assertIn("business_visible_datetime_mismatch", result["anomalies"])
        self.assertFalse(result.get("hard_tamper"))
        self.assertTrue(result.get("business_mismatch"))
        self.assertGreaterEqual(result["risk"], 0.5)
        self.assertLess(result["risk"], 0.65)

    def test_detects_visible_time_not_found_when_business_time_given(self):
        with patch("app.ai_detection.timestamp_checker.parse_exif_timestamps", return_value={"has_exif": False}):
            result = check_image_timestamps(
                "/tmp/mock.jpg",
                ocr_tokens=[],
                image_shape=(1000, 800, 3),
                business_datetime="2026-05-28 11:32:00",
            )

        self.assertIn("business_visible_time_not_found", result["anomalies"])

    @patch("app.ai_detection.timestamp_checker.Image.open")
    def test_parse_exif_timestamps(self, mock_open):
        mock_img = mock_open.return_value.__enter__.return_value
        mock_img._getexif.return_value = {
            36867: "2026:05:28 10:00:00",
            36868: "2026:05:28 10:00:01",
            305: "Adobe Photoshop",
        }

        info = parse_exif_timestamps("/tmp/mock-with-exif.jpg")

        self.assertTrue(info["has_exif"])
        self.assertEqual(info["datetime_original"], "2026-05-28 10:00:00")
        self.assertTrue(info["suspicious_software"])

    @patch("app.ai_detection.timestamp_checker.parse_exif_timestamps")
    def test_future_datetime_is_flagged(self, mock_parse_exif):
        future = datetime(2099, 1, 1, 12, 0, 0).isoformat(sep=" ", timespec="seconds")
        mock_parse_exif.return_value = {
            "has_exif": True,
            "datetime_original": future,
            "datetime_digitized": None,
            "software": None,
            "suspicious_software": False,
        }

        result = check_image_timestamps("/tmp/mock.jpg")

        self.assertIn("future_datetime", result["anomalies"])
        self.assertGreaterEqual(result["risk"], 0.72)

    def test_status_bar_single_digit_hour_aligns_with_transaction(self):
        tokens = [
            OCRToken(
                text="2:49",
                clean_text="2:49",
                bbox=(10, 10, 90, 40),
                conf=0.99,
                width=80,
                height=30,
                center_y=25.0,
            ),
            OCRToken(
                text="申请时间:  2026-01-2814:49:06",
                clean_text="申请时间:  2026-01-2814:49:06",
                bbox=(100, 420, 620, 470),
                conf=0.98,
                width=520,
                height=50,
                center_y=445.0,
            ),
        ]

        with patch("app.ai_detection.timestamp_checker.parse_exif_timestamps", return_value={"has_exif": False}):
            result = check_image_timestamps(
                "/tmp/mock.jpg",
                ocr_tokens=tokens,
                image_shape=(1000, 800, 3),
            )

        self.assertNotIn("status_transaction_time_mismatch", result["anomalies"])
        self.assertFalse(result.get("hard_tamper"))

    def test_moderate_status_mismatch_is_not_hard_tamper(self):
        tokens = [
            OCRToken(
                text="18:34",
                clean_text="18:34",
                bbox=(10, 10, 90, 40),
                conf=0.99,
                width=80,
                height=30,
                center_y=25.0,
            ),
            OCRToken(
                text="2026012720004001110013002985888",
                clean_text="2026012720004001110013002985888",
                bbox=(100, 420, 620, 470),
                conf=0.98,
                width=520,
                height=50,
                center_y=445.0,
            ),
        ]

        with patch("app.ai_detection.timestamp_checker.parse_exif_timestamps", return_value={"has_exif": False}):
            result = check_image_timestamps(
                "/tmp/mock.jpg",
                ocr_tokens=tokens,
                image_shape=(1000, 800, 3),
            )

        self.assertIn("status_transaction_time_mismatch", result["anomalies"])
        self.assertFalse(result.get("hard_tamper"))

    def test_unparsed_transaction_time_is_flagged(self):
        tokens = [
            OCRToken(
                text="2026-01-28170 37",
                clean_text="2026-01-28170 37",
                bbox=(100, 420, 620, 470),
                conf=0.98,
                width=520,
                height=50,
                center_y=445.0,
            ),
        ]

        with patch("app.ai_detection.timestamp_checker.parse_exif_timestamps", return_value={"has_exif": False}):
            result = check_image_timestamps(
                "/tmp/mock.jpg",
                ocr_tokens=tokens,
                image_shape=(1000, 800, 3),
                business_datetime="2026-05-28 11:32:00",
            )

        self.assertIn("transaction_time_unparsed", result["anomalies"])
        self.assertGreaterEqual(result["risk"], 0.38)
        self.assertFalse(result.get("hard_tamper"))


if __name__ == "__main__":
    unittest.main()
