import unittest

from app.ai_detection.history_db import normalize_history_original_filename


class NormalizeHistoryOriginalFilenameTests(unittest.TestCase):
    def test_prefers_upload_name(self):
        name = normalize_history_original_filename(
            "chatgptedit.png",
            fallback_path="/data/uploads/a1b2-c3d4.jpg",
        )
        self.assertEqual(name, "chatgptedit.png")

    def test_strips_path_from_upload_name(self):
        name = normalize_history_original_filename(
            r"C:\fake\path\receipt.png",
            fallback_path="/data/uploads/task.jpg",
        )
        self.assertEqual(name, "receipt.png")

    def test_falls_back_to_disk_basename(self):
        name = normalize_history_original_filename(
            None,
            fallback_path="/data/uploads/7709f1e7-4327-43b4.jpg",
        )
        self.assertEqual(name, "7709f1e7-4327-43b4.jpg")

    def test_empty_upload_uses_fallback(self):
        name = normalize_history_original_filename(
            "   ",
            fallback_path="/data/uploads/task-id.jpg",
        )
        self.assertEqual(name, "task-id.jpg")


if __name__ == "__main__":
    unittest.main()
