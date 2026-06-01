import unittest
from unittest.mock import MagicMock, patch

from app.ai_detection.history_db import delete_ai_detection_history, normalize_history_original_filename


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

    @patch("app.ai_detection.history_db.get_conn")
    def test_delete_history_removes_database_row(self, mock_get_conn):
        cursor = MagicMock()
        cursor.fetchone.return_value = (None,)
        cursor.rowcount = 1
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.cursor.return_value.__enter__.return_value = cursor
        mock_get_conn.return_value = conn

        removed = delete_ai_detection_history(12)

        self.assertTrue(removed)
        executed = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any("DELETE FROM ai_detection_history" in sql for sql in executed))

    @patch("app.ai_detection.history_db.get_conn")
    def test_delete_history_returns_false_when_missing(self, mock_get_conn):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.cursor.return_value.__enter__.return_value = cursor
        mock_get_conn.return_value = conn

        self.assertFalse(delete_ai_detection_history(99))


if __name__ == "__main__":
    unittest.main()
