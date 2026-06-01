import json
import tempfile
import unittest
from pathlib import Path

import yaml

from app.ai_detection.feedback_manager import FeedbackManager


class FeedbackManagerTests(unittest.TestCase):
    def _manager(self, tmp: str) -> FeedbackManager:
        cfg = {
            "feedback": {
                "storage_dir": str(Path(tmp) / "feedback"),
            }
        }
        cfg_path = Path(tmp) / "config.yaml"
        cfg_path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
        return FeedbackManager(str(cfg_path))

    def _seed_entry(self, manager: FeedbackManager, judgment: str = "suspicious") -> dict:
        src = manager.base_dir / "source.jpg"
        src.write_bytes(b"fake-image")
        return manager.save_judgment(
            task_id="task-1",
            judgment=judgment,
            image_path=str(src),
            bbox=[1, 2, 3, 4],
            result={"result": "篡改"},
            note="note",
        )

    def test_list_entries_includes_folder_name_and_image_urls(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = self._manager(tmp)
            entry = self._seed_entry(manager, "wrong")

            rows = manager.list_entries("wrong")

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["entry_id"], entry["entry_id"])
            self.assertTrue(rows[0]["folder_name"])
            self.assertEqual(rows[0]["judgment"], "wrong")
            self.assertIn("/api/v3/feedback/", rows[0]["image_url"])

    def test_update_entry_moves_between_judgments(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = self._manager(tmp)
            entry = self._seed_entry(manager, "suspicious")
            folder = manager.list_entries("suspicious")[0]["folder_name"]

            updated = manager.update_entry(folder, "wrong")

            self.assertIsNotNone(updated)
            self.assertEqual(updated["judgment"], "wrong")
            self.assertFalse((manager.suspicious_dir / folder).exists())
            self.assertTrue((manager.wrong_dir / folder).exists())

            metadata = json.loads((manager.wrong_dir / folder / "metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata["judgment"], "wrong")

    def test_delete_entry_removes_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = self._manager(tmp)
            self._seed_entry(manager, "correct")
            folder = manager.list_entries("correct")[0]["folder_name"]

            removed = manager.delete_entry(folder)

            self.assertTrue(removed)
            self.assertIsNone(manager.get_entry(folder))


if __name__ == "__main__":
    unittest.main()
