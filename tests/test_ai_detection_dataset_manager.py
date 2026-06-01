import json
import tempfile
import unittest
from pathlib import Path

import yaml
from PIL import Image

from app.ai_detection.dataset_manager import DatasetManager


class DatasetManagerTests(unittest.TestCase):
    def _manager(self, tmp: str) -> DatasetManager:
        cfg = {
            "dataset": {
                "image_dir": str(Path(tmp) / "images"),
                "json_dir": str(Path(tmp) / "locate_json"),
            }
        }
        cfg_path = Path(tmp) / "config.yaml"
        cfg_path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
        return DatasetManager(str(cfg_path))

    def _write_image(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (12, 10), (255, 255, 255)).save(path)

    def test_list_entries_uses_filename_label_and_annotation(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = self._manager(tmp)
            self._write_image(manager.image_dir / "no (12).jpg")
            (manager.json_dir / "no (12).json").write_text('{"key_regions": []}', encoding="utf-8")

            rows = manager.list_entries()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["label"], 0)
            self.assertEqual(rows[0]["label_text"], "正常")
            self.assertTrue(rows[0]["has_annotation"])
            self.assertIn("/api/v3/training-dataset/", rows[0]["image_url"])

    def test_update_label_moves_base_enhanced_and_annotation(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = self._manager(tmp)
            self._write_image(manager.image_dir / "no (12).jpg")
            self._write_image(manager.image_dir / "no (12)_enhanced.jpg")
            (manager.json_dir / "no (12).json").write_text(json.dumps({"key_regions": []}), encoding="utf-8")

            entry = manager.update_label("no (12).jpg", 1)

            self.assertIsNotNone(entry)
            self.assertEqual(entry["filename"], "p (12).jpg")
            self.assertTrue((manager.image_dir / "p (12).jpg").is_file())
            self.assertTrue((manager.image_dir / "p (12)_enhanced.jpg").is_file())
            self.assertTrue((manager.json_dir / "p (12).json").is_file())
            self.assertFalse((manager.image_dir / "no (12).jpg").exists())
            self.assertFalse((manager.image_dir / "no (12)_enhanced.jpg").exists())

    def test_delete_entry_removes_family_and_annotation(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = self._manager(tmp)
            self._write_image(manager.image_dir / "p (1).jpg")
            self._write_image(manager.image_dir / "p (1)_enhanced.jpg")
            (manager.json_dir / "p (1).json").write_text("{}", encoding="utf-8")

            removed = manager.delete_entry("p (1).jpg")

            self.assertTrue(removed)
            self.assertFalse((manager.image_dir / "p (1).jpg").exists())
            self.assertFalse((manager.image_dir / "p (1)_enhanced.jpg").exists())
            self.assertFalse((manager.json_dir / "p (1).json").exists())


if __name__ == "__main__":
    unittest.main()
