# -*- coding: utf-8 -*-
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.api.v1.routes.ai_detection import (
    STORAGE_DIR,
    TaskStatusEnum,
    build_task_record_from_persistence,
)


class TaskRecoveryTests(unittest.TestCase):
    def test_build_task_record_from_storage_after_restart(self):
        task_id = "115e4ba8-e2bc-41c1-9ec8-c9cd91f0e1bf"
        storage_path = STORAGE_DIR / f"{task_id}.jpg"
        storage_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            storage_path.write_bytes(b"\xff\xd8\xff")
            with patch(
                "app.api.v1.routes.ai_detection.get_async_v3_history_by_task_id",
                return_value=None,
            ):
                task = build_task_record_from_persistence(task_id)
            self.assertIsNotNone(task)
            assert task is not None
            self.assertEqual(task.status, TaskStatusEnum.FAILED)
            self.assertIn("重启", task.error_msg or "")
        finally:
            if storage_path.is_file():
                storage_path.unlink()

    def test_build_task_record_from_completed_history(self):
        task_id = "test-task-completed"
        with patch(
            "app.api.v1.routes.ai_detection.get_async_v3_history_by_task_id",
            return_value={
                "task_id": task_id,
                "status": "COMPLETED",
                "created_at": "2026-06-01 17:00:00",
                "original_filename": "chatgptedit5.png",
                "bbox": None,
                "outcome": {
                    "result": {"result": "正常", "confidence": 0.2},
                    "linked_rule_checks": {"status": "正常", "available": True},
                },
            },
        ):
            with patch(
                "app.api.v1.routes.ai_detection.get_rule_checks_history_by_task_id",
                return_value=None,
            ):
                task = build_task_record_from_persistence(task_id)
        self.assertIsNotNone(task)
        assert task is not None
        self.assertEqual(task.status, TaskStatusEnum.COMPLETED)
        self.assertEqual(task.result.get("result"), "正常")


if __name__ == "__main__":
    unittest.main()
