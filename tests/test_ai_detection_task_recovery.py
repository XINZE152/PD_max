# -*- coding: utf-8 -*-
import tempfile
import unittest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import cv2
import numpy as np

from app.ai_detection.history_export import render_annotated_jpeg
from app.api.v1.routes.ai_detection import (
    DetectionDomainServiceV3,
    MemoryTaskRegistry,
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
            self.assertIn("中断", task.error_msg or "")
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

    def test_assign_region_numbers_overwrites_sorted_order(self):
        rows = DetectionDomainServiceV3._assign_region_numbers(
            [
                {"result": "篡改", "region_no": 99, "field_label": "金额"},
                {"result": "正常", "field_label": "姓名"},
            ]
        )

        self.assertEqual([row["region_no"] for row in rows], [1, 2])

    def test_execute_async_without_key_regions_returns_unable_to_detect(self):
        async def run_case():
            registry = MemoryTaskRegistry()
            task_id = "no-key-region-task"
            with tempfile.NamedTemporaryFile(suffix=".jpg") as tmp:
                await registry.create_task(
                    task_id=task_id,
                    image_path=tmp.name,
                    original_filename="receipt.jpg",
                )
                service = DetectionDomainServiceV3(registry, asyncio.Semaphore(1))
                service._cached_key_rois = []

                with patch("app.api.v1.routes.ai_detection.ensure_ai_detection_runtime", new=AsyncMock()):
                    with patch("app.api.v1.routes.ai_detection.EngineContainer.instance", object()):
                        with patch("app.api.v1.routes.ai_detection.EngineContainer.ocr_reader", object()):
                            with patch.object(service, "_run_ocr_once", return_value=None):
                                with patch.object(service, "_finalize_completed_task") as finalize:
                                    await service.execute_async(task_id, tmp.name, None)

                finalize.assert_called_once()
                result = finalize.call_args.kwargs["result"]
                self.assertEqual(result["result"], "无法检测")
                self.assertIn("金额、姓名、时间", result["reason"])
                self.assertEqual(finalize.call_args.kwargs["persist_bbox"]["note"], "no_key_field_regions")

        asyncio.run(run_case())

    def test_render_annotated_jpeg_draws_region_number_labels(self):
        image = np.full((120, 180, 3), 255, dtype=np.uint8)
        with tempfile.NamedTemporaryFile(suffix=".jpg") as tmp:
            cv2.imencode(".jpg", image)[1].tofile(tmp.name)
            outcome = {
                "multi_results": [
                    {
                        "result": "正常",
                        "confidence": 0.1,
                        "bbox": [20, 20, 50, 40],
                        "original_bbox": [20, 20, 70, 60],
                        "region_no": 1,
                        "field_label": "金额",
                    },
                    {
                        "result": "篡改",
                        "confidence": 0.9,
                        "bbox": [90, 20, 50, 40],
                        "original_bbox": [90, 20, 140, 60],
                        "region_no": 2,
                        "field_label": "姓名",
                    },
                ]
            }

            with patch("app.ai_detection.history_export.load_chinese_font") as mock_font:
                from PIL import ImageFont

                mock_font.return_value = ImageFont.load_default()
                jpeg = render_annotated_jpeg(Path(tmp.name), outcome)

        rendered = cv2.imdecode(np.frombuffer(jpeg, dtype=np.uint8), cv2.IMREAD_COLOR)
        self.assertIsNotNone(rendered)
        assert rendered is not None
        self.assertLess(float(np.mean(rendered[24:48, 24:48])), 245.0)
        self.assertLess(float(np.mean(rendered[24:48, 94:118])), 245.0)


if __name__ == "__main__":
    unittest.main()
