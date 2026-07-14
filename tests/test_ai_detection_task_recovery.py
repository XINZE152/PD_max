# -*- coding: utf-8 -*-
import tempfile
import unittest
import asyncio
import io
from pathlib import Path
from unittest.mock import AsyncMock, patch

import cv2
import numpy as np
from fastapi import UploadFile

from app.ai_detection.history_export import render_annotated_jpeg
from app.api.v1.routes.ai_detection import (
    DetectionDomainServiceV3,
    MemoryTaskRegistry,
    STORAGE_DIR,
    TaskStatusEnum,
    _persist_upload_task,
    _task_sidecar_path,
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
            sidecar = _task_sidecar_path(task_id)
            if sidecar.is_file():
                sidecar.unlink()

    def test_build_task_record_from_upload_sidecar_after_restart(self):
        async def run_case():
            task_id = "115e4ba8-e2bc-41c1-9ec8-c9cd91f1bf"
            storage_path = STORAGE_DIR / f"{task_id}.jpg"
            sidecar = _task_sidecar_path(task_id)
            storage_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                storage_path.write_bytes(b"\xff\xd8\xff")
                registry = MemoryTaskRegistry()
                await registry.create_task(
                    task_id=task_id,
                    image_path=str(storage_path),
                    original_filename="receipt.jpg",
                    image_created_at="2026-07-06 10:00:00",
                    batch="20260706001",
                )
                registry._store.clear()
                with patch(
                    "app.api.v1.routes.ai_detection.get_async_v3_history_by_task_id",
                    return_value=None,
                ):
                    task = build_task_record_from_persistence(task_id)
                self.assertIsNotNone(task)
                assert task is not None
                self.assertEqual(task.status, TaskStatusEnum.UPLOADED)
                self.assertEqual(task.original_filename, "receipt.jpg")
                self.assertEqual(task.image_created_at, "2026-07-06 10:00:00")
                self.assertEqual(task.batch, "20260706001")
            finally:
                if storage_path.is_file():
                    storage_path.unlink()
                if sidecar.is_file():
                    sidecar.unlink()

        asyncio.run(run_case())

    def test_delete_uploaded_task_removes_image_and_sidecar(self):
        async def run_case():
            task_id = "delete-sidecar-task"
            storage_path = STORAGE_DIR / f"{task_id}.jpg"
            sidecar = _task_sidecar_path(task_id)
            storage_path.parent.mkdir(parents=True, exist_ok=True)
            storage_path.write_bytes(b"\xff\xd8\xff")
            registry = MemoryTaskRegistry()
            await registry.create_task(
                task_id=task_id,
                image_path=str(storage_path),
                original_filename="receipt.jpg",
                batch="20260706002",
            )
            self.assertTrue(sidecar.is_file())

            removed = await registry.delete_task(task_id)

            self.assertTrue(removed)
            self.assertFalse(storage_path.exists())
            self.assertFalse(sidecar.exists())

        asyncio.run(run_case())

    def test_persist_upload_task_only_creates_uploaded_task(self):
        async def run_case():
            registry = MemoryTaskRegistry()
            upload = UploadFile(io.BytesIO(b"fake-image"), filename="receipt.jpg")
            task = await _persist_upload_task(
                file=upload,
                registry=registry,
                image_created_at="2026-07-06 11:00:00",
                batch="20260706003",
            )
            try:
                self.assertEqual(task.status, TaskStatusEnum.UPLOADED)
                self.assertEqual(task.original_filename, "receipt.jpg")
                self.assertEqual(task.image_created_at, "2026-07-06 11:00:00")
                self.assertEqual(task.batch, "20260706003")
                self.assertTrue(Path(task.image_path or "").is_file())
                self.assertTrue(_task_sidecar_path(task.task_id).is_file())
            finally:
                await registry.delete_task(task.task_id)

        asyncio.run(run_case())

    def test_finalize_completed_task_clears_ephemeral_storage_after_history_archive(self):
        async def run_case():
            task_id = "cleanup-completed-task"
            storage_path = STORAGE_DIR / f"{task_id}.jpg"
            sidecar = _task_sidecar_path(task_id)
            storage_path.parent.mkdir(parents=True, exist_ok=True)
            storage_path.write_bytes(b"\xff\xd8\xff")
            registry = MemoryTaskRegistry()
            await registry.create_task(
                task_id=task_id,
                image_path=str(storage_path),
                original_filename="receipt.jpg",
                batch="20260706004",
            )
            service = DetectionDomainServiceV3(registry, asyncio.Semaphore(1))
            with patch.object(service, "_persist_history", new=AsyncMock()):
                with patch(
                    "app.api.v1.routes.ai_detection.get_async_v3_history_by_task_id",
                    return_value={"id": 321, "status": "COMPLETED"},
                ):
                    with patch(
                        "app.api.v1.routes.ai_detection.get_ai_detection_history_image_path",
                        return_value=Path("/tmp/ai_detection_history_images/321.jpg"),
                    ):
                        await service._finalize_completed_task(
                            task_id,
                            str(storage_path),
                            original_filename="receipt.jpg",
                            bbox=None,
                            result={"result": "正常", "confidence": 0.1},
                            multi_results=[{"result": "正常", "confidence": 0.1}],
                            image_created_at="2026-07-06 11:00:00",
                            batch="20260706004",
                        )

            self.assertFalse(storage_path.exists())
            self.assertFalse(sidecar.exists())
            self.assertIsNone(await registry.get_task(task_id))

        asyncio.run(run_case())

    def test_build_task_record_from_completed_history(self):
        task_id = "test-task-completed"
        with patch(
            "app.api.v1.routes.ai_detection.get_async_v3_history_by_task_id",
            return_value={
                "task_id": task_id,
                "status": "COMPLETED",
                "created_at": "2026-06-01 17:00:00",
                "image_created_at": "2026-05-31 12:34:56",
                "batch": "codex-history-batch-001",
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
        self.assertEqual(task.image_created_at, "2026-05-31 12:34:56")
        self.assertEqual(task.batch, "codex-history-batch-001")

    def test_assign_region_numbers_overwrites_sorted_order(self):
        rows = DetectionDomainServiceV3._assign_region_numbers(
            [
                {"result": "篡改", "region_no": 99, "field_label": "金额"},
                {"result": "正常", "field_label": "姓名"},
            ]
        )

        self.assertEqual([row["region_no"] for row in rows], [1, 2])

    def test_v3_suspicious_is_preserved_by_default(self):
        source = {
            "result": "可疑",
            "confidence": 0.596,
            "reason": "全局UI布局异常",
        }

        result = DetectionDomainServiceV3._resolve_v3_suspicious_result(source)

        self.assertIs(result, source)
        self.assertEqual(result["result"], "可疑")
        self.assertNotIn("v3_suspicious_resolved", result)

    def test_v3_suspicious_below_decision_threshold_resolves_to_normal(self):
        with patch("app.api.v1.routes.ai_detection.V3_RESOLVE_SUSPICIOUS_RESULTS", True):
            result = DetectionDomainServiceV3._resolve_v3_suspicious_result(
                {
                    "result": "可疑",
                    "confidence": 0.585,
                    "reason": "全局UI布局异常",
                }
            )

        self.assertEqual(result["result"], "正常")
        self.assertTrue(result["v3_suspicious_resolved"])
        self.assertIn("未达到自动篡改阈值", result["reason"])

    def test_v3_suspicious_above_decision_threshold_resolves_to_tampered(self):
        with patch("app.api.v1.routes.ai_detection.V3_RESOLVE_SUSPICIOUS_RESULTS", True):
            result = DetectionDomainServiceV3._resolve_v3_suspicious_result(
                {
                    "result": "可疑",
                    "confidence": 0.596,
                    "reason": "全局UI布局异常",
                }
            )

        self.assertEqual(result["result"], "篡改")
        self.assertTrue(result["v3_suspicious_resolved"])
        self.assertIn("达到自动篡改阈值", result["reason"])

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
                self.assertEqual(result["result"], "无法自动检测")
                self.assertIn("金额、姓名、时间", result["reason"])
                self.assertEqual(finalize.call_args.kwargs["persist_bbox"]["note"], "no_key_field_regions")

        asyncio.run(run_case())

    def test_large_document_visual_override_marks_tampered(self):
        registry = MemoryTaskRegistry()
        service = DetectionDomainServiceV3(registry, asyncio.Semaphore(1))
        image = np.full((2600, 3000, 3), 245, dtype=np.uint8)

        left, right = 180, 2700
        top, bottom = 360, 1420
        for y in range(top, bottom + 1, 140):
            cv2.line(image, (left, y), (right, y), (0, 0, 0), 5)
        for x in (left, 760, 1120, right):
            cv2.line(image, (x, top), (x, bottom), (0, 0, 0), 5)
        cv2.circle(image, (2200, 2100), 230, (0, 0, 210), 28)
        service._cached_img_cv2 = image

        result = service._visual_document_override()

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["result"], "篡改")
        self.assertEqual(result["field_label"], "电子凭证")

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
