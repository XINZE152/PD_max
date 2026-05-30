from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import shutil
import tempfile
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

import cv2
import numpy as np
from functools import partial

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from PIL import Image, ImageDraw

from app.ai_detection.amount_candidates import (
    build_amount_candidates,
    detect_certificate_document_override,
)
from app.ai_detection.core.utils import load_chinese_font
from app.config import UPLOAD_DIR
from app.ai_detection.easyocr_download_patch import patch_easyocr_download
from app.ai_detection.history_db import (
    HISTORY_RETENTION_DAYS,
    get_ai_detection_history_image_path,
    get_latest_ai_detection_history_by_task_id,
    insert_ai_detection_history,
    list_ai_detection_history,
    purge_ai_detection_history_older_than,
)
from app.ai_detection.ocr_utils import build_detection_bboxes_from_tokens, run_full_image_ocr
from app.ai_detection.rule_check_service import (
    run_pixel_overlap_check,
    run_rule_checks,
    run_timestamp_check,
)
from app.ai_detection.runtime_assets import get_easyocr_reader_kwargs

if TYPE_CHECKING:
    from app.ai_detection.inference_api import InferenceEngineAPI

logger = logging.getLogger(__name__)

STORAGE_DIR = Path(UPLOAD_DIR) / "ai_detection_storage"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

MAX_CONCURRENT_AI_TASKS = int(os.getenv("AI_MAX_CONCURRENT_TASKS", "1"))
GC_MAX_AGE_HOURS = int(os.getenv("AI_GC_MAX_AGE_HOURS", "24"))
GC_INTERVAL_SECONDS = int(os.getenv("AI_GC_INTERVAL_SECONDS", "3600"))


class TaskStatusEnum(str, Enum):
    """异步任务状态（鉴伪队列）。"""

    UPLOADED = "UPLOADED"  # 图片已落盘
    PENDING = "PENDING"  # 已排队待处理
    PROCESSING = "PROCESSING"  # 推理中
    COMPLETED = "COMPLETED"  # 已完成
    FAILED = "FAILED"  # 失败
    CANCELED = "CANCELED"  # 已取消


class BBoxDTO(BaseModel):
    """检测区域：左上角 (x1,y1)、右下角 (x2,y2)，像素坐标，原点在图像左上角。"""

    x1: int = Field(ge=0, description="区域左上角 x（像素）")
    y1: int = Field(ge=0, description="区域左上角 y（像素）")
    x2: int = Field(gt=0, description="区域右下角 x（像素），须大于 x1")
    y2: int = Field(gt=0, description="区域右下角 y（像素），须大于 y1")
    model_config = ConfigDict(strict=True)


class TaskRecordDTO(BaseModel):
    """异步鉴伪任务记录（查询结果接口返回体）。"""

    task_id: str = Field(description="任务 ID（UUID）")
    status: TaskStatusEnum = Field(description="任务状态")
    created_at: str = Field(description="创建时间（ISO8601）")
    image_path: Optional[str] = Field(None, description="服务端保存的原图路径（仅调试/内部用）")
    bbox: Optional[BBoxDTO] = Field(None, description="用户指定的检测框；未传则后台自动 OCR 找数字区域")
    result: Optional[Dict[str, Any]] = Field(
        None,
        description="单框检测结果：含 result / confidence / bbox / reason 等，见接口说明中的输出样例",
    )
    multi_results: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="多框检测时，每个框一条结果列表；单框成功时一般为 null",
    )
    error_msg: Optional[str] = Field(None, description="失败时的错误信息")

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "status": "COMPLETED",
                    "created_at": "2026-04-03T10:00:00",
                    "image_path": "/path/to/uploads/ai_detection_storage/a1b2....jpg",
                    "bbox": {"x1": 120, "y1": 80, "x2": 400, "y2": 140},
                    "result": {
                        "result": "正常",
                        "confidence": 0.32,
                        "bbox": [120, 80, 280, 60],
                        "reason": "未检出明显篡改痕迹",
                        "original_bbox": [120, 80, 400, 140],
                    },
                    "multi_results": None,
                    "error_msg": None,
                },
                {
                    "task_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
                    "status": "PENDING",
                    "created_at": "2026-04-03T10:01:00",
                    "image_path": "/path/to/uploads/ai_detection_storage/b2c3....jpg",
                    "bbox": None,
                    "result": None,
                    "multi_results": None,
                    "error_msg": None,
                },
            ]
        }
    )


class AbstractTaskRegistry(ABC):
    @abstractmethod
    async def create_task(self, task_id: str, image_path: str) -> None:
        pass

    @abstractmethod
    async def update_task(self, task_id: str, **kwargs) -> None:
        pass

    @abstractmethod
    async def get_task(self, task_id: str) -> Optional[TaskRecordDTO]:
        pass

    @abstractmethod
    async def delete_task(self, task_id: str) -> bool:
        pass


class MemoryTaskRegistry(AbstractTaskRegistry):
    def __init__(self):
        self._store: Dict[str, TaskRecordDTO] = {}

    async def create_task(self, task_id: str, image_path: str) -> None:
        self._store[task_id] = TaskRecordDTO(
            task_id=task_id,
            status=TaskStatusEnum.UPLOADED,
            created_at=datetime.now().isoformat(),
            image_path=image_path,
        )

    async def update_task(self, task_id: str, **kwargs) -> None:
        if task_id in self._store:
            task = self._store[task_id]
            for key, value in kwargs.items():
                if hasattr(task, key):
                    setattr(task, key, value)

    async def get_task(self, task_id: str) -> Optional[TaskRecordDTO]:
        return self._store.get(task_id)

    async def delete_task(self, task_id: str) -> bool:
        if task_id not in self._store:
            return False

        img_path = self._store[task_id].image_path
        if img_path and os.path.exists(img_path):
            os.remove(img_path)

        vis_path = STORAGE_DIR / f"vis_{task_id}.jpg"
        if vis_path.exists():
            vis_path.unlink()

        del self._store[task_id]
        return True


async def cleanup_daemon(registry: AbstractTaskRegistry):
    logger.info(
        "AI detection GC daemon started (interval=%ss, max_age=%sh)",
        GC_INTERVAL_SECONDS,
        GC_MAX_AGE_HOURS,
    )
    while True:
        try:
            await asyncio.sleep(GC_INTERVAL_SECONDS)
            now = datetime.now()
            if not isinstance(registry, MemoryTaskRegistry):
                continue

            tasks_to_delete: List[str] = []
            for task_id, task in registry._store.items():
                try:
                    created_time = datetime.fromisoformat(task.created_at)
                    if now - created_time > timedelta(hours=GC_MAX_AGE_HOURS):
                        tasks_to_delete.append(task_id)
                except Exception:
                    logger.warning("Skip invalid task timestamp for %s", task_id)

            for task_id in tasks_to_delete:
                await registry.delete_task(task_id)

            if tasks_to_delete:
                logger.info("GC removed %s expired AI detection task(s)", len(tasks_to_delete))

            try:
                purged = await run_in_threadpool(purge_ai_detection_history_older_than)
                if purged:
                    logger.info(
                        "AI detection DB history purge removed %s row(s) older than %s day(s)",
                        purged,
                        HISTORY_RETENTION_DAYS,
                    )
            except Exception:
                logger.exception("AI detection DB history purge failed")
        except asyncio.CancelledError:
            logger.info("AI detection GC daemon stopped")
            break
        except Exception:
            logger.exception("AI detection GC daemon failed in one cycle")


class EngineContainer:
    instance: Optional[InferenceEngineAPI] = None
    registry: Optional[AbstractTaskRegistry] = None
    ocr_reader: Optional[Any] = None
    ai_semaphore: Optional[asyncio.Semaphore] = None
    cleanup_task: Optional[asyncio.Task] = None
    _runtime_lock: Optional[asyncio.Lock] = None


async def startup_ai_detection() -> None:
    """仅注册任务表、并发与 GC；EasyOCR / 推理引擎在首次请求时再加载，避免阻塞 HTTP 端口监听。"""
    if EngineContainer.registry is not None:
        return

    EngineContainer._runtime_lock = asyncio.Lock()
    EngineContainer.registry = MemoryTaskRegistry()
    EngineContainer.ai_semaphore = asyncio.Semaphore(MAX_CONCURRENT_AI_TASKS)
    EngineContainer.cleanup_task = asyncio.create_task(
        cleanup_daemon(EngineContainer.registry)
    )
    logger.info(
        "AI detection registry ready (EasyOCR/engine load deferred until first AI request)"
    )


def _create_easyocr_reader(use_gpu: bool):
    """
    EasyOCR 首次运行可能从网络拉取模型；网络不稳时易触发 RemoteDisconnected。
    短暂重试可缓解偶发断连。模型目录等见 runtime_assets（AI_EASYOCR_MODEL_DIR 等）；
    若设置 EASYOCR_MODULE_PATH，则覆盖为 {path}/model/。
    """
    import easyocr

    patch_easyocr_download()

    kwargs: Dict[str, Any] = dict(get_easyocr_reader_kwargs(gpu=use_gpu, verbose=False))
    model_dir = os.getenv("EASYOCR_MODULE_PATH", "").strip()
    if model_dir:
        mdir = os.path.join(model_dir, "model")
        Path(mdir).mkdir(parents=True, exist_ok=True)
        kwargs["model_storage_directory"] = mdir

    last_err: Optional[BaseException] = None
    for attempt in range(3):
        try:
            return easyocr.Reader(["ch_sim", "en"], **kwargs)
        except Exception as e:
            last_err = e
            if attempt < 2:
                wait_s = 2.0 * (attempt + 1)
                logger.warning(
                    "EasyOCR 初始化失败 (%s)，%ss 后重试 (%s/2)",
                    e,
                    wait_s,
                    attempt + 1,
                )
                time.sleep(wait_s)
    assert last_err is not None
    raise last_err


async def ensure_ai_detection_runtime() -> None:
    if EngineContainer.instance is not None and EngineContainer.ocr_reader is not None:
        return

    if EngineContainer._runtime_lock is None:
        EngineContainer._runtime_lock = asyncio.Lock()

    async with EngineContainer._runtime_lock:
        if EngineContainer.instance is not None and EngineContainer.ocr_reader is not None:
            return

        import torch

        _tn = os.getenv("TORCH_NUM_THREADS", "").strip()
        if _tn:
            try:
                torch.set_num_threads(max(1, int(_tn)))
                torch.set_num_interop_threads(1)
            except (ValueError, RuntimeError):
                pass

        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info("Loading AI detection runtime on %s (first use; may download EasyOCR models)", device)
        try:
            import easyocr  # noqa: F401 — 提前校验依赖
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Missing dependency 'easyocr'. Run `uv sync` or `pip install easyocr`."
            ) from exc

        ocr_reader = await run_in_threadpool(
            _create_easyocr_reader,
            device == "cuda",
        )
        EngineContainer.ocr_reader = ocr_reader
        from app.ai_detection.inference_api import InferenceEngineAPI

        def _build_engine() -> InferenceEngineAPI:
            # 与 FeatureExtractor 共用同一 EasyOCR，避免双份检测模型常驻（原先可占数百 MB～1GB+）
            return InferenceEngineAPI("config.yaml", shared_ocr_reader=ocr_reader)

        EngineContainer.instance = await run_in_threadpool(_build_engine)
        logger.info("AI detection runtime ready")


async def shutdown_ai_detection() -> None:
    cleanup_task = EngineContainer.cleanup_task
    if cleanup_task:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

    EngineContainer.instance = None
    EngineContainer.registry = None
    EngineContainer.ocr_reader = None
    EngineContainer.ai_semaphore = None
    EngineContainer.cleanup_task = None
    EngineContainer._runtime_lock = None


async def get_engine() -> InferenceEngineAPI:
    await ensure_ai_detection_runtime()
    if not EngineContainer.instance:
        raise HTTPException(status_code=503, detail="Engine unavailable")
    return EngineContainer.instance


def get_registry() -> AbstractTaskRegistry:
    if not EngineContainer.registry:
        raise HTTPException(status_code=503, detail="Registry unavailable")
    return EngineContainer.registry


async def get_ocr_reader() -> Any:
    await ensure_ai_detection_runtime()
    if not EngineContainer.ocr_reader:
        raise HTTPException(status_code=503, detail="OCR unavailable")
    return EngineContainer.ocr_reader


def get_ai_semaphore() -> asyncio.Semaphore:
    if not EngineContainer.ai_semaphore:
        raise HTTPException(status_code=503, detail="Semaphore unavailable")
    return EngineContainer.ai_semaphore


class DetectionService:
    @staticmethod
    async def process_detection(
        file: UploadFile,
        bbox_list: List[int],
        engine: InferenceEngineAPI,
        semaphore: asyncio.Semaphore,
        ocr_reader: Any,
        *,
        retain_temp_for_history: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """成功时若 retain_temp_for_history=True，返回 (结果, 临时图路径)，由调用方在归档后删除临时文件。"""
        tmp_path: Optional[str] = None
        keep_tmp = False
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
                tmp.write(await file.read())
                tmp_path = tmp.name

            async with semaphore:
                img_cv2, ocr_tokens = await run_in_threadpool(run_full_image_ocr, tmp_path, ocr_reader)
                detection_bboxes: List[List[int]] = []
                if img_cv2 is not None and ocr_tokens:
                    detection_bboxes = build_detection_bboxes_from_tokens(ocr_tokens, img_cv2.shape)
                result_str = await run_in_threadpool(
                    partial(
                        engine.predict,
                        tmp_path,
                        bbox_list,
                        "xyxy",
                        detection_bboxes=detection_bboxes or None,
                    ),
                )

            result_dict = json.loads(result_str)
            if result_dict.get("result") == "错误":
                raise ValueError(result_dict.get("reason", "Unknown engine internal error."))
            if retain_temp_for_history:
                keep_tmp = True
            return result_dict, (tmp_path if retain_temp_for_history else None)
        finally:
            if tmp_path and os.path.exists(tmp_path) and not keep_tmp:
                os.remove(tmp_path)


def _parse_bbox_form(bbox: str) -> List[int]:
    clean_bbox = bbox.strip().strip("'").strip('"').strip()
    if clean_bbox.startswith("["):
        parsed = json.loads(clean_bbox)
    else:
        parsed = [int(x.strip()) for x in clean_bbox.split(",")]
    if len(parsed) != 4:
        raise ValueError("bbox 格式无效，请使用 [x1,y1,x2,y2] 或 x1,y1,x2,y2")
    return [int(x) for x in parsed]


class RuleCheckService:
    """规则类检测（像素重叠、时间戳），与完整 AI 鉴伪解耦。"""

    @staticmethod
    async def _save_upload_to_temp(file: UploadFile) -> str:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
            tmp.write(await file.read())
            return tmp.name

    @staticmethod
    async def process_rule_checks(
        file: UploadFile,
        engine: InferenceEngineAPI,
        semaphore: asyncio.Semaphore,
        ocr_reader: Any,
        *,
        bbox_list: Optional[List[int]] = None,
        business_datetime: Optional[str] = None,
    ) -> Dict[str, Any]:
        tmp_path: Optional[str] = None
        try:
            tmp_path = await RuleCheckService._save_upload_to_temp(file)
            async with semaphore:
                img_cv2, ocr_tokens = await run_in_threadpool(run_full_image_ocr, tmp_path, ocr_reader)
                image_shape = None
                if img_cv2 is not None:
                    image_shape = (
                        int(img_cv2.shape[0]),
                        int(img_cv2.shape[1]),
                        int(img_cv2.shape[2]) if len(img_cv2.shape) > 2 else 3,
                    )
                return await run_in_threadpool(
                    partial(
                        run_rule_checks,
                        tmp_path,
                        engine.pixel_detector,
                        bbox_xyxy=bbox_list,
                        business_datetime=business_datetime,
                        ocr_tokens=ocr_tokens or None,
                        image_shape=image_shape,
                        thresholds=engine.config.get("thresholds", {}),
                        business_rules=engine.config.get("business_rules", {}),
                    ),
                )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    @staticmethod
    async def process_pixel_overlap(
        file: UploadFile,
        bbox_list: List[int],
        engine: InferenceEngineAPI,
        semaphore: asyncio.Semaphore,
    ) -> Dict[str, Any]:
        tmp_path: Optional[str] = None
        try:
            tmp_path = await RuleCheckService._save_upload_to_temp(file)
            async with semaphore:
                return await run_in_threadpool(
                    partial(
                        run_pixel_overlap_check,
                        tmp_path,
                        bbox_list,
                        engine.pixel_detector,
                        thresholds=engine.config.get("thresholds", {}),
                        margin=int(engine.config.get("business_rules", {}).get("roi_expand_margin", 15)),
                    ),
                )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    @staticmethod
    async def process_timestamp(
        file: UploadFile,
        engine: InferenceEngineAPI,
        semaphore: asyncio.Semaphore,
        ocr_reader: Any,
        *,
        business_datetime: Optional[str] = None,
    ) -> Dict[str, Any]:
        tmp_path: Optional[str] = None
        try:
            tmp_path = await RuleCheckService._save_upload_to_temp(file)
            async with semaphore:
                img_cv2, ocr_tokens = await run_in_threadpool(run_full_image_ocr, tmp_path, ocr_reader)
                image_shape = None
                if img_cv2 is not None:
                    image_shape = (
                        int(img_cv2.shape[0]),
                        int(img_cv2.shape[1]),
                        int(img_cv2.shape[2]) if len(img_cv2.shape) > 2 else 3,
                    )
                return await run_in_threadpool(
                    partial(
                        run_timestamp_check,
                        tmp_path,
                        ocr_tokens=ocr_tokens or None,
                        image_shape=image_shape,
                        business_datetime=business_datetime,
                        thresholds=engine.config.get("thresholds", {}),
                    ),
                )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)


class DetectionDomainServiceV3:
    def __init__(
        self,
        registry: AbstractTaskRegistry,
        semaphore: asyncio.Semaphore,
    ):
        self.registry = registry
        self.semaphore = semaphore
        self._cached_img_cv2: Optional[np.ndarray] = None
        self._cached_tokens: Optional[List[Any]] = None
        self._cached_candidates: Optional[List[Any]] = None
        self._ocr_reader: Optional[Any] = None

    @staticmethod
    def _bbox_iou(a: BBoxDTO, b: BBoxDTO) -> float:
        inter_x1 = max(a.x1, b.x1)
        inter_y1 = max(a.y1, b.y1)
        inter_x2 = min(a.x2, b.x2)
        inter_y2 = min(a.y2, b.y2)
        inter_w = max(0, inter_x2 - inter_x1)
        inter_h = max(0, inter_y2 - inter_y1)
        inter_area = inter_w * inter_h
        if inter_area == 0:
            return 0.0
        area_a = (a.x2 - a.x1) * (a.y2 - a.y1)
        area_b = (b.x2 - b.x1) * (b.y2 - b.y1)
        union_area = max(area_a + area_b - inter_area, 1)
        return inter_area / union_area

    def _deduplicate_bboxes(self, bboxes: List[BBoxDTO], iou_threshold: float = 0.85) -> List[BBoxDTO]:
        deduped: List[BBoxDTO] = []
        for bbox in sorted(bboxes, key=lambda b: ((b.x2 - b.x1) * (b.y2 - b.y1)), reverse=True):
            if any(self._bbox_iou(bbox, kept) >= iou_threshold for kept in deduped):
                continue
            deduped.append(bbox)
        return deduped

    @staticmethod
    def _xyxy_to_xywh(bbox_xyxy: Sequence[int]) -> List[int]:
        x1, y1, x2, y2 = [int(value) for value in bbox_xyxy[:4]]
        return [x1, y1, max(1, x2 - x1), max(1, y2 - y1)]

    @staticmethod
    def _result_sort_key(item: Dict[str, Any]) -> Tuple[int, float]:
        rank = {"篡改": 2, "可疑": 1, "正常": 0, "错误": -1}
        return rank.get(str(item.get("result", "")), -1), float(item.get("confidence", 0.0))

    @staticmethod
    def _select_top_result(results: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not results:
            return None
        return max(results, key=DetectionDomainServiceV3._result_sort_key)

    async def _is_canceled(self, task_id: str) -> bool:
        task = await self.registry.get_task(task_id)
        return bool(not task or task.status == TaskStatusEnum.CANCELED)

    def _run_ocr_once(self, image_path: str, ocr_reader: Any) -> None:
        """读取图片并执行一次 OCR tokenize + amount 候选构建，结果缓存供后续复用。"""
        if self._cached_tokens is not None:
            return
        img_cv2, tokens = run_full_image_ocr(image_path, ocr_reader)
        if img_cv2 is None:
            return
        self._cached_img_cv2 = img_cv2
        self._cached_tokens = tokens
        self._cached_candidates = build_amount_candidates(self._cached_tokens, img_cv2.shape)
        self._ocr_reader = ocr_reader

    def _predict_kwargs(self) -> Dict[str, Any]:
        detection_bboxes: List[List[int]] = []
        if self._cached_candidates and self._cached_img_cv2 is not None:
            detection_bboxes = [list(candidate.bbox) for candidate in self._cached_candidates]
        return {
            "detection_bboxes": detection_bboxes or None,
        }

    def _easyocr_auto_detect(self, image_path: str) -> List[BBoxDTO]:
        _ = image_path
        if not self._cached_candidates:
            return []
        return [
            BBoxDTO(
                x1=int(candidate.bbox[0]),
                y1=int(candidate.bbox[1]),
                x2=int(candidate.bbox[2]),
                y2=int(candidate.bbox[3]),
            )
            for candidate in self._cached_candidates
        ]

    def _document_rule_override(self, image_path: str) -> Optional[Dict[str, Any]]:
        if self._cached_img_cv2 is None or not self._cached_tokens:
            return None

        override = detect_certificate_document_override(
            image_path=Path(image_path),
            image=self._cached_img_cv2,
            tokens=self._cached_tokens,
            candidates=self._cached_candidates or [],
            ocr_reader=self._ocr_reader,
        )
        if not override:
            return None

        bbox_xyxy = [int(value) for value in override["bbox_xyxy"]]
        return {
            "result": override["result"],
            "confidence": float(override["confidence"]),
            "reason": override["reason"],
            "bbox": DetectionDomainServiceV3._xyxy_to_xywh(bbox_xyxy),
            "original_bbox": bbox_xyxy,
            "source": override.get("source"),
            "text": override.get("text"),
            "flags": override.get("flags"),
            "ocr_confidence": override.get("ocr_confidence"),
            "amount_score": override.get("amount_score"),
        }

    async def execute_async(
        self,
        task_id: str,
        image_path: str,
        bbox: Optional[BBoxDTO] = None,
    ) -> None:
        task = await self.registry.get_task(task_id)
        if not task or task.status == TaskStatusEnum.CANCELED:
            return

        await self.registry.update_task(task_id, status=TaskStatusEnum.PROCESSING)
        stored_name = Path(image_path).name

        try:
            await ensure_ai_detection_runtime()
            engine = EngineContainer.instance
            ocr_reader = EngineContainer.ocr_reader
            if not engine or not ocr_reader:
                raise RuntimeError("AI detection runtime unavailable")
            if await self._is_canceled(task_id):
                return

            async with self.semaphore:
                await run_in_threadpool(self._run_ocr_once, image_path, ocr_reader)
            predict_extra = self._predict_kwargs()

            if bbox:
                bbox_list = [bbox.x1, bbox.y1, bbox.x2, bbox.y2]
                async with self.semaphore:
                    res_str = await run_in_threadpool(
                        partial(engine.predict, image_path, bbox_list, "xyxy", **predict_extra),
                    )
                if await self._is_canceled(task_id):
                    return

                res_dict = json.loads(res_str)
                if res_dict.get("result") == "错误":
                    raise ValueError(res_dict.get("reason"))

                res_dict["original_bbox"] = bbox_list
                await self.registry.update_task(task_id, status=TaskStatusEnum.COMPLETED, result=res_dict)
                await self._persist_history(
                    task_id=task_id,
                    original_filename=stored_name,
                    bbox=bbox.model_dump(),
                    status="COMPLETED",
                    result=res_dict,
                    source_image_path=image_path,
                )
                return

            async with self.semaphore:
                bboxes = await run_in_threadpool(self._easyocr_auto_detect, image_path)
            if await self._is_canceled(task_id):
                return
            bboxes = self._deduplicate_bboxes(bboxes)

            if not bboxes:
                async with self.semaphore:
                    document_override = await run_in_threadpool(self._document_rule_override, image_path)
                if await self._is_canceled(task_id):
                    return

                if document_override:
                    await self.registry.update_task(
                        task_id,
                        status=TaskStatusEnum.COMPLETED,
                        result=document_override,
                        multi_results=[document_override],
                    )
                    await self._persist_history(
                        task_id=task_id,
                        original_filename=stored_name,
                        bbox={"auto_ocr": True, "note": "document_rule_override"},
                        status="COMPLETED",
                        result=document_override,
                        multi_results=[document_override],
                        source_image_path=image_path,
                    )
                    return

                empty_res = {"result": "正常", "confidence": 0.0, "reason": "未发现关键数值或单号区域"}
                await self.registry.update_task(task_id, status=TaskStatusEnum.COMPLETED, result=empty_res)
                await self._persist_history(
                    task_id=task_id,
                    original_filename=stored_name,
                    bbox={"auto_ocr": True, "note": "no_numeric_regions"},
                    status="COMPLETED",
                    result=empty_res,
                    source_image_path=image_path,
                )
                return

            all_results = []
            for b in bboxes:
                if await self._is_canceled(task_id):
                    return
                try:
                    b_list = [b.x1, b.y1, b.x2, b.y2]
                    async with self.semaphore:
                        res_str = await run_in_threadpool(
                            partial(engine.predict, image_path, b_list, "xyxy", **predict_extra),
                        )
                    if await self._is_canceled(task_id):
                        return

                    res_dict = json.loads(res_str)
                    if res_dict.get("result") != "错误":
                        res_dict["original_bbox"] = b_list
                        all_results.append(res_dict)
                except Exception as exc:
                    logger.warning("Task %s single bbox failed: %s", task_id, exc)

            async with self.semaphore:
                document_override = await run_in_threadpool(self._document_rule_override, image_path)
            if await self._is_canceled(task_id):
                return
            if document_override and not any(item.get("result") == "篡改" for item in all_results):
                all_results.append(document_override)

            ordered_results = sorted(all_results, key=self._result_sort_key, reverse=True)
            top_result = self._select_top_result(ordered_results)
            await self.registry.update_task(
                task_id,
                status=TaskStatusEnum.COMPLETED,
                result=top_result,
                multi_results=ordered_results,
            )
            await self._persist_history(
                task_id=task_id,
                original_filename=stored_name,
                bbox={"auto_ocr": True, "box_count": len(ordered_results)},
                status="COMPLETED",
                result=top_result,
                multi_results=ordered_results,
                source_image_path=image_path,
            )

        except Exception as exc:
            logger.exception("Task %s failed", task_id)
            await self.registry.update_task(task_id, status=TaskStatusEnum.FAILED, error_msg=str(exc))
            await self._persist_history(
                task_id=task_id,
                original_filename=stored_name,
                bbox=bbox.model_dump() if bbox else None,
                status="FAILED",
                error_msg=str(exc),
                source_image_path=image_path,
            )

    async def generate_visualization(self, task_id: str) -> str:
        task = await self.registry.get_task(task_id)
        image_path: Optional[str] = None
        result: Optional[Dict[str, Any]] = None
        multi_results: List[Dict[str, Any]] = []
        if task and task.status == TaskStatusEnum.COMPLETED:
            image_path = task.image_path
            result = task.result
            multi_results = list(task.multi_results or [])
        else:
            history = await run_in_threadpool(get_latest_ai_detection_history_by_task_id, task_id)
            if history:
                image_path = str(history["image_path"])
                outcome = history.get("outcome") or {}
                result = outcome.get("result")
                multi_results = list(outcome.get("multi_results") or [])
            if not image_path:
                raise ValueError("Task not completed.")

        vis_path = STORAGE_DIR / f"vis_{task_id}.jpg"
        if vis_path.exists():
            return str(vis_path)

        def draw_bboxes() -> None:
            img_cv2 = cv2.imdecode(np.fromfile(image_path, dtype=np.uint8), cv2.IMREAD_COLOR)
            if img_cv2 is None:
                raise ValueError("无法读取任务原图")

            img_pil = Image.fromarray(cv2.cvtColor(img_cv2, cv2.COLOR_BGR2RGB))
            draw = ImageDraw.Draw(img_pil)
            font = load_chinese_font(22)

            results_to_draw = list(multi_results)
            if result and not results_to_draw:
                results_to_draw.append(result)

            for res in results_to_draw:
                original_b = res.get("original_bbox") or res.get("bbox", [0, 0, 10, 10])
                x1, y1, x2, y2 = original_b[0], original_b[1], original_b[2], original_b[3]

                status = res.get("result", "正常")
                confidence = res.get("confidence", 0.0)

                if status == "篡改":
                    color, text_color = (255, 0, 0), (255, 255, 255)
                    label = f"篡改 | 风险:{confidence:.1%}"
                elif status == "可疑":
                    color, text_color = (255, 165, 0), (0, 0, 0)
                    label = f"可疑 | 风险:{confidence:.1%}"
                else:
                    color, text_color = (0, 255, 0), (0, 0, 0)
                    label = f"正常 | 风险:{confidence:.1%}"

                draw.rectangle([(x1, y1), (x2, y2)], outline=color, width=3)

                text_bbox = draw.textbbox((0, 0), label, font=font)
                text_width = text_bbox[2] - text_bbox[0]
                text_height = text_bbox[3] - text_bbox[1]
                label_bg_y1 = max(y1 - text_height - 6, 0)

                draw.rectangle(
                    [(x1, label_bg_y1), (min(x1 + text_width + 6, img_pil.width), max(y1, text_height + 6))],
                    fill=color,
                )
                draw.text((x1 + 3, label_bg_y1 + 3), label, font=font, fill=text_color)

            img_cv2_result = cv2.cvtColor(np.array(img_pil), cv2.COLOR_RGB2BGR)
            cv2.imencode(".jpg", img_cv2_result)[1].tofile(str(vis_path))

        await run_in_threadpool(draw_bboxes)
        return str(vis_path)

    async def _persist_history(
        self,
        *,
        task_id: str,
        original_filename: str,
        bbox: Optional[Any],
        status: str,
        result: Optional[Dict[str, Any]] = None,
        multi_results: Optional[List[Dict[str, Any]]] = None,
        error_msg: Optional[str] = None,
        source_image_path: Optional[str] = None,
    ) -> None:
        try:
            outcome: Dict[str, Any] = {}
            if result is not None:
                outcome["result"] = result
            if multi_results is not None:
                outcome["multi_results"] = multi_results
            if error_msg:
                outcome["error_msg"] = error_msg
            await run_in_threadpool(
                partial(
                    insert_ai_detection_history,
                    mode="async_v3",
                    task_id=task_id,
                    original_filename=original_filename,
                    bbox=bbox,
                    status=status,
                    outcome=outcome,
                    source_image_path=source_image_path,
                ),
            )
        except Exception:
            logger.exception("AI detection history async persist failed task=%s", task_id)


router = APIRouter(
    prefix="/ai-detection",
    tags=["AI鉴伪模块"],
)


_DETECT_RESULT_SCHEMA = (
    "引擎返回的 `data` / `result` 中单条结构示例（**不含**像素重叠与时间戳，请用规则检测接口）：\n"
    "```json\n"
    "{\n"
    '  "result": "正常",\n'
    '  "confidence": 0.32,\n'
    '  "bbox": [120, 80, 280, 60],\n'
    '  "reason": "未检出明显篡改痕迹",\n'
    '  "bbox_overlap_check": {\n'
    '    "max_iou": 0.42,\n'
    '    "overlapping_pairs": [],\n'
    '    "box_count": 3,\n'
    '    "anomalies": []\n'
    "  },\n"
    '  "hard_tamper_flags": { "bbox_iou": false }\n'
    "}\n"
    "```\n"
    "- **result**：`正常` | `可疑` | `篡改` | `错误`\n"
    "- **confidence**：综合风险 0~1，越高越可疑\n"
    "- **bbox**：引擎实际使用的 ROI（x, y, 宽, 高）\n"
    "- **bbox_overlap_check**：OCR 检测框 IoU 重叠分析（max_iou、overlapping_pairs）\n"
    "- **hard_tamper_flags.bbox_iou**：检测框高度重叠是否触发直接判「篡改」\n"
    "- **reason**：中文简要说明；异步任务成功时可能另含 **original_bbox**（用户传入的四点框）\n"
    "- 像素重叠、时间戳请调用 `POST .../api/v1/rule-checks`（或子接口）\n"
)

_RULE_CHECK_SCHEMA = (
    "规则检测返回的 `data` 结构示例（`POST .../api/v1/rule-checks` 及子接口）：\n"
    "```json\n"
    "{\n"
    '  "pixel_overlap": {\n'
    '    "pixel_overlap_score": 0.18,\n'
    '    "bbox": [120, 80, 280, 60],\n'
    '    "alert": false,\n'
    '    "hard_tamper": false,\n'
    '    "reasons": []\n'
    "  },\n"
    '  "timestamp": {\n'
    '    "timestamp_check": {\n'
    '      "status_bar_time": "11:32",\n'
    '      "transaction_time": "2026-05-28 11:32:00",\n'
    '      "business_document_time": null,\n'
    '      "anomalies": []\n'
    "    },\n"
    '    "risk": 0.0,\n'
    '    "hard_tamper": false\n'
    "  },\n"
    '  "hard_tamper_flags": { "pixel_overlap": false, "timestamp": false },\n'
    '  "reason": "未检出明显规则类异常"\n'
    "}\n"
    "```\n"
    "- 未传 **bbox** 时 `pixel_overlap` 为 `null`，仅执行时间戳检测。\n"
    "- 子接口 `/pixel-overlap/check` 仅返回像素重叠块；`/timestamp/check` 仅返回时间戳块。\n"
)


@router.post(
    "/api/v1/rule-checks",
    summary="规则检测（像素重叠 + 时间戳）",
    description=(
        "上传图片，执行**规则类**鉴伪：像素重叠与图内时间戳校验，**不加载 XGBoost/字体模型**。\n\n"
        "**请求方式**：`multipart/form-data`\n\n"
        "**输入参数**\n"
        "- **file**：图片文件（必填）\n"
        "- **bbox**（可选）：检测框 `[x1,y1,x2,y2]`；传入则额外做 ROI 像素重叠检测\n"
        "- **document_time**（可选）：业务单据时间，与图内交易时间比对\n\n"
        "**说明**：可与 `POST .../api/v3/detect` 并行调用；主鉴伪接口不再包含像素重叠与时间戳。\n\n"
        + _RULE_CHECK_SCHEMA
    ),
)
async def rule_checks_endpoint(
    file: UploadFile = File(..., description="待检测图片文件"),
    bbox: Optional[str] = Form(
        None,
        description="可选。像素重叠检测 ROI：[x1,y1,x2,y2]",
        examples=["[120,80,400,140]"],
    ),
    document_time: Optional[str] = Form(
        None,
        description="可选。业务单据时间",
        examples=["2026-05-28 11:32:00"],
    ),
    engine: InferenceEngineAPI = Depends(get_engine),
    ocr_reader: Any = Depends(get_ocr_reader),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    bbox_list: Optional[List[int]] = None
    if bbox:
        try:
            bbox_list = _parse_bbox_form(bbox)
        except Exception:
            raise HTTPException(status_code=400, detail="bbox 格式无效，请使用 [x1,y1,x2,y2] 或 x1,y1,x2,y2")
    try:
        data = await RuleCheckService.process_rule_checks(
            file,
            engine,
            semaphore,
            ocr_reader,
            bbox_list=bbox_list,
            business_datetime=document_time,
        )
        return {"status": "success", "data": data}
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"status": "error", "message": str(exc)})


@router.post(
    "/api/v1/pixel-overlap/check",
    summary="像素重叠规则检测",
    description=(
        "对指定 ROI 执行像素重叠/拼接规则检测（OpenCV + 统计阈值），不执行完整 AI 鉴伪。\n\n"
        "**请求方式**：`multipart/form-data`\n\n"
        "- **file**：图片文件（必填）\n"
        "- **bbox**：检测框 `[x1,y1,x2,y2]`（必填）\n"
    ),
)
async def pixel_overlap_check_endpoint(
    file: UploadFile = File(..., description="待检测图片文件"),
    bbox: str = Form(
        ...,
        description="像素重叠检测 ROI：[x1,y1,x2,y2]",
        examples=["[120,80,400,140]"],
    ),
    engine: InferenceEngineAPI = Depends(get_engine),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    try:
        bbox_list = _parse_bbox_form(bbox)
    except Exception:
        raise HTTPException(status_code=400, detail="bbox 格式无效，请使用 [x1,y1,x2,y2] 或 x1,y1,x2,y2")
    try:
        data = await RuleCheckService.process_pixel_overlap(file, bbox_list, engine, semaphore)
        return {"status": "success", "data": data}
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"status": "error", "message": str(exc)})


@router.post(
    "/api/v1/timestamp/check",
    summary="图片时间戳规则检测",
    description=(
        "对图片执行 OCR + EXIF + 业务时间规则校验，不执行完整 AI 鉴伪。\n\n"
        "**请求方式**：`multipart/form-data`\n\n"
        "- **file**：图片文件（必填）\n"
        "- **document_time**（可选）：业务单据时间\n"
    ),
)
async def timestamp_check_endpoint(
    file: UploadFile = File(..., description="待检测图片文件"),
    document_time: Optional[str] = Form(
        None,
        description="可选。业务单据时间",
        examples=["2026-05-28 11:32:00"],
    ),
    engine: InferenceEngineAPI = Depends(get_engine),
    ocr_reader: Any = Depends(get_ocr_reader),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    try:
        data = await RuleCheckService.process_timestamp(
            file,
            engine,
            semaphore,
            ocr_reader,
            business_datetime=document_time,
        )
        return {"status": "success", "data": data}
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"status": "error", "message": str(exc)})


@router.post(
    "/api/v1/image-detection/detect",
    summary="单图单框鉴伪（同步）",
    description=(
        "上传一张图片并指定一个矩形检测区域，**同步**返回鉴伪结果。适合低延迟、单区域场景。\n\n"
        "**网关 504**：经 Nginx/负载均衡时，**首次**调用可能因加载 EasyOCR 与模型耗时 1～数分钟，"
        "超过代理默认超时（常见 60s）会返回 **504**。处理办法：① 反向代理调大 `proxy_read_timeout`（建议 ≥300s）；"
        "② 后端设 `AI_DETECTION_PRELOAD=1` 在启动时预加载；③ 或改用异步接口 `POST .../api/v3/detect` 再轮询结果。\n\n"
        "**请求方式**：`multipart/form-data`\n\n"
        "**输入参数**\n"
        "- **file**：图片文件（如 JPG/PNG）\n"
        "- **bbox**：字符串。支持 JSON 数组 `[x1,y1,x2,y2]` 或英文逗号分隔 `x1,y1,x2,y2`（均为像素，"
        "左上角到右下角）\n\n"
        "**说明**：像素重叠、时间戳规则检测请使用 `POST .../api/v1/rule-checks` 或子接口。\n\n"
        "**输出说明**\n"
        "- 成功：`{ \"status\": \"success\", \"data\": { ...引擎结果... } }`\n"
        "- 业务失败（引擎报「错误」）：HTTP 422，`{ \"status\": \"error\", \"message\": \"...\" }`\n\n"
        "**输入示例（表单字段）**\n"
        "- `bbox`: `[100,50,500,200]` 或 `100,50,500,200`\n\n"
        "**输出示例（成功）**\n"
        "```json\n"
        "{\n"
        '  "status": "success",\n'
        '  "data": {\n'
        '    "result": "可疑",\n'
        '    "confidence": 0.58,\n'
        '    "bbox": [100, 50, 400, 150],\n'
        '    "reason": "存在局部边缘拼接/像素涂抹痕迹"\n'
        "  }\n"
        "}\n"
        "```\n\n"
        + _DETECT_RESULT_SCHEMA
    ),
    response_description="成功时为 JSON；引擎判定为错误时返回 422 JSON",
)
async def detect_tampering_endpoint(
    file: UploadFile = File(..., description="待检测图片文件"),
    bbox: str = Form(
        ...,
        description="检测框：JSON 数组 [x1,y1,x2,y2] 或逗号分隔的四个整数",
        examples=["[120,80,400,140]", "120,80,400,140"],
    ),
    engine: InferenceEngineAPI = Depends(get_engine),
    ocr_reader: Any = Depends(get_ocr_reader),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    try:
        bbox_parsed = _parse_bbox_form(bbox)
    except Exception:
        raise HTTPException(status_code=400, detail="bbox 格式无效，请使用 [x1,y1,x2,y2] 或 x1,y1,x2,y2")

    tmp_history_path: Optional[str] = None
    try:
        res, tmp_history_path = await DetectionService.process_detection(
            file,
            bbox_parsed,
            engine,
            semaphore,
            ocr_reader,
            retain_temp_for_history=True,
        )
        try:
            await run_in_threadpool(
                partial(
                    insert_ai_detection_history,
                    mode="sync_v1",
                    task_id=None,
                    original_filename=file.filename,
                    bbox=bbox_parsed,
                    status="COMPLETED",
                    outcome={"result": res},
                    source_image_path=tmp_history_path,
                ),
            )
        except Exception:
            logger.exception("AI detection sync history persist failed")
        return {"status": "success", "data": res}
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"status": "error", "message": str(exc)})
    finally:
        if tmp_history_path and os.path.exists(tmp_history_path):
            try:
                os.remove(tmp_history_path)
            except OSError:
                pass


@router.get(
    "/api/v1/history",
    summary="鉴伪检测历史记录",
    description=(
        "分页返回最近 **7 天**（可用环境变量 `AI_DETECTION_HISTORY_DAYS` 调整）内的检测记录；"
        "每次查询前会清理超过保留期的数据。\n\n"
        "**查询参数**：`page`（默认 1）、`page_size`（默认 20，最大 200）。\n\n"
        "**单条字段**：`id`、`created_at`、`mode`（sync_v1 | async_v3）、`task_id`、`original_filename`、"
        "`bbox`、`status`（COMPLETED | FAILED）、`outcome`（含 `result` / `multi_results` / `error_msg`）、"
        "`image_url`（有归档图时为 `GET /ai-detection/api/v1/history/{id}/image` 的路径前缀，否则为 null）。\n"
    ),
)
async def list_detection_history(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(20, ge=1, le=200, description="每页条数"),
):
    total, rows = await run_in_threadpool(
        partial(list_ai_detection_history, page=page, page_size=page_size),
    )
    return {
        "status": "success",
        "retention_days": HISTORY_RETENTION_DAYS,
        "total": total,
        "page": page,
        "page_size": page_size,
        "list": rows,
    }


@router.get(
    "/api/v1/history/{record_id}/image",
    summary="鉴伪历史归档图",
    description="返回该条历史记录对应的上传原图（JPEG）。无归档或记录不存在时返回 404。",
    response_class=FileResponse,
)
async def get_detection_history_image(record_id: int):
    path = await run_in_threadpool(get_ai_detection_history_image_path, record_id)
    if path is None:
        raise HTTPException(status_code=404, detail="记录不存在或未归档图片")
    return FileResponse(
        path,
        media_type="image/jpeg",
        filename=path.name,
    )


@router.post(
    "/api/v3/detect",
    summary="提交鉴伪任务（异步）",
    description=(
        "上传图片创建任务，在后台执行鉴伪；立即返回 **task_id**，再通过「查询结果」轮询。\n\n"
        "**请求方式**：`multipart/form-data`\n\n"
        "**输入（二选一）**\n"
        "1. 上传 **file**：新建任务，自动生成 `task_id` 并保存图片。\n"
        "2. 仅传 **task_id**：对已有任务重新触发排队（一般与上传二选一）。\n\n"
        "可选 **bbox**：与 v1 相同格式；**不传**则使用 EasyOCR 自动框选图中疑似单号/金额等数字区域，"
        "对每个框分别推理，结果在 `multi_results` 中。\n"
        "像素重叠、时间戳请并行调用 `POST .../api/v1/rule-checks` 或子接口。\n\n"
        "**输出示例（受理成功）**\n"
        "```json\n"
        "{\n"
        '  "status": "pending",\n'
        '  "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"\n'
        "}\n"
        "```\n\n"
        "**说明**：若未预加载 OCR 与模型，后台任务会在首次执行时再加载；接口本身会优先返回 `task_id`。\n"
    ),
    response_description="受理后返回 pending 与 task_id",
)
async def submit_detection(
    background_tasks: BackgroundTasks,
    task_id: Optional[str] = Form(None, description="已有任务 ID（与 file 二选一）"),
    file: Optional[UploadFile] = File(None, description="待检测图片；上传则创建新任务"),
    bbox: Optional[str] = Form(
        None,
        description="可选。指定框 [x1,y1,x2,y2]；不传则自动 OCR 多框检测",
        examples=["[120,80,400,140]"],
    ),
    registry: AbstractTaskRegistry = Depends(get_registry),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    if file:
        task_id = str(uuid.uuid4())
        file_path = STORAGE_DIR / f"{task_id}.jpg"
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        await registry.create_task(task_id, str(file_path))
    elif not task_id:
        raise HTTPException(status_code=400, detail="必须提供上传文件 file，或已有任务的 task_id")

    task = await registry.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    bbox_dto = None
    if bbox:
        try:
            arr = json.loads(bbox) if bbox.startswith("[") else [int(x.strip()) for x in bbox.split(",")]
            if len(arr) != 4:
                raise ValueError
            bbox_dto = BBoxDTO(x1=arr[0], y1=arr[1], x2=arr[2], y2=arr[3])
        except Exception:
            raise HTTPException(status_code=400, detail="bbox 格式无效，请使用 [x1,y1,x2,y2] 或 x1,y1,x2,y2")

    await registry.update_task(task_id, status=TaskStatusEnum.PENDING)
    service = DetectionDomainServiceV3(registry, semaphore)
    background_tasks.add_task(service.execute_async, task_id, task.image_path, bbox_dto)
    return {"status": "pending", "task_id": task_id}


@router.get(
    "/api/v3/result/{task_id}",
    response_model=TaskRecordDTO,
    summary="查询鉴伪任务结果",
    description=(
        "根据 **task_id** 查询异步任务状态与结果。\n\n"
        "**路径参数**：`task_id` — 提交任务时返回的 UUID。\n\n"
        "**输出说明**\n"
        "- `status` 为 `COMPLETED` 时：`result`（单框）或 `multi_results`（多框）有值。\n"
        "- `FAILED` 时查看 `error_msg`。\n"
        "- `PENDING` / `PROCESSING` 时请稍后重试。\n\n"
        "**输出示例（多框自动检测）**\n"
        "```json\n"
        "{\n"
        '  "task_id": "...",\n'
        '  "status": "COMPLETED",\n'
        '  "created_at": "2026-04-03T10:00:00",\n'
        '  "result": null,\n'
        '  "multi_results": [\n'
        "    {\n"
        '      "result": "正常",\n'
        '      "confidence": 0.25,\n'
        '      "bbox": [10, 20, 100, 30],\n'
        '      "reason": "未检出明显篡改痕迹",\n'
        '      "original_bbox": [10, 20, 110, 50]\n'
        "    }\n"
        "  ],\n"
        '  "error_msg": null\n'
        "}\n"
        "```\n\n"
        + _DETECT_RESULT_SCHEMA
    ),
    response_description="任务记录 JSON，结构见下方 Schema 与示例",
)
async def get_result(task_id: str, registry: AbstractTaskRegistry = Depends(get_registry)):
    task = await registry.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    return task


@router.get(
    "/api/v3/result/{task_id}/visualization",
    summary="获取鉴伪可视化图",
    description=(
        "任务状态为 **COMPLETED** 后，生成并在原图上绘制检测框与风险标签的 JPEG 图。\n\n"
        "**路径参数**：`task_id`\n\n"
        "**成功响应**：`image/jpeg` 二进制流（非 JSON）。\n\n"
        "**失败示例**：HTTP 400，JSON `{\"detail\": \"...\"}`（如任务未完成）。\n"
    ),
    response_class=FileResponse,
    responses={
        200: {
            "content": {"image/jpeg": {}},
            "description": "带框与文字标注的结果图",
        },
        400: {"description": "任务未完成或无法生成图"},
    },
)
async def get_visualization(
    task_id: str,
    registry: AbstractTaskRegistry = Depends(get_registry),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    service = DetectionDomainServiceV3(registry, semaphore)
    try:
        vis_path = await service.generate_visualization(task_id)
        return FileResponse(vis_path, media_type="image/jpeg")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete(
    "/api/v3/task/{task_id}",
    summary="取消或删除鉴伪任务",
    description=(
        "若任务仍为 **UPLOADED** / **PENDING**，则标记为 **CANCELED**；否则删除任务记录并清理临时图片。\n\n"
        "**输出示例**\n"
        "```json\n"
        "{ \"status\": \"success\" }\n"
        "```\n"
    ),
    response_description="固定返回 success 状态",
)
async def cancel_task(task_id: str, registry: AbstractTaskRegistry = Depends(get_registry)):
    task = await registry.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.status in [TaskStatusEnum.PENDING, TaskStatusEnum.UPLOADED, TaskStatusEnum.PROCESSING]:
        await registry.update_task(task_id, status=TaskStatusEnum.CANCELED)
    else:
        await registry.delete_task(task_id)

    return {"status": "success"}
