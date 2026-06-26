from __future__ import annotations

import asyncio
import io
import json
import logging
import mimetypes
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
import yaml

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from PIL import Image, ImageDraw

from app.ai_detection.amount_candidates import (
    build_amount_candidates,
    detect_certificate_document_override,
)
from app.ai_detection.core.utils import load_chinese_font
from app.config import AI_RULE_CHECK_PERSIST, AI_RULE_CHECK_STORE_IMAGE, UPLOAD_DIR
from app.ai_detection.easyocr_download_patch import patch_easyocr_download
from app.ai_detection.history_export import (
    EXPORT_MAX_RECORDS,
    build_export_zip,
    preview_export,
    render_annotated_jpeg,
)
from app.ai_detection.history_db import (
    HISTORY_RETENTION_DAYS,
    clear_feedback_status,
    delete_ai_detection_history,
    get_ai_detection_history_image_path,
    get_ai_detection_history_outcome,
    get_async_v3_history_by_task_id,
    get_feedback_status,
    get_latest_ai_detection_history_by_task_id,
    get_rule_checks_history_by_task_id,
    insert_ai_detection_history,
    list_ai_detection_history,
    mark_feedback_status,
    normalize_history_original_filename,
    purge_ai_detection_history_older_than,
)
from app.ai_detection.ocr_utils import build_detection_bboxes_from_tokens, run_full_image_ocr
from app.ai_detection.rule_check_display import build_rule_check_public_summary
from app.ai_detection.rule_check_history import (
    MODE_RULE_CHECKS,
    MODE_RULE_PIXEL_OVERLAP,
    MODE_RULE_TIMESTAMP,
    build_pixel_overlap_outcome,
    build_rule_check_failed_outcome,
    build_rule_checks_outcome,
    build_timestamp_outcome,
    persist_rule_check_history,
)
from app.ai_detection.rule_check_service import (
    merge_pixel_overlap_results,
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
FORGEGUARD_REPLACE_RULE_CHECKS = os.getenv("FORGEGUARD_REPLACE_RULE_CHECKS", "").strip().lower() in ("1", "true", "yes")

TASK_INTERRUPTED_MSG = (
    "检测任务已中断：后端进程曾退出并重新启动（常见于崩溃后自动拉起、部署或内存不足），"
    "任务队列在内存中已丢失。请重新点击「提交检测」；若原图仍在，无需重新选文件。"
)


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
    original_filename: Optional[str] = Field(
        None,
        description="用户上传时的原始文件名（用于历史展示；磁盘文件仍为 task_id.jpg）",
    )
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
    with_rule_checks: bool = Field(
        False,
        description="是否在 AI 鉴伪完成后自动执行规则检测并关联同一 task_id",
    )
    linked_rule_checks: Optional[Dict[str, Any]] = Field(
        None,
        description="关联的规则检测摘要（辅助核查）；含 status / reason / pixel_overlap / timestamp",
    )

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
    async def create_task(
        self,
        task_id: str,
        image_path: str,
        original_filename: Optional[str] = None,
    ) -> None:
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

    async def create_task(
        self,
        task_id: str,
        image_path: str,
        original_filename: Optional[str] = None,
    ) -> None:
        self._store[task_id] = TaskRecordDTO(
            task_id=task_id,
            status=TaskStatusEnum.UPLOADED,
            created_at=datetime.now().isoformat(),
            image_path=image_path,
            original_filename=normalize_history_original_filename(
                original_filename,
                fallback_path=image_path,
            ),
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


def _read_model_config() -> Dict[str, Any]:
    cfg_path = Path(__file__).resolve().parents[3] / "ai_detection" / "config.yaml"
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception:
        logger.exception("Read AI detection config failed")
        return {}
    return data if isinstance(data, dict) else {}


def _resolve_model_path(path_value: Any) -> str:
    raw = str(path_value or "").strip()
    if not raw:
        return ""
    p = Path(raw)
    if p.is_absolute():
        return str(p)
    return str((Path(__file__).resolve().parents[3] / "ai_detection" / p).resolve())


def _list_model_versions_from_registry() -> Dict[str, Any]:
    cfg = _read_model_config()
    paths = cfg.get("paths") if isinstance(cfg.get("paths"), dict) else {}
    training = cfg.get("training") if isinstance(cfg.get("training"), dict) else {}
    current_model = _resolve_model_path(paths.get("xgb_model_path", "models/global_layout_model.pkl"))
    registry_path = _resolve_model_path(training.get("registry_path", "models/registry.json"))
    if not registry_path or not os.path.exists(registry_path):
        return {"versions": [], "current_model": current_model}
    try:
        with open(registry_path, "r", encoding="utf-8") as f:
            registry = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"versions": [], "current_model": current_model}
    if not isinstance(registry, dict):
        registry = {"versions": []}
    registry["current_model"] = current_model
    return registry


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


def _storage_image_path(task_id: str) -> Path:
    return STORAGE_DIR / f"{task_id}.jpg"


def _bbox_dto_from_history(bbox_val: Any) -> Optional[BBoxDTO]:
    if not isinstance(bbox_val, dict):
        return None
    try:
        if "x1" in bbox_val:
            return BBoxDTO(
                x1=int(bbox_val["x1"]),
                y1=int(bbox_val["y1"]),
                x2=int(bbox_val["x2"]),
                y2=int(bbox_val["y2"]),
            )
    except (TypeError, ValueError):
        return None
    return None


def build_task_record_from_persistence(task_id: str) -> Optional[TaskRecordDTO]:
    """
    内存任务丢失时（如进程重启）从 DB 历史或磁盘原图恢复 TaskRecordDTO。
    若仅有原图、无历史，返回 FAILED 并提示重新提交。
    """
    tid = str(task_id or "").strip()
    if not tid:
        return None

    storage_path = _storage_image_path(tid)
    history = get_async_v3_history_by_task_id(tid)
    image_path = str(storage_path) if storage_path.is_file() else None
    created_at = datetime.now().isoformat()
    original_filename: Optional[str] = None
    bbox_dto: Optional[BBoxDTO] = None

    if history:
        created_at = str(history.get("created_at") or created_at)
        original_filename = history.get("original_filename")
        bbox_dto = _bbox_dto_from_history(history.get("bbox"))
        outcome = history.get("outcome") or {}
        if history.get("status") == "COMPLETED":
            linked = outcome.get("linked_rule_checks")
            if linked is None:
                rule_row = get_rule_checks_history_by_task_id(tid)
                if rule_row:
                    linked = build_rule_check_public_summary(rule_row.get("outcome") or {})
            return TaskRecordDTO(
                task_id=tid,
                status=TaskStatusEnum.COMPLETED,
                created_at=created_at,
                image_path=image_path,
                original_filename=original_filename,
                bbox=bbox_dto,
                result=outcome.get("result"),
                multi_results=outcome.get("multi_results"),
                linked_rule_checks=linked,
            )
        if history.get("status") == "FAILED":
            return TaskRecordDTO(
                task_id=tid,
                status=TaskStatusEnum.FAILED,
                created_at=created_at,
                image_path=image_path,
                original_filename=original_filename,
                bbox=bbox_dto,
                error_msg=outcome.get("error_msg") or TASK_INTERRUPTED_MSG,
            )

    if image_path:
        logger.warning(
            "Task %s not in memory registry; image still on disk — likely process recycle (crash/deploy/OOM)",
            tid,
        )
        return TaskRecordDTO(
            task_id=tid,
            status=TaskStatusEnum.FAILED,
            created_at=created_at,
            image_path=image_path,
            original_filename=original_filename,
            error_msg=TASK_INTERRUPTED_MSG,
        )
    return None


async def resolve_task_record(
    task_id: str,
    registry: AbstractTaskRegistry,
) -> Optional[TaskRecordDTO]:
    """先查内存注册表，再查历史/磁盘兜底。"""
    task = await registry.get_task(task_id)
    if task:
        return task
    return await run_in_threadpool(build_task_record_from_persistence, task_id)


async def ensure_task_in_registry_for_retry(
    task_id: str,
    registry: AbstractTaskRegistry,
) -> TaskRecordDTO:
    """提交检测时：内存无任务则从历史/磁盘恢复并写回注册表，便于仅 task_id 重新排队。"""
    task = await resolve_task_record(task_id, registry)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    if not task.image_path or not Path(task.image_path).is_file():
        raise HTTPException(status_code=404, detail="任务原图已不存在，请重新上传图片")

    existing = await registry.get_task(task_id)
    if not existing:
        await registry.create_task(
            task_id,
            task.image_path,
            original_filename=task.original_filename,
        )
        restored = await registry.get_task(task_id)
        if restored:
            return restored
    return task


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


def _parse_bboxes_form(raw: str) -> List[List[int]]:
    """解析多框参数：JSON 数组的数组 [[x1,y1,x2,y2], ...]。

    也兼容前端传单个框的数组形式 [[x1,y1,x2,y2]]。"""
    clean = raw.strip().strip("'").strip('"').strip()
    if not clean.startswith("["):
        raise ValueError("bboxes 格式无效，请使用 [[x1,y1,x2,y2], ...]")
    parsed = json.loads(clean)
    if not isinstance(parsed, list):
        raise ValueError("bboxes 必须是数组")
    bboxes: List[List[int]] = []
    for i, item in enumerate(parsed):
        if not isinstance(item, list) or len(item) != 4:
            raise ValueError(f"bboxes[{i}] 格式无效，每项须为 [x1,y1,x2,y2]")
        bboxes.append([int(x) for x in item])
    if not bboxes:
        raise ValueError("bboxes 不能为空数组")
    return bboxes


class RuleCheckService:
    """规则类检测（像素重叠、时间戳），与完整 AI 鉴伪解耦。"""

    @staticmethod
    async def _save_upload_to_temp(file: UploadFile) -> str:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
            tmp.write(await file.read())
            return tmp.name

    @staticmethod
    async def _persist_rule_check_history(
        *,
        mode: str,
        original_filename: Optional[str],
        bbox: Optional[List[int]],
        bboxes: Optional[List[List[int]]] = None,
        status: str,
        outcome: Dict[str, Any],
        tmp_path: Optional[str],
        task_id: Optional[str] = None,
        image_created_at: Optional[str] = None,
    ) -> None:
        if not AI_RULE_CHECK_PERSIST:
            return
        source_image_path = None
        if status == "COMPLETED" and AI_RULE_CHECK_STORE_IMAGE and tmp_path and os.path.isfile(tmp_path):
            source_image_path = tmp_path
        await run_in_threadpool(
            partial(
                persist_rule_check_history,
                mode=mode,
                original_filename=original_filename,
                bbox=bbox,
                bboxes=bboxes,
                status=status,
                outcome=outcome,
                source_image_path=source_image_path,
                task_id=task_id,
                image_created_at=image_created_at,
            ),
        )

    @staticmethod
    async def _process_via_forgeguard(
        file: UploadFile,
        *,
        bbox_list: Optional[List[int]] = None,
        bboxes_list: Optional[List[List[int]]] = None,
        business_datetime: Optional[str] = None,
        task_id: Optional[str] = None,
        image_created_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """FORGEGUARD_REPLACE_RULE_CHECKS=1 时，将规则检测请求转发到 ForgeGuard。"""
        import requests as _requests

        from app.ai_detection.forgeguard_client import (
            FORGEGUARD_BASE_URL,
            forgeguard_detect,
            forgeguard_verify,
        )

        image_bytes = await file.read()
        if not image_bytes:
            raise ValueError("上传文件为空")
        filename = file.filename or "image.jpg"

        roi_bbox = bbox_list
        detection_bboxes = bboxes_list
        if roi_bbox is None and detection_bboxes:
            roi_bbox = detection_bboxes[0]

        tmp_path: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
                tmp.write(image_bytes)
                tmp_path = tmp.name
            if roi_bbox is not None:
                # 有 bbox → /verify（区域验证 + 重叠分析）
                raw = await run_in_threadpool(
                    forgeguard_verify,
                    image_bytes,
                    roi_bbox=roi_bbox,
                    detection_bboxes=detection_bboxes,
                    filename=filename,
                )
                data = raw.get("data") or raw
                htf = data.get("hard_tamper_flags") or {}
                overlap = data.get("bbox_overlap_check") or {}

                converted: Dict[str, Any] = {
                    "pixel_overlap": {
                        "pixel_overlap_score": data.get("confidence", 0),
                        "alert": bool(htf.get("bbox_iou")) or data.get("result") in ("篡改", "可疑"),
                        "hard_tamper": bool(htf.get("bbox_iou")),
                        "bbox": data.get("bbox"),
                        "reasons": [data.get("reason", "")] if data.get("reason") else [],
                        "overlap_metrics": {},
                    },
                    "pixel_overlap_source": "forgeguard_verify",
                    "suggested_rois": None,
                    "timestamp": {
                        "timestamp_check": {},
                        "risk": data.get("confidence", 0),
                        "reasons": [],
                        "anomalies": [],
                        "hard_tamper": False,
                        "business_mismatch": False,
                    },
                    "hard_tamper_flags": {
                        "pixel_overlap": bool(htf.get("bbox_iou")),
                        "timestamp": False,
                    },
                    "reason": data.get("reason") or "未检出明显规则类异常",
                    "forgeguard_overlap": overlap,
                }
            else:
                # 无 bbox → /detect（整图三引擎检测）
                raw = await run_in_threadpool(
                    forgeguard_detect, image_bytes, filename=filename, technique="auto",
                )
                prediction = raw.get("prediction", "authentic")
                confidence = float(raw.get("confidence", 0) or 0)
                is_tampered = prediction == "forged"
                is_suspicious = prediction == "uncertain"
                regions = raw.get("forgery_regions") or []

                converted = {
                    "pixel_overlap": {
                        "pixel_overlap_score": confidence,
                        "alert": is_tampered or is_suspicious,
                        "hard_tamper": is_tampered,
                        "reasons": [f"ForgeGuard 整图: {prediction} (confidence={confidence:.2f})"],
                        "overlap_metrics": {},
                    },
                    "pixel_overlap_source": "forgeguard_detect",
                    "suggested_rois": [
                        {
                            "bbox": [r.get("x"), r.get("y"), r.get("w"), r.get("h")],
                            "label": r.get("label", ""),
                            "type": r.get("type", ""),
                            "source": "forgeguard",
                        }
                        for r in regions
                    ] if regions else None,
                    "timestamp": {
                        "timestamp_check": {},
                        "risk": confidence,
                        "reasons": [],
                        "anomalies": [],
                        "hard_tamper": False,
                        "business_mismatch": False,
                    },
                    "hard_tamper_flags": {
                        "pixel_overlap": is_tampered,
                        "timestamp": False,
                    },
                    "reason": f"ForgeGuard 整图: {prediction} (confidence={confidence:.2f})",
                    "forgeguard_detect": {
                        "prediction": prediction,
                        "confidence": confidence,
                        "technique": raw.get("technique"),
                        "votes_forged": raw.get("votes_forged"),
                        "detectors": raw.get("detectors"),
                    },
                }

            await RuleCheckService._persist_rule_check_history(
                mode=MODE_RULE_CHECKS,
                original_filename=file.filename,
                bbox=bbox_list,
                bboxes=bboxes_list,
                status="COMPLETED",
                outcome=build_rule_checks_outcome(
                    converted,
                    bbox=bbox_list,
                    bboxes=bboxes_list,
                    document_time=business_datetime,
                ),
                tmp_path=tmp_path,
                task_id=task_id,
                image_created_at=image_created_at,
            )

            return converted

        except _requests.ConnectionError:
            raise ValueError(f"ForgeGuard 服务连接失败 ({FORGEGUARD_BASE_URL})，请确认服务已启动")
        except _requests.Timeout:
            raise ValueError(f"ForgeGuard 服务响应超时 ({FORGEGUARD_BASE_URL})")
        except _requests.HTTPError as exc:
            detail = str(exc)
            try:
                detail = str(exc.response.json() if exc.response is not None else str(exc))
            except Exception:
                pass
            raise ValueError(f"ForgeGuard 返回错误: {detail}")
        except ValueError:
            raise
        except Exception as exc:
            logger.exception("forgeguard rule checks failed")
            raise ValueError(f"ForgeGuard 检测异常: {exc}")
        finally:
            if tmp_path and os.path.isfile(tmp_path):
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

    @staticmethod
    async def process_rule_checks(
        file: UploadFile,
        engine: InferenceEngineAPI,
        semaphore: asyncio.Semaphore,
        ocr_reader: Any,
        *,
        bbox_list: Optional[List[int]] = None,
        bboxes_list: Optional[List[List[int]]] = None,
        business_datetime: Optional[str] = None,
        task_id: Optional[str] = None,
        image_created_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        tmp_path: Optional[str] = None
        try:
            if FORGEGUARD_REPLACE_RULE_CHECKS:
                return await RuleCheckService._process_via_forgeguard(
                    file, bbox_list=bbox_list, bboxes_list=bboxes_list,
                    business_datetime=business_datetime, task_id=task_id,
                    image_created_at=image_created_at,
                )
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
                data = await run_in_threadpool(
                    partial(
                        run_rule_checks,
                        tmp_path,
                        engine.pixel_detector,
                        bbox_xyxy=bbox_list,
                        bboxes=bboxes_list,
                        business_datetime=business_datetime,
                        ocr_tokens=ocr_tokens or None,
                        image_shape=image_shape,
                        thresholds=engine.config.get("thresholds", {}),
                        business_rules=engine.config.get("business_rules", {}),
                        image_bgr=img_cv2,
                    ),
                )
            await RuleCheckService._persist_rule_check_history(
                mode=MODE_RULE_CHECKS,
                original_filename=file.filename,
                bbox=bbox_list,
                bboxes=bboxes_list,
                status="COMPLETED",
                outcome=build_rule_checks_outcome(
                    data,
                    bbox=bbox_list,
                    bboxes=bboxes_list,
                    document_time=business_datetime,
                ),
                tmp_path=tmp_path,
                task_id=task_id,
                image_created_at=image_created_at,
            )
            return data
        except ValueError as exc:
            await RuleCheckService._persist_rule_check_history(
                mode=MODE_RULE_CHECKS,
                original_filename=file.filename,
                bbox=bbox_list,
                bboxes=bboxes_list,
                status="FAILED",
                outcome=build_rule_check_failed_outcome(
                    MODE_RULE_CHECKS,
                    str(exc),
                    bbox=bbox_list,
                    document_time=business_datetime,
                ),
                tmp_path=None,
                task_id=task_id,
                image_created_at=image_created_at,
            )
            raise
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    @staticmethod
    async def process_rule_checks_from_path(
        image_path: str,
        engine: InferenceEngineAPI,
        semaphore: asyncio.Semaphore,
        ocr_reader: Any,
        *,
        original_filename: Optional[str] = None,
        bbox_list: Optional[List[int]] = None,
        business_datetime: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """对已落盘图片执行规则检测（供 v3 任务链式调用）。"""
        async with semaphore:
            img_cv2, ocr_tokens = await run_in_threadpool(run_full_image_ocr, image_path, ocr_reader)
            image_shape = None
            if img_cv2 is not None:
                image_shape = (
                    int(img_cv2.shape[0]),
                    int(img_cv2.shape[1]),
                    int(img_cv2.shape[2]) if len(img_cv2.shape) > 2 else 3,
                )
            data = await run_in_threadpool(
                partial(
                    run_rule_checks,
                    image_path,
                    engine.pixel_detector,
                    bbox_xyxy=bbox_list,
                    business_datetime=business_datetime,
                    ocr_tokens=ocr_tokens or None,
                    image_shape=image_shape,
                    thresholds=engine.config.get("thresholds", {}),
                    business_rules=engine.config.get("business_rules", {}),
                    image_bgr=img_cv2,
                ),
            )
        await RuleCheckService._persist_rule_check_history(
            mode=MODE_RULE_CHECKS,
            original_filename=original_filename,
            bbox=bbox_list,
            status="COMPLETED",
            outcome=build_rule_checks_outcome(
                data,
                bbox=bbox_list,
                document_time=business_datetime,
            ),
            tmp_path=image_path,
            task_id=task_id,
        )
        return data

    @staticmethod
    async def process_pixel_overlap(
        file: UploadFile,
        bbox_list: Optional[List[int]],
        engine: InferenceEngineAPI,
        semaphore: asyncio.Semaphore,
        *,
        bboxes_list: Optional[List[List[int]]] = None,
        task_id: Optional[str] = None,
        image_created_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        tmp_path: Optional[str] = None
        try:
            tmp_path = await RuleCheckService._save_upload_to_temp(file)
            async with semaphore:
                # 多框：逐个检测后合并；单框：直接检测
                if bboxes_list:
                    all_results: List[Dict[str, Any]] = []
                    for bbox in bboxes_list:
                        r = await run_in_threadpool(
                            partial(
                                run_pixel_overlap_check,
                                tmp_path,
                                bbox,
                                engine.pixel_detector,
                                thresholds=engine.config.get("thresholds", {}),
                                margin=int(engine.config.get("business_rules", {}).get("roi_expand_margin", 15)),
                            ),
                        )
                        all_results.append(r)
                    if len(all_results) == 1:
                        data = all_results[0]
                    else:
                        data = merge_pixel_overlap_results(all_results[0], all_results[1:])
                elif bbox_list:
                    data = await run_in_threadpool(
                        partial(
                            run_pixel_overlap_check,
                            tmp_path,
                            bbox_list,
                            engine.pixel_detector,
                            thresholds=engine.config.get("thresholds", {}),
                            margin=int(engine.config.get("business_rules", {}).get("roi_expand_margin", 15)),
                        ),
                    )
                else:
                    raise ValueError("请提供 bbox 或 bboxes 参数")
            await RuleCheckService._persist_rule_check_history(
                mode=MODE_RULE_PIXEL_OVERLAP,
                original_filename=file.filename,
                bbox=bbox_list,
                bboxes=bboxes_list,
                status="COMPLETED",
                outcome=build_pixel_overlap_outcome(data, bbox=bbox_list or (bboxes_list[0] if bboxes_list else []), bboxes=bboxes_list),
                tmp_path=tmp_path,
                task_id=task_id,
                image_created_at=image_created_at,
            )
            return data
        except ValueError as exc:
            await RuleCheckService._persist_rule_check_history(
                mode=MODE_RULE_PIXEL_OVERLAP,
                original_filename=file.filename,
                bbox=bbox_list,
                bboxes=bboxes_list,
                status="FAILED",
                outcome=build_rule_check_failed_outcome(
                    MODE_RULE_PIXEL_OVERLAP,
                    str(exc),
                    bbox=bbox_list,
                ),
                tmp_path=None,
                task_id=task_id,
                image_created_at=image_created_at,
            )
            raise
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
        task_id: Optional[str] = None,
        image_created_at: Optional[str] = None,
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
                data = await run_in_threadpool(
                    partial(
                        run_timestamp_check,
                        tmp_path,
                        ocr_tokens=ocr_tokens or None,
                        image_shape=image_shape,
                        business_datetime=business_datetime,
                        thresholds=engine.config.get("thresholds", {}),
                    ),
                )
            await RuleCheckService._persist_rule_check_history(
                mode=MODE_RULE_TIMESTAMP,
                original_filename=file.filename,
                bbox=None,
                status="COMPLETED",
                outcome=build_timestamp_outcome(data, document_time=business_datetime),
                tmp_path=tmp_path,
                task_id=task_id,
                image_created_at=image_created_at,
            )
            return data
        except ValueError as exc:
            await RuleCheckService._persist_rule_check_history(
                mode=MODE_RULE_TIMESTAMP,
                original_filename=file.filename,
                bbox=None,
                status="FAILED",
                outcome=build_rule_check_failed_outcome(
                    MODE_RULE_TIMESTAMP,
                    str(exc),
                    document_time=business_datetime,
                ),
                tmp_path=None,
                task_id=task_id,
                image_created_at=image_created_at,
            )
            raise
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
        self._cached_global_feat: Optional[np.ndarray] = None

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
        """读取图片并执行一次 OCR tokenize + amount 候选构建 + 全局特征提取，结果缓存供后续复用。"""
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

    async def _run_linked_rule_checks(
        self,
        task_id: str,
        image_path: str,
        *,
        original_filename: Optional[str],
        bbox: Optional[BBoxDTO] = None,
    ) -> Dict[str, Any]:
        await ensure_ai_detection_runtime()
        engine = EngineContainer.instance
        ocr_reader = EngineContainer.ocr_reader
        if not engine or not ocr_reader:
            raise RuntimeError("AI detection runtime unavailable")

        bbox_list = [bbox.x1, bbox.y1, bbox.x2, bbox.y2] if bbox else None
        data = await RuleCheckService.process_rule_checks_from_path(
            image_path,
            engine,
            self.semaphore,
            ocr_reader,
            original_filename=original_filename,
            bbox_list=bbox_list,
            task_id=task_id,
        )
        return build_rule_check_public_summary(data)

    async def _finalize_completed_task(
        self,
        task_id: str,
        image_path: str,
        *,
        original_filename: str,
        bbox: Optional[BBoxDTO],
        result: Optional[Dict[str, Any]],
        multi_results: Optional[List[Dict[str, Any]]] = None,
        persist_bbox: Any = None,
        image_created_at: Optional[str] = None,
    ) -> None:
        linked_rule_checks: Optional[Dict[str, Any]] = None
        task = await self.registry.get_task(task_id)
        if task and task.with_rule_checks:
            try:
                linked_rule_checks = await self._run_linked_rule_checks(
                    task_id,
                    image_path,
                    original_filename=original_filename,
                    bbox=bbox,
                )
            except Exception:
                logger.exception("Task %s linked rule checks failed", task_id)

        await self.registry.update_task(
            task_id,
            status=TaskStatusEnum.COMPLETED,
            result=result,
            multi_results=multi_results,
            linked_rule_checks=linked_rule_checks,
        )
        await self._persist_history(
            task_id=task_id,
            original_filename=original_filename,
            bbox=persist_bbox if persist_bbox is not None else (bbox.model_dump() if bbox else None),
            status="COMPLETED",
            result=result,
            multi_results=multi_results,
            source_image_path=image_path,
            linked_rule_checks=linked_rule_checks,
            image_created_at=image_created_at,
        )

    async def execute_async(
        self,
        task_id: str,
        image_path: str,
        bbox: Optional[BBoxDTO] = None,
        image_created_at: Optional[str] = None,
    ) -> None:
        task = await self.registry.get_task(task_id)
        if not task or task.status == TaskStatusEnum.CANCELED:
            return

        await self.registry.update_task(task_id, status=TaskStatusEnum.PROCESSING)
        history_filename = normalize_history_original_filename(
            task.original_filename,
            fallback_path=image_path,
        )

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
                await self._finalize_completed_task(
                    task_id,
                    image_path,
                    original_filename=history_filename,
                    bbox=bbox,
                    result=res_dict,
                    image_created_at=image_created_at,
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
                    await self._finalize_completed_task(
                        task_id,
                        image_path,
                        original_filename=history_filename,
                        bbox=None,
                        result=document_override,
                        multi_results=[document_override],
                        persist_bbox={"auto_ocr": True, "note": "document_rule_override"},
                        image_created_at=image_created_at,
                    )
                    return

                empty_res = {"result": "正常", "confidence": 0.0, "reason": "未发现关键数值或单号区域"}
                await self._finalize_completed_task(
                    task_id,
                    image_path,
                    original_filename=history_filename,
                    bbox=None,
                    result=empty_res,
                    persist_bbox={"auto_ocr": True, "note": "no_numeric_regions"},
                    image_created_at=image_created_at,
                )
                return

            global_feat = self._cached_global_feat
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
            await self._finalize_completed_task(
                task_id,
                image_path,
                original_filename=history_filename,
                bbox=None,
                result=top_result,
                multi_results=ordered_results,
                persist_bbox={"auto_ocr": True, "box_count": len(ordered_results)},
                image_created_at=image_created_at,
            )

        except Exception as exc:
            logger.exception("Task %s failed", task_id)
            await self.registry.update_task(task_id, status=TaskStatusEnum.FAILED, error_msg=str(exc))
            await self._persist_history(
                task_id=task_id,
                original_filename=history_filename,
                bbox=bbox.model_dump() if bbox else None,
                status="FAILED",
                error_msg=str(exc),
                source_image_path=image_path,
                image_created_at=image_created_at,
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
        linked_rule_checks: Optional[Dict[str, Any]] = None,
        image_created_at: Optional[str] = None,
    ) -> None:
        try:
            outcome: Dict[str, Any] = {}
            if result is not None:
                outcome["result"] = result
            if multi_results is not None:
                outcome["multi_results"] = multi_results
            if error_msg:
                outcome["error_msg"] = error_msg
            if linked_rule_checks is not None:
                outcome["linked_rule_checks"] = linked_rule_checks
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
                    image_created_at=image_created_at,
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
        "- **bboxes**（可选）：多框检测 `[[x1,y1,x2,y2], ...]`；优先级高于 bbox"
        "- **document_time**（可选）：业务单据时间，与图内交易时间比对\n"
        "- **task_id**（可选）：与 `async_v3` 主鉴伪任务关联，便于历史聚合\n\n"
        "**说明**：可与 `POST .../api/v3/detect` 并行调用；主鉴伪接口不再包含像素重叠与时间戳。"
        "成功/失败均写入 `ai_detection_history`（`mode=rule_checks`，可用 `AI_RULE_CHECK_PERSIST=0` 关闭）。\n\n"
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
    bboxes: Optional[str] = Form(
        None,
        description="可选。多框像素重叠检测：[[x1,y1,x2,y2], ...]，优先级高于 bbox",
        examples=["[[120,80,400,140],[500,200,700,350]]"],
    ),
    document_time: Optional[str] = Form(
        None,
        description="可选。业务单据时间",
        examples=["2026-05-28 11:32:00"],
    ),
    task_id: Optional[str] = Form(
        None,
        description="可选。与主鉴伪 async_v3 任务关联的 UUID",
    ),
    image_created_at: Optional[str] = Form(
        None,
        description="可选。图片创建时间，格式如 2026-05-28 11:32:00",
        examples=["2026-05-28 11:32:00"],
    ),
    engine: InferenceEngineAPI = Depends(get_engine),
    ocr_reader: Any = Depends(get_ocr_reader),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    bbox_list: Optional[List[int]] = None
    bboxes_list: Optional[List[List[int]]] = None
    if bboxes:
        try:
            bboxes_list = _parse_bboxes_form(bboxes)
        except Exception:
            raise HTTPException(status_code=400, detail="bboxes 格式无效，请使用 [[x1,y1,x2,y2], ...]")
    elif bbox:
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
            bboxes_list=bboxes_list,
            business_datetime=document_time,
            task_id=task_id,
            image_created_at=image_created_at,
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
        "- **bbox**：检测框 `[x1,y1,x2,y2]`（与 bboxes 二选一）\n"
        "- **bboxes**：多框检测 `[[x1,y1,x2,y2], ...]`（与 bbox 二选一，优先级高于 bbox）\n"
        "- **task_id**（可选）：与主鉴伪 async_v3 任务关联\n"
        "\n成功/失败写入 `ai_detection_history`（`mode=rule_pixel_overlap`）。\n"
    ),
)
async def pixel_overlap_check_endpoint(
    file: UploadFile = File(..., description="待检测图片文件"),
    bbox: Optional[str] = Form(
        None,
        description="像素重叠检测 ROI：[x1,y1,x2,y2]（与 bboxes 二选一）",
        examples=["[120,80,400,140]"],
    ),
    bboxes: Optional[str] = Form(
        None,
        description="多框像素重叠检测：[[x1,y1,x2,y2], ...]（与 bbox 二选一，优先级高于 bbox）",
        examples=["[[120,80,400,140],[500,200,700,350]]"],
    ),
    task_id: Optional[str] = Form(None, description="可选。与主鉴伪 async_v3 任务关联的 UUID"),
    image_created_at: Optional[str] = Form(
        None,
        description="可选。图片创建时间，格式如 2026-05-28 11:32:00",
    ),
    engine: InferenceEngineAPI = Depends(get_engine),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    bbox_list: Optional[List[int]] = None
    bboxes_list: Optional[List[List[int]]] = None
    if bboxes:
        try:
            bboxes_list = _parse_bboxes_form(bboxes)
        except Exception:
            raise HTTPException(status_code=400, detail="bboxes 格式无效，请使用 [[x1,y1,x2,y2], ...]")
    elif bbox:
        try:
            bbox_list = _parse_bbox_form(bbox)
        except Exception:
            raise HTTPException(status_code=400, detail="bbox 格式无效，请使用 [x1,y1,x2,y2] 或 x1,y1,x2,y2")
    else:
        raise HTTPException(status_code=400, detail="请提供 bbox 或 bboxes 参数")
    try:
        data = await RuleCheckService.process_pixel_overlap(
            file, bbox_list, engine, semaphore, bboxes_list=bboxes_list, task_id=task_id,
            image_created_at=image_created_at,
        )
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
        "- **task_id**（可选）：与主鉴伪 async_v3 任务关联\n"
        "\n成功/失败写入 `ai_detection_history`（`mode=rule_timestamp`）。\n"
    ),
)
async def timestamp_check_endpoint(
    file: UploadFile = File(..., description="待检测图片文件"),
    document_time: Optional[str] = Form(
        None,
        description="可选。业务单据时间",
        examples=["2026-05-28 11:32:00"],
    ),
    task_id: Optional[str] = Form(None, description="可选。与主鉴伪 async_v3 任务关联的 UUID"),
    image_created_at: Optional[str] = Form(
        None,
        description="可选。图片创建时间，格式如 2026-05-28 11:32:00",
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
            task_id=task_id,
            image_created_at=image_created_at,
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
    image_created_at: Optional[str] = Form(
        None,
        description="可选。图片创建时间，格式如 2026-05-28 11:32:00",
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
                    image_created_at=image_created_at,
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
        "**查询参数**：`page`（默认 1）、`page_size`（默认 20，最大 200）、"
        "`mode`（可选，逗号分隔，如 `rule_checks,rule_pixel_overlap` 或 `sync_v1,async_v3`）。\n\n"
        "**单条字段**：`id`、`created_at`、`mode`（sync_v1 | async_v3 | rule_checks | rule_pixel_overlap | rule_timestamp）、"
        "`task_id`、`original_filename`、"
        "`bbox`、`status`（COMPLETED | FAILED）、`outcome`（含 `result` / `multi_results` / `error_msg` / 规则检测完整结果）、"
        "`summary`（规则检测摘要，若有）、`detection_result`（统一鉴定结果：正常/可疑/篡改，已对所有 mode 做标准化提取）、"
        "`image_url`（有归档图时为 `GET /ai-detection/api/v1/history/{id}/image` 的路径前缀，否则为 null）。\n"
    ),
)
async def list_detection_history(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(20, ge=1, le=200, description="每页条数"),
    mode: Optional[str] = Query(
        None,
        description="可选。按 mode 过滤，逗号分隔，如 rule_checks,sync_v1",
    ),
):
    modes = [m.strip() for m in mode.split(",")] if mode else None
    total, rows = await run_in_threadpool(
        partial(list_ai_detection_history, page=page, page_size=page_size, modes=modes),
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


@router.get(
    "/api/v1/history/{record_id}/image/annotated",
    summary="鉴伪历史标注图",
    description="返回该条历史记录对应的标注图（带检测框与结论的 JPEG）。无归档或记录不存在时返回 404。",
    response_class=Response,
)
async def get_detection_history_annotated_image(record_id: int):
    data = await run_in_threadpool(get_ai_detection_history_outcome, record_id)
    if data is None or data.get("image_path") is None:
        raise HTTPException(status_code=404, detail="记录不存在或未归档图片")
    try:
        jpeg_bytes = await run_in_threadpool(
            render_annotated_jpeg, data["image_path"], data["outcome"]
        )
    except ValueError:
        raise HTTPException(status_code=422, detail="图片处理失败")
    return Response(content=jpeg_bytes, media_type="image/jpeg")


@router.delete(
    "/api/v1/history/{record_id}",
    summary="删除鉴伪历史记录",
    description="删除单条历史记录，并清理其归档图片（若存在）。",
)
async def delete_detection_history(record_id: int):
    removed = await run_in_threadpool(delete_ai_detection_history, record_id)
    if not removed:
        raise HTTPException(status_code=404, detail="历史记录不存在")
    return {"status": "success"}


class HistoryExportRequest(BaseModel):
    """鉴伪历史导出/预览共用筛选条件。"""

    retention_days: Optional[int] = Field(
        None,
        ge=1,
        le=365,
        description="与 GET /history 一致：最近 N 天（UTC）；传此项时可不传 start_time/end_time，且与列表条数对齐",
    )
    start_time: Optional[datetime] = Field(
        None,
        description="开始时间（含，精确到分钟，如 2026-06-26T15:30）；未传 retention_days 时必填",
    )
    end_time: Optional[datetime] = Field(
        None,
        description="结束时间（含，精确到分钟，如 2026-06-26T18:45）；未传 retention_days 时必填",
    )
    detection_results: Optional[List[str]] = Field(
        None,
        description="鉴伪结论过滤：正常、可疑、篡改；不传或空数组表示全部",
    )
    bbox_mode: str = Field(
        "all",
        description="检测框来源：all=全部，manual=用户提交检测时手动画框，auto=自动 OCR 框选",
    )
    modes: Optional[List[str]] = Field(
        None,
        description="历史 mode；不传表示全部（与 GET /history 未传 mode 一致），如 async_v3,sync_v1,rule_checks",
    )
    status: Optional[str] = Field(
        None,
        description="记录状态 COMPLETED/FAILED；不传表示全部",
    )
    feedback_status: Optional[List[str]] = Field(
        None,
        description="人工标注：correct、wrong、suspicious、unmarked（未标注）；可多选，不传表示全部",
    )
    match_mode: str = Field(
        "primary",
        description="结论匹配：primary=按主结果 result；any=multi_results 任一条命中即保留",
    )
    batch: Optional[str] = Field(
        None,
        description="批次号筛选，如 20260626-001；不传表示全部",
    )
    image_variant: str = Field(
        "original",
        description="图片类型：original=原图，annotated=在原图上绘制检测框与结论（预览与导出均生效）",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "start_time": "2026-06-01T00:00",
                    "end_time": "2026-06-17T23:59",
                    "detection_results": ["篡改", "可疑"],
                    "bbox_mode": "all",
                    "image_variant": "original",
                    "batch": "20260617-001",
                }
            ]
        }
    )


def _parse_history_export_request(req: HistoryExportRequest) -> HistoryExportRequest:
    from app.ai_detection.history_db import HISTORY_RETENTION_DAYS

    if req.retention_days is None:
        if req.start_time is None and req.end_time is None:
            req.retention_days = HISTORY_RETENTION_DAYS
        elif req.start_time is None or req.end_time is None:
            raise HTTPException(
                status_code=400,
                detail="请传 retention_days，或同时传 start_time 与 end_time",
            )
        elif req.end_time < req.start_time:
            raise HTTPException(status_code=400, detail="end_time 不能早于 start_time")
    bbox = (req.bbox_mode or "all").strip().lower()
    if bbox not in ("all", "manual", "auto"):
        raise HTTPException(status_code=400, detail="bbox_mode 须为 all、manual 或 auto")
    match = (req.match_mode or "primary").strip().lower()
    if match not in ("primary", "any"):
        raise HTTPException(status_code=400, detail="match_mode 须为 primary 或 any")
    variant = (req.image_variant or "original").strip().lower()
    if variant not in ("original", "annotated"):
        raise HTTPException(status_code=400, detail="image_variant 须为 original 或 annotated")
    allowed_results = {"正常", "可疑", "篡改"}
    if req.detection_results:
        bad = [x for x in req.detection_results if x not in allowed_results]
        if bad:
            raise HTTPException(
                status_code=400,
                detail=f"detection_results 含非法值: {bad}，仅支持 正常、可疑、篡改",
            )
    allowed_fb = {"correct", "wrong", "suspicious", "unmarked", "none", "null", "未标注"}
    if req.feedback_status:
        bad_fb = [x for x in req.feedback_status if str(x).strip().lower() not in allowed_fb]
        if bad_fb:
            raise HTTPException(
                status_code=400,
                detail=f"feedback_status 含非法值: {bad_fb}，支持 correct、wrong、suspicious、unmarked",
            )
    if req.status is not None:
        st = str(req.status).strip().upper()
        if st and st not in ("COMPLETED", "FAILED"):
            raise HTTPException(status_code=400, detail="status 须为 COMPLETED、FAILED 或不传")
        req.status = st or None  # type: ignore[assignment]
    req.bbox_mode = bbox  # type: ignore[assignment]
    req.match_mode = match  # type: ignore[assignment]
    req.image_variant = variant  # type: ignore[assignment]
    return req


@router.post(
    "/api/v1/history/export/preview",
    summary="鉴伪历史导出预览",
    description=(
        "按时间范围、鉴伪结论、是否手动画框等条件统计并列出将参与导出的记录（默认最多返回 "
        f"{int(os.getenv('AI_DETECTION_EXPORT_PREVIEW_MAX', '200'))} 条明细）。"
        "不打包、不下载；用于导出前确认数量与是否超过单次上限。"
    ),
)
async def history_export_preview(req: HistoryExportRequest):
    req = _parse_history_export_request(req)
    data = await run_in_threadpool(
        partial(
            preview_export,
            start_time=req.start_time,
            end_time=req.end_time,
            retention_days=req.retention_days,
            detection_results=req.detection_results,
            bbox_mode=req.bbox_mode,  # type: ignore[arg-type]
            modes=req.modes,
            status=req.status,
            match_mode=req.match_mode,  # type: ignore[arg-type]
            image_variant=req.image_variant,  # type: ignore[arg-type]
            feedback_status=req.feedback_status,
            batch=req.batch,
        ),
    )
    return {"status": "success", **data}


@router.post(
    "/api/v1/history/export",
    summary="导出鉴伪历史图片 ZIP",
    description=(
        "筛选条件与预览接口相同；将匹配记录的图片打入 ZIP 并直接下载。"
        f"单次最多 {EXPORT_MAX_RECORDS} 条（可用环境变量 AI_DETECTION_EXPORT_MAX_RECORDS 调整）。"
        "ZIP 内含 `images/` 与根目录 `export_manifest.json`。"
    ),
    response_class=StreamingResponse,
)
async def history_export_download(req: HistoryExportRequest):
    req = _parse_history_export_request(req)
    preview = await run_in_threadpool(
        partial(
            preview_export,
            start_time=req.start_time,
            end_time=req.end_time,
            retention_days=req.retention_days,
            detection_results=req.detection_results,
            bbox_mode=req.bbox_mode,  # type: ignore[arg-type]
            modes=req.modes,
            status=req.status,
            match_mode=req.match_mode,  # type: ignore[arg-type]
            image_variant=req.image_variant,  # type: ignore[arg-type]
            feedback_status=req.feedback_status,
            batch=req.batch,
        ),
    )
    if preview["total_matched"] == 0:
        raise HTTPException(status_code=404, detail="没有符合筛选条件的记录")
    if preview["exceeds_limit"]:
        raise HTTPException(
            status_code=413,
            detail=(
                f"匹配 {preview['total_matched']} 条，超过单次导出上限 {EXPORT_MAX_RECORDS}，"
                "请缩小时间范围或增加 detection_results 过滤"
            ),
        )

    try:
        zip_bytes, filename, stats = await run_in_threadpool(
            partial(
                build_export_zip,
                start_time=req.start_time,
                end_time=req.end_time,
                retention_days=req.retention_days,
                detection_results=req.detection_results,
                bbox_mode=req.bbox_mode,  # type: ignore[arg-type]
                modes=req.modes,
                status=req.status,
                match_mode=req.match_mode,  # type: ignore[arg-type]
                image_variant=req.image_variant,  # type: ignore[arg-type]
                feedback_status=req.feedback_status,
                batch=req.batch,
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Export-Record-Count": str(stats["record_count"]),
        "X-Export-Images-Added": str(stats["images_added"]),
        "X-Export-Skipped-No-Image": str(stats["skipped_no_image"]),
    }
    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers=headers,
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
    with_rule_checks: bool = Form(
        False,
        description="AI 鉴伪完成后自动执行规则检测，并写入同一 task_id 供辅助核查聚合",
    ),
    image_created_at: Optional[str] = Form(
        None,
        description="可选。图片创建时间，格式如 2026-05-28 11:32:00",
    ),
    registry: AbstractTaskRegistry = Depends(get_registry),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    if file:
        task_id = str(uuid.uuid4())
        file_path = STORAGE_DIR / f"{task_id}.jpg"
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        await registry.create_task(
            task_id,
            str(file_path),
            original_filename=file.filename,
        )
    elif not task_id:
        raise HTTPException(status_code=400, detail="必须提供上传文件 file，或已有任务的 task_id")

    if file:
        task = await registry.get_task(task_id)
    else:
        task = await ensure_task_in_registry_for_retry(task_id, registry)
    if not task or not task.image_path:
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

    await registry.update_task(task_id, status=TaskStatusEnum.PENDING, with_rule_checks=with_rule_checks)
    service = DetectionDomainServiceV3(registry, semaphore)
    background_tasks.add_task(service.execute_async, task_id, task.image_path, bbox_dto, image_created_at)
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
    task = await resolve_task_record(task_id, registry)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.linked_rule_checks is None:
        rule_row = await run_in_threadpool(get_rule_checks_history_by_task_id, task_id)
        if rule_row:
            task.linked_rule_checks = build_rule_check_public_summary(rule_row.get("outcome") or {})
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


# ---- 人工标注反馈系统 ----

class JudgmentRequest(BaseModel):
    task_id: str
    judgment: str = Field(..., pattern="^(correct|wrong|suspicious)$")
    bbox: Optional[List[int]] = None
    note: str = ""


class FeedbackUpdateRequest(BaseModel):
    judgment: str = Field(..., pattern="^(correct|wrong|suspicious)$")
    note: Optional[str] = None


class DatasetUpdateRequest(BaseModel):
    label: int = Field(..., ge=0, le=1, description="训练标签：0=正常，1=篡改")


@router.post(
    "/api/v3/feedback/judge",
    summary="提交人工判断标注",
    description=(
        "对鉴伪结果进行人工标注，支持 **correct**（正确）、**wrong**（错误）、**suspicious**（疑似）三种判定。\n\n"
        "- **wrong**：保存原图 + 框选区域裁剪图 + 完整元数据到 feedback/wrong/ 目录\n"
        "- **suspicious**：保存到 feedback/suspicious/ 待确认目录\n"
        "- **correct**：保存到 feedback/correct/ 目录\n\n"
        "**请求体**：JSON\n"
        "- `task_id`：任务 ID\n"
        "- `judgment`：判定结果（correct | wrong | suspicious）\n"
        "- `bbox`（可选）：标注框 [x1, y1, x2, y2]，不传则使用检测结果中的框\n"
        "- `note`（可选）：备注说明\n"
    ),
)
async def submit_judgment(
    req: JudgmentRequest,
    registry: AbstractTaskRegistry = Depends(get_registry),
):
    from app.ai_detection.feedback_manager import FeedbackManager

    # 检查是否已标注（一个检测任务只允许标注一次）
    existing_status = await run_in_threadpool(get_feedback_status, req.task_id)
    if existing_status:
        raise HTTPException(
            409,
            f"该任务已标注为「{existing_status}」，不可重复标注。如需修改请使用 PATCH 接口或先删除原标注。",
        )

    fb = FeedbackManager()

    task = await registry.get_task(req.task_id)
    image_path: Optional[str] = None
    result: Dict[str, Any] = {}
    if task:
        image_path = task.image_path
        result = task.result or {}
    else:
        # 内存注册表中不存在时，从持久化历史记录回退（服务重启/GC 后仍可标注）
        history = await run_in_threadpool(get_latest_ai_detection_history_by_task_id, req.task_id)
        if not history:
            raise HTTPException(404, "任务不存在")
        image_path = str(history["image_path"])
        outcome = history.get("outcome") or {}
        result = outcome.get("result") or {}

    bbox = req.bbox
    if bbox is None:
        bbox = result.get("original_bbox") or result.get("bbox")
    entry = fb.save_judgment(
        task_id=req.task_id,
        judgment=req.judgment,
        image_path=image_path,
        bbox=bbox,
        result=result,
        note=req.note,
    )
    # 同步标注状态到数据库
    await run_in_threadpool(mark_feedback_status, req.task_id, req.judgment)
    logger.info("反馈已保存: task=%s judgment=%s entry=%s", req.task_id, req.judgment, entry.get("entry_id"))
    return {"status": "success", "entry": entry}


@router.get(
    "/api/v3/feedback/list",
    summary="列出反馈记录",
    description=(
        "列出所有反馈记录，可按判定类型过滤。\n\n"
        "**查询参数**：`judgment`（可选）— 过滤 correct / wrong / suspicious\n"
    ),
)
async def list_feedback(judgment: Optional[str] = Query(None, pattern="^(correct|wrong|suspicious)$")):
    from app.ai_detection.feedback_manager import FeedbackManager

    fb = FeedbackManager()
    entries = fb.list_entries(judgment_filter=judgment)
    return {"total": len(entries), "items": entries}


@router.get(
    "/api/v3/feedback/{folder_name}",
    summary="获取反馈详情",
    description="按反馈条目文件夹名返回元数据、AI 原始结果、图片访问地址等。",
)
async def get_feedback_detail(folder_name: str):
    from app.ai_detection.feedback_manager import FeedbackManager

    fb = FeedbackManager()
    entry = fb.get_entry(folder_name)
    if not entry:
        raise HTTPException(404, "反馈条目不存在")
    return {"status": "success", "entry": entry}


@router.get(
    "/api/v3/feedback/{folder_name}/image",
    summary="获取反馈原图",
    response_class=FileResponse,
)
async def get_feedback_image(folder_name: str):
    from app.ai_detection.feedback_manager import FeedbackManager

    fb = FeedbackManager()
    path = fb.get_entry_file(folder_name, "image")
    if path is None:
        raise HTTPException(404, "反馈原图不存在")
    media_type, _enc = mimetypes.guess_type(path.name)
    return FileResponse(str(path), media_type=media_type or "application/octet-stream", filename=path.name)


@router.get(
    "/api/v3/feedback/{folder_name}/roi",
    summary="获取反馈裁剪区域图",
    response_class=FileResponse,
)
async def get_feedback_roi(folder_name: str):
    from app.ai_detection.feedback_manager import FeedbackManager

    fb = FeedbackManager()
    path = fb.get_entry_file(folder_name, "roi")
    if path is None:
        raise HTTPException(404, "反馈裁剪图不存在")
    return FileResponse(str(path), media_type="image/jpeg", filename=path.name)


@router.patch(
    "/api/v3/feedback/{folder_name}",
    summary="修改反馈判断",
    description="在 correct / wrong / suspicious 之间移动反馈条目，可用于纠错或撤回疑似状态。",
)
async def update_feedback(folder_name: str, req: FeedbackUpdateRequest):
    from app.ai_detection.feedback_manager import FeedbackManager

    fb = FeedbackManager()
    entry = fb.update_entry(folder_name, req.judgment, note=req.note)
    if not entry:
        raise HTTPException(404, "反馈条目不存在")
    # 同步标注状态到数据库
    task_id = entry.get("task_id")
    if task_id:
        await run_in_threadpool(mark_feedback_status, task_id, req.judgment)
    return {"status": "success", "entry": entry}


@router.delete(
    "/api/v3/feedback/{folder_name}",
    summary="删除/撤销反馈标注",
)
async def delete_feedback(folder_name: str):
    from app.ai_detection.feedback_manager import FeedbackManager

    fb = FeedbackManager()
    # 删除前先获取 task_id
    entry = fb.get_entry(folder_name)
    task_id = entry.get("task_id") if entry else None
    if not fb.delete_entry(folder_name):
        raise HTTPException(404, "反馈条目不存在")
    # 同步清除数据库标注状态（恢复为可再次标注）
    if task_id:
        await run_in_threadpool(clear_feedback_status, task_id)
    return {"status": "success"}


@router.post(
    "/api/v3/feedback/confirm",
    summary="确认疑似标注转向",
    description=(
        "将 suspicious（疑似）条目确认后移入 correct 或 wrong 目录。\n\n"
        "**请求体**：`multipart/form-data`\n"
        "- `folder_name`：疑似条目文件夹名\n"
        "- `judgment`：最终判定（correct | wrong）\n"
    ),
)
async def confirm_suspicious(folder_name: str = Form(...), judgment: str = Form(..., pattern="^(correct|wrong)$")):
    from app.ai_detection.feedback_manager import FeedbackManager

    fb = FeedbackManager()
    entry = fb.confirm_suspicious(folder_name, judgment)
    if not entry:
        raise HTTPException(404, "疑似条目不存在或已处理")
    # 同步标注状态到数据库
    task_id = entry.get("task_id")
    if task_id:
        await run_in_threadpool(mark_feedback_status, task_id, judgment)
    return {"status": "success", "entry": entry}


# ---- 原始训练集管理 ----

@router.get(
    "/api/v3/training-dataset/list",
    summary="列出图片检测训练集样本",
    description="列出训练管线读取的 images/ 样本，可按 label 过滤。label=0 为正常，label=1 为篡改。",
)
async def list_training_dataset(
    label: Optional[int] = Query(None, ge=0, le=1, description="可选。0=正常，1=篡改"),
    include_enhanced: bool = Query(True, description="是否包含 *_enhanced 增强样本"),
):
    from app.ai_detection.dataset_manager import DatasetManager

    manager = DatasetManager()
    entries = await run_in_threadpool(manager.list_entries, label, include_enhanced)
    return {
        "status": "success",
        "summary": manager.summary(),
        "total": len(entries),
        "items": entries,
    }


@router.get(
    "/api/v3/training-dataset/{filename}/image",
    summary="获取训练集样本原图",
    response_class=FileResponse,
)
async def get_training_dataset_image(filename: str):
    from app.ai_detection.dataset_manager import DatasetManager

    manager = DatasetManager()
    path = await run_in_threadpool(manager.get_image_file, filename)
    if path is None:
        raise HTTPException(404, "训练样本不存在")
    return FileResponse(str(path), media_type=manager.image_media_type(path.name), filename=path.name)


@router.get(
    "/api/v3/training-dataset/{filename}/annotation",
    summary="获取训练集样本区域标注 JSON",
)
async def get_training_dataset_annotation(filename: str):
    from app.ai_detection.dataset_manager import DatasetManager

    manager = DatasetManager()
    annotation = await run_in_threadpool(manager.get_annotation, filename)
    if annotation is None:
        raise HTTPException(404, "训练样本标注不存在")
    return {"status": "success", "annotation": annotation}


@router.patch(
    "/api/v3/training-dataset/{filename}",
    summary="修改训练集样本标签",
    description="通过重命名样本及其 *_enhanced 配套图、locate_json 标注来修改训练标签。",
)
async def update_training_dataset_entry(filename: str, req: DatasetUpdateRequest):
    from app.ai_detection.dataset_manager import DatasetManager

    manager = DatasetManager()
    try:
        entry = await run_in_threadpool(manager.update_label, filename, req.label)
    except FileExistsError as exc:
        raise HTTPException(409, str(exc))
    if entry is None:
        raise HTTPException(404, "训练样本不存在")
    return {"status": "success", "entry": entry, "summary": manager.summary()}


@router.delete(
    "/api/v3/training-dataset/{filename}",
    summary="删除训练集样本",
    description="删除该样本，默认同时删除同名 *_enhanced 配套图和 locate_json 标注。",
)
async def delete_training_dataset_entry(
    filename: str,
    delete_family: bool = Query(True, description="是否同时删除同一基础样本的增强图和 JSON 标注"),
):
    from app.ai_detection.dataset_manager import DatasetManager

    manager = DatasetManager()
    removed = await run_in_threadpool(manager.delete_entry, filename, delete_family)
    if not removed:
        raise HTTPException(404, "训练样本不存在")
    return {"status": "success", "summary": manager.summary()}


# ---- 训练端点 (含风险提示) ----

class TrainResponse(BaseModel):
    status: str
    warning: str = "训练将使用反馈数据+原始数据集重新训练模型。这将覆盖当前模型（旧模型自动备份）。训练期间 GPU 资源占用高，可能影响正在进行的检测任务。"
    summary: Optional[Dict[str, Any]] = None


@router.post(
    "/api/v3/train",
    summary="触发模型训练（含风险警告）",
    description=(
        "使用反馈数据 + 原始数据集重新训练全局模型与字体库。\n\n"
        "**风险提示**：训练将覆盖当前模型文件（旧模型自动备份）。训练期间 GPU 资源占用高，可能影响正在进行的检测任务。\n\n"
        "**请求体**：`multipart/form-data`\n"
        "- `confirm`：必须设为 `true` 以确认风险并开始训练\n"
    ),
)
async def trigger_training(
    confirm: bool = Form(False, description="必须设为 true 以确认风险"),
    engine: "InferenceEngineAPI" = Depends(get_engine),
    ocr_reader: Any = Depends(get_ocr_reader),
):
    if not confirm:
        return TrainResponse(
            status="aborted",
            warning="请仔细阅读风险提示，确认后将 confirm 设为 true 重新提交。",
        )

    from app.ai_detection.train_pipeline_v2 import TrainPipeline

    try:
        summary = await run_in_threadpool(
            lambda: TrainPipeline(ocr_reader=ocr_reader).run()
        )
        return TrainResponse(status="completed", summary=summary)
    except Exception as e:
        logger.exception("训练失败")
        return TrainResponse(status="failed", warning=f"训练异常: {str(e)}")


@router.get(
    "/api/v3/train/viz/{filename}",
    summary="获取训练可视化图片",
    description=(
        "获取训练过程中生成的可视化图片（特征重要性、分数分布、学习曲线）。\n\n"
        "**路径参数**：`filename` — 可视化文件名（如 `feature_importance_20250101_120000.png`）\n"
    ),
)
async def get_train_visualization(filename: str):
    from app.ai_detection.train_pipeline_v2 import TrainPipeline

    pipeline = TrainPipeline()
    viz_file = pipeline.viz_dir / filename
    if not viz_file.exists():
        raise HTTPException(404, "可视化图片不存在")
    return FileResponse(str(viz_file), media_type="image/png")


# ---- 运维端点：健康检查 / 模型版本 / 模型重载 ----


@router.get(
    "/api/v3/health",
    summary="AI 鉴伪服务健康检查",
    description=(
        "返回推理引擎状态指标，包括模型、字体库、OCR 就绪情况。\n\n"
        "前端可在鉴伪页面加载时调用，用于展示服务可用性。"
    ),
)
async def health_check():
    engine = EngineContainer.instance
    ocr_reader = EngineContainer.ocr_reader
    metrics = engine.get_metrics() if engine is not None else {}
    return {
        "status": "ok",
        "font_lib_ready": bool(metrics.get("font_lib_ready", False)),
        "font_lib_size": int(metrics.get("font_lib_size", 0)),
        "global_model_loaded": bool(engine is not None and engine.global_model is not None),
        "ocr_available": ocr_reader is not None,
        "metrics": {
            "total_predictions": metrics.get("total_predictions", 0),
            "tampered_count": metrics.get("tampered_count", 0),
            "suspicious_count": metrics.get("suspicious_count", 0),
            "normal_count": metrics.get("normal_count", 0),
            "error_count": metrics.get("error_count", 0),
            "avg_inference_ms": metrics.get("avg_inference_ms", 0),
            "inference_p50_ms": metrics.get("inference_p50_ms", 0),
            "inference_p99_ms": metrics.get("inference_p99_ms", 0),
        },
    }


@router.get(
    "/api/v3/models",
    summary="模型版本列表",
    description="返回模型注册表中所有版本及当前活跃模型。",
)
async def list_models():
    engine = EngineContainer.instance
    if engine is not None:
        return engine.list_model_versions()
    return _list_model_versions_from_registry()


@router.post(
    "/api/v3/reload",
    summary="热重载模型",
    description=(
        "无需重启服务即可重载 FAISS 字体库和 XGBoost 全局模型。\n\n"
        "- `version`（可选）：指定注册表中的版本时间戳；不传则重新加载当前版本。\n"
        "Python 属性赋值为原子操作，读取端无锁安全。"
    ),
)
async def reload_model(
    version: Optional[str] = Form(None),
    engine: "InferenceEngineAPI" = Depends(get_engine),
):
    result = await run_in_threadpool(lambda: engine.reload_models(version))
    return {"status": "ok", "detail": result}
