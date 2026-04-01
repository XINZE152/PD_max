import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import cv2
import numpy as np
import torch
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from PIL import Image, ImageDraw, ImageFont

from app.ai_detection.inference_api import InferenceEngineAPI
from app.config import UPLOAD_DIR

logger = logging.getLogger(__name__)

STORAGE_DIR = Path(UPLOAD_DIR) / "ai_detection_storage"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

MAX_CONCURRENT_AI_TASKS = int(os.getenv("AI_MAX_CONCURRENT_TASKS", "1"))
GC_MAX_AGE_HOURS = int(os.getenv("AI_GC_MAX_AGE_HOURS", "24"))
GC_INTERVAL_SECONDS = int(os.getenv("AI_GC_INTERVAL_SECONDS", "3600"))


class TaskStatusEnum(str, Enum):
    UPLOADED = "UPLOADED"
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


class BBoxDTO(BaseModel):
    x1: int = Field(ge=0)
    y1: int = Field(ge=0)
    x2: int = Field(gt=0)
    y2: int = Field(gt=0)
    model_config = ConfigDict(strict=True)


class TaskRecordDTO(BaseModel):
    task_id: str
    status: TaskStatusEnum
    created_at: str
    image_path: Optional[str] = None
    bbox: Optional[BBoxDTO] = None
    result: Optional[Dict[str, Any]] = None
    multi_results: Optional[List[Dict[str, Any]]] = None
    error_msg: Optional[str] = None


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


async def ensure_ai_detection_runtime() -> None:
    if EngineContainer.instance is not None and EngineContainer.ocr_reader is not None:
        return

    if EngineContainer._runtime_lock is None:
        EngineContainer._runtime_lock = asyncio.Lock()

    async with EngineContainer._runtime_lock:
        if EngineContainer.instance is not None and EngineContainer.ocr_reader is not None:
            return

        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info("Loading AI detection runtime on %s (first use; may download EasyOCR models)", device)
        try:
            import easyocr
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Missing dependency 'easyocr'. Run `uv sync` or `pip install easyocr`."
            ) from exc

        EngineContainer.ocr_reader = await run_in_threadpool(
            easyocr.Reader,
            ["ch_sim", "en"],
            gpu=(device == "cuda"),
        )
        EngineContainer.instance = await run_in_threadpool(InferenceEngineAPI, "config.yaml")
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
    ) -> Dict[str, Any]:
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
                tmp.write(await file.read())
                tmp_path = tmp.name

            async with semaphore:
                result_str = await run_in_threadpool(engine.predict, tmp_path, bbox_list)

            result_dict = json.loads(result_str)
            if result_dict.get("result") == "错误":
                raise ValueError(result_dict.get("reason", "Unknown engine internal error."))
            return result_dict
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)


class DetectionDomainServiceV3:
    def __init__(
        self,
        engine: InferenceEngineAPI,
        registry: AbstractTaskRegistry,
        ocr_reader: Any,
        semaphore: asyncio.Semaphore,
    ):
        self.engine = engine
        self.registry = registry
        self.ocr_reader = ocr_reader
        self.semaphore = semaphore

    def _easyocr_auto_detect(self, image_path: str) -> List[BBoxDTO]:
        img_cv2 = cv2.imdecode(np.fromfile(image_path, dtype=np.uint8), cv2.IMREAD_COLOR)
        if img_cv2 is None:
            return []

        gray = cv2.cvtColor(img_cv2, cv2.COLOR_BGR2GRAY)
        blurred = cv2.medianBlur(gray, 3)

        ocr_results = self.ocr_reader.readtext(
            blurred,
            adjust_contrast=0.5,
            mag_ratio=2.0,
            text_threshold=0.25,
        )

        bboxes: List[BBoxDTO] = []
        for bbox, text, _ in ocr_results:
            text_clean = text.replace(" ", "")
            total_len = len(text_clean)
            if total_len == 0:
                continue

            digits_count = len(re.findall(r"\d", text_clean))
            digit_ratio = digits_count / total_len

            if digits_count < 3 or (total_len > 18 and digit_ratio < 0.5):
                continue

            xs = [p[0] for p in bbox]
            ys = [p[1] for p in bbox]
            bboxes.append(BBoxDTO(x1=int(min(xs)), y1=int(min(ys)), x2=int(max(xs)), y2=int(max(ys))))

        return bboxes

    async def execute_async(self, task_id: str, image_path: str, bbox: Optional[BBoxDTO] = None) -> None:
        task = await self.registry.get_task(task_id)
        if not task or task.status == TaskStatusEnum.CANCELED:
            return

        await self.registry.update_task(task_id, status=TaskStatusEnum.PROCESSING)

        try:
            if bbox:
                bbox_list = [bbox.x1, bbox.y1, bbox.x2, bbox.y2]
                async with self.semaphore:
                    res_str = await run_in_threadpool(self.engine.predict, image_path, bbox_list)

                res_dict = json.loads(res_str)
                if res_dict.get("result") == "错误":
                    raise ValueError(res_dict.get("reason"))

                res_dict["original_bbox"] = bbox_list
                await self.registry.update_task(task_id, status=TaskStatusEnum.COMPLETED, result=res_dict)
                return

            async with self.semaphore:
                bboxes = await run_in_threadpool(self._easyocr_auto_detect, image_path)

            if not bboxes:
                empty_res = {"result": "正常", "confidence": 0.0, "reason": "未发现关键数值或单号区域"}
                await self.registry.update_task(task_id, status=TaskStatusEnum.COMPLETED, result=empty_res)
                return

            all_results = []
            for b in bboxes:
                try:
                    b_list = [b.x1, b.y1, b.x2, b.y2]
                    async with self.semaphore:
                        res_str = await run_in_threadpool(self.engine.predict, image_path, b_list)

                    res_dict = json.loads(res_str)
                    if res_dict.get("result") != "错误":
                        res_dict["original_bbox"] = b_list
                        all_results.append(res_dict)
                except Exception as exc:
                    logger.warning("Task %s single bbox failed: %s", task_id, exc)

            await self.registry.update_task(task_id, status=TaskStatusEnum.COMPLETED, multi_results=all_results)

        except Exception as exc:
            logger.exception("Task %s failed", task_id)
            await self.registry.update_task(task_id, status=TaskStatusEnum.FAILED, error_msg=str(exc))

    async def generate_visualization(self, task_id: str) -> str:
        task = await self.registry.get_task(task_id)
        if not task or task.status != TaskStatusEnum.COMPLETED:
            raise ValueError("Task not completed.")

        vis_path = STORAGE_DIR / f"vis_{task_id}.jpg"
        if vis_path.exists():
            return str(vis_path)

        def draw_bboxes() -> None:
            img_cv2 = cv2.imdecode(np.fromfile(task.image_path, dtype=np.uint8), cv2.IMREAD_COLOR)
            if img_cv2 is None:
                raise ValueError("无法读取任务原图")

            img_pil = Image.fromarray(cv2.cvtColor(img_cv2, cv2.COLOR_BGR2RGB))
            draw = ImageDraw.Draw(img_pil)
            try:
                font = ImageFont.truetype("simhei.ttf", 22)
            except IOError:
                font = ImageFont.load_default()

            results_to_draw = task.multi_results if task.multi_results else []
            if task.result and not task.multi_results:
                results_to_draw.append(task.result)

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


router = APIRouter(prefix="/ai-detection", tags=["AI鉴伪模块"])


@router.post("/api/v1/image-detection/detect")
async def detect_tampering_endpoint(
    file: UploadFile = File(...),
    bbox: str = Form(...),
    engine: InferenceEngineAPI = Depends(get_engine),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    try:
        clean_bbox = bbox.strip().strip("'").strip('"').strip()
        bbox_parsed = json.loads(clean_bbox) if clean_bbox.startswith("[") else [int(x.strip()) for x in clean_bbox.split(",")]
        if len(bbox_parsed) != 4:
            raise ValueError
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid bbox format.")

    try:
        res = await DetectionService.process_detection(file, [int(x) for x in bbox_parsed], engine, semaphore)
        return {"status": "success", "data": res}
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"status": "error", "message": str(exc)})


@router.post("/api/v3/detect", summary="提交检测任务")
async def submit_detection(
    background_tasks: BackgroundTasks,
    task_id: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    bbox: Optional[str] = Form(None),
    engine: InferenceEngineAPI = Depends(get_engine),
    registry: AbstractTaskRegistry = Depends(get_registry),
    ocr_reader: Any = Depends(get_ocr_reader),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    if file:
        task_id = str(uuid.uuid4())
        file_path = STORAGE_DIR / f"{task_id}.jpg"
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        await registry.create_task(task_id, str(file_path))
    elif not task_id:
        raise HTTPException(status_code=400, detail="Must provide task_id or file.")

    task = await registry.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found.")

    bbox_dto = None
    if bbox:
        try:
            arr = json.loads(bbox) if bbox.startswith("[") else [int(x.strip()) for x in bbox.split(",")]
            if len(arr) != 4:
                raise ValueError
            bbox_dto = BBoxDTO(x1=arr[0], y1=arr[1], x2=arr[2], y2=arr[3])
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid bbox format.")

    await registry.update_task(task_id, status=TaskStatusEnum.PENDING)
    service = DetectionDomainServiceV3(engine, registry, ocr_reader, semaphore)
    background_tasks.add_task(service.execute_async, task_id, task.image_path, bbox_dto)
    return {"status": "pending", "task_id": task_id}


@router.get("/api/v3/result/{task_id}", response_model=TaskRecordDTO)
async def get_result(task_id: str, registry: AbstractTaskRegistry = Depends(get_registry)):
    task = await registry.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.get("/api/v3/result/{task_id}/visualization")
async def get_visualization(
    task_id: str,
    engine: InferenceEngineAPI = Depends(get_engine),
    registry: AbstractTaskRegistry = Depends(get_registry),
    ocr_reader: Any = Depends(get_ocr_reader),
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    service = DetectionDomainServiceV3(engine, registry, ocr_reader, semaphore)
    try:
        vis_path = await service.generate_visualization(task_id)
        return FileResponse(vis_path, media_type="image/jpeg")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/api/v3/task/{task_id}")
async def cancel_task(task_id: str, registry: AbstractTaskRegistry = Depends(get_registry)):
    task = await registry.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.status in [TaskStatusEnum.PENDING, TaskStatusEnum.UPLOADED]:
        await registry.update_task(task_id, status=TaskStatusEnum.CANCELED)
    else:
        await registry.delete_task(task_id)

    return {"status": "success"}
