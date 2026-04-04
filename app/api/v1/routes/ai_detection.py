from __future__ import annotations

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
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import cv2
import numpy as np
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from PIL import Image, ImageDraw, ImageFont

from app.config import UPLOAD_DIR

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

        import torch

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
        from app.ai_detection.inference_api import InferenceEngineAPI

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


router = APIRouter(
    prefix="/ai-detection",
    tags=["AI鉴伪模块"],
)


_DETECT_RESULT_SCHEMA = (
    "引擎返回的 `data` / `result` 中单条结构示例：\n"
    "```json\n"
    "{\n"
    '  "result": "正常",\n'
    '  "confidence": 0.32,\n'
    '  "bbox": [120, 80, 280, 60],\n'
    '  "reason": "未检出明显篡改痕迹"\n'
    "}\n"
    "```\n"
    "- **result**：`正常` | `可疑` | `篡改` | `错误`\n"
    "- **confidence**：综合风险 0~1，越高越可疑\n"
    "- **bbox**：引擎实际使用的 ROI（x, y, 宽, 高）\n"
    "- **reason**：中文简要说明；异步任务成功时可能另含 **original_bbox**（用户传入的四点框）\n"
)


@router.post(
    "/api/v1/image-detection/detect",
    summary="单图单框鉴伪（同步）",
    description=(
        "上传一张图片并指定一个矩形检测区域，**同步**返回鉴伪结果。适合低延迟、单区域场景。\n\n"
        "**请求方式**：`multipart/form-data`\n\n"
        "**输入参数**\n"
        "- **file**：图片文件（如 JPG/PNG）\n"
        "- **bbox**：字符串。支持 JSON 数组 `[x1,y1,x2,y2]` 或英文逗号分隔 `x1,y1,x2,y2`（均为像素，"
        "左上角到右下角）\n\n"
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
    semaphore: asyncio.Semaphore = Depends(get_ai_semaphore),
):
    try:
        clean_bbox = bbox.strip().strip("'").strip('"').strip()
        bbox_parsed = json.loads(clean_bbox) if clean_bbox.startswith("[") else [int(x.strip()) for x in clean_bbox.split(",")]
        if len(bbox_parsed) != 4:
            raise ValueError
    except Exception:
        raise HTTPException(status_code=400, detail="bbox 格式无效，请使用 [x1,y1,x2,y2] 或 x1,y1,x2,y2")

    try:
        res = await DetectionService.process_detection(file, [int(x) for x in bbox_parsed], engine, semaphore)
        return {"status": "success", "data": res}
    except ValueError as exc:
        return JSONResponse(status_code=422, content={"status": "error", "message": str(exc)})


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
        "对每个框分别推理，结果在 `multi_results` 中。\n\n"
        "**输出示例（受理成功）**\n"
        "```json\n"
        "{\n"
        '  "status": "pending",\n'
        '  "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"\n'
        "}\n"
        "```\n\n"
        "**说明**：首次调用会加载 OCR 与模型，可能较慢。\n"
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
    service = DetectionDomainServiceV3(engine, registry, ocr_reader, semaphore)
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

    if task.status in [TaskStatusEnum.PENDING, TaskStatusEnum.UPLOADED]:
        await registry.update_task(task_id, status=TaskStatusEnum.CANCELED)
    else:
        await registry.delete_task(task_id)

    return {"status": "success"}
