# -*- coding: utf-8 -*-
"""图片鉴伪检测历史：入库、按天清理、分页列表。"""
from __future__ import annotations

import json
import logging
import os
import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.config import UPLOAD_DIR
from app.database import get_conn
from app.ai_detection.rule_check_display import build_rule_check_public_summary

logger = logging.getLogger(__name__)

HISTORY_RETENTION_DAYS = int(os.getenv("AI_DETECTION_HISTORY_DAYS", "7"))
HISTORY_IMAGES_DIR = Path(UPLOAD_DIR) / "ai_detection_history_images"
HISTORY_ORIGINAL_FILENAME_MAX = 512


def normalize_history_original_filename(
    upload_name: Optional[str],
    *,
    fallback_path: str,
) -> str:
    """历史记录展示用文件名：优先用户上传名，否则用磁盘路径 basename。"""
    raw = str(upload_name or "").strip()
    if raw:
        name = os.path.basename(raw.replace("\\", "/"))
        if name and name not in (".", ".."):
            return name[:HISTORY_ORIGINAL_FILENAME_MAX]
    return os.path.basename(str(fallback_path))[:HISTORY_ORIGINAL_FILENAME_MAX]


def _ensure_history_images_dir() -> None:
    HISTORY_IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def get_ai_detection_history_image_path(record_id: int) -> Optional[Path]:
    """若该记录有归档图且文件存在，返回绝对路径；否则 None。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stored_image FROM ai_detection_history WHERE id=%s",
                (record_id,),
            )
            row = cur.fetchone()
    if not row or not row[0]:
        return None
    name = str(row[0])
    if "/" in name or "\\" in name or name.startswith("."):
        return None
    p = HISTORY_IMAGES_DIR / name
    return p if p.is_file() else None


def get_rule_checks_history_by_task_id(task_id: str) -> Optional[Dict[str, Any]]:
    """按 task_id 返回最近一条 rule_checks 历史（AI+规则 聚合用）。"""
    tid = str(task_id or "").strip()
    if not tid:
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, task_id, status, outcome_json, original_filename, created_at
                FROM ai_detection_history
                WHERE task_id=%s AND mode='rule_checks' AND status='COMPLETED'
                ORDER BY id DESC
                LIMIT 1
                """,
                (tid,),
            )
            row = cur.fetchone()
    if not row:
        return None

    rid, _task_id, status, outcome_json, original_filename, created_at = row
    try:
        outcome = json.loads(outcome_json) if isinstance(outcome_json, str) else _jsonish(outcome_json)
    except json.JSONDecodeError:
        outcome = {}

    created_text = created_at.isoformat(sep=" ", timespec="seconds") if hasattr(created_at, "isoformat") else created_at
    return {
        "id": int(rid),
        "task_id": _task_id,
        "status": status,
        "outcome": outcome or {},
        "original_filename": original_filename,
        "created_at": created_text,
    }


def get_async_v3_history_by_task_id(task_id: str) -> Optional[Dict[str, Any]]:
    """按 task_id 返回最近一条 async_v3 历史（任意 status），供重启后恢复任务结果。"""
    tid = str(task_id or "").strip()
    if not tid:
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, task_id, status, outcome_json, stored_image, original_filename,
                       created_at, bbox
                FROM ai_detection_history
                WHERE task_id=%s AND mode='async_v3'
                ORDER BY id DESC
                LIMIT 1
                """,
                (tid,),
            )
            row = cur.fetchone()
    if not row:
        return None

    (
        rid,
        _task_id,
        status,
        outcome_json,
        stored_image,
        original_filename,
        created_at,
        bbox_raw,
    ) = row
    try:
        outcome = json.loads(outcome_json) if isinstance(outcome_json, str) else _jsonish(outcome_json)
    except json.JSONDecodeError:
        outcome = {}
    bbox_val = bbox_raw
    if isinstance(bbox_val, str):
        try:
            bbox_val = json.loads(bbox_val)
        except json.JSONDecodeError:
            bbox_val = None

    created_text = (
        created_at.isoformat(sep=" ", timespec="seconds")
        if hasattr(created_at, "isoformat")
        else str(created_at or "")
    )
    return {
        "id": int(rid),
        "task_id": _task_id,
        "status": status,
        "outcome": outcome or {},
        "stored_image": stored_image,
        "original_filename": original_filename,
        "created_at": created_text,
        "bbox": bbox_val,
    }


def get_latest_ai_detection_history_by_task_id(task_id: str) -> Optional[Dict[str, Any]]:
    """按异步 task_id 返回最近一条成功历史及归档图路径，用于任务内存丢失后的兜底读取。"""
    tid = str(task_id or "").strip()
    if not tid:
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, task_id, status, outcome_json, stored_image
                FROM ai_detection_history
                WHERE task_id=%s AND mode='async_v3' AND status='COMPLETED'
                ORDER BY id DESC
                LIMIT 1
                """,
                (tid,),
            )
            row = cur.fetchone()
    if not row:
        return None

    rid, _task_id, status, outcome_json, stored_image = row
    if not stored_image:
        return None
    name = str(stored_image)
    if "/" in name or "\\" in name or name.startswith("."):
        return None
    image_path = HISTORY_IMAGES_DIR / name
    if not image_path.is_file():
        return None

    try:
        outcome = json.loads(outcome_json) if isinstance(outcome_json, str) else _jsonish(outcome_json)
    except json.JSONDecodeError:
        outcome = {}
    return {
        "id": int(rid),
        "task_id": _task_id,
        "status": status,
        "outcome": outcome or {},
        "image_path": image_path,
    }


def purge_ai_detection_history_older_than(days: Optional[int] = None) -> int:
    """删除早于「当前 UTC 往前 days 天」的记录，并删除对应归档图。返回删除行数。"""
    d = HISTORY_RETENTION_DAYS if days is None else max(1, int(days))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, stored_image FROM ai_detection_history
                WHERE created_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                """,
                (d,),
            )
            for _rid, stored_name in cur.fetchall():
                if not stored_name:
                    continue
                name = str(stored_name)
                if "/" in name or "\\" in name or name.startswith("."):
                    continue
                fp = HISTORY_IMAGES_DIR / name
                try:
                    if fp.is_file():
                        fp.unlink()
                except OSError as exc:
                    logger.warning("删除历史归档图失败 %s: %s", fp, exc)
            cur.execute(
                "DELETE FROM ai_detection_history WHERE created_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)",
                (d,),
            )
            return int(cur.rowcount or 0)


def insert_ai_detection_history(
    *,
    mode: str,
    task_id: Optional[str],
    original_filename: Optional[str],
    bbox: Any,
    status: str,
    outcome: Dict[str, Any],
    source_image_path: Optional[str] = None,
) -> None:
    """写入一条检测历史（失败时仅打日志，不抛出给上层）。可选复制源图到归档目录。"""
    try:
        _ensure_history_images_dir()
        bbox_sql = json.dumps(bbox, ensure_ascii=False) if bbox is not None else None
        out_sql = json.dumps(outcome, ensure_ascii=False)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ai_detection_history
                    (mode, task_id, original_filename, bbox, status, outcome_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (mode, task_id, original_filename, bbox_sql, status, out_sql),
                )
                rid = cur.lastrowid
                if (
                    rid
                    and source_image_path
                    and os.path.isfile(source_image_path)
                ):
                    stored_name = f"{int(rid)}.jpg"
                    dest = HISTORY_IMAGES_DIR / stored_name
                    try:
                        shutil.copy2(source_image_path, dest)
                        cur.execute(
                            "UPDATE ai_detection_history SET stored_image=%s WHERE id=%s",
                            (stored_name, rid),
                        )
                    except Exception:
                        logger.exception(
                            "归档鉴伪历史图片失败 id=%s src=%s", rid, source_image_path
                        )
    except Exception:
        logger.exception(
            "写入 ai_detection_history 失败 mode=%s task_id=%s", mode, task_id
        )


def _jsonish(val: Any) -> Any:
    if isinstance(val, dict):
        return {k: _jsonish(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_jsonish(x) for x in val]
    if isinstance(val, Decimal):
        return float(val)
    return val


def list_ai_detection_history(
    *,
    page: int = 1,
    page_size: int = 20,
    retention_days: Optional[int] = None,
    modes: Optional[Sequence[str]] = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    列出最近 retention_days 天内的记录（先执行清理，再查询）。
    返回 (total, rows)。每条含 image_url（可 GET 取图），无图则为 None。
    modes 非空时仅返回指定 mode（如 rule_checks,rule_pixel_overlap）。
    """
    d = HISTORY_RETENTION_DAYS if retention_days is None else max(1, int(retention_days))
    purge_ai_detection_history_older_than(d)

    page = max(1, page)
    page_size = min(max(1, page_size), 200)
    offset = (page - 1) * page_size

    mode_list = [str(m).strip() for m in (modes or []) if str(m).strip()]
    mode_clause = ""
    mode_params: List[Any] = [d]
    if mode_list:
        placeholders = ", ".join(["%s"] * len(mode_list))
        mode_clause = f" AND mode IN ({placeholders})"
        mode_params.extend(mode_list)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM ai_detection_history
                WHERE created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                {mode_clause}
                """,
                tuple(mode_params),
            )
            total = int(cur.fetchone()[0])

            cur.execute(
                f"""
                SELECT id, created_at, mode, task_id, original_filename, bbox, status, outcome_json, stored_image
                FROM ai_detection_history
                WHERE created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                {mode_clause}
                ORDER BY id DESC
                LIMIT %s OFFSET %s
                """,
                tuple(mode_params + [page_size, offset]),
            )
            cols = [c[0] for c in cur.description]
            rows_out: List[Dict[str, Any]] = []
            for r in cur.fetchall():
                item = dict(zip(cols, r))
                created = item.get("created_at")
                if created is not None and hasattr(created, "isoformat"):
                    item["created_at"] = created.isoformat(sep=" ", timespec="seconds")
                bbox_v = item.get("bbox")
                if isinstance(bbox_v, str):
                    try:
                        item["bbox"] = json.loads(bbox_v)
                    except json.JSONDecodeError:
                        pass
                elif bbox_v is not None:
                    item["bbox"] = _jsonish(bbox_v)
                out_v = item.get("outcome_json")
                if isinstance(out_v, str):
                    try:
                        item["outcome"] = json.loads(out_v)
                    except json.JSONDecodeError:
                        item["outcome"] = {"raw": out_v}
                else:
                    item["outcome"] = _jsonish(out_v) if out_v is not None else None
                del item["outcome_json"]
                outcome_obj = item.get("outcome")
                if isinstance(outcome_obj, dict) and isinstance(outcome_obj.get("summary"), dict):
                    item["summary"] = outcome_obj["summary"]
                linked_task_id = item.get("task_id")
                if item.get("mode") == "async_v3" and linked_task_id:
                    rule_row = get_rule_checks_history_by_task_id(str(linked_task_id))
                    if rule_row:
                        item["linked_rule_checks"] = build_rule_check_public_summary(rule_row.get("outcome") or {})
                    else:
                        item["linked_rule_checks"] = None
                rid = item.get("id")
                stored = item.pop("stored_image", None)
                if stored and rid is not None:
                    item["image_url"] = f"/ai-detection/api/v1/history/{int(rid)}/image"
                else:
                    item["image_url"] = None
                rows_out.append(item)

    return total, rows_out
