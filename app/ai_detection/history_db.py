# -*- coding: utf-8 -*-
"""图片鉴伪检测历史：入库、按天清理、分页列表。"""
from __future__ import annotations

import json
import logging
import os
import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.config import UPLOAD_DIR
from app.database import get_conn

logger = logging.getLogger(__name__)

HISTORY_RETENTION_DAYS = int(os.getenv("AI_DETECTION_HISTORY_DAYS", "7"))
HISTORY_IMAGES_DIR = Path(UPLOAD_DIR) / "ai_detection_history_images"


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
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    列出最近 retention_days 天内的记录（先执行清理，再查询）。
    返回 (total, rows)。每条含 image_url（可 GET 取图），无图则为 None。
    """
    d = HISTORY_RETENTION_DAYS if retention_days is None else max(1, int(retention_days))
    purge_ai_detection_history_older_than(d)

    page = max(1, page)
    page_size = min(max(1, page_size), 200)
    offset = (page - 1) * page_size

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM ai_detection_history
                WHERE created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                """,
                (d,),
            )
            total = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT id, created_at, mode, task_id, original_filename, bbox, status, outcome_json, stored_image
                FROM ai_detection_history
                WHERE created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                ORDER BY id DESC
                LIMIT %s OFFSET %s
                """,
                (d, page_size, offset),
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
                rid = item.get("id")
                stored = item.pop("stored_image", None)
                if stored and rid is not None:
                    item["image_url"] = f"/ai-detection/api/v1/history/{int(rid)}/image"
                else:
                    item["image_url"] = None
                rows_out.append(item)

    return total, rows_out
