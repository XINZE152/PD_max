# -*- coding: utf-8 -*-
"""图片鉴伪检测历史：入库、按天清理、分页列表。"""
from __future__ import annotations

import json
import logging
import os
import shutil
import threading
from decimal import Decimal
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.config import UPLOAD_DIR
from app.database import get_conn
from app.ai_detection.rule_check_display import build_rule_check_public_summary, derive_rule_check_status

logger = logging.getLogger(__name__)

_column_cache: Dict[str, Dict[str, bool]] = {}


def _table_has_column(table: str, column: str) -> bool:
    """检查表是否存在某列（结果缓存，避免重复查询 INFORMATION_SCHEMA）。"""
    if table not in _column_cache:
        cols: Dict[str, bool] = {}
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s",
                        (table,),
                    )
                    for row in cur.fetchall():
                        cols[str(row[0]).lower()] = True
        except Exception:
            logger.exception("查询表列信息失败 table=%s", table)
        _column_cache[table] = cols
    return _column_cache[table].get(column.lower(), False)

HISTORY_RETENTION_DAYS = int(os.getenv("AI_DETECTION_HISTORY_DAYS", "7"))
HISTORY_IMAGES_DIR = Path(UPLOAD_DIR) / "ai_detection_history_images"
HISTORY_STORAGE_DIR = Path(UPLOAD_DIR) / "ai_detection_storage"
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


def get_ai_detection_history_outcome(record_id: int) -> Optional[Dict[str, Any]]:
    """按 ID 查询单条历史记录，返回 {image_path, outcome, mode} 或 None。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, mode, task_id, outcome_json, stored_image FROM ai_detection_history WHERE id=%s",
                (record_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    rid, mode, task_id, outcome_json, stored_image = row
    if isinstance(outcome_json, str):
        try:
            outcome = json.loads(outcome_json)
        except json.JSONDecodeError:
            outcome = {}
    else:
        outcome = outcome_json or {}
    image_path = None
    if stored_image:
        name = str(stored_image)
        if not ("/" in name or "\\" in name or name.startswith(".")):
            p = HISTORY_IMAGES_DIR / name
            if p.is_file():
                image_path = p
    if image_path is None and task_id:
        p = HISTORY_STORAGE_DIR / f"{task_id}.jpg"
        if p.is_file():
            image_path = p
    return {"id": rid, "mode": mode, "outcome": outcome, "image_path": image_path}


def delete_ai_detection_history(record_id: int) -> bool:
    """删除单条鉴伪历史记录，并清理对应归档图。"""
    rid = int(record_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stored_image FROM ai_detection_history WHERE id=%s",
                (rid,),
            )
            row = cur.fetchone()
            if not row:
                return False
            stored_name = row[0]
            if stored_name:
                name = str(stored_name)
                if "/" not in name and "\\" not in name and not name.startswith("."):
                    fp = HISTORY_IMAGES_DIR / name
                    try:
                        if fp.is_file():
                            fp.unlink()
                    except OSError as exc:
                        logger.warning("删除历史归档图失败 %s: %s", fp, exc)
            cur.execute("DELETE FROM ai_detection_history WHERE id=%s", (rid,))
            return bool(cur.rowcount)


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


_batch_lock = threading.Lock()


def _generate_batch(cur) -> Optional[str]:
    """生成当天批次号，格式 YYYYMMDD + 自增序号（如 202606261）；batch 列不存在时返回 None。"""
    if not _table_has_column("ai_detection_history", "batch"):
        return None
    tz = timezone(timedelta(hours=8))
    today_str = datetime.now(tz).strftime("%Y%m%d")
    with _batch_lock:
        cur.execute(
            "SELECT MAX(batch) FROM ai_detection_history WHERE batch LIKE %s",
            (today_str + "%",),
        )
        row = cur.fetchone()
        max_batch = row[0] if row and row[0] else None
        if max_batch:
            num = int(max_batch[len(today_str):]) + 1
        else:
            num = 1
        return f"{today_str}{num}"


def insert_ai_detection_history(
    *,
    mode: str,
    task_id: Optional[str],
    original_filename: Optional[str],
    bbox: Any,
    status: str,
    outcome: Dict[str, Any],
    source_image_path: Optional[str] = None,
    image_created_at: Optional[str] = None,
    batch: Optional[str] = None,
) -> None:
    """写入一条检测历史（失败时仅打日志，不抛出给上层）。可选复制源图到归档目录。

    若提供 batch 则直接使用（同一批上传的多张图共享批次号），否则自动生成。
    """
    try:
        _ensure_history_images_dir()
        bbox_sql = json.dumps(bbox, ensure_ascii=False) if bbox is not None else None
        out_sql = json.dumps(outcome, ensure_ascii=False)
        with get_conn() as conn:
            with conn.cursor() as cur:
                if batch is None:
                    batch = _generate_batch(cur)
                has_img_created = _table_has_column("ai_detection_history", "image_created_at")
                has_batch = _table_has_column("ai_detection_history", "batch")
                columns = ["mode", "task_id", "original_filename", "bbox", "status", "outcome_json"]
                values: List[Any] = [mode, task_id, original_filename, bbox_sql, status, out_sql]
                if has_img_created:
                    columns.append("image_created_at")
                    values.append(image_created_at)
                if has_batch:
                    columns.append("batch")
                    values.append(batch)
                placeholders = ", ".join(["%s"] * len(values))
                cur.execute(
                    f"INSERT INTO ai_detection_history ({', '.join(columns)}) VALUES ({placeholders})",
                    tuple(values),
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


def get_feedback_by_task_ids(task_ids: Sequence[str]) -> Dict[str, Optional[str]]:
    """批量查询各 task_id 在 async_v3 上的最新人工标注（供 rule 行继承）。"""
    ids = [str(t).strip() for t in task_ids if str(t or "").strip()]
    if not ids:
        return {}
    out: Dict[str, Optional[str]] = {i: None for i in ids}
    placeholders = ", ".join(["%s"] * len(ids))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT task_id, feedback_status
                FROM ai_detection_history
                WHERE mode='async_v3' AND task_id IN ({placeholders})
                  AND feedback_status IS NOT NULL
                ORDER BY id DESC
                """,
                tuple(ids),
            )
            for tid, fb in cur.fetchall():
                key = str(tid)
                if key in out and out[key] is None and fb:
                    out[key] = str(fb)
    return out


def get_feedback_status(task_id: str) -> Optional[str]:
    """查询指定 task_id 的当前标注状态，返回 'correct' / 'wrong' / 'suspicious' 或 None（未标注）。"""
    tid = str(task_id or "").strip()
    if not tid:
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT feedback_status FROM ai_detection_history "
                "WHERE task_id=%s AND mode='async_v3' AND feedback_status IS NOT NULL "
                "ORDER BY id DESC LIMIT 1",
                (tid,),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def mark_feedback_status(task_id: str, judgment: str) -> None:
    """将指定 task_id 的标注状态写入数据库，同时记录标注时间（供提交标注时调用）。"""
    tid = str(task_id or "").strip()
    if not tid:
        return
    judgment = str(judgment or "").strip().lower()
    if judgment not in ("correct", "wrong", "suspicious"):
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ai_detection_history SET feedback_status=%s, feedback_marked_at=NOW() WHERE task_id=%s AND mode='async_v3'",
                (judgment, tid),
            )


def clear_feedback_status(task_id: str) -> bool:
    """清除指定 task_id 的标注状态及标注时间（恢复为未标注），供删除标注时调用。"""
    tid = str(task_id or "").strip()
    if not tid:
        return False
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ai_detection_history SET feedback_status=NULL, feedback_marked_at=NULL WHERE task_id=%s AND mode='async_v3'",
                (tid,),
            )
            return bool(cur.rowcount)


def _normalize_detection_result(mode: str, outcome: Optional[Dict[str, Any]]) -> Optional[str]:
    """从各模式异构的 outcome 中提取统一的检测结果：正常 / 可疑 / 篡改。"""
    if not isinstance(outcome, dict):
        return None
    # sync_v1 / async_v3：结果在 outcome.result.result 两层嵌套
    if mode in ("sync_v1", "async_v3"):
        inner = outcome.get("result")
        if isinstance(inner, dict):
            v = inner.get("result")
            if isinstance(v, str) and v:
                return v
        if isinstance(inner, str) and inner in ("正常", "可疑", "篡改"):
            return inner
        return None
    # rule_checks：outcome 本身包含 hard_tamper_flags / pixel_overlap / timestamp / reason
    if mode == "rule_checks":
        return derive_rule_check_status(outcome)
    # rule_pixel_overlap：outcome.result 是原始 pixel_overlap 数据
    if mode == "rule_pixel_overlap":
        pixel_data = outcome.get("result")
        return derive_rule_check_status({"pixel_overlap": pixel_data} if isinstance(pixel_data, dict) else {})
    # rule_timestamp：outcome.result 是原始 timestamp 数据
    if mode == "rule_timestamp":
        ts_data = outcome.get("result")
        return derive_rule_check_status({"timestamp": ts_data} if isinstance(ts_data, dict) else {})
    return None


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

            has_img_created = _table_has_column("ai_detection_history", "image_created_at")
            has_batch = _table_has_column("ai_detection_history", "batch")
            select_fields = [
                "id", "created_at",
                "image_created_at" if has_img_created else "NULL AS image_created_at",
                "batch" if has_batch else "NULL AS batch",
                "mode", "task_id", "original_filename", "bbox", "status",
                "outcome_json", "stored_image", "feedback_status", "feedback_marked_at",
            ]
            cur.execute(
                f"""
                SELECT {', '.join(select_fields)}
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
                img_created = item.get("image_created_at")
                if img_created is not None and hasattr(img_created, "isoformat"):
                    item["image_created_at"] = img_created.isoformat(sep=" ", timespec="seconds")
                marked_at = item.get("feedback_marked_at")
                if marked_at is not None and hasattr(marked_at, "isoformat"):
                    item["feedback_marked_at"] = marked_at.isoformat(sep=" ", timespec="seconds")
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
                item["detection_result"] = _normalize_detection_result(
                    item.get("mode", ""), item.get("outcome")
                )
                rid = item.get("id")
                stored = item.pop("stored_image", None)
                if stored and rid is not None:
                    item["image_url"] = f"/ai-detection/api/v1/history/{int(rid)}/image"
                else:
                    item["image_url"] = None
                rows_out.append(item)

    return total, rows_out


def query_ai_detection_history_for_export(
    *,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    retention_days: Optional[int] = None,
    modes: Optional[Sequence[str]] = None,
    status: Optional[str] = None,
    feedback_status: Optional[Sequence[str]] = None,
    batch: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    导出查询（不触发保留期 purge）。
    - 传 retention_days：与 GET /history 相同，created_at >= UTC 往前 N 天（推荐与列表对齐）。
    - 否则用 start_time～end_time（含边界）。
    modes 为空时不按 mode 过滤；status 为空不过滤状态。
    feedback_status：correct / wrong / suspicious / unmarked；空表示不过滤。
    batch：按批次号筛选（可选），如 20260626-001。
    """
    mode_list = [str(m).strip() for m in (modes or []) if str(m).strip()]
    status_val = str(status or "").strip()

    if retention_days is not None:
        d = max(1, int(retention_days))
        clauses = ["created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)"]
        params: List[Any] = [d]
    else:
        if start_time is None or end_time is None:
            raise ValueError("须同时提供 start_time 与 end_time，或提供 retention_days")
        clauses = ["created_at >= %s", "created_at <= %s"]
        params = [start_time, end_time]

    if status_val:
        clauses.append("status = %s")
        params.append(status_val)

    if mode_list:
        placeholders = ", ".join(["%s"] * len(mode_list))
        clauses.append(f"mode IN ({placeholders})")
        params.extend(mode_list)

    if batch is not None and _table_has_column("ai_detection_history", "batch"):
        clauses.append("batch = %s")
        params.append(batch)

    has_img_created = _table_has_column("ai_detection_history", "image_created_at")
    has_batch = _table_has_column("ai_detection_history", "batch")
    select_fields = [
        "id", "created_at",
        "image_created_at" if has_img_created else "NULL AS image_created_at",
        "batch" if has_batch else "NULL AS batch",
        "mode", "task_id", "original_filename", "bbox", "status",
        "outcome_json", "stored_image", "feedback_status",
    ]

    sql = f"""
        SELECT {', '.join(select_fields)}
        FROM ai_detection_history
        WHERE {" AND ".join(clauses)}
        ORDER BY id DESC
    """

    rows_out: List[Dict[str, Any]] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            cols = [c[0] for c in cur.description]
            for r in cur.fetchall():
                item = dict(zip(cols, r))
                created = item.get("created_at")
                if created is not None and hasattr(created, "isoformat"):
                    item["created_at"] = created.isoformat(sep=" ", timespec="seconds")
                img_created = item.get("image_created_at")
                if img_created is not None and hasattr(img_created, "isoformat"):
                    item["image_created_at"] = img_created.isoformat(sep=" ", timespec="seconds")
                rows_out.append(item)
    return rows_out
