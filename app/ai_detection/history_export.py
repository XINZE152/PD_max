# -*- coding: utf-8 -*-
"""鉴伪历史：筛选查询与 ZIP 导出。"""
from __future__ import annotations

import io
import json
import logging
import os
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

import cv2
import numpy as np
from PIL import Image, ImageDraw

from app.ai_detection.core.utils import load_chinese_font
from app.ai_detection.history_db import (
    HISTORY_IMAGES_DIR,
    HISTORY_RETENTION_DAYS,
    get_ai_detection_history_image_path,
    get_feedback_by_task_ids,
    query_ai_detection_history_for_export,
)
from app.config import UPLOAD_DIR

from app.ai_detection.rule_check_display import derive_rule_check_status

logger = logging.getLogger(__name__)

STORAGE_DIR = Path(UPLOAD_DIR) / "ai_detection_storage"
DetectionResultFilter = Literal["正常", "可疑", "篡改"]
BboxModeFilter = Literal["all", "manual", "auto"]
ImageVariant = Literal["original", "annotated"]
MatchMode = Literal["primary", "any"]

EXPORT_MAX_RECORDS = int(os.getenv("AI_DETECTION_EXPORT_MAX_RECORDS", "500"))
PREVIEW_MAX_LIST = int(os.getenv("AI_DETECTION_EXPORT_PREVIEW_MAX", "200"))


def _jsonish_outcome(outcome_json: Any) -> Dict[str, Any]:
    if isinstance(outcome_json, dict):
        return outcome_json
    if isinstance(outcome_json, str):
        try:
            parsed = json.loads(outcome_json)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def classify_bbox_mode(bbox: Any) -> str:
    """manual | auto | unknown"""
    if bbox is None:
        return "unknown"
    if isinstance(bbox, dict):
        if bbox.get("auto_ocr") is True:
            return "auto"
        if all(k in bbox for k in ("x1", "y1", "x2", "y2")):
            return "manual"
        return "unknown"
    if isinstance(bbox, list):
        if len(bbox) == 4 and not isinstance(bbox[0], (list, tuple, dict)):
            try:
                [int(x) for x in bbox]
                return "manual"
            except (TypeError, ValueError):
                return "unknown"
        if len(bbox) >= 1 and isinstance(bbox[0], (list, tuple)) and len(bbox[0]) == 4:
            return "manual"
    return "unknown"


def extract_primary_detection_result(outcome: Dict[str, Any]) -> Optional[str]:
    # sync_v1 / async_v3：结果在 outcome.result.result 两层嵌套
    inner = outcome.get("result")
    if isinstance(inner, dict) and inner.get("result"):
        return str(inner.get("result"))
    if isinstance(inner, str) and inner in ("正常", "可疑", "篡改", "错误"):
        return inner
    if isinstance(outcome.get("error_msg"), str) and outcome.get("error_msg"):
        return None
    # rule_* 模式：通过 check_type 识别结构
    check_type = outcome.get("check_type")
    if check_type == "rule_checks":
        return derive_rule_check_status(outcome)
    if check_type in ("rule_pixel_overlap", "rule_timestamp"):
        key = "pixel_overlap" if check_type == "rule_pixel_overlap" else "timestamp"
        inner_data = outcome.get("result")
        return derive_rule_check_status({key: inner_data} if isinstance(inner_data, dict) else {})
    return None


def extract_all_detection_results(outcome: Dict[str, Any]) -> List[str]:
    labels: List[str] = []
    primary = extract_primary_detection_result(outcome)
    if primary:
        labels.append(primary)
    multi = outcome.get("multi_results")
    if isinstance(multi, list):
        for item in multi:
            if isinstance(item, dict) and item.get("result"):
                labels.append(str(item.get("result")))
    return list(dict.fromkeys(labels))


def record_matches_detection_filter(
    outcome: Dict[str, Any],
    detection_results: Optional[Sequence[str]],
    match_mode: MatchMode,
) -> bool:
    if not detection_results:
        return True
    allowed = {str(x) for x in detection_results}
    if match_mode == "any":
        return bool(allowed.intersection(extract_all_detection_results(outcome)))
    primary = extract_primary_detection_result(outcome)
    return primary in allowed if primary else False


def record_matches_feedback_filter(
    row_feedback: Optional[str],
    feedback_status: Optional[Sequence[str]],
) -> bool:
    if not feedback_status:
        return True
    allowed = {str(x).strip().lower() for x in feedback_status if str(x).strip()}
    current = (str(row_feedback).strip().lower() if row_feedback else None)
    want_unmarked = bool(allowed.intersection({"unmarked", "none", "null", "未标注"}))
    if current in ("correct", "wrong", "suspicious"):
        return current in allowed
    return want_unmarked


def _parse_bbox_row(row: Dict[str, Any]) -> Any:
    bbox_raw = row.get("bbox")
    if isinstance(bbox_raw, str):
        try:
            return json.loads(bbox_raw)
        except json.JSONDecodeError:
            return bbox_raw
    return bbox_raw


def effective_feedback_status(
    row: Dict[str, Any],
    feedback_by_task: Optional[Dict[str, Optional[str]]] = None,
) -> Optional[str]:
    """本行 feedback；rule 等无标注时回退同 task_id 的 async_v3 标注。"""
    fb = row.get("feedback_status")
    if fb:
        return str(fb)
    tid = str(row.get("task_id") or "").strip()
    if tid and feedback_by_task:
        return feedback_by_task.get(tid)
    return None


def row_passes_export_filters(
    row: Dict[str, Any],
    *,
    detection_results: Optional[List[str]],
    bbox_mode: BboxModeFilter,
    match_mode: MatchMode,
    feedback_status: Optional[List[str]],
    feedback_by_task: Optional[Dict[str, Optional[str]]] = None,
) -> bool:
    outcome = _jsonish_outcome(row.get("outcome_json"))
    if not record_matches_detection_filter(outcome, detection_results, match_mode):
        return False
    if not record_matches_feedback_filter(
        effective_feedback_status(row, feedback_by_task),
        feedback_status,
    ):
        return False
    bbox_raw = _parse_bbox_row(row)
    if bbox_mode != "all" and classify_bbox_mode(bbox_raw) != bbox_mode:
        return False
    return True


def _safe_zip_name(original_filename: Optional[str], record_id: int, suffix: str = ".jpg") -> str:
    base = (original_filename or f"record_{record_id}").strip()
    base = os.path.basename(base.replace("\\", "/"))
    base = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", base)
    if not base or base in (".", ".."):
        base = f"record_{record_id}"
    stem, ext = os.path.splitext(base)
    if not ext:
        ext = suffix
    return f"{record_id}_{stem}{ext}"


def resolve_record_image_path(
    record_id: int,
    task_id: Optional[str],
    stored_image: Optional[str],
) -> Optional[Path]:
    path = get_ai_detection_history_image_path(record_id)
    if path and path.is_file():
        return path
    if stored_image:
        name = str(stored_image)
        if "/" not in name and "\\" not in name and not name.startswith("."):
            p = HISTORY_IMAGES_DIR / name
            if p.is_file():
                return p
    tid = str(task_id or "").strip()
    if tid:
        p = STORAGE_DIR / f"{tid}.jpg"
        if p.is_file():
            return p
    return None


def _xyxy_from_result(res: Dict[str, Any]) -> Optional[List[int]]:
    ob = res.get("original_bbox")
    if isinstance(ob, list) and len(ob) >= 4:
        return [int(ob[0]), int(ob[1]), int(ob[2]), int(ob[3])]
    bb = res.get("bbox")
    if isinstance(bb, list) and len(bb) >= 4:
        x, y, w, h = int(bb[0]), int(bb[1]), int(bb[2]), int(bb[3])
        return [x, y, x + w, y + h]
    return None


def render_annotated_jpeg(image_path: Path, outcome: Dict[str, Any]) -> bytes:
    img_cv2 = cv2.imdecode(np.fromfile(str(image_path), dtype=np.uint8), cv2.IMREAD_COLOR)
    if img_cv2 is None:
        raise ValueError("无法读取图片")

    img_pil = Image.fromarray(cv2.cvtColor(img_cv2, cv2.COLOR_BGR2RGB))
    draw = ImageDraw.Draw(img_pil)
    font = load_chinese_font(22)

    results_to_draw: List[Dict[str, Any]] = []
    multi = outcome.get("multi_results")
    if isinstance(multi, list) and multi:
        results_to_draw.extend([x for x in multi if isinstance(x, dict)])
    inner = outcome.get("result")
    if isinstance(inner, dict) and inner:
        if not results_to_draw:
            results_to_draw.append(inner)

    for res in results_to_draw:
        xyxy = _xyxy_from_result(res)
        if not xyxy:
            continue
        x1, y1, x2, y2 = xyxy
        status = str(res.get("result", "正常"))
        confidence = float(res.get("confidence", 0.0) or 0.0)

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

    out_bgr = cv2.cvtColor(np.array(img_pil), cv2.COLOR_RGB2BGR)
    ok, buf = cv2.imencode(".jpg", out_bgr)
    if not ok:
        raise ValueError("标注图编码失败")
    return buf.tobytes()


def build_export_preview_item(row: Dict[str, Any], *, image_variant: ImageVariant = "original") -> Dict[str, Any]:
    outcome = _jsonish_outcome(row.get("outcome_json"))
    bbox_raw = row.get("bbox")
    if isinstance(bbox_raw, str):
        try:
            bbox_raw = json.loads(bbox_raw)
        except json.JSONDecodeError:
            pass
    rid = int(row["id"])
    has_image = resolve_record_image_path(rid, row.get("task_id"), row.get("stored_image")) is not None
    if has_image:
        image_url = (
            f"/ai-detection/api/v1/history/{rid}/image/annotated"
            if image_variant == "annotated"
            else f"/ai-detection/api/v1/history/{rid}/image"
        )
    else:
        image_url = None
    return {
        "id": rid,
        "created_at": row.get("created_at"),
        "image_created_at": row.get("image_created_at"),
        "batch": row.get("batch"),
        "mode": row.get("mode"),
        "task_id": row.get("task_id"),
        "original_filename": row.get("original_filename"),
        "status": row.get("status"),
        "detection_result": extract_primary_detection_result(outcome),
        "detection_results_all": extract_all_detection_results(outcome),
        "bbox_mode": classify_bbox_mode(bbox_raw),
        "has_image": has_image,
        "image_url": image_url,
        "feedback_status": row.get("feedback_status"),
    }


def _fetch_export_rows(
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    retention_days: Optional[int],
    modes: Optional[List[str]],
    status: Optional[str],
) -> List[Dict[str, Any]]:
    if retention_days is not None:
        return query_ai_detection_history_for_export(
            retention_days=retention_days,
            modes=modes,
            status=status,
        )
    assert start_time is not None and end_time is not None
    return query_ai_detection_history_for_export(
        start_time=start_time,
        end_time=end_time,
        modes=modes,
        status=status,
    )


def preview_export(
    *,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    retention_days: Optional[int] = None,
    detection_results: Optional[List[str]] = None,
    bbox_mode: BboxModeFilter = "all",
    modes: Optional[List[str]] = None,
    status: Optional[str] = None,
    match_mode: MatchMode = "primary",
    image_variant: ImageVariant = "original",
    feedback_status: Optional[List[str]] = None,
) -> Dict[str, Any]:
    rows = _fetch_export_rows(
        start_time=start_time,
        end_time=end_time,
        retention_days=retention_days,
        modes=modes,
        status=status,
    )
    task_ids = [str(r.get("task_id") or "").strip() for r in rows if r.get("task_id")]
    feedback_by_task = get_feedback_by_task_ids(task_ids)

    matched: List[Dict[str, Any]] = []
    with_image = 0
    without_image = 0
    for row in rows:
        if not row_passes_export_filters(
            row,
            detection_results=detection_results,
            bbox_mode=bbox_mode,
            match_mode=match_mode,
            feedback_status=feedback_status,
            feedback_by_task=feedback_by_task,
        ):
            continue
        item = build_export_preview_item(row, image_variant=image_variant)
        item["feedback_status"] = effective_feedback_status(row, feedback_by_task)
        matched.append(item)
        if item["has_image"]:
            with_image += 1
        else:
            without_image += 1

    total = len(matched)
    exceeds_limit = total > EXPORT_MAX_RECORDS
    list_slice = matched[:PREVIEW_MAX_LIST]
    filters_applied: Dict[str, Any] = {
        "detection_results": detection_results or [],
        "bbox_mode": bbox_mode,
        "modes": modes if modes else "all",
        "status": status if status else "all",
        "feedback_status": feedback_status or [],
        "match_mode": match_mode,
        "image_variant": image_variant,
    }
    if retention_days is not None:
        filters_applied["retention_days"] = retention_days
    else:
        filters_applied["start_time"] = start_time.isoformat(sep=" ", timespec="seconds") if start_time else None
        filters_applied["end_time"] = end_time.isoformat(sep=" ", timespec="seconds") if end_time else None
    return {
        "total_matched": total,
        "with_image": with_image,
        "without_image": without_image,
        "export_max_records": EXPORT_MAX_RECORDS,
        "exceeds_limit": exceeds_limit,
        "preview_truncated": total > len(list_slice),
        "preview_list_size": len(list_slice),
        "filters_applied": filters_applied,
        "list": list_slice,
    }


def _manifest_filters(
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    retention_days: Optional[int],
    detection_results: Optional[List[str]],
    bbox_mode: BboxModeFilter,
    modes: Optional[List[str]],
    status: Optional[str],
    feedback_status: Optional[List[str]],
    match_mode: MatchMode,
    image_variant: ImageVariant,
) -> Dict[str, Any]:
    f: Dict[str, Any] = {
        "detection_results": detection_results or [],
        "bbox_mode": bbox_mode,
        "modes": modes if modes else "all",
        "status": status if status else "all",
        "feedback_status": feedback_status or [],
        "match_mode": match_mode,
        "image_variant": image_variant,
    }
    if retention_days is not None:
        f["retention_days"] = retention_days
    else:
        if start_time:
            f["start_time"] = start_time.isoformat(sep=" ", timespec="seconds")
        if end_time:
            f["end_time"] = end_time.isoformat(sep=" ", timespec="seconds")
    return f


def build_export_zip(
    *,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    retention_days: Optional[int] = None,
    detection_results: Optional[List[str]] = None,
    bbox_mode: BboxModeFilter = "all",
    modes: Optional[List[str]] = None,
    status: Optional[str] = None,
    match_mode: MatchMode = "primary",
    image_variant: ImageVariant = "original",
    feedback_status: Optional[List[str]] = None,
) -> Tuple[bytes, str, Dict[str, Any]]:
    rows = _fetch_export_rows(
        start_time=start_time,
        end_time=end_time,
        retention_days=retention_days,
        modes=modes,
        status=status,
    )
    task_ids = [str(r.get("task_id") or "").strip() for r in rows if r.get("task_id")]
    feedback_by_task = get_feedback_by_task_ids(task_ids)

    manifest_records: List[Dict[str, Any]] = []
    zip_buffer = io.BytesIO()
    images_added = 0
    skipped_no_image = 0

    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for row in rows:
            if not row_passes_export_filters(
                row,
                detection_results=detection_results,
                bbox_mode=bbox_mode,
                match_mode=match_mode,
                feedback_status=feedback_status,
                feedback_by_task=feedback_by_task,
            ):
                continue

            rid = int(row["id"])
            bbox_raw = _parse_bbox_row(row)
            if len(manifest_records) >= EXPORT_MAX_RECORDS:
                break

            outcome = _jsonish_outcome(row.get("outcome_json"))

            img_path = resolve_record_image_path(rid, row.get("task_id"), row.get("stored_image"))
            entry_name = _safe_zip_name(row.get("original_filename"), rid)
            if image_variant == "annotated":
                stem, _ = os.path.splitext(entry_name)
                entry_name = f"{stem}_annotated.jpg"

            file_bytes: Optional[bytes] = None
            if img_path:
                try:
                    if image_variant == "annotated":
                        file_bytes = render_annotated_jpeg(img_path, outcome)
                    else:
                        file_bytes = img_path.read_bytes()
                except Exception:
                    logger.exception("export image failed record_id=%s", rid)

            arcname = f"images/{entry_name}"
            if file_bytes:
                zf.writestr(arcname, file_bytes)
                images_added += 1
            else:
                skipped_no_image += 1
                arcname = None

            inner = outcome.get("result") if isinstance(outcome.get("result"), dict) else {}
            manifest_records.append(
                {
                    "id": rid,
                    "created_at": row.get("created_at"),
                    "image_created_at": row.get("image_created_at"),
                    "batch": row.get("batch"),
                    "mode": row.get("mode"),
                    "task_id": row.get("task_id"),
                    "original_filename": row.get("original_filename"),
                    "status": row.get("status"),
                    "detection_result": extract_primary_detection_result(outcome),
                    "confidence": inner.get("confidence") if isinstance(inner, dict) else None,
                    "bbox_mode": classify_bbox_mode(bbox_raw),
                    "feedback_status": effective_feedback_status(row, feedback_by_task),
                    "zip_path": arcname,
                    "has_image_in_zip": bool(file_bytes),
                }
            )

        manifest = {
            "exported_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
            "record_count": len(manifest_records),
            "images_added": images_added,
            "skipped_no_image": skipped_no_image,
            "filters": _manifest_filters(
                start_time=start_time,
                end_time=end_time,
                retention_days=retention_days,
                detection_results=detection_results,
                bbox_mode=bbox_mode,
                modes=modes,
                status=status,
                feedback_status=feedback_status,
                match_mode=match_mode,
                image_variant=image_variant,
            ),
            "records": manifest_records,
        }
        zf.writestr(
            "export_manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        )

    if not manifest_records:
        raise ValueError("没有符合筛选条件的记录")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"ai_detection_export_{stamp}.zip"
    stats = {
        "record_count": len(manifest_records),
        "images_added": images_added,
        "skipped_no_image": skipped_no_image,
        "filename": filename,
    }
    return zip_buffer.getvalue(), filename, stats