#!/usr/bin/env python3
"""Mark legacy feedback as pending second review and recover upload metadata."""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from PIL import Image

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ai_detection.services.feedback_manager import FeedbackManager
from app.ai_detection.services.history_db import get_async_v3_history_by_task_id
from app.ai_detection.services.reviewed_dataset import normalize_display_filename
from app.config import UPLOAD_DIR


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sidecar(storage_dir: Path, task_id: str) -> Dict[str, Any]:
    path = storage_dir / f"{task_id}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _media_type(path: Path) -> Optional[str]:
    try:
        with Image.open(path) as image:
            image_format = str(image.format or "").upper()
    except Exception:
        return mimetypes.guess_type(path.name)[0]
    return {
        "JPEG": "image/jpeg",
        "PNG": "image/png",
        "WEBP": "image/webp",
    }.get(image_format)


def migrate_feedback_records(
    manager: FeedbackManager,
    *,
    storage_dir: Path,
    apply: bool,
    history_lookup: Callable[[str], Optional[Dict[str, Any]]] = get_async_v3_history_by_task_id,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "mode": "apply" if apply else "dry-run",
        "total": 0,
        "changed": 0,
        "already_reviewed": 0,
        "fallback_names": 0,
        "errors": [],
    }
    for root in (manager.correct_dir, manager.wrong_dir, manager.suspicious_dir):
        for folder in sorted(root.iterdir()) if root.exists() else []:
            metadata_path = folder / "metadata.json"
            metadata = manager._read_metadata_file(metadata_path)
            if metadata is None:
                continue
            summary["total"] += 1
            before = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
            task_id = str(metadata.get("task_id") or "").strip()
            image_path = manager.get_entry_file(folder.name, "image")
            sidecar = _sidecar(storage_dir, task_id) if task_id else {}
            try:
                history = history_lookup(task_id) if task_id else None
            except Exception as exc:
                history = None
                summary["errors"].append({"folder": folder.name, "error": str(exc)})
            history = history or {}

            if metadata.get("reviewed_sample_id"):
                metadata["review_status"] = "reviewed"
                summary["already_reviewed"] += 1
            else:
                metadata["review_status"] = "pending"
                for key in ("true_label", "true_label_name"):
                    metadata.pop(key, None)

            recovered_name = (
                metadata.get("original_filename")
                or sidecar.get("original_filename")
                or history.get("original_filename")
            )
            if not recovered_name:
                extension = image_path.suffix.lower() if image_path else ".jpg"
                recovered_name = f"task-{(task_id or folder.name)[:8]}{extension}"
                summary["fallback_names"] += 1
            metadata["original_filename"] = normalize_display_filename(
                recovered_name,
                fallback=f"task-{(task_id or folder.name)[:8]}.jpg",
            )

            if image_path and image_path.is_file():
                metadata["content_sha256"] = (
                    metadata.get("content_sha256")
                    or sidecar.get("content_sha256")
                    or history.get("content_sha256")
                    or _sha256(image_path)
                )
                metadata["size_bytes"] = int(
                    metadata.get("size_bytes")
                    or sidecar.get("size_bytes")
                    or history.get("size_bytes")
                    or image_path.stat().st_size
                )
                metadata["media_type"] = (
                    metadata.get("media_type")
                    or sidecar.get("media_type")
                    or history.get("media_type")
                    or _media_type(image_path)
                )

            after = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
            if after != before:
                summary["changed"] += 1
                if apply:
                    manager._write_metadata(metadata_path, metadata)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="实际写入；默认仅 dry-run")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    manager = FeedbackManager(args.config)
    result = migrate_feedback_records(
        manager,
        storage_dir=Path(UPLOAD_DIR) / "ai_detection_storage",
        apply=args.apply,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not result["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
