"""
人工标注反馈管理模块
- 支持 correct / wrong / suspicious 三级标注
- wrong: 保存原图 + 框选区域 + 元数据
- suspicious: 存入待确认目录，确认后移回 correct/wrong
"""

import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import cv2
import numpy as np
import yaml

from app.ai_detection.services.reviewed_dataset import (
    ReviewedDatasetManager,
    normalize_display_filename,
)


class FeedbackEntryReviewedError(RuntimeError):
    """Raised when a reviewed feedback source is deleted before review revocation."""


class FeedbackManager:
    """管理用户对检测结果的标注反馈。"""

    def __init__(self, config_path: str = "config.yaml"):
        from app.ai_detection.runtime.paths import resolve_config_path
        config_file = resolve_config_path(config_path)
        with open(config_file, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        fb_cfg = self.config.get("feedback", {})
        self.base_dir = Path(self._resolve_path(fb_cfg.get("storage_dir", "feedback")))
        self.correct_dir = self.base_dir / "correct"
        self.wrong_dir = self.base_dir / "wrong"
        self.suspicious_dir = self.base_dir / "suspicious"
        self.reviewed = ReviewedDatasetManager(self.base_dir)

        for d in [self.correct_dir, self.wrong_dir, self.suspicious_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def _resolve_path(self, path_str: str) -> str:
        p = Path(path_str)
        if p.is_absolute():
            return str(p)
        return str((Path(__file__).resolve().parent / p).resolve())

    def save_judgment(
        self,
        task_id: str,
        judgment: str,  # "correct" | "wrong" | "suspicious"
        image_path: str,
        bbox: Optional[List[int]] = None,
        result: Optional[Dict[str, Any]] = None,
        note: str = "",
        original_filename: Optional[str] = None,
        content_sha256: Optional[str] = None,
        size_bytes: Optional[int] = None,
        media_type: Optional[str] = None,
        initial_reviewer: Optional[str] = None,
    ) -> Dict[str, Any]:
        """保存用户判断。wrong 时额外保存裁剪区域图。"""
        entry_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder_name = f"{timestamp}_{task_id}_{entry_id}"

        if judgment == "wrong":
            target_dir = self.wrong_dir / folder_name
        elif judgment == "suspicious":
            target_dir = self.suspicious_dir / folder_name
        else:
            target_dir = self.correct_dir / folder_name

        target_dir.mkdir(parents=True, exist_ok=True)

        # 复制原图
        src_img = Path(image_path)
        dst_img = target_dir / f"original{src_img.suffix}"
        if src_img.exists():
            shutil.copy2(str(src_img), str(dst_img))

        # wrong: 额外保存裁剪区域图
        cropped_path = None
        if judgment in ("wrong", "suspicious") and bbox and src_img.exists():
            try:
                img = cv2.imdecode(np.fromfile(str(src_img), dtype=np.uint8), cv2.IMREAD_COLOR)
                if img is not None:
                    x1, y1, x2, y2 = bbox
                    x1, x2 = sorted([max(0, x1), min(img.shape[1], x2)])
                    y1, y2 = sorted([max(0, y1), min(img.shape[0], y2)])
                    roi = img[y1:y2, x1:x2]
                    cropped_path = str(target_dir / "roi.jpg")
                    cv2.imencode(".jpg", roi)[1].tofile(cropped_path)
            except Exception:
                pass

        # 保存元数据
        metadata = {
            "task_id": task_id,
            "judgment": judgment,
            "timestamp": timestamp,
            "bbox": bbox,
            "engine_result": result,
            "user_note": note,
            "entry_id": entry_id,
            "original_image": str(dst_img),
            "cropped_roi": cropped_path,
            "original_filename": normalize_display_filename(
                original_filename,
                fallback=src_img.name or f"task-{task_id}{src_img.suffix}",
            ),
            "content_sha256": content_sha256,
            "size_bytes": size_bytes,
            "media_type": media_type,
            "initial_reviewer": initial_reviewer,
            "review_status": "pending",
        }
        self._write_metadata(target_dir / "metadata.json", metadata)

        return metadata

    def _dir_for_judgment(self, judgment: str) -> Optional[Path]:
        mapping = {
            "correct": self.correct_dir,
            "wrong": self.wrong_dir,
            "suspicious": self.suspicious_dir,
        }
        return mapping.get(str(judgment or "").strip().lower())

    def _find_entry_folder(self, entry_folder: str) -> Optional[Path]:
        name = Path(str(entry_folder or "").strip()).name
        if not name or name in (".", ".."):
            return None
        for root in (self.correct_dir, self.wrong_dir, self.suspicious_dir):
            candidate = root / name
            if candidate.is_dir() and (candidate / "metadata.json").is_file():
                return candidate
        return None

    def _read_entry(self, folder: Path) -> Optional[Dict[str, Any]]:
        meta_file = folder / "metadata.json"
        if not meta_file.exists():
            return None
        try:
            with open(meta_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

        judgment = folder.parent.name
        metadata["judgment"] = metadata.get("judgment") or judgment
        metadata["folder_name"] = folder.name
        metadata["image_url"] = f"/ai-detection/api/v3/feedback/{folder.name}/image"
        roi = folder / "roi.jpg"
        metadata["roi_url"] = f"/ai-detection/api/v3/feedback/{folder.name}/roi" if roi.is_file() else None
        metadata["can_confirm"] = judgment == "suspicious"
        metadata["original_filename"] = normalize_display_filename(
            metadata.get("original_filename"),
            fallback=Path(str(metadata.get("original_image") or "image")).name,
        )
        metadata["review_status"] = (
            "reviewed" if metadata.get("reviewed_sample_id") else "pending"
        )
        metadata["can_review"] = True
        metadata["can_revoke_review"] = metadata["review_status"] == "reviewed"
        return metadata

    @staticmethod
    def _write_metadata(path: Path, metadata: Dict[str, Any]) -> None:
        temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temp_path.open("w", encoding="utf-8") as stream:
                json.dump(metadata, stream, ensure_ascii=False, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, path)
        finally:
            temp_path.unlink(missing_ok=True)

    def get_entry(self, entry_folder: str) -> Optional[Dict[str, Any]]:
        """按文件夹名返回单条反馈元数据。"""
        folder = self._find_entry_folder(entry_folder)
        if folder is None:
            return None
        return self._read_entry(folder)

    def get_entry_file(self, entry_folder: str, kind: str = "image") -> Optional[Path]:
        """返回反馈条目的原图或 ROI 图片路径。kind=image|roi。"""
        folder = self._find_entry_folder(entry_folder)
        if folder is None:
            return None
        if kind == "roi":
            roi = folder / "roi.jpg"
            return roi if roi.is_file() else None

        metadata = self._read_entry(folder) or {}
        raw = metadata.get("original_image")
        if isinstance(raw, str) and raw.strip():
            p = Path(raw)
            if p.is_file():
                return p
            fallback = folder / p.name
            if fallback.is_file():
                return fallback
        for candidate in folder.glob("original.*"):
            if candidate.is_file():
                return candidate
        return None

    def update_entry(
        self,
        entry_folder: str,
        judgment: str,
        note: Optional[str] = None,
        original_filename: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """修改反馈判断类型，可在 correct / wrong / suspicious 之间移动。"""
        dst_root = self._dir_for_judgment(judgment)
        src = self._find_entry_folder(entry_folder)
        if src is None or dst_root is None:
            return None

        dst = dst_root / src.name
        if dst != src:
            if dst.exists():
                shutil.rmtree(str(dst))
            shutil.move(str(src), str(dst))
        else:
            dst = src

        meta_file = dst / "metadata.json"
        with open(meta_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        metadata["judgment"] = judgment
        metadata["updated_at"] = datetime.now().strftime("%Y%m%d_%H%M%S")
        if note is not None:
            metadata["user_note"] = note
        if original_filename is not None:
            metadata["original_filename"] = normalize_display_filename(
                original_filename,
                fallback=Path(str(metadata.get("original_image") or "image")).name,
            )
        self._write_metadata(meta_file, metadata)
        return self._read_entry(dst)

    def delete_entry(self, entry_folder: str) -> bool:
        """删除一条反馈记录，相当于撤销该标注。"""
        folder = self._find_entry_folder(entry_folder)
        if folder is None:
            return False
        metadata = self._read_entry(folder) or {}
        if metadata.get("reviewed_sample_id"):
            raise FeedbackEntryReviewedError("该反馈已完成二审，请先撤销二审再删除")
        shutil.rmtree(str(folder))
        return True

    def _sync_review_links(self, record: Dict[str, Any]) -> None:
        """Keep all feedback sources linked to a deduplicated reviewed sample in sync."""
        for source in list(record.get("sources") or []):
            source_id = str(source.get("source_id") or "").strip()
            folder = self._find_entry_folder(source_id)
            if folder is None:
                continue
            metadata_path = folder / "metadata.json"
            metadata = self._read_metadata_file(metadata_path)
            if metadata is None:
                continue
            metadata.update(
                {
                    "review_status": "reviewed",
                    "reviewed_sample_id": record.get("sample_id"),
                    "true_label": record.get("label"),
                    "true_label_name": record.get("label_name"),
                    "second_reviewed_at": source.get("reviewed_at"),
                    "second_reviewer": source.get("reviewer"),
                    "second_review_note": source.get("note", ""),
                    "second_review_regions": record.get("regions") or [],
                }
            )
            self._write_metadata(metadata_path, metadata)

    @staticmethod
    def _read_metadata_file(path: Path) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    def review_entry(
        self,
        entry_folder: str,
        *,
        label: int,
        reviewer: str,
        note: str = "",
        regions: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Apply an explicit truth label and link the feedback to reviewed training data."""
        folder = self._find_entry_folder(entry_folder)
        if folder is None:
            return None
        metadata_path = folder / "metadata.json"
        metadata = self._read_metadata_file(metadata_path)
        image_path = self.get_entry_file(folder.name, "image")
        if metadata is None or image_path is None:
            return None

        existing_sample_id = str(metadata.get("reviewed_sample_id") or "").strip()
        if existing_sample_id:
            existing = self.reviewed.get_entry(existing_sample_id)
            if existing and int(existing.get("label", -1)) != int(label):
                self.reviewed.reclassify(
                    existing_sample_id,
                    int(label),
                    reviewer=reviewer,
                    note=note,
                    regions=regions,
                )

        record = self.reviewed.add_review(
            image_path=image_path,
            label=int(label),
            original_filename=metadata.get("original_filename"),
            source={
                "source_id": folder.name,
                "task_id": metadata.get("task_id"),
                "initial_judgment": metadata.get("judgment") or folder.parent.name,
                "initial_reviewer": metadata.get("initial_reviewer"),
                "initial_timestamp": metadata.get("timestamp"),
                "engine_result": metadata.get("engine_result"),
                "regions": regions or [],
            },
            reviewer=reviewer,
            note=note,
            regions=regions,
        )
        self._sync_review_links(record)
        return self._read_entry(folder)

    def revoke_review(
        self,
        entry_folder: str,
        *,
        reviewer: str,
        note: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Remove one feedback source from the reviewed dataset."""
        folder = self._find_entry_folder(entry_folder)
        if folder is None:
            return None
        metadata_path = folder / "metadata.json"
        metadata = self._read_metadata_file(metadata_path)
        if metadata is None:
            return None
        sample_id = str(metadata.get("reviewed_sample_id") or "").strip()
        if not sample_id:
            return self._read_entry(folder)
        self.reviewed.revoke_source(
            sample_id,
            folder.name,
            reviewer=reviewer,
            note=note,
        )
        for key in (
            "reviewed_sample_id",
            "true_label",
            "true_label_name",
            "second_reviewed_at",
            "second_reviewer",
            "second_review_note",
            "second_review_regions",
        ):
            metadata.pop(key, None)
        metadata.update(
            {
                "review_status": "pending",
                "review_revoked_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "review_revoked_by": str(reviewer or "unknown"),
                "review_revoke_note": str(note or ""),
            }
        )
        self._write_metadata(metadata_path, metadata)
        return self._read_entry(folder)

    @staticmethod
    def _clear_review_fields(
        metadata: Dict[str, Any],
        *,
        reviewer: str,
        note: str,
        event: str,
    ) -> None:
        for key in (
            "reviewed_sample_id",
            "true_label",
            "true_label_name",
            "second_reviewed_at",
            "second_reviewer",
            "second_review_note",
            "second_review_regions",
        ):
            metadata.pop(key, None)
        metadata.update(
            {
                "review_status": "pending",
                f"review_{event}_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
                f"review_{event}_by": str(reviewer or "unknown"),
                f"review_{event}_note": str(note or ""),
            }
        )

    def update_reviewed_sample(
        self,
        sample_id: str,
        *,
        original_filename: Optional[str] = None,
        label: Optional[int] = None,
        regions: Optional[List[Dict[str, Any]]] = None,
        reviewer: str,
        note: str = "",
    ) -> Dict[str, Any]:
        record = self.reviewed.update_entry(
            sample_id,
            original_filename=original_filename,
            label=label,
            regions=regions,
            reviewer=reviewer,
            note=note,
        )
        self._sync_review_links(record)
        return record

    def delete_reviewed_sample(
        self,
        sample_id: str,
        *,
        reviewer: str,
        note: str = "",
    ) -> bool:
        record = self.reviewed.get_entry(sample_id)
        if record is None:
            return False
        source_ids = [
            str(item.get("source_id") or "").strip()
            for item in list(record.get("sources") or [])
        ]
        if not self.reviewed.delete_entry(sample_id):
            return False
        for source_id in source_ids:
            folder = self._find_entry_folder(source_id)
            if folder is None:
                continue
            metadata_path = folder / "metadata.json"
            metadata = self._read_metadata_file(metadata_path)
            if metadata is None:
                continue
            self._clear_review_fields(
                metadata,
                reviewer=reviewer,
                note=note,
                event="deleted",
            )
            self._write_metadata(metadata_path, metadata)
        return True

    def confirm_suspicious(self, entry_folder: str, final_judgment: str) -> Optional[Dict[str, Any]]:
        """将 suspicious 条目确认后移入 correct 或 wrong 目录。"""
        src = self.suspicious_dir / Path(entry_folder).name
        if not src.exists():
            return None

        entry = self.update_entry(src.name, final_judgment)
        if entry:
            entry["confirmed_at"] = entry.get("updated_at")
            folder = self._find_entry_folder(src.name)
            if folder:
                meta_file = folder / "metadata.json"
                with open(meta_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                metadata["confirmed_at"] = entry["confirmed_at"]
                self._write_metadata(meta_file, metadata)
                return self._read_entry(folder)
        return entry

    def list_entries(
        self,
        judgment_filter: Optional[str] = None,
        review_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """列出反馈条目。"""
        entries = []
        search_dirs = []
        if judgment_filter:
            mapping = {"correct": self.correct_dir, "wrong": self.wrong_dir, "suspicious": self.suspicious_dir}
            d = mapping.get(judgment_filter)
            if d:
                search_dirs.append(d)
        else:
            search_dirs = [self.correct_dir, self.wrong_dir, self.suspicious_dir]

        for d in search_dirs:
            if not d.exists():
                continue
            for folder in sorted(d.iterdir(), reverse=True):
                entry = self._read_entry(folder)
                if entry and (
                    not review_filter
                    or review_filter == "all"
                    or entry.get("review_status") == review_filter
                ):
                    entries.append(entry)

        return entries
