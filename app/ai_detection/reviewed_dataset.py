"""Persistent dataset for samples with an explicit second-review truth label."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import threading
import unicodedata
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

from PIL import Image
from pypinyin import lazy_pinyin


class ReviewedDatasetError(RuntimeError):
    code = "REVIEWED_DATASET_ERROR"


class ReviewedDatasetConflict(ReviewedDatasetError):
    code = "REVIEWED_LABEL_CONFLICT"


class ReviewedDatasetNotFound(ReviewedDatasetError):
    code = "REVIEWED_SAMPLE_NOT_FOUND"


_FORMAT_INFO = {
    "JPEG": (".jpg", "image/jpeg"),
    "PNG": (".png", "image/png"),
    "WEBP": (".webp", "image/webp"),
}
_LABEL_DIRS = {0: "normal", 1: "tampered"}
_MAX_DISPLAY_NAME = 512


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_display_filename(value: Optional[str], fallback: str = "image") -> str:
    raw = unicodedata.normalize("NFC", str(value or "").strip()).replace("\\", "/")
    name = raw.rsplit("/", 1)[-1].strip()
    name = "".join(
        character
        for character in name
        if not unicodedata.category(character).startswith("C")
    ).strip()
    if not name or name in {".", ".."}:
        name = fallback
    return name[:_MAX_DISPLAY_NAME]


def _pinyin_stem(filename: str) -> str:
    stem = Path(filename).stem or "image"
    transliterated = "".join(lazy_pinyin(stem, errors=lambda chars: list(chars))).lower()
    safe = re.sub(r"[^a-z0-9]+", "-", transliterated).strip("-")
    return (safe or "image")[:96]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _image_info(path: Path) -> tuple[str, str]:
    try:
        with Image.open(path) as image:
            image_format = str(image.format or "").upper()
            image.verify()
    except Exception as exc:
        raise ReviewedDatasetError("二审样本不是有效图片") from exc
    if image_format not in _FORMAT_INFO:
        raise ReviewedDatasetError("二审样本仅支持 JPEG、PNG、WebP")
    return _FORMAT_INFO[image_format]


class ReviewedDatasetManager:
    """Store reviewed samples by truth label with SHA-based deduplication."""

    def __init__(self, feedback_dir: str | Path):
        self.feedback_dir = Path(feedback_dir).resolve()
        self.reviewed_dir = self.feedback_dir / "reviewed"
        self.normal_dir = self.reviewed_dir / "normal"
        self.tampered_dir = self.reviewed_dir / "tampered"
        self._thread_lock = threading.RLock()
        for directory in (self.normal_dir, self.tampered_dir):
            directory.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        lock_path = self.reviewed_dir / ".reviewed.lock"
        lock_path.touch(exist_ok=True)
        with self._thread_lock, lock_path.open("r+") as lock_file:
            try:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                except (NameError, OSError):
                    pass

    @staticmethod
    def _validate_label(label: int) -> int:
        if isinstance(label, bool) or int(label) not in _LABEL_DIRS:
            raise ValueError("label 必须为 0（正常）或 1（篡改）")
        return int(label)

    def _directory(self, label: int) -> Path:
        return self.reviewed_dir / _LABEL_DIRS[self._validate_label(label)]

    @staticmethod
    def _read_metadata(path: Path) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def _write_metadata(path: Path, record: Dict[str, Any]) -> None:
        tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with tmp.open("w", encoding="utf-8") as stream:
                json.dump(record, stream, ensure_ascii=False, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)

    def _iter_records(self) -> Iterator[tuple[Path, Dict[str, Any]]]:
        for directory in (self.normal_dir, self.tampered_dir):
            for metadata_path in sorted(directory.glob("*.json")):
                record = self._read_metadata(metadata_path)
                if record:
                    yield metadata_path, record

    def _find(self, sample_id: str) -> Optional[tuple[Path, Dict[str, Any]]]:
        wanted = str(sample_id or "").strip().lower()
        if not wanted:
            return None
        for metadata_path, record in self._iter_records():
            if str(record.get("sample_id") or "").lower() == wanted:
                return metadata_path, record
        return None

    def _find_by_sha(self, content_sha256: str) -> Optional[tuple[Path, Dict[str, Any]]]:
        for metadata_path, record in self._iter_records():
            if record.get("content_sha256") == content_sha256:
                return metadata_path, record
        return None

    def _storage_basename(self, original_filename: str, digest: str, label: int) -> str:
        directory = self._directory(label)
        stem = _pinyin_stem(original_filename)
        for length in (8, 12, 16, 64):
            basename = f"{stem}__{digest[:length]}"
            metadata_path = directory / f"{basename}.json"
            if not metadata_path.exists():
                return basename
            record = self._read_metadata(metadata_path)
            if record and record.get("content_sha256") == digest:
                return basename
        raise ReviewedDatasetConflict("训练样本文件名冲突")

    @staticmethod
    def _source_record(
        source: Dict[str, Any],
        *,
        original_filename: str,
        reviewer: str,
        note: str,
        reviewed_at: str,
    ) -> Dict[str, Any]:
        source_id = str(source.get("source_id") or source.get("folder_name") or "").strip()
        if not source_id:
            raise ValueError("source.source_id 不能为空")
        record = dict(source)
        record.update(
            {
                "source_id": source_id,
                "original_filename": original_filename,
                "reviewer": str(reviewer or "unknown"),
                "reviewed_at": reviewed_at,
                "note": str(note or ""),
            }
        )
        return record

    def add_review(
        self,
        *,
        image_path: str | Path,
        label: int,
        original_filename: Optional[str],
        source: Dict[str, Any],
        reviewer: str,
        note: str = "",
    ) -> Dict[str, Any]:
        label = self._validate_label(label)
        source_path = Path(image_path).resolve()
        if not source_path.is_file():
            raise FileNotFoundError(source_path)
        extension, media_type = _image_info(source_path)
        digest = _sha256(source_path)
        display_name = normalize_display_filename(
            original_filename,
            fallback=f"task-{str(source.get('task_id') or digest[:8])}{extension}",
        )
        reviewed_at = _now_iso()
        source_record = self._source_record(
            source,
            original_filename=display_name,
            reviewer=reviewer,
            note=note,
            reviewed_at=reviewed_at,
        )

        with self._locked():
            existing = self._find_by_sha(digest)
            if existing:
                metadata_path, record = existing
                if int(record.get("label", -1)) != label:
                    raise ReviewedDatasetConflict(
                        "相同图片已存在相反的真实标签，请先撤销或改判原二审记录"
                    )
                sources = list(record.get("sources") or [])
                for index, item in enumerate(sources):
                    if item.get("source_id") == source_record["source_id"]:
                        sources[index] = source_record
                        break
                else:
                    sources.append(source_record)
                record["sources"] = sources
                record["updated_at"] = reviewed_at
                self._write_metadata(metadata_path, record)
                return record

            basename = self._storage_basename(display_name, digest, label)
            target_dir = self._directory(label)
            image_target = target_dir / f"{basename}{extension}"
            metadata_target = target_dir / f"{basename}.json"
            temp_image = target_dir / f".{basename}.{uuid.uuid4().hex}.tmp"
            try:
                shutil.copyfile(source_path, temp_image)
                if _sha256(temp_image) != digest:
                    raise ReviewedDatasetError("二审训练副本校验失败")
                os.replace(temp_image, image_target)
                record = {
                    "sample_id": digest,
                    "content_sha256": digest,
                    "label": label,
                    "label_name": "正常" if label == 0 else "篡改",
                    "original_filename": display_name,
                    "storage_filename": image_target.name,
                    "media_type": media_type,
                    "size_bytes": source_path.stat().st_size,
                    "created_at": reviewed_at,
                    "updated_at": reviewed_at,
                    "reviewer": str(reviewer or "unknown"),
                    "review_note": str(note or ""),
                    "sources": [source_record],
                }
                self._write_metadata(metadata_target, record)
                return record
            except Exception:
                temp_image.unlink(missing_ok=True)
                if not metadata_target.exists():
                    image_target.unlink(missing_ok=True)
                raise

    def get_entry(self, sample_id: str) -> Optional[Dict[str, Any]]:
        found = self._find(sample_id)
        return dict(found[1]) if found else None

    def image_path(self, sample_id: str) -> Optional[Path]:
        found = self._find(sample_id)
        if not found:
            return None
        metadata_path, record = found
        name = Path(str(record.get("storage_filename") or "")).name
        path = metadata_path.parent / name
        return path if name and path.is_file() else None

    def list_entries(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        label: Optional[int] = None,
    ) -> Dict[str, Any]:
        if label is not None:
            label = self._validate_label(label)
        rows = [
            dict(record)
            for _path, record in self._iter_records()
            if label is None or int(record.get("label", -1)) == label
        ]
        rows.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        page = max(1, int(page))
        page_size = min(200, max(1, int(page_size)))
        offset = (page - 1) * page_size
        return {
            "total": len(rows),
            "page": page,
            "page_size": page_size,
            "items": rows[offset : offset + page_size],
        }

    def reclassify(
        self,
        sample_id: str,
        label: int,
        *,
        reviewer: str,
        note: str = "",
    ) -> Dict[str, Any]:
        label = self._validate_label(label)
        with self._locked():
            found = self._find(sample_id)
            if not found:
                raise ReviewedDatasetNotFound("二审训练样本不存在")
            metadata_path, record = found
            if int(record.get("label", -1)) == label:
                return record
            image_path = self.image_path(sample_id)
            if image_path is None:
                raise ReviewedDatasetNotFound("二审训练样本图片不存在")
            target_dir = self._directory(label)
            target_image = target_dir / image_path.name
            target_metadata = target_dir / metadata_path.name
            if target_image.exists() or target_metadata.exists():
                raise ReviewedDatasetConflict("目标标签目录已存在同名训练样本")
            os.replace(image_path, target_image)
            try:
                record.update(
                    {
                        "label": label,
                        "label_name": "正常" if label == 0 else "篡改",
                        "updated_at": _now_iso(),
                        "reviewer": str(reviewer or "unknown"),
                        "review_note": str(note or ""),
                    }
                )
                self._write_metadata(target_metadata, record)
                metadata_path.unlink(missing_ok=True)
            except Exception:
                os.replace(target_image, image_path)
                target_metadata.unlink(missing_ok=True)
                raise
            return record

    def update_entry(
        self,
        sample_id: str,
        *,
        original_filename: Optional[str] = None,
        label: Optional[int] = None,
        reviewer: str = "unknown",
        note: str = "",
    ) -> Dict[str, Any]:
        if label is not None:
            self.reclassify(sample_id, label, reviewer=reviewer, note=note)
        with self._locked():
            found = self._find(sample_id)
            if not found:
                raise ReviewedDatasetNotFound("二审训练样本不存在")
            metadata_path, record = found
            if original_filename is not None:
                record["original_filename"] = normalize_display_filename(
                    original_filename,
                    fallback=str(record.get("original_filename") or "image"),
                )
                record["updated_at"] = _now_iso()
                self._write_metadata(metadata_path, record)
            return record

    def revoke_source(
        self,
        sample_id: str,
        source_id: str,
        *,
        reviewer: str,
        note: str = "",
    ) -> Dict[str, Any]:
        wanted = str(source_id or "").strip()
        with self._locked():
            found = self._find(sample_id)
            if not found:
                raise ReviewedDatasetNotFound("二审训练样本不存在")
            metadata_path, record = found
            sources = [
                item for item in list(record.get("sources") or [])
                if str(item.get("source_id") or "") != wanted
            ]
            if len(sources) == len(list(record.get("sources") or [])):
                raise ReviewedDatasetNotFound("二审来源不存在")
            if not sources:
                image_path = self.image_path(sample_id)
                if image_path:
                    image_path.unlink(missing_ok=True)
                metadata_path.unlink(missing_ok=True)
                return {"sample_id": sample_id, "sample_deleted": True}
            record["sources"] = sources
            record["updated_at"] = _now_iso()
            record["last_revoked_by"] = str(reviewer or "unknown")
            record["last_revoke_note"] = str(note or "")
            self._write_metadata(metadata_path, record)
            return {"sample_id": sample_id, "sample_deleted": False, "entry": record}

    def delete_entry(self, sample_id: str) -> bool:
        with self._locked():
            found = self._find(sample_id)
            if not found:
                return False
            metadata_path, _record = found
            image_path = self.image_path(sample_id)
            if image_path:
                image_path.unlink(missing_ok=True)
            metadata_path.unlink(missing_ok=True)
            return True
