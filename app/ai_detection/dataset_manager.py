"""Training dataset management for the image tamper detector."""
from __future__ import annotations

import json
import mimetypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from PIL import Image


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}


@dataclass(frozen=True)
class DatasetFile:
    path: Path
    stem: str


class DatasetManager:
    """Manage images/ and locate_json/ used by the training pipeline."""

    def __init__(self, config_path: str = "config.yaml"):
        config_file = Path(config_path)
        if not config_file.is_absolute():
            config_file = (Path(__file__).resolve().parent / config_file).resolve()
        with open(config_file, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f) or {}
        self.base_dir = config_file.parent
        dataset_cfg = self.config.get("dataset", {})
        self.image_dir = Path(self._resolve(dataset_cfg.get("image_dir", "images")))
        self.json_dir = Path(self._resolve(dataset_cfg.get("json_dir", "locate_json")))
        self.image_dir.mkdir(parents=True, exist_ok=True)
        self.json_dir.mkdir(parents=True, exist_ok=True)

    def _resolve(self, path_str: str) -> str:
        p = Path(path_str)
        if p.is_absolute():
            return str(p)
        return str((self.base_dir / p).resolve())

    @staticmethod
    def label_for_name(name: str) -> int:
        """Return 0 for normal, 1 for tampered, matching the training pipeline."""
        return 0 if "no" in Path(name).name.lower() else 1

    @staticmethod
    def label_text(label: int) -> str:
        return "正常" if int(label) == 0 else "篡改"

    @staticmethod
    def _safe_name(name: str) -> str:
        safe = Path(str(name or "").strip()).name
        if not safe or safe in (".", ".."):
            raise ValueError("训练样本文件名无效")
        return safe

    @staticmethod
    def _is_enhanced_stem(stem: str) -> bool:
        return stem.lower().endswith("_enhanced")

    @staticmethod
    def _base_stem(stem: str) -> str:
        return stem[:-9] if DatasetManager._is_enhanced_stem(stem) else stem

    @staticmethod
    def _retag_stem(stem: str, label: int) -> str:
        enhanced = DatasetManager._is_enhanced_stem(stem)
        base = DatasetManager._base_stem(stem)
        low = base.lower()
        if low.startswith("no"):
            rest = base[2:]
        elif low.startswith("p"):
            rest = base[1:]
        else:
            rest = f" ({base})"
        prefix = "no" if int(label) == 0 else "p"
        tagged = f"{prefix}{rest}"
        return f"{tagged}_enhanced" if enhanced else tagged

    def _image_path(self, filename: str) -> Optional[Path]:
        name = self._safe_name(filename)
        path = self.image_dir / name
        if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES:
            return path
        return None

    def _json_path_for_stem(self, stem: str) -> Optional[Path]:
        path = self.json_dir / f"{stem}.json"
        return path if path.is_file() else None

    def _entry_for_path(self, path: Path) -> Optional[Dict[str, Any]]:
        if not path.is_file() or path.suffix.lower() not in IMAGE_SUFFIXES:
            return None
        try:
            stat = path.stat()
        except OSError:
            return None

        width = height = None
        try:
            with Image.open(path) as img:
                width, height = img.size
        except Exception:
            pass

        stem = path.stem
        base_stem = self._base_stem(stem)
        json_path = self._json_path_for_stem(base_stem)
        label = self.label_for_name(path.name)
        return {
            "filename": path.name,
            "stem": stem,
            "base_stem": base_stem,
            "label": label,
            "label_text": self.label_text(label),
            "is_enhanced": self._is_enhanced_stem(stem),
            "has_annotation": json_path is not None,
            "json_name": json_path.name if json_path else None,
            "size_bytes": int(stat.st_size),
            "width": width,
            "height": height,
            "modified_at": int(stat.st_mtime),
            "image_url": f"/ai-detection/api/v3/training-dataset/{path.name}/image",
            "json_url": (
                f"/ai-detection/api/v3/training-dataset/{path.name}/annotation"
                if json_path
                else None
            ),
        }

    def list_entries(self, label: Optional[int] = None, include_enhanced: bool = True) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for path in sorted(self.image_dir.iterdir(), key=lambda p: p.name.lower()):
            entry = self._entry_for_path(path)
            if not entry:
                continue
            if not include_enhanced and entry["is_enhanced"]:
                continue
            if label is not None and int(entry["label"]) != int(label):
                continue
            entries.append(entry)
        return entries

    def get_entry(self, filename: str) -> Optional[Dict[str, Any]]:
        path = self._image_path(filename)
        if path is None:
            return None
        return self._entry_for_path(path)

    def get_image_file(self, filename: str) -> Optional[Path]:
        return self._image_path(filename)

    def image_media_type(self, filename: str) -> str:
        guessed, _enc = mimetypes.guess_type(filename)
        return guessed or "application/octet-stream"

    def get_annotation(self, filename: str) -> Optional[Dict[str, Any]]:
        path = self._image_path(filename)
        if path is None:
            return None
        json_path = self._json_path_for_stem(self._base_stem(path.stem))
        if json_path is None:
            return None
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def update_label(self, filename: str, label: int) -> Optional[Dict[str, Any]]:
        label_int = int(label)
        if label_int not in (0, 1):
            raise ValueError("训练标签只能是 0 或 1")
        src = self._image_path(filename)
        if src is None:
            return None
        if self.label_for_name(src.name) == label_int:
            return self._entry_for_path(src)

        base_stem = self._base_stem(src.stem)
        image_moves = []
        for candidate in self.image_dir.iterdir():
            if (
                candidate.is_file()
                and candidate.suffix.lower() in IMAGE_SUFFIXES
                and self._base_stem(candidate.stem) == base_stem
            ):
                target = candidate.with_name(f"{self._retag_stem(candidate.stem, label_int)}{candidate.suffix}")
                image_moves.append((candidate, target))

        json_moves = []
        old_json = self._json_path_for_stem(base_stem)
        if old_json is not None:
            json_moves.append((old_json, self.json_dir / f"{self._retag_stem(base_stem, label_int)}.json"))

        for _old, new in image_moves + json_moves:
            if new.exists():
                raise FileExistsError(f"目标文件已存在: {new.name}")

        selected_target = None
        for old, new in image_moves:
            old.rename(new)
            if old.name == src.name:
                selected_target = new
        for old, new in json_moves:
            old.rename(new)

        return self._entry_for_path(selected_target or image_moves[0][1])

    def delete_entry(self, filename: str, delete_family: bool = True) -> bool:
        path = self._image_path(filename)
        if path is None:
            return False

        targets = [path]
        base_stem = self._base_stem(path.stem)
        if delete_family:
            for candidate in self.image_dir.iterdir():
                if (
                    candidate.is_file()
                    and candidate.suffix.lower() in IMAGE_SUFFIXES
                    and self._base_stem(candidate.stem) == base_stem
                    and candidate not in targets
                ):
                    targets.append(candidate)
        removed = False
        for target in targets:
            try:
                target.unlink()
                removed = True
            except FileNotFoundError:
                pass

        json_path = self._json_path_for_stem(base_stem)
        if json_path is not None:
            try:
                json_path.unlink()
            except FileNotFoundError:
                pass
        return removed

    def summary(self) -> Dict[str, int]:
        entries = self.list_entries()
        normal = sum(1 for entry in entries if entry["label"] == 0)
        tampered = sum(1 for entry in entries if entry["label"] == 1)
        annotations = sum(1 for entry in entries if entry["has_annotation"] and not entry["is_enhanced"])
        return {
            "total": len(entries),
            "normal": normal,
            "tampered": tampered,
            "annotations": annotations,
        }
