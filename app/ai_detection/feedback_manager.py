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


class FeedbackManager:
    """管理用户对检测结果的标注反馈。"""

    def __init__(self, config_path: str = "config.yaml"):
        config_file = Path(config_path)
        if not config_file.is_absolute():
            config_file = (Path(__file__).resolve().parent / config_file).resolve()
        with open(config_file, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        fb_cfg = self.config.get("feedback", {})
        self.base_dir = Path(self._resolve_path(fb_cfg.get("storage_dir", "feedback")))
        self.correct_dir = self.base_dir / "correct"
        self.wrong_dir = self.base_dir / "wrong"
        self.suspicious_dir = self.base_dir / "suspicious"

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
        }
        with open(target_dir / "metadata.json", "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

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
        return metadata

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

    def update_entry(self, entry_folder: str, judgment: str, note: Optional[str] = None) -> Optional[Dict[str, Any]]:
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
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        return self._read_entry(dst)

    def delete_entry(self, entry_folder: str) -> bool:
        """删除一条反馈记录，相当于撤销该标注。"""
        folder = self._find_entry_folder(entry_folder)
        if folder is None:
            return False
        shutil.rmtree(str(folder))
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
                with open(meta_file, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                return self._read_entry(folder)
        return entry

    def list_entries(self, judgment_filter: Optional[str] = None) -> List[Dict[str, Any]]:
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
                if entry:
                    entries.append(entry)

        return entries
