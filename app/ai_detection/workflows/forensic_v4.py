"""Offline v4 local forensic model.

This module is deliberately independent from the v3 production engine. It creates
auditable ROI examples, trains a CPU-sized RGB/SRM/scalar fusion model, and writes
candidate artifacts without touching the active model registry.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import cv2
import numpy as np

from app.ai_detection.runtime.paths import annotation_root, legacy_annotation_dir

try:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, Dataset
    import torchvision.models as tv_models
except ImportError:  # pragma: no cover - reportable environment failure
    torch = None
    nn = None
    DataLoader = None
    Dataset = None
    tv_models = None


FIELD_TYPES = {"amount", "name", "time"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
SCALAR_FEATURE_NAMES = (
    "ela_mean",
    "ela_p95",
    "noise_std",
    "noise_mad",
    "dct_neighbor_delta",
    "dct_block_std",
    "edge_density",
    "edge_border_density",
    "color_std",
    "color_neighbor_delta",
    "ocr_confidence",
    "ocr_height_cv",
    "ocr_baseline_cv",
)


class V4DataError(RuntimeError):
    pass


@dataclass(frozen=True)
class ROIRecord:
    image_path: Path
    bbox: tuple[int, int, int, int]
    field_type: str
    label: int
    group_id: str
    split: str
    source: str
    is_derived: bool = False
    parent_group_id: Optional[str] = None
    transform: Optional[dict[str, Any]] = None
    ocr_tokens: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class ForensicExample:
    image_path: Path
    image: np.ndarray
    bbox: tuple[int, int, int, int]
    label: int
    group_id: str
    split: str
    source: str
    field_type: str
    transform: dict[str, Any]
    parent_group_id: Optional[str] = None
    ocr_tokens: tuple[dict[str, Any], ...] = ()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def image_phash(image: np.ndarray) -> str:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    small = cv2.resize(gray, (32, 32), interpolation=cv2.INTER_AREA)
    coeff = cv2.dct(np.float32(small))[:8, :8]
    median = float(np.median(coeff.flatten()[1:]))
    bits = (coeff > median).flatten()
    return f"{int(''.join('1' if value else '0' for value in bits), 2):016x}"


def normalize_field_type(value: Any) -> Optional[str]:
    raw = str(value or "").strip().lower()
    if raw in {"amount", "money", "price", "金额"}:
        return "amount"
    if raw in {"name", "姓名", "收款人", "付款人"}:
        return "name"
    if raw in {"time", "date", "datetime", "时间", "日期"}:
        return "time"
    return None


def normalize_bbox(raw: Sequence[Any], width: int, height: int) -> tuple[int, int, int, int]:
    if len(raw) != 4:
        raise V4DataError("ROI bbox 必须包含四个坐标")
    x1, y1, x2, y2 = (int(round(float(value))) for value in raw)
    x1 = max(0, min(x1, width - 1))
    y1 = max(0, min(y1, height - 1))
    x2 = max(x1 + 1, min(x2, width))
    y2 = max(y1 + 1, min(y2, height))
    return x1, y1, x2, y2


def classify_v4_result(
    scores: Sequence[float],
    *,
    low_threshold: float,
    high_threshold: float,
    quality_score: float = 1.0,
    template_coverage: float = 1.0,
) -> tuple[str, float]:
    """Map ROI probabilities to v4's three production states.

    A high-confidence local edit remains tampered even when the surrounding
    image is low quality.  Low-risk images with insufficient quality or
    template coverage stay suspicious instead of being promoted to normal.
    """
    if not scores:
        return "无法自动检测", 0.0
    peak = max(float(score) for score in scores)
    if peak >= high_threshold:
        return "篡改", round(peak, 6)
    if peak >= low_threshold or quality_score < 0.45 or template_coverage < 0.50:
        return "可疑", round(max(peak, 1.0 - quality_score, 1.0 - template_coverage), 6)
    return "正常", round(1.0 - peak, 6)


def estimate_quality_score(image: np.ndarray) -> float:
    """Estimate input quality without using filename, path, or content hash."""
    if image is None or image.size == 0:
        return 0.0
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    sharpness = min(1.0, float(cv2.Laplacian(gray, cv2.CV_64F).var()) / 80.0)
    resolution = min(1.0, min(image.shape[:2]) / 400.0)
    return round(0.6 * sharpness + 0.4 * resolution, 6)


class ForensicFeatureExtractorV4:
    def __init__(self, output_size: int = 224, context_ratio: float = 0.20):
        self.output_size = int(output_size)
        self.context_ratio = float(context_ratio)

    def expand_bbox(self, bbox: Sequence[int], width: int, height: int) -> tuple[int, int, int, int]:
        x1, y1, x2, y2 = normalize_bbox(bbox, width, height)
        margin_x = max(2, int((x2 - x1) * self.context_ratio))
        margin_y = max(2, int((y2 - y1) * self.context_ratio))
        return normalize_bbox(
            (x1 - margin_x, y1 - margin_y, x2 + margin_x, y2 + margin_y),
            width,
            height,
        )

    def crop(self, image: np.ndarray, bbox: Sequence[int]) -> np.ndarray:
        if image is None or image.size == 0:
            raise V4DataError("ROI 图片为空")
        x1, y1, x2, y2 = self.expand_bbox(bbox, image.shape[1], image.shape[0])
        return image[y1:y2, x1:x2].copy()

    def prepare_rgb(self, crop: np.ndarray) -> np.ndarray:
        rgb = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB)
        height, width = rgb.shape[:2]
        scale = min(self.output_size / max(width, 1), self.output_size / max(height, 1))
        resized = cv2.resize(
            rgb,
            (max(1, int(round(width * scale))), max(1, int(round(height * scale)))),
            interpolation=cv2.INTER_AREA,
        )
        canvas = np.zeros((self.output_size, self.output_size, 3), dtype=np.uint8)
        y = (self.output_size - resized.shape[0]) // 2
        x = (self.output_size - resized.shape[1]) // 2
        canvas[y:y + resized.shape[0], x:x + resized.shape[1]] = resized
        return canvas

    @staticmethod
    def _highpass(gray: np.ndarray) -> np.ndarray:
        kernel = np.array(
            [[-1, -1, -1], [-1, 8, -1], [-1, -1, -1]],
            dtype=np.float32,
        )
        return cv2.filter2D(gray.astype(np.float32), -1, kernel)

    def prepare_srm(self, crop: np.ndarray) -> np.ndarray:
        gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
        residual = self._highpass(gray)
        residual = cv2.resize(
            residual,
            (self.output_size, self.output_size),
            interpolation=cv2.INTER_AREA,
        )
        residual = np.clip(residual / 32.0, -1.0, 1.0)
        return residual.astype(np.float32)[None, ...]

    @staticmethod
    def _ela(crop: np.ndarray) -> tuple[float, float]:
        ok, encoded = cv2.imencode(".jpg", crop, [cv2.IMWRITE_JPEG_QUALITY, 90])
        if not ok:
            return 0.0, 0.0
        recompressed = cv2.imdecode(encoded, cv2.IMREAD_COLOR)
        if recompressed is None or recompressed.shape != crop.shape:
            return 0.0, 0.0
        diff = cv2.absdiff(crop, recompressed).astype(np.float32)
        gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
        return float(gray.mean() / 255.0), float(np.percentile(gray, 95) / 255.0)

    @staticmethod
    def _dct_features(gray: np.ndarray) -> tuple[float, float]:
        height = (gray.shape[0] // 8) * 8
        width = (gray.shape[1] // 8) * 8
        if height < 16 or width < 16:
            return 0.0, 0.0
        blocks = []
        for y in range(0, height, 8):
            for x in range(0, width, 8):
                blocks.append(cv2.dct(np.float32(gray[y:y + 8, x:x + 8]))[1:, 1:])
        values = np.asarray(blocks, dtype=np.float32)
        neighbor_delta = float(np.mean(np.abs(values[1:] - values[:-1]))) / 255.0
        return neighbor_delta, float(values.std()) / 255.0

    def scalar_features(
        self,
        crop: np.ndarray,
        ocr_tokens: Optional[Sequence[dict[str, Any]]] = None,
    ) -> np.ndarray:
        gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
        ela_mean, ela_p95 = self._ela(crop)
        residual = self._highpass(gray)
        noise_std = float(residual.std()) / 32.0
        noise_mad = float(np.median(np.abs(residual - np.median(residual)))) / 32.0
        dct_neighbor_delta, dct_block_std = self._dct_features(gray)
        edges = cv2.Canny(gray, 80, 180)
        edge_density = float(np.mean(edges > 0))
        border = np.concatenate((edges[0, :], edges[-1, :], edges[:, 0], edges[:, -1]))
        edge_border_density = float(np.mean(border > 0)) if border.size else 0.0
        color_std = float(crop.astype(np.float32).std()) / 255.0
        if crop.shape[0] > 4 and crop.shape[1] > 4:
            left = crop[:, :crop.shape[1] // 2].astype(np.float32)
            right = crop[:, crop.shape[1] - left.shape[1]:].astype(np.float32)
            color_neighbor_delta = float(np.abs(left.mean() - right.mean())) / 255.0
        else:
            color_neighbor_delta = 0.0

        tokens = list(ocr_tokens or [])
        confidences = [float(item.get("confidence", item.get("conf", 0.0)) or 0.0) for item in tokens]
        heights = []
        baselines = []
        for item in tokens:
            box = item.get("bbox") or []
            if len(box) >= 4:
                _, top, _, bottom = [float(value) for value in box[:4]]
                heights.append(max(0.0, bottom - top))
                baselines.append(bottom)
        confidence = float(np.mean(confidences)) if confidences else 0.0
        height_cv = float(np.std(heights) / max(np.mean(heights), 1.0)) if heights else 0.0
        baseline_cv = float(np.std(baselines) / max(abs(np.mean(baselines)), 1.0)) if baselines else 0.0
        values = np.array(
            [
                ela_mean, ela_p95, noise_std, noise_mad,
                dct_neighbor_delta, dct_block_std, edge_density,
                edge_border_density, color_std, color_neighbor_delta,
                confidence, height_cv, baseline_cv,
            ],
            dtype=np.float32,
        )
        return np.nan_to_num(values, nan=0.0, posinf=10.0, neginf=-10.0)

    def extract(
        self,
        image: np.ndarray,
        bbox: Sequence[int],
        ocr_tokens: Optional[Sequence[dict[str, Any]]] = None,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        crop = self.crop(image, bbox)
        return self.prepare_rgb(crop), self.prepare_srm(crop), self.scalar_features(crop, ocr_tokens)


class SyntheticTamperGenerator:
    """Deterministic local edits used when paired originals are unavailable."""

    def __init__(self, seed: int = 42):
        self.seed = int(seed)

    def generate(
        self,
        image: np.ndarray,
        bbox: Sequence[int],
        *,
        group_id: str,
        field_type: str,
        variant: int,
    ) -> tuple[np.ndarray, dict[str, Any]]:
        rng = np.random.default_rng(self.seed + variant + sum(ord(c) for c in group_id))
        output = image.copy()
        x1, y1, x2, y2 = normalize_bbox(bbox, image.shape[1], image.shape[0])
        operation = ("erase_render", "local_reencode", "patch_blend", "resample")[variant % 4]
        if operation == "erase_render":
            mask = np.zeros(image.shape[:2], dtype=np.uint8)
            mask[y1:y2, x1:x2] = 255
            output = cv2.inpaint(output, mask, 3, cv2.INPAINT_TELEA)
            color = tuple(int(value) for value in rng.integers(0, 90, size=3))
            text = {"amount": "999.99", "name": "张三", "time": "2099-01-01"}.get(field_type, "999")
            baseline = max(y1 + 2, min(y2 - 2, y1 + int((y2 - y1) * 0.75)))
            cv2.putText(output, text, (x1 + 2, baseline), cv2.FONT_HERSHEY_SIMPLEX,
                        max(0.25, min(1.2, (y2 - y1) / 42.0)), color, 1, cv2.LINE_AA)
        elif operation == "local_reencode":
            crop = output[y1:y2, x1:x2]
            ok, encoded = cv2.imencode(".jpg", crop, [cv2.IMWRITE_JPEG_QUALITY, int(rng.integers(25, 65))])
            if ok:
                decoded = cv2.imdecode(encoded, cv2.IMREAD_COLOR)
                if decoded is not None:
                    decoded = cv2.resize(decoded, (x2 - x1, y2 - y1), interpolation=cv2.INTER_CUBIC)
                    output[y1:y2, x1:x2] = decoded
        elif operation == "patch_blend":
            crop = output[y1:y2, x1:x2].copy()
            if crop.size:
                shifted = cv2.warpAffine(crop, np.float32([[1, 0, 1], [0, 1, 0]]),
                                          (crop.shape[1], crop.shape[0]), borderMode=cv2.BORDER_REFLECT)
                output[y1:y2, x1:x2] = cv2.addWeighted(crop, 0.35, shifted, 0.65, 0)
        else:
            crop = output[y1:y2, x1:x2]
            if crop.size:
                small = cv2.resize(crop, (max(2, crop.shape[1] // 2), max(2, crop.shape[0] // 2)))
                output[y1:y2, x1:x2] = cv2.resize(small, (x2 - x1, y2 - y1), interpolation=cv2.INTER_NEAREST)
        return output, {
            "name": operation,
            "seed": self.seed + variant + sum(ord(c) for c in group_id),
            "field_type": field_type,
            "bbox": [x1, y1, x2, y2],
            "parent_group_id": group_id,
        }


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise V4DataError("dataset_manifest.json 不可读取") from exc
    if not isinstance(data, dict) or not isinstance(data.get("entries"), list):
        raise V4DataError("数据清单缺少 entries")
    return data


def v4_sidecar_relative_path(relative_image_path: str) -> str:
    """Return a collision-resistant sidecar name derived from the path only.

    This is storage bookkeeping, not an image/content signal.  The relative
    path is included in the sidecar payload so the index remains auditable.
    """
    normalized = Path(str(relative_image_path or "").replace("\\", "/")).as_posix()
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]
    return f"v4/{digest}.json"


def resolve_roi_sidecar(
    image_path: Path,
    manifest: dict[str, Any],
    row: dict[str, Any],
    locate_root: Path,
) -> Optional[Path]:
    """Resolve a row's ROI annotation without ambiguous stem matching."""
    locate_root = annotation_root(locate_root)
    explicit = str(row.get("roi_sidecar") or "").strip()
    if explicit:
        candidate = locate_root / Path(explicit).name if Path(explicit).is_absolute() else locate_root / explicit
        return candidate if candidate.is_file() else None

    relative_path = str(row.get("path") or "")
    stem = image_path.stem
    same_stem_count = sum(
        1
        for item in manifest.get("entries", [])
        if not item.get("is_derived") and Path(str(item.get("path") or "")).stem == stem
    )
    if same_stem_count != 1:
        return None
    legacy = legacy_annotation_dir(locate_root) / f"{stem}.json"
    if not legacy.is_file():
        return None
    try:
        payload = json.loads(legacy.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    annotated_path = str(payload.get("image_path") or "").replace("\\", "/")
    if annotated_path and Path(annotated_path).name != Path(relative_path).name:
        return None
    return legacy


def audit_manifest(manifest: dict[str, Any], image_root: Path) -> dict[str, Any]:
    entries = manifest.get("entries", [])
    seen_sha: dict[str, tuple[str, str]] = {}
    seen_groups: dict[str, str] = {}
    seen_templates: dict[str, str] = {}
    errors: list[str] = []
    split_counts: dict[str, int] = {}
    for row in entries:
        path = str(row.get("path") or "")
        file_path = image_root / path
        if not file_path.is_file() or file_path.suffix.lower() not in IMAGE_EXTENSIONS:
            errors.append("缺失或不支持图片: " + path)
            continue
        split = str(row.get("split") or "train")
        if row.get("is_derived") or split == "derived":
            continue
        digest = str(row.get("sha256") or "").lower()
        if digest:
            prior = seen_sha.get(digest)
            current = (str(row.get("split")), path)
            if prior and prior[0] != current[0]:
                errors.append("同 SHA 跨 split: " + path + " / " + prior[1])
            seen_sha[digest] = current
        group_id = str(row.get("group_id") or row.get("parent_group_id") or path)
        prior_split = seen_groups.get(group_id)
        if prior_split and prior_split != split:
            errors.append("同原图组跨 split: " + group_id)
        seen_groups[group_id] = split
        template_cluster = str(row.get("template_cluster") or "").strip()
        if template_cluster:
            prior_template_split = seen_templates.get(template_cluster)
            if prior_template_split and prior_template_split != split:
                errors.append("同视觉模板簇跨 split: " + template_cluster)
            seen_templates[template_cluster] = split
        split_counts[split] = split_counts.get(split, 0) + 1
    return {
        "passed": not errors,
        "errors": errors,
        "entry_count": len(entries),
        "split_counts": split_counts,
        "sha_count": len(seen_sha),
        "group_count": len(seen_groups),
        "template_cluster_count": len(seen_templates),
    }


def audit_example_groups(rows: Sequence[ForensicExample]) -> dict[str, Any]:
    """Ensure derived examples inherit one parent group and split."""
    split_by_parent: dict[str, str] = {}
    errors: list[str] = []
    for row in rows:
        parent = str(row.parent_group_id or row.group_id or row.image_path)
        prior = split_by_parent.get(parent)
        if prior is not None and prior != row.split:
            errors.append(f"同 parent_group_id 跨 split: {parent} ({prior}/{row.split})")
        split_by_parent[parent] = row.split
    return {"passed": not errors, "errors": errors, "parent_group_count": len(split_by_parent)}


def audit_roi_coverage(
    manifest: dict[str, Any],
    image_root: Path,
    locate_root: Optional[Path],
) -> dict[str, Any]:
    """Report key-field ROI coverage by source image and split.

    ROI metrics are image-level metrics. Counting only ROI rows can hide images
    that had no detectable key field, so those images remain visible in the
    audit and can block a candidate when coverage is too low.
    """
    locate_root = locate_root or image_root.parent / "locate_json"
    result: dict[str, dict[str, int]] = {
        split: {"images": 0, "covered_images": 0}
        for split in ("train", "validation", "test")
    }
    uncovered: dict[str, list[str]] = {split: [] for split in result}
    for row in manifest.get("entries", []):
        if row.get("is_derived"):
            continue
        split = str(row.get("split") or "")
        if split not in result:
            continue
        image_path = image_root / str(row.get("path") or "")
        if not image_path.is_file():
            continue
        result[split]["images"] += 1
        sidecar = resolve_roi_sidecar(image_path, manifest, row, locate_root)
        covered = False
        if sidecar is not None and sidecar.is_file():
            try:
                payload = json.loads(sidecar.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = {}
            for region in payload.get("key_regions", []) if isinstance(payload, dict) else []:
                if not isinstance(region, dict):
                    continue
                if normalize_field_type(region.get("type") or region.get("field_type")):
                    covered = True
                    break
        if covered:
            result[split]["covered_images"] += 1
        else:
            uncovered[split].append(str(row.get("path") or ""))

    for split, counts in result.items():
        counts["coverage_percent"] = round(
            100.0 * counts["covered_images"] / max(counts["images"], 1), 2
        )
    return {"by_split": result, "uncovered_paths": uncovered}


def load_roi_records(
    image_root: Path,
    manifest: dict[str, Any],
    locate_root: Optional[Path] = None,
) -> list[ROIRecord]:
    records: list[ROIRecord] = []
    locate_root = locate_root or image_root.parent / "locate_json"
    rows_by_path = {str(row.get("path")): row for row in manifest.get("entries", [])}
    for row in manifest.get("entries", []):
        if row.get("is_derived") or str(row.get("split")) not in {"train", "validation", "test"}:
            continue
        image_path = image_root / str(row.get("path") or "")
        if not image_path.is_file():
            continue
        source = str(row.get("source") or "base")
        if int(row.get("label", 0)) == 1 and source != "reviewed" and not row.get("training_replay_regression"):
            if str(row.get("split")) == "train":
                continue
            source = "legacy_weak_holdout"
        locate_path = resolve_roi_sidecar(image_path, manifest, row, locate_root)
        if locate_path is None:
            continue
        try:
            data = json.loads(locate_path.read_text(encoding="utf-8"))
            image = cv2.imdecode(np.fromfile(str(image_path), dtype=np.uint8), cv2.IMREAD_COLOR)
        except (OSError, json.JSONDecodeError):
            continue
        if image is None:
            continue
        dedup: set[tuple[tuple[int, int, int, int], str]] = set()
        raw_ocr_tokens = data.get("ocr_tokens") or data.get("key_regions") or []
        ocr_tokens = tuple(
            {
                "text": str(item.get("text") or ""),
                "confidence": float(item.get("confidence", item.get("conf", 0.0)) or 0.0),
                "bbox": [int(value) for value in (item.get("bbox") or [])[:4]],
            }
            for item in raw_ocr_tokens
            if isinstance(item, dict) and len(item.get("bbox") or []) >= 4
        )
        for token in data.get("key_regions", []):
            field_type = normalize_field_type(token.get("type") or token.get("field_type"))
            if field_type is None:
                continue
            try:
                bbox = normalize_bbox(token.get("bbox") or [], image.shape[1], image.shape[0])
            except (V4DataError, TypeError, ValueError):
                continue
            key = (bbox, field_type)
            if key in dedup:
                continue
            dedup.add(key)
            records.append(
                ROIRecord(
                    image_path=image_path,
                    bbox=bbox,
                    field_type=field_type,
                    label=int(row.get("label", 0)),
                    group_id=str(row.get("group_id") or image_path.name),
                    split=str(row.get("split") or "train"),
                    source=source,
                    parent_group_id=str(row.get("parent_group_id") or row.get("group_id") or image_path.name),
                    ocr_tokens=ocr_tokens,
                )
            )
    return records


def load_reviewed_roi_records(reviewed_root: Optional[Path]) -> list[ROIRecord]:
    if reviewed_root is None or not reviewed_root.is_dir():
        return []
    records: list[ROIRecord] = []
    for label, directory_name in ((0, "normal"), (1, "tampered")):
        directory = reviewed_root / directory_name
        for metadata_path in sorted(directory.glob("*.json")):
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if int(metadata.get("label", label)) != label:
                continue
            image_path = directory / Path(str(metadata.get("storage_filename") or "")).name
            image = cv2.imdecode(np.fromfile(str(image_path), dtype=np.uint8), cv2.IMREAD_COLOR)
            if image is None:
                continue
            for region in metadata.get("regions") or []:
                field_type = normalize_field_type(region.get("field_type")) if isinstance(region, dict) else None
                if field_type is None:
                    continue
                try:
                    bbox = normalize_bbox(
                        (float(region["x1"]) * image.shape[1], float(region["y1"]) * image.shape[0],
                         float(region["x2"]) * image.shape[1], float(region["y2"]) * image.shape[0]),
                        image.shape[1], image.shape[0],
                    )
                except (KeyError, TypeError, ValueError, V4DataError):
                    continue
                sample_id = str(metadata.get("sample_id") or metadata_path.stem)
                records.append(ROIRecord(
                    image_path=image_path, bbox=bbox, field_type=field_type, label=label,
                    group_id=f"reviewed:{sample_id}", split="train", source="reviewed",
                    parent_group_id=f"reviewed:{sample_id}",
                    ocr_tokens=(),
                ))
    return records


def _ocr_fallback_records(
    image_root: Path,
    manifest: dict[str, Any],
    locate_root: Path,
) -> list[ROIRecord]:
    """Create auditable replay ROIs for production tampered samples without filename rules."""
    try:
        import easyocr
    except ImportError:
        return []
    reader = easyocr.Reader(["ch_sim", "en"], gpu=False, verbose=False)
    records: list[ROIRecord] = []
    for row in manifest.get("entries", []):
        if not row.get("training_replay_regression") or row.get("is_derived"):
            continue
        image_path = image_root / str(row.get("path") or "")
        image = cv2.imdecode(np.fromfile(str(image_path), dtype=np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            continue
        if resolve_roi_sidecar(image_path, manifest, row, locate_root) is not None:
            continue
        scale = min(1.0, 1600.0 / max(image.shape[:2]))
        work = cv2.resize(image, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA) if scale < 1.0 else image
        tokens = reader.readtext(cv2.cvtColor(work, cv2.COLOR_BGR2RGB), detail=1, paragraph=False)
        ocr_tokens = tuple(
            {
                "text": str(text or ""),
                "confidence": float(confidence or 0.0),
                "bbox": [
                    int(round(min(point[0] for point in box) / scale)),
                    int(round(min(point[1] for point in box) / scale)),
                    int(round(max(point[0] for point in box) / scale)),
                    int(round(max(point[1] for point in box) / scale)),
                ],
            }
            for box, text, confidence in tokens
            if len(box) >= 4
        )
        seen: set[tuple[tuple[int, int, int, int], str]] = set()
        for box, text, confidence in tokens:
            text_value = str(text or "")
            field_type = normalize_field_type(text_value)
            if field_type is None:
                if any(char.isdigit() for char in text_value) and any(mark in text_value for mark in (".", ":", "/", "元")):
                    field_type = "amount" if any(mark in text_value for mark in (".", "元")) else "time"
                elif 2 <= len(text_value) <= 4 and any("\u4e00" <= char <= "\u9fff" for char in text_value) and not any(char.isdigit() for char in text_value):
                    field_type = "name"
            if field_type is None:
                continue
            xs = [point[0] for point in box]
            ys = [point[1] for point in box]
            bbox = normalize_bbox((min(xs) / scale, min(ys) / scale, max(xs) / scale, max(ys) / scale), image.shape[1], image.shape[0])
            key = (bbox, field_type)
            if key in seen:
                continue
            seen.add(key)
            records.append(ROIRecord(
                image_path=image_path,
                bbox=bbox,
                field_type=field_type,
                label=int(row.get("label", 1)),
                group_id=str(row.get("group_id") or image_path.name),
                split=str(row.get("split") or "train"),
                source=("production_tampered_ocr_replay" if row.get("training_replay_regression") else "ocr_generated"),
                parent_group_id=str(row.get("parent_group_id") or row.get("group_id") or image_path.name),
                ocr_tokens=ocr_tokens,
            ))
    return records


if torch is not None:
    class _ForensicDataset(Dataset):
        def __init__(self, rows: Sequence[ForensicExample], extractor: ForensicFeatureExtractorV4):
            self.rows = list(rows)
            self.extractor = extractor

        def __len__(self) -> int:
            return len(self.rows)

        def __getitem__(self, index: int):
            row = self.rows[index]
            rgb, srm, scalar = self.extractor.extract(row.image, row.bbox, row.ocr_tokens)
            return (
                torch.from_numpy(rgb).permute(2, 0, 1).float() / 255.0,
                torch.from_numpy(srm).float(),
                torch.from_numpy(scalar).float(),
                torch.tensor(float(row.label), dtype=torch.float32),
            )


    class ForensicFusionModel(nn.Module):
        def __init__(self, scalar_dim: int = len(SCALAR_FEATURE_NAMES)):
            super().__init__()
            backbone = tv_models.mobilenet_v3_small(weights=None)
            self.rgb = backbone.features
            self.rgb_pool = nn.AdaptiveAvgPool2d(1)
            self.srm = nn.Conv2d(1, 16, 5, padding=2, bias=False)
            kernels = torch.zeros((16, 1, 5, 5), dtype=torch.float32)
            for index in range(16):
                offset_x, offset_y = index % 4 - 1, index // 4 - 1
                kernels[index, 0, 2, 2] = 1.0
                kernels[index, 0, 2 + offset_y, 2 + offset_x] = -1.0
            with torch.no_grad():
                self.srm.weight.copy_(kernels)
            for parameter in self.srm.parameters():
                parameter.requires_grad = False
            self.srm_pool = nn.AdaptiveAvgPool2d(1)
            self.scalar = nn.Sequential(
                nn.Linear(scalar_dim, 32),
                nn.LayerNorm(32),
                nn.ReLU(inplace=True),
                nn.Dropout(0.10),
            )
            self.head = nn.Sequential(
                nn.Linear(576 + 16 + 32, 64),
                nn.ReLU(inplace=True),
                nn.Dropout(0.15),
                nn.Linear(64, 1),
            )

        def forward(self, rgb, srm, scalar):
            rgb_features = self.rgb_pool(self.rgb(rgb)).flatten(1)
            srm_features = self.srm_pool(self.srm(srm)).flatten(1)
            scalar_features = self.scalar(scalar)
            return self.head(torch.cat((rgb_features, srm_features, scalar_features), dim=1)).squeeze(1)


class V4Predictor:
    def __init__(self, model_path: str | Path, device: str = "cpu"):
        if torch is None:
            raise V4DataError("缺少 torch，无法加载 v4")
        self.device = torch.device(device)
        payload = torch.load(model_path, map_location=self.device)
        self.model = ForensicFusionModel()
        self.model.load_state_dict(payload["state_dict"])
        self.model.eval()
        self.extractor = ForensicFeatureExtractorV4()
        self.low_threshold = float(payload.get("low_threshold", 0.35))
        self.high_threshold = float(payload.get("high_threshold", 0.65))

    def predict_roi(
        self,
        image: np.ndarray,
        bbox: Sequence[int],
        ocr_tokens: Optional[Sequence[dict[str, Any]]] = None,
    ) -> float:
        rgb, srm, scalar = self.extractor.extract(image, bbox, ocr_tokens)
        with torch.no_grad():
            logit = self.model(
                torch.from_numpy(rgb).permute(2, 0, 1).float().div(255).unsqueeze(0).to(self.device),
                torch.from_numpy(srm).unsqueeze(0).to(self.device),
                torch.from_numpy(scalar).unsqueeze(0).to(self.device),
            )
            return float(torch.sigmoid(logit)[0].cpu())

    def predict_rois(self, image: np.ndarray, rois: Sequence[dict[str, Any]]) -> list[float]:
        """Run all ROIs from one image in one model call."""
        if not rois:
            return []
        prepared = [self.extractor.extract(image, roi["bbox"], roi.get("ocr_tokens")) for roi in rois]
        rgb = torch.from_numpy(np.stack([item[0] for item in prepared])).permute(0, 3, 1, 2).float().div(255)
        srm = torch.from_numpy(np.stack([item[1] for item in prepared])).float()
        scalar = torch.from_numpy(np.stack([item[2] for item in prepared])).float()
        with torch.no_grad():
            logits = self.model(rgb.to(self.device), srm.to(self.device), scalar.to(self.device))
        return [float(value) for value in torch.sigmoid(logits).cpu().numpy()]

    def predict(self, image: np.ndarray, rois: Sequence[dict[str, Any]]) -> dict[str, Any]:
        if not rois:
            return {"result": "无法自动检测", "reason": "未识别到金额、姓名、时间关键区域，无法自动检测", "regions": []}
        regions = []
        scores = self.predict_rois(image, rois)
        for index, (roi, score) in enumerate(zip(rois, scores), start=1):
            regions.append({**roi, "region_no": index, "tampered_probability": round(score, 6)})
        scores = [float(row["tampered_probability"]) for row in regions]
        quality_score = estimate_quality_score(image)
        result, confidence = classify_v4_result(
            scores,
            low_threshold=self.low_threshold,
            high_threshold=self.high_threshold,
            quality_score=quality_score,
            template_coverage=float(rois[0].get("template_coverage", 1.0) or 0.0),
        )
        return {
            "result": result,
            "confidence": confidence,
            "regions": regions,
            "quality_score": quality_score,
            "reason": "v4 局部取证融合结果",
        }


class ForensicTrainerV4:
    def __init__(self, output_dir: str | Path, seed: int = 42, device: str = "cpu"):
        if torch is None:
            raise V4DataError("缺少 torch/torchvision，无法训练 v4")
        self.output_dir = Path(output_dir)
        self.seed = int(seed)
        self.device = torch.device(device)
        self.extractor = ForensicFeatureExtractorV4()
        random.seed(self.seed)
        np.random.seed(self.seed)
        torch.manual_seed(self.seed)

    @staticmethod
    def _split(rows: Sequence[ForensicExample], split: str) -> list[ForensicExample]:
        return [row for row in rows if row.split == split]

    @staticmethod
    def _write_artifact_manifest(output_dir: Path) -> Path:
        artifacts = []
        for path in sorted(output_dir.iterdir()):
            if not path.is_file() or path.name == "artifact_manifest.json":
                continue
            artifacts.append({
                "filename": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            })
        manifest_path = output_dir / "artifact_manifest.json"
        manifest_path.write_text(
            json.dumps({"schema_version": 1, "artifacts": artifacts}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return manifest_path

    def _thresholds(self, scores: np.ndarray, labels: np.ndarray) -> tuple[float, float]:
        normal = scores[labels == 0]
        tampered = scores[labels == 1]
        if len(normal) == 0 or len(tampered) == 0:
            return 0.35, 0.65
        candidates = np.unique(np.concatenate((normal, tampered, [0.35, 0.50, 0.65])))
        best_low, best_high = 0.35, 0.65
        for low in candidates:
            fpr = float(np.mean(normal >= low))
            recall = float(np.mean(tampered >= low))
            if fpr <= 0.10 and recall >= 0.90:
                best_low = float(low)
                break
        high_candidates = candidates[candidates > best_low]
        for high in high_candidates:
            recall = float(np.mean(tampered >= high))
            fpr = float(np.mean(normal >= high))
            if recall >= 0.90 and fpr <= 0.10:
                best_high = float(high)
                break
        return best_low, best_high

    def fit(self, rows: Sequence[ForensicExample], manifest_audit: dict[str, Any]) -> dict[str, Any]:
        if not rows:
            raise V4DataError("没有 v4 ROI 样本")
        train_rows = self._split(rows, "train")
        validation_rows = self._split(rows, "validation")
        test_rows = self._split(rows, "test")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        audit = dict(manifest_audit)
        example_manifest = audit.pop("example_manifest", [])
        example_manifest_path = self.output_dir / "example_manifest.json"
        example_manifest_path.write_text(json.dumps(example_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        audit["example_manifest_path"] = str(example_manifest_path)
        if not train_rows:
            raise V4DataError("v4 必须存在 train ROI")
        gate_reasons = []
        for split_name, split_rows in (("validation", validation_rows), ("test", test_rows)):
            labels = {int(row.label) for row in split_rows}
            if labels != {0, 1}:
                gate_reasons.append(f"{split_name} ROI 必须同时包含正常和篡改，当前标签={sorted(labels)}")
            coverage = (manifest_audit.get("image_roi_coverage", {}).get("by_split", {}).get(split_name, {}))
            if float(coverage.get("coverage_percent", 0.0)) < 90.0:
                gate_reasons.append(
                    f"{split_name} 关键 ROI 图片覆盖率必须达到 90%，当前={coverage.get('coverage_percent', 0.0)}%"
                )
        if manifest_audit.get("replay_without_roi"):
            gate_reasons.append("pptest 回放存在未识别关键 ROI")
        example_group_audit = manifest_audit.get("example_group_audit") or {}
        if example_group_audit and not example_group_audit.get("passed", False):
            gate_reasons.extend(example_group_audit.get("errors") or ["派生样本 parent group 审计失败"])
        if gate_reasons:
            report = {
                "status": "blocked",
                "candidate_only": True,
                "active_model_unchanged": True,
                "reason": "数据覆盖不足，禁止生成可用 v4 候选模型",
                "gate_reasons": gate_reasons,
                "candidate_gate": {
                    "passed": False,
                    "minimum_metric": 0.90,
                    "reasons": gate_reasons,
                },
                "audit": audit,
                "training_replay_regression": {
                    "sample_count": int(sum(row.source.startswith("production_tampered") for row in rows)),
                    "all_tampered": False,
                    "not_evaluated": True,
                },
            }
            (self.output_dir / "evaluation_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            self._write_artifact_manifest(self.output_dir)
            return report
        model = ForensicFusionModel().to(self.device)
        for parameter in model.rgb.parameters():
            parameter.requires_grad = False
        positive = max(1, sum(row.label for row in train_rows))
        negative = max(1, len(train_rows) - positive)
        loss = nn.BCEWithLogitsLoss(pos_weight=torch.tensor(negative / positive, device=self.device))
        optimizer = torch.optim.AdamW(
            [parameter for parameter in model.parameters() if parameter.requires_grad],
            lr=1e-3,
            weight_decay=1e-4,
        )
        loader = DataLoader(_ForensicDataset(train_rows, self.extractor), batch_size=8, shuffle=True, num_workers=0)
        for _epoch in range(8):
            model.train()
            for rgb, srm, scalar, target in loader:
                optimizer.zero_grad(set_to_none=True)
                logits = model(rgb.to(self.device), srm.to(self.device), scalar.to(self.device))
                loss(logits, target.to(self.device)).backward()
                optimizer.step()
        for parameter in model.rgb.parameters():
            parameter.requires_grad = True
        optimizer = torch.optim.AdamW(model.parameters(), lr=1e-4, weight_decay=2e-4)
        for _epoch in range(12):
            model.train()
            for rgb, srm, scalar, target in loader:
                optimizer.zero_grad(set_to_none=True)
                logits = model(rgb.to(self.device), srm.to(self.device), scalar.to(self.device))
                loss(logits, target.to(self.device)).backward()
                optimizer.step()

        def score_rows(items: Sequence[ForensicExample]) -> tuple[np.ndarray, np.ndarray]:
            model.eval()
            scores, labels = [], []
            with torch.no_grad():
                for row in items:
                    rgb, srm, scalar = self.extractor.extract(row.image, row.bbox, row.ocr_tokens)
                    logits = model(
                        torch.from_numpy(rgb).permute(2, 0, 1).float().div(255).unsqueeze(0).to(self.device),
                        torch.from_numpy(srm).unsqueeze(0).to(self.device),
                        torch.from_numpy(scalar).unsqueeze(0).to(self.device),
                    )
                    scores.append(float(torch.sigmoid(logits)[0].cpu()))
                    labels.append(row.label)
            return np.asarray(scores), np.asarray(labels)

        validation_scores, validation_labels = score_rows(validation_rows)
        low, high = self._thresholds(validation_scores, validation_labels)
        model_path = self.output_dir / "forensic_v4.pt"
        torch.save(
            {
                "state_dict": model.state_dict(),
                "low_threshold": low,
                "high_threshold": high,
                "scalar_features": list(SCALAR_FEATURE_NAMES),
                "context_ratio": self.extractor.context_ratio,
                "audit": audit,
            },
            model_path,
        )
        onnx_path = self.output_dir / "forensic_v4.onnx"
        onnx_status = "not_attempted"
        if hasattr(torch, "onnx"):
            try:
                model.eval()
                dummy = (
                    torch.zeros(1, 3, 224, 224, device=self.device),
                    torch.zeros(1, 1, 224, 224, device=self.device),
                    torch.zeros(1, len(SCALAR_FEATURE_NAMES), device=self.device),
                )
                torch.onnx.export(model, dummy, onnx_path, opset_version=17)
                onnx_status = "exported"
            except Exception as exc:  # missing onnx package is expected on minimal workstations
                onnx_status = "failed: " + type(exc).__name__ + ": " + str(exc)
        replay_rows = [row for row in rows if row.source.startswith("production_tampered")]
        test_scores, test_labels = score_rows(test_rows) if test_rows else (np.array([]), np.array([]))
        replay_scores, replay_labels = score_rows(replay_rows) if replay_rows else (np.array([]), np.array([]))
        report = self._report(validation_scores, validation_labels, low, high, audit)
        report["test"] = self._metric_block(test_scores, test_labels, low, high)
        report["training_replay_regression"] = {
            "sample_count": int(len(replay_labels)),
            "all_tampered": bool(len(replay_labels) and np.all(replay_scores >= high)),
            "items": [
                {"path": str(row.image_path), "field_type": row.field_type, "probability": round(float(score), 6),
                 "result": "篡改" if score >= high else "可疑" if score >= low else "正常"}
                for row, score in zip(replay_rows, replay_scores)
            ],
        }
        metric_blocks = (report["validation"], report["test"])
        gate_reasons = []
        for split_name, metrics in (("validation", metric_blocks[0]), ("test", metric_blocks[1])):
            if not metrics.get("available"):
                gate_reasons.append(f"{split_name} 独立局部 ROI 指标不可用")
                continue
            for metric_name in ("balanced_accuracy", "normal_recall", "tampered_recall"):
                value = metrics.get(metric_name)
                if value is None or float(value) < 0.90:
                    gate_reasons.append(f"{split_name}.{metric_name} 未达到 90%")
        if not report["training_replay_regression"]["all_tampered"]:
            gate_reasons.append("pptest 训练回放未全部判为篡改")
        report["candidate_gate"] = {
            "passed": not gate_reasons,
            "minimum_metric": 0.90,
            "reasons": gate_reasons,
        }
        if gate_reasons:
            report["status"] = "blocked"
            report["reason"] = "独立验证门槛未通过，禁止启用 v4 候选模型"
        report["model_path"] = str(model_path)
        report["onnx_path"] = str(onnx_path) if onnx_path.exists() else None
        report["onnx_status"] = onnx_status
        report_path = self.output_dir / "evaluation_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        self._write_artifact_manifest(self.output_dir)
        return report

    @staticmethod
    def _metric_block(scores: np.ndarray, labels: np.ndarray, low: float, high: float) -> dict[str, Any]:
        if not len(labels):
            return {"available": False, "sample_count": 0}
        predicted = np.where(scores >= high, 1, np.where(scores >= low, -1, 0))
        normal = labels == 0
        tampered = labels == 1
        normal_recall = float(np.mean(predicted[normal] == 0)) if normal.any() else None
        tampered_recall = float(np.mean(predicted[tampered] == 1)) if tampered.any() else None
        recalls = [value for value in (normal_recall, tampered_recall) if value is not None]
        return {
            "available": True,
            "sample_count": int(len(labels)),
            "balanced_accuracy": float(np.mean(recalls)) if len(recalls) == 2 else None,
            "normal_recall": normal_recall,
            "tampered_recall": tampered_recall,
            "false_positive_rate": (
                float(np.mean(predicted[normal] == 1)) if normal.any() else None
            ),
            "suspicious_rate": float(np.mean(predicted == -1)),
            "normal_as_tampered": int(np.sum(normal & (predicted == 1))),
            "tampered_as_normal": int(np.sum(tampered & (predicted == 0))),
        }

    @staticmethod
    def _report(scores: np.ndarray, labels: np.ndarray, low: float, high: float, audit: dict[str, Any]) -> dict[str, Any]:
        predicted = np.where(scores >= high, 1, np.where(scores >= low, -1, 0))
        known = predicted >= 0
        normal = labels == 0
        tampered = labels == 1
        return {
            "status": "completed",
            "candidate_only": True,
            "active_model_unchanged": True,
            "audit": audit,
            "thresholds": {"low_suspicious": low, "high_tampered": high},
            "validation": ForensicTrainerV4._metric_block(scores, labels, low, high),
        }


def build_examples(
    image_root: Path,
    manifest: dict[str, Any],
    locate_root: Optional[Path] = None,
    synthetic_per_roi: int = 4,
    reviewed_root: Optional[Path] = None,
) -> tuple[list[ForensicExample], dict[str, Any]]:
    records = load_roi_records(image_root, manifest, locate_root)
    records.extend(load_reviewed_roi_records(reviewed_root))
    existing_replay = {record.image_path for record in records}
    records.extend(
        record for record in _ocr_fallback_records(image_root, manifest, locate_root or image_root.parent / "locate_json")
        if record.image_path not in existing_replay
    )
    generator = SyntheticTamperGenerator()
    examples: list[ForensicExample] = []
    weak_holdout_records = [record for record in records if record.source == "legacy_weak_holdout"]
    for record in records:
        image = cv2.imdecode(np.fromfile(str(record.image_path), dtype=np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            continue
        token_rows: list[dict[str, Any]] = []
        if record.split == "train" and record.label == 1 and not record.source.startswith("production_tampered") and record.source != "reviewed":
            continue
        if record.source == "legacy_weak_holdout":
            continue
        examples.append(ForensicExample(
            image_path=record.image_path,
            image=image,
            bbox=record.bbox,
            label=record.label,
            group_id=record.group_id,
            split=record.split,
            source=record.source,
            field_type=record.field_type,
            transform={"name": "original"},
            parent_group_id=record.parent_group_id,
            ocr_tokens=record.ocr_tokens,
        ))
        if record.split == "train" and record.label == 0:
            encoded_ok, encoded = cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, 55])
            normal_variants = [("normal_reencode", cv2.imdecode(encoded, cv2.IMREAD_COLOR) if encoded_ok else None),
                               ("normal_blur", cv2.GaussianBlur(image, (3, 3), 0))]
            for variant_name, normal_image in normal_variants:
                if normal_image is None:
                    continue
                examples.append(ForensicExample(
                    image_path=record.image_path,
                    image=normal_image,
                    bbox=record.bbox,
                    label=0,
                    group_id=record.group_id,
                    split="train",
                    source="normal_hard_negative",
                    field_type=record.field_type,
                    transform={"name": variant_name, "parent_group_id": record.parent_group_id},
                    parent_group_id=record.parent_group_id,
                    ocr_tokens=record.ocr_tokens,
                ))
        if record.split == "train" and record.label == 0:
            for variant in range(synthetic_per_roi):
                altered, transform = generator.generate(
                    image,
                    record.bbox,
                    group_id=record.group_id,
                    field_type=record.field_type,
                    variant=variant,
                )
                examples.append(ForensicExample(
                    image_path=record.image_path,
                    image=altered,
                    bbox=record.bbox,
                    label=1,
                    group_id=record.group_id + ":synthetic",
                    split="train",
                    source="synthetic_local_edit",
                    field_type=record.field_type,
                    transform=transform,
                    parent_group_id=record.parent_group_id,
                    ocr_tokens=record.ocr_tokens,
                ))
    audit = audit_manifest(manifest, image_root)
    audit["image_roi_coverage"] = audit_roi_coverage(manifest, image_root, locate_root)
    audit["roi_record_count"] = len(records)
    audit["example_count"] = len(examples)
    audit["synthetic_positive_count"] = sum(row.label == 1 for row in examples)
    audit["replay_paths"] = [
        str(image_root / str(row.get("path")))
        for row in manifest.get("entries", [])
        if row.get("training_replay_regression") and not row.get("is_derived")
    ]
    audit["replay_roi_count"] = sum(row.source.startswith("production_tampered") for row in records)
    audit["weak_holdout_roi_count"] = len(weak_holdout_records)
    audit["weak_holdout_paths"] = sorted({str(row.image_path) for row in weak_holdout_records})
    audit["replay_without_roi"] = [
        str(image_root / str(row.get("path")))
        for row in manifest.get("entries", [])
        if row.get("training_replay_regression")
        and not row.get("is_derived")
        and not any(record.image_path == image_root / str(row.get("path")) for record in records)
    ]
    audit["roi_split_label_counts"] = {
        split: {str(label): sum(row.split == split and row.label == label for row in records) for label in (0, 1)}
        for split in ("train", "validation", "test")
    }
    audit["training_roi_split_label_counts"] = {
        split: {str(label): sum(row.split == split and row.label == label for row in examples) for label in (0, 1)}
        for split in ("train", "validation", "test")
    }
    audit["roi_coverage"] = {split: sum(row.split == split for row in records) for split in ("train", "validation", "test")}
    audit["example_manifest"] = [
        {
            "image_path": str(row.image_path),
            "bbox": list(row.bbox),
            "label": row.label,
            "group_id": row.group_id,
            "parent_group_id": row.parent_group_id,
            "split": row.split,
            "source": row.source,
            "field_type": row.field_type,
            "is_derived": row.transform.get("name") != "original",
            "transform": row.transform,
        }
        for row in examples
    ]
    audit["example_group_audit"] = audit_example_groups(examples)
    return examples, audit


def run_offline_v4(
    image_root: str | Path,
    manifest_path: str | Path,
    output_dir: str | Path,
    locate_root: Optional[str | Path] = None,
    reviewed_root: Optional[str | Path] = None,
) -> dict[str, Any]:
    root = Path(image_root)
    manifest = load_manifest(Path(manifest_path))
    rows, audit = build_examples(root, manifest, Path(locate_root) if locate_root else None,
                                 reviewed_root=Path(reviewed_root) if reviewed_root else None)
    if not audit["passed"]:
        raise V4DataError("数据审计失败: " + "; ".join(audit["errors"][:10]))
    return ForensicTrainerV4(output_dir).fit(rows, audit)
