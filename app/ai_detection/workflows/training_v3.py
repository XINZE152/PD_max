"""
训练管线 — 支持反馈数据增强、模型版本化、可视化。
使用 /api/v3/train 端点触发，训练前有风险提示。
"""

import json
import logging
import os
import shutil
import hashlib
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import cv2
import joblib
import numpy as np
import re
import xgboost as xgb
import yaml
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import balanced_accuracy_score, confusion_matrix, recall_score
from PIL import Image

from app.ai_detection.core.augmentations import build_global_augmentations, build_roi_augmentations
from app.ai_detection.core.detectors import OriginalityChecker
from app.ai_detection.core.extractors import FeatureExtractor, FontFeatureLibrary
from app.ai_detection.core.known_source_matcher import image_phash_hex
from app.ai_detection.services.model_registry import ModelRegistry
from app.ai_detection.core.ocr_utils import _resize_for_ocr
from app.ai_detection.runtime.paths import legacy_annotation_dir, resolve_config_path

logger = logging.getLogger(__name__)


def write_version_manifest(
    version_dir: Path,
    *,
    version: str,
    feature_config_version: str,
) -> Path:
    """Refresh the checksum manifest after every version artifact has been written."""
    artifacts = []
    for path in sorted(version_dir.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        artifacts.append(
            {
                "filename": str(path.relative_to(version_dir).as_posix()),
                "size_bytes": path.stat().st_size,
                "sha256": TrainPipeline._sha256(path),
            }
        )
    manifest = {
        "version": version,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "feature_config_version": feature_config_version,
        "artifacts": artifacts,
    }
    path = version_dir / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


class TrainPipeline:
    """端到端训练管线 — 全局模型 + 字体库 + 可视化。"""

    def __init__(self, config_path: str = "config.yaml", ocr_reader: Any = None):
        config_file = resolve_config_path(config_path)
        with open(config_file, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        self.base_dir = config_file.parent

        train_cfg = self.config.get("training", {})
        self.output_dir = Path(self._resolve(train_cfg.get("output_dir", "models/versions")))
        self.viz_enabled = train_cfg.get("visualization_enabled", True)
        self.backup_previous = train_cfg.get("backup_previous", True)
        self.feature_cache_dir = Path(
            self._resolve(train_cfg.get("feature_cache_dir", "models/cache/v3_global"))
        )
        self.feature_config_version = str(train_cfg.get("feature_config_version", "global-v2"))

        self.ocr_reader = ocr_reader
        self.extractor: Optional[FeatureExtractor] = None
        self.font_lib: Optional[FontFeatureLibrary] = None
        self.dataset_manifest: Dict[str, Any] = {}
        self._dataset_split_by_path: Dict[str, str] = {}
        self._test_samples: list[tuple[str, int, str]] = []
        self._training_replay_samples: list[tuple[str, int, str]] = []
        self._source_counts: Dict[str, int] = {}
        self._sample_failures: list[Dict[str, str]] = []

    def _resolve(self, path_str: str) -> str:
        p = Path(path_str)
        if p.is_absolute():
            return str(p)
        return str((self.base_dir / p).resolve())

    def run(
        self,
        feedback_dir: Optional[str] = None,
        progress_callback: Optional[callable] = None,
    ) -> dict:
        """执行完整训练流程，返回训练摘要。"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version_dir = self.output_dir / timestamp
        self.output_dir.mkdir(parents=True, exist_ok=True)
        version_dir.mkdir(parents=True, exist_ok=False)
        reports_dir = version_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        # 备份旧模型
        if self.backup_previous:
            self._backup_models()

        # 初始化提取器
        if self.extractor is None:
            self.extractor = FeatureExtractor(reader=self.ocr_reader)

        reviewed_images = self._load_reviewed_dataset(feedback_dir)
        original_images = self._load_original_dataset()

        all_font_feats = []
        all_font_labels = []
        global_X = []
        global_y = []
        global_groups = []
        synthetic_tampered_samples = 0

        total_samples = len(original_images) + len(reviewed_images)
        processed = 0

        # 处理原始数据集。任何原图特征失败都阻止候选模型生成，避免静默缺样。
        for img_path, label, group_id in original_images:
            try:
                img = cv2.imdecode(np.fromfile(img_path, dtype=np.uint8), cv2.IMREAD_COLOR)
                if img is None:
                    raise ValueError("无法解码图片")
                # 增强只进入训练 split；验证/测试保持原图，避免增强图泄漏。
                split = self._dataset_split_by_path.get(str(Path(img_path).resolve()), "train")
                variants = (
                    self._global_variants(img, key=os.path.basename(img_path))
                    if split == "train"
                    else [("original", img)]
                )
                for aug_name, aug_img in variants:
                    feat = self._extract_cached_global_feature(
                        aug_img,
                        image_path=img_path,
                        variant=aug_name,
                    )
                    global_X.append(feat)
                    global_y.append(label)
                    global_groups.append(group_id)

                # 字体特征：仅正样本（label=0，无篡改）
                if label == 0:
                    self._collect_font_features(img, img_path, all_font_feats, all_font_labels)

                if split == "train" and label == 0:
                    for aug_name, aug_img in self._synthetic_tampered_variants(img, img_path):
                        feat = self._extract_cached_global_feature(
                            aug_img,
                            image_path=img_path,
                            variant=aug_name,
                        )
                        global_X.append(feat)
                        global_y.append(1)
                        global_groups.append(group_id)
                        synthetic_tampered_samples += 1

                processed += 1
                if progress_callback:
                    progress_callback(processed, total_samples, f"处理原始数据: {os.path.basename(img_path)}")
            except Exception as exc:
                self._record_sample_failure(img_path, exc)

        # 只使用显式二审标签；初审 correct/wrong/suspicious 永不参与训练。
        for fb_img_path, label, group_id in reviewed_images:
            try:
                img = cv2.imdecode(np.fromfile(fb_img_path, dtype=np.uint8), cv2.IMREAD_COLOR)
                if img is None:
                    raise ValueError("无法解码图片")
                for aug_name, aug_img in self._global_variants(img, key=os.path.basename(fb_img_path)):
                    feat = self._extract_cached_global_feature(
                        aug_img,
                        image_path=fb_img_path,
                        variant=aug_name,
                    )
                    global_X.append(feat)
                    global_y.append(label)
                    global_groups.append(group_id)

                processed += 1
                if progress_callback:
                    progress_callback(processed, total_samples, f"处理反馈数据: {os.path.basename(fb_img_path)}")
            except Exception as exc:
                self._record_sample_failure(fb_img_path, exc)

        if self._sample_failures:
            return self._failed_training_summary(
                version_dir,
                "训练样本特征提取失败，未生成候选模型",
            )

        if not global_X:
            return {"status": "failed", "reason": "没有可用于训练的样本"}

        # 训练字体库
        font_lib_path = str(version_dir / "font_lib")
        if all_font_feats:
            new_font_lib = FontFeatureLibrary()
            new_font_lib.add(all_font_feats, all_font_labels)
            new_font_lib.save(font_lib_path)
            logger.info("字体库已保存: %s (共 %d 条)", font_lib_path, len(all_font_labels))

        # 训练全局模型（含 Platt 校准 + 早停）
        X = np.array(global_X)
        y = np.array(global_y)
        groups = np.array(global_groups)

        manifest_train_groups = {
            group
            for path, _label, group in original_images
            if self._dataset_split_by_path.get(str(Path(path).resolve())) == "train"
        }
        manifest_validation_groups = {
            group
            for path, _label, group in original_images
            if self._dataset_split_by_path.get(str(Path(path).resolve())) == "validation"
        }
        if manifest_train_groups and manifest_validation_groups:
            train_groups = manifest_train_groups | {
                group for _path, _label, group in reviewed_images
            }
            validation_groups = manifest_validation_groups
        else:
            train_groups, validation_groups = self._deterministic_group_split(
                original_images + reviewed_images,
                validation_ratio=0.20,
            )
        validation_mask = np.isin(groups, list(validation_groups))
        if not validation_mask.any() or validation_mask.all():
            validation_mask = np.zeros(len(X), dtype=bool)
        X_train, y_train = X[~validation_mask], y[~validation_mask]
        X_val, y_val = X[validation_mask], y[validation_mask]

        negative_count = max(1, int(np.sum(y_train == 0)))
        positive_count = max(1, int(np.sum(y_train == 1)))
        scale_pos_weight = negative_count / positive_count

        base_model = xgb.XGBClassifier(
            max_depth=4,
            n_estimators=200,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,
            reg_lambda=1.0,
            eval_metric="logloss",
            random_state=42,
            n_jobs=2,
            scale_pos_weight=scale_pos_weight,
        )

        if X_val is not None and len(X_val) > 0:
            base_model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )
        else:
            base_model.fit(X_train, y_train)

        # Platt scaling 校准：将 XGBoost 原始概率映射到校准概率
        can_calibrate = (
            len(np.unique(y_train)) == 2
            and min(int(np.sum(y_train == 0)), int(np.sum(y_train == 1))) >= 6
        )
        if can_calibrate:
            model = CalibratedClassifierCV(base_model, method="sigmoid", cv=3)
            model.fit(X_train, y_train)
        else:
            model = base_model

        model_path = str(version_dir / "global_layout_model.pkl")
        joblib.dump(model, model_path)
        reference_index_path = self._write_normal_reference_index(version_dir)

        # 可视化
        viz_paths = []
        if self.viz_enabled:
            viz_paths = self._generate_visualizations(model, X, y, reports_dir / "charts", timestamp)

        decision_threshold = self._select_decision_threshold(model, X_val, y_val)
        evaluation = self._evaluation_metrics(model, X_val, y_val, threshold=decision_threshold)
        active_evaluation = self._active_model_evaluation(X_val, y_val)
        summary = {
            "status": "completed",
            "timestamp": timestamp,
            "total_samples": len(global_y),
            "positive_samples": int(sum(global_y)),
            "negative_samples": int(len(global_y) - sum(global_y)),
            "font_library_size": len(all_font_labels),
            "reviewed_training_samples": len(reviewed_images),
            "validation_groups": sorted(validation_groups),
            "training_groups": sorted(train_groups),
            "calibrated": can_calibrate,
            "global_fake_threshold": decision_threshold,
            "evaluation": evaluation,
            "active_evaluation": active_evaluation,
            "model_path": model_path,
            "font_lib_path": font_lib_path,
            "reference_index_path": str(reference_index_path) if reference_index_path else None,
            "visualizations": viz_paths,
            "train_accuracy": float(model.score(X_train, y_train)),
            "test_evaluation": self._evaluate_test_samples(model, threshold=decision_threshold),
            "training_replay_evaluation": self._evaluate_training_replay_samples(
                model,
                threshold=decision_threshold,
            ),
            "source_counts": self._source_counts,
            "synthetic_tampered_samples": synthetic_tampered_samples,
            "sample_failures": self._sample_failures,
        }

        # 保存训练摘要
        report_path = reports_dir / "summary.json"
        summary["report_path"] = str(report_path)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        self._write_training_result_package(reports_dir, summary)

        summary["manifest_path"] = str(self._write_manifest(version_dir, summary))
        candidate = self._register_candidate(summary)
        summary["candidate"] = candidate

        logger.info("训练完成: %s", json.dumps(summary, ensure_ascii=False))
        return summary

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _write_manifest(self, version_dir: Path, summary: Dict[str, Any]) -> Path:
        return write_version_manifest(
            version_dir,
            version=str(summary["timestamp"]),
            feature_config_version=self.feature_config_version,
        )

    def _write_normal_reference_index(self, version_dir: Path) -> Optional[Path]:
        """Index known normal layouts for legacy source-delta analysis only."""
        dataset_cfg = self.config.get("dataset", {})
        image_dir = Path(self._resolve(dataset_cfg.get("image_dir", "images")))
        references = []
        for row in self.dataset_manifest.get("entries", []):
            if (
                int(row.get("label", -1)) != 0
                or row.get("split") != "train"
                or bool(row.get("is_derived"))
            ):
                continue
            relative_path = str(row.get("path") or "")
            image = cv2.imdecode(
                np.fromfile(image_dir / relative_path, dtype=np.uint8),
                cv2.IMREAD_COLOR,
            )
            if image is None:
                continue
            height, width = image.shape[:2]
            references.append(
                {
                    "path": relative_path,
                    "width": int(width),
                    "height": int(height),
                    "phash": image_phash_hex(image),
                }
            )
        if not references:
            return None
        path = version_dir / "normal_reference_index.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "references": references,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return path

    def _register_candidate(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Register a versioned artifact without changing the active model."""
        registry_path = Path(self._resolve(
            self.config.get("training", {}).get("registry_path", "models/registry.json")
        ))
        fallback = self._resolve(
            self.config.get("paths", {}).get("xgb_model_path", "models/global_layout_model.pkl")
        )
        registry = ModelRegistry(registry_path, fallback_model_path=fallback)
        return registry.register_candidate(
            {
                "version": summary["timestamp"],
                "timestamp": summary["timestamp"],
                "model_path": summary["model_path"],
                "font_lib_path": summary["font_lib_path"],
                "reference_index_path": summary.get("reference_index_path"),
                "train_accuracy": summary["train_accuracy"],
                "total_samples": summary["total_samples"],
                "evaluation": summary["evaluation"],
                "active_evaluation": summary["active_evaluation"],
                "test_evaluation": summary.get("test_evaluation"),
                "training_replay_evaluation": summary.get("training_replay_evaluation"),
                "global_fake_threshold": summary.get("global_fake_threshold"),
                "source_counts": summary.get("source_counts"),
                "manifest_path": summary["manifest_path"],
                "report_path": summary["report_path"],
                "gates": summary.get("gates") or {"passed": False, "pending": True},
            }
        )

    def _backup_models(self):
        """备份旧模型到带时间戳的子目录。"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.output_dir / f"backup_{timestamp}"
        backup_dir.mkdir(parents=True, exist_ok=True)
        for pattern in ["global_layout_model.pkl", "font_lib.index", "font_lib_meta.pkl"]:
            src = self.output_dir / pattern
            if src.exists():
                shutil.copy2(str(src), str(backup_dir / pattern))

    def _record_sample_failure(self, image_path: str, exc: Exception) -> None:
        logger.warning("处理训练样本失败: %s", image_path, exc_info=True)
        self._sample_failures.append({"path": str(image_path), "error": str(exc)})

    def _failed_training_summary(self, version_dir: Path, reason: str) -> Dict[str, Any]:
        summary = {
            "status": "failed",
            "reason": reason,
            "sample_failures": self._sample_failures,
            "source_counts": self._source_counts,
        }
        reports_dir = version_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = reports_dir / "summary.json"
        summary["report_path"] = str(report_path)
        report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary

    def _load_reviewed_dataset(self, feedback_dir: Optional[str]) -> list:
        """Load only samples that carry an explicit second-review truth label."""
        samples = []
        if not feedback_dir:
            feedback_dir = self._resolve(self.config.get("feedback", {}).get("storage_dir", "feedback"))
        fb_root = Path(feedback_dir)
        if not fb_root.exists():
            return samples

        reviewed_root = fb_root if fb_root.name == "reviewed" else fb_root / "reviewed"
        for label, directory_name in ((0, "normal"), (1, "tampered")):
            fb_path = reviewed_root / directory_name
            if not fb_path.exists():
                continue
            for meta_file in fb_path.glob("*.json"):
                try:
                    with open(meta_file, "r", encoding="utf-8") as f:
                        metadata = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                if metadata.get("label") != label:
                    continue
                storage_name = Path(str(metadata.get("storage_filename") or "")).name
                image_path = fb_path / storage_name
                sample_id = str(metadata.get("sample_id") or "").strip()
                if not storage_name or not image_path.is_file() or not sample_id:
                    continue
                samples.append((str(image_path), label, f"reviewed:{sample_id}"))
        return samples

    @staticmethod
    def _feedback_original_path(folder: Path, metadata: Dict[str, Any]) -> Optional[Path]:
        raw = metadata.get("original_image")
        if isinstance(raw, str) and raw.strip():
            p = Path(raw)
            if p.exists():
                return p
            fallback = folder / p.name
            if fallback.exists():
                return fallback
        for candidate in folder.glob("original.*"):
            if candidate.is_file():
                return candidate
        return None

    @staticmethod
    def _base_group_name(path: Path) -> str:
        stem = re.sub(r"_enhanced(?:_\d+)?$", "", path.stem, flags=re.IGNORECASE)
        return f"base:{stem.lower()}"

    def _load_original_dataset(self) -> list:
        """Load canonical samples from the manifest, excluding the fixed test split."""
        dataset_cfg = self.config.get("dataset", {})
        img_dir = Path(self._resolve(dataset_cfg.get("image_dir", "images")))
        samples = []
        self.dataset_manifest = {}
        self._dataset_split_by_path = {}
        self._test_samples = []
        self._training_replay_samples = []
        self._source_counts = {}
        manifest_path = img_dir / "dataset_manifest.json"
        if manifest_path.is_file():
            try:
                self.dataset_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(f"训练集 manifest 无法读取: {manifest_path}") from exc
            for row in self.dataset_manifest.get("entries", []):
                try:
                    label = int(row["label"])
                    split = str(row["split"])
                    group = str(row["group_id"])
                    if bool(row.get("is_derived")) or split == "derived":
                        continue
                    raw_path = Path(str(row["path"]))
                    path = (img_dir / raw_path).resolve()
                    if not path.is_file():
                        path = (self.base_dir / raw_path).resolve()
                    if not path.is_file() or label not in (0, 1):
                        continue
                    record = (str(path), label, group)
                    source = str(row.get("source") or "unknown")
                    self._source_counts[source] = self._source_counts.get(source, 0) + 1
                    if bool(row.get("training_replay_regression")):
                        self._training_replay_samples.append(record)
                    if split == "test":
                        self._test_samples.append(record)
                    elif split in {"train", "validation"} and path.parent.name in {"normal", "tampered"}:
                        self._dataset_split_by_path[str(path)] = split
                        samples.append(record)
                except (KeyError, TypeError, ValueError):
                    continue
            return samples

        if img_dir.exists():
            for path in sorted(img_dir.iterdir()):
                if path.suffix.lower() not in (".jpg", ".jpeg", ".png"):
                    continue
                label = 0 if "no" in path.name.lower() else 1
                samples.append((str(path), label, self._base_group_name(path)))
        return samples

    def _evaluate_test_samples(self, model: Any, *, threshold: float) -> Dict[str, Any]:
        return self._evaluate_samples(model, self._test_samples, threshold=threshold, variant="test-original")

    def _evaluate_training_replay_samples(self, model: Any, *, threshold: float) -> Dict[str, Any]:
        return self._evaluate_samples(
            model,
            self._training_replay_samples,
            threshold=threshold,
            variant="training-replay-original",
        )

    def _evaluate_samples(
        self,
        model: Any,
        samples: list[tuple[str, int, str]],
        *,
        threshold: float,
        variant: str,
    ) -> Dict[str, Any]:
        if not samples or self.extractor is None:
            return {
                "available": False,
                "sample_count": 0,
                "balanced_accuracy": None,
                "normal_recall": None,
                "tampered_recall": None,
                "confusion_matrix": None,
            }
        features = []
        labels = []
        paths = []
        failures = []
        for image_path, label, _group in samples:
            try:
                image = cv2.imdecode(np.fromfile(image_path, dtype=np.uint8), cv2.IMREAD_COLOR)
                if image is None:
                    raise ValueError("无法解码图片")
                features.append(
                    self._extract_cached_global_feature(
                        image,
                        image_path=image_path,
                        variant=variant,
                    )
                )
                labels.append(label)
                paths.append(image_path)
            except Exception as exc:
                logger.warning("测试集图片处理失败: %s", image_path, exc_info=True)
                failures.append({"path": image_path, "error": str(exc)})
        if not features:
            return {
                "available": False,
                "sample_count": 0,
                "balanced_accuracy": None,
                "normal_recall": None,
                "tampered_recall": None,
                "confusion_matrix": None,
            }
        metrics = self._evaluation_metrics(
            model,
            np.asarray(features),
            np.asarray(labels),
            threshold=threshold,
        )
        metrics["sample_count"] = len(paths)
        metrics["paths"] = paths
        metrics["predictions"] = [
            int(value) for value in self._predict_labels(model, np.asarray(features), threshold)
        ]
        metrics["sample_failures"] = failures
        return metrics

    @staticmethod
    def _deterministic_group_split(samples: list, validation_ratio: float = 0.20) -> tuple[set, set]:
        by_label: Dict[int, list[str]] = {0: [], 1: []}
        for _path, label, group in samples:
            if group not in by_label[int(label)]:
                by_label[int(label)].append(group)
        train_groups: set[str] = set()
        validation_groups: set[str] = set()
        for groups in by_label.values():
            ordered = sorted(groups, key=lambda group: hashlib.sha256(group.encode("utf-8")).hexdigest())
            validation_count = 0
            if len(ordered) >= 3:
                validation_count = max(1, int(round(len(ordered) * validation_ratio)))
                validation_count = min(validation_count, len(ordered) - 1)
            validation_groups.update(ordered[:validation_count])
            train_groups.update(ordered[validation_count:])
        return train_groups, validation_groups

    @staticmethod
    def _global_variants(img: np.ndarray, key: str) -> list:
        return [("original", img), *build_global_augmentations(img, key=key)]

    def _synthetic_tampered_variants(self, img: np.ndarray, img_path: str) -> list:
        """Create one train-only localized compression/paste artifact from known key ROIs."""
        json_dir = legacy_annotation_dir(self._resolve(self.config.get("dataset", {}).get("json_dir", "locate_json")))
        json_path = json_dir / f"{Path(img_path).stem}.json"
        if not json_path.is_file():
            return []
        try:
            regions = json.loads(json_path.read_text(encoding="utf-8")).get("key_regions", [])
        except (OSError, json.JSONDecodeError):
            return []
        height, width = img.shape[:2]
        for region in regions:
            if str(region.get("type") or "") not in {"amount", "name", "time"}:
                continue
            bbox = region.get("bbox") or []
            if len(bbox) < 4:
                continue
            x1, y1, x2, y2 = [int(value) for value in bbox[:4]]
            x1, y1 = max(0, x1), max(0, y1)
            x2, y2 = min(width, x2), min(height, y2)
            if x2 - x1 < 12 or y2 - y1 < 8:
                continue
            altered = img.copy()
            patch = altered[y1:y2, x1:x2]
            ok, encoded = cv2.imencode(".jpg", patch, [int(cv2.IMWRITE_JPEG_QUALITY), 58])
            if not ok:
                continue
            pasted = cv2.imdecode(encoded, cv2.IMREAD_COLOR)
            if pasted is None:
                continue
            pasted = cv2.GaussianBlur(pasted, (3, 3), 0)
            altered[y1:y2, x1:x2] = pasted
            return [("synthetic_roi_paste", altered)]
        return []

    @staticmethod
    def _file_sha256(path: str) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _extract_cached_global_feature(self, image: np.ndarray, *, image_path: str, variant: str):
        digest = hashlib.sha256(
            f"{self._file_sha256(image_path)}:{variant}:{self.feature_config_version}".encode("utf-8")
        ).hexdigest()
        self.feature_cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = self.feature_cache_dir / f"{digest}.npy"
        if cache_path.is_file():
            return np.load(cache_path, allow_pickle=False)
        # Match InferenceEngineAPI so the candidate learns the same visual scale
        # that production v3 uses for memory-safe inference.
        inference_image, _scale = _resize_for_ocr(image)
        feature = self.extractor.extract_global_feature(inference_image)
        temp_path = cache_path.with_name(f".{cache_path.name}.tmp")
        with temp_path.open("wb") as stream:
            np.save(stream, feature, allow_pickle=False)
        os.replace(temp_path, cache_path)
        return feature

    @staticmethod
    def _predict_labels(model: Any, features: np.ndarray, threshold: float) -> np.ndarray:
        probabilities = model.predict_proba(features)[:, 1]
        return (probabilities >= float(threshold)).astype(int)

    @classmethod
    def _evaluation_metrics(
        cls,
        model: Any,
        X_val: np.ndarray,
        y_val: np.ndarray,
        *,
        threshold: float = 0.5,
    ) -> Dict[str, Any]:
        if len(X_val) == 0 or len(np.unique(y_val)) < 2:
            return {
                "available": False,
                "balanced_accuracy": None,
                "normal_recall": None,
                "tampered_recall": None,
                "confusion_matrix": None,
            }
        predicted = cls._predict_labels(model, X_val, threshold)
        matrix = confusion_matrix(y_val, predicted, labels=[0, 1])
        return {
            "available": True,
            "balanced_accuracy": float(balanced_accuracy_score(y_val, predicted)),
            "normal_recall": float(recall_score(y_val, predicted, pos_label=0)),
            "tampered_recall": float(recall_score(y_val, predicted, pos_label=1)),
            "confusion_matrix": matrix.astype(int).tolist(),
            "threshold": float(threshold),
        }

    def _select_decision_threshold(self, model: Any, X_val: np.ndarray, y_val: np.ndarray) -> float:
        if len(X_val) == 0 or len(np.unique(y_val)) < 2:
            return float(self.config.get("thresholds", {}).get("global_fake", 0.65))
        probabilities = model.predict_proba(X_val)[:, 1]
        candidates = np.linspace(0.10, 0.90, 81)
        best_threshold = 0.65
        best_score = (-1.0, -1.0, -1.0)
        for threshold in candidates:
            predicted = (probabilities >= threshold).astype(int)
            normal_recall = recall_score(y_val, predicted, pos_label=0, zero_division=0)
            tampered_recall = recall_score(y_val, predicted, pos_label=1, zero_division=0)
            score = (
                min(float(normal_recall), float(tampered_recall)),
                float(balanced_accuracy_score(y_val, predicted)),
                -abs(float(threshold) - 0.65),
            )
            if score > best_score:
                best_score = score
                best_threshold = float(threshold)
        return best_threshold

    def _active_model_evaluation(self, X_val: np.ndarray, y_val: np.ndarray) -> Dict[str, Any]:
        fallback = self._resolve(
            self.config.get("paths", {}).get("xgb_model_path", "models/global_layout_model.pkl")
        )
        registry_path = self._resolve(
            self.config.get("training", {}).get("registry_path", "models/registry.json")
        )
        registry = ModelRegistry(
            registry_path,
            fallback_model_path=fallback,
        )
        active_entry = registry.resolve_active()
        active_path = active_entry.get("model_path")
        active_threshold = float(active_entry.get("global_fake_threshold", 0.65))
        try:
            active_model = joblib.load(str(active_path))
            return self._evaluation_metrics(active_model, X_val, y_val, threshold=active_threshold)
        except Exception as exc:
            logger.warning("无法在本次验证组评估当前活跃模型: %s", exc)
            return {
                "available": False,
                "balanced_accuracy": None,
                "normal_recall": None,
                "tampered_recall": None,
                "confusion_matrix": None,
                "reason": str(exc),
            }

    def _collect_font_features(self, img, img_path, all_feats, all_labels):
        """从正样本图像中采集字体特征。"""
        json_dir = legacy_annotation_dir(self._resolve(self.config.get("dataset", {}).get("json_dir", "locate_json")))
        json_path = json_dir / (Path(img_path).stem + ".json")
        if not json_path.exists():
            return

        with open(json_path, "r", encoding="utf-8") as f:
            regions_data = json.load(f)

        roi_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        seen = set()
        for region in regions_data.get("key_regions", []):
            if region.get("type") != "amount":
                continue
            text = region.get("text", "")
            if len(re.findall(r"\d", str(text))) < 3:
                continue
            bbox = region.get("bbox", [])
            if len(bbox) < 4:
                continue
            x1, y1, x2, y2 = [int(v) for v in bbox[:4]]
            key = (x1, y1, x2, y2, str(text))
            if key in seen:
                continue
            seen.add(key)
            char_img = roi_rgb[y1:y2, x1:x2]
            if char_img.size == 0:
                continue
            augs = build_roi_augmentations(char_img, key=f"{img_path}_{x1}_{y1}")
            for aug_name, aug_img in augs:
                tensor = self.extractor.transform_local(aug_img)
                if self.extractor.device.type == "cuda":
                    tensor = tensor.cpu()
                feat = self.extractor.feature_extractor(tensor.unsqueeze(0).to(self.extractor.device))
                feat_np = feat.detach().view(-1).cpu().numpy()
                all_feats.append(feat_np)
                all_labels.append(str(text)[:20])

    def _write_training_result_package(self, reports_dir: Path, summary: Dict[str, Any]) -> None:
        metrics = {
            "model_internal_validation": summary.get("evaluation"),
            "model_internal_test": summary.get("test_evaluation"),
            "training_replay_global_feature_only": summary.get("training_replay_evaluation"),
            "threshold": summary.get("global_fake_threshold"),
            "source_counts": summary.get("source_counts"),
            "note": "训练内指标仅衡量全图特征模型；候选启用前必须另行运行生产端到端 OCR/ROI 评估。",
        }
        (reports_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
        rows = []
        for name in ("test_evaluation", "training_replay_evaluation"):
            block = summary.get(name) or {}
            for path, prediction in zip(block.get("paths") or [], block.get("predictions") or []):
                rows.append({"cohort": name, "image_path": path, "global_prediction": prediction})
        with (reports_dir / "per_image_results.csv").open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=["cohort", "image_path", "global_prediction"])
            writer.writeheader()
            writer.writerows(rows)
        markdown = ["# v3 Training Result", "", f"- Version: `{summary.get('timestamp')}`", f"- Threshold: `{summary.get('global_fake_threshold')}`", "", "该目录的全图特征指标不替代生产 OCR/ROI 端到端评估。"]
        (reports_dir / "report.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")

    def _generate_visualizations(self, model, X, y, chart_dir: Path, timestamp: str) -> list:
        """生成训练可视化图片并保存。"""
        paths = []
        chart_dir.mkdir(parents=True, exist_ok=True)
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            # 1. 特征重要性图
            fig, ax = plt.subplots(figsize=(10, 6))
            importance = model.feature_importances_
            indices = np.argsort(importance)[-30:]
            ax.barh(range(len(indices)), importance[indices])
            ax.set_yticks(range(len(indices)))
            ax.set_yticklabels([f"dim_{i}" for i in indices], fontsize=7)
            ax.set_xlabel("Importance")
            ax.set_title(f"XGBoost Feature Importance (top 30)\n{timestamp}")
            fig.tight_layout()
            imp_path = str(chart_dir / "feature_importance.png")
            fig.savefig(imp_path, dpi=120)
            plt.close(fig)
            paths.append(imp_path)

            # 2. 训练分数分布
            probs = model.predict_proba(X)[:, 1]
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
            ax1.hist(probs[y == 0], bins=30, alpha=0.6, label="Real (label=0)", color="green")
            ax1.hist(probs[y == 1], bins=30, alpha=0.6, label="Fake (label=1)", color="red")
            ax1.set_xlabel("Predicted Probability")
            ax1.set_ylabel("Count")
            ax1.set_title("Score Distribution by Class")
            ax1.legend()

            y_pred = model.predict(X)
            from sklearn.metrics import confusion_matrix as cm
            cmat = cm(y, y_pred)
            im = ax2.imshow(cmat, cmap="Blues")
            ax2.set_xticks([0, 1])
            ax2.set_xticklabels(["Real", "Fake"])
            ax2.set_yticks([0, 1])
            ax2.set_yticklabels(["Real", "Fake"])
            ax2.set_xlabel("Predicted")
            ax2.set_ylabel("Actual")
            for i in range(2):
                for j in range(2):
                    ax2.text(j, i, str(cmat[i, j]), ha="center", va="center", fontsize=14)
            ax2.set_title("Confusion Matrix")
            plt.colorbar(im, ax=ax2)

            fig.tight_layout()
            dist_path = str(chart_dir / "training_analysis.png")
            fig.savefig(dist_path, dpi=120)
            plt.close(fig)
            paths.append(dist_path)

            # 3. 学习曲线采样图
            fig, ax = plt.subplots(figsize=(8, 5))
            train_sizes = np.linspace(0.1, 1.0, 10)
            scores = []
            for frac in train_sizes:
                n = max(10, int(len(X) * frac))
                idx = np.random.choice(len(X), n, replace=False)
                sub_model = xgb.XGBClassifier(max_depth=4, n_estimators=100, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, eval_metric="logloss")
                sub_model.fit(X[idx], y[idx])
                scores.append(sub_model.score(X, y))
            ax.plot(train_sizes * 100, scores, "b-o")
            ax.set_xlabel("Training Data %")
            ax.set_ylabel("Accuracy")
            ax.set_title("Learning Curve (approx)")
            ax.set_ylim(0, 1.05)
            ax.grid(True, alpha=0.3)
            fig.tight_layout()
            curve_path = str(chart_dir / "learning_curve.png")
            fig.savefig(curve_path, dpi=120)
            plt.close(fig)
            paths.append(curve_path)

        except Exception:
            logger.warning("matplotlib 不可用，使用 OpenCV 生成训练图表", exc_info=True)
            try:
                importance = np.asarray(getattr(model, "feature_importances_", []), dtype=np.float32)
                if importance.size:
                    fallback_path = chart_dir / "feature_importance.png"
                    self._draw_feature_importance_chart(importance, fallback_path, timestamp)
                    paths.append(str(fallback_path))

                probabilities = np.asarray(model.predict_proba(X)[:, 1], dtype=np.float32)
                analysis_path = chart_dir / "training_analysis.png"
                self._draw_training_analysis_chart(probabilities, np.asarray(y), analysis_path, timestamp)
                paths.append(str(analysis_path))
            except Exception:
                logger.warning("OpenCV 训练图表生成失败", exc_info=True)

        return paths

    @staticmethod
    def _draw_feature_importance_chart(importance: np.ndarray, target: Path, timestamp: str) -> None:
        canvas = np.full((640, 1080, 3), 255, dtype=np.uint8)
        cv2.putText(canvas, f"XGBoost feature importance {timestamp}", (42, 44), cv2.FONT_HERSHEY_SIMPLEX, 0.74, (35, 42, 48), 2, cv2.LINE_AA)
        top_indices = np.argsort(importance)[-25:][::-1]
        maximum = max(float(importance[index]) for index in top_indices) if len(top_indices) else 1.0
        start_y, row_height, left, right = 86, 20, 160, 1000
        for row_index, feature_index in enumerate(top_indices):
            y_pos = start_y + row_index * row_height
            value = float(importance[feature_index])
            bar_right = int(left + (right - left) * value / max(maximum, 1e-12))
            cv2.putText(canvas, f"dim_{feature_index}", (20, y_pos + 12), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (80, 88, 94), 1, cv2.LINE_AA)
            cv2.rectangle(canvas, (left, y_pos), (bar_right, y_pos + 13), (90, 120, 60), -1)
            cv2.putText(canvas, f"{value:.4f}", (min(bar_right + 8, 1012), y_pos + 12), cv2.FONT_HERSHEY_SIMPLEX, 0.36, (55, 60, 65), 1, cv2.LINE_AA)
        cv2.imencode(".png", canvas)[1].tofile(str(target))

    @staticmethod
    def _draw_training_analysis_chart(probabilities: np.ndarray, labels: np.ndarray, target: Path, timestamp: str) -> None:
        canvas = np.full((620, 1080, 3), 255, dtype=np.uint8)
        cv2.putText(canvas, f"Training score distribution {timestamp}", (42, 44), cv2.FONT_HERSHEY_SIMPLEX, 0.74, (35, 42, 48), 2, cv2.LINE_AA)
        left, right, top, bottom = 70, 1010, 88, 500
        cv2.line(canvas, (left, bottom), (right, bottom), (80, 88, 94), 1, cv2.LINE_AA)
        cv2.line(canvas, (left, top), (left, bottom), (80, 88, 94), 1, cv2.LINE_AA)
        bins = 25
        normal = np.histogram(probabilities[labels == 0], bins=bins, range=(0.0, 1.0))[0]
        tampered = np.histogram(probabilities[labels == 1], bins=bins, range=(0.0, 1.0))[0]
        maximum = max(1, int(max(normal.max(initial=0), tampered.max(initial=0))))
        group_width = (right - left) / bins
        for index in range(bins):
            x1 = int(left + index * group_width + 2)
            middle = int(left + (index + 0.5) * group_width)
            x2 = int(left + (index + 1) * group_width - 2)
            normal_height = int((bottom - top) * int(normal[index]) / maximum)
            tampered_height = int((bottom - top) * int(tampered[index]) / maximum)
            cv2.rectangle(canvas, (x1, bottom - normal_height), (middle - 1, bottom), (73, 143, 59), -1)
            cv2.rectangle(canvas, (middle + 1, bottom - tampered_height), (x2, bottom), (77, 74, 195), -1)
        cv2.line(canvas, (int(left + 0.65 * (right - left)), top), (int(left + 0.65 * (right - left)), bottom), (56, 70, 188), 2, cv2.LINE_AA)
        cv2.putText(canvas, "threshold 0.65", (int(left + 0.65 * (right - left)) + 7, top + 21), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (56, 70, 188), 1, cv2.LINE_AA)
        cv2.rectangle(canvas, (760, 540), (778, 555), (73, 143, 59), -1)
        cv2.putText(canvas, "normal", (786, 554), cv2.FONT_HERSHEY_SIMPLEX, 0.47, (55, 60, 65), 1, cv2.LINE_AA)
        cv2.rectangle(canvas, (888, 540), (906, 555), (77, 74, 195), -1)
        cv2.putText(canvas, "tampered", (914, 554), cv2.FONT_HERSHEY_SIMPLEX, 0.47, (55, 60, 65), 1, cv2.LINE_AA)
        cv2.imencode(".png", canvas)[1].tofile(str(target))
