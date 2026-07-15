"""
训练管线 — 支持反馈数据增强、模型版本化、可视化。
使用 /api/v3/train 端点触发，训练前有风险提示。
"""

import json
import logging
import os
import shutil
import hashlib
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
from app.ai_detection.model_registry import ModelRegistry

logger = logging.getLogger(__name__)


class TrainPipeline:
    """端到端训练管线 — 全局模型 + 字体库 + 可视化。"""

    def __init__(self, config_path: str = "config.yaml", ocr_reader: Any = None):
        config_file = Path(config_path)
        if not config_file.is_absolute():
            config_file = (Path(__file__).resolve().parent / config_file).resolve()
        with open(config_file, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        self.base_dir = config_file.parent

        train_cfg = self.config.get("training", {})
        self.output_dir = Path(self._resolve(train_cfg.get("output_dir", "models/trained")))
        self.viz_enabled = train_cfg.get("visualization_enabled", True)
        self.viz_dir = Path(self._resolve(train_cfg.get("visualization_dir", "models/trained/viz")))
        self.backup_previous = train_cfg.get("backup_previous", True)
        self.feature_cache_dir = Path(
            self._resolve(train_cfg.get("feature_cache_dir", "models/feature_cache"))
        )
        self.feature_config_version = str(train_cfg.get("feature_config_version", "global-v1"))

        self.ocr_reader = ocr_reader
        self.extractor: Optional[FeatureExtractor] = None
        self.font_lib: Optional[FontFeatureLibrary] = None

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
        if self.viz_enabled:
            self.viz_dir.mkdir(parents=True, exist_ok=True)

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

        total_samples = len(original_images) + len(reviewed_images)
        processed = 0

        # 处理原始数据集
        for img_path, label, group_id in original_images:
            try:
                img = cv2.imdecode(np.fromfile(img_path, dtype=np.uint8), cv2.IMREAD_COLOR)
                if img is None:
                    continue
                # 全局特征 + 增强
                for aug_name, aug_img in self._global_variants(img, key=os.path.basename(img_path)):
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

                processed += 1
                if progress_callback:
                    progress_callback(processed, total_samples, f"处理原始数据: {os.path.basename(img_path)}")
            except Exception:
                logger.warning("处理图像失败: %s", img_path, exc_info=True)

        # 只使用显式二审标签；初审 correct/wrong/suspicious 永不参与训练。
        for fb_img_path, label, group_id in reviewed_images:
            try:
                img = cv2.imdecode(np.fromfile(fb_img_path, dtype=np.uint8), cv2.IMREAD_COLOR)
                if img is None:
                    continue
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
            except Exception:
                logger.warning("处理反馈图像失败: %s", fb_img_path, exc_info=True)

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

        # 可视化
        viz_paths = []
        if self.viz_enabled:
            viz_paths = self._generate_visualizations(model, X, y, timestamp)

        evaluation = self._evaluation_metrics(model, X_val, y_val)
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
            "evaluation": evaluation,
            "active_evaluation": active_evaluation,
            "model_path": model_path,
            "font_lib_path": font_lib_path,
            "visualizations": viz_paths,
            "train_accuracy": float(model.score(X, y)),
        }

        # 保存训练摘要
        report_path = version_dir / "evaluation_report.json"
        summary["report_path"] = str(report_path)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

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
        artifacts = []
        for path in sorted(version_dir.iterdir()):
            if not path.is_file() or path.name in {"manifest.json", "evaluation_report.json"}:
                continue
            artifacts.append(
                {
                    "filename": path.name,
                    "size_bytes": path.stat().st_size,
                    "sha256": self._sha256(path),
                }
            )
        manifest = {
            "version": summary["timestamp"],
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "feature_config_version": self.feature_config_version,
            "artifacts": artifacts,
        }
        path = version_dir / "manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
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
                "train_accuracy": summary["train_accuracy"],
                "total_samples": summary["total_samples"],
                "evaluation": summary["evaluation"],
                "active_evaluation": summary["active_evaluation"],
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
        """加载原始 images/ 数据集并分配标签。"""
        dataset_cfg = self.config.get("dataset", {})
        img_dir = Path(self._resolve(dataset_cfg.get("image_dir", "images")))
        samples = []
        if img_dir.exists():
            for path in sorted(img_dir.iterdir()):
                if path.suffix.lower() not in (".jpg", ".jpeg", ".png"):
                    continue
                label = 0 if "no" in path.name.lower() else 1
                samples.append((str(path), label, self._base_group_name(path)))
        return samples

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
        feature = self.extractor.extract_global_feature(image)
        temp_path = cache_path.with_name(f".{cache_path.name}.tmp")
        with temp_path.open("wb") as stream:
            np.save(stream, feature, allow_pickle=False)
        os.replace(temp_path, cache_path)
        return feature

    @staticmethod
    def _evaluation_metrics(model: Any, X_val: np.ndarray, y_val: np.ndarray) -> Dict[str, Any]:
        if len(X_val) == 0 or len(np.unique(y_val)) < 2:
            return {
                "available": False,
                "balanced_accuracy": None,
                "normal_recall": None,
                "tampered_recall": None,
                "confusion_matrix": None,
            }
        predicted = model.predict(X_val)
        matrix = confusion_matrix(y_val, predicted, labels=[0, 1])
        return {
            "available": True,
            "balanced_accuracy": float(balanced_accuracy_score(y_val, predicted)),
            "normal_recall": float(recall_score(y_val, predicted, pos_label=0)),
            "tampered_recall": float(recall_score(y_val, predicted, pos_label=1)),
            "confusion_matrix": matrix.astype(int).tolist(),
        }

    def _active_model_evaluation(self, X_val: np.ndarray, y_val: np.ndarray) -> Dict[str, Any]:
        fallback = self._resolve(
            self.config.get("paths", {}).get("xgb_model_path", "models/global_layout_model.pkl")
        )
        registry_path = self._resolve(
            self.config.get("training", {}).get("registry_path", "models/registry.json")
        )
        active_path = ModelRegistry(
            registry_path,
            fallback_model_path=fallback,
        ).resolve_active().get("model_path")
        try:
            active_model = joblib.load(str(active_path))
            return self._evaluation_metrics(active_model, X_val, y_val)
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
        json_dir = Path(self._resolve(self.config.get("dataset", {}).get("json_dir", "locate_json")))
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
                feat_np = feat.view(-1).cpu().numpy()
                all_feats.append(feat_np)
                all_labels.append(str(text)[:20])

    def _generate_visualizations(self, model, X, y, timestamp) -> list:
        """生成训练可视化图片并保存。"""
        import re

        paths = []
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
            imp_path = str(self.viz_dir / f"feature_importance_{timestamp}.png")
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
            dist_path = str(self.viz_dir / f"training_analysis_{timestamp}.png")
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
            curve_path = str(self.viz_dir / f"learning_curve_{timestamp}.png")
            fig.savefig(curve_path, dpi=120)
            plt.close(fig)
            paths.append(curve_path)

        except Exception:
            logger.warning("生成可视化失败", exc_info=True)

        return paths
