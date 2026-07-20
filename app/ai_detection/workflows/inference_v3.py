import cv2
import json
import math
import time
import yaml
import numpy as np
import logging
import os
import joblib
import re
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.ai_detection.core.bbox_overlap_checker import analyze_bbox_iou_overlaps
from app.ai_detection.core.exceptions import RecoverableError
from app.ai_detection.core.extractors import FeatureExtractor, FontFeatureLibrary, TamperAnalyzer
from app.ai_detection.core.detectors import PixelLevelDetector, OriginalityChecker
from app.ai_detection.core.utils import NumpyEncoder, safe_read_image
from app.ai_detection.core.ocr_utils import _resize_for_ocr
from app.ai_detection.core.known_source_matcher import KnownSourcePairMatcher
from app.ai_detection.services.model_registry import ModelRegistry
from app.ai_detection.runtime.paths import resolve_config_path

logger = logging.getLogger(__name__)

PREDICT_MAX_SIDE = max(1, int(os.getenv("AI_PREDICT_MAX_SIDE", "2200") or "2200"))
PREDICT_MAX_PIXELS = max(1, int(os.getenv("AI_PREDICT_MAX_PIXELS", "4000000") or "4000000"))


class InferenceEngineAPI:
    def __init__(self, config_path="config.yaml", shared_ocr_reader: Optional[Any] = None):
        config_file = resolve_config_path(config_path)

        with open(config_file, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.base_dir = config_file.parent

        preprocess_cfg = self.config.get("preprocessing", {})
        self.extractor = FeatureExtractor(
            reader=shared_ocr_reader,
            preserve_aspect_ratio=preprocess_cfg.get("preserve_aspect_ratio", True),
        )
        self.font_lib = FontFeatureLibrary()
        font_lib_path = self._resolve_path(self.config['paths']['font_lib_path'])
        self.font_lib.load(font_lib_path)

        xgb_path = self.config.get('paths', {}).get('xgb_model_path', "models/global_layout_model.pkl")

        pixel_cfg = self.config.get('pixel_detector', {})
        self.pixel_detector = PixelLevelDetector(config=pixel_cfg)

        self.originality_checker = OriginalityChecker()
        self._origin_enabled = self.config.get('originality', {}).get('enabled', True)

        self._font_lib_path = font_lib_path
        registry_path = self.config.get("training", {}).get("registry_path", "models/registry.json")
        self._registry_path = self._resolve_path(registry_path)
        self._legacy_xgb_path = self._resolve_path(xgb_path)
        registry = ModelRegistry(
            self._registry_path,
            fallback_model_path=self._legacy_xgb_path,
        )
        registry.bootstrap_fallback()
        active_entry = registry.resolve_active()
        self._xgb_path = str(active_entry.get("model_path") or self._legacy_xgb_path)
        self._model_reload_lock = threading.RLock()
        self.global_model = joblib.load(self._xgb_path)

        calib_cfg = self.config.get("thresholds", {})
        self._calibration_temp = float(calib_cfg.get("calibration_temperature", 1.0))
        self._global_fake_threshold = float(
            active_entry.get("global_fake_threshold", calib_cfg.get("global_fake", 0.65))
        )
        self._has_calibrated_global_threshold = "global_fake_threshold" in active_entry
        self._known_source_matcher = self._load_known_source_matcher(active_entry)

        self._metrics: dict = {
            "total_predictions": 0,
            "tampered_count": 0,
            "suspicious_count": 0,
            "normal_count": 0,
            "error_count": 0,
            "total_inference_time_ms": 0.0,
            "inference_times_ms": [],
        }

    def list_model_versions(self) -> dict:
        """返回模型版本注册表中所有版本。"""
        return self._model_registry().list_models()

    def _model_registry(self) -> ModelRegistry:
        fallback = getattr(self, "_legacy_xgb_path", None) or getattr(self, "_xgb_path", "")
        return ModelRegistry(self._registry_path, fallback_model_path=fallback)

    def _load_known_source_matcher(self, entry: Dict[str, Any]) -> Optional[KnownSourcePairMatcher]:
        dataset_cfg = self.config.get("dataset", {})
        image_root = self._resolve_path(dataset_cfg.get("image_dir", "images"))
        return KnownSourcePairMatcher.from_file(
            entry.get("reference_index_path"),
            image_root=image_root,
        )

    def _resolve_active_model_path(self, fallback_path: str) -> str:
        registry = ModelRegistry(self._registry_path, fallback_model_path=fallback_path)
        return str(registry.resolve_active().get("model_path") or fallback_path)

    def reload_models(self, version: Optional[str] = None) -> dict:
        """热重载 FAISS 字体库和 XGBoost 模型，无需重启服务。

        指定 version 时从注册表中查找对应版本路径进行加载。
        Python 属性赋值是原子的，读取端无锁安全。
        """
        result = {"font_lib": "unchanged", "global_model": "unchanged"}

        active_entry = self._model_registry().resolve_active()
        xgb_path = str(active_entry.get("model_path") or self._legacy_xgb_path)
        font_lib_path = self._font_lib_path
        global_fake_threshold = float(
            active_entry.get("global_fake_threshold", self.config.get("thresholds", {}).get("global_fake", 0.65))
        )
        has_calibrated_global_threshold = "global_fake_threshold" in active_entry
        known_source_matcher = self._load_known_source_matcher(active_entry)

        if version:
            active_version = self._model_registry().resolve_active().get("version")
            if version != active_version:
                raise ValueError("指定版本切换必须使用模型启用接口完成评估与审计")
            entry = self._model_registry().get(version)
            if entry is None:
                raise ValueError(f"未找到模型版本: {version}")
            xgb_path = entry.get("model_path", xgb_path)
            font_lib_path = entry.get("font_lib_path", font_lib_path)
            global_fake_threshold = float(entry.get("global_fake_threshold", global_fake_threshold))
            has_calibrated_global_threshold = "global_fake_threshold" in entry
            known_source_matcher = self._load_known_source_matcher(entry)
            result["version"] = version

        new_font_lib = FontFeatureLibrary()
        if new_font_lib.load(font_lib_path):
            self.font_lib = new_font_lib
            result["font_lib"] = "reloaded"
        else:
            logger.warning("字体库重载失败，保留当前库")

        try:
            new_model = joblib.load(xgb_path)
        except Exception as exc:
            raise RuntimeError("全局模型加载失败，当前模型未变更") from exc
        if not callable(getattr(new_model, "predict_proba", None)):
            raise RuntimeError("全局模型格式无效，当前模型未变更")
        with self._model_reload_lock:
            self.global_model = new_model
            self._xgb_path = str(Path(xgb_path).resolve())
            self._global_fake_threshold = global_fake_threshold
            self._has_calibrated_global_threshold = has_calibrated_global_threshold
            self._known_source_matcher = known_source_matcher
            if result["font_lib"] == "reloaded":
                self._font_lib_path = str(font_lib_path)
        result["global_model"] = "reloaded"
        result["current_model"] = self._xgb_path

        logger.info("模型热重载完成: %s", result)
        return result

    def install_validated_model(self, model: Any, entry: Dict[str, Any]) -> dict:
        """Atomically install a model that ModelRegistry already validated."""
        if not callable(getattr(model, "predict_proba", None)):
            raise ValueError("候选模型缺少 predict_proba")
        model_path = str(Path(str(entry.get("model_path") or "")).resolve())
        if not model_path:
            raise ValueError("候选模型路径为空")

        next_font_lib = None
        next_known_source_matcher = self._load_known_source_matcher(entry)
        font_path = str(entry.get("font_lib_path") or "").strip()
        if font_path:
            candidate_font_lib = FontFeatureLibrary()
            if candidate_font_lib.load(font_path):
                next_font_lib = candidate_font_lib

        with self._model_reload_lock:
            self.global_model = model
            self._xgb_path = model_path
            self._global_fake_threshold = float(
                entry.get("global_fake_threshold", self.config.get("thresholds", {}).get("global_fake", 0.65))
            )
            self._has_calibrated_global_threshold = "global_fake_threshold" in entry
            self._known_source_matcher = next_known_source_matcher
            if next_font_lib is not None:
                self.font_lib = next_font_lib
                self._font_lib_path = font_path
        return {
            "version": entry.get("version"),
            "global_model": "reloaded",
            "font_lib": "reloaded" if next_font_lib is not None else "unchanged",
            "current_model": self._xgb_path,
        }

    def get_metrics(self) -> dict:
        """返回累计推理指标快照。"""
        times = self._metrics.get("inference_times_ms", [])
        sorted_times = sorted(times) if times else []
        n = len(sorted_times)
        p50 = sorted_times[n // 2] if n > 0 else 0.0
        p99 = sorted_times[min(n - 1, int(n * 0.99))] if n > 0 else 0.0

        return {
            "total_predictions": self._metrics["total_predictions"],
            "tampered_count": self._metrics["tampered_count"],
            "suspicious_count": self._metrics["suspicious_count"],
            "normal_count": self._metrics["normal_count"],
            "error_count": self._metrics["error_count"],
            "inference_p50_ms": round(p50, 2),
            "inference_p99_ms": round(p99, 2),
            "avg_inference_ms": round(
                self._metrics["total_inference_time_ms"] / max(1, self._metrics["total_predictions"]), 2
            ),
            "font_lib_size": self.font_lib.index.ntotal,
            "font_lib_ready": self.font_lib.is_ready,
        }

    def _resolve_path(self, path_str: str) -> str:
        path = Path(path_str)
        if path.is_absolute():
            return str(path)
        return str((self.base_dir / path).resolve())

    @staticmethod
    def _calibrate_proba(raw_proba: float, temperature: float) -> float:
        """二次校准：温度缩放防止 XGBoost 极端置信度。temperature > 1 压低极端值。"""
        p = max(1e-3, min(1.0 - 1e-3, raw_proba))
        logit = math.log(p / (1.0 - p))
        return 1.0 / (1.0 + math.exp(-logit / temperature))

    @staticmethod
    def _clip_bbox_xyxy(bbox_xyxy: List[int], img_w: int, img_h: int) -> Tuple[int, int, int, int]:
        x1, y1, x2, y2 = [int(v) for v in bbox_xyxy]
        x1 = max(0, min(x1, img_w - 1))
        y1 = max(0, min(y1, img_h - 1))
        x2 = max(x1 + 1, min(x2, img_w))
        y2 = max(y1 + 1, min(y2, img_h))
        return x1, y1, x2, y2

    def _normalize_roi_bbox(self, roi_bbox: List[int], img_w: int, img_h: int, bbox_format: str) -> Tuple[int, int, int, int]:
        if len(roi_bbox) != 4:
            raise ValueError("ROI bbox must contain exactly four integers.")

        x1, y1, third, fourth = [int(v) for v in roi_bbox]
        format_name = (bbox_format or "auto").lower()

        if format_name == "xyxy":
            return self._clip_bbox_xyxy([x1, y1, third, fourth], img_w, img_h)

        if format_name == "xywh":
            return self._clip_bbox_xyxy([x1, y1, x1 + third, y1 + fourth], img_w, img_h)

        looks_like_xyxy = third > x1 and fourth > y1 and third <= img_w and fourth <= img_h
        if looks_like_xyxy:
            return self._clip_bbox_xyxy([x1, y1, third, fourth], img_w, img_h)

        return self._clip_bbox_xyxy([x1, y1, x1 + third, y1 + fourth], img_w, img_h)

    @staticmethod
    def _scale_bbox_xyxy(
        bbox_xyxy: Tuple[int, int, int, int],
        *,
        scale: float,
        img_w: int,
        img_h: int,
    ) -> Tuple[int, int, int, int]:
        if scale >= 0.999:
            return InferenceEngineAPI._clip_bbox_xyxy(list(bbox_xyxy), img_w, img_h)
        x1, y1, x2, y2 = bbox_xyxy
        return InferenceEngineAPI._clip_bbox_xyxy(
            [
                int(round(x1 * scale)),
                int(round(y1 * scale)),
                int(round(x2 * scale)),
                int(round(y2 * scale)),
            ],
            img_w,
            img_h,
        )

    @staticmethod
    def _profile_numeric_text(extracted_text: str, max_len: int) -> Dict[str, float]:
        text_clean = extracted_text.replace(" ", "")
        total_len = len(text_clean)
        digit_count = len(re.findall(r"\d", text_clean))
        digit_ratio = (digit_count / total_len) if total_len else 0.0

        amount_pattern = re.search(r"\d[\d,]*[.:]\d{1,2}", text_clean)
        currency_hint = re.search(r"(小写|金额|元|¥|￥|人民币)", text_clean)
        order_hint = re.search(r"(单号|订单|流水|凭证|参考号)", text_clean)

        is_core_candidate = digit_count >= 3 and (
            digit_ratio >= 0.35 or amount_pattern is not None or currency_hint is not None or order_hint is not None
        )
        should_use_font_signal = is_core_candidate or (digit_count >= 3 and total_len <= max_len * 2)

        return {
            "digit_count": digit_count,
            "digit_ratio": digit_ratio,
            "total_len": total_len,
            "is_core_candidate": float(is_core_candidate),
            "should_use_font_signal": float(should_use_font_signal),
        }

    @staticmethod
    def _has_hard_metadata_evidence(
        metadata_risk: float,
        metadata_reasons: List[str],
        threshold: float = 0.50,
    ) -> bool:
        _ = metadata_risk, threshold
        return any("EXIF检测到已知修图软件" in reason for reason in (metadata_reasons or []))

    def predict(
        self,
        full_image_path: str,
        roi_bbox: List[int],
        bbox_format: str = "auto",
        precomputed_global_feat: Optional[np.ndarray] = None,
        detection_bboxes: Optional[List[List[int]]] = None,
    ) -> str:
        try:
            _t0 = time.time()
            reasons = []
            result_status = "正常"

            img = safe_read_image(full_image_path)
            if img is None:
                self._metrics["error_count"] += 1
                return json.dumps({"result": "错误", "reason": "无法读取图片或路径不存在"}, ensure_ascii=False)

            original_img = img
            orig_h, orig_w = original_img.shape[:2]
            known_source_matcher = getattr(self, "_known_source_matcher", None)

            work_img, work_scale = _resize_for_ocr(
                original_img,
                max_side=PREDICT_MAX_SIDE,
                max_pixels=PREDICT_MAX_PIXELS,
            )
            img = work_img
            img_h, img_w = img.shape[:2]

            rules = self.config.get('business_rules', {})
            weights = self.config.get('weights', {})
            thresh = self.config.get('thresholds', {})
            fusion_cfg = self.config.get('fusion', {})

            margin = rules.get('roi_expand_margin', 15)
            max_len = rules.get('max_core_text_length', 15)

            thresh_global = float(getattr(self, "_global_fake_threshold", thresh.get('global_fake', 0.65)))
            thresh_pixel_alert = thresh.get('pixel_anomaly_alert', 0.60)
            thresh_exempt = thresh.get('exempt_pixel_safe', 0.40)
            thresh_high = thresh.get('suspect_high', 0.65)
            thresh_low = thresh.get('suspect_low', 0.50)

            fusion_method = fusion_cfg.get('method', 'weighted')
            w_global = fusion_cfg.get('weight_global', 0.35)
            w_local = fusion_cfg.get('weight_local', 0.65)

            orig_x1, orig_y1, orig_x2, orig_y2 = self._normalize_roi_bbox(roi_bbox, orig_w, orig_h, bbox_format)
            x1, y1, x2, y2 = self._scale_bbox_xyxy(
                (orig_x1, orig_y1, orig_x2, orig_y2),
                scale=work_scale,
                img_w=img_w,
                img_h=img_h,
            )
            x, y, w, h = x1, y1, x2 - x1, y2 - y1
            out_x, out_y = orig_x1, orig_y1
            out_w, out_h = orig_x2 - orig_x1, orig_y2 - orig_y1

            # ================== 0. EXIF/元数据分析 ==================
            metadata_risk = 0.0
            metadata_reasons = []
            if self._origin_enabled:
                orig_feats, hard_rule, _ = self.originality_checker.extract_features(full_image_path)
                if hard_rule:
                    metadata_reasons.append("EXIF检测到已知修图软件")
                    reasons.extend(metadata_reasons)
                    metadata_risk = 0.55
                elif orig_feats:
                    m_risk, m_reasons = OriginalityChecker.compute_metadata_risk(orig_feats)
                    metadata_risk = m_risk
                    metadata_reasons = list(m_reasons)
                    reasons.extend(metadata_reasons)

            # ================== 1. 全局特征分析 ==================
            if precomputed_global_feat is not None:
                global_feat = precomputed_global_feat
            else:
                global_feat = self.extractor.extract_global_feature(img)
            global_fake_prob_raw = float(self.global_model.predict_proba(np.array([global_feat]))[0][1])
            global_fake_prob = self._calibrate_proba(global_fake_prob_raw, self._calibration_temp)

            # ================== 2. 局部微观分析 ==================
            x_exp, y_exp = max(0, x - margin), max(0, y - margin)
            w_exp = min(img_w - x_exp, w + 2 * margin)
            h_exp = min(img_h - y_exp, h + 2 * margin)

            roi_img = img[y:y + h, x:x + w]
            roi_img_expanded = img[y_exp:y_exp + h_exp, x_exp:x_exp + w_exp]

            roi_rgb = cv2.cvtColor(roi_img, cv2.COLOR_BGR2RGB)
            feats, stats = self.extractor.extract_from_roi(roi_rgb)

            feature_texts = [s['text'] for s in stats if s.get('is_core_number')]
            extracted_text = "".join(feature_texts) if feature_texts else "".join([s['text'] for s in stats])
            text_profile = self._profile_numeric_text(extracted_text, max_len)
            should_use_font_signal = bool(text_profile["should_use_font_signal"])

            # 字体库冷启动降级：库为空时跳过字体信号，像素权重自动提升
            if not self.font_lib.is_ready:
                should_use_font_signal = False

            # 批量字体相似度查询
            font_sims = self.font_lib.search_similarity_batch(feats) if (feats and self.font_lib.is_ready) else []
            font_sim = np.mean(font_sims) if font_sims else 0.5
            font_anomaly = max(0.0, 1.0 - font_sim)

            # 字体异常分级：仅当 ROI 包含核心文本且匹配度足够时才计入
            if font_sim < 0.3:
                effective_font_anomaly = 0.0       # 字体不在库中
            elif font_sim < 0.5:
                effective_font_anomaly = font_anomaly * 0.3  # 中匹配缩放
            else:
                effective_font_anomaly = font_anomaly         # 高匹配全量

            # 像素检测（增加周围背景用于噪声一致性对比）
            surrounding = None
            if margin > 0 and img.size > 0:
                sur_x1 = max(0, x - margin * 4)
                sur_y1 = max(0, y - margin * 4)
                sur_x2 = min(img_w, x + w + margin * 4)
                sur_y2 = min(img_h, y + h + margin * 4)
                surrounding = img[sur_y1:sur_y2, sur_x1:sur_x2]

            pixel_anomaly = self.pixel_detector.detect(roi_img_expanded, surrounding_np=surrounding)
            geo_reasons, geo_penalty = TamperAnalyzer.check_internal_consistency(stats)

            # ---- 像素重叠 / 检测框 IoU 分析 ----
            bbox_overlap_result = analyze_bbox_iou_overlaps(
                detection_bboxes or [],
                roi_bbox_xyxy=[orig_x1, orig_y1, orig_x2, orig_y2],
                thresholds=thresh,
            )
            bbox_iou_risk = float(bbox_overlap_result.get("risk", 0.0))
            bbox_iou_hard_tamper = bool(bbox_overlap_result.get("hard_tamper"))

            # ================== 3. 自适应权重计算 ==================
            if should_use_font_signal and len(extracted_text) > 0:
                local_tamper_prob = (
                    pixel_anomaly * weights.get('core_pixel', 0.6)
                    + effective_font_anomaly * weights.get('core_font', 0.4)
                    + geo_penalty
                )
                if text_profile["digit_count"] >= 8 and effective_font_anomaly > 0.75:
                    local_tamper_prob = max(local_tamper_prob, thresh_low + 0.02)
            else:
                local_tamper_prob = (pixel_anomaly * weights.get('non_core_pixel', 0.8)) + geo_penalty
                if pixel_anomaly < thresh_exempt and geo_penalty == 0:
                    local_tamper_prob = 0.0

            local_tamper_prob = max(0.0, min(1.0, float(local_tamper_prob)))

            # ================== 4. 融合策略：两层 AI 篡改检测 ==================
            # 第一层：全图 AI 生成特征（全局模型）
            global_ai_score = global_fake_prob

            # 第二层：ROI 局部异常（像素 + 字体 + 几何）
            local_anomaly = max(local_tamper_prob, float(geo_penalty))

            # 融合：全局 AI 信号 + 局部异常交叉验证
            if global_ai_score > thresh_global and local_anomaly > 0.50:
                # 全局和局部都异常 → AI 局部篡改，高置信度
                final_risk = (global_ai_score + local_anomaly) / 2.0
            elif global_ai_score > thresh_global:
                # 仅全局异常 → EXIF + 元数据辅助判定可信度
                _has_exif = 0
                if self._origin_enabled:
                    _of, _, _ = self.originality_checker.extract_features(full_image_path)
                    _has_exif = _of.get("has_exif", 0) if _of else 0

                _effective_global = global_ai_score

                # EXIF 分级乘数：全局越确信，乘数越高
                if _has_exif:
                    if _effective_global >= 0.75:
                        g_mult = 0.73
                    else:
                        g_mult = 0.60
                elif _effective_global > 0.90:
                    g_mult = 0.70
                else:
                    g_mult = 0.50
                final_risk = _effective_global * g_mult + local_anomaly * (1.0 - g_mult) * 0.5
            else:
                # 全局正常 → 局部 + 元数据兜底
                if local_anomaly > 0.80:
                    final_risk = local_anomaly * 0.90
                else:
                    final_risk = local_anomaly * 0.70

            # bbox IOU 风险叠加
            if bbox_iou_risk > 0:
                final_risk = max(final_risk, bbox_iou_risk * 0.65)

            metadata_hard_tamper = self._has_hard_metadata_evidence(
                metadata_risk,
                metadata_reasons,
                float(thresh.get("metadata_hard_evidence", 0.50)),
            )
            strong_global_tamper = bool(
                getattr(self, "_has_calibrated_global_threshold", False)
            ) and global_fake_prob >= float(thresh.get("strong_global_tamper", 0.65))
            if metadata_hard_tamper:
                final_risk = max(final_risk, float(thresh_high) + 0.02)

            final_risk = max(0.0, min(1.0, float(final_risk)))

            # ================== 5. 结果判定与理由梳理 ==================
            if global_fake_prob > thresh_global:
                reasons.append("全局UI布局异常")
            if pixel_anomaly > thresh_pixel_alert:
                reasons.append("存在局部边缘拼接/像素涂抹痕迹")
            if bbox_overlap_result.get("reasons"):
                reasons.extend(bbox_overlap_result["reasons"])
            if should_use_font_signal and font_anomaly > 0.65:
                reasons.append("局部字体风格异常")
            if geo_penalty > 0:
                reasons.extend(geo_reasons)

            if bbox_iou_hard_tamper:
                result_status = "篡改"
                final_risk = max(final_risk, float(thresh_high) + 0.05)
            elif metadata_hard_tamper:
                result_status = "篡改"
            elif strong_global_tamper:
                result_status = "篡改"
                final_risk = max(final_risk, float(thresh_high) + 0.01)
            elif final_risk > thresh_high:
                result_status = "篡改"
            elif final_risk > thresh_low:
                result_status = "可疑"
            else:
                if not reasons:
                    reasons.append("未检出明显篡改痕迹")

            output = {
                "result": result_status,
                "confidence": final_risk,
                "bbox": [int(i) for i in [out_x, out_y, out_w, out_h]],
                "reason": "；".join(dict.fromkeys(reasons)),
                "bbox_overlap_check": bbox_overlap_result.get("bbox_overlap_check"),
                "hard_tamper_flags": {
                    "bbox_iou": bbox_iou_hard_tamper,
                },
            }

            elapsed_ms = (time.time() - _t0) * 1000.0
            self._metrics["total_predictions"] += 1
            self._metrics["total_inference_time_ms"] += elapsed_ms
            self._metrics["inference_times_ms"].append(elapsed_ms)
            if result_status == "篡改":
                self._metrics["tampered_count"] += 1
            elif result_status == "可疑":
                self._metrics["suspicious_count"] += 1
            else:
                self._metrics["normal_count"] += 1

            return json.dumps(output, ensure_ascii=False, indent=4, cls=NumpyEncoder)

        except RecoverableError as e:
            self._metrics["error_count"] += 1
            logger.warning("可恢复的业务异常: %s", e)
            error_output = {
                "result": "错误",
                "confidence": 0.0,
                "bbox": roi_bbox,
                "reason": str(e),
            }
            return json.dumps(error_output, ensure_ascii=False, indent=4, cls=NumpyEncoder)
        except Exception as e:
            self._metrics["error_count"] += 1
            logger.error("引擎推理引发未捕获系统异常: %s", e, exc_info=True)
            error_output = {
                "result": "错误",
                "confidence": 0.0,
                "bbox": roi_bbox,
                "reason": "引擎内部解析失败，请联系运维排查",
            }
            return json.dumps(error_output, ensure_ascii=False, indent=4, cls=NumpyEncoder)


if __name__ == "__main__":
    import time

    logger.info("启动单图推理本地测试 (Inference API)")

    try:
        engine = InferenceEngineAPI(config_path="config.yaml")
        logger.info("引擎初始化成功")
    except Exception as e:
        logger.error(f"引擎初始化失败: {e}", exc_info=True)
        exit(1)

    test_image_path = "images/no (11).jpg"
    test_bbox = [150, 200, 180, 45]

    if not os.path.exists(test_image_path):
        logger.warning(f"找不到测试图片: {test_image_path}，请修改路径后重试。")
    else:
        logger.info(f"目标图片: {test_image_path} | BBox: {test_bbox}")
        start_time = time.time()

        try:
            result_json = engine.predict(full_image_path=test_image_path, roi_bbox=test_bbox)
            cost_time = time.time() - start_time
            logger.info(f"推理耗时: {cost_time:.3f} 秒")
            logger.info(f"返回结果:\n{result_json}")
        except Exception as e:
            logger.error(f"推理过程中发生错误: {e}", exc_info=True)
