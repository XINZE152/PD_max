import os
import cv2
import numpy as np
import joblib
from PIL import Image, ExifTags
from typing import Dict, List


class PixelLevelDetector:
    """终极版：捕捉拼接、高频突变以及 '生成器假图(零噪点)' 的检测器"""

    # Alpha 羽化贴图：双重边缘 + 长梯度（参考 overlay 造样本标定）
    _BLEND_DE_COMBINED_MIN = 0.045
    _BLEND_LG_COMBINED_MIN = 0.05
    _BLEND_DE_STRONG_MIN = 0.08

    @staticmethod
    def _structural_overlap_score(gray: np.ndarray, band_ratio: float = 0.08, min_band: int = 4) -> float:
        """ROI 中心与边缘带统计差异 + 拉普拉斯投影接缝。"""
        h, w = gray.shape
        if h < 24 or w < 24:
            return 0.0

        band_h = max(min_band, int(h * band_ratio))
        band_w = max(min_band, int(w * band_ratio))

        core = gray[band_h:-band_h, band_w:-band_w]
        if core.size == 0:
            return 0.0

        core_var = float(np.var(core.astype(np.float64)))
        core_mean = float(np.mean(core.astype(np.float64)))

        bands = [
            gray[:band_h, :],
            gray[-band_h:, :],
            gray[:, :band_w],
            gray[:, -band_w:],
        ]
        band_scores = []
        for band in bands:
            if band.size == 0:
                continue
            band_var = float(np.var(band.astype(np.float64)))
            band_mean = float(np.mean(band.astype(np.float64)))
            var_ratio = abs(band_var - core_var) / (core_var + 1e-6)
            mean_diff = abs(band_mean - core_mean) / 255.0
            band_scores.append(min(1.0, var_ratio * 0.15 + mean_diff * 0.8))

        lap = cv2.Laplacian(gray, cv2.CV_64F)
        seam_score = 0.0
        h_proj = np.mean(np.abs(lap), axis=1)
        v_proj = np.mean(np.abs(lap), axis=0)
        if len(h_proj) > band_h * 2 + 4:
            h_core = h_proj[band_h:-band_h]
            h_peak = (float(np.max(h_core)) - float(np.mean(h_core))) / (float(np.std(h_core)) + 1e-6)
            seam_score = max(seam_score, min(0.5, h_peak * 0.12))
        if len(v_proj) > band_w * 2 + 4:
            v_core = v_proj[band_w:-band_w]
            v_peak = (float(np.max(v_core)) - float(np.mean(v_core))) / (float(np.std(v_core)) + 1e-6)
            seam_score = max(seam_score, min(0.5, v_peak * 0.12))

        edge_score = max(band_scores) if band_scores else 0.0
        return float(min(1.0, edge_score * 0.65 + seam_score * 0.35))

    @staticmethod
    def _alpha_blend_metrics(gray: np.ndarray) -> tuple[float, float]:
        """Canny 双重边缘占比 + Sobel 强梯度占比（针对羽化/Alpha 贴图）。"""
        h, w = gray.shape
        if h < 8 or w < 8:
            return 0.0, 0.0

        edges = cv2.Canny(gray, 40, 120)
        kernel = np.ones((3, 1), np.uint8)
        double_edge = cv2.absdiff(edges, cv2.erode(edges, kernel, iterations=1))
        double_edge_ratio = float(np.sum(double_edge > 0) / (w * h))

        grad = cv2.Sobel(gray.astype(np.float64), cv2.CV_64F, 1, 1, ksize=3)
        long_gradient_ratio = float(np.sum(np.abs(grad) > 5) / (w * h))
        return double_edge_ratio, long_gradient_ratio

    @staticmethod
    def _ela_inconsistency_score(gray: np.ndarray, quality: int = 85) -> float:
        """ELA 局部不一致：同层文字替换/粘贴常出现中心区域压缩残差偏高。"""
        from io import BytesIO

        h, w = gray.shape
        if h < 8 or w < 8:
            return 0.0

        pil = Image.fromarray(gray)
        buffer = BytesIO()
        pil.save(buffer, "JPEG", quality=quality)
        buffer.seek(0)
        recompressed = np.array(Image.open(buffer).convert("L"), dtype=np.int16)
        if recompressed.shape != gray.shape:
            recompressed = cv2.resize(recompressed.astype(np.uint8), (w, h)).astype(np.int16)

        ela = np.abs(gray.astype(np.int16) - recompressed)
        band = max(2, min(h, w) // 8)
        if h <= band * 3 or w <= band * 3:
            core = ela
            edge_mean = float(np.mean(ela))
        else:
            core = ela[band:-band, band:-band]
            edges = np.concatenate(
                [
                    ela[:band, :].reshape(-1),
                    ela[-band:, :].reshape(-1),
                    ela[:, :band].reshape(-1),
                    ela[:, -band:].reshape(-1),
                ]
            )
            edge_mean = float(np.mean(edges)) if edges.size else 0.0

        core_mean = float(np.mean(core)) if core.size else 0.0
        ratio = core_mean / (edge_mean + 1e-6)
        std_boost = float(np.std(core) / (np.std(ela) + 1e-6))
        score = max(0.0, (ratio - 1.15) * 0.55) + max(0.0, (std_boost - 0.35) * 0.35)
        return float(min(1.0, score))

    @staticmethod
    def _noise_inconsistency_score(gray: np.ndarray) -> float:
        """水平条带噪声方差差异：无痕文字替换常仅影响局部条带。"""
        h, w = gray.shape
        if h < 12 or w < 12:
            return 0.0

        kernel = np.array([[-1, -1, -1], [-1, 8, -1], [-1, -1, -1]], dtype=np.float32)
        noise = cv2.filter2D(gray.astype(np.float32), -1, kernel)
        band_count = 5
        band_h = max(2, h // band_count)
        band_vars: List[float] = []
        for index in range(band_count):
            start = index * band_h
            end = h if index == band_count - 1 else (index + 1) * band_h
            strip = noise[start:end, :]
            if strip.size:
                band_vars.append(float(np.var(strip)))

        if len(band_vars) < 2:
            return 0.0

        var_range = max(band_vars) - min(band_vars)
        var_mean = float(np.mean(band_vars)) + 1e-6
        ratio = var_range / var_mean
        if ratio < 0.65:
            return 0.0
        return float(min(1.0, (ratio - 0.55) * 0.55))

    @classmethod
    def _alpha_blend_overlap_score(cls, double_edge_ratio: float, long_gradient_ratio: float) -> float:
        """
        以双重边缘为主、长梯度为辅；避免仅凭高梯度误报正常 UI 截图。
        """
        de = double_edge_ratio
        lg = long_gradient_ratio

        if de >= cls._BLEND_DE_COMBINED_MIN and lg >= cls._BLEND_LG_COMBINED_MIN:
            de_boost = min(1.0, (de - cls._BLEND_DE_COMBINED_MIN) / 0.085)
            lg_boost = min(0.30, max(0.0, (lg - cls._BLEND_LG_COMBINED_MIN) * 1.2))
            return float(min(1.0, 0.42 + de_boost * 0.48 + lg_boost))

        if de >= cls._BLEND_DE_STRONG_MIN:
            return float(min(1.0, 0.38 + (de - cls._BLEND_DE_STRONG_MIN) / 0.10))

        return float(min(0.35, max(0.0, (de - 0.030) / 0.08) * 0.28))

    @classmethod
    def overlap_metrics(cls, cropped_img_np: np.ndarray) -> Dict[str, float]:
        """返回像素重叠分项指标，供 API 调试展示。"""
        if cropped_img_np is None or cropped_img_np.size == 0:
            return {
                "structural_score": 0.0,
                "blend_score": 0.0,
                "double_edge_ratio": 0.0,
                "long_gradient_ratio": 0.0,
                "ela_score": 0.0,
                "noise_inconsistency_score": 0.0,
                "text_splice_score": 0.0,
                "pixel_overlap_score": 0.0,
            }

        gray = cv2.cvtColor(cropped_img_np, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (3, 3), 0)
        structural = cls._structural_overlap_score(gray)
        de, lg = cls._alpha_blend_metrics(gray)
        blend = cls._alpha_blend_overlap_score(de, lg)
        ela = cls._ela_inconsistency_score(gray)
        noise_inc = cls._noise_inconsistency_score(gray)
        text_splice = float(min(1.0, max(ela, noise_inc * 0.9)))
        final = float(min(1.0, max(structural, blend, text_splice)))
        return {
            "structural_score": round(structural, 4),
            "blend_score": round(blend, 4),
            "double_edge_ratio": round(de, 4),
            "long_gradient_ratio": round(lg, 4),
            "ela_score": round(ela, 4),
            "noise_inconsistency_score": round(noise_inc, 4),
            "text_splice_score": round(text_splice, 4),
            "pixel_overlap_score": round(final, 4),
        }

    def detect(self, cropped_img_np, quality=85):
        if cropped_img_np is None or cropped_img_np.size == 0:
            return 0.0

        # 1. 基础 ELA 检测 (抓取传统 PS 拼接)
        from io import BytesIO
        img_pil = Image.fromarray(cv2.cvtColor(cropped_img_np, cv2.COLOR_BGR2RGB))
        buffer = BytesIO()
        img_pil.save(buffer, 'JPEG', quality=quality)
        buffer.seek(0)
        ela_img = np.abs(np.array(img_pil).astype(np.int16) - np.array(Image.open(buffer)).astype(np.int16))
        ela_gray = np.max(ela_img, axis=2)

        ela_mean = np.mean(ela_gray) / 255.0
        ela_std = np.std(ela_gray) / 128.0
        ela_score = (ela_mean * (1 + ela_std)) * 2.0

        # 2. 拉普拉斯高频突变检测 (抓边缘生硬的贴图)
        gray = cv2.cvtColor(cropped_img_np, cv2.COLOR_BGR2GRAY)

        # 【新增：物理屏摄摩尔纹抗性装甲】
        # 利用 3x3 高斯核熔断屏幕像素点带来的高频周期性噪声
        gray = cv2.GaussianBlur(gray, (3, 3), 0)

        laplacian = cv2.Laplacian(gray, cv2.CV_64F)

        h, w = gray.shape
        edge_penalty = 0.0
        generator_penalty = 0.0

        if h > 20 and w > 20:
            # 取图片外围一圈的背景区域
            mask = np.ones((h, w), dtype=bool)
            mask[5:-5, 5:-5] = False
            bg_pixels = gray[mask]

            # 【核心杀招】：造假生成器的背景通常是绝对的纯色 (方差接近 0)
            # 真实截图经过微信等压缩，背景方差必然大于 0.1
            bg_var = np.var(bg_pixels)
            if bg_var < 0.05:
                # 如果背景平滑到极其不自然的地步，赋予极高的生成器假图惩罚分！
                generator_penalty = 0.70

                # 传统边缘接缝检测
            core = laplacian[10:-10, 10:-10]
            core_var = np.var(core)
            total_var = np.var(laplacian)
            if core_var > 0:
                noise_diff_ratio = abs(total_var - core_var) / core_var
                edge_penalty = min(0.4, noise_diff_ratio * 0.3)

        # 最终像素得分 = ELA得分 + 边缘贴图惩罚 + 生成器纯色惩罚
        score = ela_score + edge_penalty + generator_penalty
        return float(min(1.0, score))

    def detect_overlap(self, cropped_img_np, band_ratio=0.08, min_band=4):
        """
        检测 ROI 像素重叠/拼接：结构接缝分 + Alpha 羽化贴图分（双重边缘/长梯度），取较高者。
        """
        metrics = self.overlap_metrics(cropped_img_np)
        return float(metrics["pixel_overlap_score"])


class OriginalityChecker:
    """原图与 EXIF 校验器"""
    def __init__(self, model_path=None):
        self.model = joblib.load(model_path) if model_path and os.path.exists(model_path) else None

    @staticmethod
    def extract_features(image_path):
        feats = {'has_exif': 0, 'exif_count': 0, 'time_diff': 0, 'noise_std': 0,
                 'noise_mean': 0, 'noise_skew': 0, 'size_per_pixel': 0, 'color_entropy': 0}
        hard_rule_tampered = False
        suspicious_software = ''

        if not os.path.exists(image_path): return None, False, ""

        try:
            img_pil = Image.open(image_path)
            exif = img_pil._getexif()
            if exif:
                feats['has_exif'] = 1
                feats['exif_count'] = len(exif)
                exif_dict = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
                if 'EXIF DateTimeOriginal' in exif_dict and 'EXIF DateTimeDigitized' in exif_dict:
                    feats['time_diff'] = 1
                software = str(exif_dict.get('Software', '')).lower()
                bad_softwares = ['photoshop', 'picsart', '美图', 'snapseed', 'lightroom']
                for bad in bad_softwares:
                    if bad in software:
                        hard_rule_tampered = True
                        suspicious_software = software
                        break
        except:
            pass

        img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
        if img is not None:
            kernel = np.array([[-1, -1, -1], [-1, 8, -1], [-1, -1, -1]])
            noise = cv2.filter2D(img.astype(np.float32), -1, kernel)
            feats['noise_std'] = float(np.std(noise))
            feats['noise_mean'] = float(np.mean(np.abs(noise)))
            feats['noise_skew'] = float(np.mean((noise - feats['noise_mean']) ** 3) / (feats['noise_std'] ** 3 + 1e-10))
            h, w = img.shape
            feats['size_per_pixel'] = float(os.path.getsize(image_path) / (h * w) if (h * w) > 0 else 0)

        img_color = cv2.imread(image_path)
        if img_color is not None:
            hist = cv2.calcHist([img_color], [0], None, [256], [0, 256])
            hist = hist / hist.sum()
            hist = hist[hist > 0]
            feats['color_entropy'] = float(-np.sum(hist * np.log2(hist)) if len(hist) > 0 else 0)

        return feats, hard_rule_tampered, suspicious_software

    def predict(self, image_path):
        feats, hard_rule, software = self.extract_features(image_path)
        if feats is None: return 0.0, False, ""
        if hard_rule: return 0.0, True, f"EXIF检测到修图软件: {software}"
        if self.model:
            prob = self.model.predict_proba(np.array([list(feats.values())]))[0][1]
            return float(prob), False, ""
        return 0.5, False, ""