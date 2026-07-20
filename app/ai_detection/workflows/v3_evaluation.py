"""End-to-end evaluator for the production v3 OCR and ROI inference path."""
from __future__ import annotations

import csv
import hashlib
import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import cv2
import numpy as np

from app.ai_detection.core.amount_candidates import build_amount_candidates, detect_certificate_document_override
from app.ai_detection.core.ocr_utils import build_key_field_rois_from_tokens, run_full_image_ocr


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
RESULT_ORDER = {"篡改": 3, "可疑": 2, "正常": 1, "无法自动检测": 0, "错误": -1}


def _now_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _select_top(rows: Sequence[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not rows:
        return None
    return max(
        rows,
        key=lambda row: (RESULT_ORDER.get(str(row.get("result")), -1), float(row.get("confidence") or 0.0)),
    )


def _dedupe_rois(rois: Sequence[dict[str, Any]], threshold: float = 0.85) -> list[dict[str, Any]]:
    def iou(left: Sequence[int], right: Sequence[int]) -> float:
        x1, y1 = max(left[0], right[0]), max(left[1], right[1])
        x2, y2 = min(left[2], right[2]), min(left[3], right[3])
        overlap = max(0, x2 - x1) * max(0, y2 - y1)
        if not overlap:
            return 0.0
        area_left = max(1, (left[2] - left[0]) * (left[3] - left[1]))
        area_right = max(1, (right[2] - right[0]) * (right[3] - right[1]))
        return overlap / max(1, area_left + area_right - overlap)

    result: list[dict[str, Any]] = []
    for roi in sorted(rois, key=lambda item: (item["bbox"][2] - item["bbox"][0]) * (item["bbox"][3] - item["bbox"][1]), reverse=True):
        bbox = [int(value) for value in roi.get("bbox", [])[:4]]
        if len(bbox) != 4 or bbox[2] <= bbox[0] or bbox[3] <= bbox[1]:
            continue
        if any(iou(bbox, existing["bbox"]) >= threshold for existing in result):
            continue
        result.append({**roi, "bbox": bbox})
    return result


class ProductionV3Evaluator:
    """Run the same OCR -> key ROI -> v3 aggregate sequence used by auto detection."""

    def __init__(self, engine: Any, ocr_reader: Any):
        self.engine = engine
        self.ocr_reader = ocr_reader

    def evaluate_image(self, image_path: str | Path) -> dict[str, Any]:
        path = Path(image_path)
        started_at = time.perf_counter()
        image, tokens = run_full_image_ocr(str(path), self.ocr_reader)
        if image is None:
            return self._empty(path, "错误", "无法读取图片或路径不存在", started_at)

        key_rois = _dedupe_rois(build_key_field_rois_from_tokens(tokens, image.shape))
        if not key_rois:
            return self._empty(path, "无法自动检测", "未识别到金额、姓名、时间关键区域，无法自动检测", started_at, tokens=tokens)

        detection_bboxes = [list(roi["bbox"]) for roi in key_rois]
        regions: list[dict[str, Any]] = []
        for roi in key_rois:
            raw = self.engine.predict(str(path), list(roi["bbox"]), "xyxy", detection_bboxes=detection_bboxes)
            item = json.loads(raw)
            if item.get("result") == "错误":
                continue
            item.update(
                {
                    "original_bbox": list(roi["bbox"]),
                    "field_type": roi.get("field_type"),
                    "field_label": roi.get("field_label"),
                }
            )
            regions.append(item)

        candidates = build_amount_candidates(tokens, image.shape)
        override = detect_certificate_document_override(
            image_path=path,
            image=image,
            tokens=tokens,
            candidates=candidates,
            ocr_reader=self.ocr_reader,
        )
        if override and not any(item.get("result") == "篡改" for item in regions):
            bbox = [int(value) for value in override["bbox_xyxy"]]
            regions.append(
                {
                    "result": override["result"],
                    "confidence": float(override["confidence"]),
                    "reason": override["reason"],
                    "bbox": [bbox[0], bbox[1], bbox[2] - bbox[0], bbox[3] - bbox[1]],
                    "original_bbox": bbox,
                    "field_type": "amount",
                    "field_label": "金额",
                    "source": override.get("source"),
                }
            )

        for number, item in enumerate(regions, start=1):
            item["region_no"] = number
        top = _select_top(regions)
        return {
            "image_path": str(path),
            "result": (top or {}).get("result", "无法自动检测"),
            "confidence": float((top or {}).get("confidence") or 0.0),
            "reason": str((top or {}).get("reason") or "未产生可用区域结果"),
            "regions": regions,
            "roi_count": len(key_rois),
            "ocr_token_count": len(tokens),
            "elapsed_ms": round((time.perf_counter() - started_at) * 1000.0, 1),
        }

    @staticmethod
    def _empty(path: Path, result: str, reason: str, started_at: float, *, tokens: Sequence[Any] = ()) -> dict[str, Any]:
        return {
            "image_path": str(path),
            "result": result,
            "confidence": 0.0,
            "reason": reason,
            "regions": [],
            "roi_count": 0,
            "ocr_token_count": len(tokens),
            "elapsed_ms": round((time.perf_counter() - started_at) * 1000.0, 1),
        }


def load_manifest_samples(image_root: str | Path) -> tuple[list[tuple[Path, int, str]], list[tuple[Path, int, str]]]:
    root = Path(image_root)
    manifest = json.loads((root / "dataset_manifest.json").read_text(encoding="utf-8"))
    seen: set[Path] = set()
    independent: list[tuple[Path, int, str]] = []
    replay: list[tuple[Path, int, str]] = []
    for row in manifest.get("entries", []):
        if row.get("is_derived"):
            continue
        path = (root / str(row.get("path") or "")).resolve()
        if not path.is_file() or path.suffix.lower() not in IMAGE_SUFFIXES or path in seen:
            continue
        seen.add(path)
        item = (path, int(row.get("label", 1)), str(row.get("split") or "unspecified"))
        if row.get("training_replay_regression"):
            replay.append(item)
        else:
            independent.append(item)
    return independent, replay


def run_v3_retest(
    evaluator: ProductionV3Evaluator,
    image_root: str | Path,
    output_root: str | Path,
    *,
    run_id: Optional[str] = None,
    model_version: Optional[str] = None,
) -> dict[str, Any]:
    run_id = run_id or _now_run_id()
    output_dir = Path(output_root) / run_id
    output_dir.mkdir(parents=True, exist_ok=False)
    independent, replay = load_manifest_samples(image_root)
    rows = [_evaluate_sample(evaluator, path, label, split, "independent") for path, label, split in independent]
    rows.extend(_evaluate_sample(evaluator, path, label, split, "training_replay") for path, label, split in replay)
    report = _write_result_package(
        output_dir,
        rows,
        model_version=model_version,
        run_id=run_id,
        engine=getattr(evaluator, "engine", None),
    )
    return {"output_dir": str(output_dir), **report}


def _evaluate_sample(evaluator: ProductionV3Evaluator, path: Path, label: int, split: str, cohort: str) -> dict[str, Any]:
    evaluation = evaluator.evaluate_image(path)
    expected = "正常" if label == 0 else "篡改"
    return {
        "filename": path.name,
        "image_path": str(path),
        "expected_label": label,
        "expected": expected,
        "split": split,
        "cohort": cohort,
        "strict_correct": evaluation["result"] == expected,
        **evaluation,
    }


def _metrics(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    items = list(rows)
    counts = Counter(str(item["result"]) for item in items)
    normal = [item for item in items if item["expected_label"] == 0]
    tampered = [item for item in items if item["expected_label"] == 1]
    recognized = sum(item["roi_count"] > 0 for item in items)
    return {
        "sample_count": len(items),
        "strict_correct": sum(bool(item["strict_correct"]) for item in items),
        "strict_accuracy": round(sum(bool(item["strict_correct"]) for item in items) / max(1, len(items)), 6),
        "normal_recall": round(sum(item["result"] == "正常" for item in normal) / max(1, len(normal)), 6),
        "tampered_recall": round(sum(item["result"] == "篡改" for item in tampered) / max(1, len(tampered)), 6),
        "result_counts": dict(counts),
        "suspicious_count": counts["可疑"],
        "unable_count": counts["无法自动检测"],
        "roi_coverage": round(recognized / max(1, len(items)), 6),
        "average_elapsed_ms": round(sum(float(item["elapsed_ms"]) for item in items) / max(1, len(items)), 2),
        "max_elapsed_ms": round(max((float(item["elapsed_ms"]) for item in items), default=0.0), 2),
        "risk": {
            "minimum": round(min((float(item["confidence"]) for item in items), default=0.0), 6),
            "maximum": round(max((float(item["confidence"]) for item in items), default=0.0), 6),
        },
    }


def _write_result_package(
    output_dir: Path,
    rows: list[dict[str, Any]],
    *,
    model_version: Optional[str],
    run_id: str,
    engine: Any = None,
) -> dict[str, Any]:
    independent = [row for row in rows if row["cohort"] == "independent"]
    replay = [row for row in rows if row["cohort"] == "training_replay"]
    metrics = {"independent": _metrics(independent), "training_replay": _metrics(replay)}
    thresholds = _decision_thresholds(engine)
    metrics["decision_thresholds"] = thresholds
    metrics["training_replay_details"] = [_replay_detail(row, thresholds) for row in replay]
    metrics["production_model_version"] = model_version
    metrics_path = output_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = {"run_id": run_id, "created_at": datetime.now().isoformat(timespec="seconds"), "metrics": metrics}
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "results.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    columns = [
        "cohort", "split", "filename", "image_path", "expected", "result", "confidence", "strict_correct",
        "roi_count", "ocr_token_count", "elapsed_ms", "reason", "regions_json",
        "distance_to_suspicious", "distance_to_tampered",
    ]
    with (output_dir / "per_image_results.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            csv_row = {key: row.get(key) for key in columns}
            csv_row["regions_json"] = json.dumps(row.get("regions") or [], ensure_ascii=False)
            confidence = float(row.get("confidence") or 0.0)
            csv_row["distance_to_suspicious"] = round(confidence - thresholds["suspicious"], 6)
            csv_row["distance_to_tampered"] = round(confidence - thresholds["tampered"], 6)
            writer.writerow(csv_row)

    annotated = output_dir / "annotated"
    annotated.mkdir(exist_ok=True)
    failures = [row for row in rows if not row["strict_correct"]]
    samples = failures + [row for cohort in ("independent", "training_replay") for row in rows if row["cohort"] == cohort][:5]
    selected: dict[str, dict[str, Any]] = {row["image_path"]: row for row in samples}
    for row in selected.values():
        _draw_annotation(row, annotated / f"{Path(row['filename']).stem}__{row['cohort']}.jpg")

    _write_charts(output_dir, rows, metrics)
    report = _markdown_report(metrics, failures)
    (output_dir / "report.md").write_text(report, encoding="utf-8")
    manifest_path = _write_package_manifest(output_dir)
    return {
        "metrics": metrics,
        "failure_count": len(failures),
        "report_path": str(output_dir / "report.md"),
        "manifest_path": str(manifest_path),
    }


def _draw_annotation(row: dict[str, Any], target: Path) -> None:
    image = cv2.imdecode(np.fromfile(row["image_path"], dtype=np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        return
    colors = {"篡改": (40, 40, 220), "可疑": (0, 170, 230), "正常": (40, 160, 50)}
    color = colors.get(str(row["result"]), (100, 100, 100))
    for region in row.get("regions") or []:
        bbox = region.get("original_bbox") or []
        if len(bbox) != 4:
            continue
        x1, y1, x2, y2 = [int(value) for value in bbox]
        cv2.rectangle(image, (x1, y1), (x2, y2), color, 2)
        label = f"#{region.get('region_no')} {region.get('field_type')} {float(region.get('confidence') or 0):.2f}"
        cv2.putText(image, label, (x1, max(18, y1 - 5)), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1, cv2.LINE_AA)
    cv2.putText(image, f"{row['expected']} -> {row['result']} {float(row['confidence']):.3f}", (10, 24), cv2.FONT_HERSHEY_SIMPLEX, 0.65, color, 2, cv2.LINE_AA)
    cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, 92])[1].tofile(str(target))


def _decision_thresholds(engine: Any) -> dict[str, float]:
    """Record production v3 thresholds without imposing a separate evaluator policy."""
    config = getattr(engine, "config", {}) if engine is not None else {}
    thresholds = config.get("thresholds", {}) if isinstance(config, dict) else {}
    return {
        "suspicious": float(thresholds.get("suspect_low", 0.50)),
        "tampered": float(thresholds.get("suspect_high", 0.65)),
    }


def _replay_detail(row: dict[str, Any], thresholds: dict[str, float]) -> dict[str, Any]:
    regions = []
    for region in row.get("regions") or []:
        risk = float(region.get("confidence") or 0.0)
        regions.append(
            {
                "region_no": region.get("region_no"),
                "field_type": region.get("field_type"),
                "field_label": region.get("field_label"),
                "risk": risk,
                "result": region.get("result"),
                "distance_to_suspicious": round(risk - thresholds["suspicious"], 6),
                "distance_to_tampered": round(risk - thresholds["tampered"], 6),
                "reason": region.get("reason"),
            }
        )
    final_risk = float(row.get("confidence") or 0.0)
    return {
        "filename": row.get("filename"),
        "expected": row.get("expected"),
        "result": row.get("result"),
        "final_risk": final_risk,
        "distance_to_suspicious": round(final_risk - thresholds["suspicious"], 6),
        "distance_to_tampered": round(final_risk - thresholds["tampered"], 6),
        "reason": row.get("reason"),
        "regions": regions,
    }


def _write_charts(output_dir: Path, rows: list[dict[str, Any]], metrics: dict[str, Any]) -> None:
    chart_dir = output_dir / "charts"
    chart_dir.mkdir(exist_ok=True)
    independent = [row for row in rows if row["cohort"] == "independent"]
    _draw_risk_distribution_chart(independent, chart_dir / "risk_distribution.png")
    _draw_outcome_counts_chart(metrics["independent"]["result_counts"], chart_dir / "outcome_counts.png")


def _chart_canvas(title: str, width: int = 980, height: int = 560) -> np.ndarray:
    canvas = np.full((height, width, 3), 255, dtype=np.uint8)
    cv2.putText(canvas, title, (48, 44), cv2.FONT_HERSHEY_SIMPLEX, 0.82, (35, 42, 48), 2, cv2.LINE_AA)
    return canvas


def _draw_risk_distribution_chart(rows: Sequence[dict[str, Any]], target: Path) -> None:
    canvas = _chart_canvas("Production v3 risk distribution")
    left, right, top, bottom = 78, 930, 92, 478
    cv2.line(canvas, (left, bottom), (right, bottom), (80, 88, 94), 1, cv2.LINE_AA)
    cv2.line(canvas, (left, top), (left, bottom), (80, 88, 94), 1, cv2.LINE_AA)
    bins = 20
    normal = np.histogram([float(row["confidence"]) for row in rows if row["expected_label"] == 0], bins=bins, range=(0.0, 1.0))[0]
    tampered = np.histogram([float(row["confidence"]) for row in rows if row["expected_label"] == 1], bins=bins, range=(0.0, 1.0))[0]
    max_count = max(1, int(max(normal.max(initial=0), tampered.max(initial=0))))
    group_width = (right - left) / bins
    for index in range(bins):
        x1 = int(left + index * group_width + 2)
        mid = int(left + (index + 0.5) * group_width)
        x2 = int(left + (index + 1) * group_width - 2)
        normal_height = int((bottom - top) * int(normal[index]) / max_count)
        tampered_height = int((bottom - top) * int(tampered[index]) / max_count)
        cv2.rectangle(canvas, (x1, bottom - normal_height), (mid - 1, bottom), (73, 143, 59), -1)
        cv2.rectangle(canvas, (mid + 1, bottom - tampered_height), (x2, bottom), (77, 74, 195), -1)
    for risk, color, label in ((0.55, (0, 156, 202), "suspicious 0.55"), (0.65, (56, 70, 188), "tampered 0.65")):
        x = int(left + (right - left) * risk)
        cv2.line(canvas, (x, top), (x, bottom), color, 2, cv2.LINE_AA)
        cv2.putText(canvas, label, (min(x + 5, right - 150), top + 22), cv2.FONT_HERSHEY_SIMPLEX, 0.46, color, 1, cv2.LINE_AA)
    for tick in range(0, 6):
        risk = tick / 5
        x = int(left + (right - left) * risk)
        cv2.line(canvas, (x, bottom), (x, bottom + 6), (80, 88, 94), 1, cv2.LINE_AA)
        cv2.putText(canvas, f"{risk:.1f}", (x - 12, bottom + 27), cv2.FONT_HERSHEY_SIMPLEX, 0.44, (80, 88, 94), 1, cv2.LINE_AA)
    cv2.putText(canvas, f"count (max {max_count})", (left, top - 12), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (80, 88, 94), 1, cv2.LINE_AA)
    cv2.rectangle(canvas, (690, 505), (706, 519), (73, 143, 59), -1)
    cv2.putText(canvas, "normal", (714, 518), cv2.FONT_HERSHEY_SIMPLEX, 0.47, (55, 60, 65), 1, cv2.LINE_AA)
    cv2.rectangle(canvas, (805, 505), (821, 519), (77, 74, 195), -1)
    cv2.putText(canvas, "tampered", (829, 518), cv2.FONT_HERSHEY_SIMPLEX, 0.47, (55, 60, 65), 1, cv2.LINE_AA)
    cv2.imencode(".png", canvas)[1].tofile(str(target))


def _draw_outcome_counts_chart(outcomes: dict[str, int], target: Path) -> None:
    canvas = _chart_canvas("Production v3 final outcomes")
    labels = [("normal", "正常"), ("tampered", "篡改"), ("suspicious", "可疑"), ("unable", "无法自动检测"), ("error", "错误")]
    colors = [(73, 143, 59), (77, 74, 195), (0, 171, 230), (115, 115, 115), (70, 70, 70)]
    left, right, top, bottom = 78, 930, 100, 462
    cv2.line(canvas, (left, bottom), (right, bottom), (80, 88, 94), 1, cv2.LINE_AA)
    max_count = max(1, max(outcomes.values(), default=0))
    bar_width = 110
    gap = 54
    for index, ((english, chinese), color) in enumerate(zip(labels, colors)):
        count = int(outcomes.get(chinese, 0))
        x1 = left + index * (bar_width + gap) + 26
        x2 = x1 + bar_width
        height = int((bottom - top) * count / max_count)
        cv2.rectangle(canvas, (x1, bottom - height), (x2, bottom), color, -1)
        cv2.putText(canvas, str(count), (x1 + 42, max(top + 18, bottom - height - 10)), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (45, 50, 55), 1, cv2.LINE_AA)
        cv2.putText(canvas, english, (x1, bottom + 28), cv2.FONT_HERSHEY_SIMPLEX, 0.43, (80, 88, 94), 1, cv2.LINE_AA)
    cv2.putText(canvas, f"maximum count: {max_count}", (left, top - 14), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (80, 88, 94), 1, cv2.LINE_AA)
    cv2.imencode(".png", canvas)[1].tofile(str(target))


def _write_package_manifest(output_dir: Path) -> Path:
    entries = []
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        entries.append(
            {
                "path": str(path.relative_to(output_dir)),
                "sha256": digest.hexdigest(),
                "size_bytes": path.stat().st_size,
            }
        )
    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "files": entries,
    }
    path = output_dir / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _markdown_report(metrics: dict[str, Any], failures: Sequence[dict[str, Any]]) -> str:
    independent = metrics["independent"]
    replay = metrics["training_replay"]
    lines = [
        "# v3 Production Retest",
        "",
        f"- Independent: {independent['strict_correct']}/{independent['sample_count']} ({independent['strict_accuracy']:.1%})",
        f"- Normal recall: {independent['normal_recall']:.1%}",
        f"- Tampered recall: {independent['tampered_recall']:.1%}",
        f"- ROI coverage: {independent['roi_coverage']:.1%}",
        f"- Production thresholds: suspicious {metrics['decision_thresholds']['suspicious']:.2f}, tampered {metrics['decision_thresholds']['tampered']:.2f}",
        f"- Replay: {replay['strict_correct']}/{replay['sample_count']} ({replay['strict_accuracy']:.1%})",
        "",
        "## Failures",
    ]
    for row in failures:
        lines.append(f"- `{row['filename']}`: expected {row['expected']}, got {row['result']} ({row['confidence']:.4f}); {row['reason']}")
    lines.extend(["", "## Training Replay Details"])
    for item in metrics["training_replay_details"]:
        lines.append(
            f"- `{item['filename']}`: expected {item['expected']}, got {item['result']} "
            f"(risk {item['final_risk']:.4f}; to suspicious {item['distance_to_suspicious']:+.4f}; "
            f"to tampered {item['distance_to_tampered']:+.4f})"
        )
        for region in item["regions"]:
            lines.append(
                f"  - region #{region['region_no']} {region['field_label'] or region['field_type']}: "
                f"{region['result']} {region['risk']:.4f}; to suspicious {region['distance_to_suspicious']:+.4f}; "
                f"to tampered {region['distance_to_tampered']:+.4f}"
            )
    return "\n".join(lines) + "\n"
