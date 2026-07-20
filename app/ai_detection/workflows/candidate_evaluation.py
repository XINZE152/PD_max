"""Evaluation gates for candidate image-detection models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


METRIC_NAMES = ("balanced_accuracy", "normal_recall", "tampered_recall")
MINIMUM_HOLDOUT_METRIC = 0.90
MINIMUM_ROI_COVERAGE = 0.90


def evaluate_regression_gate(predictions: Iterable[tuple[str, int, str]]) -> Dict[str, Any]:
    failures = []
    total = 0
    for path, expected_label, actual in predictions:
        total += 1
        expected = "正常" if int(expected_label) == 0 else "篡改"
        if actual != expected:
            failures.append({"path": str(path), "expected": expected, "actual": actual})
    return {"passed": not failures, "total": total, "failures": failures}


def compare_validation_metrics(
    candidate: Optional[Dict[str, Any]],
    active: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    candidate = candidate or {}
    active = active or {}
    if not candidate.get("available"):
        return {"passed": False, "reason": "候选模型缺少可用的分组验证指标", "comparisons": {}}
    comparisons: Dict[str, Any] = {}
    passed = True
    for name in METRIC_NAMES:
        current_value = active.get(name)
        candidate_value = candidate.get(name)
        metric_passed = candidate_value is not None and (
            current_value is None or float(candidate_value) >= float(current_value)
        )
        comparisons[name] = {
            "candidate": candidate_value,
            "active": current_value,
            "passed": metric_passed,
        }
        passed = passed and metric_passed
    return {"passed": passed, "comparisons": comparisons}


def evaluate_holdout_metrics(metrics: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    metrics = metrics or {}
    if not metrics.get("available"):
        return {"passed": False, "reason": "候选模型缺少独立原图测试指标", "comparisons": {}}
    comparisons = {}
    passed = True
    for name in METRIC_NAMES:
        value = metrics.get(name)
        metric_passed = value is not None and float(value) >= MINIMUM_HOLDOUT_METRIC
        comparisons[name] = {
            "candidate": value,
            "minimum": MINIMUM_HOLDOUT_METRIC,
            "passed": metric_passed,
        }
        passed = passed and metric_passed
    return {"passed": passed, "comparisons": comparisons}


def evaluate_roi_coverage(coverage: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    coverage = coverage or {}
    value = coverage.get("coverage")
    passed = value is not None and float(value) >= MINIMUM_ROI_COVERAGE
    return {
        "passed": passed,
        "coverage": value,
        "minimum": MINIMUM_ROI_COVERAGE,
        "sample_count": int(coverage.get("sample_count") or 0),
        "recognized_count": int(coverage.get("recognized_count") or 0),
    }


def build_candidate_gates(
    *,
    regression_predictions: Iterable[tuple[str, int, str]],
    candidate_metrics: Optional[Dict[str, Any]],
    active_metrics: Optional[Dict[str, Any]],
    training_replay_predictions: Iterable[tuple[str, int, str]] = (),
    holdout_metrics: Optional[Dict[str, Any]] = None,
    roi_coverage: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    regression = evaluate_regression_gate(regression_predictions)
    replay = evaluate_regression_gate(training_replay_predictions)
    validation = compare_validation_metrics(candidate_metrics, active_metrics)
    holdout = evaluate_holdout_metrics(holdout_metrics)
    coverage = evaluate_roi_coverage(roi_coverage)
    return {
        "passed": bool(
            regression["passed"]
            and replay["passed"]
            and validation["passed"]
            and holdout["passed"]
            and coverage["passed"]
        ),
        "fixed_regression": regression,
        "training_replay_regression": replay,
        "validation_metrics": validation,
        "holdout_metrics": holdout,
        "roi_coverage": coverage,
    }


def _manifest_samples(base: Path, marker: str):
    manifest_path = base / "dataset_manifest.json"
    if not manifest_path.is_file():
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    seen = set()
    for row in manifest.get("entries", []):
        if not row.get(marker) or row.get("is_derived"):
            continue
        path = (base / str(row.get("path") or "")).resolve()
        if path.is_file() and path not in seen:
            seen.add(path)
            yield path, int(row.get("label", 1))


def fixed_regression_samples(base_dir: str | Path, _pptest_dir: Optional[str | Path] = None):
    base = Path(base_dir)
    manifest_path = base / "dataset_manifest.json"
    if manifest_path.is_file():
        yield from _manifest_samples(base, "fixed_regression")
        return
    for class_name, label in (("normal", 0), ("tampered", 1)):
        class_dir = base / class_name
        if class_dir.is_dir():
            for path in sorted(class_dir.glob("*")):
                if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
                    yield path, label


def training_replay_samples(base_dir: str | Path):
    yield from _manifest_samples(Path(base_dir), "training_replay_regression")


def holdout_samples(base_dir: str | Path):
    base = Path(base_dir)
    manifest_path = base / "dataset_manifest.json"
    if not manifest_path.is_file():
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    seen = set()
    for row in manifest.get("entries", []):
        if row.get("is_derived") or row.get("split") != "test":
            continue
        path = (base / str(row.get("path") or "")).resolve()
        if path.is_file() and path not in seen:
            seen.add(path)
            yield path, int(row.get("label", 1))
