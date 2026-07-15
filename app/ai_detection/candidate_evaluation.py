"""Evaluation gates for candidate image-detection models."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional


METRIC_NAMES = ("balanced_accuracy", "normal_recall", "tampered_recall")


def evaluate_regression_gate(
    predictions: Iterable[tuple[str, int, str]],
) -> Dict[str, Any]:
    failures = []
    total = 0
    for path, expected_label, actual in predictions:
        total += 1
        expected = "正常" if int(expected_label) == 0 else "篡改"
        if actual != expected:
            failures.append(
                {
                    "path": str(path),
                    "expected": expected,
                    "actual": actual,
                }
            )
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


def build_candidate_gates(
    *,
    regression_predictions: Iterable[tuple[str, int, str]],
    candidate_metrics: Optional[Dict[str, Any]],
    active_metrics: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    regression = evaluate_regression_gate(regression_predictions)
    metrics = compare_validation_metrics(candidate_metrics, active_metrics)
    return {
        "passed": bool(regression["passed"] and metrics["passed"]),
        "fixed_regression": regression,
        "validation_metrics": metrics,
    }


def fixed_regression_samples(base_dir: str | Path, pptest_dir: Optional[str | Path] = None):
    base = Path(base_dir)
    for path in sorted(base.glob("*")):
        if not path.is_file() or path.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp"}:
            continue
        name = path.stem.lower()
        if name.startswith("no"):
            yield path, 0
        elif name.startswith("p"):
            yield path, 1
    if pptest_dir:
        for path in sorted(Path(pptest_dir).glob("*")):
            if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
                yield path, 1
