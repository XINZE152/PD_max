#!/usr/bin/env python3
"""Create auditable v4 key-field ROI sidecars.

The command only runs OCR and the shared key-field ROI builder.  It never
infers a label from a filename and never changes image bytes.  Existing
sidecars are preserved unless ``--force`` is supplied, so the command can be
stopped and resumed safely on a CPU-only machine.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import cv2
import numpy as np

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ai_detection.workflows.forensic_v4 import resolve_roi_sidecar, v4_sidecar_relative_path


def _configure_cpu() -> None:
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    os.environ.setdefault("OMP_NUM_THREADS", "2")
    os.environ.setdefault("MKL_NUM_THREADS", "2")
    try:
        import torch

        torch.set_num_threads(2)
        torch.set_num_interop_threads(1)
    except (ImportError, RuntimeError):
        pass


def _load_image(path: Path) -> np.ndarray:
    image = cv2.imdecode(np.fromfile(str(path), dtype=np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        raise RuntimeError(f"图片无法读取: {path}")
    return image


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def _ocr_rois(image: np.ndarray, reader: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    from app.ai_detection.core.amount_candidates import tokenize_ocr_results
    from app.ai_detection.core.ocr_utils import _resize_for_ocr, _scale_ocr_results_to_original
    from app.ai_detection.core.rule_check_roi import find_key_field_rois

    work, scale = _resize_for_ocr(
        image,
        max_side=1800,
        max_pixels=2_500_000,
        min_short_side=1100,
    )
    gray = cv2.cvtColor(work, cv2.COLOR_BGR2GRAY)
    raw = reader.readtext(
        gray,
        detail=1,
        paragraph=False,
        adjust_contrast=0.5,
        mag_ratio=1.5,
        text_threshold=0.25,
    )
    original = _scale_ocr_results_to_original(raw, scale=scale, original_shape=image.shape)
    tokens = tokenize_ocr_results(original)
    ocr_tokens = [
        {
            "text": str(token.text or ""),
            "confidence": float(token.conf),
            "bbox": [int(value) for value in token.bbox],
        }
        for token in tokens
    ]
    return find_key_field_rois(tokens, image.shape), ocr_tokens


def prepare(
    image_root: Path,
    manifest_path: Path,
    locate_root: Path,
    *,
    force: bool = False,
    only_split: str | None = None,
) -> dict[str, Any]:
    _configure_cpu()
    import easyocr

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = manifest.get("entries")
    if not isinstance(rows, list):
        raise ValueError("数据清单缺少 entries")

    reader = easyocr.Reader(
        ["ch_sim", "en"],
        gpu=False,
        model_storage_directory=os.getenv("AI_EASYOCR_MODEL_DIR", ".cache/easyocr"),
        download_enabled=False,
        verbose=False,
    )
    counts = {"processed": 0, "skipped": 0, "failed": 0, "roi_count": 0}
    failures: list[dict[str, str]] = []
    started_at = time.time()
    for row in rows:
        if row.get("is_derived"):
            continue
        split = str(row.get("split") or "")
        if only_split and split != only_split:
            continue
        relative_path = str(row.get("path") or "")
        image_path = image_root / relative_path
        existing_sidecar = resolve_roi_sidecar(image_path, manifest, row, locate_root)
        sidecar_relative = v4_sidecar_relative_path(relative_path)
        sidecar_path = locate_root / sidecar_relative
        if existing_sidecar is not None and not force:
            row["roi_sidecar"] = str(existing_sidecar.relative_to(locate_root).as_posix())
            counts["skipped"] += 1
            continue
        try:
            image = _load_image(image_path)
            rois, ocr_tokens = _ocr_rois(image, reader)
            key_regions = []
            for roi in rois:
                bbox = [int(value) for value in roi["bbox"]]
                key_regions.append(
                    {
                        "text": str(roi.get("label") or ""),
                        "confidence": 0.0,
                        "bbox": bbox,
                        "type": str(roi["field_type"]),
                        "field_type": str(roi["field_type"]),
                        "field_label": str(roi.get("field_label") or ""),
                        "source": str(roi.get("source") or "ocr"),
                        "is_tampered": False,
                    }
                )
            _write_json(
                sidecar_path,
                {
                    "image_path": relative_path,
                    "relative_image_path": relative_path,
                    "width": int(image.shape[1]),
                    "height": int(image.shape[0]),
                    "key_regions": key_regions,
                    "ocr_tokens": ocr_tokens,
                    "ocr_token_count": len(ocr_tokens),
                    "ocr_success": bool(ocr_tokens),
                    "schema_version": 2,
                    "generated_by": "prepare_forensic_v4_rois",
                },
            )
            row["roi_sidecar"] = sidecar_relative
            counts["processed"] += 1
            counts["roi_count"] += len(key_regions)
            print(
                f"PROCESSED {relative_path} rois={len(key_regions)} "
                f"progress={counts['processed']}/{counts['processed'] + counts['skipped'] + counts['failed']}",
                flush=True,
            )
        except Exception as exc:
            counts["failed"] += 1
            failures.append({"path": relative_path, "error": f"{type(exc).__name__}: {exc}"})
            print(f"FAILED {relative_path}: {failures[-1]['error']}", file=sys.stderr)

    manifest_path_tmp = manifest_path.with_name(f".{manifest_path.name}.tmp")
    manifest_path_tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(manifest_path_tmp, manifest_path)
    return {
        "counts": counts,
        "failures": failures,
        "elapsed_seconds": round(time.time() - started_at, 2),
        "locate_root": str(locate_root),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-root", default="app/ai_detection/images")
    parser.add_argument("--manifest", default="app/ai_detection/images/dataset_manifest.json")
    parser.add_argument("--locate-root", default="app/ai_detection/locate_json")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--only-split", choices=("train", "validation", "test"))
    args = parser.parse_args()
    result = prepare(
        Path(args.image_root),
        Path(args.manifest),
        Path(args.locate_root),
        force=args.force,
        only_split=args.only_split,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["counts"]["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
