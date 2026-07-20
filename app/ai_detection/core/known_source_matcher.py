"""Reference-assisted evidence for localized edits of known source images."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import cv2
import numpy as np


def image_phash_hex(image: np.ndarray) -> str:
    """Return a compact perceptual hash used to shortlist source references."""
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY) if image.ndim == 3 else image
    resized = cv2.resize(gray, (32, 32), interpolation=cv2.INTER_AREA)
    coefficients = cv2.dct(np.float32(resized))[:8, :8]
    median = float(np.median(coefficients.flatten()[1:]))
    bits = (coefficients > median).flatten()
    return f"{int(''.join('1' if bit else '0' for bit in bits), 2):016x}"


def _phash_distance(left: str, right: str) -> int:
    try:
        return (int(left, 16) ^ int(right, 16)).bit_count()
    except ValueError:
        return 64


class KnownSourcePairMatcher:
    """Detect a localized mutation of an indexed normal source image."""

    def __init__(self, index: Dict[str, Any], *, image_root: str | Path):
        self.image_root = Path(image_root).resolve()
        self.references = [
            row for row in index.get("references", [])
            if isinstance(row, dict) and row.get("path") and row.get("phash")
        ]
        self._cache: Dict[str, Optional[Dict[str, Any]]] = {}

    @classmethod
    def from_file(
        cls,
        index_path: str | Path | None,
        *,
        image_root: str | Path,
    ) -> Optional["KnownSourcePairMatcher"]:
        if not index_path:
            return None
        try:
            data = json.loads(Path(index_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(data, dict) or not data.get("references"):
            return None
        return cls(data, image_root=image_root)

    @staticmethod
    def _intersects(left: Sequence[int], right: Sequence[int]) -> bool:
        return not (
            int(left[2]) <= int(right[0])
            or int(right[2]) <= int(left[0])
            or int(left[3]) <= int(right[1])
            or int(right[3]) <= int(left[1])
        )

    def _find_match(self, image: np.ndarray) -> Optional[Dict[str, Any]]:
        if image is None or image.size == 0:
            return None
        height, width = image.shape[:2]
        candidate_hash = image_phash_hex(image)
        candidate_gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

        for reference in self.references:
            if int(reference.get("width") or 0) != width or int(reference.get("height") or 0) != height:
                continue
            if _phash_distance(candidate_hash, str(reference["phash"])) > 2:
                continue

            reference_path = self.image_root / str(reference["path"])
            reference_color = cv2.imdecode(
                np.fromfile(reference_path, dtype=np.uint8),
                cv2.IMREAD_COLOR,
            )
            if reference_color is None or reference_color.shape[:2] != candidate_gray.shape:
                continue
            reference_image = cv2.cvtColor(reference_color, cv2.COLOR_BGR2GRAY)

            difference = cv2.absdiff(candidate_gray, reference_image)
            changed = difference > 20
            changed_count = int(np.count_nonzero(changed))
            changed_fraction = changed_count / float(changed.size)
            if not (
                10 <= changed_count
                and changed_fraction <= 0.002
                and float(np.mean(difference)) <= 2.0
            ):
                continue

            ys, xs = np.where(changed)
            if not len(xs) or not len(ys):
                continue
            return {
                "reference_path": str(reference["path"]),
                "changed_bbox": [
                    int(xs.min()),
                    int(ys.min()),
                    int(xs.max()) + 1,
                    int(ys.max()) + 1,
                ],
                "changed_fraction": round(changed_fraction, 8),
            }
        return None

    def match(
        self,
        image: np.ndarray,
        roi_bbox_xyxy: Sequence[int],
        *,
        cache_key: str,
    ) -> Optional[Dict[str, Any]]:
        if cache_key not in self._cache:
            self._cache[cache_key] = self._find_match(image)
        evidence = self._cache[cache_key]
        if not evidence or not self._intersects(evidence["changed_bbox"], roi_bbox_xyxy):
            return None
        return dict(evidence)
