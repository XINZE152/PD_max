"""Stable paths for image-detection runtime assets and annotations."""
from __future__ import annotations

from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def resolve_config_path(config_path: str | Path = "config.yaml") -> Path:
    path = Path(config_path)
    return path if path.is_absolute() else PACKAGE_ROOT / path


def annotation_root(path: str | Path) -> Path:
    root = Path(path)
    return root / "annotations" if (root / "annotations").is_dir() else root


def legacy_annotation_dir(path: str | Path) -> Path:
    root = annotation_root(path)
    return root / "legacy" if (root / "legacy").is_dir() else root

