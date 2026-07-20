from __future__ import annotations

import os
from pathlib import Path

from app.paths import PROJECT_ROOT


def _truthy_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _resolve_dir(name: str, default_relative: str) -> str:
    raw = (os.getenv(name) or default_relative).strip() or default_relative
    path = Path(raw)
    resolved = path if path.is_absolute() else (PROJECT_ROOT / path)
    resolved.mkdir(parents=True, exist_ok=True)
    return str(resolved)


EASYOCR_MODEL_DIR = _resolve_dir("AI_EASYOCR_MODEL_DIR", ".cache/easyocr")
TORCH_HOME_DIR = _resolve_dir("TORCH_HOME", ".cache/torch")
EASYOCR_DOWNLOAD_ENABLED = _truthy_env("AI_EASYOCR_DOWNLOAD_ENABLED", True)

# torchvision / torch hub 会读取这个环境变量决定权重缓存目录。
os.environ.setdefault("TORCH_HOME", TORCH_HOME_DIR)


def get_easyocr_reader_kwargs(*, gpu: bool, verbose: bool = True) -> dict:
    return {
        "gpu": gpu,
        "model_storage_directory": EASYOCR_MODEL_DIR,
        "download_enabled": EASYOCR_DOWNLOAD_ENABLED,
        "verbose": verbose,
    }
