from __future__ import annotations

import os
import sys
from typing import Dict


AI_DETECTION_RESOURCE_DEFAULTS: Dict[str, str] = {
    "MALLOC_ARENA_MAX": "2",
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "VECLIB_MAXIMUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "TORCH_NUM_THREADS": "1",
    "TORCH_NUM_INTEROP_THREADS": "1",
    "OMP_WAIT_POLICY": "PASSIVE",
}


def apply_ai_detection_resource_defaults() -> Dict[str, str]:
    """Set conservative native-library defaults before cv2/numpy/torch are imported."""
    applied: Dict[str, str] = {}
    for key, value in AI_DETECTION_RESOURCE_DEFAULTS.items():
        if os.environ.get(key):
            continue
        os.environ[key] = value
        applied[key] = value
    return applied


def configure_loaded_cv2() -> None:
    """Reduce OpenCV worker threads after cv2 has been imported."""
    try:
        import cv2

        threads = int(os.getenv("OPENCV_NUM_THREADS", "1") or "1")
        cv2.setNumThreads(max(1, threads))
        try:
            cv2.ocl.setUseOpenCL(False)
        except Exception:
            pass
    except Exception:
        pass


def trim_native_memory() -> bool:
    """Ask glibc to return free native heap pages to the OS after long OCR/model work."""
    if not sys.platform.startswith("linux"):
        return False
    try:
        import ctypes

        libc = ctypes.CDLL("libc.so.6")
        return int(libc.malloc_trim(0)) == 1
    except Exception:
        return False
