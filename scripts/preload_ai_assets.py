from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ai_detection.runtime.easyocr_download_patch import patch_easyocr_download
from app.ai_detection.workflows.inference_v3 import InferenceEngineAPI
from app.ai_detection.runtime.assets import (
    EASYOCR_DOWNLOAD_ENABLED,
    EASYOCR_MODEL_DIR,
    TORCH_HOME_DIR,
    get_easyocr_reader_kwargs,
)


def main() -> None:
    import easyocr
    import torch

    patch_easyocr_download()

    print(f"EASYOCR_MODEL_DIR={EASYOCR_MODEL_DIR}")
    print(f"TORCH_HOME={TORCH_HOME_DIR}")
    print(f"AI_EASYOCR_DOWNLOAD_ENABLED={int(EASYOCR_DOWNLOAD_ENABLED)}")
    mp = os.getenv("EASYOCR_GITHUB_MIRROR", "").strip()
    if mp:
        print(f"EASYOCR_GITHUB_MIRROR={mp}")

    reader = easyocr.Reader(
        ["ch_sim", "en"],
        **get_easyocr_reader_kwargs(gpu=False),
    )

    # 与线上一致：引擎与预热脚本只保留一份 EasyOCR Reader
    engine = InferenceEngineAPI("config.yaml", shared_ocr_reader=reader)
    print(f"engine_ready={engine.__class__.__name__}")
    print(f"torch_cuda_available={torch.cuda.is_available()}")


if __name__ == "__main__":
    main()
