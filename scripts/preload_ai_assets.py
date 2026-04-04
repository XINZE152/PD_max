from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ai_detection.inference_api import InferenceEngineAPI
from app.ai_detection.runtime_assets import (
    EASYOCR_DOWNLOAD_ENABLED,
    EASYOCR_MODEL_DIR,
    TORCH_HOME_DIR,
    get_easyocr_reader_kwargs,
)


def main() -> None:
    import easyocr
    import torch

    print(f"EASYOCR_MODEL_DIR={EASYOCR_MODEL_DIR}")
    print(f"TORCH_HOME={TORCH_HOME_DIR}")
    print(f"AI_EASYOCR_DOWNLOAD_ENABLED={int(EASYOCR_DOWNLOAD_ENABLED)}")

    # 先单独预热 EasyOCR 自动找框用到的模型。
    easyocr.Reader(
        ["ch_sim", "en"],
        **get_easyocr_reader_kwargs(gpu=False),
    )

    # 再实例化推理引擎，顺带预热 FeatureExtractor 里的 EasyOCR 与 ResNet 权重。
    engine = InferenceEngineAPI("config.yaml")
    print(f"engine_ready={engine.__class__.__name__}")
    print(f"torch_cuda_available={torch.cuda.is_available()}")


if __name__ == "__main__":
    main()
