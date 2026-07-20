"""Train an offline v4 candidate without changing the active v3 model."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ai_detection.workflows.forensic_v4 import run_offline_v4


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-root", default="app/ai_detection/images")
    parser.add_argument("--manifest", default="app/ai_detection/images/dataset_manifest.json")
    parser.add_argument("--locate-root", default="app/ai_detection/locate_json")
    parser.add_argument("--output", default="app/ai_detection/models/forensic_v4_candidate")
    parser.add_argument("--reviewed-root", default="app/ai_detection/feedback/reviewed")
    args = parser.parse_args()
    report = run_offline_v4(
        Path(args.image_root),
        Path(args.manifest),
        Path(args.output),
        Path(args.locate_root),
        Path(args.reviewed_root),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
