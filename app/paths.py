"""项目路径常量（单一来源，避免各模块对「项目根」推断不一致）。"""
from pathlib import Path

# app/paths.py → 上级目录为项目根
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
