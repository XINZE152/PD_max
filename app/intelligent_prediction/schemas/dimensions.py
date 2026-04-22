"""筛选维度：大区经理、仓库、冶炼厂去重列表。"""

from __future__ import annotations

from pydantic import BaseModel, Field


class DimensionListsResponse(BaseModel):
    """去重、排序后的字符串列表（不含仅空白）。"""

    regional_managers: list[str] = Field(
        default_factory=list,
        description="大区经理（来自送货历史或预测结果表，视接口而定）",
    )
    warehouses: list[str] = Field(default_factory=list, description="仓库")
    smelters: list[str] = Field(
        default_factory=list,
        description="冶炼厂（仅含历史或结果中非空的冶炼厂名）",
    )
