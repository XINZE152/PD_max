"""智能预测模块操作审计（查询）。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class OperationAuditItem(BaseModel):
    """单条审计记录。"""

    model_config = ConfigDict(from_attributes=True)

    id: int
    user_id: Optional[int] = None
    user_label: Optional[str] = None
    action: str
    resource: Optional[str] = None
    detail: Optional[dict[str, Any]] = None
    client_ip: Optional[str] = None
    created_at: datetime


class OperationAuditListResponse(BaseModel):
    """分页审计列表。"""

    total: int
    page: int
    page_size: int
    items: list[OperationAuditItem]
