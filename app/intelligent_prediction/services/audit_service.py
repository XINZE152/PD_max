"""智能预测操作审计写入。"""

from __future__ import annotations

from typing import Any, Optional

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.api.audit_deps import AuditActor
from app.intelligent_prediction.db import get_prediction_session_factory
from app.intelligent_prediction.models import OperationAudit

logger = get_logger(__name__)


async def append_audit(
    session: AsyncSession,
    action: str,
    *,
    resource: Optional[str] = None,
    detail: Optional[dict[str, Any]] = None,
    actor: AuditActor,
) -> None:
    """与当前请求 Session 同一事务提交。"""
    session.add(
        OperationAudit(
            user_id=actor.user_id,
            user_label=actor.user_label or None,
            action=action,
            resource=resource,
            detail=detail,
            client_ip=actor.client_ip,
        )
    )


async def write_audit_standalone(
    action: str,
    *,
    resource: Optional[str] = None,
    detail: Optional[dict[str, Any]] = None,
    actor: AuditActor,
) -> None:
    """独立短事务，用于主业务已回滚时的审计（如导入校验失败）。"""
    try:
        factory = get_prediction_session_factory()
        async with factory() as session:
            session.add(
                OperationAudit(
                    user_id=actor.user_id,
                    user_label=actor.user_label or None,
                    action=action,
                    resource=resource,
                    detail=detail,
                    client_ip=actor.client_ip,
                )
            )
            await session.commit()
    except Exception:
        logger.warning("write_audit_standalone failed", exc_info=True)


async def list_audit_events(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
    action: str | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
) -> tuple[list[OperationAudit], int]:
    """分页查询操作审计（按创建时间倒序）。"""
    filters = []
    if action and action.strip():
        filters.append(OperationAudit.action == action.strip())
    if created_from is not None:
        filters.append(OperationAudit.created_at >= created_from)
    if created_to is not None:
        filters.append(OperationAudit.created_at <= created_to)

    count_stmt = select(func.count()).select_from(OperationAudit)
    stmt = select(OperationAudit)
    for f in filters:
        count_stmt = count_stmt.where(f)
        stmt = stmt.where(f)

    total_res = await session.execute(count_stmt)
    total = int(total_res.scalar_one())
    offset = (page - 1) * page_size
    stmt = stmt.order_by(OperationAudit.created_at.desc(), OperationAudit.id.desc())
    stmt = stmt.offset(offset).limit(page_size)
    res = await session.execute(stmt)
    return list(res.scalars().all()), total
