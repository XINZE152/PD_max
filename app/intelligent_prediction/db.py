"""智能预测模块：SQLAlchemy 2.0 异步 MySQL 连接（与主库 MYSQL_* 一致）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.intelligent_prediction.settings import settings

_engine = None
AsyncSessionLocal: async_sessionmaker[AsyncSession] | None = None


def get_prediction_session_factory() -> async_sessionmaker[AsyncSession]:
    """取得已初始化的 async_sessionmaker（供预测服务、Celery 任务使用）。"""
    _ensure_engine()
    assert AsyncSessionLocal is not None
    return AsyncSessionLocal


def _ensure_engine() -> None:
    """延迟创建引擎（避免未配置 URL 时 import 失败）。"""
    global _engine, AsyncSessionLocal
    if _engine is not None and AsyncSessionLocal is not None:
        return
    url = (settings.prediction_async_db_url or "").strip()
    if not url:
        raise RuntimeError(
            "未配置智能预测异步数据库 URL：请设置 PREDICTION_ASYNC_DATABASE_URL，"
            "或确保 MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE 已填写以自动组装 mysql+aiomysql。"
        )
    _engine = create_async_engine(url, echo=False, pool_pre_ping=True)
    AsyncSessionLocal = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


async def get_prediction_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI 依赖：智能预测专用 Session。"""
    _ensure_engine()
    assert AsyncSessionLocal is not None
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
