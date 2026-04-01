import os
import logging
import time

from fastapi import FastAPI, Request

from app.api.v1.router import api_router
from app.api.v1.routes.ai_detection import shutdown_ai_detection, startup_ai_detection
from app.database import create_tables, init_default_data
from app.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)
access_logger = logging.getLogger("app.access")

app = FastAPI(title="TL比价系统", version="1.0.0")

app.include_router(api_router)


@app.middleware("http")
async def log_http_requests(request: Request, call_next):
    start_time = time.perf_counter()
    client_host = request.client.host if request.client else "-"
    query = request.url.query or "-"
    user_agent = request.headers.get("user-agent", "-")

    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        access_logger.exception(
            "REQ %s %s query=%s client=%s ua=%s status=500 duration_ms=%.2f",
            request.method,
            request.url.path,
            query,
            client_host,
            user_agent,
            elapsed_ms,
        )
        raise

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    access_logger.info(
        "REQ %s %s query=%s client=%s ua=%s status=%s duration_ms=%.2f",
        request.method,
        request.url.path,
        query,
        client_host,
        user_agent,
        response.status_code,
        elapsed_ms,
    )
    return response


@app.on_event("startup")
async def on_startup():
    create_tables()
    init_default_data()
    _init_admin()
    try:
        await startup_ai_detection()
    except Exception:
        logger.exception("AI detection init failed; TL core APIs remain available.")


@app.on_event("shutdown")
async def on_shutdown():
    await shutdown_ai_detection()


def _init_admin():
    """启动时自动创建默认管理员账户（若不存在）"""
    from app.database import get_conn
    from app.services.user_service import hash_password

    username = os.getenv("ADMIN_USERNAME", "admin")
    password = os.getenv("ADMIN_PASSWORD", "admin123")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE username = %s", (username,))
            if cur.fetchone():
                return
            cur.execute(
                "INSERT INTO users (username, hashed_password, real_name, role, is_active) "
                "VALUES (%s, %s, %s, 'admin', 1)",
                (username, hash_password(password), "管理员"),
            )
    logger.info("默认管理员账户已创建：username=%s", username)
