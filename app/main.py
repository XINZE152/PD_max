import logging
import os
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

import app.config  # noqa: F401 — 加载项目根 .env（副作用）
from app.api.v1.router import api_router
from app.api.v1.routes.ai_detection import shutdown_ai_detection, startup_ai_detection
from app.database import create_tables, init_default_data
from app.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)
access_logger = logging.getLogger("app.access")

app = FastAPI(title="TL比价系统", version="1.0.0")

# 浏览器前端与 API 不同源时，须配置 CORS，否则请求会被浏览器拦截（控制台常见 CORS / Network failed）
_cors_origins = os.getenv("CORS_ORIGINS", "").strip()
if _cors_origins:
    _origins = [o.strip() for o in _cors_origins.split(",") if o.strip()]
    if _origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

app.include_router(api_router)


@app.middleware("http")
async def log_http_requests(request: Request, call_next):
    start_time = time.perf_counter()
    client_host = request.client.host if request.client else "-"
    path = request.url.path
    if request.url.query:
        path = f"{path}?{request.url.query}"

    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        access_logger.exception(
            "%s %s 500 %.0fms %s",
            request.method,
            path,
            elapsed_ms,
            client_host,
        )
        raise

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    access_logger.info(
        "%s %s %s %.0fms %s",
        request.method,
        path,
        response.status_code,
        elapsed_ms,
        client_host,
    )
    return response


@app.on_event("startup")
async def on_startup():
    _warn_insecure_defaults()
    create_tables()
    init_default_data()
    _init_admin()
    try:
        await startup_ai_detection()
    except Exception:
        logger.exception("AI detection init failed; TL core APIs remain available.")
    # 经 Nginx/云网关时，首次检测若现场加载 OCR+模型易超 60s 触发 504；预加载可拉长启动、缩短首请求耗时
    if os.getenv("AI_DETECTION_PRELOAD", "").strip().lower() in ("1", "true", "yes", "on"):
        try:
            from app.api.v1.routes import ai_detection as _ai_det_mod

            await _ai_det_mod.ensure_ai_detection_runtime()
            logger.info("AI 鉴伪运行时已预加载（AI_DETECTION_PRELOAD=1）")
        except Exception:
            logger.exception("AI 鉴伪预加载失败，将在首次检测请求时再加载")


@app.on_event("shutdown")
async def on_shutdown():
    await shutdown_ai_detection()


def _warn_insecure_defaults() -> None:
    from app import config as app_config

    if app_config.JWT_SECRET_KEY == "change_this_to_a_strong_random_secret":
        logger.warning(
            "JWT_SECRET_KEY 仍为占位默认值，生产环境请务必在 .env 中更换为强随机密钥"
        )


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
