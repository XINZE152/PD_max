import logging
import os
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import app.config as app_config  # noqa: F401 — 加载项目根 .env（副作用）
from app.api.v1.router import api_router
from app.database import create_tables, init_default_data
from app.logging_config import setup_logging
from app.intelligent_prediction.exceptions import BusinessException

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


@app.exception_handler(BusinessException)
async def business_exception_handler(request: Request, exc: BusinessException) -> JSONResponse:
    _ = request
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.code, "message": exc.message, "details": exc.details},
    )


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
    if app_config.AI_DETECTION_ENABLED:
        from app.api.v1.routes.ai_detection import startup_ai_detection

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
                logger.exception(
                    "AI 鉴伪预加载失败（多为 EasyOCR 从 GitHub 下载模型时网络中断；"
                    "比价等接口不受影响，首次鉴伪请求会再尝试加载）。"
                    "可：关 AI_DETECTION_PRELOAD、配置 HTTPS 代理、或设置 EASYOCR_MODULE_PATH 使用离线模型目录。"
                )
    else:
        logger.info("AI 鉴伪模块已关闭（AI_DETECTION_ENABLED=0），不注册 /ai-detection 路由、不加载模型")

    if app_config.INTELLIGENT_PREDICTION_ENABLED:
        try:
            from app.intelligent_prediction.services.cache_manager import get_cache_manager

            await get_cache_manager().redis.connect()
        except Exception:
            logger.exception("智能預測 Redis 預連線失敗（不影響主服務）")
        from app.intelligent_prediction.settings import settings as ip_settings

        if ip_settings.intelligent_prediction_schedule_enabled:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger

            from app.intelligent_prediction.services.scheduled_prediction import (
                run_scheduled_intelligent_prediction_sync,
            )

            sched = BackgroundScheduler(timezone="Asia/Shanghai")
            sched.add_job(
                func=run_scheduled_intelligent_prediction_sync,
                trigger=CronTrigger(
                    hour=ip_settings.intelligent_prediction_schedule_cron_hour,
                    minute=ip_settings.intelligent_prediction_schedule_cron_minute,
                ),
                id="intelligent_prediction_schedule",
                replace_existing=True,
            )
            sched.start()
            app.state.ip_prediction_scheduler = sched
            logger.info(
                "智能預測定時任務已啟用：cron %s:%s",
                ip_settings.intelligent_prediction_schedule_cron_hour,
                ip_settings.intelligent_prediction_schedule_cron_minute,
            )
        else:
            app.state.ip_prediction_scheduler = None
    else:
        logger.info(
            "智能預測模組已關閉（INTELLIGENT_PREDICTION_ENABLED=0），不註冊相關路由"
        )


@app.on_event("shutdown")
async def on_shutdown():
    if app_config.AI_DETECTION_ENABLED:
        from app.api.v1.routes.ai_detection import shutdown_ai_detection

        await shutdown_ai_detection()
    if app_config.INTELLIGENT_PREDICTION_ENABLED:
        sched = getattr(app.state, "ip_prediction_scheduler", None)
        if sched is not None:
            sched.shutdown(wait=False)
        try:
            from app.intelligent_prediction.services.cache_manager import get_cache_manager

            await get_cache_manager().redis.close()
        except Exception:
            pass


def _warn_insecure_defaults() -> None:
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
