import os
import logging

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.database import create_tables, init_default_data
from app.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="TL比价系统", version="1.0.0")

app.include_router(api_router)


@app.on_event("startup")
def on_startup():
    create_tables()
    init_default_data()
    _init_admin()


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
