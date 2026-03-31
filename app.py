import os
from pathlib import Path

import uvicorn
from dotenv import load_dotenv

from app.logging_config import setup_logging

# 始终从项目根目录（本文件所在目录）加载 .env，避免从其它工作目录启动时读不到 PORT
load_dotenv(Path(__file__).resolve().parent / ".env")
setup_logging()

if __name__ == "__main__":
    port_str = os.getenv("PORT")
    if not port_str or not str(port_str).strip():
        raise ValueError(
            "Missing required env var: PORT — add it to .env (e.g. PORT=8002) or export PORT before starting."
        )

    try:
        port = int(str(port_str).strip())
    except ValueError as exc:
        raise ValueError("Env var PORT must be an integer") from exc

    reload = os.getenv("RELOAD", "").strip().lower() in ("1", "true", "yes", "on")

    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=reload)
