import os

import uvicorn

import app.config  # noqa: F401 — 先加载项目根 .env，再读 PORT 等变量

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

    # 日志在 app.main 导入时通过 setup_logging() 初始化（仅 worker 内执行一次即可）
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=reload)
