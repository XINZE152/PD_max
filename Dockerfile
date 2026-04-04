# 1. 基础镜像
FROM python:3.11-slim

# 2. 安装系统依赖（opencv 需要 libxcb 等库）
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxcb1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# 3. 设置工作目录
WORKDIR /app

# 4. 安装 uv，并使用锁文件安装依赖
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

# 5. 复制项目代码
COPY . .

# 6. AI 资源缓存目录
ENV AI_EASYOCR_MODEL_DIR=/opt/ai-assets/easyocr
ENV TORCH_HOME=/opt/ai-assets/torch
ENV AI_DETECTION_PRELOAD=1
ENV AI_EASYOCR_DOWNLOAD_ENABLED=1

# 7. 构建阶段预下载 EasyOCR / ResNet 资源，避免服务器首启冷下载
ARG PRELOAD_AI_ASSETS=1
RUN if [ "$PRELOAD_AI_ASSETS" = "1" ]; then uv run --no-sync python scripts/preload_ai_assets.py; fi

# 8. 运行时默认不再现场下载 OCR 模型；若镜像未预热，可在部署时显式覆盖为 1
ENV AI_EASYOCR_DOWNLOAD_ENABLED=0

# 9. 暴露端口
EXPOSE 8000

# 10. 启动
CMD ["uv", "run", "--no-sync", "app.py"]
