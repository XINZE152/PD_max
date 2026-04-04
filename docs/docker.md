# Docker 部署文档

---

## 一、环境准备

确保已安装 Docker 和 Docker Compose：
```bash
docker --version
docker compose version
```

---

## 二、构建镜像

```bash
docker build -t my-backend .
```

说明：
- 当前 Dockerfile 已改为使用 `uv sync` 安装依赖，并通过 `uv run --no-sync app.py` 启动。
- 构建阶段默认会预下载 EasyOCR 与 ResNet 权重，避免服务器首个请求冷启动时现场下载模型。
- 若构建机无法联网，可临时关闭预下载：

```bash
docker build --build-arg PRELOAD_AI_ASSETS=0 -t my-backend .
```

---

## 三、启动（手动传入配置）

### 1. 创建网络

```bash
docker network create mynet
```

### 2. 启动 MySQL

```bash
docker run -d \
  --name mysql-lite \
  --network mynet \
  -e MYSQL_ROOT_PASSWORD=your_db_password \
  -e MYSQL_DATABASE=demo \
  -p 3306:3306 \
  -v mysql_data:/var/lib/mysql \
  mysql:8.0
```

### 3. 启动后端

```bash
docker run -d \
  --name my-backend \
  --network mynet \
  -p 8000:8000 \
  -e PORT=8000 \
  -e AI_DETECTION_PRELOAD=1 \
  -e AI_EASYOCR_MODEL_DIR=/opt/ai-assets/easyocr \
  -e AI_EASYOCR_DOWNLOAD_ENABLED=0 \
  -e TORCH_HOME=/opt/ai-assets/torch \
  -e MYSQL_HOST=mysql-lite \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=root \
  -e MYSQL_PASSWORD=your_db_password \
  -e MYSQL_DATABASE=demo \
  -e JWT_SECRET_KEY=your_random_secret \
  -e ADMIN_USERNAME=admin \
  -e ADMIN_PASSWORD=your_admin_password \
  -e LLM_API_KEY=sk-xxxxxx \
  -e LLM_BASE_URL=https://api.anthropic.com \
  -e LLM_MODEL=claude-sonnet-4-6 \
  -e VLM_API_KEY=sk-xxxxxx \
  -e VLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1 \
  -e VLM_MODEL=qwen-vl-max-latest \
  my-backend
```

---

## 四、Docker Compose（可选）

`docker-compose.yml` 通过 `${VAR}` 占位读取 shell 环境变量。
在本地开发时可以创建 `.env` 文件（已在 `.gitignore` 中，不会上传）：

```bash
cp .env.example .env
# 编辑 .env 填入真实配置
docker compose up -d --build
```

服务器部署时直接 export 环境变量后启动：

```bash
export MYSQL_PASSWORD=your_db_password
export MYSQL_DATABASE=demo
export JWT_SECRET_KEY=your_random_secret
# ... 其余变量同理
docker compose up -d --build
```

---

## 五、更新代码后重新部署

```bash
# 停止并删除旧容器
docker rm -f my-backend

# 重新构建并启动（同上 docker run 命令）
docker build -t my-backend .
docker run -d ...
```

使用 Compose：
```bash
docker compose down
docker compose up -d --build
```

---

## 六、常用指令

### 查看容器状态
```bash
docker ps
docker ps -a
```

### 查看日志
```bash
docker logs my-backend
docker logs my-backend -f
docker logs my-backend --tail 50
```

### 启动 / 停止 / 重启
```bash
docker start my-backend
docker stop my-backend
docker restart my-backend
```

### 进入容器
```bash
docker exec -it my-backend bash
docker exec -it mysql-lite mysql -uroot -p demo
```

### 删除容器 / 镜像
```bash
docker rm -f my-backend
docker rm -f mysql-lite
docker rmi my-backend
```

### 查看网络 / 数据卷
```bash
docker network ls
docker volume ls
docker volume inspect mysql_data
```

---

## 七、环境变量说明

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `MYSQL_HOST` | MySQL 地址（Compose 内用容器名） | 必填 |
| `MYSQL_PORT` | MySQL 端口 | `3306` |
| `MYSQL_USER` | MySQL 用户名 | 必填 |
| `MYSQL_PASSWORD` | MySQL 密码 | 必填 |
| `MYSQL_DATABASE` | 数据库名 | 必填 |
| `JWT_SECRET_KEY` | JWT 签名密钥（同时用于改密校验） | 必填，建议随机字符串 |
| `JWT_ACCESS_TOKEN_EXPIRE_MINUTES` | Token 有效期（分钟） | `1440`（24小时） |
| `ADMIN_USERNAME` | 首次启动自动创建的管理员账号 | `admin` |
| `ADMIN_PASSWORD` | 首次启动自动创建的管理员密码 | `admin123` |
| `LLM_API_KEY` | 采购建议 LLM 的 API Key | 必填 |
| `LLM_BASE_URL` | 采购建议 LLM 的 Base URL | `https://api.anthropic.com` |
| `LLM_MODEL` | 采购建议使用的模型名 | `claude-sonnet-4-6` |
| `VLM_API_KEY` | 报价表识别 VLM 的 API Key | 必填 |
| `VLM_BASE_URL` | 报价表识别 VLM 的 Base URL | `https://dashscope.aliyuncs.com/compatible-mode/v1` |
| `VLM_MODEL` | 报价表识别使用的模型名 | `qwen-vl-max-latest` |

---

## 八、接口访问

- 后端 API：http://localhost:8000
- Swagger 文档：http://localhost:8000/docs
- ReDoc 文档：http://localhost:8000/redoc

---

## 九、避免 AI 冷启动

若服务器此前出现“首次检测等待 10 分钟以上仍不返回”，通常是容器在运行时下载 EasyOCR / torchvision 权重，或外网访问模型源受限。

推荐配置：

```bash
export AI_DETECTION_PRELOAD=1
export AI_EASYOCR_MODEL_DIR=/opt/ai-assets/easyocr
export AI_EASYOCR_DOWNLOAD_ENABLED=0
export TORCH_HOME=/opt/ai-assets/torch
docker compose up -d --build
```

排查建议：
- 先看构建日志，确认镜像构建阶段已执行 `scripts/preload_ai_assets.py`
- 再看容器日志，若仍出现 “may download EasyOCR models”，说明运行时目录没有命中预热缓存
- 若前面有 Nginx / SLB，仍需将 `proxy_read_timeout` / `proxy_send_timeout` 至少调到 `300s`
