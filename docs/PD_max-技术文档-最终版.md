# PD_max（TL 比价系统）— 技术文档（最终合并版）

本文档由仓库内以下文件**合并**而成，便于交付归档与离线阅读：

| 原文件 | 合并后章节 |
|--------|------------|
| `docs/技术文档.md` | 第 1 章、第 6 章 |
| `docs/docker.md` | 第 2 章 |
| `docs/数据库文档.md` | 第 3 章 |
| `docs/后端接口文档.md` | 第 4 章 |
| `docs/api.md` | 第 5 章 |

**验收准则**：HTTP 路径、Query/Body 字段、枚举值及错误码以运行环境 **`/docs`（OpenAPI）** 为准；本文中的 JSON 为示例，若与线上实现不一致，以 OpenAPI 为准。

---

## 目录

- [1. 系统架构、配置与运维摘要](#1-系统架构配置与运维摘要)
- [2. Docker 部署](#2-docker-部署)
- [3. 数据库说明](#3-数据库说明)
- [4. HTTP 接口总览（全模块路由表）](#4-http-接口总览全模块路由表)
- [5. TL 与认证接口详解（含 JSON 示例）](#5-tl-与认证接口详解含-json-示例)
- [6. 日志、测试、脚本与交付清单](#6-日志测试脚本与交付清单)

---

# 1. 系统架构、配置与运维摘要

本文档面向**交付验收、运维部署与二次开发**，在通读仓库核心代码（`app/main.py`、`app/config.py`、`app/database.py`、`app/api`、`app/services`、`app/ai_detection`、`app/intelligent_prediction`、`scripts` 等）基础上整理。接口字段级契约以运行实例的 OpenAPI（`/docs`）为准；**数据库、Docker、全模块路由与 TL/认证 JSON 示例见本文第 2～5 章。**

---

## 1. 系统概述

### 1.1 业务定位

**废旧电池回收比价后端**：支撑仓库 / 冶炼厂 / 品类字典、库房类型与颜色、库房单向关联（有向图 + 阶梯价差）、运费、报价数据维护；基于 **VLM（视觉大模型，OpenAI 兼容协议）** 对报价表图片或 **openpyxl** 解析 Excel 报价列表；提供**多维度比价**、采购建议（LLM）、**省份对标定价 / 冶炼厂标定价格 / 库房差额与毛利 / AI 定价对标分析快照**等扩展能力。可选模块：**AI 图片篡改鉴伪**（本地 EasyOCR + PyTorch 等）、**智能送货量预测**（异步 SQLAlchemy + Redis 缓存 + 可选 Celery 批量任务 + APScheduler 定时任务）。

### 1.2 技术特征摘要

| 维度 | 说明 |
|------|------|
| 运行时 | Python ≥ 3.11，[uv](https://github.com/astral-sh/uv) 管理依赖（`pyproject.toml` + `uv.lock`），默认 PyPI 索引为阿里云镜像 |
| Web 框架 | FastAPI 1.x，ASGI 服务为 Uvicorn；入口 `app.py` 读取 **`PORT`**（必填） |
| 主业务库 | MySQL 8：`pymysql` 同步连接；启动时 **`create_database_if_not_exists`** + **`create_tables`** |
| 智能预测库访问 | SQLAlchemy 2 异步 + **aiomysql**（连接串见 `PREDICTION_ASYNC_DATABASE_URL` 或由 `MYSQL_*` 组装） |
| 缓存与任务 | **Redis**（预测缓存、Celery broker/backend）；**Celery** 任务名 `intelligent_prediction.run_prediction_batch`（见 `export_tasks.py`） |
| 定时任务 | **APScheduler** `BackgroundScheduler`，时区 `Asia/Shanghai`，由 `INTELLIGENT_PREDICTION_SCHEDULE_ENABLED` 等控制 |
| 外部 AI | VLM / LLM：OpenAI 兼容客户端；天地图服务端地理编码；可选高德天气（送货历史导入） |
| 本地推理 | PyTorch、torchvision、EasyOCR、OpenCV-headless、**faiss-cpu**（鉴伪特征索引）等 |

### 1.3 主要第三方库（节选）

除上表外，`pyproject.toml` 中还包含：`python-jose`、`bcrypt`、`python-multipart`、`Pillow`、`pandas`、`joblib`、`xgboost`（当前业务代码中未检索到直接 import，保留为依赖项）、`chinese-calendar`（工作日/节假日相关逻辑）等。完整列表以 `pyproject.toml` / `uv.lock` 为准。

---

## 2. 系统架构

### 2.1 逻辑分层

```
客户端 / 前端
        │  HTTPS（建议经 Nginx 反代）
        ▼
┌─────────────────────────────────────────────────────────────┐
│  FastAPI 应用（app.main:app）                                │
│  · root_path：FASTAPI_ROOT_PATH / ROOT_PATH（子路径反代）    │
│  · 可选 CORS：CORS_ORIGINS                                   │
│  · 中间件：访问日志 + JWT 操作人上下文（ContextVar）          │
│  · 异常：智能预测 BusinessException → 统一 JSON               │
│  · 启动：create_tables、管理员、鉴伪初始化、Redis、定时调度    │
│  · 关闭：鉴伪 shutdown、调度器、Redis                          │
└─────────────────────────────────────────────────────────────┘
        │
        ├── /tl/*              TL 比价、字典、运费、报价、比价、采购建议、对标与快照
        ├── /auth/*            注册登录、用户、角色、动态权限列
        ├── /ai-detection/*    鉴伪（受 AI_DETECTION_ENABLED）
        └── /predict 等        智能预测（受 INTELLIGENT_PREDICTION_ENABLED）
        │
        ▼
   MySQL ◄──► Redis / Celery Worker（需单独进程）/ 外部 LLM·VLM / 天地图 / 天气 API
```

**说明**：`docker-compose.yml` 仅编排 **mysql、redis、backend**，**未包含 Celery Worker 容器**。若使用 `POST /predict/async` 等异步能力，需在相同代码与 `.env` 下单独启动 Worker（见 [4.2](#42-celery-worker智能预测异步批次)）。

### 2.2 代码目录（与职责）

| 路径 | 职责 |
|------|------|
| `app.py` | 进程入口：`PORT`、`RELOAD`，`uvicorn.run("app.main:app", ...)` |
| `app/main.py` | FastAPI 实例、CORS、`include_router`、生命周期、`_init_admin`、JWT 弱密钥告警 |
| `app/paths.py` | `PROJECT_ROOT`：统一项目根路径（`.env`、日志、上传目录解析） |
| `app/config.py` | `load_dotenv(PROJECT_ROOT/.env)`；MySQL、JWT、UPLOAD、LLM/VLM、地图、模块开关、循融宝加价等 |
| `app/database.py` | `get_conn()`、`TABLE_STATEMENTS` 全量建表；大量 `ensure_*` 做旧库增量迁移 |
| `app/logging_config.py` | 根日志级别、控制台/滚动文件、`app.finance_audit` 独立财务审计 logger |
| `app/request_context.py` | 从 `Authorization: Bearer` 解析 uid/role，注入日志 `operator` 字段 |
| `app/finance_log.py` | `log_finance_event` → `app.finance_audit` |
| `app/api/v1/router.py` | 挂载 `tl`、`auth`；条件挂载 `ai_detection`、`intelligent_prediction` |
| `app/api/v1/routes/tl.py` | TL 全部 HTTP 路由（体量最大） |
| `app/api/v1/routes/auth.py` | 认证与用户、角色、权限模板、动态 permission_definitions |
| `app/api/v1/routes/ai_detection.py` | 鉴伪同步/异步/历史/可视化；`startup_ai_detection` / `shutdown_ai_detection` |
| `app/services/tl_service.py` | TL 领域逻辑 |
| `app/services/vlm_extractor_service.py` | VLM 报价表解析、CLI 能力（Typer）同文件 |
| `app/services/user_service.py` | 用户、JWT 编解码 |
| `app/services/permission_service.py` / `role_definition_service.py` | 权限与角色 |
| `app/price_tax_utils.py` | 含税价与不含税基准双向换算、备注推断口径 |
| `app/quote_price_sources.py` | `price_field_sources` 原数据/换算合并规则 |
| `app/ai_detection/` | 推理 API、检测器、OCR 补丁、历史持久化、运行时资源 |
| `app/intelligent_prediction/` | settings、db、models、services、api、tasks、Celery app、调度 |

### 2.3 模块开关

| 变量 | 默认 | 作用 |
|------|------|------|
| `AI_DETECTION_ENABLED` | 开 | 关：不注册 `/ai-detection/*`，不执行鉴伪启动与预加载逻辑 |
| `INTELLIGENT_PREDICTION_ENABLED` | 开 | 关：不注册 `/predict`、`/forecast`、`/delivery-history`、`/knowledge`，不连 Redis 预连接、不启调度 |

解析规则见 `app/config.py` 中 `_env_enabled`（`0/false/off` 为关）。

### 2.4 HTTP 路由与响应约定

与本文第 4 章「通用约定」一致：

- 业务路由挂在**应用根**，**无**统一 `/api/v1` 前缀。
- **`/tl/*`**：多数返回 `{ "code", "msg", "data" }`（具体以接口与 [api.md](./api.md) 为准）。
- **`/auth/*`**：错误时为 HTTP 状态码 + FastAPI 默认 `detail`。
- **智能预测**：注册全局 `BusinessException` 处理器，JSON 形如 `{ "code", "message", "details" }`，HTTP 状态码与 `code` 以实现为准。

### 2.5 鉴权现状与加固建议

- **`/auth/login`** 签发 JWT；管理类接口依赖 Bearer。
- **`/tl/*` 当前实现不强制 JWT**（由网关或后续中间件统一加固的需求见后端接口文档说明）。

---

## 3. 功能模块说明

### 3.1 TL 比价（`/tl`）

涵盖：仓库与冶炼厂 CRUD（含软删/硬删、批量停用）、地理与缺失经纬度查询、循融宝发货及批量设置、品类与映射、报价上传（VLM 图 / Excel）、确认写入与手工录入、单条更新、列表与导出、运费模板与导入、税率维护、**比价**（`get_comparison`）、**采购建议**（`get_purchase_suggestion`，LLM）、**省份对标城市定价**、**冶炼厂标定价格**、**库房对标差额与毛利配置**、**AI 定价对标分析**与**快照** CRUD 等。路由级清单见 本文 [第 4 章](#4-http-接口总览全模块路由表)。

### 3.2 认证与权限（`/auth`）

JWT；用户；可配置 **`role_definitions`**；**`permission_definitions` + `user_permissions` 动态列**与 **`role_templates`** JSON 模板。启动时 `PermissionService.ensure_table_exists()` 与默认管理员权限行（失败仅打日志，不阻塞主流程）。

### 3.3 AI 鉴伪（`/ai-detection`）

- **前缀**：`/ai-detection`；子路径保留 `/api/v1/...`、`/api/v3/...` 形态。
- **同步**：`POST /ai-detection/api/v1/image-detection/detect`（`multipart`：`file`、`bbox`）。
- **异步**：`POST .../api/v3/detect` → `GET .../api/v3/result/{task_id}` → 可选 `.../visualization`；`DELETE .../api/v3/task/{task_id}`。
- **历史**：`GET .../api/v1/history`（保留天数默认 7，可调 `AI_DETECTION_HISTORY_DAYS`）；单条图 `GET .../api/v1/history/{id}/image`。
- **存储**：`UPLOAD_DIR/ai_detection_storage`（任务图、历史归档等，以代码为准）。
- **并发**：`AI_MAX_CONCURRENT_TASKS` 控制信号量；**冷启动**与网关 504 对策见路由 Swagger 描述及下文排障。

### 3.4 智能预测（`/predict`、`/forecast`、`/delivery-history`、`/knowledge`）

- **`/predict`**：字典地址查询、操作审计、同步/异步批量预测、结果分页、批次状态与导出下载、维度选项等。
- **`/forecast`**：规则预测图表、明细、导出、维度选项。
- **`/delivery-history`**：与 **`/history`** 为同一路由器重复挂载（`/history` 在 OpenAPI 中 `include_in_schema=False`），建议客户端统一使用 **`/delivery-history`**。
- **`/knowledge/*`**：预留，当前 **501**。
- **大模型**：`app/intelligent_prediction/settings.py` 中 `OPENAI_*` 优先，否则链接到主应用 `LLM_*` / 百炼兜底；另支持 Azure OpenAI、Anthropic 环境变量（以代码读取为准）。
- **定时预测**：`INTELLIGENT_PREDICTION_SCHEDULE_ENABLED=1` 时，由 `main.py` 启动 APScheduler，cron 默认每日 `INTELLIGENT_PREDICTION_SCHEDULE_CRON_HOUR:MINUTE`（默认 2:30），调用 `run_scheduled_intelligent_prediction_sync`。

### 3.5 核心业务规则（实现摘要）

| 主题 | 说明 | 代码参考 |
|------|------|----------|
| 比价基准日 | `get_comparison`：请求体可传报价日期；不传则按 **`QUOTE_COMPARISON_TZ`**（默认上海）取「当天」，在 `quote_details` 中取与基准日日历距离最近的一条（并列按 `created_at`） | `.env.example` 注释；`tl_service` |
| 含税换算 | 确认写入时以 **`factory_tax_rates`** 与默认 1%/3%/13% **合并**为权威税率；支持不含税基准→各档含税，或某一档含税→反推基准再正算其余档 | `app/price_tax_utils.py` |
| 备注推断口径 | 行备注参与判断报价数字为不含税或哪档含税（正则关键字） | `parse_price_basis_from_remark` |
| 价格字段来源 | 入库 `quote_details.price_field_sources`：键为库列名，值为「原数据」或「换算」；客户端可传中文键，服务端归一为英文列名并合并服务端补全规则 | `app/quote_price_sources.py` |
| 循融宝 | `dict_factories.use_xunrongbao` 为真时，比价中货物单价按 **`XUNRONGBAO_SHIPPING_PREMIUM_PER_TON`**（默认 80 元/吨）加价 | `app/config.py` |

---

## 4. 依赖与运行方式

### 4.1 本地开发

```bash
cp .env.example .env   # 必填 MYSQL_*、PORT 等
uv sync
uv run app.py          # 或 uv run python app.py
```

交互文档：`http://localhost:<PORT>/docs`。单独初始化表（可选）：`uv run python -m app.database` 会执行 `create_tables()`（见 `database.py` 末尾 `if __name__ == "__main__"`）。

### 4.2 Celery Worker（智能预测异步批次）

Compose **未**自带 Worker。与 Web 共用项目根、`.env` 及虚拟环境时，示例：

```bash
uv run celery -A app.intelligent_prediction.tasks.celery_app worker --loglevel=info
```

生产可参考 **`scripts/systemd/pd-max-celery.service`**（`celery -A app.intelligent_prediction.tasks.celery_app worker`）。异步批次会写 `pd_ip_prediction_batches`、更新结果表并生成临时路径下的 Excel（见 `export_tasks.py`）。

### 4.3 容器部署

**完整步骤见本文 [第 2 章](#2-docker-部署)。** 要点：`Dockerfile` 构建期可执行 `scripts/preload_ai_assets.py`（`PRELOAD_AI_ASSETS`）；运行时建议 `AI_EASYOCR_DOWNLOAD_ENABLED=0` 配合镜像内缓存目录。

### 4.4 反向代理与子路径、CORS

- **`FASTAPI_ROOT_PATH` / `ROOT_PATH`**：与网关对外子路径一致（无尾部 `/`），否则 `/docs` 加载 `openapi.json` 路径错误。
- **`CORS_ORIGINS`**：前后端不同源时必填，英文逗号分隔。

---

## 5. 配置与环境变量

完整模板：[.env.example](../.env.example)。除前文已列外，交付建议关注：

| 类别 | 变量示例 | 说明 |
|------|-----------|------|
| 日志 | `LOG_LEVEL`、`LOG_ENABLE_CONSOLE`、`LOG_FILE` / `LOG_ENABLE_FILE`、`LOG_DIR`、`LOG_FINANCE_FILE`、`LOG_ENABLE_FINANCE_FILE` | 主日志与 `app.finance_audit` 财务审计；滚动策略见 `logging_config.py` |
| VLM | `VLM_IMAGE_MAX_EDGE`、`VLM_JPEG_QUALITY`、`VLM_MAX_TOKENS`、`VLM_REQUEST_TIMEOUT` | 大图压缩、token 上限、HTTP 读超时（需 ≤ Nginx `proxy_read_timeout`） |
| LLM 兜底 | `DASHSCOPE_API_KEY`、`QWEN_API_KEY` | 未设 `LLM_API_KEY` 时 `config.py` 中的回退链 |
| 鉴伪 | `AI_DETECTION_HISTORY_DAYS`、`TORCH_NUM_THREADS`、`EASYOCR_*` 等 | 见 `.env.example` |
| 智能预测 | `PREDICTION_ASYNC_DATABASE_URL`、`REDIS_URL`、`CELERY_BROKER_URL`、`CELERY_RESULT_BACKEND`、`AI_REQUEST_TIMEOUT_SECONDS`、`PREDICTION_REDIS_TTL_SECONDS`、`INTELLIGENT_PREDICTION_SCHEDULE_*`、`INTELLIGENT_PREDICTION_HISTORY_PURGE_SECRET`、`WEATHER_API_BASE_URL`、`WEATHER_API_KEY` | 详见 `app/intelligent_prediction/settings.py` |

---

## 6. 数据与存储

### 6.1 主要表分组（与 `database.py` 对齐）

| 分组 | 表名（节选） |
|------|----------------|
| 用户与权限 | `users`、`role_definitions`、`permission_definitions`、`role_templates`、`user_permissions` |
| TL 字典与业务 | `dict_categories`、`dict_warehouse_types`、`dict_warehouses`、`dict_warehouse_links`、`dict_factories`、`freight_rates`、`factory_tax_rates`、`quote_table_metadata`、`quote_details` |
| 预留 | `warehouse_inventories`、`factory_demands`、`factory_demand_items` |
| 鉴伪 | `ai_detection_history` |
| 智能预测 | `pd_ip_delivery_records`、`pd_ip_prediction_batches`、`pd_ip_prediction_results`、`pd_ip_operation_audit` |
| 对标与 AI 分析 | `pd_province_benchmark_prices`、`pd_smelter_calibration_prices`、`pd_warehouse_spread_configs`、`pd_ai_pricing_snapshots`、`pd_ai_pricing_snapshot_items` |

字段与业务含义见本文 [第 3 章](#3-数据库说明)。

### 6.2 Schema 演进策略

- **新库**：`create_tables()` 执行 `TABLE_STATEMENTS` 中全部 `CREATE TABLE IF NOT EXISTS`。
- **旧库升级**：同一入口顺序调用 `ensure_quote_details_price_field_sources_column`、`ensure_dict_warehouse_types_migration`、`ensure_pd_pricing_benchmark_tables` 等函数，用 `information_schema` 检测后 `ALTER` / 补建表。
- **生产建议**：定期备份；重大变更仍建议评审 `database.py` 迁移块与线上数据兼容性。

### 6.3 文件存储

- **`UPLOAD_DIR`**：相对路径相对项目根解析为绝对路径；通用上传与鉴伪子目录并存。
- **鉴伪历史图**：文件名写入 `ai_detection_history.stored_image`，与 `ai_detection_history_images/` 等目录配合（以 `history_db` 实现为准）。

---

## 7. 应用启动流程（摘要）

1. 加载 `.env`（`app.config` 副作用）。
2. `setup_logging()`（仅首次安装 handlers）。
3. 每个请求：`bind_operator_context` → 业务 → `reset_operator_context`。
4. **`startup`**：`create_tables()` 及各类 `ensure_*`；`PermissionService.ensure_table_exists()`；`_init_admin()`；若开启鉴伪则 `startup_ai_detection()`，可选 `AI_DETECTION_PRELOAD` 预加载；若开启智能预测则 Redis 预连接与可选 APScheduler。
5. **`shutdown`**：鉴伪清理、停止调度器、关闭 Redis 连接。

---

---

# 2. Docker 部署

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

---

# 3. 数据库说明

本文描述 TL 比价模块相关的主要表及**对标定价**等新表；完整 DDL 见仓库 [`app/database.py`](../app/database.py) 中 `TABLE_STATEMENTS` 与 `ensure_*` 迁移。

---

## 字典与运费（既有）

| 表名 | 说明 |
|------|------|
| `dict_warehouses` | 库房字典（名称、省市区、`freight_amount` 运费参考等） |
| `dict_factories` | 冶炼厂字典 |
| `freight_rates` | 冶炼厂—库房运费，`effective_date` 生效日；唯一键 `(factory_id, warehouse_id, effective_date)` |

---

## 对标定价与 AI 分析（新增）

### `pd_province_benchmark_prices`

省份维度「对标城市」定价历史。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT PK | 主键 |
| `province` | VARCHAR(64) | 省份（建议与 `dict_warehouses.province` 一致） |
| `benchmark_city` | VARCHAR(128) | 对标城市名称 |
| `benchmark_price` | DECIMAL(18,4) | 对标城市定价 |
| `price_date` | DATE | 定价日期；新增默认当天（服务端按 `QUOTE_COMPARISON_TZ`，默认 `Asia/Shanghai`） |
| `created_at` | TIMESTAMP | 上传时间 |

**当前有效（按省）**：同一 `province` 取 `price_date` 最大；同日多条则取 `id` 最大。

### `pd_smelter_calibration_prices`

冶炼厂标定价格历史。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT PK | 主键 |
| `factory_id` | INT FK → `dict_factories.id` | 冶炼厂 |
| `calibration_price` | DECIMAL(18,4) | 标定价格 |
| `price_date` | DATE | 定价日期 |
| `created_at` | TIMESTAMP | 上传时间 |

**当前有效（按厂）**：同一 `factory_id` 取 `price_date` 最大；同日取 `id` 最大。业务上分析接口固定解析名称「金利」对应的冶炼厂 id。

### `pd_warehouse_spread_configs`

库房对标城市差额与毛利（配置版）；**每库房一行**。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INT PK | 主键 |
| `warehouse_id` | INT UNIQUE FK → `dict_warehouses.id` | 库房 |
| `benchmark_city` | VARCHAR(128) | 对标城市（人工配置） |
| `city_spread` | DECIMAL(18,4) | 对标城市差额（可负） |
| `gross_margin_config` | DECIMAL(18,4) NULL | 毛利（配置版） |
| `created_at` / `updated_at` | TIMESTAMP | 维护时间 |

删除配置后，实时分析中该库房的差额/毛利配置按缺失处理。

### `pd_ai_pricing_snapshots`

AI 定价对标分析快照头。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT PK | 快照 id |
| `title` | VARCHAR(255) NULL | 标题 |
| `as_of_date` | DATE NULL | 口径日期（解析基准价、运费时：`<= as_of_date` 的最新一条） |
| `created_at` | TIMESTAMP | 创建时间 |

### `pd_ai_pricing_snapshot_items`

快照明细：生成时刻固化的数值（后续基准表变更不影响历史快照）。

| 字段 | 类型 | 说明 |
|------|------|------|
| `snapshot_id` | BIGINT FK → `pd_ai_pricing_snapshots.id` ON DELETE CASCADE | 快照 |
| `warehouse_id` | INT FK → `dict_warehouses.id` | 库房 |
| `warehouse_name`, `province`, `city`, `district` | VARCHAR | 冗余展示 |
| `benchmark_city` | VARCHAR(128) NULL | 对标城市（展示） |
| `benchmark_city_price` | DECIMAL(18,4) NULL | 对标城市定价 |
| `city_spread` | DECIMAL(18,4) NULL | 对标城市差额 |
| `gross_margin_config` | DECIMAL(18,4) NULL | 毛利（配置版） |
| `calibration_price` | DECIMAL(18,4) NULL | 冶炼厂标定价格（金利） |
| `freight` | DECIMAL(18,4) NULL | 库房运费 |
| `warehouse_price` | DECIMAL(18,4) NULL | 库房定价（计算） |
| `gross_margin_computed` | DECIMAL(18,4) NULL | 毛利（计算版） |
| `remark` | TEXT NULL | 备注 |

---

## 实时分析用到的规则（与接口一致）

1. **省份对标定价**：按库房 `province` 匹配 `pd_province_benchmark_prices`，在口径日 `as_of_date`（缺省为当天）下取 `price_date <= as_of_date` 的最新一行。
2. **冶炼厂标定价格**：取「金利」对应 `factory_id`，在 `pd_smelter_calibration_prices` 上同上按日取最新。
3. **库房运费**：优先 `freight_rates` 中 `(金利 factory_id, warehouse_id)` 且 `effective_date <= as_of_date` 的最新 `price_per_ton`；若无则使用 `dict_warehouses.freight_amount`。
4. **计算公式**  
   - 库房定价 = 对标城市定价 + 对标城市差额（差额来自 `pd_warehouse_spread_configs.city_spread`；无配置则两项无法合成定价时相关计算为空）  
   - 毛利（计算版）= 冶炼厂标定价格 − 库房运费 − 库房定价  

---

## 迁移说明

新建库由 `create_tables()` 中 `TABLE_STATEMENTS` 创建上述表；已有库在启动时执行 `ensure_pd_pricing_benchmark_tables()` 补建（见 [`app/database.py`](../app/database.py)）。

---

# 4. HTTP 接口总览（全模块路由表）

本文档汇总本仓库 **FastAPI** 对外 HTTP 接口，便于联调与验收。更细的 **TL 比价** 请求/响应示例见同目录 [`api.md`](./api.md)。

---

## 1. 概述

| 项 | 说明 |
|----|------|
| 框架 | FastAPI，应用标题「TL比价系统」，版本 `1.0.0` |
| 路由挂载 | 业务路由直接挂在应用根路径下（**无**统一 `/api/v1` 前缀），例如 `/tl/...`、`/auth/...` |
| 反向代理 | 若网关以子路径挂载服务，需配置环境变量 **`FASTAPI_ROOT_PATH`** 或 **`ROOT_PATH`**（与部署路径一致、无尾部 `/`），否则 Swagger `/docs` 内 OpenAPI 地址可能错误 |
| 交互式文档 | 服务启动后访问 **`/docs`**（Swagger UI）、**`/openapi.json`**（OpenAPI 规范） |
| CORS | 前后端不同源时配置 **`CORS_ORIGINS`**（英文逗号分隔多个源），见项目根 `.env.example` |

---

## 2. 通用约定

### 2.1 请求与响应

- 字符编码：**UTF-8**。
- **JSON** 接口：`Content-Type: application/json`（文件上传接口为 `multipart/form-data`）。
- **TL 模块**（`/tl/*`）多数返回形如：`{ "code": 200, "msg": "...", "data": ... }`（具体字段以各接口与 [`api.md`](./api.md) 为准）。
- **用户认证**（`/auth/*`）错误时返回 HTTP 状态码 + FastAPI 标准 `detail`。
- **智能预测子域**（`/predict`、`/forecast`、`/delivery-history` 等）业务异常可能返回 JSON：`{ "code", "message", "details" }`（HTTP 状态码与 `code` 以实际为准）。

### 2.2 鉴权（JWT）

- **`POST /auth/login`** 成功后返回访问令牌；受保护接口在请求头携带：  
  `Authorization: Bearer <token>`
- **需要登录**的接口集中在 **`/auth/*`** 中除注册、登录外的管理类接口（如用户列表、权限、角色等），以路由实现中的 `Depends` 为准；**`/tl/*` 当前实现不强制 JWT**（若生产环境需鉴权，应在网关或中间件层统一加固）。

---

## 3. 可选模块与环境开关

| 模块 | 路径前缀 | 启用条件 |
|------|-----------|----------|
| TL 比价 | `/tl` | 始终注册（核心模块） |
| 用户认证 | `/auth` | 始终注册 |
| AI 鉴伪 | `/ai-detection` | **`AI_DETECTION_ENABLED`** 非 `0` / 关闭类取值时注册 |
| 智能预测 | `/predict`、`/forecast`、`/delivery-history`、`/knowledge` | **`INTELLIGENT_PREDICTION_ENABLED`** 非 `0` 时注册 |

说明：`/knowledge/*` 为预留接口，当前返回 **501**。

---

## 4. 用户认证 `/auth`

| 方法 | 路径 | 摘要 |
|------|------|------|
| POST | `/auth/register` | 用户注册 |
| POST | `/auth/login` | 登录，返回 JWT |
| GET | `/auth/users` | 用户列表（仅 admin） |
| POST | `/auth/users` | 新增用户（仅 admin） |
| POST | `/auth/update_role` | 修改用户角色（仅 admin） |
| POST | `/auth/change_password` | 修改密码 |
| POST | `/auth/delete_user` | 删除用户软删除（仅 admin） |
| GET | `/auth/roles/manage` | 角色定义全量（含停用，仅 admin） |
| GET | `/auth/roles` | 启用角色列表（登录即可，下拉用） |
| GET | `/auth/roles/{code}` | 角色详情（仅 admin） |
| POST | `/auth/roles` | 新增角色（仅 admin） |
| PUT | `/auth/roles/{code}` | 更新角色（仅 admin） |
| DELETE | `/auth/roles/{code}` | 删除角色（仅 admin，非内置且无用户使用） |
| GET | `/auth/permissions/me` | 当前用户权限详情 |
| GET | `/auth/permissions/roles/templates` | 各角色权限模板 |
| PUT | `/auth/permissions/roles/{role}/template` | 更新某角色权限模板 |
| GET | `/auth/permissions` | 用户权限分页列表 |
| GET | `/auth/permissions/{user_id}` | 指定用户权限详情 |
| PUT | `/auth/permissions/{user_id}` | 更新用户权限或权限行角色 |
| POST | `/auth/permissions/{user_id}/reset` | 按角色模板重置用户权限 |
| GET | `/auth/permission/definitions` | 权限字段定义列表 |
| POST | `/auth/permission/definitions` | 新增权限字段（会 ALTER 表） |
| DELETE | `/auth/permission/definitions/{field_name}` | 删除权限字段（会 ALTER 表） |

请求体字段以 Swagger `/docs` 中 **Schemas** 与路由说明为准。

---

## 5. TL 比价模块 `/tl`

### 5.1 详细说明与 JSON 示例

仓库内 **`docs/api.md`** 按接口编号维护了比价相关的**请求/响应示例、业务规则**（含比价取价、`get_comparison` 报价日期与 **`QUOTE_COMPARISON_TZ`** 等），交付时建议与本文档一并提供。

### 5.2 路由一览（方法 + 路径 + 摘要）

| 方法 | 路径 | 摘要 |
|------|------|------|
| POST | `/tl/add_warehouse` | 添加仓库 |
| POST | `/tl/import_partner_warehouses_excel` | 批量导入合作库房 Excel |
| GET | `/tl/get_warehouses` | 获取仓库列表 |
| GET | `/tl/get_warehouse_types` | 库房类型列表 |
| POST | `/tl/add_warehouse_type` | 新增库房类型 |
| POST | `/tl/update_warehouse_type` | 修改库房类型 |
| DELETE | `/tl/delete_warehouse_type` | 删除库房类型（软删除） |
| POST | `/tl/update_warehouse` | 修改仓库信息 |
| DELETE | `/tl/delete_warehouse` | 删除仓库（软删除） |
| DELETE | `/tl/purge_warehouse` | 永久删除仓库（硬删除） |
| POST | `/tl/bind_warehouse_link` | 绑定库房单向关联 |
| DELETE | `/tl/unbind_warehouse_link` | 解绑库房单向关联 |
| POST | `/tl/batch_bind_warehouse_links` | 批量绑定出边 |
| POST | `/tl/batch_unbind_warehouse_links` | 批量解绑出边 |
| GET | `/tl/get_warehouse_links_list` | 库房关联列表（分页） |
| GET | `/tl/get_warehouse_links_outbound` | 库房出边列表 |
| GET | `/tl/get_warehouse_links_inbound` | 库房入边列表 |
| PUT | `/tl/replace_warehouse_links_outbound` | 替换库房全部出边 |
| POST | `/tl/add_smelter` | 新建冶炼厂 |
| GET | `/tl/get_smelter` | 单个冶炼厂详情 |
| GET | `/tl/list_smelter_xunrongbao` | 循融宝发货状态列表 |
| GET | `/tl/get_smelters` | 冶炼厂列表 |
| GET | `/tl/get_missing_geo_info` | 地址经纬度缺失列表 |
| GET | `/tl/calculate_distance` | 两组经纬度球面距离（km） |
| POST | `/tl/update_smelter` | 修改冶炼厂信息 |
| POST | `/tl/set_smelter_xunrongbao` | 设置循融宝发货 |
| DELETE | `/tl/smelter_xunrongbao/{smelter_id}` | 关闭循融宝发货 |
| POST | `/tl/batch_set_smelters_xunrongbao` | 批量设置循融宝 |
| DELETE | `/tl/delete_smelter` | 删除冶炼厂（软删除） |
| DELETE | `/tl/purge_smelter` | 永久删除冶炼厂（硬删除） |
| POST | `/tl/batch_delete_warehouses` | 批量停用仓库 |
| POST | `/tl/batch_delete_smelters` | 批量停用冶炼厂 |
| GET | `/tl/get_categories` | 品类列表 |
| POST | `/tl/upload_variety` | 上传品种 |
| POST | `/tl/get_comparison` | 获取比价表 |
| GET | `/tl/get_comparison_options` | 比价取价/最优价口径选项 |
| POST | `/tl/upload_price_table` | 上传价格表（OCR/VLM） |
| GET | `/tl/download_quote_list_template_excel` | 下载报价列表导入模板 |
| POST | `/tl/upload_price_table_excel` | 上传报价列表 xlsx |
| POST | `/tl/confirm_price_table` | 确认并写入报价 |
| POST | `/tl/manual_quote` | 手写录入报价 |
| POST | `/tl/update_quote_detail` | 按 id 修改单条报价明细 |
| GET | `/tl/get_quote_details_list` | 报价数据列表 |
| GET | `/tl/export_quote_details_excel` | 导出报价 Excel |
| POST | `/tl/export_quote_details_excel` | 导出报价 Excel（POST） |
| POST | `/tl/upload_freight` | 上传运费 |
| POST | `/tl/download_freight_template_excel` | 下载运费导入模板 |
| POST | `/tl/import_freight_excel` | 导入运费 Excel |
| GET | `/tl/get_freight_list` | 运费列表 |
| POST | `/tl/update_freight` | 编辑运费 |
| DELETE | `/tl/delete_freight` | 删除运费 |
| GET | `/tl/get_category_mapping` | 品类映射表 |
| POST | `/tl/get_purchase_suggestion` | 采购建议（LLM） |
| GET | `/tl/get_tax_rates` | 获取税率表 |
| POST | `/tl/upsert_tax_rates` | 批量设置税率 |
| DELETE | `/tl/delete_tax_rate` | 删除某冶炼厂某税率 |
| POST | `/tl/update_category_mapping` | 更新品类映射 |
| POST | `/tl/update_category_row` | 按行修改品类别名 |
| DELETE | `/tl/delete_category` | 删除品类分组（软删除） |
| DELETE | `/tl/delete_category_row` | 删除单条品类别名（软删除） |
| GET | `/tl/province_benchmark_prices` | 省份对标城市定价列表（历史；支持仅当前有效） |
| POST | `/tl/province_benchmark_prices` | 新增省份对标城市定价 |
| PUT | `/tl/province_benchmark_prices/{price_id}` | 修改省份对标定价历史行 |
| DELETE | `/tl/province_benchmark_prices/{price_id}` | 删除省份对标定价历史行 |
| GET | `/tl/smelter_calibration_prices` | 冶炼厂标定价格列表 |
| POST | `/tl/smelter_calibration_prices` | 新增冶炼厂标定价格 |
| PUT | `/tl/smelter_calibration_prices/{price_id}` | 修改冶炼厂标定价格历史行 |
| DELETE | `/tl/smelter_calibration_prices/{price_id}` | 删除冶炼厂标定价格历史行 |
| GET | `/tl/warehouse_spread_configs` | 库房对标差额与毛利配置列表 |
| POST | `/tl/warehouse_spread_configs` | 新增库房对标差额配置 |
| PUT | `/tl/warehouse_spread_configs/{config_id}` | 修改库房对标差额配置 |
| DELETE | `/tl/warehouse_spread_configs/{config_id}` | 删除库房对标差额配置 |
| GET | `/tl/ai_pricing_analysis` | 库房 AI 定价对标分析（实时计算） |
| POST | `/tl/ai_pricing_snapshots` | 生成分析快照 |
| GET | `/tl/ai_pricing_snapshots` | 快照列表 |
| GET | `/tl/ai_pricing_snapshots/{snapshot_id}` | 快照详情（含明细） |
| PUT | `/tl/ai_pricing_snapshots/{snapshot_id}` | 更新快照元数据 |
| DELETE | `/tl/ai_pricing_snapshots/{snapshot_id}` | 删除快照 |
| PUT | `/tl/ai_pricing_snapshots/{snapshot_id}/items/{item_id}` | 修改快照明细备注 |
| DELETE | `/tl/ai_pricing_snapshots/{snapshot_id}/items/{item_id}` | 删除快照明细行 |

---

## 6. 智能预测模块（`INTELLIGENT_PREDICTION_ENABLED` 开启时）

以下路径均**无前缀**（与 `app/main.py` 中 `include_router` 一致），与 `/tl` 并列。

### 6.1 智能预测 `/predict`

| 方法 | 路径 | 摘要 |
|------|------|------|
| GET | `/predict/dict-addresses` | 按名称查 TL 字典中仓库/冶炼厂地址与经纬度 |
| GET | `/predict/operation-audit` | 智能预测操作审计分页 |
| POST | `/predict` | 同步批量预测并写库 |
| POST | `/predict/async` | 异步批量预测（Celery），返回 `task_id` / `predict_id` |
| GET | `/predict/results` | 分页查询已落库预测结果 |
| GET | `/predict/batches/{predict_id}` | 查询异步批次状态 |
| GET | `/predict/batches/{predict_id}/download` | 下载批次导出 Excel |
| GET | `/predict/dimension-options` | 已落库预测结果筛选维度 |

### 6.2 规则预测（送货量）`/forecast`

| 方法 | 路径 | 摘要 |
|------|------|------|
| GET | `/forecast/chart` | 预测图表数据 |
| GET | `/forecast/details` | 预测明细分页 |
| GET | `/forecast/export` | 导出预测明细 xlsx |
| GET | `/forecast/dimension-options` | 筛选维度列表（与送货历史同源） |

### 6.3 送货历史 `/delivery-history`

与 **`/history`** 为同一路由器的重复挂载（**`/history/*` 不在 OpenAPI 中展示**），路径等价，建议客户端统一使用 **`/delivery-history`**。

| 方法 | 路径 | 摘要 |
|------|------|------|
| GET | `/delivery-history/template/fields` | 导入模板列定义 JSON |
| GET | `/delivery-history/template` | 下载导入模板 xlsx |
| GET | `/delivery-history/template.csv` | 下载导入模板 CSV |
| POST | `/delivery-history/import` | 导入送货历史 |
| GET | `/delivery-history/statistics` | 统计分析 |
| GET | `/delivery-history/daily-weight-matrix` | 按日合计重量透视 |
| PUT | `/delivery-history/{record_id}` | 更新单条记录 |
| GET | `/delivery-history` | 分页查询列表 |
| DELETE | `/delivery-history/batch-delete` | 批量删除 |
| POST | `/delivery-history/purge-all` | 一键清空（需密钥请求头，见 Swagger） |
| GET | `/delivery-history/dimension-options` | 筛选维度列表 |

### 6.4 知识库预留 `/knowledge`

| 方法 | 路径 | 摘要 |
|------|------|------|
| GET | `/knowledge/search` | 预留，501 |
| POST | `/knowledge/ingest` | 预留，501 |

### 6.5 依赖说明（交付环境）

异步预测、导出等依赖 **Celery Broker**、**Redis**、**异步数据库 URL** 等，详见 **`.env.example`** 中「智能送货量预测」一节。

---

## 7. AI 鉴伪模块（`AI_DETECTION_ENABLED` 开启时）

前缀：**`/ai-detection`**。子路径中保留 **`/api/v1/...`、`/api/v3/...`** 历史形态，全路径示例：`POST /ai-detection/api/v1/image-detection/detect`。

| 方法 | 路径 | 摘要 |
|------|------|------|
| POST | `/ai-detection/api/v1/image-detection/detect` | 单图单框同步鉴伪 |
| GET | `/ai-detection/api/v1/history` | 检测历史分页 |
| GET | `/ai-detection/api/v1/history/{record_id}/image` | 历史归档图 |
| POST | `/ai-detection/api/v3/detect` | 提交异步鉴伪任务 |
| GET | `/ai-detection/api/v3/result/{task_id}` | 查询任务结果 |
| GET | `/ai-detection/api/v3/result/{task_id}/visualization` | 结果可视化图 |
| DELETE | `/ai-detection/api/v3/task/{task_id}` | 取消或删除任务 |

**注意**：首次加载 OCR/模型可能较慢，经 Nginx 时需调大 **`proxy_read_timeout`**，或使用 **`AI_DETECTION_PRELOAD`** 等策略，详见各路由的 Swagger 描述。


---

> 更细的 TL 请求/响应 JSON 示例见本文 **第 5 章**。

---

# 5. TL 与认证接口详解（含 JSON 示例）

---

## 接口0：添加仓库
- 方法：`POST`
- 路由：`/tl/add_warehouse`
- 传入：仓库名
- 输出：仓库id、是否新建
- 逻辑说明：按名称查 `dict_warehouses`，已存在则返回现有id，不存在则自动新建
- 模拟请求JSON：
```json
{ "仓库名": "北京仓" }
```
- 模拟返回JSON（已存在）：
```json
{ "code": 200, "msg": "仓库已存在", "仓库id": 101, "新建": false }
```
- 模拟返回JSON（新建）：
```json
{ "code": 200, "msg": "仓库新建成功", "仓库id": 103, "新建": true }
```

---

## 接口1：获取仓库列表
- 方法：`GET`
- 路由：`/tl/get_warehouses`
- 传入（Query，可选）：`keyword` — 仓库名模糊匹配（`LIKE %keyword%`）；不传则返回全部启用仓库
- 输出：仓库id、仓库名
- 数据来源：`dict_warehouses` 表（`is_active=1`）
- 模拟请求：`GET /tl/get_warehouses?keyword=北京`
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": [
    { "仓库id": 101, "仓库名": "仓1" },
    { "仓库id": 102, "仓库名": "仓2" }
  ]
}
```

---

## 接口1a：地址经纬度缺失列表
- 方法：`GET`
- 路由：`/tl/get_missing_geo_info`
- 传入（Query，可选）：`include_inactive` — 是否包含已停用的仓库和冶炼厂，默认 `false`
- 输出：缺失经度或纬度的仓库、冶炼厂列表，以及汇总数量
- 逻辑说明：查询 `dict_warehouses` 与 `dict_factories` 中 `longitude IS NULL OR latitude IS NULL` 的记录，默认仅返回启用数据
- 模拟请求：`GET /tl/get_missing_geo_info`
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": {
    "warehouses": [
      {
        "仓库id": 101,
        "仓库名": "北京仓",
        "地址": "朝阳区示例路1号",
        "省": "北京市",
        "市": "北京市",
        "区": "朝阳区",
        "经度": null,
        "纬度": null,
        "is_active": 1,
        "类型": "合作库房",
        "缺失字段": ["经度", "纬度"]
      }
    ],
    "smelters": [
      {
        "冶炼厂id": 201,
        "冶炼厂": "华北冶炼厂",
        "地址": "示例地址",
        "省": "河北省",
        "市": "保定市",
        "区": "竞秀区",
        "经度": null,
        "纬度": 38.8,
        "is_active": 1,
        "缺失字段": ["经度"]
      }
    ],
    "summary": { "warehouses": 1, "smelters": 1, "total": 2 }
  }
}
```

---

## 接口1b：修改仓库信息
- 方法：`POST`
- 路由：`/tl/update_warehouse`
- 传入：仓库id，仓库名（可选），is_active（可选）
- 输出：状态提示
- 逻辑说明：至少要传一个可修改字段；`仓库名` 需保持唯一（与其它仓库不重复）
- 模拟请求JSON：
```json
{ "仓库id": 101, "仓库名": "北京仓（新）", "is_active": true }
```
- 模拟返回JSON：
```json
{ "code": 200, "msg": "仓库信息修改成功" }
```

---

## 接口1c：删除仓库（软删除）
- 方法：`DELETE`
- 路由：`/tl/delete_warehouse`
- 传入（Query参数）：`warehouse_id`
- 输出：状态提示
- 逻辑说明：将 `dict_warehouses.is_active` 置为0，不物理删除，避免运费等关联数据受外键约束影响
- 模拟请求：`DELETE /tl/delete_warehouse?warehouse_id=101`
- 模拟返回JSON：
```json
{ "code": 200, "msg": "仓库已删除" }
```

---

## 接口1d：新建冶炼厂
- 方法：`POST`
- 路由：`/tl/add_smelter`
- 传入：冶炼厂名
- 输出：冶炼厂id、是否新建
- 逻辑说明：按名称查 `dict_factories`，已存在且启用则返回现有id；已存在但停用则自动恢复启用；不存在则新建
- 模拟请求JSON：
```json
{ "冶炼厂名": "华北冶炼厂" }
```
- 模拟返回JSON（已存在）：
```json
{ "code": 200, "msg": "冶炼厂已存在", "冶炼厂id": 201, "新建": false }
```
- 模拟返回JSON（新建）：
```json
{ "code": 200, "msg": "冶炼厂新建成功", "冶炼厂id": 203, "新建": true }
```

---

## 接口2：获取冶炼厂列表
- 方法：`GET`
- 路由：`/tl/get_smelters`
- 传入：无
- 输出：冶炼厂id、冶炼厂名
- 数据来源：`dict_factories` 表（`is_active=1`）
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": [
    { "冶炼厂id": 201, "冶炼厂": "华北冶炼厂" },
    { "冶炼厂id": 202, "冶炼厂": "华东冶炼厂" }
  ]
}
```

---

## 接口2b：修改冶炼厂信息
- 方法：`POST`
- 路由：`/tl/update_smelter`
- 传入：冶炼厂id，冶炼厂名（可选），is_active（可选）
- 输出：状态提示
- 逻辑说明：至少要传一个可修改字段；`冶炼厂名` 需保持唯一
- 模拟请求JSON：
```json
{ "冶炼厂id": 201, "冶炼厂名": "华北冶炼厂（新）", "is_active": true }
```
- 模拟返回JSON：
```json
{ "code": 200, "msg": "冶炼厂信息修改成功" }
```

---

## 接口2c：删除冶炼厂（软删除）
- 方法：`DELETE`
- 路由：`/tl/delete_smelter`
- 传入（Query参数）：`smelter_id`
- 输出：状态提示
- 逻辑说明：将 `dict_factories.is_active` 置为0，不物理删除，避免关联数据受影响
- 模拟请求：`DELETE /tl/delete_smelter?smelter_id=201`
- 模拟返回JSON：
```json
{ "code": 200, "msg": "冶炼厂已删除" }
```

---

## 接口3：获取品类列表
- 方法：`GET`
- 路由：`/tl/get_categories`
- 传入：无
- 输出：品类id、品类名（聚合所有关联名称，用「、」分隔）
- 数据来源：`dict_categories` 表（`is_active=1`），按 `category_id` 分组，`GROUP_CONCAT(name)` 聚合
- 逻辑说明：相同品类的不同名称共用一个 `category_id`，读表时通过 id 将所有名称以"铜、紫铜、黄铜"的形式拼接输出
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": [
    { "品类id": 301, "品类名": "铜、紫铜、黄铜" },
    { "品类id": 302, "品类名": "铝、氧化铝、电解铝" },
    { "品类id": 303, "品类名": "锌" },
    { "品类id": 304, "品类名": "铅" }
  ]
}
```

---

## 接口4：获取比价表
- 方法：`POST`
- 路由：`/tl/get_comparison`
- 传入：选中仓库id列表、冶炼厂id列表、品类id列表、price_type（取价口径）、**吨数**（可选，默认 `1`；未传 **品类吨数列表** 时所有品类共用）、**品类吨数列表**（可选；多品类不同重量混选时使用：`[{"品类id":301,"吨数":5},{"品类id":302,"吨数":12}]`，须与 **品类id列表** 中每个 id **恰好一条**，覆盖后各行按对应品类吨数计算总价/运费/利润）、**报价日期**（可选 `YYYY-MM-DD`；传入则只取该日的 `quote_details`，不传则以比价基准日（默认 `Asia/Shanghai` 当天，环境变量 `QUOTE_COMPARISON_TZ`）为参照，每个冶炼厂+品种在 `quote_details` 中取 **与该日日历距离最近** 的 `quote_date` 一条；若多条距离相同，则取 **`created_at` 最新** 的一条）。**明细范围**：对每个「选中仓库 × 启用中的选中冶炼厂 × 选中品类」均返回一行；`冶炼厂id列表` 去掉停用厂后 **顺序与请求一致**（去重）；若某仓库—冶炼厂尚无 `freight_rates` 记录，**运费单价按 0** 仍返回该行。计价关系：**总价** = **单价**×**该品类吨数**（与 **`报价金额`** 一致，**`单价`/`报价`** 为折合后元/吨不含税）；**运费** = **运费单价**×**该品类吨数**（与 **`总运费`** 一致，**`运费单价`** 来自 `freight_rates.price_per_ton`，无记录时为 `0`，元/吨）；**利润** = **总价 − 运费**（与 **`报价金额` − `总运费`** 一致）。**最终比价**以 **`利润`** 为准；明细按「最优价排序口径」下利润排序；**`冶炼厂利润排行`** 为各冶炼厂明细 **`利润`** 之和。**`最优价各口径利润`** = 该口径元/吨单价×该品类吨数 − **总运费**（与主行共用按该品类折算的全程运费金额）。
- 输出：
  - `data`：明细列表含 **`单价`**、**`总价`**、**`运费单价`**、**`运费`**（全程运费元）、**`总运费`**（与 `运费` 同值）、**`报价`**、**`报价金额`**（与 `总价` 同值）、`运费计价方式`（固定 `per_ton`）、`利润`、`最优价各口径利润` 等；若 `price_type` 为含 1%/3%/13% 增值税，先将含税价折合为不含税再写入 `单价`/`报价`；无报价时 `单价`/`报价`/`总价`/`报价金额` 为 `null`，`报价来源` 为 `unavailable`，此时 **`利润`** = **−`运费`**（即 −`总运费`）
  - **`冶炼厂利润排行`**：按冶炼厂汇总明细中的 `利润` 之和，**从高到低**排序，每项含 `冶炼厂id`、`冶炼厂`、`利润`
  - **前端提示**：到库利润请直接使用返回的 **`利润`**（= 总价 − 运费）；**`运费单价`** 为元/吨，**`运费`**/**`总运费`** 为按吨数折算后的全程运费（元），勿混淆。

**price_type 可选值：**
| 值 | 含义 |
|---|---|
| `null` | 普通价（不含税基础价） |
| `"1pct"` | 含1%增值税 |
| `"3pct"` | 含3%增值税 |
| `"13pct"` | 含13%增值税 |
| `"normal_invoice"` | 普通发票价 |
| `"reverse_invoice"` | 反向发票价 |

**报价取价逻辑（按优先级）：**
1. 报价表中直接有对应 `price_type` 的价格 → 直接使用，`报价来源: "direct"`
2. 报价表有不含税基础价（`unit_price`）+ 该冶炼厂税率表有目标税率 → 正向换算，`报价来源: "calc_from_base"`
3. 报价表有其他已知含税价 + 税率表有对应税率 → 先反算不含税价，再正向换算，`报价来源: "calc_from_3pct"` 等
4. **`price_type` 为普通价（不含税）时**：若仅有 `price_normal_invoice` / `price_reverse_invoice` 有值（无基准价与 1%/3%/13% 含税列），按 **不含税单价** 理解，与入库推算逻辑一致，`报价来源: "direct_price_normal_invoice"` / `"direct_price_reverse_invoice"`
5. 以上均无 → `报价: null`，`报价来源: "unavailable"`

**匹配说明**：比价按 `dict_categories` 中该品类的 **全部别名** 与 `quote_details.category_name` 关联；比较时对库中品种名做 **`TRIM` 去首尾空白**，避免 OCR/录入多空格导致整表 `unavailable`。

- 模拟请求JSON（多品类不同吨数）：
```json
{
  "选中仓库id列表": [101, 102],
  "冶炼厂id列表": [201],
  "品类id列表": [301, 302],
  "price_type": "3pct",
  "品类吨数列表": [
    { "品类id": 301, "吨数": 10 },
    { "品类id": 302, "吨数": 5.5 }
  ]
}
```
- 仍兼容仅传单一 **`吨数`**（所有品类共用），例如 `"吨数": 10.5` 且省略 **`品类吨数列表`**。
- 模拟返回JSON（与上行请求中铜 10 吨、铝 5.5 吨对应；明细行 **`吨数`** 为该品类本次所用吨数）：
```json
{
  "code": 200,
  "data": [
    { "仓库id": 101, "冶炼厂id": 201, "品类id": 301, "仓库": "北京仓", "冶炼厂": "华北冶炼厂", "品类": "铜", "price_type": "3%增值税", "吨数": 10, "单价": 9453.4, "总价": 94534.0, "运费单价": 200, "运费": 2000, "总运费": 2000, "报价": 9453.4, "报价金额": 94534.0, "报价来源": "direct", "利润": 92534.0 },
    { "仓库id": 101, "冶炼厂id": 201, "品类id": 302, "仓库": "北京仓", "冶炼厂": "华北冶炼厂", "品类": "铝", "price_type": "3%增值税", "吨数": 5.5, "单价": 8106.8, "总价": 44587.4, "运费单价": 200, "运费": 1100, "总运费": 1100, "报价": 8106.8, "报价金额": 44587.4, "报价来源": "calc_from_base", "利润": 43487.4 },
    { "仓库id": 102, "冶炼厂id": 201, "品类id": 301, "仓库": "上海仓", "冶炼厂": "华北冶炼厂", "品类": "铜", "price_type": "3%增值税", "吨数": 10, "单价": null, "总价": null, "运费单价": 300, "运费": 3000, "总运费": 3000, "报价": null, "报价金额": null, "报价来源": "unavailable", "利润": -3000.0 }
  ],
  "冶炼厂利润排行": [
    { "冶炼厂id": 201, "冶炼厂": "华北冶炼厂", "利润": 133021.4 }
  ]
}
```

---

## 接口5：上传价格表（VLM识别）
- 方法：`POST`
- 路由：`/tl/upload_price_table`
- 传入：图片文件（FormData，支持批量）
- 输出：每张图的全量识别数据（`full_data`）+ 映射后的前端可编辑条目（`items`）
- 支持格式：jpg、png、bmp、webp
- 逻辑说明：
  1. 后端为每张图片生成UUID文件名，保存到 `uploads/price_tables/` 目录
  2. 调用千问VLM对每张图片进行全量识别，提取公司名、日期、表头、各行价格、备注等完整信息
  3. 将识别结果映射为前端可编辑的 `items` 列表（`冶炼厂名/品类名/各税率价格`）
  4. `full_data` 由前端保留，确认后原样回传给接口5b存档，不需要前端解析
- 模拟请求（FormData）：
```
file: [报价单1.jpg]
```
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": {
    "details": [
      {
        "image": "报价单1.jpg",
        "success": true,
        "full_data": {
          "company_name": "山西亿晨环保科技有限公司",
          "doc_title": "废铅酸蓄电池回收价格报价表",
          "execution_date": "2026年03月24日",
          "quote_date": "2026-03-24",
          "price_column_type": "1_3_percent",
          "headers": ["电池名称", "含1%普票单价", "含3%专票单价", "质检标准"],
          "rows": [
            { "index": 1, "category": "电动车电池", "price_1pct_vat": 9550, "price_3pct_vat": 9737, "remark": "控水" }
          ],
          "footer_notes": ["价格随市场波动每日更新"],
          "raw_full_text": "..."
        },
        "items": [
          { "冶炼厂名": "山西亿晨环保科技有限公司", "冶炼厂id": null, "品类名": "电动车电池", "品类id": null, "价格": null, "价格_1pct增值税": 9550, "价格_3pct增值税": 9737, "价格_13pct增值税": null, "普通发票价格": null, "反向发票价格": null }
        ]
      }
    ]
  }
}
```

---

## 接口5b：确认价格表写入
- 方法：`POST`
- 路由：`/tl/confirm_price_table`
- 传入：报价日期、full_data（可选，VLM全量数据原样回传）、报价明细列表（前端确认/修改后）
- 输出：写入状态
- 逻辑说明：
  1. 前端对接口5返回的 `items` 确认/修正后，连同原始 `full_data` 一起提交
  2. `冶炼厂id` 为 null 时：按名称查 `dict_factories`，存在则复用，不存在则自动新建
  3. 品类处理：按 `品类名` 查 `dict_categories`，存在则复用其 `category_id`，不存在则自动新建（分配新的 `category_id`）
  4. `full_data` 存入 `quote_table_metadata` 表（按 `factory_id + quote_date` 唯一键，已存在则更新）
  5. 每条明细以 `(报价日期, 冶炼厂id, 品类名)` 为唯一键写入 `quote_details`，已存在则更新价格，并关联 `metadata_id`
  6. **重要变更**：价格表现在存储品类名称而不是品类ID，这样当品类映射变化时，价格表无需更新
- 模拟请求JSON：
```json
{
  "报价日期": "2026-03-24",
  "full_data": {
    "company_name": "山西亿晨环保科技有限公司",
    "execution_date": "2026年03月24日",
    "quote_date": "2026-03-24",
    "source_image": "报价单1.jpg",
    "headers": ["电池名称", "含1%普票单价", "含3%专票单价"],
    "rows": [...],
    "footer_notes": ["价格随市场波动每日更新"],
    "raw_full_text": "..."
  },
  "数据": [
    { "冶炼厂名": "山西亿晨环保科技有限公司", "冶炼厂id": 1, "品类名": "电动车电池", "品类id": 3, "价格": null, "价格_1pct增值税": 9550, "价格_3pct增值税": 9737, "价格_13pct增值税": null, "普通发票价格": null, "反向发票价格": null },
    { "冶炼厂名": "山西亿晨环保科技有限公司", "冶炼厂id": 1, "品类名": "摩托车电池", "品类id": null, "价格": null, "价格_1pct增值税": 8500, "价格_3pct增值税": 8665, "价格_13pct增值税": null, "普通发票价格": null, "反向发票价格": null }
  ]
}
```
- 模拟返回JSON：
```json
{
  "code": 200,
  "msg": "写入成功：新增 8 条，更新 2 条"
}
```

---

## 接口5c：报价数据列表
- 方法：`GET`
- 路由：`/tl/get_quote_details_list`
- 传入（Query，均可选）：`factory_id`（冶炼厂下拉，传冶炼厂 id）、`quote_date`（精确，YYYY-MM-DD）、`date_from` / `date_to`（报价日期区间）、**`start_date` / `end_date`**（与 `date_from` / `date_to` 等价，便于对齐「日期范围」控件）、`category_name` 或 **`variety`**（品种；同时传时以 `variety` 为准）、`category_exact`（默认 `false` 模糊匹配；下拉选具体品种名时建议 `true`）、`page`（默认1）、`page_size`（默认50，最大500）、`response_format`（默认 `full`）
- **`response_format` 说明**
  - `full`：`list` 每条为库表全量列（含 metadata、各价格列、时间戳等），适合后台或详情。
  - `table`：**与「报价数据列表」前端表格列一致**，字段为：`id`、`日期`、`冶炼厂`、`品种`、`基准价`、`3%含税价`、`13%含税价`（分别对应 `quote_details` 的报价日期、冶炼厂名、品类名、`unit_price`、`price_3pct_vat`、`price_13pct_vat`）。
- 数据来源：`quote_details` 联表 `dict_factories`
- 排序：报价日期倒序，再冶炼厂、品类名、id
- 列表页推荐请求（对齐查询条件区）：`GET /tl/get_quote_details_list?response_format=table&factory_id=66&start_date=2026-01-01&end_date=2026-03-31&variety=电动车电池&category_exact=true&page=1&page_size=20`
- 模拟返回JSON（`response_format=full`）：
```json
{
  "code": 200,
  "data": {
    "total": 120,
    "list": [
      {
        "id": 1,
        "报价日期": "2026-03-24",
        "冶炼厂id": 1,
        "冶炼厂": "默认冶炼厂",
        "品类名": "电动车电池",
        "metadata_id": 5,
        "普通价": 9000.0,
        "价格_1pct增值税": 9550.0,
        "价格_3pct增值税": 9737.0,
        "价格_13pct增值税": null,
        "普通发票价格": null,
        "反向发票价格": null,
        "创建时间": "2026-03-24 10:00:00",
        "更新时间": "2026-03-24 10:00:00"
      }
    ]
  }
}
```
- 模拟返回JSON（`response_format=table`，对齐列表页表头）：
```json
{
  "code": 200,
  "data": {
    "total": 0,
    "list": [
      {
        "id": 1,
        "日期": "2026-03-24",
        "冶炼厂": "默认冶炼厂",
        "品种": "电动车电池",
        "基准价": 9000.0,
        "3%含税价": 9737.0,
        "13%含税价": null
      }
    ]
  }
}
```

---

## 接口5d：导出报价数据 Excel
- 方法：`GET`
- 路由：`/tl/export_quote_details_excel`
- 传入（Query）：与接口 5c **相同的筛选参数**（`factory_id`、`quote_date`、`date_from`/`date_to` 或 `start_date`/`end_date`、`category_name` 或 `variety`、`category_exact` 等），**无分页**；服务端最多导出 50000 行（可在代码中调整上限）。
- 输出：`.xlsx` 文件下载；表头为 **日期、冶炼厂、品种、基准价、3%含税价、13%含税价**（与列表页一致）。
- 依赖：需安装 `openpyxl`（已写入 `requirements.txt` / `pyproject.toml`）。
- 模拟请求：`GET /tl/export_quote_details_excel?factory_id=66&start_date=2026-01-01&end_date=2026-03-31&variety=电动车电池&category_exact=true`

---

## 接口6：上传运费
- 方法：`POST`
- 路由：`/tl/upload_freight`
- 传入：`{仓库, 冶炼厂, 运费}` **列表**（支持批量）
- 输出：状态提示
- 逻辑说明：根据仓库名和冶炼厂名查找对应ID，以当日日期为生效日期，写入 `freight_rates` 表；同一 (仓库, 冶炼厂, 日期) 已存在则更新运费
- 模拟请求JSON：
```json
[
  { "仓库": "北京仓", "冶炼厂": "华北冶炼厂", "运费": 200 },
  { "仓库": "上海仓", "冶炼厂": "华东冶炼厂", "运费": 300 }
]
```
- 模拟返回JSON：
```json
{
  "code": 200,
  "msg": "运费数据已存入数据库"
}
```

---

## 接口6b：运费列表
- 方法：`GET`
- 路由：`/tl/get_freight_list`
- 传入（Query，均可选）：`warehouse_id`、`factory_id`、`date_from`、`date_to`（生效日期区间，YYYY-MM-DD）、`page`（默认1）、`page_size`（默认50，最大500）
- 输出：`total` + `list`（每条含 id、仓库/冶炼厂 id 与名称、运费、生效日期、创建/更新时间）
- 数据来源：`freight_rates` 联表 `dict_warehouses`、`dict_factories`
- 排序：生效日期倒序，再 id
- 模拟请求：`GET /tl/get_freight_list?warehouse_id=1&date_from=2026-04-01`
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": {
    "total": 3,
    "list": [
      {
        "id": 1,
        "仓库id": 1,
        "仓库名": "默认仓库",
        "冶炼厂id": 1,
        "冶炼厂": "默认冶炼厂",
        "运费": 200.0,
        "生效日期": "2026-04-01",
        "创建时间": "2026-04-01 08:00:00",
        "更新时间": "2026-04-01 08:00:00"
      }
    ]
  }
}
```

---

## 接口6c：编辑运费
- 方法：`POST`
- 路由：`/tl/update_freight`
- 传入（JSON）：`运费id`（必填，`freight_rates` 主键，与接口 6b 列表中的 `id` 一致）、`运费`（必填，每吨运费，元，≥0）、`生效日期`（可选，`YYYY-MM-DD`；不传则保持原日期；若修改，同一仓库+冶炼厂组合下目标日期不能已有**其它**记录）
- 输出：状态提示
- 逻辑说明：按主键更新 `price_per_ton`；可选更新 `effective_date`，受表上 `(factory_id, warehouse_id, effective_date)` 唯一约束
- 模拟请求JSON：
```json
{ "运费id": 1, "运费": 220.5, "生效日期": "2026-04-15" }
```
- 仅改单价、不改生效日期时可省略 `生效日期`：
```json
{ "运费id": 1, "运费": 218 }
```
- 模拟返回JSON：
```json
{ "code": 200, "msg": "运费已更新" }
```

---

## 接口6d：删除运费
- 方法：`DELETE`
- 路由：`/tl/delete_freight`
- 传入（Query）：`freight_id` — `freight_rates` 主键，与接口 6b 列表中的 `id` 一致
- 输出：状态提示
- 逻辑说明：物理删除该条运费记录；`freight_id` 无效返回 400；记录不存在返回 404
- 模拟请求：`DELETE /tl/delete_freight?freight_id=1`
- 模拟返回JSON：
```json
{ "code": 200, "msg": "运费已删除" }
```

---

## 接口7a：获取品类映射表
- 方法：`GET`
- 路由：`/tl/get_category_mapping`
- 传入：无
- 输出：所有品类id及其对应的全部名称列表（第一个为主名称），以及 **`别名行`**（含 `行id`，供接口7b/7d 使用）
- 数据来源：`dict_categories` 表（`is_active=1`），按 `category_id` 分组，`is_main=1` 的排在首位
- 用途：前端展示当前映射关系；整组替换可调用接口7；单条改名/删别名可调用接口7b、7d；整组停用可调用接口7c
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": [
    {
      "品类id": 301,
      "品类名称": ["铜", "紫铜", "黄铜"],
      "别名行": [
        { "行id": 1, "名称": "铜", "是否主名称": true },
        { "行id": 2, "名称": "紫铜", "是否主名称": false },
        { "行id": 3, "名称": "黄铜", "是否主名称": false }
      ]
    },
    {
      "品类id": 302,
      "品类名称": ["铝", "氧化铝", "电解铝"],
      "别名行": [
        { "行id": 10, "名称": "铝", "是否主名称": true },
        { "行id": 11, "名称": "氧化铝", "是否主名称": false },
        { "行id": 12, "名称": "电解铝", "是否主名称": false }
      ]
    }
  ]
}
```

---

## 接口7：更新品类映射表
- 方法：`POST`
- 路由：`/tl/update_category_mapping`
- 传入：品类映射列表（支持批量，每条含 **`品类id`**、**`品类名称`** 列表；可选 **`仅追加别名`**，默认 `false`）
- 兼容字段名（与上面等价）：`品类ID` / `category_id`；`names` / `aliasNames`；`append_only` / `appendOnly` / `append_aliases` / `appendAliases`。`品类名称` 也可传 **单个字符串**（视为只含一项的列表）
- 输出：状态提示
- **批量一次提交**：`dict_categories.name` 在库中 **全局唯一**。若同一品种名出现在多条「整组替换」（未设 `仅追加别名`）里，服务端会先 **消解冲突**：该名只保留在 **`品类名称` 条数更多** 的那一条；条数相同则保留在 **JSON 数组中更靠前** 的那一条。被抢光名称的分组（过滤后为空）会对 `品类id>0` 做 **整组软删除**。这样「主组里写了别名 + 另有一条只含重复名单」时，别名会留在主组，而不会在后面那条里「另起一组」把名称拽走。
- 逻辑说明：
  1. 名称列表中 **第一个为主名称**（`is_main=1`），用于比价表展示，其余为 `is_main=0`（在 **最终参与写入的列表** 上计算，见下）
  2. **`仅追加别名` = `false`（默认，整表保存）**：当 `品类id > 0` 时，该分组下原先启用、但 **未出现在本次 `品类名称` 列表中的别名** 会软删除（`is_active=0`）；提交列表即为最终别名集合（经上面批量冲突消解后的有效列表）
  3. **`仅追加别名` = `true`（只加别名）**：`品类id` **必须 >0**。先读出该分组当前所有启用别名（主名称在前），再与本次 `品类名称` **按顺序合并去重**（已有别名保留顺序，新名称接在后面），**不会**软删除旧别名。**此时 `品类名称` 只填要追加的别名即可，不要写主名称**（主名称仍用库中已有）。若分组在写入前已有启用行，则本次 **新插入** 的名称一律 `is_main=0`，不会变成主名称
  4. **`品类id ≤ 0`**：分配新的品类分组 id，仅写入本次名称（不能与 `仅追加别名: true` 同用）
  5. 该 `category_id` 下相关记录的 `is_main` 先全部置 0，再按最终列表顺序写回主名称
  6. 名称在库中已存在（任意分组、含已停用行）则更新其 `category_id`、`is_main` 并置为启用；不存在则插入新行
  7. 列表会去重（保留首次出现顺序），首尾空白会去掉；单项不能为空串，长度 ≤50
- 模拟请求JSON（整组替换，与旧行为一致）：
```json
[
  { "品类id": 301, "品类名称": ["铜", "紫铜", "黄铜"] },
  { "品类id": 302, "品类名称": ["铝", "氧化铝", "电解铝"] }
]
```
- 只增加别名（**不必**写主名称，也不必带上已有别名）：
```json
[
  { "品类id": 301, "品类名称": ["电解紫铜"], "仅追加别名": true }
]
```
- 全量覆盖各分组映射时**不要**带 `仅追加别名: true`，且每条 `品类名称` 须为**完整列表**（第一项主名称，其余别名），否则会误删未写上的旧别名
- 模拟返回JSON：
```json
{
  "code": 200,
  "msg": "品类映射表更新成功，数据已存入数据库"
}
```

---

## 接口7b：按行修改品类别名
- 方法：`POST`
- 路由：`/tl/update_category_row`
- 传入：JSON
  - **`行id`**（必填）：`dict_categories.row_id`，见接口7a `别名行`
  - **`品种名`**（可选）：新名称；与已有启用别名不能重复；改名后会 **`UPDATE quote_details.category_name`**，将历史报价里旧名称一并替换为新名称
  - **`设为主名称`**（可选）：传 `true` 时将该行设为该 `category_id` 下唯一主名称（`is_main=1`）
- 约束：至少需要 **非空的 `品种名`** 或 **`设为主名称`: true** 之一
- 模拟请求JSON：`{ "行id": 2, "品种名": "电解紫铜" }` 或 `{ "行id": 3, "设为主名称": true }`
- 模拟返回JSON：`{ "code": 200, "msg": "品类别名已更新" }`

---

## 接口7c：删除品类分组（软删除）
- 方法：`DELETE`
- 路由：`/tl/delete_category`
- 传入（Query）：**`品类id`**（`category_id`）
- 逻辑：将该分组下所有别名的 `is_active` 置为 `0`；不物理删除，不修改 `quote_details`
- 模拟请求：`DELETE /tl/delete_category?品类id=304`
- 模拟返回JSON：`{ "code": 200, "msg": "品类分组已删除", "影响行数": 2 }`

---

## 接口7d：删除单条品类别名（软删除）
- 方法：`DELETE`
- 路由：`/tl/delete_category_row`
- 传入（Query）：**`行id`**（`row_id`）
- 逻辑：将该行 `is_active=0`；若删除的是主名称，则同组剩余启用别名中 **`row_id` 最小** 的一行自动升为 `is_main=1`
- 模拟请求：`DELETE /tl/delete_category_row?行id=3`
- 模拟返回JSON：`{ "code": 200, "msg": "品类别名已删除" }`

---

## 接口T1：获取税率表
- 方法：`GET`
- 路由：`/tl/get_tax_rates`
- 传入（Query参数）：`factory_ids`（可选，逗号分隔的冶炼厂ID，不传则返回全部）
- 输出：税率记录列表
- 数据来源：`factory_tax_rates` 表
- 逻辑说明：每个冶炼厂对每种税率（1pct/3pct/13pct）存一行，由用户手动维护，用于比价表中的税率换算
- 模拟请求：`GET /tl/get_tax_rates?factory_ids=201,202`
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": [
    { "id": 1, "factory_id": 201, "factory_name": "华北冶炼厂", "tax_type": "1pct", "tax_rate": 0.01 },
    { "id": 2, "factory_id": 201, "factory_name": "华北冶炼厂", "tax_type": "3pct", "tax_rate": 0.03 },
    { "id": 3, "factory_id": 202, "factory_name": "华东冶炼厂", "tax_type": "3pct", "tax_rate": 0.03 }
  ]
}
```

---

## 接口T2：批量设置税率
- 方法：`POST`
- 路由：`/tl/upsert_tax_rates`
- 传入：税率列表（冶炼厂id、税率类型、税率值）
- 输出：状态提示
- 逻辑说明：存在则更新，不存在则插入（upsert）；`tax_rate` 为小数，如 `0.03` 表示3%；`tax_type` 有效值为 `1pct`/`3pct`/`13pct`
- 模拟请求JSON：
```json
{
  "items": [
    { "factory_id": 201, "tax_type": "1pct", "tax_rate": 0.01 },
    { "factory_id": 201, "tax_type": "3pct", "tax_rate": 0.03 },
    { "factory_id": 202, "tax_type": "3pct", "tax_rate": 0.03 }
  ]
}
```
- 模拟返回JSON：
```json
{
  "code": 200,
  "msg": "已保存 3 条税率记录"
}
```

---

## 接口T3：删除税率记录
- 方法：`DELETE`
- 路由：`/tl/delete_tax_rate`
- 传入（Query参数）：`factory_id`、`tax_type`
- 输出：状态提示
- 模拟请求：`DELETE /tl/delete_tax_rate?factory_id=201&tax_type=3pct`
- 模拟返回JSON：
```json
{
  "code": 200,
  "msg": "删除成功"
}
```

---

## 接口A7：采购建议
- 方法：`POST`
- 路由：`/tl/get_purchase_suggestion`
- 传入：
  - `warehouse_ids`：仓库 ID 列表（必填）
  - `demands`：需求列表（必填），每条仅含 **品类分组 ID**、**需求吨数**；**冶炼厂不传**，后端默认使用 `dict_factories` 中 **全部启用（`is_active=1`）** 的冶炼厂参与比价与运费计算
  - `price_type`（可选）：`null` 表示普通价；其余为 `1pct` / `3pct` / `13pct` / `normal_invoice` / `reverse_invoice`
- 输出：大语言模型生成的各仓库发车意见文案 + 原始结构化明细列表
- 逻辑说明：
  1. 读取全部启用冶炼厂 ID；查询各(仓库, 冶炼厂)最新运费、各(冶炼厂, 品类)最新报价（与 `get_comparison` 不传「报价日期」时一致：按比价基准日取日历距离最近的 `quote_date`，并列时 `created_at` 最新）
  2. 与 `get_comparison` 一致：**比价利润** = **报价×吨数 − 运费×吨数**；结构化数据中 **`比价利润元每吨`** = 报价(元/吨) − 运费(元/吨)，**`比价利润(元)`** = **`比价利润元每吨` × 需求吨数**（报价或运费缺失时为 `null`，勿再做「报价+运费」加算）
  3. 将数据交由大语言模型分析，要求：同仓库货物可混装、尽量整车（20–30 吨）、**优先比价利润更高**的方案
  4. 返回 LLM 文本 `suggestion` 与明细 `raw`
- 模拟请求JSON：
```json
{
  "warehouse_ids": [101, 102],
  "demands": [
    { "category_id": 301, "demand": 5.0 },
    { "category_id": 302, "demand": 3.0 }
  ],
  "price_type": null
}
```
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": {
    "suggestion": "## 各仓库发车意见表\n\n**北京仓**\n| 装车方案 | 品类 | 吨数 | 目的冶炼厂 | 比价利润 | 备注 |\n...",
    "raw": [
      {
        "冶炼厂": "华北冶炼厂",
        "品类": "铜",
        "需求吨数": 5.0,
        "报价(元/吨)": 9350,
        "仓库": "北京仓",
        "运费(元/吨)": 200,
        "比价利润元每吨": 9150,
        "比价利润(元)": 45750
      }
    ]
  }
}
```

---

# 用户认证模块 — 接口文档

---

## 接口A1：登录
- 方法：`POST`
- 路由：`/auth/login`
- 传入：username、password
- 输出：JWT token、用户信息
- 逻辑说明：校验账号密码，成功返回 JWT token 及用户基本信息；失败返回 401
- 模拟请求JSON：
```json
{ "username": "admin", "password": "123456" }
```
- 模拟返回JSON（成功）：
```json
{
  "code": 200,
  "msg": "登录成功",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user": { "id": 1, "username": "admin", "real_name": "管理员", "role": "admin", "phone": "13800138001", "email": "admin@example.com" }
}
```
- 模拟返回JSON（失败）：
```json
{ "detail": "账号或密码错误" }
```

---

## 接口A0：注册
- 方法：`POST`
- 路由：`/auth/register`
- 传入：username、real_name、password、phone（可选）
- 输出：新建用户id
- 逻辑说明：账号唯一，重复则返回 400；新用户默认角色为 `user`；密码后端加盐存储
- 模拟请求JSON：
```json
{ "username": "user2", "real_name": "张三", "password": "123456", "phone": "13800138003" }
```
- 模拟返回JSON（成功）：
```json
{ "code": 200, "msg": "注册成功", "id": 3 }
```
- 模拟返回JSON（账号重复）：
```json
{ "code": 400, "msg": "账号已存在" }
```

---

## 接口A2：获取用户列表
- 方法：`GET`
- 路由：`/auth/users`
- 权限：仅 admin
- 传入（Query参数）：keyword（可选，账号/姓名/手机模糊搜索）、role（可选，`admin`/`user`）、page（默认1）、page_size（默认10）
- 输出：用户列表、总条数
- 请求头：`Authorization: Bearer <token>`
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": {
    "total": 1,
    "list": [
      { "id": 1, "username": "admin", "real_name": "管理员", "role": "admin", "phone": "13800138001", "email": "admin@example.com" }
    ]
  }
}
```

---

## 接口A3：新增用户
- 方法：`POST`
- 路由：`/auth/users`
- 权限：仅 admin
- 传入：username、password、real_name（可选）、role（admin/user，默认user）、phone（可选）、email（可选）
- 输出：新建用户id
- 请求头：`Authorization: Bearer <token>`
- 模拟请求JSON：
```json
{ "username": "user2", "real_name": "张三", "password": "123456", "role": "user", "phone": "13800138003", "email": "zhangsan@example.com" }
```
- 模拟返回JSON：
```json
{ "code": 200, "msg": "用户创建成功", "id": 3 }
```

---

## 接口A4：修改用户角色
- 方法：`POST`
- 路由：`/auth/update_role`
- 权限：仅 admin
- 传入：id（用户id）、role（新角色）
- 请求头：`Authorization: Bearer <token>`
- 模拟请求JSON：
```json
{ "id": 2, "role": "admin" }
```
- 模拟返回JSON：
```json
{ "code": 200, "msg": "角色修改成功" }
```

---

## 接口A5：修改用户密码
- 方法：`POST`
- 路由：`/auth/change_password`
- 传入：id（用户id）、admin_key（服务端配置的固定密钥）、new_password
- 逻辑说明：校验 admin_key 与服务端 `JWT_SECRET_KEY` 一致后更新密码
- 模拟请求JSON：
```json
{ "id": 2, "admin_key": "your-secret-key", "new_password": "newpass123" }
```
- 模拟返回JSON：
```json
{ "code": 200, "msg": "密码修改成功" }
```

---

## 接口A6：删除用户
- 方法：`POST`
- 路由：`/auth/delete_user`
- 权限：仅 admin，且不可删除自己
- 传入：id（用户id）
- 逻辑说明：软删除，将 `is_active` 置0
- 请求头：`Authorization: Bearer <token>`
- 模拟请求JSON：
```json
{ "id": 3 }
```
- 模拟返回JSON：
```json
{ "code": 200, "msg": "用户已删除" }
```

---

## 对标定价 / 标定价格 / 库房差额 / AI 分析快照（TL）

前缀均为 `/tl`。默认日期与 TL 比价一致：环境变量 **`QUOTE_COMPARISON_TZ`**（默认 `Asia/Shanghai`）。分析接口依赖字典中存在名称为 **「金利」** 的冶炼厂（`dict_factories.name`）。

### 计算公式（实时分析与快照生成一致）

- **库房定价** = 对标城市定价 + 对标城市差额（差额来自库房配置 `pd_warehouse_spread_configs`；无配置则无差额参与合成）
- **毛利（计算版）** = 冶炼厂标定价格（金利最新标定） − 库房运费 − 库房定价

**库房运费**：优先 `freight_rates` 中（金利、`warehouse_id`）在口径日及以前的最新 `price_per_ton`；否则回退 `dict_warehouses.freight_amount`。

### 主要路由（详见 Swagger `/docs`）

| 说明 | 方法 | 路径 |
|------|------|------|
| 省份对标定价列表 | GET | `/tl/province_benchmark_prices` |
| 新增 / 改 / 删省份对标定价 | POST、PUT、DELETE | `/tl/province_benchmark_prices`、`/tl/province_benchmark_prices/{price_id}` |
| 冶炼厂标定价格 CRUD | GET/POST/PUT/DELETE | `/tl/smelter_calibration_prices`、`.../{price_id}` |
| 库房差额与毛利配置 CRUD | GET/POST/PUT/DELETE | `/tl/warehouse_spread_configs`、`.../{config_id}` |
| 实时 AI 对标分析（不落库） | GET | `/tl/ai_pricing_analysis` |
| 快照列表 / 创建 / 详情 / 更新元数据 / 删除 | GET/POST/PUT/DELETE | `/tl/ai_pricing_snapshots`、`/tl/ai_pricing_snapshots/{snapshot_id}` |
| 快照明细备注 / 删除明细 | PUT/DELETE | `/tl/ai_pricing_snapshots/{snapshot_id}/items/{item_id}` |

响应与其它 TL 接口一致时多为 `{ "code": 200, "data": ... }`；列表内含 `total`、`list`、`page` 等分页字段。数据库表说明见 **本文 [第 3 章](#3-数据库说明)**。

---

## 补充说明
1. 所有JSON中的 `code: 200` 为通用成功状态码
2. 需登录的接口请求头须携带 `Authorization: Bearer <token>`
3. token 过期或无效返回 `401`，非 admin 调用管理接口返回 `403`
4. 错误状态码：`400`（参数校验失败）、`401`（未登录）、`403`（权限不足）、`404`（资源不存在）、`500`（服务器内部错误）
5. 数据库连接使用 `autocommit=True`，写操作自动提交
6. LLM 配置通过环境变量注入：`LLM_API_KEY`、`LLM_BASE_URL`、`LLM_MODEL`
7. VLM 配置通过环境变量注入：`VLM_API_KEY`、`VLM_BASE_URL`、`VLM_MODEL`（默认 `qwen-vl-max-latest`）

---

# 6. 日志、测试、脚本与交付清单

## 6.1 日志、观测与排障

- **访问日志**（`app.access`）：方法、路径（含 query）、状态码、耗时 ms、客户端 IP。
- **根日志**：格式含 `operator`（JWT 解析失败为 `-`）、短 logger 名；第三方 HTTP 库默认抬高到 WARNING，减少刷屏。
- **财务审计**（`app.finance_audit`）：金额/税率/运费/报价等变更应通过 `finance_log.log_finance_event` 写入；默认可与主日志同目录下 `pd-max-finance.log`（见 `logging_config._resolve_finance_log_file_path`）。
- **长链路**：VLM、鉴伪同步；除 `VLM_REQUEST_TIMEOUT` 外，需对齐前端、网关、Nginx `proxy_read_timeout` / `client_max_body_size`（`.env.example` 中有分层说明）。

---

## 6.2 测试与质量

| 文件 | 内容 |
|------|------|
| `tests/test_ai_detection_amount_candidates.py` | 鉴伪金额候选与 OCR 分词相关单测 |
| `tests/test_ai_detection_inference_api.py` | 鉴伪推理 API 测 |

执行：`uv run pytest`（需在环境中安装 dev 依赖时以项目约定为准）。

---

## 6.3 脚本与运维样例

| 路径 | 用途 |
|------|------|
| `scripts/preload_ai_assets.py` | Docker 构建阶段预拉 EasyOCR / Torch 等资源 |
| `scripts/import_partner_warehouses_excel.py` | 合作库房 Excel 导入（`PARTNER_WAREHOUSES_EXCEL_PATH`） |
| `scripts/systemd/pd-max.service` | FastAPI 本机 systemd 示例（`python app.py`） |
| `scripts/systemd/pd-max-celery.service` | Celery Worker systemd 示例 |

---

## 6.4 版本与交付物清单建议

| 交付物 | 说明 |
|--------|------|
| 源代码 | 含 `pyproject.toml`、`uv.lock`、`Dockerfile`、`docker-compose.yml` |
| 配置模板 | `.env.example` |
| 文档 | `README.md`、`docs/`（api、后端接口、数据库、docker、**技术文档**） |
| 数据库 | 启动自愈式建表 + `ensure_*`；生产须有备份与变更评审 |
| 进程 | Web 必选；**使用异步预测/导出时 Celery Worker 必选**；Redis/MySQL 按模块依赖部署 |

---

## 6.5 文档修订记录

| 日期 | 说明 |
|------|------|
| 2026-05-13 | 初版：结构与配置索引 |
| 2026-05-13 | 补全：通读 `main`、`database`、`logging`、`intelligent_prediction`、`ai_detection` 路由、Celery/systemd、核心业务规则与表分组 |

---

*项目 PyPI 元数据名：`pd-max`（`pyproject.toml`）。对外产品名以 FastAPI `title`「TL比价系统」及 README 为准。*

---

*合并说明：原拆分文档仍保留于 `docs/` 目录；若后续接口变更，可运行 `uv run python scripts/merge_docs_final.py` 重新生成本文件。*
