# TL比价系统（含AI鉴伪）

废旧电池回收比价系统后端，基于 FastAPI 构建。支持仓库/冶炼厂/品类管理、运费维护、**OCR图片识别自动提取报价**、比价表生成，以及**图片篡改鉴伪**能力。

## 项目结构

```
app/
├── main.py                  # FastAPI 入口，启动时自动建表
├── config.py                # 环境变量配置（数据库、上传目录）
├── database.py              # 数据库连接 & 建表语句
├── api/v1/
│   ├── router.py            # 路由汇总
│   └── routes/tl.py         # TL模块全部接口
├── models/tl.py             # Pydantic 请求体模型
└── services/tl_service.py   # 业务逻辑层
battery_quote_service1.py    # OCR识图 + 报价解析引擎（RapidOCR）
docs/api.md                  # 接口文档（含JSON示例）
test_ocr.py                  # OCR功能测试脚本
```

## 接口列表

| # | 方法 | 路由 | 说明 |
|---|------|------|------|
| 1 | GET | `/tl/get_warehouses` | 获取仓库列表 |
| 1b | POST | `/tl/update_warehouse` | 修改仓库信息 |
| 1c | DELETE | `/tl/delete_warehouse` | 删除仓库（软删除） |
| 1d | POST | `/tl/add_smelter` | 新建冶炼厂 |
| 2 | GET | `/tl/get_smelters` | 获取冶炼厂列表 |
| 2b | POST | `/tl/update_smelter` | 修改冶炼厂信息 |
| 2c | DELETE | `/tl/delete_smelter` | 删除冶炼厂（软删除） |
| 3 | GET | `/tl/get_categories` | 获取品类列表 |
| 4 | POST | `/tl/get_comparison` | 获取比价表 |
| 5 | POST | `/tl/upload_price_table` | 上传价格表图片，OCR解析并返回匹配结果 |
| 5b | POST | `/tl/confirm_price_table` | 确认并写入报价数据到数据库 |
| 6 | POST | `/tl/upload_freight` | 上传运费 |
| 7 | POST | `/tl/update_category_mapping` | 更新品类映射表 |
| 8 | POST | `/ai-detection/api/v1/image-detection/detect` | 单图单框鉴伪（同步） |
| 9 | POST | `/ai-detection/api/v3/detect` | 提交鉴伪任务（异步） |
| 10 | GET | `/ai-detection/api/v3/result/{task_id}` | 查询鉴伪结果 |
| 11 | GET | `/ai-detection/api/v3/result/{task_id}/visualization` | 获取可视化标注图 |
| 12 | DELETE | `/ai-detection/api/v3/task/{task_id}` | 取消/删除鉴伪任务 |

详细接口文档见 [docs/api.md](docs/api.md)。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 环境变量（准备一次即可）

应用启动时会读取项目根目录的 `.env`（`app/config.py` 要求配置 `MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_USER`、`MYSQL_PASSWORD`、`MYSQL_DATABASE` 等）。**第一次**从示例复制并改好密码即可，之后每次启动不必再配。

```bash
cp .env.example .env
# 编辑 .env 填入数据库密码等
```

### 3. 本地启动（推荐 `uv`）

在项目根目录：

```bash
uv sync                    # 首次或依赖变更后执行一次
uv run app.py              # 入口是根目录 app.py，不是 main.py
```

若你感觉每次 `uv run` 都在「装包 / 同步环境」，那是 `uv run` 默认会检查并同步虚拟环境与锁文件。依赖已稳定后可改用：

```bash
uv run --no-sync app.py
# 或：source .venv/bin/activate && python app.py
```

`app.py` **必须**在 `.env`（或环境中）配置 `PORT`，否则无法启动；文档地址为 `http://localhost:<PORT>/docs`。

也可用 uvicorn（需自行指定端口，与 `PORT` 一致即可）：

```bash
uv run --no-sync uvicorn app.main:app --host 0.0.0.0 --reload --port 8002
```

### 4. Docker 启动

- **推荐**：`docker compose up -d --build`（见 `docs/docker.md`，需先配置 `.env` 或导出变量）。
- **手动 `docker run`**：最后一行必须是已构建的镜像名（如 `my-backend`），且**反斜杠续行后不能有空格**，不要把文档里的 `...` 粘进命令。示例见 `docs/docker.md` 第三节。

以上方法适用于本地快速开发；Compose 细节见 [docs/docker.md](docs/docker.md)。

若遇到 `ModuleNotFoundError: No module named 'easyocr'`，请在项目根目录执行：

```bash
uv sync
```

## AI鉴伪模块说明

- 接口前缀：`/ai-detection`
- 启动时会自动加载 OCR 与鉴伪模型（首次启动耗时会更长）
- 鉴伪文件默认保存在 `UPLOAD_DIR/ai_detection_storage`（默认 `uploads/ai_detection_storage`）

### 4. 测试OCR
### 5. 测试OCR

```bash
python test_ocr.py
```

## 数据库表

| 表名 | 说明 |
|------|------|
| `dict_warehouses` | 仓库字典 |
| `dict_factories` | 冶炼厂字典 |
| `dict_categories` | 品类字典（多名称映射同一category_id） |
| `freight_rates` | 运费价格表 |
| `quote_orders` | 报价主单 |
| `quote_details` | 报价明细 |
| `optimization_results` | 利润计算结果 |

## OCR报价识别流程

1. 前端上传报价图片 → `POST /tl/upload_price_table`
2. 后端 RapidOCR 识别文字，提取工厂名、日期、品类+价格
3. 自动匹配冶炼厂ID和品类ID，返回 `{冶炼厂id: {品类id: 价格}}` 及未匹配项
4. 前端确认/修正后 → `POST /tl/confirm_price_table` 写入数据库
