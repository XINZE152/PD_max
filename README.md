# TL比价系统（含 AI 鉴伪）

废旧电池回收比价后端（FastAPI）：仓库/冶炼厂/品类、运费、**VLM 报价表识别**、比价、**图片篡改鉴伪**。

## 目录结构（核心）

```
app/
├── main.py                 # FastAPI 应用
├── app.py                  # 启动入口（读 PORT，调 uvicorn）
├── config.py / paths.py    # 配置与项目根路径
├── database.py             # MySQL 连接与建表
├── api/v1/router.py        # 路由汇总
├── api/v1/routes/
│   ├── tl.py               # 比价 / 报价 / 运费等
│   ├── auth.py             # 登录 JWT
│   └── ai_detection.py     # 鉴伪同步/异步/历史
├── services/
│   ├── tl_service.py
│   ├── vlm_extractor_service.py
│   └── user_service.py
├── ai_detection/           # 鉴伪引擎与 history_db
├── models/                 # Pydantic 模型
├── price_tax_utils.py
└── quote_price_sources.py
docs/api.md                 # 接口说明与 JSON 示例
docs/docker.md              # Compose / 部署
```

完整接口列表与参数以 **[docs/api.md](docs/api.md)** 为准（README 不再逐条维护，避免与代码脱节）。

## 快速开始

```bash
cp .env.example .env   # 填好 MySQL、JWT、VLM 等
uv sync
uv run app.py            # 需在 .env 中配置 PORT
```

开发文档：`http://localhost:<PORT>/docs`

Docker 见 [docs/docker.md](docs/docker.md)。

## AI 鉴伪

- 前缀：`/ai-detection`
- 结果图与上传缓存：`UPLOAD_DIR/ai_detection_storage`（默认 `uploads/ai_detection_storage`）
- 历史记录：`GET /ai-detection/api/v1/history`（默认保留 7 天，见环境变量 `AI_DETECTION_HISTORY_DAYS`）

## 数据库表（主要）

| 表名 | 说明 |
|------|------|
| `users` | 用户 |
| `dict_warehouses` / `dict_factories` / `dict_categories` | 仓库、冶炼厂、品类 |
| `freight_rates` | 运费 |
| `quote_table_metadata` | 报价表元数据（VLM） |
| `quote_details` | 报价明细 |
| `factory_tax_rates` | 冶炼厂税率 |
| `ai_detection_history` | 鉴伪历史 |
| `warehouse_inventories` / `factory_demands` 等 | 预留 |

## 报价识别流程（与实现对齐）

1. `POST /tl/upload_price_table` 上传图片 → VLM 解析 → 返回 `items` + `full_data`
2. 前端确认后 `POST /tl/confirm_price_table` 写入 `quote_details`（可带回 `价格字段来源`）
