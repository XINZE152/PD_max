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
docs/api.md                 # TL 接口说明与 JSON 示例
docs/后端接口文档.md       # 交付用：全模块路由总览（TL / 认证 / 智能预测 / 鉴伪）
docs/数据库文档.md         # 主要业务表说明（含对标定价相关表）
docs/docker.md              # Compose / 部署
```

TL 详细示例以 **[docs/api.md](docs/api.md)** 为准；**全后端路径总表与模块开关**见 **[docs/后端接口文档.md](docs/后端接口文档.md)**；字段级契约以运行环境 **`/docs`** OpenAPI 为准。

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
| `pd_province_benchmark_prices` | 省份对标城市定价历史 |
| `pd_smelter_calibration_prices` | 冶炼厂标定价格历史 |
| `pd_warehouse_spread_configs` | 库房对标差额与毛利配置 |
| `pd_ai_pricing_snapshots` / `pd_ai_pricing_snapshot_items` | AI 定价对标分析快照及明细 |

表字段说明与计算规则见 **[docs/数据库文档.md](docs/数据库文档.md)**。

## 报价识别流程（与实现对齐）

1. `POST /tl/upload_price_table` 上传图片 → VLM 解析 → 返回 `items` + `full_data`
2. 前端确认后 `POST /tl/confirm_price_table` 写入 `quote_details`（可带回 `价格字段来源`）
