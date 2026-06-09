# PD_max 迭代开发文档

> 本次迭代包含两大核心优化：循融宝价格基准动态配置 + AI 预测数据后台任务。

---

## 一、循融宝价格基准动态配置

### 1.1 功能概述

- 取消原固定加价常量 `XUNRONGBAO_SHIPPING_PREMIUM_PER_TON`（默认 80 元/吨）
- 改为从数据库 `pd_xunrongbao_price_premiums` 按冶炼厂 + 生效日期实时读取最新加价
- 新增价格配置管理页面所需的后端 CRUD 接口
- 所有价格变更自动记录操作审计日志

### 1.2 新增数据库表

#### `pd_xunrongbao_price_audit` — 循融宝加价操作审计

| 字段 | 类型 | 说明 |
|------|------|------|
| id | BIGINT AUTO_INCREMENT PK | 主键 |
| factory_id | INT NOT NULL | 冶炼厂 ID |
| action | VARCHAR(32) NOT NULL | 操作类型：create / update / delete |
| old_premium | DECIMAL(18,4) | 变更前加价金额 |
| new_premium | DECIMAL(18,4) | 变更后加价金额 |
| effective_date | DATE | 生效日期 |
| remark | VARCHAR(255) | 备注 |
| operator | VARCHAR(255) | 操作人 |
| client_ip | VARCHAR(64) | 客户端 IP |
| detail | JSON | 补充详情 |
| created_at | TIMESTAMP | 创建时间 |

索引：`idx_xrb_audit_factory`(factory_id), `idx_xrb_audit_created`(created_at)

### 1.3 新增 API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/tl/xunrongbao_price_premium/latest` | 获取最新循融宝加价幅度（页面顶部展示）。可选 `?factory_id=`，不传默认查金利 |
| GET | `/tl/xunrongbao_price_premium` | 按冶炼厂和日期范围查询历史加价记录。参数：`factory_id`, `date_from`, `date_to` |
| POST | `/tl/xunrongbao_price_premium` | 新增/修改加价配置。同厂同日期则更新金额。自动写审计日志 |
| DELETE | `/tl/xunrongbao_price_premium/{record_id}` | 删除指定加价记录。自动写审计日志 |
| GET | `/tl/xunrongbao_price_audit` | 分页查询循融宝加价操作审计日志。参数：`factory_id`, `page`, `page_size` |

#### POST 请求体示例

```json
{
  "冶炼厂id": 1,
  "加价金额": 100.00,
  "生效日期": "2026-06-09",
  "备注": "调价原因",
  "操作人": "admin"
}
```

#### GET `/tl/xunrongbao_price_premium/latest` 响应示例

```json
{
  "code": 200,
  "data": {
    "最新加价元每吨": 100.00,
    "最新生效日期": "2026-06-09",
    "记录列表": [...],
    "总数": 5
  }
}
```

### 1.4 原有接口行为变更

| 接口 | 变更内容 |
|------|---------|
| `POST /tl/get_comparison` | 循融宝加价由全局常量改为按冶炼厂从 DB 读取各自配置 |
| `POST /tl/get_purchase_suggestion` | 同上 |
| `GET /tl/list_smelter_xunrongbao` | 响应新增每个冶炼厂的 `加价元每吨` 字段，返回 DB 实际配置值 |

### 1.5 涉及文件

| 文件 | 改动 |
|------|------|
| `app/database.py` | 新增 `pd_xunrongbao_price_audit` 建表语句 + `ensure_pd_xunrongbao_price_audit_table()` migration |
| `app/models/tl.py` | 新增 7 个 Pydantic 模型 |
| `app/services/tl_service.py` | 新增 6 个方法 + 修改 3 个已有方法 |
| `app/api/v1/routes/tl.py` | 新增 5 个路由 |

---

## 二、AI 预测数据后台任务

### 2.1 功能概述

- AI 比价「冶炼厂价格查询」区域新增【更新今日 AI 预测】按钮
- 点击后立即返回，后台 Celery 异步执行全量预测计算
- 结果写入 `pd_ip_prediction_results` 缓存，后续查询直接读取
- 支持重复点击重跑，覆盖同日旧缓存

### 2.2 库房执行范围

| 库房类型 | 执行策略 |
|---------|---------|
| 垂直库房 | 全部执行 |
| 战略库房 | 全部执行 |
| 普通合作库房 | 仅近 30 天有发货记录的库房执行 |

类型判断：通过 `dict_warehouses.warehouse_type_id` → `dict_warehouse_types.name` 中包含"垂直"或"战略"关键词匹配。

### 2.3 表结构变更

`pd_ip_prediction_batches` 新增列：

| 列名 | 类型 | 说明 |
|------|------|------|
| prediction_type | VARCHAR(32) DEFAULT 'manual' | 预测类型：manual / scheduled / export |

### 2.4 新增 API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/tl/trigger_daily_ai_prediction` | 触发今日 AI 预测后台任务。返回 task_id + batch_id |
| GET | `/tl/daily_ai_prediction_status/{batch_id}` | 查询任务执行状态与结果条数 |

#### POST 触发响应示例

```json
{
  "code": 200,
  "data": {
    "task_id": "celery-task-uuid",
    "batch_id": "prediction-batch-uuid",
    "status": "pending",
    "message": "任务已提交，正在后台执行"
  }
}
```

#### GET 状态响应示例

```json
{
  "code": 200,
  "data": {
    "batch_id": "prediction-batch-uuid",
    "status": "completed",
    "result_count": 156,
    "error_message": null,
    "created_at": "2026-06-09 10:00:00",
    "completed_at": "2026-06-09 10:05:30"
  }
}
```

### 2.5 Celery 任务

**任务名：** `intelligent_prediction.run_daily_ai_prediction`

**执行流程：**

1. 读取 `dict_warehouses` + `dict_warehouse_types`，识别垂直/战略库房
2. 垂直/战略 → 全部纳入预测范围
3. 普通合作 → 查 `pd_ip_delivery_records` 近 30 天 DISTINCT warehouse
4. 对每个 (仓库, 品种) 组合构建 `DoubaoPredictionRequest`
5. 调用 `DoubaoPredictionService.predict_batch()` 执行 AI 预测
6. 结果持久化到 `pd_ip_prediction_results`，batch_id 关联批次

### 2.6 涉及文件

| 文件 | 改动 |
|------|------|
| `app/database.py` | 新增 `ensure_pd_ip_prediction_batches_type_column()` migration |
| `app/intelligent_prediction/tasks/export_tasks.py` | 新增 `_run_daily_prediction_async()` + `run_daily_ai_prediction_task` Celery 任务 |
| `app/services/tl_service.py` | 新增 `trigger_daily_ai_prediction()` 方法 |
| `app/api/v1/routes/tl.py` | 新增 2 个路由 |
| `app/models/tl.py` | 新增 2 个 Pydantic 模型 |

---

## 三、部署注意事项

### 3.1 数据库迁移

应用启动时自动执行 `create_tables()`，以下 migration 将自动运行：

- `ensure_pd_xunrongbao_price_audit_table()` → 创建审计表
- `ensure_pd_ip_prediction_batches_type_column()` → 为批次表增加 prediction_type 列

如已有 `pd_xunrongbao_price_premiums` 表但未初始化金利数据，需手动执行或依赖已有 `ensure_pd_xunrongbao_price_premiums_table()` 函数。

### 3.2 Celery Worker

AI 预测后台任务依赖 Celery Worker 运行。需确保：

```bash
celery -A app.intelligent_prediction.tasks.celery_app worker --loglevel=info
```

### 3.3 依赖服务

- MySQL（主数据库）
- Redis（Celery Broker + 预测结果缓存）

### 3.4 前端对接

- 循融宝价格管理页：调用 `GET /tl/xunrongbao_price_premium/latest` 展示最新加价，调用 `GET/POST/DELETE /tl/xunrongbao_price_premium` 维护配置
- AI 比价页：【更新今日 AI 预测】按钮点击调用 `POST /tl/trigger_daily_ai_prediction`，轮询 `GET /tl/daily_ai_prediction_status/{batch_id}` 直至 completed
- 比价查询页：循融宝加价已自动从 DB 读取，前端无需额外处理，响应中 `循融宝加价元每吨` 字段即为各厂实际配置值
