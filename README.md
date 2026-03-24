# TL比价系统

废旧电池回收比价系统后端，基于 FastAPI 构建。支持仓库/冶炼厂/品类管理、运费维护、**OCR图片识别自动提取报价**、比价表生成。

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
| 2 | GET | `/tl/get_smelters` | 获取冶炼厂列表 |
| 3 | GET | `/tl/get_categories` | 获取品类列表 |
| 4 | POST | `/tl/get_comparison` | 获取比价表 |
| 5 | POST | `/tl/upload_price_table` | 上传价格表图片，OCR解析并返回匹配结果 |
| 5b | POST | `/tl/confirm_price_table` | 确认并写入报价数据到数据库 |
| 6 | POST | `/tl/upload_freight` | 上传运费 |
| 7 | POST | `/tl/update_category_mapping` | 更新品类映射表 |

详细接口文档见 [docs/api.md](docs/api.md)。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
pip install rapidocr_onnxruntime
```

### 2. 配置环境变量

创建 `.env` 文件：

```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=yourpassword
MYSQL_DATABASE=tl_compare
```

### 3. 启动服务

```bash
uvicorn app.main:app --reload
```

启动后自动创建数据库和所有表，访问 `http://localhost:8000/docs` 查看 Swagger 文档。

### 4. 测试OCR

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
