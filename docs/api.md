# TL比价模块 — 接口文档（含JSON示例）

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
- 传入：选中仓库id列表、冶炼厂id列表、品类id列表、price_type（取价口径）、**吨数**（可选，默认 `1`）。计价关系：**总价** = **单价**×**吨数**（与 **`报价金额`** 一致，**`单价`/`报价`** 为折合后元/吨不含税）；**运费** = **运费单价**×**吨数**（与 **`总运费`** 一致，**`运费单价`** 来自 `freight_rates.price_per_ton`，元/吨）；**利润** = **总价 − 运费**（与 **`报价金额` − `总运费`** 一致）。**最终比价**以 **`利润`** 为准；明细按「最优价排序口径」下利润排序；**`冶炼厂利润排行`** 为各冶炼厂明细 **`利润`** 之和。**`最优价各口径利润`** = 该口径元/吨单价×吨数 − **总运费**（与主行共用同一全程运费金额）。
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

- 模拟请求JSON：
```json
{
  "选中仓库id列表": [101, 102],
  "冶炼厂id列表": [201],
  "品类id列表": [301, 302],
  "price_type": "3pct",
  "吨数": 10.5
}
```
- 模拟返回JSON：
```json
{
  "code": 200,
  "data": [
    { "仓库id": 101, "冶炼厂id": 201, "品类id": 301, "仓库": "北京仓", "冶炼厂": "华北冶炼厂", "品类": "铜", "price_type": "3%增值税", "吨数": 10.5, "单价": 9453.4, "总价": 99260.7, "运费单价": 200, "运费": 2100, "总运费": 2100, "报价": 9453.4, "报价金额": 99260.7, "报价来源": "direct", "利润": 97160.7 },
    { "仓库id": 101, "冶炼厂id": 201, "品类id": 302, "仓库": "北京仓", "冶炼厂": "华北冶炼厂", "品类": "铝", "price_type": "3%增值税", "吨数": 10.5, "单价": 8106.8, "总价": 85121.4, "运费单价": 200, "运费": 2100, "总运费": 2100, "报价": 8106.8, "报价金额": 85121.4, "报价来源": "calc_from_base", "利润": 83021.4 },
    { "仓库id": 102, "冶炼厂id": 201, "品类id": 301, "仓库": "上海仓", "冶炼厂": "华北冶炼厂", "品类": "铜", "price_type": "3%增值税", "吨数": 10.5, "单价": null, "总价": null, "运费单价": 300, "运费": 3150, "总运费": 3150, "报价": null, "报价金额": null, "报价来源": "unavailable", "利润": -3150.0 }
  ],
  "冶炼厂利润排行": [
    { "冶炼厂id": 201, "冶炼厂": "华北冶炼厂", "利润": 177032.1 }
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
  1. 读取全部启用冶炼厂 ID；查询各(仓库, 冶炼厂)最新运费、各(冶炼厂, 品类)最新报价（报价日期取各品类最新 `quote_date`，与比价表「仅当天」策略不同）
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

## 补充说明
1. 所有JSON中的 `code: 200` 为通用成功状态码
2. 需登录的接口请求头须携带 `Authorization: Bearer <token>`
3. token 过期或无效返回 `401`，非 admin 调用管理接口返回 `403`
4. 错误状态码：`400`（参数校验失败）、`401`（未登录）、`403`（权限不足）、`404`（资源不存在）、`500`（服务器内部错误）
5. 数据库连接使用 `autocommit=True`，写操作自动提交
6. LLM 配置通过环境变量注入：`LLM_API_KEY`、`LLM_BASE_URL`、`LLM_MODEL`
7. VLM 配置通过环境变量注入：`VLM_API_KEY`、`VLM_BASE_URL`、`VLM_MODEL`（默认 `qwen-vl-max-latest`）
