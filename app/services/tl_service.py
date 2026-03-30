"""
TL比价模块服务层
负责仓库、冶炼厂、品类、比价、运费、价格表、品类映射等数据库操作
"""
import hashlib
import logging
import os
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.config import UPLOAD_DIR
from app.database import get_conn
from app.services.vlm_extractor_service import QwenVLFullExtractor, VLMConfig

logger = logging.getLogger(__name__)

PRICE_TABLE_UPLOAD_DIR = Path(UPLOAD_DIR) / "price_tables"
PRICE_TABLE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


class TLService:

    # ==================== 接口0：添加仓库 ====================

    def add_warehouse(self, name: str) -> Dict[str, Any]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM dict_warehouses WHERE name = %s",
                        (name,),
                    )
                    row = cur.fetchone()
                    if row:
                        return {"code": 200, "msg": "仓库已存在", "仓库id": row[0], "新建": False}
                    cur.execute(
                        "INSERT INTO dict_warehouses (name, is_active) VALUES (%s, 1)",
                        (name,),
                    )
                    return {"code": 200, "msg": "仓库新建成功", "仓库id": cur.lastrowid, "新建": True}
        except Exception as e:
            logger.error(f"添加仓库失败: {e}")
            raise

    # ==================== 接口1：获取仓库列表 ====================

    def get_warehouses(self) -> List[Dict[str, Any]]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id AS `仓库id`, name AS `仓库名` "
                        "FROM dict_warehouses "
                        "WHERE is_active = 1 "
                        "ORDER BY id"
                    )
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"获取仓库列表失败: {e}")
            raise

    # ==================== 接口2：获取冶炼厂列表 ====================

    def get_smelters(self) -> List[Dict[str, Any]]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id AS `冶炼厂id`, name AS `冶炼厂` "
                        "FROM dict_factories "
                        "WHERE is_active = 1 "
                        "ORDER BY id"
                    )
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"获取冶炼厂列表失败: {e}")
            raise

    # ==================== 接口3：获取品类列表 ====================

    def get_categories(self) -> List[Dict[str, Any]]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT category_id AS `品类id`, "
                        "GROUP_CONCAT(name ORDER BY row_id SEPARATOR '、') AS `品类名` "
                        "FROM dict_categories "
                        "WHERE is_active = 1 "
                        "GROUP BY category_id "
                        "ORDER BY category_id"
                    )
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"获取品类列表失败: {e}")
            raise

    # ==================== 接口4：获取比价表 ====================
    def get_comparison(
        self,
        warehouse_ids: List[int],
        smelter_ids: List[int],
        category_ids: List[int],
        price_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        price_type: 目标税率类型，None=普通价, 1pct/3pct/13pct/normal_invoice/reverse_invoice
        取价逻辑（按优先级）：
          1. 报价表中直接有对应 price_type 的价格 → 直接使用
          2. 报价表中有普通价(unit_price) + 税率表中有对应税率 → 正向换算
          3. 报价表中有某已知含税价 + 税率表中有该税率和目标税率 → 先反算不含税，再正向换算
          4. 以上均无 → None，返回 price_source="unavailable"
        """
        if not warehouse_ids or not smelter_ids or not category_ids:
            return []

        # price_type → (quote_details列名, 展示名)
        PRICE_COL_MAP = {
            None:             ("unit_price",            "普通价"),
            "1pct":           ("price_1pct_vat",        "1%增值税"),
            "3pct":           ("price_3pct_vat",        "3%增值税"),
            "13pct":          ("price_13pct_vat",       "13%增值税"),
            "normal_invoice": ("price_normal_invoice",  "普通发票"),
            "reverse_invoice":("price_reverse_invoice", "反向发票"),
        }
        # 仅以下三种有税率换算意义
        VAT_TAX_TYPE_MAP = {"1pct": "1pct", "3pct": "3pct", "13pct": "13pct"}

        if price_type not in PRICE_COL_MAP:
            raise ValueError(f"不支持的 price_type: {price_type}")

        target_col, price_type_name = PRICE_COL_MAP[price_type]
        target_tax = VAT_TAX_TYPE_MAP.get(price_type)  # None 表示不需要税率换算

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    wh_ph = ",".join(["%s"] * len(warehouse_ids))
                    sm_ph = ",".join(["%s"] * len(smelter_ids))
                    cat_ph = ",".join(["%s"] * len(category_ids))

                    # 品类主名称
                    cur.execute(
                        f"SELECT DISTINCT category_id, "
                        f"COALESCE(MAX(CASE WHEN is_main=1 THEN name END), MAX(name)) AS cat_name "
                        f"FROM dict_categories "
                        f"WHERE category_id IN ({cat_ph}) AND is_active = 1 "
                        f"GROUP BY category_id",
                        tuple(category_ids),
                    )
                    cat_map: Dict[int, str] = {row[0]: row[1] for row in cur.fetchall()}

                    # 最新运费
                    cur.execute(
                        f"""
                        SELECT dw.id, dw.name, df.id, df.name, fr.price_per_ton
                        FROM freight_rates fr
                        JOIN dict_warehouses dw ON fr.warehouse_id = dw.id
                        JOIN dict_factories  df ON fr.factory_id  = df.id
                        WHERE dw.id IN ({wh_ph})
                          AND df.id IN ({sm_ph})
                          AND fr.effective_date = (
                              SELECT MAX(fr2.effective_date)
                              FROM freight_rates fr2
                              WHERE fr2.factory_id  = fr.factory_id
                                AND fr2.warehouse_id = fr.warehouse_id
                          )
                        """,
                        tuple(warehouse_ids) + tuple(smelter_ids),
                    )
                    freight_map: Dict[tuple, tuple] = {}
                    for wid, wname, fid, fname, freight in cur.fetchall():
                        freight_map[(wid, fid)] = (wname, fname, freight)

                    # category_id 分组 → row_id 列表
                    cur.execute(
                        f"SELECT row_id, category_id FROM dict_categories "
                        f"WHERE category_id IN ({cat_ph}) AND is_active = 1",
                        tuple(category_ids),
                    )
                    row_id_to_cat: Dict[int, int] = {row[0]: row[1] for row in cur.fetchall()}
                    row_ids = list(row_id_to_cat.keys())

                    if not row_ids:
                        return []

                    ri_ph = ",".join(["%s"] * len(row_ids))

                    # 税率表：{factory_id: {tax_type: rate}}
                    cur.execute(
                        f"SELECT factory_id, tax_type, tax_rate "
                        f"FROM factory_tax_rates "
                        f"WHERE factory_id IN ({sm_ph})",
                        tuple(smelter_ids),
                    )
                    tax_rate_map: Dict[int, Dict[str, float]] = {}
                    for fid, ttype, rate in cur.fetchall():
                        tax_rate_map.setdefault(fid, {})[ttype] = float(rate)

                    # 最新报价（取全部价格列，用于换算回退）
                    cur.execute(
                        f"""
                        SELECT factory_id, category_id,
                               unit_price, price_1pct_vat, price_3pct_vat, price_13pct_vat,
                               price_normal_invoice, price_reverse_invoice
                        FROM quote_details
                        WHERE factory_id IN ({sm_ph})
                          AND category_id IN ({ri_ph})
                          AND quote_date = (
                              SELECT MAX(qd2.quote_date)
                              FROM quote_details qd2
                              WHERE qd2.factory_id  = quote_details.factory_id
                                AND qd2.category_id = quote_details.category_id
                          )
                        """,
                        tuple(smelter_ids) + tuple(row_ids),
                    )
                    # raw_price_map: {(factory_id, row_id): {col: value}}
                    col_names = ["unit_price", "price_1pct_vat", "price_3pct_vat",
                                 "price_13pct_vat", "price_normal_invoice", "price_reverse_invoice"]
                    raw_price_map: Dict[tuple, Dict[str, Optional[float]]] = {}
                    for row in cur.fetchall():
                        fid_r, rid = row[0], row[1]
                        raw_price_map[(fid_r, rid)] = {
                            col: (float(v) if v is not None else None)
                            for col, v in zip(col_names, row[2:])
                        }

            # 换算逻辑（纯 Python，连接已关闭）
            # col → tax_type 的对应关系，用于反算不含税价
            COL_TO_TAX: Dict[str, str] = {
                "price_1pct_vat": "1pct",
                "price_3pct_vat": "3pct",
                "price_13pct_vat": "13pct",
            }

            def resolve_price(fid: int, rid: int) -> Tuple[Optional[float], str]:
                """
                返回 (price, source)
                source: "direct" | "calc_from_base" | "calc_from_other_vat" | "unavailable"
                """
                prices = raw_price_map.get((fid, rid), {})
                rates = tax_rate_map.get(fid, {})

                # 1. 直接有目标列
                direct = prices.get(target_col)
                if direct is not None:
                    return direct, "direct"

                # 2. 目标是含税价，且有不含税基础价 + 目标税率
                if target_tax and prices.get("unit_price") is not None and target_tax in rates:
                    base = prices["unit_price"]
                    calc = round(base * (1 + rates[target_tax]), 2)
                    return calc, "calc_from_base"

                # 3. 目标是基础价(unit_price)，从已知含税价反算
                if target_col == "unit_price":
                    for col, src_tax in COL_TO_TAX.items():
                        known_price = prices.get(col)
                        if known_price is not None and src_tax in rates:
                            base = round(known_price / (1 + rates[src_tax]), 2)
                            return base, f"calc_from_{src_tax}"

                # 4. 从其他已知含税价反算不含税，再正向换算
                if target_tax and target_tax in rates:
                    for col, src_tax in COL_TO_TAX.items():
                        known_price = prices.get(col)
                        if known_price is not None and src_tax in rates:
                            base = round(known_price / (1 + rates[src_tax]), 4)
                            calc = round(base * (1 + rates[target_tax]), 2)
                            return calc, f"calc_from_{src_tax}"

                return None, "unavailable"

            # 组合结果
            result = []
            for (wid, fid), (wname, fname, freight) in freight_map.items():
                for cid in category_ids:
                    cat_name = cat_map.get(cid)
                    if cat_name is None:
                        continue
                    # 找该 category_id 下所有 row_id，取第一个有报价的
                    price, source = None, "unavailable"
                    for rid, cat in row_id_to_cat.items():
                        if cat == cid:
                            p, s = resolve_price(fid, rid)
                            if p is not None:
                                price, source = p, s
                                break

                    result.append({
                        "仓库": wname,
                        "冶炼厂": fname,
                        "品类": cat_name,
                        "price_type": price_type_name,
                        "运费": float(freight) if freight is not None else 0.0,
                        "报价": price if price is not None else 0.0,
                        "报价来源": source,
                    })
            return result

        except Exception as e:
            logger.error(f"获取比价表失败: {e}")
            raise

    # ==================== 税率表 CRUD ====================

    def get_tax_rates(self, factory_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """获取税率表，可按冶炼厂过滤"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    if factory_ids:
                        ph = ",".join(["%s"] * len(factory_ids))
                        cur.execute(
                            f"SELECT ftr.id, ftr.factory_id, df.name AS factory_name, "
                            f"ftr.tax_type, ftr.tax_rate "
                            f"FROM factory_tax_rates ftr "
                            f"JOIN dict_factories df ON ftr.factory_id = df.id "
                            f"WHERE ftr.factory_id IN ({ph}) "
                            f"ORDER BY ftr.factory_id, ftr.tax_type",
                            tuple(factory_ids),
                        )
                    else:
                        cur.execute(
                            "SELECT ftr.id, ftr.factory_id, df.name AS factory_name, "
                            "ftr.tax_type, ftr.tax_rate "
                            "FROM factory_tax_rates ftr "
                            "JOIN dict_factories df ON ftr.factory_id = df.id "
                            "ORDER BY ftr.factory_id, ftr.tax_type"
                        )
                    cols = [d[0] for d in cur.description]
                    return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"获取税率表失败: {e}")
            raise

    def upsert_tax_rates(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """批量设置税率（存在则更新，不存在则插入）"""
        from app.models.tl import VALID_TAX_TYPES
        for item in items:
            if item["tax_type"] not in VALID_TAX_TYPES:
                raise ValueError(f"不支持的 tax_type: {item['tax_type']}，有效值：{VALID_TAX_TYPES}")
            if not (0 <= item["tax_rate"] <= 1):
                raise ValueError(f"tax_rate 必须在 0~1 之间，收到：{item['tax_rate']}")
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for item in items:
                        # 验证冶炼厂是否存在
                        cur.execute("SELECT id FROM dict_factories WHERE id = %s", (item["factory_id"],))
                        if not cur.fetchone():
                            raise ValueError(f"冶炼厂 ID {item['factory_id']} 不存在")

                        cur.execute(
                            "INSERT INTO factory_tax_rates (factory_id, tax_type, tax_rate) "
                            "VALUES (%s, %s, %s) "
                            "ON DUPLICATE KEY UPDATE tax_rate = VALUES(tax_rate), "
                            "updated_at = CURRENT_TIMESTAMP",
                            (item["factory_id"], item["tax_type"], item["tax_rate"]),
                        )
            return {"code": 200, "msg": f"已保存 {len(items)} 条税率记录"}
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"设置税率失败: {e}")
            raise

    def delete_tax_rate(self, factory_id: int, tax_type: str) -> Dict[str, Any]:
        """删除某冶炼厂的某税率记录"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM factory_tax_rates WHERE factory_id = %s AND tax_type = %s",
                        (factory_id, tax_type),
                    )
                    if cur.rowcount == 0:
                        raise ValueError(f"未找到 factory_id={factory_id}, tax_type={tax_type} 的记录")
            return {"code": 200, "msg": "删除成功"}
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"删除税率失败: {e}")
            raise

    # ==================== 接口5：上传价格表（OCR解析） ====================

    def _match_factory(
        self, ocr_name: str, factory_list: List[Tuple[int, str]]
    ) -> Optional[int]:
        """将 OCR 识别出的工厂名匹配到 dict_factories 中的冶炼厂，返回 factory_id"""
        if not ocr_name or ocr_name == "未知工厂":
            return None
        for fid, fname in factory_list:
            # 双向包含匹配
            if fname in ocr_name or ocr_name in fname:
                return fid
        return None

    def _match_category(
        self, ocr_cat: str, category_list: List[Tuple[int, int, str]]
    ) -> Optional[Tuple[int, int]]:
        """将 OCR 识别出的品类名匹配到 dict_categories，返回 (category_id, row_id)"""
        if not ocr_cat:
            return None
        for row_id, cat_id, cname in category_list:
            if cname in ocr_cat or ocr_cat in cname:
                return (cat_id, row_id)
        return None

    def upload_price_table(self, files: List[Any]) -> Dict[str, Any]:
        saved_paths: List[Tuple[str, str, str]] = []
        try:
            # 1. 保存图片到磁盘
            for upload_file in files:
                content = upload_file.file.read()
                md5 = hashlib.md5(content).hexdigest()
                suffix = Path(upload_file.filename).suffix or ".jpg"
                filename = f"{uuid.uuid4().hex}{suffix}"
                save_path = PRICE_TABLE_UPLOAD_DIR / filename

                with open(save_path, "wb") as f:
                    f.write(content)
                saved_paths.append((str(save_path), md5, upload_file.filename))

            # 2. VLM识别
            from app import config as app_config
            if not app_config.VLM_API_KEY:
                raise ValueError("未配置 VLM_API_KEY，请在环境变量中设置 VLM_API_KEY")
            vlm_config = VLMConfig(
                api_key=app_config.VLM_API_KEY,
                base_url=app_config.VLM_BASE_URL,
                model=app_config.VLM_MODEL,
                save_individual=False,
            )

            details = []
            with QwenVLFullExtractor(vlm_config) as extractor:
                for image_path, md5, orig_name in saved_paths:
                    result = extractor.recognize(image_path, save_output=False)

                    if not result.success:
                        details.append({
                            "image": orig_name,
                            "success": False,
                            "error": result.error_message,
                        })
                        continue

                    # 3. 构建 full_data（VlmFullData格式，供前端保留并回传）
                    full_data = {
                        "image_path": result.image_path,
                        "file_name": result.file_name,
                        "source_image": orig_name,
                        "company_name": result.company_name,
                        "doc_title": result.doc_title,
                        "subtitle": result.subtitle,
                        "quote_date": result.quote_date,
                        "execution_date": result.execution_date,
                        "valid_period": result.valid_period,
                        "price_unit": result.price_unit,
                        "headers": result.headers,
                        "rows": [row.model_dump() for row in result.rows],
                        "policies": result.policies,
                        "footer_notes": result.footer_notes,
                        "footer_notes_raw": result.footer_notes_raw,
                        "brand_specifications": result.brand_specifications,
                        "raw_full_text": result.raw_full_text,
                        "elapsed_time": result.elapsed_time,
                    }

                    # 4. 映射为前端可编辑的 items（ConfirmPriceTableItem格式）
                    items = self._map_vlm_to_confirm_items(result)

                    details.append({
                        "image": orig_name,
                        "success": True,
                        "full_data": full_data,
                        "items": items,
                    })

            return {"code": 200, "data": {"details": details}}

        except Exception as e:
            logger.error(f"上传价格表失败: {e}")
            for path, _, _ in saved_paths:
                try:
                    os.remove(path)
                except OSError:
                    pass
            raise

    def _map_vlm_to_confirm_items(self, result) -> List[Dict[str, Any]]:
        """将 VLM 提取结果映射为前端可编辑的确认条目"""
        items = []
        factory_name = result.company_name or ""
        for row in result.rows:
            # 根据价格列类型，确定主价格字段
            price_general = row.price_general
            price_1pct = row.price_1pct_vat
            price_3pct = row.price_3pct_vat
            price_13pct = row.price_13pct_vat
            price_normal = row.price_normal_invoice
            price_reverse = row.price_reverse_invoice

            # 对于单价列类型，price_general 填入 unit_price
            unit_price = price_general

            items.append({
                "冶炼厂名": factory_name,
                "冶炼厂id": None,
                "品类名": row.category,
                "品类id": None,
                "价格": float(unit_price) if unit_price is not None else None,
                "价格_1pct增值税": float(price_1pct) if price_1pct is not None else None,
                "价格_3pct增值税": float(price_3pct) if price_3pct is not None else None,
                "价格_13pct增值税": float(price_13pct) if price_13pct is not None else None,
                "普通发票价格": float(price_normal) if price_normal is not None else None,
                "反向发票价格": float(price_reverse) if price_reverse is not None else None,
            })
        return items

    # ==================== 接口5b：确认价格表写入数据库 ====================

    def confirm_price_table(
        self,
        quote_date_str: str,
        items: List[Dict[str, Any]],
        full_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not items:
            raise ValueError("报价数据不能为空")

        try:
            quote_dt = date.fromisoformat(quote_date_str)
        except (ValueError, TypeError):
            raise ValueError(f"日期格式不正确: {quote_date_str}，应为 YYYY-MM-DD")

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    inserted, updated = 0, 0

                    for item in items:
                        # 1. 冶炼厂不存在则新建
                        if item.get("冶炼厂id") is None:
                            factory_name = item["冶炼厂名"]
                            cur.execute(
                                "SELECT id FROM dict_factories WHERE name = %s",
                                (factory_name,),
                            )
                            row = cur.fetchone()
                            if row:
                                item["冶炼厂id"] = row[0]
                            else:
                                cur.execute(
                                    "INSERT INTO dict_factories (name, is_active) "
                                    "VALUES (%s, 1)",
                                    (factory_name,),
                                )
                                item["冶炼厂id"] = cur.lastrowid

                        # 2. 品类不存在则新建，item["品类id"] 存的是 row_id
                        if item.get("品类id") is None:
                            cat_name = item["品类名"]
                            cur.execute(
                                "SELECT row_id FROM dict_categories WHERE name = %s AND is_active = 1",
                                (cat_name,),
                            )
                            row = cur.fetchone()
                            if row:
                                item["品类id"] = row[0]
                            else:
                                cur.execute("SELECT COALESCE(MAX(category_id), 0) + 1 FROM dict_categories")
                                new_cat_id = cur.fetchone()[0]
                                cur.execute(
                                    "INSERT INTO dict_categories "
                                    "(category_id, name, is_main, is_active) "
                                    "VALUES (%s, %s, 1, 1)",
                                    (new_cat_id, cat_name),
                                )
                                item["品类id"] = cur.lastrowid

                    # 3. 存储全量元数据（如果有 full_data）
                    metadata_id = None
                    if full_data:
                        # 取第一条 item 的冶炼厂id作为元数据的 factory_id
                        factory_id_for_meta = items[0].get("冶炼厂id") if items else None
                        if factory_id_for_meta:
                            import json as _json
                            cur.execute(
                                """
                                INSERT INTO quote_table_metadata
                                (factory_id, quote_date, execution_date, doc_title, subtitle,
                                 valid_period, price_unit, headers, footer_notes, footer_notes_raw,
                                 brand_specifications, policies, raw_full_text, source_image)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    execution_date = VALUES(execution_date),
                                    doc_title = VALUES(doc_title),
                                    subtitle = VALUES(subtitle),
                                    valid_period = VALUES(valid_period),
                                    price_unit = VALUES(price_unit),
                                    headers = VALUES(headers),
                                    footer_notes = VALUES(footer_notes),
                                    footer_notes_raw = VALUES(footer_notes_raw),
                                    brand_specifications = VALUES(brand_specifications),
                                    policies = VALUES(policies),
                                    raw_full_text = VALUES(raw_full_text),
                                    source_image = VALUES(source_image),
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                (
                                    factory_id_for_meta,
                                    quote_dt,
                                    full_data.get("execution_date", ""),
                                    full_data.get("doc_title", ""),
                                    full_data.get("subtitle", ""),
                                    full_data.get("valid_period", ""),
                                    full_data.get("price_unit", "元/吨"),
                                    _json.dumps(full_data.get("headers", []), ensure_ascii=False),
                                    _json.dumps(full_data.get("footer_notes", []), ensure_ascii=False),
                                    full_data.get("footer_notes_raw", ""),
                                    full_data.get("brand_specifications", ""),
                                    _json.dumps(full_data.get("policies", {}), ensure_ascii=False),
                                    full_data.get("raw_full_text", ""),
                                    full_data.get("source_image", full_data.get("file_name", "")),
                                ),
                            )
                            # 取 metadata_id（INSERT 或 已存在的）
                            if cur.lastrowid:
                                metadata_id = cur.lastrowid
                            else:
                                cur.execute(
                                    "SELECT id FROM quote_table_metadata WHERE factory_id=%s AND quote_date=%s",
                                    (factory_id_for_meta, quote_dt),
                                )
                                row = cur.fetchone()
                                metadata_id = row[0] if row else None

                    # 4. 写入明细，相同(日期+冶炼厂+品类)则更新价格
                    for item in items:
                        cur.execute(
                            """
                            INSERT INTO quote_details
                            (quote_date, factory_id, category_id, metadata_id, raw_category_name,
                             unit_price, price_1pct_vat, price_3pct_vat, price_13pct_vat,
                             price_normal_invoice, price_reverse_invoice)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                metadata_id = VALUES(metadata_id),
                                raw_category_name = VALUES(raw_category_name),
                                unit_price = VALUES(unit_price),
                                price_1pct_vat = VALUES(price_1pct_vat),
                                price_3pct_vat = VALUES(price_3pct_vat),
                                price_13pct_vat = VALUES(price_13pct_vat),
                                price_normal_invoice = VALUES(price_normal_invoice),
                                price_reverse_invoice = VALUES(price_reverse_invoice),
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            (
                                quote_dt,
                                item["冶炼厂id"],
                                item["品类id"],
                                metadata_id,
                                item["品类名"],
                                item.get("价格"),
                                item.get("价格_1pct增值税"),
                                item.get("价格_3pct增值税"),
                                item.get("价格_13pct增值税"),
                                item.get("普通发票价格"),
                                item.get("反向发票价格"),
                            ),
                        )
                        if cur.rowcount == 1:
                            inserted += 1
                        else:
                            updated += 1

            return {
                "code": 200,
                "msg": f"写入成功：新增 {inserted} 条，更新 {updated} 条",
            }

        except ValueError:
            raise
        except Exception as e:
            logger.error(f"确认价格表写入失败: {e}")
            raise

    # ==================== 接口6：上传运费 ====================

    def upload_freight(self, freight_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    today = date.today().isoformat()
                    for item in freight_list:
                        warehouse_name = item["仓库"]
                        smelter_name = item["冶炼厂"]
                        freight = item["运费"]

                        cur.execute(
                            "SELECT id FROM dict_warehouses WHERE name = %s AND is_active = 1",
                            (warehouse_name,),
                        )
                        wh_row = cur.fetchone()
                        if not wh_row:
                            raise ValueError(f"仓库 '{warehouse_name}' 不存在或未启用")

                        cur.execute(
                            "SELECT id FROM dict_factories WHERE name = %s AND is_active = 1",
                            (smelter_name,),
                        )
                        sm_row = cur.fetchone()
                        if not sm_row:
                            raise ValueError(f"冶炼厂 '{smelter_name}' 不存在或未启用")

                        cur.execute(
                            "INSERT INTO freight_rates "
                            "(factory_id, warehouse_id, price_per_ton, effective_date) "
                            "VALUES (%s, %s, %s, %s) "
                            "ON DUPLICATE KEY UPDATE "
                            "price_per_ton = VALUES(price_per_ton), "
                            "updated_at = CURRENT_TIMESTAMP",
                            (sm_row[0], wh_row[0], freight, today),
                        )
            return {"code": 200, "msg": "运费数据已存入数据库"}

        except ValueError:
            raise
        except Exception as e:
            logger.error(f"上传运费失败: {e}")
            raise

    # ==================== 接口7a：获取品类映射表 ====================

    def get_category_mapping(self) -> List[Dict[str, Any]]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT category_id, name, is_main "
                        "FROM dict_categories "
                        "WHERE is_active = 1 "
                        "ORDER BY category_id, is_main DESC, row_id"
                    )
                    rows = cur.fetchall()

            result: Dict[int, Dict[str, Any]] = {}
            for cat_id, name, is_main in rows:
                if cat_id not in result:
                    result[cat_id] = {"品类id": cat_id, "品类名称": []}
                if is_main:
                    result[cat_id]["品类名称"].insert(0, name)
                else:
                    result[cat_id]["品类名称"].append(name)

            return list(result.values())
        except Exception as e:
            logger.error(f"获取品类映射表失败: {e}")
            raise

    # ==================== 接口7：更新品类映射表 ====================

    def update_category_mapping(
        self,
        category_id: int,
        names: List[str],
    ) -> Dict[str, Any]:
        if not names:
            raise ValueError("品类名称列表不能为空")

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 将该 category_id 下所有旧记录的 is_main 置为 0
                    cur.execute(
                        "UPDATE dict_categories SET is_main = 0 WHERE category_id = %s",
                        (category_id,),
                    )

                    for i, name in enumerate(names):
                        is_main = 1 if i == 0 else 0

                        cur.execute(
                            "SELECT row_id, category_id FROM dict_categories WHERE name = %s",
                            (name,),
                        )
                        existing = cur.fetchone()

                        if existing:
                            cur.execute(
                                "UPDATE dict_categories "
                                "SET category_id = %s, is_main = %s, is_active = 1 "
                                "WHERE row_id = %s",
                                (category_id, is_main, existing[0]),
                            )
                        else:
                            cur.execute(
                                "INSERT INTO dict_categories "
                                "(category_id, name, is_main, is_active) "
                                "VALUES (%s, %s, %s, 1)",
                                (category_id, name, is_main),
                            )

            return {"code": 200, "msg": "品类映射表更新成功，数据已存入数据库"}

        except ValueError:
            raise
        except Exception as e:
            logger.error(f"更新品类映射失败: {e}")
            raise

    # ==================== 接口A7：采购建议 ====================

    def get_purchase_suggestion(
        self,
        warehouse_ids: List[int],
        demands: List[Dict[str, Any]],
        price_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        根据仓库列表和需求（冶炼厂+品类+吨数），查询最新运费和报价，
        整理结构化数据后调用 Claude 生成各仓库发车建议表。
        同一仓库发出的货物可混装，尽量整车发。
        price_type: 目标税率类型，None=普通价, 1pct/3pct/13pct/normal_invoice/reverse_invoice
        """
        if not warehouse_ids or not demands:
            raise ValueError("仓库列表和需求不能为空")

        # price_type → (quote_details列名, 展示名)
        PRICE_COL_MAP = {
            None:             ("unit_price",            "普通价"),
            "1pct":           ("price_1pct_vat",        "1%增值税"),
            "3pct":           ("price_3pct_vat",        "3%增值税"),
            "13pct":          ("price_13pct_vat",       "13%增值税"),
            "normal_invoice": ("price_normal_invoice",  "普通发票"),
            "reverse_invoice":("price_reverse_invoice", "反向发票"),
        }
        VAT_TAX_TYPE_MAP = {"1pct": "1pct", "3pct": "3pct", "13pct": "13pct"}

        if price_type not in PRICE_COL_MAP:
            raise ValueError(f"不支持的 price_type: {price_type}")

        target_col, price_type_name = PRICE_COL_MAP[price_type]
        target_tax = VAT_TAX_TYPE_MAP.get(price_type)

        smelter_ids = list({d["smelter_id"] for d in demands})
        category_ids = list({d["category_id"] for d in demands})

        wh_ph = ",".join(["%s"] * len(warehouse_ids))
        sm_ph = ",".join(["%s"] * len(smelter_ids))
        cat_ph = ",".join(["%s"] * len(category_ids))

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 仓库名称
                cur.execute(
                    f"SELECT id, name FROM dict_warehouses WHERE id IN ({wh_ph})",
                    tuple(warehouse_ids),
                )
                warehouse_name_map: Dict[int, str] = {r[0]: r[1] for r in cur.fetchall()}

                # 品类主名称
                cur.execute(
                    f"SELECT category_id, "
                    f"COALESCE(MAX(CASE WHEN is_main=1 THEN name END), MAX(name)) "
                    f"FROM dict_categories "
                    f"WHERE category_id IN ({cat_ph}) AND is_active=1 "
                    f"GROUP BY category_id",
                    tuple(category_ids),
                )
                cat_name_map: Dict[int, str] = {r[0]: r[1] for r in cur.fetchall()}

                # 冶炼厂名称
                cur.execute(
                    f"SELECT id, name FROM dict_factories WHERE id IN ({sm_ph})",
                    tuple(smelter_ids),
                )
                factory_name_map: Dict[int, str] = {r[0]: r[1] for r in cur.fetchall()}

                # 最新运费：每个(仓库, 冶炼厂)取最新日期，保留仓库维度
                cur.execute(
                    f"""
                    SELECT dw.id AS wid, dw.name AS wname,
                           df.id AS fid, df.name AS fname,
                           fr.price_per_ton
                    FROM freight_rates fr
                    JOIN dict_warehouses dw ON fr.warehouse_id = dw.id
                    JOIN dict_factories  df ON fr.factory_id  = df.id
                    WHERE dw.id IN ({wh_ph})
                      AND df.id IN ({sm_ph})
                      AND fr.effective_date = (
                          SELECT MAX(fr2.effective_date)
                          FROM freight_rates fr2
                          WHERE fr2.factory_id  = fr.factory_id
                            AND fr2.warehouse_id = fr.warehouse_id
                      )
                    """,
                    tuple(warehouse_ids) + tuple(smelter_ids),
                )
                # freight_map: {(warehouse_id, factory_id): freight}
                freight_map: Dict[tuple, float] = {
                    (r[0], r[2]): (float(r[4]) if r[4] is not None else 0.0) for r in cur.fetchall()
                }

                # 税率表
                cur.execute(
                    f"SELECT factory_id, tax_type, tax_rate "
                    f"FROM factory_tax_rates WHERE factory_id IN ({sm_ph})",
                    tuple(smelter_ids),
                )
                tax_rate_map: Dict[int, Dict[str, float]] = {}
                for fid, ttype, rate in cur.fetchall():
                    tax_rate_map.setdefault(fid, {})[ttype] = float(rate)

                # category_id → row_id 映射
                cur.execute(
                    f"SELECT row_id, category_id FROM dict_categories "
                    f"WHERE category_id IN ({cat_ph}) AND is_active = 1",
                    tuple(category_ids),
                )
                row_id_to_cat: Dict[int, int] = {row[0]: row[1] for row in cur.fetchall()}
                row_ids = list(row_id_to_cat.keys())

                if not row_ids:
                    return {"demand_rows": [], "raw": []}

                ri_ph = ",".join(["%s"] * len(row_ids))

                # 最新报价：查询所有价格列
                cur.execute(
                    f"""
                    SELECT factory_id, category_id,
                           unit_price, price_1pct_vat, price_3pct_vat, price_13pct_vat,
                           price_normal_invoice, price_reverse_invoice
                    FROM quote_details
                    WHERE factory_id IN ({sm_ph})
                      AND category_id IN ({ri_ph})
                      AND quote_date = (
                          SELECT MAX(qd2.quote_date)
                          FROM quote_details qd2
                          WHERE qd2.factory_id = quote_details.factory_id
                            AND qd2.category_id = quote_details.category_id
                      )
                    """,
                    tuple(smelter_ids) + tuple(row_ids),
                )
                col_names = ["unit_price", "price_1pct_vat", "price_3pct_vat",
                             "price_13pct_vat", "price_normal_invoice", "price_reverse_invoice"]
                raw_price_map: Dict[tuple, Dict[str, Optional[float]]] = {}
                for row in cur.fetchall():
                    fid_r, rid = row[0], row[1]
                    raw_price_map[(fid_r, rid)] = {
                        col: (float(v) if v is not None else None)
                        for col, v in zip(col_names, row[2:])
                    }

        # 价格反算逻辑
        COL_TO_TAX: Dict[str, str] = {
            "price_1pct_vat": "1pct",
            "price_3pct_vat": "3pct",
            "price_13pct_vat": "13pct",
        }

        def resolve_price(fid: int, rid: int) -> Optional[float]:
            prices = raw_price_map.get((fid, rid), {})
            rates = tax_rate_map.get(fid, {})

            # 1. 直接有目标列
            direct = prices.get(target_col)
            if direct is not None:
                return direct

            # 2. 目标是含税价，且有不含税基础价 + 目标税率
            if target_tax and prices.get("unit_price") is not None and target_tax in rates:
                base = prices["unit_price"]
                return round(base * (1 + rates[target_tax]), 2)

            # 3. 目标是基础价，从已知含税价反算
            if target_col == "unit_price":
                for col, src_tax in COL_TO_TAX.items():
                    known_price = prices.get(col)
                    if known_price is not None and src_tax in rates:
                        return round(known_price / (1 + rates[src_tax]), 2)

            # 4. 从其他已知含税价反算
            if target_tax and target_tax in rates:
                for col, src_tax in COL_TO_TAX.items():
                    known_price = prices.get(col)
                    if known_price is not None and src_tax in rates:
                        base = round(known_price / (1 + rates[src_tax]), 4)
                        return round(base * (1 + rates[target_tax]), 2)

            return None

        # 构建 price_map: {(factory_id, category_id): price}
        price_map: Dict[tuple, Optional[float]] = {}
        for (fid, rid), _ in raw_price_map.items():
            cid = row_id_to_cat.get(rid)
            if cid:
                price_map[(fid, cid)] = resolve_price(fid, rid)

        # 构造结构化数据：以需求为主体，报价统一（不含仓库），各仓库运费嵌套对比
        demand_rows = []
        raw = []
        for d in demands:
            fid = d["smelter_id"]
            cid = d["category_id"]
            fname = factory_name_map.get(fid, f"冶炼厂{fid}")
            cat_name = cat_name_map.get(cid, f"品类{cid}")
            price = price_map.get((fid, cid))
            demand_tons = d["demand"]

            warehouse_options = []
            for wid in warehouse_ids:
                wname = warehouse_name_map.get(wid, f"仓库{wid}")
                freight = freight_map.get((wid, fid))
                total_cost = (price + freight) if (price is not None and freight is not None) else None
                warehouse_options.append({
                    "仓库": wname,
                    "运费(元/吨)": freight,
                    "综合成本(元/吨)": total_cost,
                })
                raw.append({
                    "冶炼厂": fname,
                    "品类": cat_name,
                    "需求吨数": demand_tons,
                    "报价(元/吨)": price,
                    "仓库": wname,
                    "运费(元/吨)": freight,
                    "综合成本(元/吨)": total_cost,
                })

            demand_rows.append({
                "冶炼厂": fname,
                "品类": cat_name,
                "需求吨数(吨)": demand_tons,
                "报价(元/吨)": price,
                "各仓库运费对比": warehouse_options,
            })

        # 构造 prompt，调用 Claude
        import json
        from openai import OpenAI
        from app import config as app_config

        client = OpenAI(api_key=app_config.LLM_API_KEY, base_url=app_config.LLM_BASE_URL)
        data_str = json.dumps(demand_rows, ensure_ascii=False, indent=2)
        prompt = f"""以下是各需求的报价及各仓库运费数据：

{data_str}

请给出各仓库发车建议，要求：
1. 优先选综合成本低的仓库
2. 同仓库不同品类可混装，尽量整车（20-30吨）
3. 按仓库分段输出：仓库名、装车方案（品类+吨数+冶炼厂+综合成本）、备注
4. 数据缺失的在备注注明
5. 纯文本，简洁"""

        resp = client.chat.completions.create(
            model=app_config.LLM_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        suggestion = resp.choices[0].message.content

        return {"code": 200, "data": {"suggestion": suggestion, "raw": raw}}


# ==================== 单例工厂 ====================

_tl_service: Optional[TLService] = None


def get_tl_service() -> TLService:
    global _tl_service
    if _tl_service is None:
        _tl_service = TLService()
    return _tl_service
