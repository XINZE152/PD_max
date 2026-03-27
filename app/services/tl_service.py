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
from app.services.battery_quote_service1 import BatteryQuoteService

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
                    warehouse_code = f"WH_{uuid.uuid4().hex[:8].upper()}"
                    cur.execute(
                        "INSERT INTO dict_warehouses (warehouse_code, name, is_active) VALUES (%s, %s, 1)",
                        (warehouse_code, name),
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
    ) -> List[Dict[str, Any]]:
        if not warehouse_ids or not smelter_ids or not category_ids:
            return []

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    wh_placeholders = ",".join(["%s"] * len(warehouse_ids))
                    sm_placeholders = ",".join(["%s"] * len(smelter_ids))
                    cat_placeholders = ",".join(["%s"] * len(category_ids))

                    # 品类主名称
                    cur.execute(
                        f"SELECT DISTINCT category_id, "
                        f"COALESCE(MAX(CASE WHEN is_main=1 THEN name END), MAX(name)) AS cat_name "
                        f"FROM dict_categories "
                        f"WHERE category_id IN ({cat_placeholders}) AND is_active = 1 "
                        f"GROUP BY category_id",
                        tuple(category_ids),
                    )
                    cat_map: Dict[int, str] = {row[0]: row[1] for row in cur.fetchall()}

                    # 最新运费：每个(仓库,冶炼厂)取最新日期
                    cur.execute(
                        f"""
                        SELECT dw.id, dw.name, df.id, df.name, fr.price_per_ton
                        FROM freight_rates fr
                        JOIN dict_warehouses dw ON fr.warehouse_id = dw.id
                        JOIN dict_factories  df ON fr.factory_id  = df.id
                        WHERE dw.id IN ({wh_placeholders})
                          AND df.id IN ({sm_placeholders})
                          AND fr.effective_date = (
                              SELECT MAX(fr2.effective_date)
                              FROM freight_rates fr2
                              WHERE fr2.factory_id  = fr.factory_id
                                AND fr2.warehouse_id = fr.warehouse_id
                          )
                        """,
                        tuple(warehouse_ids) + tuple(smelter_ids),
                    )
                    # freight_map: {(warehouse_id, factory_id): (wname, fname, freight)}
                    freight_map: Dict[tuple, tuple] = {}
                    for wid, wname, fid, fname, freight in cur.fetchall():
                        freight_map[(wid, fid)] = (wname, fname, freight)

                    # 最新报价：每个(冶炼厂,品类)取最新日期
                    cur.execute(
                        f"""
                        SELECT factory_id, category_id, unit_price
                        FROM quote_details
                        WHERE factory_id IN ({sm_placeholders})
                          AND category_id IN ({cat_placeholders})
                          AND quote_date = (
                              SELECT MAX(qd2.quote_date)
                              FROM quote_details qd2
                              WHERE qd2.factory_id  = quote_details.factory_id
                                AND qd2.category_id = quote_details.category_id
                          )
                        """,
                        tuple(smelter_ids) + tuple(category_ids),
                    )
                    # price_map: {(factory_id, category_id): unit_price}
                    price_map: Dict[tuple, float] = {
                        (row[0], row[1]): float(row[2]) for row in cur.fetchall()
                    }

                    # 组合结果
                    result = []
                    for (wid, fid), (wname, fname, freight) in freight_map.items():
                        for cid in category_ids:
                            cat_name = cat_map.get(cid)
                            if cat_name is None:
                                continue
                            result.append({
                                "仓库": wname,
                                "冶炼厂": fname,
                                "品类": cat_name,
                                "运费": float(freight) if freight is not None else None,
                                "报价": price_map.get((fid, cid)),
                            })
                    return result

        except Exception as e:
            logger.error(f"获取比价表失败: {e}")
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

            # 2. OCR 识别，直接返回原始识别结果
            ocr_service = BatteryQuoteService()

            items: List[Dict[str, Any]] = []
            details: List[Dict[str, Any]] = []

            for image_path, md5, orig_name in saved_paths:
                ocr_result = ocr_service.parse_image(image_path)

                if ocr_result.get("error"):
                    details.append({"image": orig_name, "error": ocr_result["error"]})
                    continue

                factory_name_ocr = ocr_result.get("factory", "未知工厂")
                image_detail: Dict[str, Any] = {
                    "image": orig_name,
                    "factory_name": factory_name_ocr,
                    "date": ocr_result.get("date"),
                    "items": [],
                }

                for item in ocr_result.get("items", []):
                    row_item = {
                        "冶炼厂名": factory_name_ocr,
                        "品类名": item["category"],
                        "价格": item["price"],
                    }
                    items.append(row_item)
                    image_detail["items"].append(row_item)

                details.append(image_detail)

            return {
                "code": 200,
                "data": {
                    "items": items,
                    "details": details,
                },
            }

        except Exception as e:
            logger.error(f"上传价格表失败: {e}")
            for path, _, _ in saved_paths:
                try:
                    os.remove(path)
                except OSError:
                    pass
            raise

    # ==================== 接口5b：确认价格表写入数据库 ====================

    def confirm_price_table(
        self,
        quote_date_str: str,
        items: List[Dict[str, Any]],
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
                                factory_code = f"FAC_{uuid.uuid4().hex[:8].upper()}"
                                cur.execute(
                                    "INSERT INTO dict_factories (factory_code, name, is_active) "
                                    "VALUES (%s, %s, 1)",
                                    (factory_code, factory_name),
                                )
                                item["冶炼厂id"] = cur.lastrowid

                        # 2. 品类不存在则新建
                        if item.get("品类id") is None:
                            cat_name = item["品类名"]
                            cur.execute(
                                "SELECT category_id FROM dict_categories WHERE name = %s AND is_active = 1",
                                (cat_name,),
                            )
                            row = cur.fetchone()
                            if row:
                                item["品类id"] = row[0]
                            else:
                                cur.execute("SELECT COALESCE(MAX(category_id), 0) + 1 FROM dict_categories")
                                new_cat_id = cur.fetchone()[0]
                                cat_code = f"CAT_{uuid.uuid4().hex[:8].upper()}"
                                cur.execute(
                                    "INSERT INTO dict_categories "
                                    "(category_id, category_code, name, is_main, is_active) "
                                    "VALUES (%s, %s, %s, 1, 1)",
                                    (new_cat_id, cat_code, cat_name),
                                )
                                item["品类id"] = new_cat_id

                        # 3. 写入明细，相同(日期+冶炼厂+品类)则更新价格
                        cur.execute(
                            "INSERT INTO quote_details "
                            "(quote_date, factory_id, category_id, raw_category_name, unit_price) "
                            "VALUES (%s, %s, %s, %s, %s) "
                            "ON DUPLICATE KEY UPDATE "
                            "unit_price = VALUES(unit_price), "
                            "raw_category_name = VALUES(raw_category_name), "
                            "updated_at = CURRENT_TIMESTAMP",
                            (quote_dt, item["冶炼厂id"], item["品类id"], item["品类名"], item["价格"]),
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
                            category_code = f"CAT_{name.upper()[:10]}_{uuid.uuid4().hex[:6]}"
                            cur.execute(
                                "INSERT INTO dict_categories "
                                "(category_id, category_code, name, is_main, is_active) "
                                "VALUES (%s, %s, %s, %s, 1)",
                                (category_id, category_code, name, is_main),
                            )

            return {"code": 200, "msg": "品类映射表更新成功，数据已存入数据库"}

        except ValueError:
            raise
        except Exception as e:
            logger.error(f"更新品类映射失败: {e}")
            raise


# ==================== 单例工厂 ====================

_tl_service: Optional[TLService] = None


def get_tl_service() -> TLService:
    global _tl_service
    if _tl_service is None:
        _tl_service = TLService()
    return _tl_service
