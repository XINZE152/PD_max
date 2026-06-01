import io
import unittest
from decimal import Decimal

from openpyxl import Workbook

from app.services.warehouse_inventory_excel import parse_warehouse_inventory_workbook
from app.services.warehouse_receipt_price_excel import parse_warehouse_receipt_price_workbook


class WarehouseInventoryExcelTests(unittest.TestCase):
    def test_parse_inventory_workbook(self) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "导入数据"
        ws.append(["库房名称", "回收品种", "当前库存", "库存日期"])
        ws.append(["测试库房", "电动电瓶", 88.5, "2026-05-29"])
        buf = io.BytesIO()
        wb.save(buf)
        rows, meta = parse_warehouse_inventory_workbook(buf.getvalue())
        self.assertEqual(meta["parsed_rows"], 1)
        self.assertEqual(rows[0].warehouse_name, "测试库房")
        self.assertEqual(rows[0].category_name, "电动电瓶")
        self.assertEqual(rows[0].inventory_ton, Decimal("88.5"))

    def test_parse_inventory_requires_category_column(self) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "导入数据"
        ws.append(["库房名称", "当前库存", "库存日期"])
        ws.append(["测试库房", 88.5, "2026-05-29"])
        buf = io.BytesIO()
        wb.save(buf)
        with self.assertRaises(Exception):
            parse_warehouse_inventory_workbook(buf.getvalue())


class WarehouseReceiptPriceExcelTests(unittest.TestCase):
    def test_parse_receipt_price_workbook(self) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "导入数据"
        ws.append(["库房名称", "回收品种", "价格", "价格日期"])
        ws.append(["测试库房", "电动电瓶", 15200, "2026-06-01"])
        buf = io.BytesIO()
        wb.save(buf)
        rows, meta = parse_warehouse_receipt_price_workbook(buf.getvalue())
        self.assertEqual(meta["parsed_rows"], 1)
        self.assertEqual(rows[0].warehouse_name, "测试库房")
        self.assertEqual(rows[0].category_name, "电动电瓶")
        self.assertEqual(rows[0].price_per_ton, Decimal("15200"))
        self.assertEqual(rows[0].price_date.isoformat(), "2026-06-01")


if __name__ == "__main__":
    unittest.main()
