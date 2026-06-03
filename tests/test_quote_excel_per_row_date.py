"""报价 Excel 逐行日期解析单元测试。"""
import sys
import unittest
from unittest.mock import MagicMock

_vlm_stub = MagicMock()
_vlm_stub.QwenVLFullExtractor = MagicMock
_vlm_stub.VLMConfig = MagicMock
sys.modules.setdefault("app.services.vlm_extractor_service", _vlm_stub)

from app.services.tl_service import TLService


class TestQuoteExcelPerRowDate(unittest.TestCase):
    def test_row_to_item_includes_normalized_date(self):
        item = TLService._excel_row_dict_to_confirm_item(
            {
                "smelter": "河南金利",
                "category": "电动",
                "quote_date": "2026/6/1",
                "net": 9560,
                "p3": 9960,
            }
        )
        self.assertEqual(item["报价日期"], "2026-06-01")
        self.assertEqual(item["冶炼厂名"], "河南金利")
        self.assertEqual(item["品类名"], "电动")

    def test_row_missing_date_raises(self):
        with self.assertRaises(ValueError) as ctx:
            TLService._excel_row_dict_to_confirm_item(
                {
                    "smelter": "厂A",
                    "category": "铜",
                    "net": 1000,
                }
            )
        self.assertIn("缺少报价日期", str(ctx.exception))

    def test_classify_date_column(self):
        self.assertEqual(TLService._classify_quote_excel_column("日期"), "quote_date")
        self.assertEqual(TLService._classify_quote_excel_column("报价日期"), "quote_date")


if __name__ == "__main__":
    unittest.main()
