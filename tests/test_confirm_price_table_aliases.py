"""confirm_price_table 请求体字段别名单元测试。"""
import unittest

from app.models.tl import ConfirmPriceTableItem, ConfirmPriceTableRequest


class TestConfirmPriceTableAliases(unittest.TestCase):
    def test_frontend_alias_field_names(self):
        body = ConfirmPriceTableRequest.model_validate(
            {
                "数据": [
                    {
                        "报价日期": "2026-06-03",
                        "冶炼厂": "某某冶炼厂",
                        "冶炼厂id": 123,
                        "品类": "铜",
                        "品类id": 456,
                        "价格": 70000,
                        "价格_3pct增值税": 72100,
                        "价格_13pct增值税": 79100,
                        "普通发票价格": 70000,
                        "反向发票价格": 69500,
                        "价格口径": "ex_vat",
                        "备注": "",
                    }
                ],
            }
        )
        item = body.数据[0]
        dumped = item.model_dump()
        self.assertEqual(dumped["报价日期"], "2026-06-03")
        self.assertEqual(dumped["冶炼厂名"], "某某冶炼厂")
        self.assertEqual(dumped["冶炼厂id"], 123)
        self.assertEqual(dumped["品类名"], "铜")
        self.assertEqual(dumped["品类id"], 456)
        self.assertEqual(dumped["价格"], 70000)
        self.assertEqual(dumped["价格口径"], "ex_vat")

    def test_date_alias_on_item(self):
        item = ConfirmPriceTableItem.model_validate(
            {
                "日期": "2026-06-01",
                "冶炼厂": "厂A",
                "品类": "铜",
                "价格": 1000,
            }
        )
        self.assertEqual(item.报价日期, "2026-06-01")

    def test_canonical_field_names_from_ocr(self):
        body = ConfirmPriceTableRequest.model_validate(
            {
                "数据": [
                    {
                        "报价日期": "2026-03-24",
                        "冶炼厂名": "山西亿晨环保科技有限公司",
                        "冶炼厂id": 1,
                        "品类名": "电动车电池",
                        "品类id": 3,
                        "价格_1pct增值税": 9550,
                        "价格_3pct增值税": 9737,
                    }
                ],
            }
        )
        item = body.数据[0]
        self.assertEqual(item.报价日期, "2026-03-24")
        self.assertEqual(item.冶炼厂名, "山西亿晨环保科技有限公司")
        self.assertEqual(item.品类名, "电动车电池")

    def test_english_alias_field_names(self):
        item = ConfirmPriceTableItem.model_validate(
            {
                "quote_date": "2026-05-01",
                "factory_name": "Test Smelter",
                "factory_id": 99,
                "category_name": "Aluminum",
                "category_id": 88,
                "价格": 5000,
            }
        )
        dumped = item.model_dump()
        self.assertEqual(dumped["报价日期"], "2026-05-01")
        self.assertEqual(dumped["冶炼厂名"], "Test Smelter")
        self.assertEqual(dumped["冶炼厂id"], 99)
        self.assertEqual(dumped["品类名"], "Aluminum")
        self.assertEqual(dumped["品类id"], 88)

    def test_factory_id_from_id_alias(self):
        item = ConfirmPriceTableItem.model_validate(
            {
                "报价日期": "2026-06-01",
                "冶炼厂": "Test Smelter",
                "id": 42,
                "品类": "铜",
                "价格": 1000,
            }
        )
        self.assertEqual(item.冶炼厂id, 42)

    def test_replace_factory_quotes_defaults_false(self):
        body = ConfirmPriceTableRequest.model_validate(
            {
                "数据": [
                    {"报价日期": "2026-06-03", "冶炼厂": "厂A", "品类": "铜", "价格": 1000},
                ],
            }
        )
        self.assertFalse(body.同冶炼厂当日整表覆盖)

    def test_missing_row_date_rejected(self):
        with self.assertRaises(Exception):
            ConfirmPriceTableRequest.model_validate(
                {
                    "数据": [
                        {"冶炼厂": "厂A", "品类": "铜", "价格": 1000},
                    ],
                }
            )

    def test_extra_fields_ignored_on_item(self):
        item = ConfirmPriceTableItem.model_validate(
            {
                "报价日期": "2026-06-01",
                "冶炼厂": "厂A",
                "品类": "铜",
                "价格": 1000,
                "unknown_field": "ignored",
            }
        )
        self.assertNotIn("unknown_field", item.model_dump())


if __name__ == "__main__":
    unittest.main()
