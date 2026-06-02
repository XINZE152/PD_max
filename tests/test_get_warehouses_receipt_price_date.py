"""get_warehouses 收货价格按品种含价格日期。"""
import sys
import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

_vlm_stub = MagicMock()
_vlm_stub.QwenVLFullExtractor = MagicMock
_vlm_stub.VLMConfig = MagicMock
sys.modules.setdefault("app.services.vlm_extractor_service", _vlm_stub)

from app.services.tl_service import TLService
from tests.test_warehouse_receipt_price_history import _FakeConn, _SeqCursor


@contextmanager
def _mock_get_conn(cur):
    yield _FakeConn(cur)


class GetWarehousesReceiptPriceDateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = TLService()

    def test_enrich_receipt_prices_include_price_date(self) -> None:
        cur = _SeqCursor(
            [
                {"fetchall": []},
                {
                    "fetchall": [
                        (1, 6, 9390.0, "电动车电瓶", date(2026, 6, 2)),
                        (1, 12, 9390.0, "电轿电瓶", date(2026, 6, 2)),
                    ]
                },
            ]
        )
        rows = [{"仓库id": 1}]
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            out = self.service._enrich_warehouse_rows_inventory_and_prices(rows)

        self.assertEqual(len(out), 1)
        prices = out[0]["收货价格按品种"]
        self.assertEqual(len(prices), 2)
        self.assertEqual(prices[0]["品类id"], 6)
        self.assertEqual(prices[0]["价格"], 9390.0)
        self.assertEqual(prices[0]["价格日期"], "2026-06-02")
        self.assertEqual(prices[1]["价格日期"], "2026-06-02")

        price_sql = cur.executed[1][0]
        self.assertIn("warehouse_category_receipt_price_history", price_sql)
        self.assertIn("latest_price", price_sql)

    def test_enrich_receipt_prices_null_date_without_history(self) -> None:
        cur = _SeqCursor(
            [
                {"fetchall": []},
                {"fetchall": [(2, 6, 8000.0, "电动车电瓶", None)]},
            ]
        )
        rows = [{"仓库id": 2}]
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            out = self.service._enrich_warehouse_rows_inventory_and_prices(rows)

        self.assertIsNone(out[0]["收货价格按品种"][0]["价格日期"])


if __name__ == "__main__":
    unittest.main()
