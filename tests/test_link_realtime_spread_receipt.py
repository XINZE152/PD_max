"""实时价差：电动车电瓶收货价口径单元测试。"""
import sys
import unittest
from decimal import Decimal
from unittest.mock import MagicMock

_vlm_stub = MagicMock()
_vlm_stub.QwenVLFullExtractor = MagicMock
_vlm_stub.VLMConfig = MagicMock
sys.modules.setdefault("app.services.vlm_extractor_service", _vlm_stub)

from app.services.tl_service import TLService


class _SpreadCursor:
    def __init__(self, handlers=None):
        self.handlers = handlers or {}
        self.executed: list[tuple] = []
        self.rowcount = 0
        self._fetchone = None
        self._fetchall: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        s = " ".join(sql.split())
        for pattern, handler in self.handlers.items():
            if pattern in s:
                result = handler(s, params)
                if isinstance(result, list):
                    self._fetchall = result
                elif result is not None:
                    self._fetchone = result
                return

    def fetchone(self):
        val = self._fetchone
        self._fetchone = None
        return val

    def fetchall(self):
        return self._fetchall


class LinkRealtimeSpreadReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = TLService()

    def test_load_warehouse_price_map_from_receipt_prices(self) -> None:
        cur = _SpreadCursor(
            {
                "SELECT category_id FROM dict_categories": lambda s, p: (6,),
                "FROM warehouse_category_receipt_prices": lambda s, p: [
                    (10, 9400.0),
                    (20, 9200.0),
                ],
            }
        )
        price_map = self.service._load_warehouse_price_map(cur)
        self.assertEqual(price_map[10], Decimal("9400.0"))
        self.assertEqual(price_map[20], Decimal("9200.0"))
        receipt_sqls = [
            s for s, _ in cur.executed if "warehouse_category_receipt_prices" in s
        ]
        self.assertTrue(receipt_sqls)
        self.assertIn("category_id = %s", receipt_sqls[0])

    def test_recompute_link_spread_uses_receipt_price_diff(self) -> None:
        cur = _SpreadCursor({})
        cur._updates: list = []

        def _on_execute2(s, p):
            if "UPDATE pd_warehouse_spread_configs" in s:
                cur.rowcount = 1
                cur._updates.append(p)
            elif "FROM warehouse_category_receipt_prices" in s:
                cur._fetchall = [(1, 9500.0), (2, 9300.0)]
            elif "FROM dict_warehouse_links" in s and "ORDER BY id ASC" in s:
                cur._fetchall = [(1, 2, 100)]
            elif "FROM dict_warehouse_links" in s and "WHERE" in s:
                cur._fetchall = [(1, 2)]
            elif "SELECT category_id FROM dict_categories" in s:
                cur._fetchone = (6,)

        cur.execute = lambda sql, params=None: _on_execute2(" ".join(sql.split()), params)

        updated = self.service._recompute_link_realtime_spreads(cur, [1])
        self.assertGreaterEqual(updated, 1)
        spread_updates = [p for p in cur._updates if p and p[1] is not None]
        self.assertTrue(spread_updates)
        self.assertEqual(spread_updates[0], (2, Decimal("200.0"), 1))

    def test_maybe_recompute_skips_non_ev_battery_category(self) -> None:
        cur = _SpreadCursor(
            {
                "SELECT category_id FROM dict_categories": lambda s, p: (6,),
            }
        )
        self.service._maybe_recompute_link_spreads_for_receipt_category(cur, 12, [1])
        self.assertFalse(
            any("UPDATE pd_warehouse_spread_configs" in s for s, _ in cur.executed)
        )


if __name__ == "__main__":
    unittest.main()
