"""库房按品种收货价格历史单元测试。"""
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


class _SeqCursor:
    def __init__(self, steps):
        self.steps = list(steps)
        self.executed: list[tuple] = []
        self.rowcount = 0
        self.lastrowid = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if not self.steps:
            return
        step = self.steps.pop(0)
        if isinstance(step, dict):
            self.rowcount = step.get("rowcount", 0)
            self.lastrowid = step.get("lastrowid", 0)
            self._fetchone = step.get("fetchone")
            self._fetchall = step.get("fetchall")
        else:
            self._fetchone = step

    def fetchone(self):
        val = getattr(self, "_fetchone", None)
        self._fetchone = None
        return val

    def fetchall(self):
        val = getattr(self, "_fetchall", None)
        self._fetchall = None
        return val or []


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def autocommit(self, flag: bool):
        pass

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass


@contextmanager
def _mock_get_conn(cur):
    yield _FakeConn(cur)


class WarehouseReceiptPriceHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = TLService()

    def test_sync_upserts_current_when_history_exists(self) -> None:
        cur = _SeqCursor([(15200.0,)])
        self.service._sync_warehouse_receipt_price_from_history(cur, 1, 2)
        sqls = " ".join(s for s, _ in cur.executed)
        self.assertIn("INSERT INTO warehouse_category_receipt_prices", sqls)

    def test_sync_deletes_current_when_no_history(self) -> None:
        cur = _SeqCursor([None])
        self.service._sync_warehouse_receipt_price_from_history(cur, 1, 2)
        sqls = " ".join(s for s, _ in cur.executed)
        self.assertIn("DELETE FROM warehouse_category_receipt_prices", sqls)

    def test_create_writes_history_and_syncs(self) -> None:
        cur = _SeqCursor(
            [
                (1,),
                (10,),
                {"lastrowid": 99},
                (15200.0,),
                (5,),
            ]
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with patch.object(
                TLService, "_pricing_calendar_date", return_value=date(2026, 6, 1)
            ):
                result = self.service.create_warehouse_receipt_price(
                    warehouse_id=1,
                    category_id=10,
                    price_per_ton=15200,
                    price_date="2026-06-01",
                )
        self.assertEqual(result["code"], 200)
        self.assertEqual(result["data"]["history_id"], 99)
        sqls = [s for s, _ in cur.executed]
        self.assertTrue(any("warehouse_category_receipt_price_history" in s for s in sqls))
        self.assertTrue(any("warehouse_category_receipt_prices" in s for s in sqls))

    def test_delete_history_syncs_current(self) -> None:
        cur = _SeqCursor([(1, 2), {"rowcount": 1}, (15000.0,)])
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            result = self.service.delete_warehouse_receipt_price_history(88)
        self.assertEqual(result["code"], 200)
        sqls = [s for s, _ in cur.executed]
        self.assertTrue(any("DELETE FROM warehouse_category_receipt_price_history" in s for s in sqls))
        self.assertTrue(any("INSERT INTO warehouse_category_receipt_prices" in s for s in sqls))


if __name__ == "__main__":
    unittest.main()
