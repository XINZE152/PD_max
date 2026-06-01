"""库房库存删除单元测试。"""
import sys
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

_vlm_stub = MagicMock()
_vlm_stub.QwenVLFullExtractor = MagicMock
_vlm_stub.VLMConfig = MagicMock
sys.modules.setdefault("app.services.vlm_extractor_service", _vlm_stub)

from app.services.tl_service import TLService


class _FakeCursor:
    def __init__(self, handlers):
        self.handlers = handlers
        self.executed: list[tuple] = []
        self.rowcount = 0
        self.sync_called = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        s = " ".join(sql.split())
        for pattern, handler in self.handlers.items():
            if pattern in s:
                result = handler(s, params)
                if isinstance(result, dict):
                    self.rowcount = result.get("rowcount", 0)
                    self._last_fetch = result.get("fetchone")
                else:
                    self._last_fetch = result
                return
        self._last_fetch = None

    def fetchone(self):
        val = getattr(self, "_last_fetch", None)
        self._last_fetch = None
        return val


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


class WarehouseInventoryDeleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = TLService()

    def test_delete_all_categories_on_date(self) -> None:
        cur = _FakeCursor(
            {
                "SELECT id FROM dict_warehouses": lambda s, p: (1,),
                "DELETE FROM warehouse_inventory_snapshots": lambda s, p: {"rowcount": 3},
            }
        )
        sync_patch = patch.object(
            TLService,
            "_sync_warehouse_current_inventory_from_snapshot",
            lambda self, c, wh_id: setattr(c, "sync_called", True),
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with sync_patch:
                result = self.service.delete_warehouse_inventory(
                    warehouse_id=1,
                    inventory_date="2026-06-01",
                )
        self.assertEqual(result["code"], 200)
        self.assertEqual(result["data"]["deleted_count"], 3)
        self.assertTrue(cur.sync_called)
        delete_sqls = [e[0] for e in cur.executed if "DELETE FROM warehouse_inventory_snapshots" in e[0]]
        self.assertEqual(len(delete_sqls), 1)
        self.assertNotIn("category_id", delete_sqls[0])

    def test_delete_single_category_on_date(self) -> None:
        cur = _FakeCursor(
            {
                "SELECT id FROM dict_warehouses": lambda s, p: (1,),
                "SELECT category_id FROM dict_categories": lambda s, p: (10,),
                "DELETE FROM warehouse_inventory_snapshots": lambda s, p: {"rowcount": 1},
            }
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with patch.object(
                TLService, "_sync_warehouse_current_inventory_from_snapshot", lambda *a, **k: None
            ):
                result = self.service.delete_warehouse_inventory(
                    warehouse_id=1,
                    inventory_date="2026-06-01",
                    category_id=10,
                )
        self.assertEqual(result["data"]["品类id"], 10)
        delete_sqls = [e[0] for e in cur.executed if "DELETE FROM warehouse_inventory_snapshots" in e[0]]
        self.assertIn("category_id", delete_sqls[0])

    def test_delete_raises_when_no_records(self) -> None:
        cur = _FakeCursor(
            {
                "SELECT id FROM dict_warehouses": lambda s, p: (1,),
                "DELETE FROM warehouse_inventory_snapshots": lambda s, p: {"rowcount": 0},
            }
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with patch.object(
                TLService, "_sync_warehouse_current_inventory_from_snapshot", lambda *a, **k: None
            ):
                with self.assertRaises(ValueError) as ctx:
                    self.service.delete_warehouse_inventory(
                        warehouse_id=1,
                        inventory_date="2026-06-01",
                    )
        self.assertIn("该日期无库存记录", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
