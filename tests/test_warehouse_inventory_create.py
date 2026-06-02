"""库房库存手工录入日期单元测试。"""
import sys
import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

_vlm_stub = MagicMock()
_vlm_stub.QwenVLFullExtractor = MagicMock
_vlm_stub.VLMConfig = MagicMock
sys.modules.setdefault("app.services.vlm_extractor_service", _vlm_stub)

from app.models.tl import WarehouseInventoryCreate
from app.services.tl_service import TLService


class _FakeCursor:
    def __init__(self, handlers):
        self.handlers = handlers
        self.executed: list[tuple] = []
        self.lastrowid = 99
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
                handler(s, params)
                return
        if "INSERT INTO warehouse_inventory_snapshots" in s:
            self.lastrowid = 99

    def fetchone(self):
        return (1,)


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


class WarehouseInventoryCreateModelTests(unittest.TestCase):
    def test_inventory_date_chinese_key(self) -> None:
        body = WarehouseInventoryCreate.model_validate(
            {
                "库房id": 1,
                "品类id": 6,
                "当前库存": 3.0,
                "库存日期": "2026-06-01",
            }
        )
        self.assertEqual(body.库存日期, "2026-06-01")

    def test_inventory_date_camel_alias(self) -> None:
        body = WarehouseInventoryCreate.model_validate(
            {
                "库房id": 1,
                "品类id": 6,
                "当前库存": 3.0,
                "inventoryDate": "2026-06-01",
            }
        )
        self.assertEqual(body.库存日期, "2026-06-01")

    def test_inventory_date_snake_alias(self) -> None:
        body = WarehouseInventoryCreate.model_validate(
            {
                "库房id": 1,
                "品类id": 6,
                "当前库存": 3.0,
                "inventory_date": "2026-06-01",
            }
        )
        self.assertEqual(body.库存日期, "2026-06-01")


class WarehouseInventoryCreateServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = TLService()

    def _handlers(self):
        return {
            "SELECT category_id FROM dict_categories": lambda s, p: (6,),
            "SELECT id FROM dict_warehouses": lambda s, p: (1,),
        }

    def test_create_uses_input_inventory_date(self) -> None:
        cur = _FakeCursor(self._handlers())

        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with patch.object(
                self.service,
                "_sync_warehouse_current_inventory_from_snapshot",
                return_value=None,
            ):
                with patch.object(
                    TLService,
                    "_pricing_calendar_date",
                    side_effect=AssertionError("不应使用默认当天"),
                ):
                    result = self.service.create_warehouse_inventory(
                    warehouse_id=665,
                    category_id=6,
                    inventory_ton=3.011,
                        inventory_date="2026-06-01",
                    )

        insert_params = [
            p
            for s, p in cur.executed
            if "INSERT INTO warehouse_inventory_snapshots" in s
        ]
        self.assertEqual(len(insert_params), 1)
        self.assertEqual(insert_params[0][3], date(2026, 6, 1))
        self.assertEqual(result["data"]["库存日期"], "2026-06-01")

    def test_create_iso_datetime_truncates_to_date(self) -> None:
        cur = _FakeCursor(self._handlers())
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            result = self.service.create_warehouse_inventory(
                warehouse_id=1,
                category_id=6,
                inventory_ton=1.0,
                inventory_date="2026-06-01T16:00:00+08:00",
            )
        insert_params = [
            p
            for s, p in cur.executed
            if "INSERT INTO warehouse_inventory_snapshots" in s
        ]
        self.assertEqual(insert_params[0][3], date(2026, 6, 1))
        self.assertEqual(result["data"]["库存日期"], "2026-06-01")


if __name__ == "__main__":
    unittest.main()
