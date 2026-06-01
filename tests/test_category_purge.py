"""品类硬删除（purge_category / purge_category_row）单元测试。"""
import sys
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

# tl_service 依赖 VLM 等重型模块，测试前注入桩避免拉全量依赖
_vlm_stub = MagicMock()
_vlm_stub.QwenVLFullExtractor = MagicMock
_vlm_stub.VLMConfig = MagicMock
sys.modules.setdefault("app.services.vlm_extractor_service", _vlm_stub)

from app.services.tl_service import TLService


class _FakeCursor:
    """按 execute SQL 片段返回预设结果的简易 cursor。"""

    def __init__(self, handlers):
        self.handlers = handlers
        self.executed: list[tuple] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        key = None
        s = " ".join(sql.split())
        for pattern in self.handlers:
            if pattern in s:
                key = pattern
                break
        if key is None:
            self._last_fetch = None
            return
        result = self.handlers[key](s, params)
        if isinstance(result, dict) and "rowcount" in result:
            self.rowcount = result["rowcount"]
            self._last_fetch = result.get("fetchone")
        elif isinstance(result, list):
            self._last_fetch = None
            self._fetchall_result = result
        else:
            self._last_fetch = result

    def fetchone(self):
        if hasattr(self, "_fetchall_result") and self._fetchall_result is not None:
            rows = self._fetchall_result
            self._fetchall_result = None
            return rows[0] if rows else None
        val = getattr(self, "_last_fetch", None)
        self._last_fetch = None
        return val

    def fetchall(self):
        if hasattr(self, "_fetchall_result") and self._fetchall_result is not None:
            rows = self._fetchall_result
            self._fetchall_result = None
            return rows
        val = getattr(self, "_last_fetch", None)
        self._last_fetch = None
        return [val] if val is not None else []


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self._autocommit = True

    def autocommit(self, flag: bool):
        self._autocommit = flag

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def _mock_get_conn(cursor):
    @contextmanager
    def _cm():
        yield _FakeConn(cursor)

    return _cm()


class CategoryPurgeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = TLService()

    def test_purge_category_rejects_active_rows(self) -> None:
        cur = _FakeCursor(
            {
                "SELECT row_id, name, is_active FROM dict_categories": lambda s, p: [
                    (1, "铜", 1),
                    (2, "紫铜", 0),
                ],
            }
        )

        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with self.assertRaises(ValueError) as ctx:
                self.service.purge_category(301, cascade=True)
        self.assertIn("仍有启用中的别名", str(ctx.exception))

    def test_purge_category_not_found(self) -> None:
        cur = _FakeCursor(
            {
                "SELECT row_id, name, is_active FROM dict_categories": lambda s, p: [],
            },
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with self.assertRaises(ValueError) as ctx:
                self.service.purge_category(999, cascade=True)
        self.assertIn("不存在", str(ctx.exception))

    def test_purge_category_cascade_true_deletes_children(self) -> None:
        counts = iter([(0,), (0,), (2,), (3,), (1,), (1,), (2,)])

        def count_handler(s, p):
            return next(counts)

        cur = _FakeCursor(
            {
                "SELECT row_id, name, is_active FROM dict_categories": lambda s, p: [
                    (1, "铜", 0),
                    (2, "紫铜", 0),
                ],
                "COUNT(*)": count_handler,
                "DELETE FROM factory_demand_items": lambda s, p: {"rowcount": 0},
                "DELETE FROM warehouse_inventories": lambda s, p: {"rowcount": 0},
                "DELETE FROM quote_details": lambda s, p: {"rowcount": 3},
                "DELETE FROM warehouse_inventory_snapshots": lambda s, p: {"rowcount": 1},
                "DELETE FROM warehouse_category_receipt_prices": lambda s, p: {"rowcount": 1},
                "DELETE FROM warehouse_category_receipt_price_history": lambda s, p: {"rowcount": 2},
                "DELETE FROM dict_categories": lambda s, p: {"rowcount": 2},
            }
        )

        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with patch("app.services.tl_service.log_finance_event"):
                result = self.service.purge_category(301, cascade=True)

        self.assertEqual(result["code"], 200)
        self.assertTrue(result["cascade"])
        self.assertEqual(result["deleted_counts"]["quote_details"], 3)
        self.assertEqual(result["deleted_counts"]["dict_categories"], 2)
        delete_sqls = [e[0] for e in cur.executed]
        self.assertTrue(any("DELETE FROM quote_details" in s for s in delete_sqls))
        self.assertTrue(any("DELETE FROM dict_categories" in s for s in delete_sqls))
        self.assertTrue(
            any("warehouse_category_receipt_price_history" in s for s in delete_sqls)
        )

    def test_purge_category_cascade_false_with_children_raises(self) -> None:
        counts = iter([(1,), (0,), (2,), (0,), (0,), (0,), (0,)])

        cur = _FakeCursor(
            {
                "SELECT row_id, name, is_active FROM dict_categories": lambda s, p: [(1, "铜", 0)],
                "COUNT(*)": lambda s, p: next(counts),
            }
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with self.assertRaises(ValueError) as ctx:
                self.service.purge_category(301, cascade=False)
        self.assertIn("cascade=false", str(ctx.exception))

    def test_purge_category_row_rejects_active(self) -> None:
        cur = _FakeCursor(
            {
                "SELECT row_id, category_id, name, is_active FROM dict_categories WHERE row_id": lambda s, p: (3, 301, "黄铜", 1),
            }
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with self.assertRaises(ValueError) as ctx:
                self.service.purge_category_row(3, cascade=True)
        self.assertIn("仍为启用状态", str(ctx.exception))

    def test_purge_category_row_not_found(self) -> None:
        cur = _FakeCursor(
            {
                "SELECT row_id, category_id, name, is_active FROM dict_categories WHERE row_id": lambda s, p: None,
            },
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with self.assertRaises(ValueError) as ctx:
                self.service.purge_category_row(999, cascade=True)
        self.assertIn("不存在", str(ctx.exception))

    def test_purge_category_row_not_last_keeps_snapshots(self) -> None:
        counts = iter([(0,), (0,), (1,), (0,), (2,)])

        cur = _FakeCursor(
            {
                "SELECT row_id, category_id, name, is_active FROM dict_categories WHERE row_id": lambda s, p: (2, 301, "紫铜", 0),
                "COUNT(*)": lambda s, p: next(counts),
                "DELETE FROM factory_demand_items": lambda s, p: {"rowcount": 0},
                "DELETE FROM warehouse_inventories": lambda s, p: {"rowcount": 0},
                "DELETE FROM quote_details": lambda s, p: {"rowcount": 0},
                "DELETE FROM dict_categories": lambda s, p: {"rowcount": 1},
            }
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with patch("app.services.tl_service.log_finance_event"):
                result = self.service.purge_category_row(2, cascade=True)

        self.assertEqual(result["code"], 200)
        delete_sqls = [e[0] for e in cur.executed]
        self.assertFalse(
            any("warehouse_inventory_snapshots" in s for s in delete_sqls)
        )

    def test_purge_category_row_last_row_cleans_snapshots(self) -> None:
        counts = iter([(0,), (0,), (1,), (0,), (1,), (2,), (1,), (2,)])

        cur = _FakeCursor(
            {
                "SELECT row_id, category_id, name, is_active FROM dict_categories WHERE row_id": lambda s, p: (2, 301, "紫铜", 0),
                "COUNT(*)": lambda s, p: next(counts),
                "DELETE FROM factory_demand_items": lambda s, p: {"rowcount": 0},
                "DELETE FROM warehouse_inventories": lambda s, p: {"rowcount": 0},
                "DELETE FROM quote_details": lambda s, p: {"rowcount": 0},
                "DELETE FROM warehouse_inventory_snapshots": lambda s, p: {"rowcount": 2},
                "DELETE FROM warehouse_category_receipt_prices": lambda s, p: {"rowcount": 1},
                "DELETE FROM warehouse_category_receipt_price_history": lambda s, p: {"rowcount": 2},
                "DELETE FROM dict_categories": lambda s, p: {"rowcount": 1},
            }
        )
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            with patch("app.services.tl_service.log_finance_event"):
                result = self.service.purge_category_row(2, cascade=True)

        self.assertEqual(result["deleted_counts"]["warehouse_inventory_snapshots"], 2)
        delete_sqls = [e[0] for e in cur.executed]
        self.assertTrue(
            any("warehouse_inventory_snapshots" in s for s in delete_sqls)
        )


if __name__ == "__main__":
    unittest.main()
