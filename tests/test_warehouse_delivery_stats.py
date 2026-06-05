"""库房送货统计缓存 — 单元测试。

覆盖：
- _enrich_warehouse_rows_with_delivery_stats 补充字段
- aggregate_delivery_stats 聚合与 UPSERT
- get_warehouses 返回包含当月发货量/年度累计发货量
"""
import sys
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

_vlm_stub = MagicMock()
_vlm_stub.QwenVLFullExtractor = MagicMock
_vlm_stub.VLMConfig = MagicMock
sys.modules.setdefault("app.services.vlm_extractor_service", _vlm_stub)

from app.services.tl_service import TLService
from app.services.warehouse_delivery_stats_service import aggregate_delivery_stats


class _SeqCursor:
    """顺序 mock 游标：每次 execute 后按 steps 队列消耗 fetchone/fetchall 结果。"""

    def __init__(self, steps):
        self.steps = list(steps)
        self.executed: list[tuple] = []
        self.rowcount = 0
        self.lastrowid = 0
        self.description = []  # 模拟 PyMySQL cursor.description
        self._fetchone = None
        self._fetchall = None

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
            self.description = step.get("description", [])
        else:
            self._fetchone = step
            self._fetchall = None

    def fetchone(self):
        val = self._fetchone
        self._fetchone = None
        return val

    def fetchall(self):
        val = self._fetchall
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


class DeliveryStatsEnrichmentTests(unittest.TestCase):
    """测试仓库列表的发货统计字段补充逻辑。"""

    def setUp(self) -> None:
        self.service = TLService()

    def test_enrich_sets_stats_when_cache_hit(self) -> None:
        """缓存命中时设置当月发货量、年度累计发货量。"""
        rows = [
            {"仓库id": 1, "仓库名": "测试库房A", "类型": "普通合作库房"},
            {"仓库id": 2, "仓库名": "测试库房B", "类型": "垂直库房"},
        ]
        cur = _SeqCursor([
            {
                "fetchall": [
                    ("测试库房A", 88.5, 999.0),
                    ("测试库房B", 12.3, 456.7),
                ]
            }
        ])
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            result = self.service._enrich_warehouse_rows_with_delivery_stats(rows)

        self.assertEqual(result[0]["当月发货量"], 88.5)
        self.assertEqual(result[0]["年度累计发货量"], 999.0)
        self.assertEqual(result[1]["当月发货量"], 12.3)
        self.assertEqual(result[1]["年度累计发货量"], 456.7)

    def test_enrich_sets_zero_when_cache_miss(self) -> None:
        """缓存未命中时填充默认值 0.0。"""
        rows = [{"仓库id": 3, "仓库名": "新库房", "类型": "战略库房"}]
        cur = _SeqCursor([{"fetchall": []}])
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            result = self.service._enrich_warehouse_rows_with_delivery_stats(rows)

        self.assertEqual(result[0]["当月发货量"], 0.0)
        self.assertEqual(result[0]["年度累计发货量"], 0.0)

    def test_enrich_handles_empty_rows(self) -> None:
        """空列表原样返回。"""
        result = self.service._enrich_warehouse_rows_with_delivery_stats([])
        self.assertEqual(result, [])

    def test_enrich_preserves_existing_fields(self) -> None:
        """补充统计字段时不覆盖已有字段。"""
        rows = [{"仓库id": 1, "仓库名": "X库房", "月均收货": 50.0, "当前库存": 100.0}]
        cur = _SeqCursor([{"fetchall": [("X库房", 30.0, 500.0)]}])
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            result = self.service._enrich_warehouse_rows_with_delivery_stats(rows)

        self.assertEqual(result[0]["月均收货"], 50.0)
        self.assertEqual(result[0]["当前库存"], 100.0)
        self.assertEqual(result[0]["当月发货量"], 30.0)
        self.assertEqual(result[0]["年度累计发货量"], 500.0)


class AggregateDeliveryStatsTests(unittest.TestCase):
    """测试聚合服务的 SQL 生成与 UPSERT 逻辑。"""

    def test_aggregate_single_warehouse(self) -> None:
        """单个库房、单月数据应正确聚合。"""
        cur = _SeqCursor([
            {  # Step 1: 聚合查询
                "fetchall": [("测试库房", 100.0, 500.0)],
            },
            {},  # Step 2: UPSERT
        ])
        with patch(
            "app.services.warehouse_delivery_stats_service.get_conn",
            lambda: _mock_get_conn(cur),
        ):
            written = aggregate_delivery_stats()

        self.assertEqual(written, 1)
        sqls = [s for s, _ in cur.executed]
        # 第一条是聚合查询
        self.assertIn("SUM(CASE", sqls[0])
        self.assertIn("pd_ip_delivery_records", sqls[0])
        # 第二条是 UPSERT
        self.assertIn("INSERT INTO pd_warehouse_delivery_stats", sqls[1])
        self.assertIn("ON DUPLICATE KEY UPDATE", sqls[1])

    def test_aggregate_multiple_warehouses(self) -> None:
        """多个库房各自聚合。"""
        cur = _SeqCursor([
            {
                "fetchall": [
                    ("库房A", 50.0, 200.0),
                    ("库房B", 75.0, 300.0),
                    ("库房C", 0.0, 150.0),
                ],
            },
            {}, {}, {},  # 3 次 UPSERT
        ])
        with patch(
            "app.services.warehouse_delivery_stats_service.get_conn",
            lambda: _mock_get_conn(cur),
        ):
            written = aggregate_delivery_stats()

        self.assertEqual(written, 3)

    def test_aggregate_empty_delivery_records(self) -> None:
        """无送货记录时返回 0。"""
        cur = _SeqCursor([{"fetchall": []}])
        with patch(
            "app.services.warehouse_delivery_stats_service.get_conn",
            lambda: _mock_get_conn(cur),
        ):
            written = aggregate_delivery_stats()

        self.assertEqual(written, 0)
        # 不应该有 INSERT 语句
        sqls = " ".join(s for s, _ in cur.executed)
        self.assertNotIn("INSERT", sqls)

    def test_aggregate_sql_binds_target_date(self) -> None:
        """确认 SQL 参数包含正确的目标日期。"""
        from datetime import date, timedelta

        yesterday = date.today() - timedelta(days=1)
        cur = _SeqCursor([{"fetchall": []}])
        with patch(
            "app.services.warehouse_delivery_stats_service.get_conn",
            lambda: _mock_get_conn(cur),
        ):
            aggregate_delivery_stats()

        # 提取参数验证
        sql, params = cur.executed[0]
        self.assertIn("delivery_date", sql)
        # params 应包含 target_date 三次（monthly WHERE / yearly WHERE / outer WHERE）
        self.assertEqual(params[2], yesterday)


class GetWarehousesDeliveryStatsIntegrationTests(unittest.TestCase):
    """测试 get_warehouses 返回结构包含发货统计字段。"""

    def setUp(self) -> None:
        self.service = TLService()

    def test_non_paginated_returns_delivery_fields(self) -> None:
        """非分页模式返回的每行应包含当月发货量、年度累计发货量。"""
        _COLS = [
            ("仓库id",), ("仓库名",), ("地址",),
            ("省",), ("市",), ("区",),
            ("经度",), ("纬度",),
            ("库房联系人",), ("电话",), ("危废经营许可数量",),
            ("月均收货",), ("当前库存",), ("收货价格",), ("运费",),
            ("仓库类型id",), ("类型",), ("库房类型颜色配置",), ("仓库颜色配置",),
        ]
        cur = _SeqCursor([
            {  # Step 1: get_warehouses 自身的 SQL
                "description": _COLS,
                "fetchall": [
                    (
                        1, "库房X", "地址1", "省", "市", "区",
                        120.5, 30.2,  # 经度, 纬度
                        "张三", "1380000", 10,  # 联系人, 电话, 危废许可
                        50.0,  # 月均收货
                        100.0,  # 当前库存
                        5000.0,  # 收货价格
                        80.0,  # 运费
                        1, "普通合作库房", '{"marker":"#FF0000"}', None,
                    ),
                ],
            },
            # Step 2-3: _enrich_warehouse_rows_inventory_and_prices（库存快照 + 收货价格）
            {"fetchall": []},
            {"fetchall": []},
            # Step 4: _batch_warehouse_factory_freights
            {},
            # Step 5: _enrich_warehouse_rows_with_delivery_stats
            {"fetchall": []},
        ])
        # Mock _warehouse_type_ids_by_names and _batch_warehouse_type_colors
        with patch("app.services.tl_service.get_conn", lambda: _mock_get_conn(cur)):
            result = self.service.get_warehouses()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        row = result[0]
        self.assertIn("当月发货量", row)
        self.assertIn("年度累计发货量", row)
        # 默认值应为数值
        self.assertIsInstance(row["当月发货量"], (int, float))
        self.assertIsInstance(row["年度累计发货量"], (int, float))
        # 已有字段不受影响
        self.assertIn("月均收货", row)
        self.assertIn("当前库存", row)


if __name__ == "__main__":
    unittest.main()
