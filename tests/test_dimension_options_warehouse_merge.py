"""测试 dimension_options_service 库房字典合并逻辑。

验证：AI 预测的仓库下拉框包含 dict_warehouses 中所有活跃库房，
即使某些库房在 pd_ip_delivery_records 中尚无送货记录。
"""

from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from app.intelligent_prediction.services.dimension_options_service import (
    _get_active_warehouse_names,
    _merged_sorted,
    list_dimensions_from_delivery_history,
    list_dimensions_from_prediction_results,
)


# ──────────────────────────────────────────────
# 纯函数测试：_merged_sorted
# ──────────────────────────────────────────────
class MergedSortedTests(unittest.TestCase):
    """_merged_sorted() 不依赖数据库，直接验证合并去重排序逻辑。"""

    def test_empty_existing_adds_all_supplement(self):
        result = _merged_sorted([], ["仓库B", "仓库A", "仓库C"])
        self.assertEqual(result, ["仓库A", "仓库B", "仓库C"])

    def test_empty_supplement_keeps_existing(self):
        result = _merged_sorted(["仓库A", "仓库B"], [])
        self.assertEqual(result, ["仓库A", "仓库B"])

    def test_both_empty(self):
        result = _merged_sorted([], [])
        self.assertEqual(result, [])

    def test_no_overlap_merges_all(self):
        result = _merged_sorted(["记录库房A"], ["字典库房B", "字典库房C"])
        self.assertEqual(result, ["字典库房B", "字典库房C", "记录库房A"])

    def test_full_overlap_no_duplicates(self):
        result = _merged_sorted(
            ["汶上县东环环保科技有限公司", "测试库房A"],
            ["汶上县东环环保科技有限公司", "测试库房B"],
        )
        # "汶上县东环环保科技有限公司" 只出现一次
        self.assertEqual(len(result), 3)
        self.assertEqual(result.count("汶上县东环环保科技有限公司"), 1)
        self.assertIn("测试库房A", result)
        self.assertIn("测试库房B", result)

    def test_partial_overlap_merges_correctly(self):
        result = _merged_sorted(["A", "C"], ["B", "C", "D"])
        self.assertEqual(result, ["A", "B", "C", "D"])

    def test_dedup_is_exact_string_match(self):
        """去重基于精确字符串匹配——不同写法视为不同库房。"""
        result = _merged_sorted(
            ["汶上县东环环保科技有限公司"],
            ["汶上县东环环保科技有限公司 "],  # 尾部多一个空格
        )
        self.assertIn("汶上县东环环保科技有限公司", result)
        self.assertIn("汶上县东环环保科技有限公司 ", result)
        self.assertEqual(len(result), 2)


# ──────────────────────────────────────────────
# 异步函数测试：_get_active_warehouse_names
# ──────────────────────────────────────────────
class GetActiveWarehouseNamesTests(unittest.TestCase):
    """验证 dict_warehouses 查询逻辑。"""

    def test_returns_scalar_names_ordered(self):
        session = AsyncMock()
        # mock 返回 DB 端 ORDER BY name 后的结果
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [
            "库房A",
            "库房B",
            "库房C",
        ]
        session.execute.return_value = mock_result

        async def _run():
            return await _get_active_warehouse_names(session)

        result = asyncio.run(_run())
        self.assertEqual(result, ["库房A", "库房B", "库房C"])

    def test_filters_empty_and_none(self):
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [
            "库房A",
            "",
            None,
            "   ",
            "库房B",
        ]
        session.execute.return_value = mock_result

        async def _run():
            return await _get_active_warehouse_names(session)

        result = asyncio.run(_run())
        # 空白和 None 被过滤
        self.assertEqual(result, ["库房A", "库房B"])

    def test_sql_contains_is_active_filter(self):
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        session.execute.return_value = mock_result

        async def _run():
            return await _get_active_warehouse_names(session)

        asyncio.run(_run())

        # 验证 SQL 中包含 is_active = 1 条件
        call_args = session.execute.call_args[0][0]
        sql_text = str(call_args)
        self.assertIn("is_active", sql_text)
        self.assertIn("dict_warehouses", sql_text)


# ──────────────────────────────────────────────
# 集成测试：list_dimensions_from_delivery_history
# ──────────────────────────────────────────────
class DeliveryHistoryDimensionsTests(unittest.TestCase):
    """验证送货历史维度查询会合并库房字典。"""

    def test_merges_dict_warehouses_into_delivery_warehouses(self):
        session = AsyncMock()

        # 模拟三次 execute 调用（大区经理、仓库、冶炼厂 → 库房字典）
        call_results = []
        # 第 1 次：大区经理
        r1 = MagicMock()
        r1.scalars.return_value.all.return_value = ["经理A"]
        call_results.append(r1)
        # 第 2 次：送货记录中的仓库
        r2 = MagicMock()
        r2.scalars.return_value.all.return_value = ["送货库房X"]
        call_results.append(r2)
        # 第 3 次：冶炼厂
        r3 = MagicMock()
        r3.scalars.return_value.all.return_value = ["冶炼厂Y"]
        call_results.append(r3)
        # 第 4 次：dict_warehouses
        r4 = MagicMock()
        r4.scalars.return_value.all.return_value = [
            "送货库房X",  # 重复
            "汶上县东环环保科技有限公司",  # 新增（无送货记录）
            "另一个登记库房",
        ]
        call_results.append(r4)

        session.execute.side_effect = call_results

        async def _run():
            return await list_dimensions_from_delivery_history(session)

        result = asyncio.run(_run())

        self.assertEqual(result.regional_managers, ["经理A"])
        self.assertEqual(result.smelters, ["冶炼厂Y"])
        # 核心断言：仓库列表包含 dict_warehouses 中所有活跃库房
        self.assertIn("送货库房X", result.warehouses)
        self.assertIn("汶上县东环环保科技有限公司", result.warehouses)
        self.assertIn("另一个登记库房", result.warehouses)
        # 去重：重复项不重复出现
        self.assertEqual(result.warehouses.count("送货库房X"), 1)
        # 排序验证
        self.assertEqual(result.warehouses, sorted(result.warehouses))

    def test_no_delivery_records_still_shows_dict_warehouses(self):
        """即使没有任何送货记录，库房字典中的库房也应出现在下拉框中。"""
        session = AsyncMock()

        call_results = []
        # 大区经理：空
        r1 = MagicMock()
        r1.scalars.return_value.all.return_value = []
        call_results.append(r1)
        # 送货记录仓库：空
        r2 = MagicMock()
        r2.scalars.return_value.all.return_value = []
        call_results.append(r2)
        # 冶炼厂：空
        r3 = MagicMock()
        r3.scalars.return_value.all.return_value = []
        call_results.append(r3)
        # dict_warehouses：有数据
        r4 = MagicMock()
        r4.scalars.return_value.all.return_value = [
            "汶上县东环环保科技有限公司",
            "库房B",
        ]
        call_results.append(r4)

        session.execute.side_effect = call_results

        async def _run():
            return await list_dimensions_from_delivery_history(session)

        result = asyncio.run(_run())

        self.assertEqual(result.regional_managers, [])
        self.assertEqual(result.smelters, [])
        self.assertEqual(
            result.warehouses,
            ["库房B", "汶上县东环环保科技有限公司"],
        )


# ──────────────────────────────────────────────
# 集成测试：list_dimensions_from_prediction_results
# ──────────────────────────────────────────────
class PredictionResultsDimensionsTests(unittest.TestCase):
    """验证预测结果维度查询会合并库房字典。"""

    def test_merges_dict_warehouses_into_prediction_warehouses(self):
        session = AsyncMock()

        call_results = []
        r1 = MagicMock()
        r1.scalars.return_value.all.return_value = ["经理P"]
        call_results.append(r1)
        r2 = MagicMock()
        r2.scalars.return_value.all.return_value = ["预测库房Z"]
        call_results.append(r2)
        r3 = MagicMock()
        r3.scalars.return_value.all.return_value = ["冶炼厂Q"]
        call_results.append(r3)
        # dict_warehouses 查询
        r4 = MagicMock()
        r4.scalars.return_value.all.return_value = ["汶上县东环环保科技有限公司"]
        call_results.append(r4)

        session.execute.side_effect = call_results

        async def _run():
            return await list_dimensions_from_prediction_results(session)

        result = asyncio.run(_run())

        self.assertIn("预测库房Z", result.warehouses)
        self.assertIn("汶上县东环环保科技有限公司", result.warehouses)

    def test_fallback_path_also_merges_dict_warehouses(self):
        """回退路径（预测结果为空 → 送货历史）也需合并库房字典。"""
        session = AsyncMock()

        call_results = []
        # 预测结果路径：全部为空 → 触发回退
        for _ in range(3):
            r = MagicMock()
            r.scalars.return_value.all.return_value = []
            call_results.append(r)
        # 回退：送货历史 → 大区经理、仓库、冶炼厂
        for vals in (["送货经理"], ["送货库房"], ["送货冶炼厂"]):
            r = MagicMock()
            r.scalars.return_value.all.return_value = vals
            call_results.append(r)
        # 最后：dict_warehouses
        r_last = MagicMock()
        r_last.scalars.return_value.all.return_value = ["登记库房A"]
        call_results.append(r_last)

        session.execute.side_effect = call_results

        async def _run():
            return await list_dimensions_from_prediction_results(session)

        result = asyncio.run(_run())

        self.assertEqual(result.regional_managers, ["送货经理"])
        self.assertIn("送货库房", result.warehouses)
        self.assertIn("登记库房A", result.warehouses)


if __name__ == "__main__":
    unittest.main()
