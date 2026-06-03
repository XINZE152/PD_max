"""confirm_price_table 逐行报价日期单元测试。"""
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


class _FakeCursor:
    def __init__(self):
        self.executed: list[tuple] = []
        self.lastrowid = 0
        self.rowcount = 1
        self._fetchone_queue: list = []
        self._fetchall_result: list = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        s = " ".join(sql.split())
        self._fetchone_queue = []
        self._fetchall_result = []
        if "SELECT category_id, name FROM dict_categories" in s:
            self._fetchall_result = [(1, "铜")]
        elif "SELECT id, is_active FROM dict_factories WHERE name" in s:
            self._fetchone_queue = [(10, 1)]
        elif "SELECT is_active FROM dict_factories WHERE id" in s:
            self._fetchone_queue = [(1,)]
        elif "SELECT factory_id, tax_type, tax_rate FROM factory_tax_rates" in s:
            self._fetchall_result = []
        elif "INSERT INTO quote_details" in s:
            self.lastrowid = 1

    def fetchone(self):
        if self._fetchone_queue:
            return self._fetchone_queue.pop(0)
        return None

    def fetchall(self):
        return self._fetchall_result


class _FakeConn:
    def __init__(self, cur: _FakeCursor):
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self._cur


class TestConfirmPriceTablePerRowDate(unittest.TestCase):
    def test_parse_calendar_date_flexible_slash(self):
        self.assertEqual(
            TLService._parse_calendar_date_str("2026/6/1"),
            date(2026, 6, 1),
        )

    def test_confirm_writes_different_dates_per_row(self):
        cur = _FakeCursor()

        @contextmanager
        def fake_get_conn():
            yield _FakeConn(cur)

        items = [
            {
                "报价日期": "2026-06-01",
                "冶炼厂名": "厂A",
                "冶炼厂id": 10,
                "品类名": "铜",
                "价格": 1000,
            },
            {
                "报价日期": "2026-06-02",
                "冶炼厂名": "厂A",
                "冶炼厂id": 10,
                "品类名": "铝",
                "价格": 2000,
            },
        ]

        def resolve(cur, name, **kw):
            return (name, 1)

        svc = TLService()
        with patch("app.services.tl_service.get_conn", fake_get_conn), patch(
            "app.services.tl_service.log_finance_event"
        ), patch.object(
            svc, "_resolve_quote_category_main_name", side_effect=resolve
        ):
            result = svc.confirm_price_table(items)

        self.assertEqual(result["code"], 200)
        inserts = [
            p
            for sql, p in cur.executed
            if p and "INSERT INTO quote_details" in " ".join(sql.split())
        ]
        self.assertEqual(len(inserts), 2)
        self.assertEqual(inserts[0][0], date(2026, 6, 1))
        self.assertEqual(inserts[1][0], date(2026, 6, 2))

    def test_replace_deletes_per_factory_date_pair(self):
        cur = _FakeCursor()

        @contextmanager
        def fake_get_conn():
            yield _FakeConn(cur)

        items = [
            {
                "报价日期": "2026-06-01",
                "冶炼厂名": "厂A",
                "冶炼厂id": 10,
                "品类名": "铜",
                "价格": 1000,
            },
            {
                "报价日期": "2026-06-02",
                "冶炼厂名": "厂A",
                "冶炼厂id": 10,
                "品类名": "铜",
                "价格": 1100,
            },
        ]

        svc = TLService()
        with patch("app.services.tl_service.get_conn", fake_get_conn), patch(
            "app.services.tl_service.log_finance_event"
        ), patch.object(
            svc, "_resolve_quote_category_main_name", return_value=("铜", 1)
        ):
            svc.confirm_price_table(items, replace_factory_quotes_on_date=True)

        deletes = [
            p
            for sql, p in cur.executed
            if p and "DELETE FROM quote_details" in " ".join(sql.split())
        ]
        self.assertEqual(len(deletes), 2)
        self.assertIn((date(2026, 6, 1), 10), deletes)
        self.assertIn((date(2026, 6, 2), 10), deletes)

    def test_missing_row_date_raises(self):
        svc = TLService()
        with self.assertRaises(ValueError) as ctx:
            svc.confirm_price_table(
                [{"冶炼厂名": "厂A", "品类名": "铜", "价格": 1000}]
            )
        self.assertIn("缺少报价日期", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
