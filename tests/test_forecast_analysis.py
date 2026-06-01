"""预测依据文案单元测试。"""

from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from types import SimpleNamespace

from app.intelligent_prediction.services.forecast_analysis_service import explain_chart_summary


class ForecastAnalysisTests(unittest.TestCase):
    def test_explain_chart_summary_empty(self) -> None:
        text = explain_chart_summary(
            date_from=date(2026, 6, 1),
            date_to=date(2026, 6, 7),
            dates=[date(2026, 6, 1)],
            total_by_date=[Decimal("0")],
            detail_rows=[],
        )
        self.assertIn("无可用送货历史", text)

    def test_explain_chart_summary_with_rows(self) -> None:
        rows = [
            SimpleNamespace(
                warehouse="华北仓",
                product_variety="1#铅",
                smelter=None,
                price_factor=Decimal("1.05"),
                own_calibration_price=Decimal("15500"),
                predicted_weight=Decimal("104"),
            ),
            SimpleNamespace(
                warehouse="华南仓",
                product_variety="1#铅",
                smelter=None,
                price_factor=Decimal("0.96"),
                own_calibration_price=Decimal("14500"),
                predicted_weight=Decimal("77"),
            ),
        ]
        text = explain_chart_summary(
            date_from=date(2026, 6, 1),
            date_to=date(2026, 6, 1),
            dates=[date(2026, 6, 1)],
            total_by_date=[Decimal("181")],
            detail_rows=rows,
        )
        self.assertIn("2026-06-01", text)
        self.assertIn("2 个库房", text)
        self.assertIn("181", text)


if __name__ == "__main__":
    unittest.main()
