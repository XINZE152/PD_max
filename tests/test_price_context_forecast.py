"""价格因素与规则预测混合逻辑单元测试。"""

from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from app.intelligent_prediction.services.price_context_service import (
    DailyPriceContext,
    blend_history_and_price,
    compute_price_factor,
)


class PriceContextForecastTests(unittest.TestCase):
    def test_compute_price_factor_advantage(self) -> None:
        ctx = DailyPriceContext(
            as_of_date=date(2026, 1, 1),
            lead_market_price=Decimal("15000"),
            own_calibration_price=Decimal("15500"),
            competitor_price_max=Decimal("15200"),
            competitor_price_avg=Decimal("15100"),
        )
        factor = compute_price_factor(ctx, "sensitive")
        self.assertGreater(float(factor), 1.0)

    def test_compute_price_factor_disadvantage(self) -> None:
        ctx = DailyPriceContext(
            as_of_date=date(2026, 1, 1),
            lead_market_price=Decimal("15000"),
            own_calibration_price=Decimal("14500"),
            competitor_price_max=Decimal("15200"),
            competitor_price_avg=None,
        )
        factor = compute_price_factor(ctx, "sensitive")
        self.assertLess(float(factor), 1.0)

    def test_stable_warehouse_dampens_price(self) -> None:
        ctx = DailyPriceContext(
            as_of_date=date(2026, 1, 1),
            lead_market_price=Decimal("15000"),
            own_calibration_price=Decimal("14000"),
            competitor_price_max=Decimal("15200"),
            competitor_price_avg=None,
        )
        sens = float(compute_price_factor(ctx, "sensitive"))
        stable = float(compute_price_factor(ctx, "stable"))
        self.assertLess(abs(stable - 1.0), abs(sens - 1.0))

    def test_blend_history_and_price_default_weights(self) -> None:
        baseline = Decimal("100")
        factor = Decimal("1.2")
        out = blend_history_and_price(baseline, factor)
        # 0.2 * 100 + 0.8 * 100 * 1.2 = 20 + 96 = 116
        self.assertEqual(out, Decimal("116"))


if __name__ == "__main__":
    unittest.main()
