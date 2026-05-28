import unittest

from app.price_tax_utils import merge_factory_rates
from app.services.tl_service import _build_comparison_price_metrics


class ComparisonRecoveryPriceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.merged = merge_factory_rates()
        self.qrow = {"unit_price": 9706.0}
        self.bases = ["base", "3pct"]
        self.t = 27.0

    def test_base_sort_basis_recovery_price(self) -> None:
        m = _build_comparison_price_metrics(
            price=9706.0,
            source="direct",
            qrow=self.qrow,
            merged=self.merged,
            target_tax=None,
            t=self.t,
            fr=0.0,
            bases=self.bases,
            sort_basis="base",
        )
        self.assertEqual(m["总回收价元每吨"], 9706.0)
        self.assertEqual(m["总回收价"], 262062.0)
        self.assertEqual(m["基准价"], 9706.0)
        self.assertEqual(m["最优价各口径利润"]["base"], 262062.0)

    def test_3pct_sort_basis_higher_than_base(self) -> None:
        m_base = _build_comparison_price_metrics(
            price=9706.0,
            source="direct",
            qrow=self.qrow,
            merged=self.merged,
            target_tax=None,
            t=self.t,
            fr=100.0,
            bases=self.bases,
            sort_basis="base",
        )
        m_3 = _build_comparison_price_metrics(
            price=9706.0,
            source="direct",
            qrow=self.qrow,
            merged=self.merged,
            target_tax=None,
            t=self.t,
            fr=100.0,
            bases=self.bases,
            sort_basis="3pct",
        )
        self.assertGreater(m_3["总回收价"], m_base["总回收价"])
        self.assertEqual(m_3["总回收价元每吨"], m_3["含3%税价"])
        freight = m_3["总运费"]
        self.assertEqual(
            m_3["总回收价"],
            round(m_3["利润_含3%"] + freight, 2),
        )
        self.assertEqual(
            m_3["总回收价"],
            round(m_3["最优价各口径利润"]["3pct"] + freight, 2),
        )

    def test_unavailable_recovery_price_null(self) -> None:
        m = _build_comparison_price_metrics(
            price=None,
            source="unavailable",
            qrow=None,
            merged=self.merged,
            target_tax=None,
            t=self.t,
            fr=50.0,
            bases=self.bases,
            sort_basis="base",
        )
        self.assertIsNone(m["总回收价"])
        self.assertIsNone(m["总回收价元每吨"])
        self.assertIsNone(m["总价"])
        self.assertEqual(m["利润"], round(-50.0 * self.t, 2))

    def test_price_type_3pct_does_not_change_recovery_when_sort_base(self) -> None:
        """price_type=3pct 时总价为不含税折合；总回收价仍随 sort_basis=base。"""
        p3_incl = round(9706.0 * (1 + self.merged["3pct"]), 2)
        m_base = _build_comparison_price_metrics(
            price=p3_incl,
            source="direct",
            qrow=self.qrow,
            merged=self.merged,
            target_tax="3pct",
            t=self.t,
            fr=0.0,
            bases=self.bases,
            sort_basis="base",
        )
        m_3 = _build_comparison_price_metrics(
            price=p3_incl,
            source="direct",
            qrow=self.qrow,
            merged=self.merged,
            target_tax="3pct",
            t=self.t,
            fr=0.0,
            bases=self.bases,
            sort_basis="3pct",
        )
        self.assertEqual(m_base["总回收价元每吨"], 9706.0)
        self.assertEqual(m_base["总价"], m_base["总回收价"])
        self.assertGreater(m_3["总回收价"], m_base["总回收价"])
        self.assertEqual(m_3["总价"], m_base["总价"])

    def test_return_dict_contains_recovery_keys(self) -> None:
        m = _build_comparison_price_metrics(
            price=9706.0,
            source="direct",
            qrow=self.qrow,
            merged=self.merged,
            target_tax=None,
            t=1.0,
            fr=0.0,
            bases=["base"],
            sort_basis="base",
        )
        self.assertIn("总回收价元每吨", m)
        self.assertIn("总回收价", m)


if __name__ == "__main__":
    unittest.main()
