# scripts/tests/test_float_compare.py
from __future__ import annotations

import unittest
from src.utils.float_cmp import isclose_money, eq_money

class TestFloatCompare(unittest.TestCase):
    def test_money_abs_tol_cent(self) -> None:
        self.assertTrue(isclose_money(100.0, 99.999999))
        self.assertTrue(eq_money("100.00", "100"))
        self.assertFalse(isclose_money(100.0, 99.98, abs_tol=0.01))

    def test_float_edge(self) -> None:
        self.assertTrue(isclose_money(0.1 + 0.2, 0.3, abs_tol=1e-9))

if __name__ == "__main__":
    unittest.main()
