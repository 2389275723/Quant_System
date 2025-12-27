# scripts/tests/test_atomic_orders.py
from __future__ import annotations

import os
import unittest
from pathlib import Path
import tempfile

from src.utils.fs_atomic import atomic_write_text

class TestAtomicOrders(unittest.TestCase):
    def test_atomic_write_creates_full_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "orders.csv"
            content = "code,price,qty\n000001.SZ,10.01,100\n"
            atomic_write_text(p, content, encoding="utf-8")

            self.assertTrue(p.exists())
            self.assertEqual(p.read_text(encoding="utf-8"), content)

    def test_atomic_write_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "orders.csv"
            atomic_write_text(p, "A\n", encoding="utf-8")
            atomic_write_text(p, "B\n", encoding="utf-8")
            self.assertEqual(p.read_text(encoding="utf-8"), "B\n")

if __name__ == "__main__":
    unittest.main()
