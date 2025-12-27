# scripts/tests/test_trade_calendar_gate.py
import unittest
from _common import run_safe, out, has_any

class TestTradeCalendarGate(unittest.TestCase):
    def test_non_trade_day_blocks(self):
        # 2025-12-28 is Sunday
        for job in ("night", "morning"):
            cp = run_safe(job, "2025-12-28")
            o = out(cp)
            self.assertNotEqual(cp.returncode, 0, msg=f"{job} should exit non-zero on NOT_TRADE_DAY.\n{o}")
            self.assertTrue(
                has_any(o, ["NOT_TRADE_DAY", "闈炰氦鏄撴棩", "not trade"]),
                msg=f"{job} should print a NOT_TRADE_DAY marker.\n{o}",
            )

if __name__ == "__main__":
    unittest.main()
