# scripts/tests/test_trade_calendar_gate.py
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from _common import run_safe, out, has_any
from src.core.trading_calendar import TradingCalendar


class _FakeSource:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.calls = 0

    def get_trade_cal(self, start_date=None, end_date=None):
        self.calls += 1
        return self.df


def _parse_last_json(text: str):
    for line in reversed([l for l in text.splitlines() if l.strip()]):
        try:
            return json.loads(line)
        except Exception:
            continue
    return None


class TestTradeCalendarGate(unittest.TestCase):
    def test_non_trade_day_blocks(self):
        # 2025-12-28 is Sunday
        for job in ("night", "morning"):
            cp = run_safe(job, "2025-12-28")
            o = out(cp)
            self.assertNotEqual(cp.returncode, 0, msg=f"{job} should exit non-zero on NOT_TRADE_DAY.\n{o}")
            self.assertTrue(
                has_any(o, ["NOT_TRADE_DAY", "非交易日", "not trade"]),
                msg=f"{job} should print a NOT_TRADE_DAY marker.\n{o}",
            )
            payload = _parse_last_json(o)
            if payload:
                self.assertEqual(payload.get("reason"), "NOT_TRADE_DAY")

    def test_trade_day_passes_via_cache(self):
        with TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cal.csv"
            cache.write_text("cal_date,is_open\n20250106,1\n", encoding="utf-8")
            cfg = {"trade_cal": {"cache_path": str(cache), "lookback_days": 30}}
            cal = TradingCalendar(cfg, cfg_path=str(cache))
            self.assertTrue(cal.is_trade_day("2025-01-06"))

    def test_cache_write_path_from_datasource(self):
        with TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cal.csv"
            df_remote = pd.DataFrame([{"cal_date": "2025-01-07", "is_open": 0}])
            src = _FakeSource(df_remote)
            cfg = {"trade_cal": {"cache_path": str(cache), "lookback_days": 10}}
            cal = TradingCalendar(cfg, cfg_path=str(cache), datasource=src)

            self.assertFalse(cache.exists())
            self.assertFalse(cal.is_trade_day("2025-01-07"))
            self.assertTrue(cache.exists())
            saved = pd.read_csv(cache)
            self.assertIn("20250107", saved["cal_date"].astype(str).tolist())
            self.assertEqual(src.calls, 1)


if __name__ == "__main__":
    unittest.main()
