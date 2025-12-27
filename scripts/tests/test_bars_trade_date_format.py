from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.data.manual_csv import ManualCSVSource


class TestBarsTradeDateFormat(unittest.TestCase):
    def test_manual_csv_accepts_both_formats(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bars.csv"
            path.write_text(
                "ts_code,trade_date,open,close\n"
                "000001.SZ,20251223,10,10.5\n"
                "000001.SZ,2025-12-24,11,11.2\n"
                "000001.SZ,20251225,12,12.3\n",
                encoding="utf-8",
            )

            cfg = {"data_source": {"manual_csv": {"bars_path": str(path)}}}
            src = ManualCSVSource(cfg)

            df = src.get_daily_bars(end_trade_date="2025-12-25", lookback_days=2)

            self.assertEqual(sorted(df["trade_date"].unique().tolist()), ["2025-12-24", "2025-12-25"])
            self.assertTrue(all("-" in td for td in df["trade_date"].unique()))
            # rows for the last two distinct trade dates only
            self.assertEqual(set(df["trade_date"].tolist()), {"2025-12-24", "2025-12-25"})


if __name__ == "__main__":
    unittest.main()
