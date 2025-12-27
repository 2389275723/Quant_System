# scripts/tests/test_reconcile_gate.py
import json
import unittest
from pathlib import Path
from datetime import datetime
from _common import run_safe, out, has_any, ROOT

class TestReconcileGate(unittest.TestCase):
    def setUp(self):
        self.logs = ROOT / "logs"
        self.logs.mkdir(parents=True, exist_ok=True)
        self.sf = self.logs / "reconcile_status.json"
        self.backup = None
        if self.sf.exists():
            self.backup = self.sf.read_text(encoding="utf-8", errors="replace")

    def tearDown(self):
        if self.backup is not None:
            self.sf.write_text(self.backup, encoding="utf-8")
        else:
            try:
                self.sf.unlink()
            except FileNotFoundError:
                pass

    def test_reconcile_fail_blocks(self):
        self.sf.write_text(
            json.dumps(
                {
                    "status": "FAIL",
                    "last_trade_date": "2025-12-25",
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                    "reason": "TEST_INJECT_FAIL",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        # Use a weekday as "next day" (Fri)
        cp = run_safe("morning", "2025-12-26")
        o = out(cp)
        self.assertNotEqual(cp.returncode, 0, msg=f"morning should be blocked when reconcile FAIL.\n{o}")
        self.assertTrue(
            has_any(o, ["RECONCILE_FAIL", "瀵硅处", "reconcile"]),
            msg=f"should print a RECONCILE_FAIL marker.\n{o}",
        )

if __name__ == "__main__":
    unittest.main()
