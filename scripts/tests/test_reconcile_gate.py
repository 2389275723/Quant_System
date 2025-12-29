# scripts/tests/test_reconcile_gate.py
import json
import unittest
from pathlib import Path
from _common import run_safe, out, has_any, ROOT


class TestReconcileGate(unittest.TestCase):
    def setUp(self):
        manual = ROOT / "data" / "manual"
        manual.mkdir(parents=True, exist_ok=True)
        self.sf = manual / "reconcile_status.json"
        self.backup = self.sf.read_text(encoding="utf-8", errors="replace") if self.sf.exists() else None

    def tearDown(self):
        if self.backup is not None:
            self.sf.write_text(self.backup, encoding="utf-8")
        else:
            try:
                self.sf.unlink()
            except FileNotFoundError:
                pass

    def _write_status(self, trade_date: str, ok: bool, reason: str = "TEST") -> None:
        payload = {
            "trade_date": trade_date,
            "ok": ok,
            "reason": reason,
            "run_id": "TEST",
            "ts": "2025-12-26T00:00:00",
        }
        self.sf.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def test_missing_status_blocks(self):
        try:
            self.sf.unlink()
        except FileNotFoundError:
            pass

        cp = run_safe("morning", "2025-12-26")
        o = out(cp)
        self.assertNotEqual(cp.returncode, 0, msg=f"morning should be blocked when status missing.\n{o}")
        self.assertTrue(
            has_any(o, ["RECONCILE_STATUS_BLOCK", "reconcile_status.json", "path="]),
            msg=f"missing status should be mentioned.\n{o}",
        )

    def test_mismatched_date_blocks(self):
        self._write_status("2025-12-25", ok=True, reason="yesterday only")

        cp = run_safe("morning", "2025-12-26")
        o = out(cp)
        self.assertNotEqual(cp.returncode, 0, msg=f"morning should block when trade_date mismatches.\n{o}")
        self.assertTrue(
            has_any(o, ["trade_date mismatch", "RECONCILE_STATUS_BLOCK"]),
            msg=f"mismatch reason should surface.\n{o}",
        )

    def test_ok_status_allows_pass(self):
        self._write_status("2025-12-26", ok=True, reason="orders ready")

        cp = run_safe("morning", "2025-12-26")
        o = out(cp)
        self.assertEqual(cp.returncode, 0, msg=f"morning should pass when status ok.\n{o}")
        self.assertFalse(
            has_any(o, ["RECONCILE_STATUS_BLOCK", "reconcile_status"]),
            msg=f"no reconcile block markers expected on pass.\n{o}",
        )


if __name__ == "__main__":
    unittest.main()
