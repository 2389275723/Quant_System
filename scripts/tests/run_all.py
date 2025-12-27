# scripts/tests/run_all.py
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).parent

def main() -> int:
    suite = unittest.defaultTestLoader.discover(str(HERE), pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    raise SystemExit(main())
