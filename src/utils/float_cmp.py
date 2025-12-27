# src/utils/float_cmp.py
from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    # Accept strings/Decimal-like
    try:
        return float(Decimal(str(x)))
    except (InvalidOperation, ValueError, TypeError):
        return None

def isclose_money(a: Any, b: Any, abs_tol: float = 0.01, rel_tol: float = 1e-6) -> bool:
    fa = _to_float(a)
    fb = _to_float(b)
    if fa is None or fb is None:
        return False
    return math.isclose(fa, fb, rel_tol=rel_tol, abs_tol=abs_tol)

def eq_money(a: Any, b: Any, abs_tol: float = 0.01, rel_tol: float = 1e-6) -> bool:
    return isclose_money(a, b, abs_tol=abs_tol, rel_tol=rel_tol)
