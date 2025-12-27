from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


@dataclass
class ReconResult:
    ok: bool
    reason: str
    details: str = ""


def isclose_money(a: float, b: float, rel_tol: float = 1e-6, abs_tol: float = 0.01) -> bool:
    return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)


def load_ptrade_positions_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, encoding="utf-8", dtype=str)
