from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Optional

import pandas as pd


def atomic_write_csv(df: pd.DataFrame, out_path: str) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    tmp = out.with_suffix(out.suffix + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    # atomic rename on same filesystem
    tmp.replace(out)


def export_orders_csv(orders: pd.DataFrame, outbox_dir: str, trade_date: str, run_id: str) -> str:
    outbox = Path(outbox_dir)
    outbox.mkdir(parents=True, exist_ok=True)
    out_path = outbox / "orders.csv"
    atomic_write_csv(orders, str(out_path))
    return str(out_path)
