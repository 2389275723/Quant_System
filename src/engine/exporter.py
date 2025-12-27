from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

from ..core.fsutil import atomic_write_text, ensure_dir
from ..core.manifest import build_manifest, write_manifest
from ..core.timeutil import fmt_ts, now_cn
from .portfolio import Order

def export_targets_json(outbox_dir: str, trade_date: str, run_id: str, targets: List[Dict[str, Any]]) -> str:
    ensure_dir(outbox_dir)
    path = Path(outbox_dir) / f"targets_{trade_date}_{run_id}.json"
    obj = {
        "trade_date": trade_date,
        "run_id": run_id,
        "created_at": fmt_ts(now_cn()),
        "targets": targets,
    }
    atomic_write_text(str(path), json.dumps(obj, ensure_ascii=False, indent=2), tmp_path=str(path) + ".tmp", encoding="utf-8")
    return str(path)

def export_orders_csv(outbox_dir: str, trade_date: str, run_id: str, orders: List[Order],
                      filename: str = "orders.csv", tmp_name: str = "orders.tmp",
                      encoding: str = "utf-8-sig") -> str:
    ensure_dir(outbox_dir)
    out_csv = Path(outbox_dir) / filename
    tmp_csv = Path(outbox_dir) / tmp_name

    rows = []
    for o in orders:
        rows.append({
            "client_order_id": o.client_order_id,
            "trade_date": trade_date,
            "ts_code": o.ts_code,
            "side": o.side,
            "qty": int(o.qty),
            "price_type": o.price_type,
            "limit_price": "" if o.limit_price is None else float(o.limit_price),
            "reason": o.reason,
            "run_id": run_id,
        })

    # write CSV (atomic)
    with tmp_csv.open("w", newline="", encoding=encoding) as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [
            "client_order_id","trade_date","ts_code","side","qty","price_type","limit_price","reason","run_id"
        ])
        w.writeheader()
        for r in rows:
            w.writerow(r)
        f.flush()

    # atomic replace
    import os
    os.replace(str(tmp_csv), str(out_csv))
    return str(out_csv)

def export_manifest_for_orders(out_csv_path: str, manifest_path: str, extra: Dict[str, Any]) -> None:
    m = build_manifest(out_csv_path, extra=extra)
    write_manifest(manifest_path, m)
