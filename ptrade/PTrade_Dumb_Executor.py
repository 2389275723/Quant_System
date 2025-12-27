# -*- coding: utf-8 -*-
"""PTrade Dumb Executor (Receiver)

Design goals:
- 极度愚蠢：只读 orders.csv 逐行下单，不做计算
- 防重复：读后 rename 为 orders_processed_{date}_{run_id}.csv
- Kill Switch：发现 STOP 文件立即停止下单
- 心跳：持续写 ptrade_heartbeat.json 供 Windows UI 监控

⚠️ 你需要根据券商环境把下单函数替换为实际可用函数：
- order / order_target / order_value / order_target_value ...
"""

import csv
import json
import os
import time
from datetime import datetime


def _now_cn_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_mkdir(p):
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass


def _write_heartbeat(inbox_dir):
    hb = {
        "ts": _now_cn_str(),
        "epoch": time.time(),
        "msg": "alive",
    }
    path = os.path.join(inbox_dir, "ptrade_heartbeat.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(hb, f, ensure_ascii=False)
    except Exception:
        pass


def _normalize_symbol(ts_code: str) -> str:
    # 600519.SH -> 600519.SS (some environments use SS)
    # 000001.SZ -> 000001.SZ
    if not ts_code:
        return ts_code
    ts_code = ts_code.strip()
    if ts_code.endswith(".SH"):
        return ts_code.replace(".SH", ".SS")
    return ts_code


def _has_stop(stop_file: str) -> bool:
    try:
        return os.path.exists(stop_file)
    except Exception:
        return False


def _read_orders_csv(path: str):
    orders = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            orders.append(row)
    return orders


def _process_orders(context, orders, order_func):
    # order_func must be provided by your PTrade env (wrap it in initialize)
    for row in orders:
        ts_code = row.get("ts_code") or row.get("code") or ""
        side = (row.get("side") or "BUY").upper()
        qty = int(float(row.get("qty") or 0))
        if qty <= 0:
            continue

        symbol = _normalize_symbol(ts_code)

        # --- Replace this block with your broker's real order api.
        # Example:
        # if side == "BUY":
        #     order(symbol, qty)
        # else:
        #     order(symbol, -qty)
        try:
            order_func(symbol, side, qty, row)
        except Exception as e:
            # best effort: print log; do not crash the whole executor
            try:
                log.info(f"Order failed {symbol} {side} {qty} err={e}")
            except Exception:
                print("Order failed", symbol, side, qty, e)


def initialize(context):
    # ---- Bridge paths
    # Recommend: map Windows Quant_System/bridge to your PTrade research directory
    bridge_dir = os.environ.get("QUANT_BRIDGE_DIR", os.path.join(os.getcwd(), "bridge"))
    outbox_dir = os.path.join(bridge_dir, "outbox")
    inbox_dir = os.path.join(bridge_dir, "inbox")
    stop_file = os.path.join(bridge_dir, "STOP")

    _safe_mkdir(outbox_dir)
    _safe_mkdir(inbox_dir)

    context._bridge_dir = bridge_dir
    context._outbox_dir = outbox_dir
    context._inbox_dir = inbox_dir
    context._stop_file = stop_file

    # ---- Hook your real order function here
    def _order_func(symbol, side, qty, row):
        # TODO: replace with real PTrade order api
        # This placeholder just prints.
        try:
            log.info(f"[DUMB_EXEC] {side} {symbol} qty={qty}")
        except Exception:
            print("[DUMB_EXEC]", side, symbol, qty)

    context._order_func = _order_func

    # write a first heartbeat immediately so UI can turn green
    _write_heartbeat(context._inbox_dir)

    try:
        log.info("[DUMB_EXEC] initialized. bridge_dir=%s" % bridge_dir)
    except Exception:
        print("[DUMB_EXEC] initialized", bridge_dir)


def handle_data(context, data):
    # 1) heartbeat
    _write_heartbeat(context._inbox_dir)

    # 2) kill switch
    if _has_stop(context._stop_file):
        try:
            log.info("[DUMB_EXEC] STOP detected. skip.")
        except Exception:
            pass
        return

    orders_path = os.path.join(context._outbox_dir, "orders.csv")
    if not os.path.exists(orders_path):
        return

    # 3) read -> execute -> rename (防重复)
    try:
        orders = _read_orders_csv(orders_path)
    except Exception as e:
        try:
            log.info(f"[DUMB_EXEC] read orders.csv failed: {e}")
        except Exception:
            pass
        return

    if not orders:
        return

    run_id = orders[0].get("run_id") or "NA"
    d = datetime.now().strftime("%Y%m%d")
    processed = os.path.join(context._outbox_dir, f"orders_processed_{d}_{run_id}.csv")

    # Prevent re-processing if already renamed
    if os.path.exists(processed):
        return

    _process_orders(context, orders, context._order_func)

    # Rename orders.csv to processed (atomic-ish)
    try:
        os.rename(orders_path, processed)
    except Exception:
        # if rename fails, fallback to copy+remove
        try:
            import shutil
            shutil.copy2(orders_path, processed)
            os.remove(orders_path)
        except Exception:
            pass
