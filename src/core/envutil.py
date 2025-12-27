from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

def load_env_file(path: str) -> Dict[str, str]:
    """Load KEY=VALUE lines into os.environ (best-effort)."""
    p = Path(path)
    if not p.exists():
        return {}
    loaded = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k:
            os.environ.setdefault(k, v)
            loaded[k] = v
    return loaded

def write_env_file(path: str, kv: Dict[str, str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Quant_System secret env (local)"]
    for k, v in kv.items():
        if v is None:
            continue
        lines.append(f"{k}={v}")
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
