from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)

def atomic_write_text(path: str, text: str, tmp_path: str | None = None, encoding: str = "utf-8") -> None:
    p = Path(path)
    if tmp_path is None:
        tmp_path = str(p.with_suffix(p.suffix + ".tmp"))
    tp = Path(tmp_path)
    ensure_dir(str(tp.parent))
    tp.write_text(text, encoding=encoding)
    os.replace(str(tp), str(p))  # atomic on Windows NTFS for same volume

def atomic_write_bytes(path: str, data: bytes, tmp_path: str | None = None) -> None:
    p = Path(path)
    if tmp_path is None:
        tmp_path = str(p.with_suffix(p.suffix + ".tmp"))
    tp = Path(tmp_path)
    ensure_dir(str(tp.parent))
    with tp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(str(tp), str(p))

def read_json(path: str, default: Dict[str, Any] | None = None) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return default or {}
    return json.loads(p.read_text(encoding="utf-8"))

def write_json(path: str, obj: Dict[str, Any], indent: int = 2) -> None:
    p = Path(path)
    ensure_dir(str(p.parent))
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=indent), encoding="utf-8")
