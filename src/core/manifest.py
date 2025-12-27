from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict
from pathlib import Path

from .hashutil import sha256_file
from .timeutil import now_cn, fmt_ts

def build_manifest(path: str, extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    p = Path(path)
    obj: Dict[str, Any] = {
        "path": str(p),
        "row_count": None,
        "sha256": sha256_file(str(p)) if p.exists() else None,
        "created_at": fmt_ts(now_cn()),
    }
    if extra:
        obj.update(extra)
    return obj

def write_manifest(manifest_path: str, payload: Dict[str, Any]) -> None:
    Path(manifest_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
