from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict

import yaml


def stable_hash_dict(d: Dict[str, Any]) -> str:
    dumped = yaml.safe_dump(d, sort_keys=True, allow_unicode=True)
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()[:16]


def file_hash(path: str | Path) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:16]
