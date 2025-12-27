# src/utils/fs_atomic.py
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Union

PathLike = Union[str, os.PathLike]

def atomic_write_text(dst: PathLike, text: str, encoding: str = "utf-8") -> Path:
    """
    Write text to dst atomically:
    1) write to temp file in same directory
    2) fsync temp
    3) os.replace(temp, dst)  (atomic on Windows/POSIX)
    """
    dst_path = Path(dst)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding=encoding,
        newline="",
        delete=False,
        dir=str(dst_path.parent),
        prefix=dst_path.name + ".tmp.",
        suffix=".txt",
    ) as f:
        tmp_path = Path(f.name)
        f.write(text)
        f.flush()
        os.fsync(f.fileno())

    os.replace(str(tmp_path), str(dst_path))
    return dst_path
