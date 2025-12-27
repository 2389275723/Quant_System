import hashlib
from pathlib import Path
from typing import Iterable

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def sha256_file(path: str) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def sha256_files(paths: Iterable[str]) -> str:
    h = hashlib.sha256()
    for p in paths:
        h.update(Path(p).as_posix().encode("utf-8"))
        h.update(sha256_file(p).encode("utf-8"))
    return h.hexdigest()
