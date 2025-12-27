from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional


def read_env_file(env_path: Path) -> Dict[str, str]:
    """Read simple KEY=VALUE lines from .env.

    Notes
    -----
    - Ignores comments and blank lines.
    - Strips surrounding quotes.
    - Does not support multiline values.
    """
    if not env_path.exists():
        return {}
    out: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def load_env_from_root(root: Path, *, override: bool = True) -> Dict[str, str]:
    """Load `${root}/.env` into `os.environ`.

    Parameters
    ----------
    root:
        Project root.
    override:
        If True, overwrite existing env vars.
    """
    env_path = (root / ".env").resolve()
    data = read_env_file(env_path)
    if not data:
        return {}

    for k, v in data.items():
        if override or (k not in os.environ):
            os.environ[k] = v
    return data


def load_env_from_cfg_path(cfg_path: str, *, override: bool = True) -> Dict[str, str]:
    """Convenience: infer project root from config path and load .env."""
    from src.core.paths import project_root_from_cfg  # local import to avoid cycles

    root = project_root_from_cfg(cfg_path)
    return load_env_from_root(root, override=override)
