from __future__ import annotations

from pathlib import Path
from typing import Union

PathLike = Union[str, Path]


def project_root() -> Path:
    # src/core/paths.py -> project root is 3 parents up (Quant_System/)
    return Path(__file__).resolve().parents[3]


def resolve_from_root(rel: PathLike) -> Path:
    p = Path(rel)
    if p.is_absolute():
        return p
    return (project_root() / p).resolve()


def project_root_from_cfg(cfg_path: PathLike) -> Path:
    """Infer project root from a config file path.

    Expected layout:
      <root>/config/config.yaml
    If the cfg file lives somewhere else, fall back to its parent.
    """
    p = Path(cfg_path).resolve()
    if p.is_file():
        if p.parent.name.lower() == "config":
            return p.parent.parent
        return p.parent
    if p.is_dir():
        return p
    return project_root()


def resolve_from_cfg(cfg_path: PathLike, rel: PathLike) -> Path:
    """Resolve a path in config relative to the project root (not CWD)."""
    p = Path(rel)
    if p.is_absolute():
        return p
    root = project_root_from_cfg(cfg_path)
    return (root / p).resolve()
