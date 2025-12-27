from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests


@dataclass
class GDELTHeadline:
    title: str
    url: str
    source: str = "GDELT"
    published: Optional[str] = None


@dataclass
class GDELTConfig:
    enabled: bool = True
    base_url: str = "https://api.gdeltproject.org/api/v2/doc/doc"
    timeout_sec: int = 15
    max_items: int = 12
    # query template, will format with {q}
    query_template: str = "{q}"


def _to_isostr(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    try:
        # GDELT uses YYYYMMDDHHMMSS
        if len(v) == 14 and v.isdigit():
            return dt.datetime.strptime(v, "%Y%m%d%H%M%S").isoformat(timespec="seconds")
    except Exception:
        return v
    return v


def fetch_headlines(q: str, cfg: Optional[GDELTConfig] = None) -> List[GDELTHeadline]:
    """Fetch recent headlines from GDELT DOC 2.1 API.

    We intentionally store only title + link (no full text) to avoid copyright issues.
    """
    cfg = cfg or GDELTConfig()
    if not cfg.enabled:
        return []

    query = (cfg.query_template or "{q}").format(q=q)
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": int(cfg.max_items),
        "sort": "HybridRel",
    }
    r = requests.get(cfg.base_url, params=params, timeout=cfg.timeout_sec)
    r.raise_for_status()
    obj = r.json()

    arts = (obj.get("articles") or []) if isinstance(obj, dict) else []
    out: List[GDELTHeadline] = []
    for a in arts:
        title = (a.get("title") or "").strip()
        url = (a.get("url") or "").strip()
        if not title or not url:
            continue
        out.append(
            GDELTHeadline(
                title=title,
                url=url,
                published=_to_isostr(a.get("seendate") or a.get("published")),
            )
        )
    return out
