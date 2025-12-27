from __future__ import annotations

import datetime as _dt
import os
import random
import string
from typing import Optional


def now_cn() -> _dt.datetime:
    # Keep it simple: use local time. If you need strict tz handling, swap to zoneinfo.
    return _dt.datetime.now()


def today_cn() -> str:
    return now_cn().date().isoformat()


def make_run_id(prefix: str = "RUN") -> str:
    ts = now_cn().strftime("%Y%m%d_%H%M%S")
    rand = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(6))
    pid = os.getpid()
    return f"{prefix}_{ts}_{pid}_{rand}"
