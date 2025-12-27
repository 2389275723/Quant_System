from __future__ import annotations

from typing import Any


def normalize_trade_date(value: Any, sep: str = "-") -> str:
    """
    Normalize trade_date strings.

    Supports inputs like "YYYYMMDD" or "YYYY-MM-DD" and returns a canonical
    string with the requested separator. Returns an empty string on blank input.
    """
    s = "" if value is None else str(value).strip()
    if not s:
        return ""
    digits = s.replace("-", "")
    if len(digits) == 8 and digits.isdigit():
        if sep:
            return f"{digits[0:4]}{sep}{digits[4:6]}{sep}{digits[6:8]}"
        return digits
    return s
