from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd


def apply_universe_filters(
    df: pd.DataFrame,
    exclude_prefixes: Optional[Iterable[str]] = None,
    *,
    exclude_bj: bool = True,
    max_total_mv: Optional[float] = None,
    mv_col: str = "total_mv",
    mv_fallback_col: str = "circ_mv",
) -> pd.DataFrame:
    """Universe hard filter.

    - Exclude by stock code prefix (e.g. 300/301/688/689).
    - Optionally exclude Beijing Stock Exchange (ts_code endswith '.BJ').
    - Optionally filter by market cap (default uses `total_mv`, fallback `circ_mv`).

    Notes
    -----
    * Market cap units follow your `daily_bars.csv` columns. In our pipeline it is RMB.
      For example, 500亿 = 5e10.
    """
    if df is None or df.empty:
        return df

    exclude_prefixes = list(exclude_prefixes or [])

    ts = df.get("ts_code")
    if ts is None:
        return df

    codes = ts.astype(str)

    # 1) Prefix exclusion (GEM/STAR etc.)
    if exclude_prefixes:
        pure = codes.str.split(".", n=1).str[0]
        prefix_mask = ~pure.apply(lambda x: any(str(x).startswith(p) for p in exclude_prefixes))
    else:
        prefix_mask = pd.Series(True, index=df.index)

    # 2) Exclude BJ board
    if exclude_bj:
        bj_mask = ~codes.str.upper().str.endswith(".BJ")
    else:
        bj_mask = pd.Series(True, index=df.index)

    out = df.loc[prefix_mask & bj_mask].copy()

    # 3) Market cap filter
    if max_total_mv is not None:
        col = mv_col if mv_col in out.columns else (mv_fallback_col if mv_fallback_col in out.columns else None)
        if col is not None:
            mv = pd.to_numeric(out[col], errors="coerce")
            out = out.loc[mv.notna() & (mv <= float(max_total_mv))].copy()

    out["universe_flag"] = 1
    return out
