from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .config import get
from .paths import resolve_from_cfg
from ..data.datasource_policy import DataSourcePolicy
from ..utils.trade_date import normalize_trade_date


def _to_digits(trade_date: str) -> str:
    norm = normalize_trade_date(trade_date, sep="-")
    if not norm:
        return ""
    return norm.replace("-", "")


def _boolish(val: Any) -> bool:
    s = str(val).strip().lower()
    return s in {"1", "true", "y", "yes", "t"}


class TradingCalendar:
    """Cache-first trade calendar helper with Tushare backing."""

    def __init__(
        self,
        cfg: Dict[str, Any],
        cfg_path: str = "config/config.yaml",
        datasource: Optional[DataSourcePolicy] = None,
    ):
        tc_cfg = get(cfg, "trade_cal", {}) or {}
        cache_path = tc_cfg.get("cache_path", "data/trade_cal.csv")
        self.cache_path = resolve_from_cfg(cfg_path, cache_path)
        manual_seed = resolve_from_cfg(cfg_path, "data/manual/trade_cal.csv")
        self._seed_paths: List[Path] = [self.cache_path]
        if manual_seed != self.cache_path:
            self._seed_paths.append(manual_seed)
        self.lookback_days = int(tc_cfg.get("lookback_days", 366))
        self.datasource = datasource or DataSourcePolicy(cfg)
        self.last_error: Optional[str] = None

        self._cache_df: pd.DataFrame = pd.DataFrame(columns=["cal_date", "is_open"])
        self._cache_lookup: Dict[str, bool] = {}
        self._load_cache()

    # ---- cache I/O helpers -------------------------------------------------
    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["cal_date", "is_open"])

        out = df.copy()
        if "cal_date" not in out.columns:
            for col in ("trade_date", "date"):
                if col in out.columns:
                    out["cal_date"] = out[col]
                    break

        if "cal_date" not in out.columns:
            return pd.DataFrame(columns=["cal_date", "is_open"])

        out["cal_date"] = out["cal_date"].astype(str).str.replace("-", "").str.strip()
        out = out[out["cal_date"].str.len() == 8]

        open_col = None
        for col in ("is_open", "open", "is_trade_day", "is_open_flag"):
            if col in out.columns:
                open_col = col
                break
        if open_col is None:
            out["is_open"] = True
        else:
            out["is_open"] = out[open_col].apply(_boolish)

        out["is_open"] = out["is_open"].astype(bool)
        out = out[["cal_date", "is_open"]].drop_duplicates(subset=["cal_date"], keep="last")
        return out

    def _load_cache_csv(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame(columns=["cal_date", "is_open"])
        try:
            df = pd.read_csv(path, dtype={"cal_date": str, "is_open": object})
        except Exception:
            return pd.DataFrame(columns=["cal_date", "is_open"])
        return df

    def _load_cache_sqlite(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame(columns=["cal_date", "is_open"])
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS trade_calendar (cal_date TEXT PRIMARY KEY, is_open INTEGER)"
            )
            df = pd.read_sql_query("SELECT cal_date, is_open FROM trade_calendar", conn)
        finally:
            conn.close()
        return df

    def _persist_cache(self, df: pd.DataFrame) -> None:
        path = Path(self.cache_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df = df.copy()
        df["is_open"] = df["is_open"].astype(int)

        if path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS trade_calendar (cal_date TEXT PRIMARY KEY, is_open INTEGER)"
                )
                conn.executemany(
                    "INSERT OR REPLACE INTO trade_calendar(cal_date, is_open) VALUES(?, ?)",
                    [(r["cal_date"], int(r["is_open"])) for _, r in df.iterrows()],
                )
            finally:
                conn.close()
        else:
            df.to_csv(path, index=False, encoding="utf-8-sig")

    def _set_cache(self, df: pd.DataFrame) -> None:
        norm = self._normalize_df(df)
        if self.lookback_days > 0 and not norm.empty:
            latest = norm["cal_date"].max()
            try:
                dt = datetime.strptime(latest, "%Y%m%d")
                cutoff = (dt - timedelta(days=self.lookback_days)).strftime("%Y%m%d")
                norm = norm[norm["cal_date"] >= cutoff]
            except ValueError:
                pass
        norm = norm.sort_values("cal_date").drop_duplicates(subset=["cal_date"], keep="last")
        self._cache_df = norm.reset_index(drop=True)
        self._cache_lookup = {row["cal_date"]: bool(row["is_open"]) for _, row in norm.iterrows()}

    def _load_cache(self) -> None:
        for path in self._seed_paths:
            if path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
                df = self._load_cache_sqlite(path)
            else:
                df = self._load_cache_csv(path)

            if not df.empty:
                self._set_cache(df)
                if path != self.cache_path:
                    self._save_cache()
                return

        self._set_cache(pd.DataFrame(columns=["cal_date", "is_open"]))

    def _save_cache(self) -> None:
        if self._cache_df.empty:
            return
        self._persist_cache(self._cache_df)

    # ---- fetch + query -----------------------------------------------------
    def _fetch_remote(self, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
        try:
            df = self.datasource.get_trade_cal(start_date=start_date, end_date=end_date)
            self.last_error = None
            return self._normalize_df(df)
        except Exception as e:  # pragma: no cover - only hit when remote fails
            self.last_error = str(e)
            return pd.DataFrame(columns=["cal_date", "is_open"])

    def _range_for(self, target_digits: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            dt = datetime.strptime(target_digits, "%Y%m%d")
        except ValueError:
            return None, None
        start = (dt - timedelta(days=max(self.lookback_days, 0))).strftime("%Y%m%d") if self.lookback_days else None
        end = (dt + timedelta(days=7)).strftime("%Y%m%d")
        return start, end

    def _ensure_cached(self, target_digits: str) -> None:
        if target_digits in self._cache_lookup:
            return

        start, end = self._range_for(target_digits)
        fetched = self._fetch_remote(start, end)
        if fetched.empty:
            return

        merged = pd.concat([self._cache_df, fetched], ignore_index=True)
        merged = merged.sort_values("cal_date").drop_duplicates(subset=["cal_date"], keep="last")
        self._set_cache(merged)
        self._save_cache()

    # ---- public API --------------------------------------------------------
    def is_trade_day(self, trade_date: str) -> bool:
        digits = _to_digits(trade_date)
        if not digits:
            return False

        self._ensure_cached(digits)

        if digits in self._cache_lookup:
            return bool(self._cache_lookup[digits])

        try:
            dt = datetime.strptime(normalize_trade_date(trade_date, sep="-"), "%Y-%m-%d")
            return dt.weekday() < 5
        except Exception:
            return False

    def gate(self, trade_date: str) -> Tuple[bool, str]:
        """Returns (ok, reason). reason='NOT_TRADE_DAY' when blocked."""
        if not self.is_trade_day(trade_date):
            return False, "NOT_TRADE_DAY"
        return True, ""
