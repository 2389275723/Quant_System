from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, Optional

import pandas as pd

from src.core.config import load_cfg, get
from src.core.paths import resolve_from_cfg
from src.core.env import load_env_from_cfg_path
from src.core.hashing import stable_hash_dict, file_hash
from src.core.timeutil import make_run_id, now_cn
from src.utils.trade_date import normalize_trade_date
from src.storage.sqlite import connect
from src.storage.schema import ensure_schema
from src.storage.upsert import upsert_df
from src.engine.filters import apply_universe_filters
from src.engine.factors import compute_factors
from src.engine.preprocess import preprocess_factors
from src.engine.scoring import compute_rule_scores, apply_vol_damper, rank_scores
from src.engine.regime import detect_regime
from src.engine.monitor import build_factpack
from src.engine.models.dual_head import DualHeadModelEngine
from src.news.market_ctx import build_market_context


def _set_state(conn: sqlite3.Connection, k: str, v: str) -> None:
    conn.execute(
        "INSERT INTO system_state(k, v, updated_at) VALUES(?, ?, datetime('now')) "
        "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
        (k, v),
    )


def run_night_job(cfg_path: str = "config/config.yaml", trade_date: Optional[str] = None) -> Dict[str, Any]:
    cfg = load_cfg(cfg_path)
    # Ensure .env is loaded when running as a job/CLI
    load_env_from_cfg_path(cfg_path, override=True)
    db_path = str(resolve_from_cfg(cfg_path, get(cfg, "paths.db_path")))
    bars_path = str(resolve_from_cfg(cfg_path, get(cfg, "paths.bars_path")))
    exclude_prefixes = get(cfg, "universe.exclude_prefixes", ["300", "301", "688", "689"])

    # --- Compat: support universe.exclude_markets (STAR/GEM) ---
    exclude_markets = get(cfg, "universe.exclude_markets", [])
    if (not exclude_prefixes) and exclude_markets:
        # If config uses exclude_markets instead of exclude_prefixes, convert here.
        _tmp = []
        if "STAR" in exclude_markets:
            _tmp += ["688", "689"]
        if "GEM" in exclude_markets:
            _tmp += ["300", "301"]
        exclude_prefixes = _tmp
    exclude_bj = bool(get(cfg, "universe.exclude_bj", True))
    max_total_mv_yi = get(cfg, "universe.max_total_mv_yi", 0)
    try:
        max_total_mv = float(max_total_mv_yi) * 1e8 if max_total_mv_yi not in (None, "", 0, "0") else None
    except Exception:
        max_total_mv = None
    run_id = make_run_id("NIGHT")
    config_hash = stable_hash_dict(cfg)
    code_hash = file_hash("main.py")

    conn = connect(db_path)
    ensure_schema(conn)

    started_at = now_cn().isoformat(timespec="seconds")
    _set_state(conn, "phase", "NIGHT_JOB")
    _set_state(conn, "last_run_id", run_id)

    conn.execute(
        "INSERT OR REPLACE INTO execution_log(run_id, job, trade_date, status, error_code, error_msg, started_at, finished_at, config_hash, code_hash) "
        "VALUES(?, 'NIGHT', ?, 'RUNNING', NULL, NULL, ?, NULL, ?, ?)",
        (run_id, trade_date or "", started_at, config_hash, code_hash),
    )

    try:
        bars = pd.read_csv(bars_path, dtype={"ts_code": str})
        if "trade_date" in bars.columns:
            bars["trade_date"] = bars["trade_date"].apply(normalize_trade_date)
        # Normalize incoming trade_date param for comparison
        if trade_date is not None:
            trade_date = normalize_trade_date(trade_date)

        if trade_date is None:
            # choose the latest available date in the bars file
            trade_date = str(bars["trade_date"].max())

        snap = bars.loc[bars["trade_date"] == trade_date].copy()
        if snap.empty:
            raise RuntimeError(f"No bars found for trade_date={trade_date} in {bars_path}")

        # 1) Universe hard filter
        universe = apply_universe_filters(snap, exclude_prefixes=exclude_prefixes, exclude_bj=exclude_bj, max_total_mv=max_total_mv)

        # 2) Factor -> preprocess
        fac = compute_factors(universe)
        fac = preprocess_factors(fac, factor_cols=["f_ret1", "f_turnover", "f_amount_log", "f_circ_mv_log"])

        # 3) Rule score
        weights = get(cfg, "scoring.weights", {"trend": 0.5, "flow": 0.3, "fund": 0.2})
        scored = compute_rule_scores(fac, weights=weights)

        # Ensure score columns exist (avoid KeyError when universe is empty or upstream changed)
        if scored is None:
            scored = pd.DataFrame()
        if "final_score" not in scored.columns:
            if "score_rule" in scored.columns:
                scored["final_score"] = scored["score_rule"]
            elif "final_score_ai" in scored.columns:
                scored["final_score"] = scored["final_score_ai"]
            else:
                scored["final_score"] = 0.0
        if "score_rule" not in scored.columns:
            scored["score_rule"] = scored["final_score"]

        # 4) V1.5 optional: regime
        if bool(get(cfg, "v1_5.enable_regime_engine", True)):
            regime = detect_regime(universe)
            scored["final_score"] = scored["final_score"] * float(regime.score_multiplier)
            _set_state(conn, "regime", json.dumps(regime.__dict__, ensure_ascii=False))
        else:
            _set_state(conn, "regime", json.dumps({"name": "DISABLED"}, ensure_ascii=False))

        # 5) V1.5 optional: vol damper
        if bool(get(cfg, "v1_5.enable_vol_damper", True)):
            scored = apply_vol_damper(scored)

        ranked = rank_scores(scored)

        top_m = int(get(cfg, "scoring.top_m", 200))
        picks = ranked.head(top_m).copy()

        # 6) (optional) dual-head model shadow / rerank
        model_cfg = get(cfg, "model", {}) or {}
        # --- Optional: build market/news context once per run (to feed LLM + UI)
        news_enabled = bool(get(cfg, "news.enabled", True))
        market_ctx = (
            build_market_context(trade_date=trade_date, picks_df=picks, raw_cfg=cfg)
            if news_enabled
            else {"trade_date": trade_date}
        )
        try:
            conn.execute(
                "INSERT OR REPLACE INTO market_context_daily(trade_date, run_id, config_hash, ctx_json, created_at) "
                "VALUES(?, ?, ?, ?, datetime('now'))",
                (trade_date, run_id, config_hash, json.dumps(market_ctx, ensure_ascii=False)),
            )
        except Exception:
            # Never fail the job due to context storage
            pass

        model_engine = DualHeadModelEngine(model_cfg)
        try:
            model_out = model_engine.score(picks, trade_date=trade_date, market_context=market_ctx)
        except TypeError:
            # backward compatibility if score() signature differs
            model_out = model_engine.score(picks, trade_date=trade_date)

        # --- Compatibility guard (score column names may differ across patches)
        if "final_score" not in model_out.columns:
            if "score_final" in model_out.columns:
                model_out["final_score"] = pd.to_numeric(model_out["score_final"], errors="coerce")
            elif "score_rule" in model_out.columns:
                model_out["final_score"] = pd.to_numeric(model_out["score_rule"], errors="coerce")
            elif "score_base" in model_out.columns:
                model_out["final_score"] = pd.to_numeric(model_out["score_base"], errors="coerce")
            else:
                model_out["final_score"] = 0.0

        # --- Optional: rerank by AI composite score
        if bool(get(model_cfg, "rerank_enabled", True)):
            w_alpha = float(get(model_cfg, "rerank_weight_alpha", 0.35))
            w_risk = float(get(model_cfg, "rerank_weight_risk", 0.50))
            w_sev = float(get(model_cfg, "rerank_weight_sev", 0.15))
            # base final_score + alpha - risk components
            model_out["final_score_ai"] = (
                model_out["final_score"].astype(float)
                + w_alpha * pd.to_numeric(model_out.get("alpha_final"), errors="coerce").fillna(0.0)
                - w_risk * pd.to_numeric(model_out.get("risk_prob_final"), errors="coerce").fillna(0.0)
                - w_sev * pd.to_numeric(model_out.get("risk_sev_final"), errors="coerce").fillna(0.0)
            )
            model_out = model_out.sort_values("final_score_ai", ascending=False).reset_index(drop=True)
            model_out["rank_ai"] = model_out.index + 1
        else:
            model_out["final_score_ai"] = None
            model_out["rank_ai"] = None

        # Save model outputs (even in shadow mode) for UI consistency
        model_rows = model_out[[
            "ts_code",
            "alpha_ds","risk_prob_ds","risk_sev_ds","conf_ds","comment_ds",
            "alpha_qw","risk_prob_qw","risk_sev_qw","conf_qw","comment_qw",
            "alpha_final","risk_prob_final","risk_sev_final","disagreement","action",
        ]].copy()
        model_rows.insert(0, "trade_date", trade_date)
        model_rows["config_hash"] = config_hash
        model_rows["run_id"] = run_id
        model_rows["created_at"] = now_cn().isoformat(timespec="seconds")
        upsert_df(conn, "model_scores_daily", model_rows, pk_cols=["trade_date","ts_code","config_hash"])

        # 7) Save picks_daily (idempotent)
        # Use AI-reranked order if enabled (rank_ai), else keep rule rank_final.
        base_for_picks = model_out if (isinstance(model_out, pd.DataFrame) and ("rank_ai" in model_out.columns)) else picks

        # ---- Robust column handling (avoid KeyError: 'final_score' / empty picks) ----
        base_for_picks = base_for_picks.copy()

        # Fallback: ensure final_score exists
        if "final_score" not in base_for_picks.columns:
            if "score_rule" in base_for_picks.columns:
                base_for_picks["final_score"] = base_for_picks["score_rule"]
            elif "final_score_ai" in base_for_picks.columns:
                base_for_picks["final_score"] = base_for_picks["final_score_ai"]
            elif set(["trend_score", "fund_score", "flow_score"]).issubset(base_for_picks.columns):
                w_trend = float(weights.get("trend", 0.5))
                w_fund  = float(weights.get("fund", 0.2))
                w_flow  = float(weights.get("flow", 0.3))
                base_for_picks["final_score"] = (
                    base_for_picks["trend_score"] * w_trend
                    + base_for_picks["fund_score"] * w_fund
                    + base_for_picks["flow_score"] * w_flow
                )
            else:
                base_for_picks["final_score"] = 0.0

        # Ensure core columns exist (even when DataFrame is empty)
        _core_cols = ["ts_code", "name", "industry", "score_rule", "trend_score", "fund_score", "flow_score",
                     "final_score", "rank", "rank_rule", "rank_final"]
        for _c in _core_cols:
            if _c not in base_for_picks.columns:
                base_for_picks[_c] = None

        picks_out = base_for_picks[[
            "ts_code","name","industry",
            "score_rule","trend_score","fund_score","flow_score",
            "final_score","rank","rank_rule","rank_final"
        ]].copy()

        # Optional model columns
        if "final_score_ai" in base_for_picks.columns:
            picks_out["final_score_ai"] = base_for_picks["final_score_ai"]
        if "rank_ai" in base_for_picks.columns:
            picks_out["rank_ai"] = base_for_picks["rank_ai"]

        # If rank_ai exists and has values, use it as final rank for display/trading
        if "rank_ai" in picks_out.columns and picks_out["rank_ai"].notna().any():
            picks_out["rank_final"] = picks_out["rank_ai"]

        picks_out.insert(0, "trade_date", trade_date)
        picks_out["config_hash"] = config_hash
        picks_out["run_id"] = run_id
        picks_out["created_at"] = now_cn().isoformat(timespec="seconds")
        upsert_df(conn, "picks_daily", picks_out, pk_cols=["trade_date","ts_code","config_hash"])

        # 7.1) Export a convenient CSV for manual inspection (optional but helpful)
        # Users often expect a visible picks file; UI has a download button, but this
        # keeps a deterministic local artifact as well.
        try:
            run_dir = resolve_from_cfg(cfg_path, f"research/runs/{run_id}")
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "picks_daily.csv").write_text(
                picks_out.to_csv(index=False, encoding="utf-8-sig"),
                encoding="utf-8-sig",
            )

            latest_path = resolve_from_cfg(cfg_path, "research/picks_daily.csv")
            latest_path.parent.mkdir(parents=True, exist_ok=True)
            latest_path.write_text(
                picks_out.to_csv(index=False, encoding="utf-8-sig"),
                encoding="utf-8-sig",
            )
        except Exception:
            # Never fail the job due to CSV export
            pass

        # 8) Build factpack (for UI)
        build_factpack(conn, trade_date, config_hash)

        finished_at = now_cn().isoformat(timespec="seconds")
        conn.execute(
            "UPDATE execution_log SET status='OK', trade_date=?, finished_at=? WHERE run_id=?",
            (trade_date, finished_at, run_id),
        )
        _set_state(conn, "phase", "IDLE")
        _set_state(conn, "last_night_ok", finished_at)
        return {"ok": True, "run_id": run_id, "trade_date": trade_date, "config_hash": config_hash}

    except Exception as e:
        finished_at = now_cn().isoformat(timespec="seconds")
        conn.execute(
            "UPDATE execution_log SET status='FAILED', error_code='EXCEPTION', error_msg=?, trade_date=?, finished_at=? WHERE run_id=?",
            (str(e), trade_date or "", finished_at, run_id),
        )
        _set_state(conn, "phase", "IDLE")
        _set_state(conn, "last_error", str(e))
        return {"ok": False, "run_id": run_id, "trade_date": trade_date, "error": str(e)}
    finally:
        conn.close()
