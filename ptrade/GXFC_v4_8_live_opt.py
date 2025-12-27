# -*- coding: utf-8 -*-
"""
策略名：恭喜发财 v4.8  · Warmup + Slice + AutoClean（No-OS / PTrade 可直接回测）
说明：
- 不使用 os；回测启动时可自动清空 CSV（AUTO_CLEAN_REPORTS）。
- “预热 + 切片评估”为工具层，不改选股/交易逻辑；不开开关时等价原版。
"""

import csv
from datetime import date, datetime

# ========================= 参数区 =========================
class CFG:
    # ------- 指数/基础设置（保持你的逻辑/风格）-------
    IDX300 = ["000300.SS", "399300.SZ"]
    IDX500 = ["000905.SS", "399905.SZ"]

    BASE_MAX_HOLD_BULL     = 5
    BASE_MAX_HOLD_NEUTRAL  = 4
    BASE_MAX_HOLD_BEAR     = 3
    BASE_PER_STOCK_BULL    = 0.18
    BASE_PER_STOCK_NEUTRAL = 0.14
    BASE_PER_STOCK_BEAR    = 0.12

    # --- 熊市火种模式（弱市只留 1 个火种，防止反复失血）---
    BEAR_SEED_ENABLED      = True
    BEAR_SEED_MAX_HOLD     = 1       # 用户选择：熊市最多 1 只
    BEAR_SEED_PER_STOCK    = 0.06    # 用户选择：单票 6%
    BEAR_SEED_STRICT_ONLY  = True    # 熊市只用严格候选（不扩展 soft）
    BEAR_SEED_FORCE_TRIM   = True    # 熊市若持仓>1，强制只保留“最好”的 1 只
    BEAR_SEED_MIN_SCORE    = 0.10    # 熊市火种最低分（0=不限制）
    BEAR_SEED_ALLOW_SWAP    = True    # 熊市允许“换火种”（卖旧买新，不增加持仓数）
    BEAR_SEED_SWAP_DELTA    = 0.05    # 新候选分数至少高出多少才触发换仓
    BEAR_SEED_SWAP_MIN_HOLD = 2       # 火种最少持有天数（避免当天频繁换）
    BEAR_SEED_SWAP_COOLDOWN = 3       # 两次换仓最小间隔天数


    MA_RANGE_THRESHOLD      = 0.08
    PRICE_MA_DIST           = 0.05
    SOFT_MA_RANGE_THRESHOLD = 0.12
    SOFT_PRICE_MA_DIST      = 0.08

    RSI_N = 6
    RSI_WINDOW = 10
    RSI_LOW = 38.0
    RSI_HIGH = 85.0

    TREND_LOOKBACK = 20
    TREND_MIN_RET  = 0.05
    TREND_MAX_RET  = 0.60
    NEAR_HIGH_LOOKBK = 20
    NEAR_HIGH_PCT    = 0.88
    SOFT_TREND_MIN_RET = 0.03
    SOFT_NEAR_HIGH_PCT = 0.88

    SURGE_DAYS = 5
    SURGE_MIN_AMP = 0.03
    SURGE_MIN_POS = 0.60
    CAND_MULTIPLIER = 4
    MIN_STRICT_CANDS = 5

    MIN_HOLD_DAYS        = 3
    BASE_MAX_HOLD_DAYS   = 12
    EXTEND_MAX_HOLD_DAYS = 20
    TIMEOUT_RET_OK       = 0.10

    LOSS_STOP = -0.08
    LOSS_STOP_AFTER_ADD = -0.10
    TRAIL_START_PROFIT  = 0.15
    TRAIL_MAX_DD        = -0.05

    # --- 出场参数“快锁利润”实验位（建议用来提升胜率/减少磨损）---
    # base：用原参数；fast：更早止盈、更早止损（适合隔日冲/短波段）
    EXIT_TUNE_MODE   = "base"   # "base" or "fast"
    FAST_LOSS_STOP   = -0.06
    FAST_TRAIL_START = 0.10
    FAST_TRAIL_DD    = -0.03


    STUCK_DAYS          = 8
    STUCK_RET_FLOOR     = 0.015

    ENABLE_ADD         = True
    ADD_LOSS_LOW       = -0.06
    ADD_LOSS_HIGH      = -0.03
    ADD_MAX_DAYS       = 3
    ADD_POS_MULTIPLIER = 1.5

    DD_YELLOW = 0.10
    DD_RED    = 0.20

    # —— 广度闸门（两日确认）& 弱势观察 ——
    BREADTH_BLOCK_LEVEL   = 0.30
    BREADTH_BLOCK_CONFIRM = 1
    WEAK_OBSERVE_DAYS     = 3
    WEAK_EXIT_RSI_FLOOR   = 40.0
    WEAK_EXIT_5D_BREAK    = True

    # ------- 基本面过滤（可选） -------
    ENABLE_FUND_FILTER      = True   # 是否启用基本面过滤
    FUND_MIN_FLOAT_MKT_CAP  = 3e9    # 最小流通市值 / 元，0 为不限
    FUND_MAX_PE             = 80.0   # 静态市盈率上限，0 为不限
    FUND_MAX_PB             = 8.0    # PB 上限，0 为不限
    FUND_REQUIRE_POS_NET_PROFIT   = True   # 要求净利润为正
    FUND_REQUIRE_POS_REV_GROWTH   = True   # 要求营收增速为正
    FUND_MIN_REV_GROWTH           = 0.0    # 最小营收增速（同口径），0 表示>0 即可

    # 数据缺失策略：接口临时取不到估值/财务时，是否允许放行（推荐 True，避免全池误过滤为空）
    FUND_SKIP_IF_DATA_MISSING      = True
    # 最小营收增速（同口径），0 表示>0 即可

    # ------- 报表/输出（No-OS） -------
    REPORT_DIR="A1_warmoff"
    POS_CSV    = "positions.csv"
    EQUITY_CSV = "equity.csv"
    SLICE_CSV  = "equity_slice.csv"

    # 开始回测时自动清空三张 CSV（不依赖 os）
    AUTO_CLEAN_REPORTS = False
    # ================= Warmup + Slice（仅工具层） =================
    # 方式A：用日期锚点（推荐）；留空字符串则不用
    WARMUP_FROM  = ""      # 实盘建议留空
    MEASURE_FROM = ""      # 实盘建议留空
    WARMUP_TRADE  = "off"            # "off"=预热不交易；"build"=预热允许建仓

    # 方式B：统计“最后N个交易日”的切片（仅用于标记输出）
    SLICE_LAST_N  = 0                # 实盘建议=0

    # ================= 实盘节奏：尾盘买 + 次日冲高卖 =================
    # 注：PTrade 日线级别交易默认在 14:50 运行一次 handle_data（券商可配置），这里再加一层保护：
    LIVE_ENABLE_TIME_GATE = True
    LIVE_BUY_AFTER_HHMM   = 1450      # 14:50 之后才允许执行“开仓/换仓/加仓”
    ENABLE_MORNING_SELL   = True
    MORNING_SELL_TIME     = "09:40"   # run_daily 定时执行
    MORNING_TAKEPROFIT_RET = 0.03     # 次日早盘冲高 >=3% 先兑现
    MORNING_SELL_MAX_HOLD_DAYS = 2      # 只卖“次日~两日内”的冲高
    MORNING_STOPLOSS_RET   = -0.06    # 次日早盘若直接走弱，-6% 先砍（兜底）

    # ================= 实盘资金获取（可选） =================
    # 注意：普通账户调用两融/信用接口可能会在后端报“无此功能”，即使 try/except 也会产生 ERROR 日志；
    # 因此默认关闭。若你确实在两融模块/信用账户交易，再手动改成 True。
    LIVE_USE_CREDIT_CASH_API = False   # True 才尝试 get_margin_asset / get_crdt_fund 获取“可用资金”
    # 次日早盘若直接走弱，-6% 先砍（兜底）

# ========================= 小工具 =========================
def _log(m):
    try: log.info(str(m))
    except: print(str(m))

def _to_list(x, keep=None):
    try:
        li = list(x)
    except:
        try: li = list(x.values())
        except: li = [x]
    if keep and len(li)>keep: li = li[-keep:]
    return li

def _hist_list(n, period, field, code):
    try:
        df = get_history(n, period, field, code, fq=None, include=False)
        return _to_list(df[field], keep=n)
    except:
        try:
            h = history(n, field, security=[code])
            return _to_list(h.tolist() if hasattr(h,"tolist") else h, keep=n)
        except:
            return []

def _get_hist(code, n):
    return {
        "close": _hist_list(n,"1d","close",code),
        "high":  _hist_list(n,"1d","high",code),
        "low":   _hist_list(n,"1d","low",code),
    }

def _get_hist_close(code, n): return _hist_list(n,"1d","close",code)

def _safe_positions(ctx=None):
    """兼容回测/交易：优先 get_positions()，否则从 ctx.portfolio.positions 兜底。"""
    try:
        return get_positions()
    except:
        try:
            c = ctx if ctx is not None else globals().get("context", None)
            if c is None: 
                return {}
            return dict(getattr(c.portfolio, "positions", {}) or {})
        except:
            return {}

def _portfolio_value(context, price_map=None):
    try: return float(context.portfolio.portfolio_value)
    except: pass
    cash=0.0; pv=0.0
    try: cash=float(context.portfolio.cash)
    except: pass
    try:
        for c,p in (_safe_positions() or {}).items():
            amt=int(getattr(p,"total_amount",0) or getattr(p,"amount",0))
            if amt<=0: continue
            px = price_map.get(c) if price_map else None
            if px is None:
                cl=_get_hist_close(c,1); px=float(cl[-1]) if cl else 0.0
            pv += max(0.0, amt*float(px))
    except: pass
    return cash+pv

# == 实盘可用资金（尽量取到柜台的“可用”金额） ==
def _available_cash(context):
    """实盘可用资金（尽量取到柜台的“可用”金额）

    - 普通账户：直接用 context.portfolio.cash（或平台提供的可用字段）。
    - 两融/信用账户：可选调用 get_margin_asset / get_crdt_fund。
      但在普通账户下，这两类接口可能在后端直接报“无此功能”，即使 try/except 也会产生 ERROR 日志，
      所以必须由 CFG.LIVE_USE_CREDIT_CASH_API 显式打开才会尝试。
    """
    # 先尝试平台可能提供的“可用资金”字段
    for k in ("available_cash", "available", "enable_cash", "enable_balance", "cash"):
        try:
            v = getattr(context.portfolio, k, None)
            if v is not None:
                v = float(v)
                if v >= 0:
                    return v
        except:
            pass

    # 两融/信用账户（可选）
    if _is_trade_env() and bool(getattr(CFG, "LIVE_USE_CREDIT_CASH_API", False)):
        try:
            if "get_margin_asset" in globals():
                ma = get_margin_asset()
                if isinstance(ma, dict):
                    for k in ("assure_enbuy_balance","fin_enrepaid_balance","enable_balance","v_enable_balance","v_enbuy_balance","cash"):
                        if k in ma and ma.get(k) is not None:
                            return float(ma.get(k))
        except:
            pass
        try:
            if "get_crdt_fund" in globals():
                cf = get_crdt_fund()
                if isinstance(cf, dict):
                    for k in ("enable_balance","v_enable_balance","assure_enbuy_balance","fin_enrepaid_balance","cash"):
                        if k in cf and cf.get(k) is not None:
                            return float(cf.get(k))
        except:
            pass

    # 普通账户兜底
    try:
        return float(getattr(context.portfolio, "cash", 0.0))
    except:
        return 0.0


def _report_paths():
    base = get_research_path()
    try: create_dir(base + CFG.REPORT_DIR + "/")
    except: pass
    root = base + CFG.REPORT_DIR + "/"
    return (root + CFG.POS_CSV, root + CFG.EQUITY_CSV, root + CFG.SLICE_CSV)

def _append_csv(path, header, row):
    try:
        need_header=True
        try:
            with open(path,"r") as f:
                if f.read(1): need_header=False
        except: need_header=True
        with open(path,"a",newline="") as f:
            w=csv.writer(f)
            if need_header and header: w.writerow(header)
            if row: w.writerow(row)
    except Exception as e:
        _log("CSV写入失败: %s" % e)

def _to_date(s):
    if not s: return None
    try:
        y,m,d = s.split("-"); return date(int(y), int(m), int(d))
    except: return None

# 基本面日期格式化：兼容 'YYYY-MM-DD' / 'YYYY-MM-DD 00:00:00' / 'YYYYMMDD'
def _fmt_yyyymmdd(d):
    if not d: return None
    try:
        if isinstance(d, (datetime, date)):
            return d.strftime('%Y%m%d')
    except:
        pass
    s = str(d).strip()
    if not s: return None
    # 去掉时间部分
    try:
        s = s.split(' ')[0]
    except:
        pass
    # YYYY-MM-DD -> YYYYMMDD
    if '-' in s:
        try:
            ps = s.split('-')
            if len(ps) == 3:
                return '%04d%02d%02d' % (int(ps[0]), int(ps[1]), int(ps[2]))
        except:
            pass
    return s


# ========================= 股票池/指数 =========================
def _get_index_members_try(codes):
    res=[]; seen=set()
    for idx in codes:
        try:
            for s in list(get_index_stocks(idx)):
                if s and s not in seen:
                    seen.add(s); res.append(s)
        except: continue
    return res

def _is_main_board(code):
    c=str(code)
    if c.endswith(".BJ"): return False
    core=c.split(".")[0]
    if core.startswith(("300","301","688","689")): return False
    return True

def _refresh_universe():
    c500=_get_index_members_try(CFG.IDX500)
    c300=set(_get_index_members_try(CFG.IDX300))
    base=[]
    for s in c500:
        if s in c300: continue
        if not _is_main_board(s): continue
        base.append(s)
    if len(base)<80:
        uni=list(c500)+list(c300); seen=set(); base=[]
        for s in uni:
            if s and s not in seen and _is_main_board(s):
                seen.add(s); base.append(s)
    return base

# ========================= 指标/打分 =========================

def _fundamental_filter(universe, pre_date_str=None):
    """基本面 / 估值过滤（稳健版）

    目标：在技术面打分前，尽量剔除明显财务差/估值极端的标的；
    但当数据接口临时不可用/返回缺失时，不应该把全池过滤成空。

    参数
    - universe: 股票列表
    - pre_date_str: 上一交易日日期，支持 'YYYYMMDD' / 'YYYY-MM-DD' / 'YYYY-MM-DD 00:00:00'
    """
    if not getattr(CFG, "ENABLE_FUND_FILTER", False):
        return list(universe)

    if not universe:
        return []

    codes = list(universe)

    # 获取基准日期（优先使用上一交易日；并格式化为 YYYYMMDD）
    d = _fmt_yyyymmdd(pre_date_str)
    if not d:
        try:
            d = _fmt_yyyymmdd(get_trading_day(-1))
        except Exception as e:
            _log("[FUND] get_trading_day 失败，跳过基本面过滤: %s" % e)
            return codes

    # 拉取估值 + 财务（批量）
    val_dict = {}
    fund_dict = {}
    try:
        val_dict = get_valuation_new_info(1, d, codes)
    except Exception as e:
        val_dict = {}
        _log("[FUND] get_valuation_new_info 失败: %s" % e)
    try:
        fund_dict = get_fundamentals_daily_info(1, d, codes)
    except Exception as e:
        fund_dict = {}
        _log("[FUND] get_fundamentals_daily_info 失败: %s" % e)

    # 数据都不可用：直接跳过（不要把股票“误杀”掉）
    has_any_data = (isinstance(val_dict, dict) and len(val_dict) > 0) or (isinstance(fund_dict, dict) and len(fund_dict) > 0)
    if not has_any_data:
        _log("[FUND] 估值/财务数据不可用，跳过基本面过滤")
        return codes

    min_float = float(getattr(CFG, "FUND_MIN_FLOAT_MKT_CAP", 0.0) or 0.0)
    max_pe    = float(getattr(CFG, "FUND_MAX_PE", 0.0) or 0.0)
    max_pb    = float(getattr(CFG, "FUND_MAX_PB", 0.0) or 0.0)
    need_np   = bool(getattr(CFG, "FUND_REQUIRE_POS_NET_PROFIT", False))
    need_g    = bool(getattr(CFG, "FUND_REQUIRE_POS_REV_GROWTH", False))
    min_g     = float(getattr(CFG, "FUND_MIN_REV_GROWTH", 0.0) or 0.0)

    # 数据缺失时是否“放行”（推荐 True，避免接口波动导致全空）
    skip_missing = bool(getattr(CFG, "FUND_SKIP_IF_DATA_MISSING", True))

    def _last_row(obj):
        if obj is None:
            return None
        try:
            return obj.iloc[-1]
        except:
            pass
        try:
            return list(obj)[-1]
        except:
            pass
        return None

    ok = []
    for code in codes:
        keep = True

        # ---------- 估值类 ----------
        df_val = None
        if isinstance(val_dict, dict):
            try:
                df_val = val_dict.get(code)
            except:
                df_val = None

        float_mv = None
        pb = None
        pe = None
        got_val = False
        row = _last_row(df_val)
        if row is not None:
            got_val = True
            try:
                float_mv = float(row.get("float_value", row.get("total_value", 0.0)) or 0.0)
            except:
                float_mv = None
            try:
                pb = float(row.get("pb", 0.0) or 0.0)
            except:
                pb = None
            try:
                pe = float(row.get("pe_static", row.get("pe_dynamic", 0.0)) or 0.0)
            except:
                pe = None

        if got_val:
            if min_float and (float_mv is None or float_mv < min_float):
                keep = False
            if max_pb and (pb is None or pb <= 0 or pb > max_pb):
                keep = False
            if max_pe and (pe is None or pe <= 0 or pe > max_pe):
                keep = False
        else:
            # 没取到估值数据：默认放行（skip_missing=True）；否则按“无法验证”处理为剔除
            if not skip_missing and (min_float or max_pb or max_pe):
                keep = False

        # ---------- 财务类 ----------
        df_f = None
        if isinstance(fund_dict, dict):
            try:
                df_f = fund_dict.get(code)
            except:
                df_f = None

        net_profit = None
        rev_grow = None
        got_fin = False
        rowf = _last_row(df_f)
        if rowf is not None:
            got_fin = True
            try:
                net_profit = float(rowf.get("net_profit", rowf.get("net_profit_cut", 0.0)) or 0.0)
            except:
                net_profit = None
            try:
                rev_grow = float(rowf.get("operating_revenue_grow_rate", 0.0) or 0.0)
            except:
                rev_grow = None

        if need_np:
            if got_fin:
                if net_profit is None or net_profit <= 0:
                    keep = False
            else:
                if not skip_missing:
                    keep = False

        if need_g:
            if got_fin:
                if rev_grow is None or rev_grow <= min_g:
                    keep = False
            else:
                if not skip_missing:
                    keep = False

        if keep:
            ok.append(code)

    if ok:
        _log("[FUND] 基本面过滤：%d -> %d (date=%s)" % (len(codes), len(ok), str(d)))
        return ok
    else:
        # 过滤结果为空：宁可退回原池，避免因数据/阈值问题导致“无股可买”
        _log("[FUND] 基本面过滤结果为空，退回原始 universe")
        return codes


def _rsi_series_from_close(cl, n=CFG.RSI_N):
    cl=[float(x) for x in cl]
    if len(cl)<=n: return []
    gains=[]; losses=[]
    for i in range(1,len(cl)):
        d=cl[i]-cl[i-1]
        gains.append(max(0.0,d)); losses.append(max(0.0,-d))
    out=[]
    for i in range(n-1,len(gains)):
        g=sum(gains[i-n+1:i+1])/float(n)
        l=sum(losses[i-n+1:i+1])/float(n)
        rs = 999999.0 if l==0 else g/l
        out.append(100.0-100.0/(1.0+rs))
    return out

def _atr(code, n=14):
    hi=_hist_list(n+1,"1d","high",code)
    lo=_hist_list(n+1,"1d","low",code)
    cl=_hist_list(n+1,"1d","close",code)
    if min(len(hi),len(lo),len(cl))<n+1: return 0.02
    trs=[]
    for i in range(1,len(cl)):
        h,l,c1=float(hi[i]),float(lo[i]),float(cl[i-1])
        trs.append(max(h-l, abs(h-c1), abs(l-c1)))
    return sum(trs[-n:])/float(n)/max(1e-6, float(cl[-1]))

def _trend_ok(cl, min_ret=None, max_ret=None, lookback=None):
    cl=[float(x) for x in cl]
    n=lookback or CFG.TREND_LOOKBACK
    if len(cl)<=n: return False
    p0,p1=cl[-n-1],cl[-1]
    if p0<=0: return False
    r=p1/p0-1.0
    lo=CFG.TREND_MIN_RET if min_ret is None else min_ret
    hi=CFG.TREND_MAX_RET if max_ret is None else max_ret
    return lo<=r<=hi

def _near_20d_high(cl, lookback=None, pct=None):
    cl=[float(x) for x in cl]
    n=lookback or CFG.NEAR_HIGH_LOOKBK
    recent=cl[-n:] if len(cl)>=n else cl
    if len(recent)<n: return False
    hi=max(recent); px=recent[-1]
    ratio = CFG.NEAR_HIGH_PCT if pct is None else pct
    return px >= hi*ratio

def _recent_ret(cl, n):
    cl=[float(x) for x in cl]
    if len(cl)<n+1: return 0.0
    p0,p1=cl[-n-1],cl[-1]
    if p0<=0: return 0.0
    return p1/p0-1.0

def _intraday_surge_score(code):
    n=CFG.SURGE_DAYS+2
    cl=_hist_list(n,"1d","close",code)
    hi=_hist_list(n,"1d","high", code)
    lo=_hist_list(n,"1d","low",  code)
    if min(len(cl),len(hi),len(lo))<CFG.SURGE_DAYS: return 0.0
    s=0.0
    for i in range(-CFG.SURGE_DAYS,0):
        c,h,l=float(cl[i]),float(hi[i]),float(lo[i])
        if c<=0 or h<=l: continue
        amp=(h-l)/c; pos=(c-l)/(h-l)
        if amp>=CFG.SURGE_MIN_AMP and pos>=CFG.SURGE_MIN_POS: s+=amp*pos
    return s

def _ma5_sideway_ok(cl, rng_thr=None, dist_thr=None):
    cl=[float(x) for x in cl]
    if len(cl)<10: return False
    ma5=[]
    for i in range(4,len(cl)):
        win=cl[i-4:i+1]; ma5.append(sum(win)/5.0)
    if len(ma5)<5: return False
    recent=ma5[-5:]; mid=sum(recent)/5.0
    if mid<=0: return False
    rng=max(recent)-min(recent)
    thr=CFG.MA_RANGE_THRESHOLD if rng_thr is None else rng_thr
    if rng/mid>thr: return False
    last_close=cl[-1]; last_ma5=recent[-1]
    dist=CFG.PRICE_MA_DIST if dist_thr is None else dist_thr
    return abs(last_close-last_ma5)/last_ma5 <= dist

def _index_close(idx, n=120):
    try:
        df=get_history(n,"1d","close",idx,fq=None,include=False)
        return _to_list(df["close"],keep=n)
    except: return []

def _market_state():
    for idx in CFG.IDX300:
        cl=_index_close(idx,120)
        if len(cl)<120: continue
        ma120=sum(cl[-120:])/120.0
        last=float(cl[-1]); base=float(cl[-60])
        pct60=(last-base)/base*100.0
        if last>ma120 and pct60>5:  return "bull"
        if last<ma120 and pct60<-5: return "bear"
        return "neutral"
    return "neutral"

def _simple_breadth(sample_codes):
    ok=0; tot=0
    for c in sample_codes[:80]:
        cl=_get_hist_close(c,25)
        if len(cl)<21: continue
        ma20=sum(cl[-20:])/20.0
        ok += 1 if float(cl[-1])>ma20 else 0
        tot+=1
    return 0.0 if tot==0 else ok/float(tot)

def _adjust_by_state_and_dd(account_dd):
    state=_market_state()

    # 出场参数：base / fast
    if getattr(CFG, "EXIT_TUNE_MODE", "base") == "fast":
        base_loss  = max(CFG.LOSS_STOP, CFG.FAST_LOSS_STOP)              # -0.06 > -0.08
        base_trail_start = min(CFG.TRAIL_START_PROFIT, CFG.FAST_TRAIL_START)  # 0.10 < 0.15
        base_trail_dd    = max(CFG.TRAIL_MAX_DD, CFG.FAST_TRAIL_DD)      # -0.03 > -0.05
    else:
        base_loss  = CFG.LOSS_STOP
        base_trail_start = CFG.TRAIL_START_PROFIT
        base_trail_dd    = CFG.TRAIL_MAX_DD

    if state=="bull":
        max_hold=CFG.BASE_MAX_HOLD_BULL
        per_stock=CFG.BASE_PER_STOCK_BULL
        loss_stop=base_loss
        trail_start=base_trail_start
        trail_dd=base_trail_dd
        enable_add=CFG.ENABLE_ADD
        stuck_on=False
    elif state=="neutral":
        max_hold=CFG.BASE_MAX_HOLD_NEUTRAL
        per_stock=CFG.BASE_PER_STOCK_NEUTRAL
        loss_stop=base_loss
        trail_start=base_trail_start
        trail_dd=base_trail_dd
        enable_add=CFG.ENABLE_ADD
        stuck_on=True
    else:
        # 熊市：默认更保守；若启用“火种模式”，只留 1 只、单票 6%
        if getattr(CFG, "BEAR_SEED_ENABLED", False):
            max_hold=getattr(CFG, "BEAR_SEED_MAX_HOLD", 1)
            per_stock=getattr(CFG, "BEAR_SEED_PER_STOCK", 0.06)
        else:
            max_hold=CFG.BASE_MAX_HOLD_BEAR
            per_stock=CFG.BASE_PER_STOCK_BEAR

        loss_stop=max(base_loss, -0.06)          # 熊市止损更紧
        trail_start=min(base_trail_start, 0.12)  # 熊市更早启动跟踪
        trail_dd=max(base_trail_dd, -0.04)       # 熊市回撤触发更敏感
        enable_add=False
        stuck_on=True

    # 按账户回撤再压一档（黄/红）
    if CFG.DD_YELLOW < account_dd <= CFG.DD_RED:
        max_hold=max(1, int(round(max_hold*0.6)))
        per_stock*=0.6; enable_add=False
    if account_dd > CFG.DD_RED:
        max_hold=max(1, int(round(max_hold*0.3)))
        per_stock*=0.5
        loss_stop=max(loss_stop,-0.06)
        trail_start=min(trail_start,0.10)

    return state, max_hold, per_stock, loss_stop, trail_start, trail_dd, enable_add, stuck_on

def _index_20d_ret():
    for idx in CFG.IDX300:
        cl=_index_close(idx,25)
        if len(cl)>=21: return _recent_ret(cl,20)
    return 0.0

def _build_candidate_scores(universe, strict=True):
    idx20=_index_20d_ret()
    out=[]
    for code in universe:
        cl=_get_hist_close(code,60)
        if len(cl)<30: continue
        if strict:
            if not (_trend_ok(cl) and _near_20d_high(cl) and _ma5_sideway_ok(cl)): continue
        else:
            if not _trend_ok(cl, min_ret=CFG.SOFT_TREND_MIN_RET): continue
            if not _near_20d_high(cl, pct=CFG.SOFT_NEAR_HIGH_PCT): continue
            if not _ma5_sideway_ok(cl, rng_thr=CFG.SOFT_MA_RANGE_THRESHOLD, dist_thr=CFG.SOFT_PRICE_MA_DIST): continue

        r10=_recent_ret(cl,10)
        if r10>0.25: continue
        r20=_recent_ret(cl,20) - idx20
        if r20<0.02: continue

        rsi=_rsi_series_from_close(cl, n=CFG.RSI_N)
        if not rsi or max(rsi[-CFG.RSI_WINDOW:]) < CFG.RSI_LOW: continue
        if not (50.0 <= rsi[-1] <= CFG.RSI_HIGH): continue

        surge=_intraday_surge_score(code)
        score = r10 + 0.5*surge + 0.5*r20
        out.append((code, score))
    return out

# ========================= Warmup + Slice 辅助 =========================
def _today(context):
    try: return context.current_dt.date()
    except: return date.today()


def _hhmm(context):
    try:
        return int(context.current_dt.strftime("%H%M"))
    except:
        try:
            return int(datetime.now().strftime("%H%M"))
        except:
            return 0

def _is_trade_env():
    try:
        return bool(is_trade())
    except:
        return False

def _sync_state_from_positions(ctx=None):
    """重启/断线后恢复：用实际持仓的 avg_cost 补齐 entry/highest 等状态。"""
    try:
        pos = _safe_positions(ctx)
    except:
        pos = {}
    holds = list(pos.keys()) if pos else []
    # 补齐/恢复
    for c in holds:
        p = pos.get(c)
        avg = 0.0
        try:
            avg = float(getattr(p, "avg_cost", 0.0) or getattr(p, "cost_price", 0.0) or 0.0)
        except:
            avg = 0.0
        if avg <= 0:
            avg = float(g.entry_price.get(c, 0.0) or 0.0)
        if avg > 0 and (c not in g.entry_price or float(g.entry_price.get(c, 0.0) or 0.0) <= 0):
            g.entry_price[c] = avg
        if c not in g.hold_days:
            g.hold_days[c] = 0
        if c not in g.highest_close or float(g.highest_close.get(c, 0.0) or 0.0) <= 0:
            # 用昨收或成本价初始化
            last = _get_hist_close(c, 1)
            px = float(last[-1]) if last else avg
            g.highest_close[c] = max(avg, px)
        if c not in g.added_once:
            g.added_once[c] = False
        if c not in g.weak_tag_days:
            g.weak_tag_days[c] = 0

    # 清理：已经不在持仓里的
    holds_set = set(holds)
    for d in (g.entry_price, g.hold_days, g.highest_close, g.added_once, g.weak_tag_days):
        for k in list(d.keys()):
            if k not in holds_set:
                try:
                    d.pop(k, None)
                except:
                    pass

def _bump_hold_days_once(today, ctx=None):
    """每天盘前把持仓天数 +1（为了让 09:40 的“次日卖”成立）。"""
    if getattr(g, "_holddays_bumped_date", None) == today:
        return
    try:
        pos = _safe_positions(ctx)
        holds = list(pos.keys()) if pos else []
        for c in holds:
            g.hold_days[c] = int(g.hold_days.get(c, 0)) + 1
    except:
        pass
    g._holddays_bumped_date = today

def _live_price_map(codes, end_dt=None):
    """实盘取当前价格（尽量批量），失败再逐个兜底。"""
    pm = {}
    if not codes:
        return pm
    try:
        df = get_price(codes, end_date=end_dt, frequency="1m", count=1, fields=["close"])
        # 兼容多种返回结构
        try:
            # 情况1：df['close'] 是 DataFrame，列=code
            close_df = df["close"]
            for c in codes:
                try:
                    v = close_df[c].iloc[-1]
                    pm[c] = float(v)
                except:
                    pass
        except:
            try:
                # 情况2：df 本身是 DataFrame，含 'code','close'
                if hasattr(df, "columns") and ("code" in df.columns) and ("close" in df.columns):
                    for _, row in df.iterrows():
                        pm[str(row["code"])] = float(row["close"])
            except:
                pass
    except:
        pass

    # 逐个兜底
    for c in codes:
        if c in pm and pm[c] > 0:
            continue
        try:
            dfi = get_price(c, end_date=end_dt, frequency="1m", count=1, fields=["close"])
            try:
                pm[c] = float(dfi["close"].iloc[-1])
            except:
                try:
                    pm[c] = float(list(dfi["close"])[-1])
                except:
                    pass
        except:
            pass
    return pm

def _morning_sell_task(context):
    """次日早盘冲高兑现：09:40 运行，只做卖，不开新仓。"""
    if not getattr(CFG, "ENABLE_MORNING_SELL", True):
        return
    if not _is_trade_env():
        return

    today = _today(context)
    # 盘前已 bump 一次；这里再同步一次，防止中途重启
    _sync_state_from_positions(context)

    pos = _safe_positions(context)
    holds = list(pos.keys()) if pos else []
    if not holds:
        return

    pm = _live_price_map(holds, end_dt=getattr(context, "current_dt", None))
    tp = float(getattr(CFG, "MORNING_TAKEPROFIT_RET", 0.03) or 0.03)
    sl = float(getattr(CFG, "MORNING_STOPLOSS_RET", -0.06) or -0.06)
    max_days = int(getattr(CFG, "MORNING_SELL_MAX_HOLD_DAYS", 2) or 2)

    for c in holds:
        price = float(pm.get(c, 0.0) or 0.0)
        if price <= 0:
            continue
        p = pos.get(c)
        entry = float(g.entry_price.get(c, 0.0) or getattr(p, "avg_cost", 0.0) or price)
        if entry <= 0:
            continue

        days = int(g.hold_days.get(c, 0))
        ret = price / entry - 1.0
        highest = max(float(g.highest_close.get(c, entry) or entry), price)
        g.highest_close[c] = highest

        # 只针对“次日~两日内”做冲高兑现（更符合隔日冲）
        if 1 <= days <= max_days and ret >= tp:
            try:
                order_target_value(c, 0.0)
            except:
                pass
            for d in (g.entry_price, g.hold_days, g.highest_close, g.added_once, g.weak_tag_days):
                try:
                    d.pop(c, None)
                except:
                    pass
            _log("[MSELL] %s | ret=%.2f%% >= %.2f%%" % (c, ret*100.0, tp*100.0))
            continue

        # 兜底：若次日直接走弱，也允许早盘砍掉（避免等到 14:50）
        if 1 <= days <= max_days and ret <= sl:
            try:
                order_target_value(c, 0.0)
            except:
                pass
            for d in (g.entry_price, g.hold_days, g.highest_close, g.added_once, g.weak_tag_days):
                try:
                    d.pop(c, None)
                except:
                    pass
            _log("[MSELL] %s | ret=%.2f%% <= %.2f%%" % (c, ret*100.0, sl*100.0))
            continue

def _write_equity_and_slice(today, total_value, account_dd, measuring):
    _append_csv(g.eq_csv,
        ["date","portfolio_value","drawdown","market_state"],
        [today.strftime("%Y-%m-%d"), round(total_value,2), round(account_dd,4), g.state_cache or "NA"])

    slice_ret = 0.0
    if measuring and g.slice_base_value and g.slice_base_value>0:
        slice_ret = total_value/g.slice_base_value - 1.0
    _append_csv(g.slice_csv,
        ["date","portfolio_value","drawdown","is_measuring","slice_return"],
        [today.strftime("%Y-%m-%d"), round(total_value,2), round(account_dd,4), 1 if measuring else 0, round(slice_ret,4)])

# ========================= 事件函数 =========================
def initialize(context):
    _log("=== 恭喜发财 v4.8 初始化（Warmup+Slice+AutoClean, No-OS）===")
    g.universe=["600000.SS"]; set_universe(g.universe)
    g.last_good_base_universe = []  # 指数成分/基础池获取失败时的兜底

    g.today_candidates=[]; g.last_trade_date=None
    g.hold_days={}; g.entry_price={}; g.highest_close={}
    g.added_once={}; g.weak_tag_days={}
    g.bear_last_swap_date = None
    g.peak_value=None; g.state_cache=None; g.breadth_below_days=0

    g._holddays_bumped_date = None
    g.hist_cache = {}
    g.pos_csv, g.eq_csv, g.slice_csv = _report_paths()

    # === 启动即清空三张报表（不依赖 os）===
    if getattr(CFG, "AUTO_CLEAN_REPORTS", False):
        try:
            with open(g.pos_csv, "w", newline="") as f:
                csv.writer(f).writerow(
                    ["date","code","amount","close","hold_days",
                     "entry_price","highest_close","added_once",
                     "weak_days","market_state"]
                )
            with open(g.eq_csv, "w", newline="") as f:
                csv.writer(f).writerow(
                    ["date","portfolio_value","drawdown","market_state"]
                )
            with open(g.slice_csv, "w", newline="") as f:
                csv.writer(f).writerow(
                    ["date","portfolio_value","drawdown","is_measuring","slice_return"]
                )
            _log("[CLEAN] 已清空 %s/*.csv 并重写表头" % CFG.REPORT_DIR)
        except Exception as e:
            _log("[CLEAN] 清空报表失败: %s" % e)

    # Warmup + Slice 变量
    g.warmup_from  = _to_date(getattr(CFG, "WARMUP_FROM", ""))
    g.measure_from = _to_date(getattr(CFG, "MEASURE_FROM", ""))
    g.slice_started = False
    g.slice_base_value = None
    g.slice_days = 0

    _log("[INIT] REPORT_DIR=%s | WARMUP_FROM=%s | MEASURE_FROM=%s | WARMUP_TRADE=%s | SLICE_LAST_N=%d" %
         (CFG.REPORT_DIR,
          (CFG.WARMUP_FROM or "-"),
          (CFG.MEASURE_FROM or "-"),
          CFG.WARMUP_TRADE, int(CFG.SLICE_LAST_N)))

    # ===== 实盘：次日早盘“冲高卖” =====
    try:
        run_daily(context, _morning_sell_task, time=getattr(CFG, "MORNING_SELL_TIME", "09:40"))
        _log("[INIT] run_daily 已注册：MORNING_SELL_TIME=%s" % getattr(CFG, "MORNING_SELL_TIME", "09:40"))
    except Exception as e:
        _log("[INIT] run_daily 注册失败（可忽略于回测环境）: %s" % e)


def before_trading_start(context, data):
    today=_today(context)
    # 实盘重启/断线：先同步持仓状态（entry/highest/added）
    _sync_state_from_positions(context)
    # 为 09:40 的次日卖：盘前把持仓天数 +1
    _bump_hold_days_once(today, context)

    # 1）基础股票池（指数成分 + 主板过滤）
    base = _refresh_universe()
    if base:
        try:
            g.last_good_base_universe = list(base)
        except:
            pass
    else:
        # 指数成分获取失败时：优先沿用上一次有效股票池，避免退化为单票
        try:
            if hasattr(g, "last_good_base_universe") and g.last_good_base_universe:
                base = list(g.last_good_base_universe)
                _log("[UNI] 指数成分获取失败，沿用上一次有效股票池 size=%d" % len(base))
            elif hasattr(g, "universe") and g.universe:
                base = list(g.universe)
                _log("[UNI] 指数成分获取失败，沿用当前 g.universe size=%d" % len(base))
            else:
                base = ["600000.SS"]
                _log("[UNI] 指数成分获取失败，使用默认股票池")
        except:
            base = ["600000.SS"]

    # 2）可选：基本面 / 估值过滤
    uni = base
    if getattr(CFG, "ENABLE_FUND_FILTER", False) and base:
        pre_date = None
        try:
            # 推荐用上一交易日（YYYYMMDD），避免盘中取当日财务数据为空
            pre_date = _fmt_yyyymmdd(get_trading_day(-1))
        except Exception as e:
            _log("[FUND] 获取上一交易日失败，跳过基本面过滤: %s" % e)
        if pre_date:
            uni = _fundamental_filter(base, pre_date)

    g.universe = uni if uni else (base if base else ["600000.SS"])
    set_universe(g.universe)

    # 3）技术面打分，生成当日候选（严格 + 可选soft扩展）
    strict = _build_candidate_scores(g.universe, strict=True)
    cand = list(strict)

    state_now = _market_state()
    allow_soft_expand = True
    if state_now=="bear" and getattr(CFG, "BEAR_SEED_ENABLED", False) and getattr(CFG, "BEAR_SEED_STRICT_ONLY", True):
        allow_soft_expand = False

    if allow_soft_expand and len(cand) < CFG.MIN_STRICT_CANDS:
        soft = _build_candidate_scores(g.universe, strict=False)
        sc = set(c for c, _ in cand)
        for c, s in soft:
            if c not in sc:
                cand.append((c, s))

    cand.sort(key=lambda x: x[1], reverse=True)
    limit = CFG.BASE_MAX_HOLD_BULL * CFG.CAND_MULTIPLIER

    # 保存分数，便于熊市火种挑“最好”的那只
    try:
        g.cand_scores = {c: float(s) for c, s in cand}
        g.strict_scores = {c: float(s) for c, s in strict}
    except:
        g.cand_scores = {}
        g.strict_scores = {}

    g.today_candidates = [c for c, _ in cand[:limit]]
    g.today_candidates_strict = [c for c, _ in strict[:limit]]

    state_now = _market_state()
    _log("候选数：%d（严格=%d，总池=%d，state=%s）" %
         (len(g.today_candidates), len(strict), len(g.universe), state_now))

def handle_data(context, data):
    today=_today(context)

    # 实盘分钟级/更高频时：只在尾盘窗口执行“开仓/换仓/加仓/日更报表”
    if _is_trade_env() and getattr(CFG, "LIVE_ENABLE_TIME_GATE", False):
        hhmm = _hhmm(context)
        if hhmm < int(getattr(CFG, "LIVE_BUY_AFTER_HHMM", 1450) or 1450):
            return

    # 保险：尾盘执行前同步一次持仓状态（防重启导致 entry 丢失）
    _sync_state_from_positions(context)

    if getattr(g,"last_trade_date",None)==today: return
    g.last_trade_date=today

    # 净值/回撤
    price_map={}
    try:
        for c in g.universe: price_map[c]=float(data[c]["close"])
    except: pass
    total_value=_portfolio_value(context, price_map)
    if g.peak_value is None: g.peak_value=total_value
    g.peak_value=max(g.peak_value,total_value)
    account_dd = 0.0 if g.peak_value<=0 else (g.peak_value-total_value)/g.peak_value

    # Warmup/Measure 判定
    allow_trade_today = True
    if g.measure_from:
        if CFG.WARMUP_TRADE == "off" and today < g.measure_from:
            allow_trade_today = False

    # 切片起点（优先 MEASURE_FROM；否则按“最后N日”仅作标记）
    if (not g.slice_started) and g.measure_from and (today >= g.measure_from):
        g.slice_started=True; g.slice_base_value=total_value; g.slice_days=0
        _log("[SLICE] Start at %s, base=%.2f" % (today.strftime("%Y-%m-%d"), total_value))
    if (not g.slice_started) and (CFG.SLICE_LAST_N and CFG.SLICE_LAST_N>0):
        # 到最后 N 天时开始标记切片（只用于输出，非交易条件）
        # 需要当前回测环境能提供“总交易日计数”，这里简单用 portfolio_value 的峰值回推不安全，故仅当 MEASURE_FROM 为空时忽略。
        pass
    if g.slice_started: g.slice_days += 1

    # 预热不交易：只写报表
    if not allow_trade_today:
        _write_equity_and_slice(today, total_value, account_dd, measuring=False)
        return

    # ======== 以下保持原交易逻辑 ========
    state, MAX_HOLD, PER_STOCK, LOSS_STOP, TRAIL_START, TRAIL_DD, ENABLE_ADD, STUCK_ON = _adjust_by_state_and_dd(account_dd)
    g.state_cache=state
    _log("[STATE] 市场=%s | DD=%.2f%% | MAX_HOLD=%d | PER_STOCK=%.2f | ADD=%s" %
         (state, account_dd*100, MAX_HOLD, PER_STOCK, str(ENABLE_ADD)))
    # 熊市火种：覆盖持仓上限/单票仓位（不影响止损/止盈/加仓开关等）
    if state=="bear" and getattr(CFG, "BEAR_SEED_ENABLED", False):
        try:
            MAX_HOLD = int(getattr(CFG, "BEAR_SEED_MAX_HOLD", 1) or 1)
            if MAX_HOLD < 1: MAX_HOLD = 1
            PER_STOCK = float(getattr(CFG, "BEAR_SEED_PER_STOCK", PER_STOCK) or PER_STOCK)
        except: pass


    breadth=_simple_breadth(g.universe)
    if breadth < CFG.BREADTH_BLOCK_LEVEL: g.breadth_below_days += 1
    else: g.breadth_below_days = 0
    allow_new = not (g.breadth_below_days >= CFG.BREADTH_BLOCK_CONFIRM)
    if not allow_new:
        _log("[GATE] 广度 %.0f%% 连续 %d 天 < %.0f%%，禁开仓" %
             (breadth*100.0, g.breadth_below_days, CFG.BREADTH_BLOCK_LEVEL*100.0))

    # == 卖出 ==
    pos=_safe_positions(); holds=list(pos.keys()) if pos else []
    g.hold_days={c:int(g.hold_days.get(c,0)) for c in holds}  # 天数在盘前 before_trading_start 已 +1
    to_sell=[]
    for c in holds:
        h=_get_hist(c,40); cl=h["close"]
        if len(cl)<10: continue
        price=float(cl[-1]); entry=float(g.entry_price.get(c, price)); 
        if entry<=0: continue
        highest=max(float(g.highest_close.get(c, entry)), price)
        g.highest_close[c]=highest
        ret=price/entry-1.0
        dd_from_high = price/highest - 1.0 if highest>0 else 0.0
        days=g.hold_days.get(c,1)
        added=bool(g.added_once.get(c,False))
        loss_stop=CFG.LOSS_STOP_AFTER_ADD if added else LOSS_STOP

        prev=float(cl[-2]) if len(cl)>=2 else price
        prev_ret=prev/entry-1.0
        today_vs_prev=price/prev-1.0 if prev>0 else 0.0
        ma5 = sum(map(float,cl[-5:]))/5.0 if len(cl)>=5 else price
        rsi_seq=_rsi_series_from_close(cl, n=CFG.RSI_N)
        rsi_last = rsi_seq[-1] if rsi_seq else 50.0

        if ret <= loss_stop:
            to_sell.append((c,"LOSS_STOP")); continue

        weak_cond = (price < ma5 and rsi_last < 45.0) or (ret <= -0.02)
        if days >= CFG.MIN_HOLD_DAYS and weak_cond:
            wd = int(g.weak_tag_days.get(c,0)) + 1
            g.weak_tag_days[c]=wd
            exit_now=False
            if wd >= CFG.WEAK_OBSERVE_DAYS: exit_now=True
            if CFG.WEAK_EXIT_5D_BREAK and price <= min(map(float, cl[-5:])): exit_now=True
            if rsi_last < CFG.WEAK_EXIT_RSI_FLOOR: exit_now=True
            if exit_now: to_sell.append((c,"WEAK_EXIT"))
            else: _log("[OBS] %s 弱势观察 %d/%d" % (c, wd, CFG.WEAK_OBSERVE_DAYS))
            if not exit_now: continue
        else:
            g.weak_tag_days[c]=0

        if days >= CFG.MIN_HOLD_DAYS and ret >= TRAIL_START:
            if dd_from_high <= TRAIL_DD:
                to_sell.append((c,"TRAIL_DD")); continue
            if prev_ret >= 0.08 and today_vs_prev <= -0.03:
                to_sell.append((c,"BIGUP_WEAK")); continue

        if STUCK_ON and days >= CFG.STUCK_DAYS:
            atr=_atr(c,14); band=max(CFG.STUCK_RET_FLOOR, 1.2*atr)
            if abs(ret) < band and price < ma5 and rsi_last < 50:
                to_sell.append((c,"STUCK_SIDEWAY")); continue

        max_days=CFG.BASE_MAX_HOLD_DAYS
        if ret >= CFG.TIMEOUT_RET_OK:
            ma5c=sum(map(float,cl[-5:]))/5.0 if len(cl)>=5 else price
            if price >= ma5c: max_days=CFG.EXTEND_MAX_HOLD_DAYS
        if days >= max_days:
            to_sell.append((c,"TIMEOUT")); continue

    for c,reason in to_sell:
        try: order_target_value(c,0.0)
        except: pass
        for d in (g.entry_price,g.hold_days,g.highest_close,g.added_once,g.weak_tag_days):
            d.pop(c, None)
        _log("[SELL] %s | %s" % (c, reason))

    # == 补仓 ==
    pos=_safe_positions(); holds=list(pos.keys()) if pos else []
    if ENABLE_ADD and holds:
        total_value=_portfolio_value(context, price_map)
        if total_value>0:
            base_per=total_value*PER_STOCK
            max_pos=base_per*CFG.ADD_POS_MULTIPLIER
            for c in list(holds):
                if g.added_once.get(c,False): continue
                days=g.hold_days.get(c,1)
                if days>CFG.ADD_MAX_DAYS: continue
                cl=_get_hist_close(c,40)
                if len(cl)<20: continue
                price=float(cl[-1]); entry=float(g.entry_price.get(c,price))
                if entry<=0: continue
                ret=price/entry-1.0
                if not (CFG.ADD_LOSS_LOW <= ret <= CFG.ADD_LOSS_HIGH): continue
                if not (_trend_ok(cl) and _near_20d_high(cl)): continue
                rsi=_rsi_series_from_close(cl, n=CFG.RSI_N)
                if not rsi or not (CFG.RSI_LOW <= rsi[-1] <= CFG.RSI_HIGH): continue
                ma5t=sum(map(float,cl[-5:]))/5.0; ma5p=sum(map(float,cl[-6:-1]))/5.0
                if ma5t < ma5p*0.99: continue
                cur_val=0.0
                try:
                    p=pos.get(c); amt=int(getattr(p,"total_amount",0) or getattr(p,"amount",0))
                    if amt>0: cur_val=amt*price
                except: pass
                if cur_val<=0: continue
                target=min(max_pos, total_value*0.95/max(1.0,len(holds)))
                if target <= cur_val*1.02: continue
                try:
                    order_target_value(c, target)
                    g.added_once[c]=True
                    _log("[ADD] %s -> %.0f" % (c, target))
                except: pass


    # == 熊市火种：强制只保留 1 只（避免弱市多票磨损） ==
    if allow_trade_today and state=="bear" and getattr(CFG, "BEAR_SEED_ENABLED", False) and getattr(CFG, "BEAR_SEED_FORCE_TRIM", True):
        try:
            pos_now=_safe_positions(); holds_now=list(pos_now.keys()) if pos_now else []
            keep_n=int(getattr(CFG, "BEAR_SEED_MAX_HOLD", 1))
            if keep_n < 1: keep_n = 1
            if len(holds_now) > keep_n:
                # 用“当前收益率”保留最强的那只
                rets=[]
                for c in holds_now:
                    px=float(price_map.get(c, 0.0) or (_get_hist_close(c,1)[-1] if _get_hist_close(c,1) else 0.0))
                    ent=float(g.entry_price.get(c, 0.0) or getattr(pos_now.get(c), "avg_cost", 0.0) or px)
                    r = 0.0 if ent<=0 else (px-ent)/ent
                    rets.append((r, c))
                rets.sort()  # 从差到好
                sell_list=[c for _,c in rets[:-keep_n]]
                for c in sell_list:
                    try:
                        order_target_value(c, 0.0)
                        _log("[BEAR_TRIM] %s 触发熊市火种，仅保留%d只" % (c, keep_n))
                    except: pass
                    for d in (g.entry_price,g.hold_days,g.highest_close,g.added_once,g.weak_tag_days):
                        try:
                            if c in d: del d[c]
                        except: pass
        except: pass


    # == 熊市火种：允许“换火种”（不增加持仓数，仅卖旧买新） ==
    # 触发条件：bear + seed_enabled + allow_new + 当前持仓已满(=MAX_HOLD) + 新候选显著更强 或 旧火种明显走弱
    if state=="bear" and getattr(CFG, "BEAR_SEED_ENABLED", False) and allow_new and getattr(CFG, "BEAR_SEED_ALLOW_SWAP", False):
        try:
            pos_now=_safe_positions(); holds_now=list(pos_now.keys()) if pos_now else []
            keep_n=int(getattr(CFG, "BEAR_SEED_MAX_HOLD", 1) or 1)
            if keep_n < 1: keep_n = 1

            # 仅在“已满仓火种”时考虑换仓
            if len(holds_now) == keep_n and keep_n == 1:
                cur = holds_now[0]

                # 冷却：避免频繁换
                min_hold = int(getattr(CFG, "BEAR_SEED_SWAP_MIN_HOLD", 2) or 2)
                cooldown = int(getattr(CFG, "BEAR_SEED_SWAP_COOLDOWN", 3) or 3)
                if int(g.hold_days.get(cur, 0)) >= min_hold:
                    ok_cool = True
                    try:
                        if getattr(g, "bear_last_swap_date", None):
                            dd = (today - g.bear_last_swap_date).days
                            if dd < cooldown: ok_cool = False
                    except:
                        ok_cool = True

                    if ok_cool:
                        # 候选池：bear 严格候选优先
                        cand_list = getattr(g, "today_candidates_strict", None) or []
                        if (not cand_list) or (not getattr(CFG, "BEAR_SEED_STRICT_ONLY", True)):
                            cand_list = getattr(g, "today_candidates", None) or []

                        # 当前火种评分（不在候选里则视为很低）
                        sc_map = getattr(g, "cand_scores", {}) or {}
                        cur_sc = float(sc_map.get(cur, -1.0))

                        # 当前火种是否“明显走弱”
                        weak = False
                        try:
                            cl=_get_hist_close(cur, 25)
                            if len(cl) >= 10:
                                price=float(cl[-1])
                                ma5=sum(map(float, cl[-5:]))/5.0
                                rsi=_rsi_series_from_close(cl, n=CFG.RSI_N)
                                rsi_last = rsi[-1] if rsi else 50.0
                                ent=float(g.entry_price.get(cur, price) or price)
                                ret = 0.0 if ent<=0 else (price/ent - 1.0)
                                weak = (price < ma5 and rsi_last < 45.0) or (ret <= -0.02)
                        except:
                            weak = False

                        # 找到最强的新候选（不等于当前持仓）
                        best = None
                        best_sc = -1e9
                        min_sc = float(getattr(CFG, "BEAR_SEED_MIN_SCORE", 0.0) or 0.0)
                        for c in cand_list:
                            if c == cur: 
                                continue
                            s = float(sc_map.get(c, -1e9))
                            if min_sc > 0 and s < min_sc:
                                continue
                            if s > best_sc:
                                best_sc = s
                                best = c

                        # 判断是否换仓
                        delta = float(getattr(CFG, "BEAR_SEED_SWAP_DELTA", 0.05) or 0.05)
                        # 若旧火种走弱，放宽一点点；否则要明显更强才换
                        need = (cur_sc + delta) if not weak else (cur_sc + max(0.0, delta*0.3))

                        if best and (best_sc >= need):
                            total_value=_portfolio_value(context, price_map)
                            if total_value > 0:
                                per = total_value * PER_STOCK
                                try:
                                    order_target_value(cur, 0.0)
                                except:
                                    pass
                                try:
                                    order_target_value(best, per)
                                    px=float(price_map.get(best, 0.0) or (_get_hist_close(best,1)[-1] if _get_hist_close(best,1) else 0.0))
                                    g.entry_price[best]=px; g.hold_days[best]=0; g.highest_close[best]=px
                                    g.added_once[best]=False; g.weak_tag_days[best]=0
                                    # 清理旧火种缓存
                                    for d in (g.entry_price,g.hold_days,g.highest_close,g.added_once,g.weak_tag_days):
                                        try:
                                            if cur in d: del d[cur]
                                        except: pass
                                    g.bear_last_swap_date = today
                                    _log("[BEAR_SWAP] %s(%.3f) -> %s(%.3f) | weak=%s" % (cur, cur_sc, best, best_sc, str(weak)))
                                except:
                                    pass
        except:
            pass

    # == 开新仓 ==
    pos=_safe_positions(); holds=list(pos.keys()) if pos else []
    free=max(0, MAX_HOLD-len(holds))
    if allow_new and g.today_candidates and free>0:
        total_value=_portfolio_value(context, price_map)
        if total_value>0:
            per=total_value*PER_STOCK
            cand_list = g.today_candidates
            if state=="bear" and getattr(CFG, "BEAR_SEED_ENABLED", False):
                if getattr(CFG, "BEAR_SEED_STRICT_ONLY", True) and hasattr(g, "today_candidates_strict") and g.today_candidates_strict:
                    cand_list = g.today_candidates_strict
            # === 实盘资金闸门：按可用资金决定最多买几只，避免“可用不足” ===
            cash_avail = _available_cash(context)
            cash_left = max(0.0, cash_avail - float(getattr(CFG, "LIVE_RESERVE_CASH", 0.0) or 0.0))
            max_new = free
            if is_trade() and getattr(CFG, "LIVE_ONLY_ONE_IF_CASH_LOW", True):
                if cash_left <= 0:
                    max_new = 0
                else:
                    # 不够按 PER_STOCK 买满多只时：只买第 1 只（不拆小仓）
                    if cash_left < per * 2:
                        max_new = 1
                    else:
                        max_new = min(free, int(cash_left // per))
                        if max_new < 1: max_new = 1
            new_cnt = 0

            for c in cand_list:
                if new_cnt >= max_new: break
                if len(holds)>=MAX_HOLD: break
                if c in holds: continue
                # 熊市火种：最低分过滤（减少弱市误买）
                if state=="bear" and getattr(CFG, "BEAR_SEED_ENABLED", False):
                    try:
                        min_sc=float(getattr(CFG, "BEAR_SEED_MIN_SCORE", 0.0) or 0.0)
                        if min_sc>0 and float(getattr(g, "cand_scores", {}).get(c, 0.0)) < min_sc:
                            continue
                    except:
                        pass
                # 实盘：单笔目标金额（不足则按剩余可用资金）
                order_val = per
                if is_trade():
                    if cash_left < order_val:
                        order_val = cash_left
                    min_val = float(getattr(CFG, "LIVE_MIN_ORDER_VALUE", 0.0) or 0.0)
                    if min_val > 0 and order_val < min_val:
                        _log("[BUY] 可用资金不足以满足最小下单金额，停止开仓")
                        break
                try:
                    order_target_value(c, order_val)
                    px=float(price_map.get(c, 0.0) or (_get_hist_close(c,1)[-1] if _get_hist_close(c,1) else 0.0))
                    g.entry_price[c]=px; g.hold_days[c]=0; g.highest_close[c]=px
                    g.added_once[c]=False; g.weak_tag_days[c]=0
                    holds.append(c)
                    new_cnt += 1
                    if is_trade():
                        cash_left = max(0.0, cash_left - float(order_val))
                    _log("[BUY] %s | 目标%.0f | 可用%.0f" % (c, order_val, cash_left))
                except:
                    pass
    elif not allow_new:
        _log("[HOLD] 闸门关闭，仅持仓管理")
    else:
        _log("[HOLD] 无空位或候选为空")

    # == 报表 ==
    try:
        pos=_safe_positions()
        for c,p in (pos or {}).items():
            amt=int(getattr(p,"total_amount",0) or getattr(p,"amount",0))
            if amt<=0: continue
            cl=_get_hist_close(c,1); px=float(cl[-1]) if cl else 0.0
            _append_csv(g.pos_csv,
                ["date","code","amount","close","hold_days","entry_price","highest_close","added_once","weak_days","market_state"],
                [today.strftime("%Y-%m-%d"), c, amt, round(px,2),
                 int(g.hold_days.get(c,0)),
                 round(float(g.entry_price.get(c,px)),2),
                 round(float(g.highest_close.get(c,px)),2),
                 "Y" if g.added_once.get(c,False) else "N",
                 int(g.weak_tag_days.get(c,0)),
                 g.state_cache or "NA"])
    except Exception as e:
        _log("报表失败: %s" % e)

    # 写 equity 与 slice
    _write_equity_and_slice(today, total_value, account_dd, measuring=g.slice_started)

# ========================= （完） =========================