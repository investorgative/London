#!/usr/bin/env python3
"""
LONDON - core engine shared by the paper trader, the backtester and the optimizer.

  * Bybit v5 public data (no API keys) + sqlite candle / funding cache
  * 4h "ladder" support/resistance zones: long-memory wick clusters that acted as S/R
    at least twice (often from both sides), variable thickness, clear space between zones.
    Built ONLY from CLOSED 4h candles that exist at the moment of the decision.
  * 15m state machine: range A-B -> close through B -> 3-candle pullback -> 61.8% touch
    (72.2% re-anchor, zone invalidation) -> engulfing -> entry next open
  * structure trailing stop
  * per-symbol strategy parameters (BTC and SOL have very different ladders)
  * size-independent trade records + account replay (BTC / SOL / combined)
  * stats, journal, Kelly, Discord (trade + error webhooks)

Paper trader and backtester call exactly the same Engine.step() code path,
so a backtest trade and a paper trade are produced by identical logic.
"""
import copy
import gzip
import hashlib
import json
import logging
import math
import os
import sqlite3
import threading
import time
import traceback
from collections import deque
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

import numpy as np
import requests

VERSION = "1.0.0"
ZONE_ALGO = "ladder-1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(DATA_DIR, "logs")
ZONE_CACHE_DIR = os.path.join(DATA_DIR, "zone_cache")
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
SETTINGS_PATH = os.path.join(DATA_DIR, "settings.json")
STATE_PATH = os.path.join(DATA_DIR, "state.json")
TRADES_PATH = os.path.join(DATA_DIR, "trades.json")
DB_PATH = os.path.join(DATA_DIR, "london.db")
M15 = 15 * 60 * 1000
H4 = 4 * 60 * 60 * 1000
DAY = 24 * 60 * 60 * 1000
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(ZONE_CACHE_DIR, exist_ok=True)

# ---------------------------------------------------------------- defaults
# Zone widths / gaps are expressed in "u" = median 4h true range (as a fraction of price) over the
# lookback window, so the same logic adapts to BTC and SOL. Defaults per symbol were calibrated to
# reproduce the structure of hand-drawn ladders (see README "How the zones are built").
ZONE_KEYS = ("zone_lookback_days", "zone_pivot", "zone_wick_bars", "zone_max_width", "zone_gap",
             "zone_sep", "zone_min_touches", "zone_wick_weight", "zone_flip_bonus", "zone_recency",
             "zone_thick_penalty", "zone_edge_wicks", "zone_min_score", "zone_refresh")

DEFAULT_CONFIG = {
    "symbols": ["BTCUSDT", "SOLUSDT"],
    "category": "linear",
    "api_base": "https://api.bybit.com",
    "timezone": "Europe/Berlin",
    "account": {
        "start_balance": 100.0,
        "risk_pct": 2.0,
        "max_leverage": 50,
        "kelly_sizing": False,
        "kelly_fraction": 0.5,
        "kelly_min_trades": 20,
        "kelly_max_risk_pct": 10.0,
    },
    "fees": {"maker": 0.0002, "taker": 0.00055, "include_funding": True},
    "instruments": {
        "BTCUSDT": {"qty_step": 0.001, "min_qty": 0.001, "min_notional": 5.0, "tick": 0.1},
        "SOLUSDT": {"qty_step": 0.1, "min_qty": 0.1, "min_notional": 5.0, "tick": 0.01},
    },
    "strategy": {
        # ---- 4h ladder zones
        "zone_lookback_days": 540,  # history the zone finder may use (only closed candles before "now")
        "zone_pivot": 3,            # 4h swing strength (bars left/right) for a wick extreme = one reaction
        "zone_wick_bars": 1,        # neighbour candles of a reaction whose wicks belong to the same move
        "zone_max_width": 4.0,      # max zone thickness in u (tried widths: 25/50/75/100 % of this)
        "zone_gap": 2.0,            # min free space between two zones in u
        "zone_sep": 12,             # reactions closer than N 4h bars count as one (same move)
        "zone_min_touches": 3,      # zone must have acted as support/resistance at least N times
        "zone_wick_weight": 0.3,    # score bonus per reaction whose move-wicks touch the zone
        "zone_flip_bonus": 1.0,     # score bonus when the zone acted as support AND resistance
        "zone_recency": 1.0,        # score bonus for recently respected zones
        "zone_thick_penalty": 0.4,  # score penalty per u of thickness (keeps zones lean)
        "zone_edge_wicks": 1,       # 1 = zone edges expand to the touching wicks of the move
        "zone_min_score": -99.0,    # drop zones scoring below this
        "zone_refresh": 1,          # rebuild zones every N closed 4h candles
        # ---- 15m entry logic (same as January)
        "range_min_bars": 4,        # 15m closes that must be inside A-B before the break
        "swing_pivot": 2,           # 15m pivot strength for "the low before the move"
        "pullback_bars": 3,         # consecutive clear pullback candles (none may be a star / hammer)
        "fib_entry": 0.618,
        "fib_deep": 0.722,
        "invalidate": "far",        # far = close back below B (in range A-B), close = close inside B, touch
        "setup_max_bars": 192,      # setup expires after N 15m bars (192 = 2 days)
        "sl_buffer_atr": 0.10,      # breathing room below the low, in 15m ATR
        "trail_pivot": 2,           # pivot strength for structure highs used for trailing
        "star_ratio": 2.0,          # hammer / shooting star: wick >= ratio * body
        "min_sl_pct": 0.15,         # skip setups with SL tighter than this (fees would dominate)
        "max_sl_pct": 6.0,
        "anchor_mode": "pivot",       # pivot | leg_low | range_low
        "pullback_mode": "strict",    # strict | color | lower_highs
        "entry_window": "after_touch",# after_touch | in_band
        "engulf_mode": "body",        # body | close_beyond
        "exit_mode": "trail",         # trail | trail_zone_tp | trail_rr_tp
        "tp_r": 3.0,
    },
    # per-symbol overrides of "strategy" (calibrated zone ladders)
    "symbol_params": {
        "BTCUSDT": {"zone_lookback_days": 540, "zone_pivot": 5, "zone_wick_bars": 0, "zone_max_width": 3.0,
                    "zone_gap": 2.0, "zone_sep": 6, "zone_min_touches": 4, "zone_wick_weight": 0.2,
                    "zone_flip_bonus": 0.0, "zone_recency": 1.0, "zone_thick_penalty": 0.0, "zone_edge_wicks": 0,
                    "zone_min_score": 2.0},
        "SOLUSDT": {"zone_lookback_days": 540, "zone_pivot": 3, "zone_wick_bars": 1, "zone_max_width": 6.0,
                    "zone_gap": 3.0, "zone_sep": 48, "zone_min_touches": 2, "zone_wick_weight": 0.2,
                    "zone_flip_bonus": 2.0, "zone_recency": 0.5, "zone_thick_penalty": 0.0, "zone_edge_wicks": 0,
                    "zone_min_score": -99.0},
    },
    # hand-drawn zones (display + "Zone Match" only - NEVER used by the backtester or the trader)
    "reference_zones": {
        "BTCUSDT": {"date": "2026-11-10", "zones": [
            [57732.88, 59891.5], [61525.05, 63158.61], [65025.53, 67184.15], [69167.75, 71909.79],
            [74604.32, 78432.23], [80665.18, 82149.54], [83794.78, 85330.18], [87370.21, 90829.18],
            [92955.66, 94540.12], [98026.83, 100440.31], [106167.73, 107431.09], [108730.56, 110180.45],
            [115869.37, 116386.55]]},
        "SOLUSDT": {"date": "2026-11-10", "zones": [
            [60.12, 64.0], [70.67, 76.96], [95.11, 98.44], [112.26, 126.26], [135.44, 146.83],
            [163.7, 173.76], [185.17, 194.68], [206.91, 217.78]]},
    },
    "optimizer": {
        "days": 1000,
        "combos": 200,
        "zone_variants": 5,
        "train_frac": 0.7,
        "min_trades": 20,
        "workers": 0,
        "space": {
            "range_min_bars": [1, 4, 8],
            "swing_pivot": [1, 2, 3],
            "invalidate": ["far", "close"],
            "setup_max_bars": [96, 192, 384],
            "sl_buffer_atr": [0.05, 0.1, 0.2],
            "trail_pivot": [1, 2, 3],
            "star_ratio": [1.5, 2.0, 2.5],
            "min_sl_pct": [0.1, 0.15, 0.25],
        },
        # zone settings are searched as small steps around the calibrated ladder (each new zone set
        # is computed once and cached, so keep this space small on a Raspberry Pi)
        "zone_space": {
            "zone_lookback_days": [365, 450, 540, 730],
            "zone_min_touches": [-1, 0, 1],
            "zone_gap": [0.75, 1.0, 1.25],
            "zone_max_width": [0.75, 1.0, 1.25],
        },
        "lab_space": {
            "anchor_mode": ["pivot", "leg_low", "range_low"],
            "pullback_mode": ["strict", "color", "lower_highs"],
            "entry_window": ["after_touch", "in_band"],
            "engulf_mode": ["body", "close_beyond"],
            "exit_mode": ["trail", "trail_zone_tp", "trail_rr_tp"],
        },
        "include_zone_space": True,
        "include_lab_space": False,
    },
    "web": {"port": 8759, "host": "0.0.0.0", "password": ""},
    "discord": {
        "BTCUSDT": "",
        "SOLUSDT": "",
        "errors": "",
        "notify_start": True,
    },
    "backtest": {"default_days": 365},
}

NO_MERGE = ("space", "lab_space", "zone_space", "reference_zones")


def deep_merge(base, over):
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict) and k not in NO_MERGE:
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def params_for(cfg, sym, base=None):
    """strategy params for one symbol = common strategy block + that symbol's overrides.
    `base` (optional) replaces the common block (backtests with custom params)."""
    p = dict(base if base is not None else cfg["strategy"])
    p.update((cfg.get("symbol_params") or {}).get(sym, {}))
    if base is not None and isinstance(base.get("_sym"), dict):
        p.update(base["_sym"].get(sym, {}))
    p.pop("_sym", None)
    return p


def load_config():
    user = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, encoding="utf-8") as f:
            user = json.load(f)
    cfg = deep_merge(DEFAULT_CONFIG, user)
    if not os.path.exists(CONFIG_PATH):
        save_json(CONFIG_PATH, cfg)
    return cfg


def save_strategy_params(params, sym=None):
    """Used by the optimizer / lab 'apply' buttons. With sym, writes symbol_params[sym]
    (zone keys and entry keys alike), otherwise the common strategy block."""
    user = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, encoding="utf-8") as f:
            user = json.load(f)
    if sym:
        sp = user.get("symbol_params", {})
        cur = sp.get(sym, {})
        cur.update(params)
        sp[sym] = cur
        user["symbol_params"] = sp
    else:
        st = user.get("strategy", {})
        st.update(params)
        user["strategy"] = st
    save_json(CONFIG_PATH, user)


def load_settings(cfg):
    """Website-editable settings (risk etc.). They only change sizing, never signals."""
    s = {"risk_pct": cfg["account"]["risk_pct"], "start_balance": cfg["account"]["start_balance"],
         "kelly_sizing": cfg["account"]["kelly_sizing"]}
    if os.path.exists(SETTINGS_PATH):
        try:
            with open(SETTINGS_PATH, encoding="utf-8") as f:
                s.update(json.load(f))
        except Exception:
            pass
    return s


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(clean_json(obj), f, indent=1)
    os.replace(tmp, path)


def read_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def clean_json(o):
    if isinstance(o, dict):
        return {str(k): clean_json(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, deque)):
        return [clean_json(v) for v in o]
    if isinstance(o, (np.floating, float)):
        f = float(o)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.bool_):
        return bool(o)
    return o


def now_ms():
    return int(time.time() * 1000)


def tz(cfg):
    try:
        return ZoneInfo(cfg.get("timezone", "Europe/Berlin")) if ZoneInfo else timezone.utc
    except Exception:
        return timezone.utc


# ---------------------------------------------------------------- logging + discord
def _discord_post(url, payload):
    try:
        if isinstance(payload, str):
            payload = {"content": payload[:1990]}
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass


def discord_send(url, payload):
    if url:
        threading.Thread(target=_discord_post, args=(url, payload), daemon=True).start()


class DiscordHandler(logging.Handler):
    """Every ERROR/CRITICAL record goes to the error webhook (identical messages max once / 5 min)."""

    def __init__(self, url, source):
        super().__init__(level=logging.ERROR)
        self.url, self.source, self._recent, self._lock = url, source, {}, threading.Lock()

    def emit(self, record):
        if not self.url:
            return
        try:
            msg = record.getMessage()
            key = msg[:200]
            with self._lock:
                if time.time() - self._recent.get(key, 0) < 300:
                    return
                self._recent[key] = time.time()
            tb = "".join(traceback.format_exception(*record.exc_info)) if record.exc_info else ""
            body = msg + ("\n\n" + tb if tb else "")
            if len(body) > 1700:
                body = body[:500] + "\n...\n" + body[-1150:]
            discord_send(self.url, f"**◆ LONDON {self.source} | {record.levelname}** "
                                   f"`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n```{body}```")
        except Exception:
            pass


def setup_logging(source, cfg):
    log = logging.getLogger("london")
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [" + source + "] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = RotatingFileHandler(os.path.join(LOG_DIR, "london.log"), maxBytes=2_000_000, backupCount=3,
                             encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    log.addHandler(DiscordHandler(cfg["discord"].get("errors", ""), source))

    import sys

    def hook(t, v, tb):
        log.critical("uncaught exception", exc_info=(t, v, tb))

    sys.excepthook = hook
    return log


# ---------------------------------------------------------------- Bybit
class Bybit:
    def __init__(self, cfg):
        self.base = cfg["api_base"].rstrip("/")
        self.cat = cfg["category"]
        self.s = requests.Session()
        self.s.headers["User-Agent"] = f"London-paper/{VERSION}"

    def _get(self, path, params):
        if os.environ.get("LONDON_OFFLINE"):  # testing without internet: use the sqlite cache only
            raise RuntimeError("offline mode (LONDON_OFFLINE set)")
        err = None
        for attempt in range(4):
            try:
                r = self.s.get(self.base + path, params=params, timeout=15)
                r.raise_for_status()
                j = r.json()
                if j.get("retCode") != 0:
                    raise RuntimeError(f"retCode={j.get('retCode')} {j.get('retMsg')}")
                return j["result"]
            except Exception as e:
                err = e
                time.sleep(1.5 * (attempt + 1))
        raise RuntimeError(f"Bybit request failed {path} {params}: {err}")

    def klines(self, sym, interval="15", start=None, end=None, limit=1000):
        p = {"category": self.cat, "symbol": sym, "interval": interval, "limit": limit}
        if start is not None:
            p["start"] = int(start)
        if end is not None:
            p["end"] = int(end)
        rows = self._get("/v5/market/kline", p).get("list", [])
        out = [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])) for r in rows]
        out.sort()
        return out

    def klines_range(self, sym, start, end, interval="15", progress=None):
        res, cur_end, total = {}, int(end), max(1, int(end) - int(start))
        while True:
            rows = self.klines(sym, interval, start=start, end=cur_end)
            if not rows:
                break
            for r in rows:
                res[r[0]] = r
            first = rows[0][0]
            if progress:
                progress(min(1.0, (end - first) / total))
            if first <= start or len(rows) < 1000:
                break
            cur_end = first - 1
            time.sleep(0.04)
        return [res[k] for k in sorted(res) if start <= k <= end]

    def ticker(self, sym):
        return self._get("/v5/market/tickers", {"category": self.cat, "symbol": sym})["list"][0]

    def funding_range(self, sym, start, end):
        res, cur_end = {}, int(end)
        while True:
            rows = self._get("/v5/market/funding/history", {"category": self.cat, "symbol": sym,
                                                             "startTime": int(start), "endTime": cur_end,
                                                             "limit": 200}).get("list", [])
            if not rows:
                break
            for r in rows:
                res[int(r["fundingRateTimestamp"])] = float(r["fundingRate"])
            oldest = min(int(r["fundingRateTimestamp"]) for r in rows)
            if oldest <= start or len(rows) < 200:
                break
            cur_end = oldest - 1
            time.sleep(0.04)
        return res


class Store:
    """sqlite cache of CLOSED 15m candles and funding rates (thread-safe via per-call connections)."""

    def __init__(self, api):
        self.api = api
        self.lock = threading.Lock()
        con = self._con()
        con.execute("CREATE TABLE IF NOT EXISTS candles (sym TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, "
                    "v REAL, PRIMARY KEY(sym, ts))")
        con.execute("CREATE TABLE IF NOT EXISTS funding (sym TEXT, ts INTEGER, rate REAL, PRIMARY KEY(sym, ts))")
        con.commit()
        con.close()

    def _con(self):
        con = sqlite3.connect(DB_PATH, timeout=30)
        con.execute("PRAGMA journal_mode=WAL")
        return con

    def insert(self, sym, rows):
        lim = now_ms()
        rows = [(sym,) + tuple(r) for r in rows if r[0] + M15 <= lim]  # closed candles only
        if rows:
            with self.lock:
                con = self._con()
                con.executemany("INSERT OR REPLACE INTO candles VALUES(?,?,?,?,?,?,?)", rows)
                con.commit()
                con.close()
        return len(rows)

    def bounds(self, sym):
        con = self._con()
        r = con.execute("SELECT MIN(ts), MAX(ts) FROM candles WHERE sym=?", (sym,)).fetchone()
        con.close()
        return r

    def ensure(self, sym, start, end, progress=None):
        end = min(int(end), now_ms())
        mn, mx = self.bounds(sym)
        segs = []
        if mn is None:
            segs.append((start, end))
        else:
            if start < mn:
                segs.append((start, mn - 1))
            if end > mx + M15:
                segs.append((mx, end))
        for s, e in segs:
            self.insert(sym, self.api.klines_range(sym, s, e, progress=progress))

    def load(self, sym, start, end):
        con = self._con()
        rows = con.execute("SELECT ts,o,h,l,c,v FROM candles WHERE sym=? AND ts>=? AND ts<=? ORDER BY ts",
                           (sym, int(start), int(end))).fetchall()
        con.close()
        if not rows:
            return {k: np.array([]) for k in ("ts", "o", "h", "l", "c", "v")}
        a = np.array(rows, dtype=float)
        return {"ts": a[:, 0].astype(np.int64), "o": a[:, 1], "h": a[:, 2], "l": a[:, 3], "c": a[:, 4],
                "v": a[:, 5]}

    def ensure_funding(self, sym, start, end):
        con = self._con()
        mn, mx = con.execute("SELECT MIN(ts), MAX(ts) FROM funding WHERE sym=?", (sym,)).fetchone()
        con.close()
        segs = []
        if mn is None:
            segs.append((start, end))
        else:
            if start < mn - 8 * 3_600_000:
                segs.append((start, mn - 1))
            if end > mx + 8 * 3_600_000:
                segs.append((mx + 1, end))
        for s, e in segs:
            d = self.api.funding_range(sym, s, e)
            if d:
                with self.lock:
                    con = self._con()
                    con.executemany("INSERT OR REPLACE INTO funding VALUES(?,?,?)",
                                    [(sym, t, r) for t, r in d.items()])
                    con.commit()
                    con.close()

    def load_funding(self, sym, start, end):
        con = self._con()
        rows = con.execute("SELECT ts, rate FROM funding WHERE sym=? AND ts>=? AND ts<=?",
                           (sym, int(start), int(end))).fetchall()
        con.close()
        return {int(t): float(r) for t, r in rows}


# ---------------------------------------------------------------- candle helpers
def candle_shape(o, h, l, c, ratio):
    """returns 'hammer', 'star' (shooting star) or '' (wicks are used, never closes)."""
    rng = h - l
    if rng <= 0:
        return ""
    body = abs(c - o)
    lower = min(o, c) - l
    upper = h - max(o, c)
    b = max(body, rng * 0.02)
    if lower >= ratio * b and lower >= 0.5 * rng and upper <= 0.25 * rng:
        return "hammer"
    if upper >= ratio * b and upper >= 0.5 * rng and lower <= 0.25 * rng:
        return "star"
    return ""


def aggregate_4h(ts, o, h, l, c):
    """15m -> closed 4h candles. Returns list of dicts and, per 4h candle, the index of the
    15m candle whose close completes it (zones may only be used AFTER that bar closed)."""
    out = []
    cur = None
    for i in range(len(ts)):
        b = int(ts[i]) - int(ts[i]) % H4
        if cur is None or cur["t"] != b:
            cur = {"t": b, "o": o[i], "h": h[i], "l": l[i], "c": c[i], "n": 0}
            out.append(cur)
        else:
            cur["h"] = max(cur["h"], h[i])
            cur["l"] = min(cur["l"], l[i])
            cur["c"] = c[i]
        cur["n"] += 1
        cur["last_i"] = i
    closed = [x for x in out if (int(ts[x["last_i"]]) + M15) % H4 == 0]
    return closed


def atr_simple(h, l, c, n=14):
    if len(h) < 2:
        return float(h[-1] - l[-1]) if len(h) else 0.0
    tr = [h[0] - l[0]]
    for i in range(1, len(h)):
        tr.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
    tr = tr[-n:]
    return float(sum(tr) / len(tr))


def _pivots(h, l, k):
    """confirmed 4h swing extremes (both sides need k bars). returns (idx, kind) kind=-1 low / +1 high."""
    n = len(h)
    out = []
    if n < 2 * k + 1:
        return out
    if k <= 0:
        return [(j, -1) for j in range(n)] + [(j, 1) for j in range(n)]
    from numpy.lib.stride_tricks import sliding_window_view as sw
    wh = sw(h, 2 * k + 1)
    wl = sw(l, 2 * k + 1)
    mh = wh.max(axis=1)
    ml = wl.min(axis=1)
    lh = sw(h[:-k] if k else h, k).max(axis=1)[: len(mh)]  # max of the k bars to the left
    ll = sw(l[:-k] if k else l, k).min(axis=1)[: len(ml)]
    mid_h = h[k:n - k]
    mid_l = l[k:n - k]
    ph = np.nonzero((mid_h >= mh) & (mid_h > lh))[0] + k
    pl = np.nonzero((mid_l <= ml) & (mid_l < ll))[0] + k
    out = [(int(j), 1) for j in ph] + [(int(j), -1) for j in pl]
    out.sort()
    return out


def ladder_zones(o, h, l, c, p):
    """LONDON support/resistance ladder from CLOSED 4h candles (arrays = the lookback window only).

    1. every confirmed 4h swing high / low is one *reaction* (its wick extreme is the level)
    2. the wicks of the neighbouring candles of that move (zone_wick_bars) belong to the reaction
       -> "as many wicks of one move as possible touch the area"
    3. candidate bands of several thicknesses are slid over the whole price range; a band scores
       distinct reactions inside it (reactions closer than zone_sep bars = one move), bonus for
       acting as support AND resistance, bonus for wick touches and recency, penalty for thickness
    4. bands need >= zone_min_touches distinct reactions, edges snap to the wick extremes
    5. greedy best-first selection with at least zone_gap free space between zones
       -> no overlapping, not washed-out, sustainable levels with space between them
    All widths are in u = median 4h true range / price of the window (adapts BTC vs SOL).
    """
    n = len(c)
    if n < 60:
        return []
    h = np.asarray(h, float)
    l = np.asarray(l, float)
    c = np.asarray(c, float)
    tr = np.maximum(h[1:], c[:-1]) - np.minimum(l[1:], c[:-1])
    u = float(np.median(tr / c[1:]))
    if not u > 0:
        return []
    # quantize u (5 % steps) so widths / gaps don't drift a little with every new candle
    u = math.exp(round(math.log(u) / 0.05) * 0.05)
    lo_, hi_ = np.log(l), np.log(h)
    k = int(p["zone_pivot"])
    piv = _pivots(h, l, k)
    if len(piv) < 2:
        return []
    J = np.array([j for j, _ in piv])
    K = np.array([kd for _, kd in piv])
    X = np.where(K == -1, lo_[J], hi_[J])
    m = int(p["zone_wick_bars"])
    if m > 0:
        W, WJ = [], []
        for j, kd in piv:
            a, b = max(0, j - m), min(n, j + m + 1)
            seg = lo_[a:b] if kd == -1 else hi_[a:b]
            W.extend(seg.tolist())
            WJ.extend([j] * len(seg))
        W = np.array(W)
        WJ = np.array(WJ)
    else:
        W, WJ = X.copy(), J.copy()
    maxw = float(p["zone_max_width"])
    widths = [math.log1p(maxw * f * u) for f in (0.25, 0.5, 0.75, 1.0)]
    gap = math.log1p(float(p["zone_gap"]) * u)
    lam = float(p["zone_thick_penalty"])
    sep = int(p["zone_sep"])
    mint = int(p["zone_min_touches"])
    wk, flip, rec = float(p["zone_wick_weight"]), float(p["zone_flip_bonus"]), float(p["zone_recency"])
    edge = int(p.get("zone_edge_wicks", 1))
    order = np.argsort(X, kind="stable")
    Xs, Js, Ks = X[order], J[order], K[order]
    wo = np.argsort(W, kind="stable")
    Ws, WJs = W[wo], WJ[wo]
    # candidate bands start on a FIXED log-price lattice (0.1 %), independent of the window,
    # so a new candle only changes the zones it really touches (stable, sustainable levels)
    step = 0.001
    g0 = math.floor((Xs[0] - widths[-1]) / step) * step
    starts = np.arange(g0, Xs[-1] + step, step)
    A = np.concatenate([starts] * len(widths))
    B = np.concatenate([starts + w for w in widths])
    I0, I1 = np.searchsorted(Xs, A), np.searchsorted(Xs, B, "right")
    W0, W1 = np.searchsorted(Ws, A), np.searchsorted(Ws, B, "right")
    ok = (I1 - I0) >= max(2, mint)
    # every score term depends only on which reactions / wicks fall inside the band, so identical
    # index ranges give identical candidates -> evaluate each unique range once (exact)
    keys = np.unique(np.stack([I0[ok], I1[ok], W0[ok], W1[ok]], axis=1), axis=0)
    xs_l, js_l, ks_l = Xs.tolist(), Js.tolist(), Ks.tolist()
    ws_l, wj_l = Ws.tolist(), WJs.tolist()
    inner = {}  # (i0, i1) -> reaction stats (shared by all wick ranges)
    cands = []
    for i0, i1, w0, w1 in keys.tolist():
        st = inner.get((i0, i1))
        if st is None:
            js = sorted(js_l[i0:i1])
            distinct = 1
            for q in range(1, len(js)):
                if js[q] - js[q - 1] > sep:
                    distinct += 1
            ns = ks_l[i0:i1].count(-1)
            nr = i1 - i0 - ns
            st = (distinct, ns, nr, js[-1])
            inner[(i0, i1)] = st
        distinct, ns, nr, last = st
        if distinct < mint:
            continue
        both = ns > 0 and nr > 0
        touch = len(set(wj_l[w0:w1]))
        bot, top = xs_l[i0], xs_l[i1 - 1]
        if edge and w1 > w0:
            bot, top = min(bot, ws_l[w0]), max(top, ws_l[w1 - 1])
        width_u = math.expm1(top - bot) / u
        score = distinct + wk * touch + flip * both + rec * float(last) / n - lam * width_u
        cands.append((score, bot, top, distinct, both, int(last), ns, nr))
    cands.sort(key=lambda x: -x[0])
    acc = []
    ms = float(p.get("zone_min_score", -99))
    for cd in cands:
        if cd[0] < ms:
            break
        b, t = cd[1], cd[2]
        okz = True
        for z in acc:
            if not (b - z[2] >= gap or z[1] - t >= gap):
                okz = False
                break
        if okz:
            acc.append(cd)
    acc.sort(key=lambda x: x[1])
    out = []
    for sc, b, t, dd, both, last, ns, nr in acc:
        out.append({"bot": float(math.exp(b)), "top": float(math.exp(t)), "touches": dd,
                    "kind": "SR" if both else ("S" if ns else "R"), "lows": ns, "highs": nr,
                    "score": round(float(sc), 3), "last_bar": n - 1 - last})
    return out


def zone_key(p):
    """hash of everything the zones depend on (algorithm version + zone params)."""
    d = {k: p.get(k) for k in ZONE_KEYS}
    d["_algo"] = ZONE_ALGO
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()[:12]


class ZoneCache:
    """Zones per (symbol, zone params, 4h close time). Zones depend only on candles that closed
    before that time, so a cached value is exactly what a live run would have computed then.
    Saves minutes on a Raspberry Pi for every backtest / optimizer run after the first one."""

    _mem = {}
    _lock = threading.Lock()

    def __init__(self, sym, p, persist=True):
        self.sym, self.key, self.persist = sym, zone_key(p), persist
        self.path = os.path.join(ZONE_CACHE_DIR, f"{sym}_{self.key}.json.gz")
        self.dirty = 0
        mk = (sym, self.key)
        with ZoneCache._lock:
            if mk not in ZoneCache._mem:
                d = {}
                if persist and os.path.exists(self.path):
                    try:
                        with gzip.open(self.path, "rt", encoding="utf-8") as f:
                            d = json.load(f)
                    except Exception:
                        d = {}
                ZoneCache._mem[mk] = d
            self.d = ZoneCache._mem[mk]

    def get(self, key):
        return self.d.get(str(key))

    def put(self, key, zones):
        self.d[str(key)] = zones
        self.dirty += 1

    def save(self):
        if not self.persist or not self.dirty:
            return
        try:
            tmp = self.path + ".tmp"
            with gzip.open(tmp, "wt", encoding="utf-8") as f:
                json.dump(self.d, f, separators=(",", ":"))
            os.replace(tmp, self.path)
            self.dirty = 0
        except Exception:
            logging.getLogger("london").warning("zone cache save failed", exc_info=True)


def zone_match(zones, ref):
    """how close an algorithmic ladder is to a hand-drawn one (log-price IoU).
    recall = avg best IoU per reference zone, precision = avg best IoU per algo zone (inside the
    reference price range), hit = share of reference zones overlapped by any algo zone."""
    R = [(math.log(a), math.log(b)) for a, b in ref if a > 0 and b > a]
    A = [(math.log(z["bot"]), math.log(z["top"])) for z in zones if z["bot"] > 0]
    if not R:
        return None
    lo, hi = min(r[0] for r in R), max(r[1] for r in R)
    A = [a for a in A if a[1] >= lo - 0.02 and a[0] <= hi + 0.02]

    def iou(x, y):
        i = max(0.0, min(x[1], y[1]) - max(x[0], y[0]))
        un = max(x[1], y[1]) - min(x[0], y[0])
        return i / un if un > 0 else 0.0
    rec = float(np.mean([max([iou(r, a) for a in A] or [0.0]) for r in R]))
    prec = float(np.mean([max(iou(r, a) for r in R) for a in A])) if A else 0.0
    hit = float(np.mean([any(iou(r, a) > 0 for a in A) for r in R]))
    return {"recall": rec, "precision": prec, "hit": hit, "score": (rec + prec) / 2, "n_algo": len(A),
            "n_ref": len(R)}


# ---------------------------------------------------------------- engine
class Engine:
    """Per-symbol strategy state machine. Feed CLOSED 15m bars in order with step()."""

    MAXBUF = 2500

    def __init__(self, symbol, params, cfg, zone_cache=None):
        self.sym = symbol
        self.p = dict(params)
        self.cfg = cfg
        self.ts, self.o, self.h, self.l, self.c = [], [], [], [], []
        self.off = 0  # absolute index of buffer[0]
        # closed 4h candles (long memory for the ladder)
        self.h4t, self.h4o, self.h4h, self.h4l, self.h4c = [], [], [], [], []
        self.h4_max = int(self.p.get("zone_lookback_days", 730)) * 6 + 10
        self.h4_since = 0  # closed 4h candles since the last zone rebuild
        self.cur4 = None
        self.zones = []
        self.zones_ts = None
        self.zcache = zone_cache if zone_cache is not None else ZoneCache(symbol, self.p)
        self.atr15 = None
        self.trs = deque(maxlen=14)
        self.setup = None
        self.pos = None
        self.pending = None
        self.events = []
        self.n = 0
        self.last_status = ""

    # ---- buffer access by absolute index
    def _i(self, a):
        return a - self.off

    def H(self, a):
        return self.h[a - self.off]

    def L(self, a):
        return self.l[a - self.off]

    def C(self, a):
        return self.c[a - self.off]

    def O(self, a):
        return self.o[a - self.off]

    # ---- 4h
    def preload_4h(self, bars):
        """seed closed 4h candles (warm-up far before the simulation start; no 15m stepping needed).
        bars: list of dicts {t,o,h,l,c} in time order, all closed before the first 15m step."""
        for b in bars:
            self._push_4h(b["t"], b["o"], b["h"], b["l"], b["c"])
        self.h4_since = 10 ** 9  # force a rebuild at the next closed 4h candle
        if bars:
            self._rebuild_zones(int(bars[-1]["t"]) + H4)

    def _push_4h(self, t, o, h, l, c):
        self.h4t.append(int(t))
        self.h4o.append(o)
        self.h4h.append(h)
        self.h4l.append(l)
        self.h4c.append(c)
        if len(self.h4t) > self.h4_max + 200:
            cut = len(self.h4t) - self.h4_max
            for a in (self.h4t, self.h4o, self.h4h, self.h4l, self.h4c):
                del a[:cut]

    def _rebuild_zones(self, close_ts):
        """zones valid from close_ts on = built from 4h candles that closed at or before close_ts."""
        lb = int(self.p.get("zone_lookback_days", 730)) * 6
        n = len(self.h4t)
        a = max(0, n - lb)
        # full window -> key is the close time; shorter history -> key also holds the window start
        ck = str(int(close_ts)) if n - a >= lb or n == 0 else f"{int(close_ts)}@{self.h4t[a]}"
        z = self.zcache.get(ck)
        if z is None:
            z = ladder_zones(np.array(self.h4o[a:]), np.array(self.h4h[a:]), np.array(self.h4l[a:]),
                             np.array(self.h4c[a:]), self.p)
            self.zcache.put(ck, z)
        self.zones = z
        self.zones_ts = close_ts
        self.h4_since = 0

    def _update_4h(self, t, o, h, l, c):
        b = t - t % H4
        if self.cur4 is None or self.cur4["t"] != b:
            self.cur4 = {"t": b, "o": o, "h": h, "l": l, "c": c}
        else:
            self.cur4["h"] = max(self.cur4["h"], h)
            self.cur4["l"] = min(self.cur4["l"], l)
            self.cur4["c"] = c
        if (t + M15) % H4 == 0:  # this 15m close completes the 4h candle
            x = self.cur4
            self._push_4h(x["t"], x["o"], x["h"], x["l"], x["c"])
            self.cur4 = None
            self.h4_since += 1
            if self.h4_since >= max(1, int(self.p.get("zone_refresh", 1))):
                self._rebuild_zones(t + M15)

    def _zone_index(self, price):
        """returns ('in', k) when inside zone k, ('between', k) when between zone k-1 and k
        (k == len(zones) means above all zones, k == 0 below all)."""
        for k, z in enumerate(self.zones):
            if z["bot"] <= price <= z["top"]:
                return "in", k
            if price < z["bot"]:
                return "between", k
        return "between", len(self.zones)

    # ---- pivots on 15m (confirmed only)
    def _pivot_lows_before(self, a_end, k, below=None):
        """confirmed pivot lows with index j, j+k <= a_end, newest first."""
        out = []
        start = max(self.off + k, a_end - 600)
        for j in range(a_end - k, start - 1, -1):
            lj = self.L(j)
            ok = all(lj <= self.L(x) for x in range(j - k, j + k + 1)) and all(lj < self.L(x) for x in range(j - k, j))
            if ok and (below is None or lj < below):
                out.append(j)
                if len(out) >= 12:
                    break
        return out

    def _pivot_highs_before(self, a_end, k, above=None):
        out = []
        start = max(self.off + k, a_end - 600)
        for j in range(a_end - k, start - 1, -1):
            hj = self.H(j)
            ok = all(hj >= self.H(x) for x in range(j - k, j + k + 1)) and all(hj > self.H(x) for x in range(j - k, j))
            if ok and (above is None or hj > above):
                out.append(j)
                if len(out) >= 12:
                    break
        return out

    # ---- main entry point
    def step(self, t, o, h, l, c):
        """process one CLOSED 15m candle. Returns list of events."""
        self.events = []
        self.ts.append(int(t))
        self.o.append(o)
        self.h.append(h)
        self.l.append(l)
        self.c.append(c)
        a = self.off + len(self.c) - 1
        prev_c = self.c[-2] if len(self.c) > 1 else c
        self.trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
        self.atr15 = sum(self.trs) / len(self.trs)

        # 1) pending entry fills at THIS bar's open (signal came from the previous close)
        if self.pending is not None:
            self._fill(self.pending, t, o)
            self.pending = None
        # 2) stop check inside this bar with the stop that was valid at bar open
        if self.pos is not None and self.pos.get("entry_ts", 0) <= t:
            self._check_stop(t, o, h, l)
        # 3) close based logic
        if self.pos is not None:
            self._trail(a)
        else:
            self._setup_logic(a)
        # 4) 4h zones update only after the 4h candle completed (used from the next bar on)
        self._update_4h(int(t), o, h, l, c)
        if len(self.c) > self.MAXBUF:
            cut = len(self.c) - self.MAXBUF
            del self.ts[:cut], self.o[:cut], self.h[:cut], self.l[:cut], self.c[:cut]
            self.off += cut
        self.n += 1
        return self.events

    # ---- setup state machine
    def _setup_logic(self, a):
        p = self.p
        c, h, l, o = self.C(a), self.H(a), self.L(a), self.O(a)
        if not self.zones or a - self.off < 30:
            self.last_status = "collecting data / no zones"
            return
        s = self.setup
        # breakout detection (also replaces an older setup)
        bo = self._detect_breakout(a)
        if bo is not None:
            s = self._new_setup(a, bo)
            self.setup = s
            if s is None:
                return
        if s is None:
            where, k = self._zone_index(c)
            self.last_status = "waiting for a breakout" if where == "between" else "price inside a zone"
            return
        d = s["dir"]
        # expiry
        if a - s["bo_a"] > p["setup_max_bars"]:
            self._drop("expired")
            return
        # invalidation: back into the broken zone
        zt, zb = s["zone"]["top"], s["zone"]["bot"]
        inv = p.get("invalidate", "far")
        if inv == "far":  # closed back beyond the far side of the zone = back in the old range A-B
            back = (c < zb) if d == 1 else (c > zt)
        elif inv == "close":  # closed back inside the broken zone
            back = (c < zt) if d == 1 else (c > zb)
        else:  # "touch": any wick back into the zone
            back = (l <= zt) if d == 1 else (h >= zb)
        if back and a > s["bo_a"]:
            self._drop("price back in the old range" if inv == "far" else "price back in the broken zone")
            return
        st = s["stage"]
        if st == "impulse":
            # track extreme of the move, count clear pullback candles
            if (d == 1 and h > s["ext"]) or (d == -1 and l < s["ext"]):
                s["ext"], s["ext_a"] = (h if d == 1 else l), a
                s["pb"] = 0
            shape = candle_shape(o, h, l, c, p["star_ratio"])
            bad = "star" if d == 1 else "hammer"
            prev_c = self.C(a - 1)
            pm = p.get("pullback_mode", "strict")
            if pm == "color":
                clear = (c < o) if d == 1 else (c > o)
            elif pm == "lower_highs":
                clear = (h < self.H(a - 1) and l < self.L(a - 1)) if d == 1 else (l > self.L(a - 1) and h > self.H(a - 1))
            else:
                clear = (c < o and c < prev_c) if d == 1 else (c > o and c > prev_c)
            if a == s["ext_a"]:
                clear = clear and False  # the candle that made the extreme is not a pullback candle
            if clear and shape != bad:
                s["pb"] += 1
            elif a != s["ext_a"]:
                s["pb"] = 0
            if s["pb"] >= p["pullback_bars"]:
                s["stage"] = "pullback"
                self._levels(s)
                self.events.append({"type": "pullback", "sym": self.sym, "ts": self.ts[-1], "dir": d})
            else:
                self.last_status = f"broke {'above' if d == 1 else 'below'} zone - waiting for {p['pullback_bars']}-candle pullback ({s['pb']}/{p['pullback_bars']})"
                return
        if s["stage"] in ("pullback", "touched"):
            # new extreme before entry -> the move continues, restart structure
            if (d == 1 and c > s["ext"]) or (d == -1 and c < s["ext"]):
                s["stage"] = "impulse"
                s["ext"], s["ext_a"] = (h if d == 1 else l), a
                s["pb"] = 0
                piv = self._pivot_lows_before(a, p["swing_pivot"]) if d == 1 else self._pivot_highs_before(a, p["swing_pivot"])
                piv = [j for j in piv if j > s["bo_a"] - 400]
                if piv:
                    s["anchors"] = piv
                    s["anc_k"] = 0
                self.last_status = "new extreme - structure restarted, waiting for pullback"
                return
            # deeper than 72.2% (close) -> the low was wrong, take the one before
            while (d == 1 and c < s["deep"]) or (d == -1 and c > s["deep"]):
                if not self._reanchor(s):
                    self._drop("no earlier swing point left")
                    return
            if s["stage"] == "pullback":
                if (d == 1 and l <= s["fib"]) or (d == -1 and h >= s["fib"]):
                    s["stage"] = "touched"
                    s["touch_a"] = a
                    self.events.append({"type": "touch", "sym": self.sym, "ts": self.ts[-1], "dir": d})
                else:
                    self.last_status = f"pullback running - waiting for {p['fib_entry'] * 100:.1f}% ({s['fib']:.4f})"
                    return
            # engulfing in the breakout direction (may be the touch bar itself)
            po, pc = self.O(a - 1), self.C(a - 1)
            ph, pl = self.H(a - 1), self.L(a - 1)
            prev_shape = candle_shape(po, ph, pl, pc, p["star_ratio"])
            em = p.get("engulf_mode", "body")
            if d == 1:
                eng = c > o and pc < po and o <= pc and c >= po and (c - o) > (po - pc)
                if em == "close_beyond":
                    eng = eng and c > ph
            else:
                eng = c < o and pc > po and o >= pc and c <= po and (o - c) > (pc - po)
                if em == "close_beyond":
                    eng = eng and c < pl
            if eng and p.get("entry_window", "after_touch") == "in_band":
                eng = (min(l, pl) <= s["fib"]) if d == 1 else (max(h, ph) >= s["fib"])
                eng = eng and ((c > s["deep"]) if d == 1 else (c < s["deep"]))
            if eng and prev_shape == "":
                sl = self._stop_price(s)
                dist = abs(c - sl) / c * 100
                if (d == 1 and sl >= c) or (d == -1 and sl <= c):
                    self._drop("stop on the wrong side")
                    return
                if dist < p["min_sl_pct"] or dist > p["max_sl_pct"]:
                    self.last_status = f"engulfing found but SL {dist:.2f}% outside limits - still waiting"
                    return
                self.pending = {"dir": d, "sl": sl, "signal_ts": self.ts[-1], "anchor": s["L"], "next": s.get("next"),
                                "ext": s["ext"], "ext_a": s["ext_a"], "zone": s["zone"],
                                "fib": s["fib"], "deep": s["deep"]}
                self.events.append({"type": "signal", "sym": self.sym, "ts": self.ts[-1], "dir": d, "sl": sl})
                self.last_status = "ENGULFING - entering at next open"
                self.setup = None
                return
            self.last_status = f"{p['fib_entry'] * 100:.1f}% touched - waiting for {'bullish' if d == 1 else 'bearish'} engulfing"

    def _detect_breakout(self, a):
        c = self.C(a)
        pc = self.C(a - 1)
        p = self.p
        for k, z in enumerate(self.zones):
            # up: first close above zone top coming from the range below
            if c > z["top"] and pc <= z["top"]:
                lower = self.zones[k - 1]["top"] if k > 0 else -math.inf
                if self._ranged(a, z, lower, 1):
                    up = self.zones[k + 1] if k + 1 < len(self.zones) else None
                    return {"dir": 1, "zone": z, "next": up, "prev_zone": self.zones[k - 1] if k > 0 else None}
            if c < z["bot"] and pc >= z["bot"]:
                upper = self.zones[k + 1]["bot"] if k + 1 < len(self.zones) else math.inf
                if self._ranged(a, z, upper, -1):
                    dn = self.zones[k - 1] if k > 0 else None
                    return {"dir": -1, "zone": z, "next": dn, "prev_zone": self.zones[k + 1] if k + 1 < len(self.zones) else None}
        return None

    def _ranged(self, a, z, other, d):
        """walk back from a-1: skip closes inside the broken zone, then require >= range_min_bars
        consecutive closes strictly between the other zone and this zone."""
        j = a - 1
        lim = max(self.off + 1, a - 800)
        while j >= lim and z["bot"] <= self.C(j) <= z["top"]:
            j -= 1
        cnt = 0
        while j >= lim:
            cj = self.C(j)
            inside = (other < cj < z["bot"]) if d == 1 else (z["top"] < cj < other)
            if not inside:
                break
            cnt += 1
            if cnt >= self.p["range_min_bars"]:
                return True
            j -= 1
        return False

    def _new_setup(self, a, bo):
        d = bo["dir"]
        k = self.p["swing_pivot"]
        if d == 1:
            anchors = self._pivot_lows_before(a, k)
            anchors = [j for j in anchors if self.L(j) < self.C(a)]
        else:
            anchors = self._pivot_highs_before(a, k)
            anchors = [j for j in anchors if self.H(j) > self.C(a)]
        am = self.p.get("anchor_mode", "pivot")
        if am != "pivot":
            j0 = self._special_anchor(a, d, am, bo)
            if j0 is not None:
                v0 = self.L(j0) if d == 1 else self.H(j0)
                rest = [j for j in anchors if (self.L(j) < v0 if d == 1 else self.H(j) > v0)]
                anchors = [j0] + rest
        if not anchors:
            self.last_status = "breakout without a usable swing point"
            return None
        ext_a = a
        s = {"dir": d, "zone": bo["zone"], "next": bo["next"], "prev_zone": bo["prev_zone"], "bo_a": a,
             "bo_ts": self.ts[-1], "anchors": anchors, "anc_k": 0, "stage": "impulse", "pb": 0,
             "ext": self.H(a) if d == 1 else self.L(a), "ext_a": ext_a}
        # extreme of the move = max high since the anchor
        j0 = anchors[0]
        if d == 1:
            s["ext"] = max(self.H(x) for x in range(j0, a + 1))
        else:
            s["ext"] = min(self.L(x) for x in range(j0, a + 1))
        self.events.append({"type": "breakout", "sym": self.sym, "ts": self.ts[-1], "dir": d,
                            "zone": [bo["zone"]["bot"], bo["zone"]["top"]]})
        self.last_status = f"BREAKOUT {'up' if d == 1 else 'down'} - waiting for pullback"
        return s

    def _special_anchor(self, a, d, mode, bo):
        """leg_low: extreme since the last swing point against the move (start of the breakout leg).
        range_low: extreme of the whole A-B range stretch before the breakout."""
        lim = max(self.off + 1, a - 400)
        if mode == "leg_low":
            k = max(1, int(self.p["swing_pivot"]))
            opp = self._pivot_highs_before(a, k) if d == 1 else self._pivot_lows_before(a, k)
            j_start = opp[0] if opp else max(lim, a - 96)
            rng = range(max(j_start, lim), a + 1)
        else:
            z = bo["zone"]
            j = a - 1
            while j >= lim and z["bot"] <= self.C(j) <= z["top"]:
                j -= 1
            j_end = j
            while j >= lim and ((self.C(j) < z["bot"]) if d == 1 else (self.C(j) > z["top"])):
                j -= 1
                if a - j > 400:
                    break
            rng = range(max(j + 1, lim), a + 1)
        if not len(rng):
            return None
        if d == 1:
            return min(rng, key=lambda x: self.L(x))
        return max(rng, key=lambda x: self.H(x))

    def _levels(self, s):
        j = s["anchors"][s["anc_k"]]
        d = s["dir"]
        s["L"] = self.L(j) if d == 1 else self.H(j)
        s["L_a"] = j
        rng = s["ext"] - s["L"]
        s["fib"] = s["ext"] - self.p["fib_entry"] * rng
        s["deep"] = s["ext"] - self.p["fib_deep"] * rng

    def _reanchor(self, s):
        """take the swing point before the current one (it must be beyond the current one)."""
        d = s["dir"]
        cur = s["L"]
        while True:
            s["anc_k"] += 1
            if s["anc_k"] >= len(s["anchors"]):
                # look further back
                j_old = s["anchors"][-1]
                k = self.p["swing_pivot"]
                more = self._pivot_lows_before(j_old - 1 + k, k, below=cur) if d == 1 else \
                    self._pivot_highs_before(j_old - 1 + k, k, above=cur)
                more = [j for j in more if j < j_old]
                if not more:
                    return False
                s["anchors"].extend(more)
            j = s["anchors"][s["anc_k"]]
            v = self.L(j) if d == 1 else self.H(j)
            if (d == 1 and v < cur) or (d == -1 and v > cur):
                self._levels(s)
                self.events.append({"type": "reanchor", "sym": self.sym, "ts": self.ts[-1], "dir": d, "L": v})
                return True

    def _stop_price(self, s):
        buf = self.p["sl_buffer_atr"] * (self.atr15 or 0)
        return s["L"] - buf if s["dir"] == 1 else s["L"] + buf

    def _drop(self, why):
        if self.setup is not None:
            self.events.append({"type": "drop", "sym": self.sym, "ts": self.ts[-1], "why": why})
        self.setup = None
        self.last_status = f"setup cancelled: {why}"

    # ---- position handling
    def _fill(self, pend, t, price):
        d = pend["dir"]
        if (d == 1 and price <= pend["sl"]) or (d == -1 and price >= pend["sl"]):
            self.events.append({"type": "skip", "sym": self.sym, "ts": t, "why": "gapped through stop"})
            return
        self.pos = {"sym": self.sym, "dir": d, "entry_ts": int(t), "entry": float(price), "sl": float(pend["sl"]),
                    "sl0": float(pend["sl"]), "anchor": pend["anchor"], "signal_ts": pend["signal_ts"],
                    "struct": float(pend["ext"]), "struct_ts": int(self.ts[self._i(pend["ext_a"])]) if self._i(pend["ext_a"]) >= 0 else int(t),
                    "struct_a": pend["ext_a"], "last_break_a": None, "trail": [], "funding": [],
                    "zone": pend["zone"], "fib": pend["fib"], "mfe": 0.0, "mae": 0.0, "tp": None}
        em = self.p.get("exit_mode", "trail")
        if em == "trail_zone_tp" and pend.get("next"):
            nz = pend["next"]
            tp = nz["bot"] if d == 1 else nz["top"]
            if (d == 1 and tp > price) or (d == -1 and tp < price):
                self.pos["tp"] = float(tp)
        elif em == "trail_rr_tp":
            self.pos["tp"] = float(price + d * self.p.get("tp_r", 3.0) * abs(price - pend["sl"]))
        self.events.append({"type": "open", "sym": self.sym, "ts": int(t), "pos": copy.deepcopy(self.pos)})

    def fill_pending_now(self, t, price):
        """live: fill the pending entry immediately at the open of the new candle."""
        if self.pending is not None:
            self.events = []
            self._fill(self.pending, t, price)
            self.pending = None
            return self.events
        return []

    def _check_stop(self, t, o, h, l):
        ps = self.pos
        d = ps["dir"]
        hit = (l <= ps["sl"]) if d == 1 else (h >= ps["sl"])
        risk = abs(ps["entry"] - ps["sl0"])
        if risk > 0:
            ps["mfe"] = max(ps["mfe"], ((h - ps["entry"]) if d == 1 else (ps["entry"] - l)) / risk)
            ps["mae"] = min(ps["mae"], ((l - ps["entry"]) if d == 1 else (ps["entry"] - h)) / risk)
        if hit:  # stop first if stop and target are both inside the bar (worst case)
            px = min(o, ps["sl"]) if d == 1 else max(o, ps["sl"])  # gap through the stop fills at the open
            self.close(t, px, "trail stop" if ps["trail"] else "stop loss")
            return
        tp = ps.get("tp")
        if tp and ((d == 1 and h >= tp) or (d == -1 and l <= tp)):
            px = max(o, tp) if d == 1 else min(o, tp)  # resting limit: fills at the target or better
            ps["exit_maker"] = True
            self.close(t, px, "take profit")

    def close(self, t, price, reason):
        ps = self.pos
        if ps is None:
            return
        ps["exit_ts"] = int(t)
        ps["exit"] = float(price)
        ps["reason"] = reason
        self.events.append({"type": "close", "sym": self.sym, "ts": int(t), "pos": copy.deepcopy(ps)})
        self.pos = None
        self.setup = None

    def _trail(self, a):
        ps = self.pos
        d = ps["dir"]
        c = self.C(a)
        k = int(self.p["trail_pivot"])
        buf = self.p["sl_buffer_atr"] * (self.atr15 or 0)
        # find a fresh structure extreme after the last break
        if ps["struct"] is None:
            lb = ps["last_break_a"]
            j = a - k
            if j - k >= self.off and j > lb:
                if d == 1:
                    hj = self.H(j)
                    if all(hj >= self.H(x) for x in range(j - k, j + k + 1)):
                        ps["struct"], ps["struct_a"] = hj, j
                else:
                    lj = self.L(j)
                    if all(lj <= self.L(x) for x in range(j - k, j + k + 1)):
                        ps["struct"], ps["struct_a"] = lj, j
        if ps["struct"] is not None:
            broke = (c > ps["struct"]) if d == 1 else (c < ps["struct"])
            if broke:
                j0 = max(ps["struct_a"], self.off)
                if d == 1:
                    lo = min(self.L(x) for x in range(j0, a + 1))
                    new = lo - buf
                    improve = new > ps["sl"] and new < c
                else:
                    hi = max(self.H(x) for x in range(j0, a + 1))
                    new = hi + buf
                    improve = new < ps["sl"] and new > c
                if improve:
                    ps["sl"] = float(new)
                    ps["trail"].append([int(self.ts[-1]), float(new)])
                    self.events.append({"type": "trail", "sym": self.sym, "ts": int(self.ts[-1]), "sl": float(new)})
                ps["struct"] = None
                ps["last_break_a"] = a
        self.last_status = "in position - trailing below structure" if d == 1 else "in position - trailing above structure"

    # ---- status for the dashboard
    def status(self):
        p = self.p
        s = self.setup
        st = {"symbol": self.sym, "text": self.last_status, "zones": self.zones, "zones_ts": self.zones_ts,
              "price": self.c[-1] if self.c else None, "atr15": self.atr15, "bars": self.n,
              "pending": self.pending is not None}
        nodes = [{"k": "zones", "l": "4h zones", "ok": bool(self.zones), "v": f"{len(self.zones)} active"}]
        if self.c and self.zones:
            where, k = self._zone_index(self.c[-1])
            if where == "in":
                rng = f"inside zone {k + 1}"
            else:
                lo = self.zones[k - 1]["top"] if k > 0 else None
                hi = self.zones[k]["bot"] if k < len(self.zones) else None
                rng = "between zones" if lo and hi else ("above all zones" if lo else "below all zones")
                st["range_lo"], st["range_hi"] = lo, hi
            st["range"] = rng
        if self.pos is not None:
            stage = 6
        elif s is None:
            stage = 1
        else:
            stage = {"impulse": 2, "pullback": 3, "touched": 4}.get(s["stage"], 1)
            if s["stage"] == "impulse" and s["pb"] > 0:
                stage = 2
        labels = [("range", "range A-B"), ("breakout", "close through B"), ("pullback", f"{p['pullback_bars']}-candle pullback"),
                  ("fib", f"{p['fib_entry'] * 100:.1f}% touch"), ("engulf", "engulfing"), ("pos", "in position")]
        for n_, (key, lab) in enumerate(labels, start=1):
            nodes.append({"k": key, "l": lab, "ok": stage > n_ or (stage == 6 and n_ == 6),
                          "act": stage == n_})
        st["nodes"] = nodes
        if s is not None:
            st["setup"] = {"dir": s["dir"], "stage": s["stage"], "zone": s["zone"], "pb": s["pb"],
                           "ext": s["ext"], "bo_ts": s["bo_ts"], "L": s.get("L"), "fib": s.get("fib"),
                           "deep": s.get("deep"), "bars_left": p["setup_max_bars"] - (self.off + len(self.c) - 1 - s["bo_a"])}
        if self.pos is not None:
            ps = self.pos
            st["position"] = {k: ps.get(k) for k in ("dir", "entry", "entry_ts", "sl", "sl0", "struct", "trail", "tp")}
        return st

    # ---- persistence of an open position (paper trader)
    def export_pos(self):
        return copy.deepcopy(self.pos) if self.pos else None

    def import_pos(self, ps):
        """re-attach a stored live position after warm-up. struct indices are re-mapped by timestamp."""
        self.setup = None
        self.pending = None
        if not ps:
            self.pos = None
            return
        ps = copy.deepcopy(ps)
        ts_list = self.ts

        def a_of(t):
            if t is None:
                return None
            for i in range(len(ts_list) - 1, -1, -1):
                if ts_list[i] <= t:
                    return self.off + i
            return self.off

        ps["struct_a"] = a_of(ps.get("struct_ts")) if ps.get("struct") is not None else None
        ps["last_break_a"] = a_of(ps.get("last_break_ts"))
        self.pos = ps

    def export_pos_for_store(self):
        ps = self.export_pos()
        if ps:
            sa = ps.get("struct_a")
            ps["struct_ts"] = int(self.ts[sa - self.off]) if sa is not None and 0 <= sa - self.off < len(self.ts) else None
            lb = ps.get("last_break_a")
            ps["last_break_ts"] = int(self.ts[lb - self.off]) if lb is not None and 0 <= lb - self.off < len(self.ts) else None
        return ps


# ---------------------------------------------------------------- simulation (backtest / optimizer)
STEP_LEAD = 10 * DAY  # 15m stepping starts this long before the first counted trade


def run_symbol(sym, data, params, cfg, start_ms, funding=None, collect_chart=False, persist_cache=True):
    """Runs the engine over the data. Candles far before start_ms only feed the 4h zone history
    (preload, no look-ahead: they are all in the past). 15m stepping starts STEP_LEAD before
    start_ms so the 15m structure is warm. Trades whose ENTRY is before start_ms are discarded.
    `params` must already be the symbol's params (see params_for)."""
    zc = ZoneCache(sym, params, persist=persist_cache)
    eng = Engine(sym, params, cfg, zone_cache=zc)
    ts, o, h, l, c = data["ts"], data["o"], data["h"], data["l"], data["c"]
    step_from = int(start_ms) - STEP_LEAD
    step_from -= step_from % H4  # begin on a 4h boundary
    i0 = int(np.searchsorted(ts, step_from))
    if i0 > 0:
        bars = aggregate_4h(ts[:i0], o[:i0], h[:i0], l[:i0], c[:i0])
        eng.preload_4h(bars)
    trades, marks = [], []
    zone_hist = []
    if collect_chart and eng.zones_ts is not None:
        zone_hist.append({"ts": eng.zones_ts, "zones": [[z["bot"], z["top"]] for z in eng.zones]})
    fund_ts = sorted(funding) if funding else []
    fk = 0
    for i in range(i0, len(ts)):
        t = int(ts[i])
        if eng.pos is not None and fund_ts:
            while fk < len(fund_ts) and fund_ts[fk] < t:
                fk += 1
            while fk < len(fund_ts) and fund_ts[fk] < t + M15:
                ft = fund_ts[fk]
                if eng.pos is not None and eng.pos["entry_ts"] <= ft:
                    eng.pos["funding"].append([ft, funding[ft], float(o[i])])
                fk += 1
        z_before = eng.zones_ts
        evs = eng.step(t, float(o[i]), float(h[i]), float(l[i]), float(c[i]))
        for e in evs:
            if e["type"] == "close":
                ps = e["pos"]
                if ps["entry_ts"] >= start_ms:
                    trades.append(trade_record(ps))
            if collect_chart and t >= start_ms and e["type"] in ("breakout", "touch", "reanchor", "drop"):
                marks.append(e)
        if collect_chart and eng.zones_ts != z_before and t >= start_ms - H4:
            zone_hist.append({"ts": eng.zones_ts, "zones": [[z["bot"], z["top"]] for z in eng.zones]})
    zc.save()
    open_pos = eng.export_pos()
    return {"trades": trades, "marks": marks, "zone_hist": zone_hist, "engine": eng,
            "open": open_pos if open_pos and open_pos["entry_ts"] >= start_ms else None}


def trade_record(ps):
    return {"sym": ps["sym"], "dir": ps["dir"], "side": "LONG" if ps["dir"] == 1 else "SHORT",
            "entry_ts": ps["entry_ts"], "entry": ps["entry"], "sl0": ps["sl0"], "sl": ps["sl"],
            "exit_ts": ps.get("exit_ts"), "exit": ps.get("exit"), "reason": ps.get("reason"),
            "trail": ps.get("trail", []), "funding": ps.get("funding", []), "signal_ts": ps.get("signal_ts"),
            "mfe": ps.get("mfe", 0.0), "mae": ps.get("mae", 0.0), "tp": ps.get("tp"),
            "exit_maker": bool(ps.get("exit_maker"))}


def unit_r(t, fees):
    """R multiple of a trade independent of the position size (fees and funding included).
    size = risk / (stop distance + round-trip taker fees) -> a full stop is exactly -1 R."""
    tk = fees["taker"]
    d = t["dir"]
    risk_unit = abs(t["entry"] - t["sl0"]) + tk * (t["entry"] + t["sl0"])
    if risk_unit <= 0:
        return 0.0
    ex = t["exit"]
    gross = d * (ex - t["entry"])
    fee = tk * t["entry"] + (fees.get("maker", tk) if t.get("exit_maker") else tk) * ex
    fund = sum(d * r * px for _, r, px in t.get("funding", [])) if fees.get("include_funding", True) else 0.0
    return (gross - fee - fund) / risk_unit


# ---------------------------------------------------------------- account replay
def replay(trades, cfg, settings, symbols, open_positions=None, marks=None, start_ms=None, end_ms=None):
    """Replays size-independent trade records into a $ account for the given symbols.
    Risk in $ = risk% x realized balance at the moment of entry -> two positions opened while the
    balance is unchanged carry the same absolute risk (combined account). Kelly sizing (if enabled)
    only uses trades CLOSED before the entry (no look-ahead)."""
    acc = cfg["account"]
    fees = cfg["fees"]
    tk = fees["taker"]
    bal = float(settings.get("start_balance", acc["start_balance"]))
    start_bal = bal
    base_risk = float(settings.get("risk_pct", acc["risk_pct"])) / 100.0
    kelly_on = bool(settings.get("kelly_sizing", False))
    kfrac, kmin, kmax = acc["kelly_fraction"], acc["kelly_min_trades"], acc["kelly_max_risk_pct"] / 100.0
    maxlev = acc["max_leverage"]
    sel = [t for t in trades if t["sym"] in symbols]
    opens = [dict(p) for p in (open_positions or []) if p and p["sym"] in symbols]
    ev = []
    for idx, t in enumerate(sel):
        ev.append((t["entry_ts"], 1, idx, "t"))
        ev.append((t["exit_ts"], 2, idx, "t"))
    for idx, p in enumerate(opens):
        ev.append((p["entry_ts"], 1, idx, "o"))
    ev.sort(key=lambda x: (x[0], x[1]))
    closed_r = []
    sizes = {}
    out_trades = []
    curve = [[int(start_ms or (ev[0][0] if ev else now_ms())), bal]]
    for ts_, kind, idx, src in ev:
        rec = sel[idx] if src == "t" else opens[idx]
        if kind == 1:
            r_pct = base_risk
            if kelly_on and len(closed_r) >= kmin:
                kf = kelly(closed_r)["kelly"]
                r_pct = min(kmax, max(0.0025, kf * kfrac)) if kf > 0 else 0.0025
            risk_usd = bal * r_pct
            inst = cfg["instruments"].get(rec["sym"], {"qty_step": 0.001, "min_qty": 0.001, "min_notional": 5})
            unit = abs(rec["entry"] - rec["sl0"]) + tk * (rec["entry"] + rec["sl0"])
            qty = risk_usd / unit if unit > 0 else 0
            cap = maxlev * bal / rec["entry"]
            qty = min(qty, cap)
            step = inst["qty_step"]
            qty = math.floor(qty / step + 1e-9) * step
            ok = qty >= inst["min_qty"] and qty * rec["entry"] >= inst.get("min_notional", 5) and bal > 0
            sizes[(src, idx)] = {"qty": qty if ok else 0.0, "risk_usd": risk_usd, "risk_pct": r_pct * 100,
                                 "bal_at_entry": bal, "ok": ok}
        else:
            sz = sizes.get((src, idx))
            if not sz or not sz["ok"]:
                out_trades.append(dict(rec, skipped=True, qty=0, pnl=0, r=0, fees=0, funding_usd=0,
                                       sl_pct=abs(rec["entry"] - rec["sl0"]) / rec["entry"] * 100,
                                       risk_usd=sz["risk_usd"] if sz else 0, notional=0, lev=0, bal_after=bal))
                continue
            q = sz["qty"]
            d = rec["dir"]
            gross = d * (rec["exit"] - rec["entry"]) * q
            fee = (tk * rec["entry"] + (fees.get("maker", tk) if rec.get("exit_maker") else tk) * rec["exit"]) * q
            fund = sum(d * r * px for _, r, px in rec.get("funding", [])) * q if fees.get("include_funding", True) else 0.0
            pnl = gross - fee - fund
            bal += pnl
            r_mult = pnl / sz["risk_usd"] if sz["risk_usd"] > 0 else 0
            closed_r.append(r_mult)
            out_trades.append(dict(rec, skipped=False, qty=q, pnl=pnl, r=r_mult, fees=fee, funding_usd=fund,
                                   risk_usd=sz["risk_usd"], risk_pct=sz["risk_pct"], notional=q * rec["entry"],
                                   lev=q * rec["entry"] / sz["bal_at_entry"] if sz["bal_at_entry"] > 0 else 0,
                                   sl_pct=abs(rec["entry"] - rec["sl0"]) / rec["entry"] * 100, bal_after=bal,
                                   r_unit=unit_r(rec, fees)))
            curve.append([int(rec["exit_ts"]), bal])
    # open positions: size + unrealized
    out_open = []
    unreal = 0.0
    for idx, p in enumerate(opens):
        sz = sizes.get(("o", idx), {"qty": 0, "risk_usd": 0, "ok": False})
        mk = (marks or {}).get(p["sym"])
        q = sz["qty"]
        u = 0.0
        if mk and q:
            u = p["dir"] * (mk - p["entry"]) * q - tk * p["entry"] * q
            u -= sum(p["dir"] * r * px for _, r, px in p.get("funding", [])) * q
        unreal += u
        risk_now = p["dir"] * (p["entry"] - p["sl"]) * q + tk * (p["entry"] + p["sl"]) * q
        out_open.append(dict(p, qty=q, risk_usd=sz["risk_usd"], notional=q * p["entry"], mark=mk, upnl=u,
                             r_open=u / sz["risk_usd"] if sz["risk_usd"] else 0, risk_now=risk_now,
                             lev=q * p["entry"] / sz.get("bal_at_entry", bal) if sz.get("bal_at_entry") else 0,
                             sl_pct=abs(p["entry"] - p["sl0"]) / p["entry"] * 100, ok=sz["ok"]))
    if end_ms:
        curve.append([int(end_ms), bal])
    return {"trades": out_trades, "open": out_open, "balance": bal, "start_balance": start_bal,
            "curve": curve, "unrealized": unreal}


def kelly(rs):
    rs = [r for r in rs if r is not None]
    if len(rs) < 2:
        return {"kelly": 0.0, "half": 0.0, "w": 0.0, "b": 0.0}
    wins = [r for r in rs if r > 0]
    losses = [-r for r in rs if r <= 0]
    w = len(wins) / len(rs)
    aw = sum(wins) / len(wins) if wins else 0.0
    al = sum(losses) / len(losses) if losses else 0.0
    if al <= 0:
        return {"kelly": 1.0 if aw > 0 else 0.0, "half": 0.5, "w": w, "b": math.inf}
    b = aw / al
    k = w - (1 - w) / b if b > 0 else -1.0
    return {"kelly": k, "half": k / 2, "w": w, "b": b}


# ---------------------------------------------------------------- stats + journal
def compute_stats(rep, cfg, settings, start_ms, end_ms):
    tr = [t for t in rep["trades"] if not t.get("skipped")]
    sb = rep["start_balance"]
    bal = rep["balance"]
    n = len(tr)
    wins = [t for t in tr if t["pnl"] > 0]
    losses = [t for t in tr if t["pnl"] <= 0]
    rs = [t["r"] for t in tr]
    cum = np.cumsum(rs) if rs else np.array([0.0])
    peak_r = np.maximum.accumulate(np.concatenate([[0.0], cum]))
    dd_r = float(np.max(peak_r - np.concatenate([[0.0], cum]))) if rs else 0.0
    eq = [p[1] for p in rep["curve"]]
    peak, mdd = -1e18, 0.0
    for v in eq:
        peak = max(peak, v)
        if peak > 0:
            mdd = max(mdd, (peak - v) / peak)
    # daily drawdown (worst calendar day, local tz)
    z = tz(cfg)
    day_pnl = {}
    day_start = {}
    for t in tr:
        dkey = datetime.fromtimestamp(t["exit_ts"] / 1000, z).strftime("%Y-%m-%d")
        day_pnl[dkey] = day_pnl.get(dkey, 0.0) + t["pnl"]
        day_start.setdefault(dkey, t["bal_after"] - t["pnl"])
    max_daily = 0.0
    for k, v in day_pnl.items():
        if v < 0 and day_start[k] > 0:
            max_daily = max(max_daily, -v / day_start[k])
    cw = cl = mcw = mcl = 0
    for t in tr:
        if t["pnl"] > 0:
            cw, cl = cw + 1, 0
        else:
            cl, cw = cl + 1, 0
        mcw, mcl = max(mcw, cw), max(mcl, cl)
    gw = sum(t["pnl"] for t in wins)
    gl = -sum(t["pnl"] for t in losses)
    days = max((end_ms - start_ms) / DAY, 1e-9)
    years = days / 365.25
    cagr = ((bal / sb) ** (1 / years) - 1) * 100 if sb > 0 and bal > 0 and years > 0.02 else \
        ((bal / sb - 1) * 100 / years if years > 0 else 0)
    avg_w_r = sum(t["r"] for t in wins) / len(wins) if wins else 0
    avg_l_r = sum(t["r"] for t in losses) / len(losses) if losses else 0
    fees_total = sum(t["fees"] for t in tr)
    k = kelly(rs)
    tk = cfg["fees"]["taker"] * 100
    mk = cfg["fees"]["maker"] * 100
    return {
        "total_trades": n, "wins": len(wins), "losses": len(losses),
        "winrate": len(wins) / n * 100 if n else 0.0,
        "cum_r": float(sum(rs)), "max_dd_r": dd_r,
        "account": bal, "equity": bal + rep.get("unrealized", 0.0), "start_balance": sb,
        "risk_pct": float(settings.get("risk_pct", cfg["account"]["risk_pct"])),
        "kelly_sizing": bool(settings.get("kelly_sizing")),
        "fee_setup": f"Bybit taker {tk:.3f}% / maker {mk:.3f}% (market in, stop-market out)",
        "days": days, "net_pnl": bal - sb, "net_pct": (bal / sb - 1) * 100 if sb else 0,
        "profit_factor": (gw / gl) if gl > 0 else (math.inf if gw > 0 else 0.0),
        "annual_return": cagr,
        "avg_win": gw / len(wins) if wins else 0.0, "avg_loss": -gl / len(losses) if losses else 0.0,
        "avg_win_r": avg_w_r, "avg_loss_r": avg_l_r,
        "max_dd_pct": mdd * 100, "max_daily_dd_pct": max_daily * 100,
        "max_consec_wins": mcw, "max_consec_losses": mcl,
        "avg_rtr": (avg_w_r / abs(avg_l_r)) if avg_l_r else (avg_w_r if wins else 0.0),
        "avg_r": float(np.mean(rs)) if rs else 0.0,
        "total_fees": fees_total, "avg_fees": fees_total / n if n else 0.0,
        "total_funding": sum(t["funding_usd"] for t in tr),
        "avg_order": sum(t["notional"] for t in tr) / n if n else 0.0,
        "avg_leverage": sum(t["lev"] for t in tr) / n if n else 0.0,
        "avg_time_h": sum((t["exit_ts"] - t["entry_ts"]) for t in tr) / n / 3_600_000 if n else 0.0,
        "avg_sl_pct": sum(t["sl_pct"] for t in tr) / n if n else 0.0,
        "kelly": k["kelly"] * 100, "half_kelly": k["half"] * 100,
        "skipped": sum(1 for t in rep["trades"] if t.get("skipped")),
        "open_positions": len(rep["open"]), "unrealized": rep.get("unrealized", 0.0),
        "longs": sum(1 for t in tr if t["dir"] == 1), "shorts": sum(1 for t in tr if t["dir"] == -1),
        "best_r": max(rs) if rs else 0.0, "worst_r": min(rs) if rs else 0.0,
    }


def journal(rep, cfg, start_ms, end_ms):
    """Balances per year / month / ISO-week / day (local timezone). Each row knows its parent."""
    z = tz(cfg)
    tr = sorted([t for t in rep["trades"] if not t.get("skipped")], key=lambda t: t["exit_ts"])
    sb = rep["start_balance"]
    d0 = datetime.fromtimestamp(start_ms / 1000, z).date()
    d1 = datetime.fromtimestamp(end_ms / 1000, z).date()
    if (d1 - d0).days > 3700:
        d0 = d1 - timedelta(days=3700)
    days = {}
    bal = sb
    ti = 0
    cur = d0
    while cur <= d1:
        key = cur.isoformat()
        st = bal
        pnl, n, w, r = 0.0, 0, 0, 0.0
        while ti < len(tr) and datetime.fromtimestamp(tr[ti]["exit_ts"] / 1000, z).date() == cur:
            t = tr[ti]
            pnl += t["pnl"]
            r += t["r"]
            n += 1
            w += 1 if t["pnl"] > 0 else 0
            ti += 1
        while ti < len(tr) and datetime.fromtimestamp(tr[ti]["exit_ts"] / 1000, z).date() < cur:
            ti += 1
        bal = st + pnl
        iso = cur.isocalendar()
        days[key] = {"key": key, "label": cur.strftime("%a %d %b"), "start": st, "end": bal, "pnl": pnl, "r": r,
                     "trades": n, "wins": w, "week": f"{iso[0]}-W{iso[1]:02d}", "month": cur.strftime("%Y-%m"),
                     "year": cur.strftime("%Y")}
        cur += timedelta(days=1)

    def group(field, label_fn, parent_fn):
        g = {}
        for d in days.values():
            k = d[field]
            if k not in g:
                g[k] = {"key": k, "label": label_fn(k, d), "start": d["start"], "end": d["end"], "pnl": 0.0, "r": 0.0,
                        "trades": 0, "wins": 0, "parent": parent_fn(d), "first": d["key"]}
            x = g[k]
            x["end"] = d["end"]
            x["pnl"] += d["pnl"]
            x["r"] += d["r"]
            x["trades"] += d["trades"]
            x["wins"] += d["wins"]
        return list(g.values())

    def wk_label(k, d):
        y, w = k.split("-W")
        mon = datetime.fromisocalendar(int(y), int(w), 1).date()
        return f"W{w} · {mon.strftime('%d %b')} - {(mon + timedelta(days=6)).strftime('%d %b')}"

    years = group("year", lambda k, d: k, lambda d: None)
    months = group("month", lambda k, d: datetime.strptime(k, "%Y-%m").strftime("%B %Y"), lambda d: d["year"])
    weeks = group("week", wk_label, lambda d: d["month"])
    dl = list(days.values())
    for d in dl:
        d["parent"] = d["week"]
    for rows in (years, months, weeks, dl):
        for x in rows:
            x["pct"] = (x["end"] / x["start"] - 1) * 100 if x["start"] else 0.0
    return {"year": years, "month": months, "week": weeks, "day": dl}


def account_bundle(trades, open_positions, cfg, settings, symbols, start_ms, end_ms, marks=None,
                   with_trades=True):
    rep = replay(trades, cfg, settings, symbols, open_positions, marks, start_ms, end_ms)
    st = compute_stats(rep, cfg, settings, start_ms, end_ms)
    jr = journal(rep, cfg, start_ms, end_ms)
    curve = rep["curve"]
    if rep["open"] and end_ms:
        curve = curve + [[int(end_ms), rep["balance"] + rep["unrealized"]]]
    out = {"stats": st, "curve": curve, "open": rep["open"], "journal": jr}
    if with_trades:
        out["trades"] = rep["trades"]
    return out


ACCOUNTS = {"BTC": ["BTCUSDT"], "SOL": ["SOLUSDT"], "ALL": ["BTCUSDT", "SOLUSDT"]}


def fmt_discord_trade(kind, t, bundle_rows):
    """bundle_rows: list of (account_name, sized_row)"""
    side = "🟢 LONG" if t["dir"] == 1 else "🔴 SHORT"
    lines = []
    if kind == "open":
        title = f"◆ LONDON · {t['sym']} · {side} OPENED"
        lines.append(f"Entry `{t['entry']:.6g}`  SL `{t['sl']:.6g}`  ({abs(t['entry'] - t['sl']) / t['entry'] * 100:.2f}%)")
        for name, r in bundle_rows:
            lines.append(f"**{name}** qty `{r.get('qty', 0):.6g}` · notional `${r.get('notional', 0):.2f}` · risk `${r.get('risk_usd', 0):.2f}`")
        color = 0x1fe0a0 if t["dir"] == 1 else 0xff4d6d
    elif kind == "trail":
        title = f"◆ LONDON · {t['sym']} · stop trailed"
        lines.append(f"New SL `{t['sl']:.6g}` (entry `{t['entry']:.6g}`)")
        color = 0x7cc4ff
    else:
        title = f"◆ LONDON · {t['sym']} · {side} CLOSED ({t.get('reason', '')})"
        lines.append(f"Entry `{t['entry']:.6g}` → Exit `{t['exit']:.6g}`")
        for name, r in bundle_rows:
            lines.append(f"**{name}** PnL `${r.get('pnl', 0):+.2f}` · `{r.get('r', 0):+.2f} R` · fees `${r.get('fees', 0):.3f}`")
        color = 0x1fe0a0 if any(r.get("pnl", 0) > 0 for _, r in bundle_rows) else 0xff4d6d
    return {"embeds": [{"title": title, "description": "\n".join(lines), "color": color,
                        "timestamp": datetime.now(timezone.utc).isoformat()}]}
