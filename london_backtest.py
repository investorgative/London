#!/usr/bin/env python3
"""
LONDON - backtester + optimizer (uses the exact same Engine as the paper trader).

No look-ahead:
  * 4h zones at time T are built only from 4h candles that CLOSED at or before T
  * 15m signals use closed candles only, entries fill at the NEXT candle's open
  * stops are checked inside the bar with the stop valid at the bar's open (worst case first)
  * Kelly sizing only uses trades that closed before the new entry
  * your hand-drawn reference zones are never read here

CLI:
  ./venv/bin/python london_backtest.py --days 365            # backtest with config.json params
  ./venv/bin/python london_backtest.py --optimize --combos 150
  ./venv/bin/python london_backtest.py --warm                # pre-compute the zone cache (setup)
"""
import argparse
import copy
import json
import multiprocessing as mp
import os
import random
import time

import numpy as np

import london_core as core

BT_PATH = os.path.join(core.DATA_DIR, "backtest_last.json")
OPT_PATH = os.path.join(core.DATA_DIR, "optimizer_last.json")


def warmup_ms(params):
    """history needed before the first counted trade: zone lookback + 15m lead."""
    return int(params.get("zone_lookback_days", 730)) * core.DAY + core.STEP_LEAD + 2 * core.DAY


def load_data(cfg, days, lookback_days, progress=None, symbols=None):
    api = core.Bybit(cfg)
    store = core.Store(api)
    end = core.now_ms() - core.now_ms() % core.M15 - 1
    start = end - int(days * core.DAY)
    data_start = start - warmup_ms({"zone_lookback_days": lookback_days})
    out = {}
    syms = symbols or cfg["symbols"]
    for n, sym in enumerate(syms):
        def pr(x, n=n):
            if progress:
                progress("download " + sym, (n + x) / len(syms) * 0.3)
        try:
            store.ensure(sym, data_start, end, progress=pr)
        except Exception as e:  # Bybit unreachable: use what is cached, never block a backtest
            core.logging.getLogger("london").warning(f"{sym}: download failed, using cached candles ({e})")
        d = store.load(sym, data_start, end)
        if not len(d["ts"]):
            raise RuntimeError(f"no candles for {sym} (no cache and Bybit unreachable)")
        fund = {}
        if cfg["fees"].get("include_funding", True):
            try:
                store.ensure_funding(sym, data_start, end)
            except Exception as e:
                core.logging.getLogger("london").warning(f"funding download failed {sym}: {e}")
            fund = store.load_funding(sym, data_start, end)
        out[sym] = {"data": d, "funding": fund}
    last = min(int(v["data"]["ts"][-1]) for v in out.values()) + core.M15 - 1
    if last < end:  # cached data ends earlier -> shift the window so every symbol has data
        end = last
        start = end - int(days * core.DAY)
    first = max(int(v["data"]["ts"][0]) for v in out.values())
    if start < first + 30 * core.DAY:  # not enough history for the requested period
        start = first + 30 * core.DAY
    return out, start, end


def max_lookback(cfg, base=None, extra=()):
    lbs = [core.params_for(cfg, s, base).get("zone_lookback_days", 730) for s in cfg["symbols"]]
    return max(list(lbs) + list(extra))


def run_backtest(cfg, days, settings, params=None, progress=None):
    """params: optional custom common block (may contain "_sym": {sym: overrides})."""
    t0 = time.time()
    data, start, end = load_data(cfg, days, max_lookback(cfg, params), progress)
    trades, opens, charts, used = [], [], {}, {}
    syms = cfg["symbols"]
    for n, sym in enumerate(syms):
        if progress:
            progress("simulate " + sym + " (first run builds the zone cache)", 0.3 + n / len(syms) * 0.65)
        p = core.params_for(cfg, sym, params)
        used[sym] = p
        r = core.run_symbol(sym, data[sym]["data"], p, cfg, start, data[sym]["funding"], collect_chart=True)
        trades += r["trades"]
        if r["open"]:
            opens.append(r["open"])
        d = data[sym]["data"]
        eng = r["engine"]
        charts[sym] = {"marks": r["marks"][-400:], "zone_hist": r["zone_hist"], "zones": eng.zones,
                       "last": float(d["c"][-1]) if len(d["c"]) else None}
    marks = {s: charts[s]["last"] for s in syms}
    accounts = {}
    for name, ss in core.ACCOUNTS.items():
        accounts[name] = core.account_bundle(trades, opens, cfg, settings, ss, start, end, marks)
    res = {"meta": {"days": round((end - start) / core.DAY, 1), "start": start, "end": end, "settings": settings,
                    "params": params or cfg["strategy"], "params_used": used,
                    "run_at": core.now_ms(), "secs": time.time() - t0, "raw_trades": len(trades),
                    "lab": lab_summary(trades, cfg, start, end)},
           "accounts": accounts, "raw": {"trades": trades, "open": opens}, "chart": charts}
    if progress:
        progress("done", 1.0)
    return res


def r_metrics(rs):
    if not rs:
        return {"n": 0, "r": 0.0, "wr": 0.0, "pf": 0.0, "dd": 0.0, "avg": 0.0}
    c = np.cumsum(rs)
    pk = np.maximum.accumulate(np.concatenate([[0.0], c]))
    gw = sum(r for r in rs if r > 0)
    gl = -sum(r for r in rs if r <= 0)
    return {"n": len(rs), "r": float(sum(rs)), "wr": sum(1 for r in rs if r > 0) / len(rs) * 100,
            "pf": gw / gl if gl > 0 else (99.0 if gw > 0 else 0.0),
            "dd": float(np.max(pk - np.concatenate([[0.0], c]))), "avg": float(np.mean(rs))}


def lab_summary(trades, cfg, start, end):
    """size-independent R summary: whole period, first half vs second half, per symbol."""
    fees = cfg["fees"]
    mid = start + (end - start) // 2

    def m(ts):
        return r_metrics([core.unit_r(t, fees) for t in sorted(ts, key=lambda t: t["exit_ts"])])
    out = {"all": m(trades), "h1": m([t for t in trades if t["entry_ts"] < mid]),
           "h2": m([t for t in trades if t["entry_ts"] >= mid])}
    for sym in cfg["symbols"]:
        out[sym] = m([t for t in trades if t["sym"] == sym])
    return out


# ---------------------------------------------------------------- optimizer (per symbol)
_G = {}


def _init(data, cfg, split, start):
    _G["data"], _G["cfg"], _G["split"], _G["start"] = data, cfg, split, start


def _zone_job(args):
    """pre-compute one zone timeline (symbol x zone variant) into the disk cache."""
    sym, p = args
    try:
        cfg = _G["cfg"]
        q = dict(p)
        q["setup_max_bars"] = 1  # irrelevant for zones; keeps the run cheap
        core.run_symbol(sym, _G["data"][sym]["data"], q, cfg, _G["start"], None)
        return {"sym": sym, "key": core.zone_key(p)}
    except Exception as e:
        return {"sym": sym, "error": str(e)}


def _eval(job):
    sym, params = job
    cfg = _G["cfg"]
    fees = cfg["fees"]
    try:
        r = core.run_symbol(sym, _G["data"][sym]["data"], params, cfg, _G["start"], _G["data"][sym]["funding"],
                            persist_cache=False)
    except Exception as e:
        return {"sym": sym, "params": params, "error": str(e)}
    trades = sorted(r["trades"], key=lambda t: t["exit_ts"])
    split = _G["split"]
    rs_tr = [core.unit_r(t, fees) for t in trades if t["entry_ts"] < split]
    rs_te = [core.unit_r(t, fees) for t in trades if t["entry_ts"] >= split]
    return {"sym": sym, "params": params, "train": r_metrics(rs_tr), "test": r_metrics(rs_te),
            "all": r_metrics(rs_tr + rs_te)}


def zone_variants(cfg, sym, n, seed=0):
    """small set of zone parameter variants around the symbol's calibrated ladder."""
    base = core.params_for(cfg, sym)
    zs = cfg["optimizer"].get("zone_space", {})
    rnd = random.Random(seed + hash(sym) % 1000)
    out, seen = [], set()

    def make(ch):
        p = dict(base)
        for k, v in ch.items():
            if k == "zone_lookback_days":
                p[k] = int(v)
            elif k == "zone_min_touches":
                p[k] = max(2, int(base[k]) + int(v))
            else:
                p[k] = round(float(base[k]) * float(v), 4)
        return p
    variants = [{}]
    keys = list(zs)
    tries = 0
    while len(variants) < n and tries < 500 and keys:
        tries += 1
        ch = {k: rnd.choice(zs[k]) for k in keys}
        variants.append(ch)
    for ch in variants:
        p = make(ch)
        key = core.zone_key(p)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def sample_entry(space, base, n, rnd):
    keys = list(space)
    out = []
    for _ in range(n):
        p = dict(base)
        for k in keys:
            p[k] = rnd.choice(space[k])
        out.append(p)
    return out


def optimize(cfg, days, combos, progress=None, stop_flag=None, lab=None, symbols=None, zone_n=None, seed=None):
    oc = cfg["optimizer"]
    syms = symbols or cfg["symbols"]
    space = dict(oc["space"])
    if lab if lab is not None else oc.get("include_lab_space"):
        space.update(oc.get("lab_space", {}))
    use_zone = oc.get("include_zone_space", True)
    zone_n = zone_n or (int(oc.get("zone_variants", 5)) if use_zone else 1)
    rnd = random.Random(seed)
    zv = {s: zone_variants(cfg, s, zone_n, seed or 0) for s in syms}
    lb = max(p["zone_lookback_days"] for s in syms for p in zv[s])
    data, start, end = load_data(cfg, days, lb, progress, syms)
    split = start + int((end - start) * oc["train_frac"])
    workers = oc.get("workers") or max(1, (os.cpu_count() or 2))
    t0 = time.time()
    ctx = mp.get_context("fork") if hasattr(os, "fork") else mp.get_context()
    # 1) zone timelines (cached on disk, shared by every combo that uses the same zone variant)
    _init(data, cfg, split, start)
    zjobs = [(s, p) for s in syms for p in zv[s]]
    with ctx.Pool(min(workers, len(zjobs)), initializer=_init, initargs=(data, cfg, split, start)) as pool:
        for i, r in enumerate(pool.imap_unordered(_zone_job, zjobs)):
            if progress:
                progress(f"zones {i + 1}/{len(zjobs)} (cached for next time)", 0.3 + 0.2 * (i + 1) / len(zjobs))
            if stop_flag and stop_flag():
                pool.terminate()
                break
    core.ZoneCache._mem.clear()
    for s, p in zjobs:  # load into memory so forked workers share them
        core.ZoneCache(s, p)
    # 2) combos: every symbol gets its own search (BTC and SOL ladders differ)
    jobs = []
    per = max(1, combos // len(syms))
    for s in syms:
        base = core.params_for(cfg, s)
        jobs.append((s, dict(base)))
        for zp in zv[s]:
            for p in sample_entry(space, zp, max(1, per // len(zv[s])), rnd):
                jobs.append((s, p))
    seen, uniq = set(), []
    for s, p in jobs:
        k = s + json.dumps(p, sort_keys=True)
        if k not in seen:
            seen.add(k)
            uniq.append((s, p))
    results = []
    with ctx.Pool(workers, initializer=_init, initargs=(data, cfg, split, start)) as pool:
        for i, r in enumerate(pool.imap_unordered(_eval, uniq)):
            results.append(r)
            if progress:
                el = time.time() - t0
                eta = el / (i + 1) * (len(uniq) - i - 1)
                progress(f"combo {i + 1}/{len(uniq)} · eta {eta / 60:.1f} min", 0.5 + 0.5 * (i + 1) / len(uniq))
            if stop_flag and stop_flag():
                pool.terminate()
                break
    mt = oc["min_trades"]
    out_sym = {}
    for s in syms:
        base = core.params_for(cfg, s)
        good = [r for r in results if r["sym"] == s and "error" not in r]
        need = mt * oc["train_frac"] / max(1, len(syms))

        def score(r):
            if r["train"]["n"] < need:
                return -1e9 + r["train"]["n"]
            return r["train"]["r"]
        good.sort(key=score, reverse=True)
        keys = set(space) | set(core.ZONE_KEYS)
        for r in good:
            r["valid"] = r["train"]["n"] >= need
            r["changed"] = {k: r["params"][k] for k in keys if r["params"].get(k) != base.get(k)}
        base_res = next((r for r in good if not r["changed"]), None)
        out_sym[s] = {"base": base_res, "top": good[:20]}
    return {"meta": {"days": round((end - start) / core.DAY, 1), "start": start, "end": end, "split": split,
                     "combos": len(results), "secs": time.time() - t0, "run_at": core.now_ms(), "min_trades": mt,
                     "zone_variants": {s: len(zv[s]) for s in syms},
                     "objective": "net R after fees on the first 70 % (train), checked on the last 30 % (test)"},
            "symbols": out_sym}


def warm_cache(cfg, days, progress=None):
    data, start, end = load_data(cfg, days, max_lookback(cfg), progress)
    for sym in cfg["symbols"]:
        p = core.params_for(cfg, sym)
        t = time.time()
        core.run_symbol(sym, data[sym]["data"], p, cfg, start, None)
        print(f"{sym}: zone cache ready ({time.time() - t:.0f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=None)
    ap.add_argument("--optimize", action="store_true")
    ap.add_argument("--warm", action="store_true", help="download history + build the zone cache")
    ap.add_argument("--combos", type=int, default=None)
    ap.add_argument("--risk", type=float, default=None)
    ap.add_argument("--balance", type=float, default=None)
    ap.add_argument("--symbol", default=None, help="optimize only this symbol")
    ap.add_argument("--progress-file", default=None, help="write progress json (used by the website)")
    ap.add_argument("--lab", action="store_true", help="also search the interpretation variants")
    a = ap.parse_args()
    cfg = core.load_config()
    log = core.setup_logging("optimizer" if a.optimize else "backtest", cfg)
    last = {"t": 0}

    def pr(stage, x):
        print(f"\r{stage[:60]:60s} {x * 100:5.1f}%", end="", flush=True)
        if a.progress_file and (time.time() - last["t"] > 1 or x >= 1):
            last["t"] = time.time()
            core.save_json(a.progress_file, {"running": True, "stage": stage, "pct": x, "pid": os.getpid(),
                                             "ts": core.now_ms()})
    if a.warm:
        warm_cache(cfg, a.days or cfg["backtest"]["default_days"], pr)
    elif a.optimize:
        try:
            res = optimize(cfg, a.days or cfg["optimizer"]["days"], a.combos or cfg["optimizer"]["combos"], pr,
                           lab=True if a.lab else None, symbols=[a.symbol] if a.symbol else None)
        except Exception as e:
            log.exception("optimizer failed")
            if a.progress_file:
                core.save_json(a.progress_file, {"running": False, "stage": "error", "error": str(e), "pct": 0})
            raise
        core.save_json(OPT_PATH, res)
        if a.progress_file:
            core.save_json(a.progress_file, {"running": False, "stage": "done", "pct": 1.0, "ts": core.now_ms()})
        print()
        for s, v in res["symbols"].items():
            print(f"== {s}")
            for r in v["top"][:8]:
                print(f"train {r['train']['r']:+7.1f}R n={r['train']['n']:4d} | test {r['test']['r']:+7.1f}R "
                      f"n={r['test']['n']:4d} pf={r['test']['pf']:.2f} | {r['changed']}")
    else:
        st = core.load_settings(cfg)
        if a.risk:
            st["risk_pct"] = a.risk
        if a.balance:
            st["start_balance"] = a.balance
        res = run_backtest(cfg, a.days or cfg["backtest"]["default_days"], st, progress=pr)
        core.save_json(BT_PATH, res)
        print()
        for k, acc in res["accounts"].items():
            s = acc["stats"]
            print(f"{k:4s} trades={s['total_trades']:4d} wr={s['winrate']:5.1f}% R={s['cum_r']:+7.2f} "
                  f"pnl=${s['net_pnl']:+9.2f} pf={s['profit_factor']:.2f} maxDD={s['max_dd_pct']:.1f}%")
