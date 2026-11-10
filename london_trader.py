#!/usr/bin/env python3
"""
LONDON - live paper trader (tmux session "London").

* evaluates only CLOSED 15m candles (same Engine.step() as the backtester)
* entries fill at the open of the new candle (= next open after the engulfing close)
* stops are watched every few seconds on the last price and filled at the stop price
* trades are stored size-independent; the website sizes them with the risk you set
  (BTC $100 / SOL $100 / combined $100 accounts)

  ./venv/bin/python london_trader.py           # run
  ./venv/bin/python london_trader.py --reset   # wipe paper history (stop the trader first)
"""
import argparse
import json
import os
import signal
import sys
import time
from collections import deque

import numpy as np

import london_core as core
from london_backtest import warmup_ms

EVENTS_PATH = os.path.join(core.DATA_DIR, "events.json")
RUN = {"stop": False}


def _sig(*_):
    RUN["stop"] = True


class Trader:
    def __init__(self):
        self.cfg = core.load_config()
        self.log = core.setup_logging("trader", self.cfg)
        self.api = core.Bybit(self.cfg)
        self.store = core.Store(self.api)
        self.syms = self.cfg["symbols"]
        st = core.read_json(core.STATE_PATH, {}) or {}
        self.started = st.get("started") or core.now_ms()
        self.positions = st.get("positions", {}) or {}
        self.trades = core.read_json(core.TRADES_PATH, []) or []
        self.events = deque(core.read_json(EVENTS_PATH, []) or [], maxlen=400)
        self.last_bar = {}
        self.tick = {}
        self.fund_rate = {}
        self.engines = {}
        self.cfg_mtime = os.path.getmtime(core.CONFIG_PATH) if os.path.exists(core.CONFIG_PATH) else 0
        self.strategy_sig = self._sig(self.cfg)

    # ------------------------------------------------------------ warm-up
    def build_engines(self):
        end = core.now_ms()
        for sym in self.syms:
            p = core.params_for(self.cfg, sym)
            start = end - warmup_ms(p)
            self.log.info(f"{sym}: loading {p['zone_lookback_days']} days of history for the zone ladder ...")
            try:
                self.store.ensure(sym, start, end)
            except Exception:
                self.log.exception(f"{sym}: history download failed - using the cached candles")
            d = self.store.load(sym, start, end)
            eng = core.Engine(sym, p, self.cfg)
            ts = d["ts"]
            step_from = end - core.STEP_LEAD
            step_from -= step_from % core.H4
            i0 = int(np.searchsorted(ts, step_from)) if len(ts) else 0
            if i0 > 0:
                eng.preload_4h(core.aggregate_4h(ts[:i0], d["o"][:i0], d["h"][:i0], d["l"][:i0], d["c"][:i0]))
            for i in range(i0, len(ts)):
                eng.step(int(ts[i]), float(d["o"][i]), float(d["h"][i]), float(d["l"][i]), float(d["c"][i]))
            eng.zcache.save()
            pending = eng.pending
            eng.import_pos(self.positions.get(sym))  # live position (or none) replaces warm-up state
            last_ts = int(ts[-1]) if len(ts) else end - end % core.M15 - core.M15
            # a signal from the last closed bar may still be taken if we are within 120 s of its close
            if pending is not None and not eng.pos and core.now_ms() - (last_ts + core.M15) < 120_000:
                eng.pending = pending
            self.engines[sym] = eng
            self.last_bar[sym] = last_ts
            self.log.info(f"{sym}: warm-up done ({len(ts)} candles, {len(eng.zones)} ladder zones) - {eng.last_status}")
            if eng.pending is not None:
                self._fill_pending(sym)

    @staticmethod
    def _sig(cfg):
        return json.dumps({s: core.params_for(cfg, s) for s in cfg["symbols"]}, sort_keys=True)

    # ------------------------------------------------------------ persistence
    def save(self):
        st = {"version": core.VERSION, "started": self.started, "heartbeat": core.now_ms(),
              "positions": {s: (self.engines[s].export_pos_for_store() if s in self.engines else self.positions.get(s))
                            for s in self.syms},
              "status": {s: self.engines[s].status() for s in self.engines},
              "ticks": self.tick, "last_bar": self.last_bar}
        self.positions = st["positions"]
        core.save_json(core.STATE_PATH, st)

    def save_trades(self):
        core.save_json(core.TRADES_PATH, self.trades)

    def add_event(self, e):
        e = dict(e)
        e.pop("pos", None)
        self.events.append(e)
        core.save_json(EVENTS_PATH, list(self.events))

    # ------------------------------------------------------------ discord
    def sized_rows(self, sym, rec, closed=False):
        settings = core.load_settings(self.cfg)
        rows = []
        opens = [p for s, p in self.positions.items() if p] if not closed else []
        for name, ss in core.ACCOUNTS.items():
            if sym not in ss:
                continue
            if closed:
                rep = core.replay(self.trades, self.cfg, settings, ss)
                r = next((t for t in reversed(rep["trades"]) if t["entry_ts"] == rec["entry_ts"] and t["sym"] == sym), {})
            else:
                rep = core.replay(self.trades, self.cfg, settings, ss, opens)
                r = next((p for p in rep["open"] if p["sym"] == sym), {})
            rows.append(({"BTC": "BTC acct", "SOL": "SOL acct", "ALL": "Combined"}[name], r))
        return rows

    def notify(self, kind, sym, rec):
        url = self.cfg["discord"].get(sym, "")
        if not url:
            return
        try:
            core.discord_send(url, core.fmt_discord_trade(kind, rec, self.sized_rows(sym, rec, kind == "close")))
        except Exception:
            self.log.exception("discord trade notification failed")

    # ------------------------------------------------------------ events
    def handle(self, sym, events):
        for e in events:
            t = e["type"]
            if t == "open":
                ps = e["pos"]
                self.positions[sym] = self.engines[sym].export_pos_for_store()
                self.log.info(f"{sym} OPEN {'LONG' if ps['dir'] == 1 else 'SHORT'} @ {ps['entry']:.6g} SL {ps['sl']:.6g}")
                self.add_event(e)
                self.save()
                self.notify("open", sym, ps)
            elif t == "close":
                ps = e["pos"]
                rec = core.trade_record(ps)
                self.trades.append(rec)
                self.save_trades()
                self.positions[sym] = None
                r = core.unit_r(rec, self.cfg["fees"])
                self.log.info(f"{sym} CLOSE {rec['side']} @ {rec['exit']:.6g} ({rec['reason']}) {r:+.2f} R")
                self.add_event(dict(e, r=r))
                self.save()
                self.notify("close", sym, rec)
            elif t == "trail":
                ps = self.engines[sym].pos
                self.log.info(f"{sym} stop trailed to {e['sl']:.6g}")
                self.add_event(e)
                self.save()
                if ps:
                    self.notify("trail", sym, ps)
            elif t in ("breakout", "touch", "reanchor", "drop", "signal", "skip", "pullback"):
                msg = {"breakout": f"breakout {'up' if e.get('dir') == 1 else 'down'} through zone "
                                   f"{e.get('zone', ['', ''])[0]:.6g}-{e.get('zone', ['', ''])[1]:.6g}" if e.get('zone') else "breakout",
                       "touch": "61.8% retracement touched", "reanchor": f"closed beyond 72.2% - re-anchored to {e.get('L', 0):.6g}",
                       "drop": f"setup cancelled: {e.get('why')}", "signal": f"ENGULFING signal, SL {e.get('sl', 0):.6g}",
                       "skip": f"entry skipped: {e.get('why')}", "pullback": "3-candle pullback confirmed"}[t]
                self.log.info(f"{sym} {msg}")
                self.add_event(dict(e, msg=msg))

    # ------------------------------------------------------------ live pieces
    def _fill_pending(self, sym):
        eng = self.engines[sym]
        try:
            rows = self.api.klines(sym, "15", limit=2)
            cur = rows[-1]
            if cur[0] <= self.last_bar[sym]:
                return  # new candle not visible yet, try again next loop
            evs = eng.fill_pending_now(cur[0], cur[1])
            self.handle(sym, evs)
        except Exception:
            self.log.exception(f"{sym}: could not fill pending entry")

    def poll_ticks(self):
        for sym in self.syms:
            try:
                tk = self.api.ticker(sym)
                last = float(tk["lastPrice"])
                self.tick[sym] = {"last": last, "mark": float(tk.get("markPrice") or last),
                                  "chg24": float(tk.get("price24hPcnt") or 0) * 100,
                                  "funding": float(tk.get("fundingRate") or 0),
                                  "next_funding": int(tk.get("nextFundingTime") or 0), "ts": core.now_ms()}
                self.fund_rate[sym] = (int(tk.get("nextFundingTime") or 0), float(tk.get("fundingRate") or 0))
                eng = self.engines.get(sym)
                if eng and eng.pos:
                    ps = eng.pos
                    hit = last <= ps["sl"] if ps["dir"] == 1 else last >= ps["sl"]
                    tp = ps.get("tp")
                    tp_hit = bool(tp) and (last >= tp if ps["dir"] == 1 else last <= tp)
                    if hit and core.now_ms() >= ps["entry_ts"]:
                        eng.events = []
                        eng.close(core.now_ms(), ps["sl"], "trail stop" if ps["trail"] else "stop loss")
                        self.handle(sym, eng.events)
                    elif tp_hit and core.now_ms() >= ps["entry_ts"]:
                        eng.events = []
                        ps["exit_maker"] = True
                        eng.close(core.now_ms(), tp, "take profit")
                        self.handle(sym, eng.events)
            except Exception:
                self.log.exception(f"{sym}: ticker poll failed")

    def process_bars(self):
        for sym in self.syms:
            eng = self.engines[sym]
            due = self.last_bar[sym] + 2 * core.M15 + 2000
            if core.now_ms() < due:
                if eng.pending is not None:
                    self._fill_pending(sym)
                continue
            try:
                rows = self.api.klines(sym, "15", limit=6)
            except Exception:
                self.log.exception(f"{sym}: kline fetch failed")
                continue
            closed = [r for r in rows if r[0] + core.M15 <= core.now_ms() and r[0] > self.last_bar[sym]]
            if not closed:
                continue
            if closed[0][0] != self.last_bar[sym] + core.M15:
                self.log.warning(f"{sym}: gap in candles ({self.last_bar[sym]} -> {closed[0][0]}), filling from REST")
                gap = self.api.klines_range(sym, self.last_bar[sym] + core.M15, closed[-1][0])
                closed = [r for r in gap if r[0] > self.last_bar[sym] and r[0] + core.M15 <= core.now_ms()]
            self.store.insert(sym, closed)
            for r in closed:
                t, o, h, l, c = r[0], r[1], r[2], r[3], r[4]
                if eng.pos is not None:
                    _, rate = self.fund_rate.get(sym, (0, 0))
                    ft = t - t % (8 * 3_600_000) + 8 * 3_600_000
                    for f in (t - t % (8 * 3_600_000), ft):
                        if t <= f < t + core.M15 and eng.pos["entry_ts"] <= f and \
                                not any(x[0] == f for x in eng.pos["funding"]):
                            eng.pos["funding"].append([f, rate, o])
                zts = eng.zones_ts
                evs = eng.step(t, o, h, l, c)
                self.last_bar[sym] = t
                self.handle(sym, evs)
                if eng.zones_ts != zts:
                    eng.zcache.save()
                    self.log.info(f"{sym}: 4h candle closed - ladder rebuilt ({len(eng.zones)} zones)")
            if eng.pending is not None:
                if core.now_ms() - (self.last_bar[sym] + core.M15) < 120_000:
                    self._fill_pending(sym)
                else:
                    self.log.warning(f"{sym}: signal too old (>120 s after close) - not taken")
                    eng.pending = None

    def check_config(self):
        try:
            m = os.path.getmtime(core.CONFIG_PATH)
        except OSError:
            return
        if m == self.cfg_mtime:
            return
        self.cfg_mtime = m
        new = core.load_config()
        sig = self._sig(new)
        self.cfg = new
        if sig != self.strategy_sig:
            self.strategy_sig = sig
            self.log.info("strategy parameters changed - rebuilding engines (open positions are kept)")
            self.positions = {s: (self.engines[s].export_pos_for_store() if s in self.engines else None) for s in self.syms}
            self.build_engines()

    def run(self):
        self.log.info(f"LONDON paper trader v{core.VERSION} starting - {', '.join(self.syms)}")
        self.build_engines()
        self.save()
        if self.cfg["discord"].get("notify_start"):
            for sym in self.syms:
                url = self.cfg["discord"].get(sym)
                if url:
                    core.discord_send(url, f"◆ **LONDON** paper trader online · {sym} · {self.engines[sym].last_status}")
        last_tick = last_save = 0
        while not RUN["stop"]:
            try:
                if time.time() - last_tick >= 5:
                    self.poll_ticks()
                    last_tick = time.time()
                self.process_bars()
                self.check_config()
                if time.time() - last_save >= 10:
                    self.save()
                    last_save = time.time()
            except Exception:
                self.log.exception("main loop error")
                time.sleep(10)
            time.sleep(2)
        self.save()
        self.log.info("paper trader stopped")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="delete paper trades, positions and events")
    a = ap.parse_args()
    if a.reset:
        for p in (core.STATE_PATH, core.TRADES_PATH, EVENTS_PATH):
            if os.path.exists(p):
                os.remove(p)
        print("paper account reset.")
        sys.exit(0)
    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    Trader().run()
