#!/usr/bin/env python3
"""
LONDON - debug bundle for Claude.

Collects everything needed to analyse and tune the bot into ONE zip file:
  * config.json (Discord webhooks + web password removed)
  * paper trades, open positions, bot state, events, website settings
  * last backtest + optimizer results
  * logs (webhook URLs and tunnel URLs scrubbed)
  * the candle + funding database (so the backtests can be re-run on the real Bybit data)
  * system info: git version, python, tmux sessions, disk, memory, CPU temperature, Bybit reachability
  * the last screen of every tmux session (London, LondonWeb, LondonCloud)

Usage (on the Pi):
  cd ~/london
  python3 debug_bundle.py            # creates ~/london_debug_<date>.zip
  python3 debug_bundle.py --serve    # same + lets you download it on your phone/iPad (same WiFi)
Nothing is uploaded anywhere - you send the zip yourself.
"""
import argparse
import datetime as dt
import io
import json
import os
import platform
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
LOGS = os.path.join(DATA, "logs")

SCRUB = [
    (re.compile(r"https://(?:ptb\.|canary\.)?discord(?:app)?\.com/api/webhooks/\S+"), "<discord-webhook-removed>"),
    (re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com"), "<tunnel-url-removed>"),
]


def scrub(text):
    for rx, rep in SCRUB:
        text = rx.sub(rep, text)
    return text


def run(cmd, timeout=15):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip()
    except Exception as e:
        return f"<failed: {e}>"


def read_text(path, tail_bytes=None):
    try:
        with open(path, "rb") as f:
            if tail_bytes:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - tail_bytes))
            return f.read().decode("utf-8", "ignore")
    except Exception as e:
        return f"<could not read {path}: {e}>"


def clean_config():
    p = os.path.join(BASE, "config.json")
    try:
        with open(p, encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        return json.dumps({"error": f"config.json unreadable: {e}"})
    d = cfg.get("discord", {})
    for k, v in list(d.items()):
        if isinstance(v, str):
            d[k] = "<set>" if v.strip() else "<empty>"
    if isinstance(cfg.get("web"), dict) and cfg["web"].get("password"):
        cfg["web"]["password"] = "<set>"
    return json.dumps(cfg, indent=2)


def db_snapshot(dst):
    """consistent copy of the sqlite db (works while the trader is running) + a summary."""
    src = os.path.join(DATA, "london.db")
    if not os.path.exists(src):
        return "no database yet"
    s = sqlite3.connect(src, timeout=30)
    d = sqlite3.connect(dst)
    s.backup(d)
    d.execute("VACUUM")
    d.close()
    rows = s.execute("SELECT sym, COUNT(*), MIN(ts), MAX(ts) FROM candles GROUP BY sym").fetchall()
    fr = s.execute("SELECT sym, COUNT(*) FROM funding GROUP BY sym").fetchall()
    s.close()
    f = lambda t: dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M") if t else "-"
    out = [f"{r[0]}: {r[1]} candles (15m) {f(r[2])} -> {f(r[3])} UTC" for r in rows]
    out += [f"{r[0]}: {r[1]} funding rates" for r in fr]
    return "\n".join(out)


def bybit_check():
    out = []
    for path in ("/v5/market/time", "/v5/market/tickers?category=linear&symbol=BTCUSDT"):
        t0 = time.time()
        try:
            with urllib.request.urlopen("https://api.bybit.com" + path, timeout=10) as r:
                body = r.read(300).decode("utf-8", "ignore")
            out.append(f"OK {int((time.time() - t0) * 1000)} ms {path} -> {body[:160]}")
        except Exception as e:
            out.append(f"FAIL {path}: {e}")
    return "\n".join(out)


def local_web_check():
    try:
        with urllib.request.urlopen("http://127.0.0.1:8759/healthz", timeout=5) as r:
            return f"dashboard /healthz -> {r.status} {r.read(20).decode()}"
    except Exception as e:
        return f"dashboard not reachable on :8759 -> {e}"


def sysinfo():
    lines = [
        f"created: {dt.datetime.now().isoformat(timespec='seconds')} (local) / {dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).isoformat(timespec='seconds')} UTC",
        f"host: {socket.gethostname()}  platform: {platform.platform()}  machine: {platform.machine()}",
        f"system python: {sys.version.split()[0]}",
        f"venv python: {run(os.path.join(BASE, 'venv/bin/python') + ' --version')}",
        "",
        "== git", run(f"git -C '{BASE}' log --oneline -5"), run(f"git -C '{BASE}' status --short"),
        "", "== pip (venv)", run(os.path.join(BASE, "venv/bin/pip") + " freeze 2>/dev/null | grep -iE 'flask|waitress|numpy|requests|tzdata'"),
        "", "== tmux sessions", run("tmux ls"),
        "", "== processes", run("ps aux | grep -E 'london_|cloudflared' | grep -v grep"),
        "", "== uptime / load", run("uptime"),
        "", "== memory", run("free -h"),
        "", "== disk", run(f"df -h '{BASE}'"),
        "", "== data folder", run(f"ls -la '{DATA}' '{LOGS}'"),
        "", "== cpu temperature / throttling", run("vcgencmd measure_temp 2>/dev/null; vcgencmd get_throttled 2>/dev/null"),
        "", "== clock sync (important for candle timing)", run("timedatectl 2>/dev/null | head -8"),
        "", "== cloudflared", run("cloudflared --version 2>/dev/null"),
        "", "== Bybit API from the Pi", bybit_check(),
        "", "== local dashboard", local_web_check(),
    ]
    return "\n".join(lines)


def tmux_screens():
    out = []
    for s in ("London", "LondonWeb", "LondonCloud"):
        out.append(f"===== tmux {s} =====")
        out.append(run(f"tmux capture-pane -p -t {s} -S -300 2>&1"))
        out.append("")
    return scrub("\n".join(out))


def summary():
    """short human-readable overview so the important numbers are visible at a glance."""
    lines = []
    tr = []
    try:
        with open(os.path.join(DATA, "trades.json"), encoding="utf-8") as f:
            tr = json.load(f)
    except Exception:
        pass
    lines.append(f"paper trades closed: {len(tr)}")
    try:
        with open(os.path.join(DATA, "state.json"), encoding="utf-8") as f:
            st = json.load(f)
        hb = st.get("heartbeat") or 0
        lines.append(f"trader heartbeat: {int(time.time() - hb / 1000)} s ago, started {dt.datetime.fromtimestamp((st.get('started') or 0) / 1000)}")
        for sym, s in (st.get("status") or {}).items():
            lines.append(f"{sym}: {s.get('text')} | zones: {len(s.get('zones') or [])} | position: {'yes' if (st.get('positions') or {}).get(sym) else 'no'}")
    except Exception as e:
        lines.append(f"state.json: {e}")
    try:
        with open(os.path.join(DATA, "backtest_last.json"), encoding="utf-8") as f:
            bt = json.load(f)
        m = bt["meta"]
        lines.append(f"last backtest: {m.get('days')} days, run {dt.datetime.fromtimestamp(m['run_at'] / 1000)}, raw trades {m.get('raw_trades')}")
        for k, a in bt.get("accounts", {}).items():
            s = a["stats"]
            lines.append(f"  {k:4s} n={s['total_trades']} wr={s['winrate']:.1f}% R={s['cum_r']:+.2f} pnl=${s['net_pnl']:+.2f} "
                         f"pf={s['profit_factor'] if s['profit_factor'] is not None else 'inf'} maxDD={s['max_dd_pct']:.1f}%")
    except Exception as e:
        lines.append(f"backtest: none ({e.__class__.__name__})")
    try:
        with open(os.path.join(DATA, "optimizer_last.json"), encoding="utf-8") as f:
            op = json.load(f)
        lines.append(f"last optimizer: {op['meta']['combos']} combos, {op['meta']['days']} days")
        for r in op.get("top", [])[:5]:
            lines.append(f"  train {r['train']['r']:+.1f}R n={r['train']['n']} | test {r['test']['r']:+.1f}R n={r['test']['n']} | {r.get('changed')}")
    except Exception as e:
        lines.append(f"optimizer: none ({e.__class__.__name__})")
    lines.append("")
    lines.append("errors/warnings in the log (last 40):")
    log = read_text(os.path.join(LOGS, "london.log"), 2_000_000)
    errs = [l for l in log.splitlines() if " ERROR " in l or " CRITICAL " in l or " WARNING " in l]
    lines += errs[-40:] or ["  none"]
    return scrub("\n".join(lines))


def build(out_path, notes):
    tmp = tempfile.mkdtemp(prefix="london_dbg_")
    try:
        with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            def add_text(name, text):
                z.writestr(name, text)

            print("  summary ...")
            add_text("00_SUMMARY.txt", summary())
            if notes:
                add_text("00_NOTES_FROM_LEON.txt", notes)
            print("  system info ...")
            add_text("system_info.txt", scrub(sysinfo()))
            add_text("tmux_screens.txt", tmux_screens())
            add_text("config.json", clean_config())
            for name in ("trades.json", "state.json", "events.json", "settings.json", "backtest_last.json",
                         "optimizer_last.json", "optimizer_progress.json"):
                p = os.path.join(DATA, name)
                if os.path.exists(p):
                    add_text("data/" + name, scrub(read_text(p)))
            print("  logs ...")
            for name in os.listdir(LOGS) if os.path.isdir(LOGS) else []:
                p = os.path.join(LOGS, name)
                if os.path.isfile(p):
                    add_text("logs/" + name, scrub(read_text(p, 5_000_000)))
            print("  candle database (can take a moment) ...")
            dbp = os.path.join(tmp, "london.db")
            info = db_snapshot(dbp)
            add_text("data/database_summary.txt", info)
            if os.path.exists(dbp):
                z.write(dbp, "data/london.db")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "<pi-ip>"


def serve(path, port):
    import http.server
    import functools
    folder, fname = os.path.dirname(path), os.path.basename(path)

    class H(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            if self.path.strip("/") != fname:
                self.send_response(302)
                self.send_header("Location", "/" + fname)
                self.end_headers()
                return
            super().do_GET()

        def log_message(self, *a):
            pass

    handler = functools.partial(H, directory=folder)
    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", port), handler)
    print(f"\nOpen this on your phone/iPad (same WiFi):  http://{lan_ip()}:{port}/{fname}")
    print("Then upload the zip in the chat. Press Ctrl+C here when the download is done.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="create a debug zip for Claude")
    ap.add_argument("--serve", action="store_true", help="serve the zip on your local network for download")
    ap.add_argument("--port", type=int, default=8799)
    ap.add_argument("--notes", default="", help='optional text, e.g. --notes "entries feel too late"')
    a = ap.parse_args()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M")
    out = os.path.join(os.path.expanduser("~"), f"london_debug_{stamp}.zip")
    print("LONDON debug bundle")
    build(out, a.notes)
    print(f"\ncreated {out}  ({os.path.getsize(out) / 1e6:.1f} MB)")
    print("Discord webhooks, the web password and tunnel URLs were removed.")
    if a.serve:
        serve(out, a.port)
    else:
        print("To download it on your phone/iPad: python3 debug_bundle.py --serve")
