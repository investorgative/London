#!/usr/bin/env bash
# Starts LONDON: paper trader (tmux "London"), dashboard on :8759 ("LondonWeb"),
# Cloudflare quick tunnel ("LondonCloud"). Every process restarts itself if it crashes.
cd "$(dirname "$0")"
DIR="$(pwd)"
PY="$DIR/venv/bin/python"
PORT=$("$PY" -c "import london_core as c; print(c.load_config()['web']['port'])" 2>/dev/null || echo 8759)
[ -x "$PY" ] || { echo "venv missing - run ./setup.sh first"; exit 1; }
mkdir -p "$DIR/data/logs"
if ! tmux has-session -t London 2>/dev/null; then
  tmux new-session -d -s London "cd '$DIR' && while true; do '$PY' london_trader.py; echo 'trader exited - restarting in 10s'; sleep 10; done"
  echo "started tmux session London      (paper trader)"
else echo "tmux session London already running"; fi
if ! tmux has-session -t LondonWeb 2>/dev/null; then
  tmux new-session -d -s LondonWeb "cd '$DIR' && while true; do '$PY' london_web.py; echo 'web exited - restarting in 10s'; sleep 10; done"
  echo "started tmux session LondonWeb   (dashboard http://localhost:$PORT)"
else echo "tmux session LondonWeb already running"; fi
if command -v cloudflared >/dev/null 2>&1; then
  if ! tmux has-session -t LondonCloud 2>/dev/null; then
    : > "$DIR/data/logs/cloudflared.log"
    tmux new-session -d -s LondonCloud "cd '$DIR' && while true; do cloudflared tunnel --no-autoupdate --url http://localhost:$PORT 2>&1 | tee -a data/logs/cloudflared.log; echo 'tunnel exited - restarting in 10s'; sleep 10; done"
    echo "started tmux session LondonCloud (quick tunnel - URL appears in a few seconds)"
    sleep 8
    URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' "$DIR/data/logs/cloudflared.log" | tail -1)
    [ -n "$URL" ] && echo "public URL: $URL" || echo "public URL: run 'tmux attach -t LondonCloud' or look at the dashboard sidebar"
  else echo "tmux session LondonCloud already running"; fi
else
  echo "cloudflared not installed - tunnel not started (dashboard still works on the LAN)"
fi
