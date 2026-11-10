#!/usr/bin/env bash
# LONDON one-time setup on Raspberry Pi OS (Bookworm or newer, 64-bit recommended)
set -e
cd "$(dirname "$0")"
echo "== system packages"
sudo apt-get update
sudo apt-get install -y python3-venv python3-pip tmux curl sqlite3
echo "== python venv"
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt
mkdir -p data/logs static
echo "== config.json"
./venv/bin/python -c "import london_core as c; c.load_config(); print('config.json ready')"
echo "== candle cache"
if [ ! -s data/london.db ]; then
  for J in ../january/data/january.db ../January/data/january.db "$HOME/january/data/january.db" "$HOME/January/data/january.db"; do
    if [ -s "$J" ]; then
      echo "copying candles + funding from $J (saves a long download)"
      sqlite3 "$J" ".backup data/london.db" && break
    fi
  done
fi
echo "== chart library (local copy, CDN is the fallback)"
if [ ! -s static/lightweight-charts.js ]; then
  curl -fsSL -o static/lightweight-charts.js \
    https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js \
    || echo "WARN: chart library download failed - the dashboard will load it from the CDN"
fi
echo "== cloudflared (for the free quick tunnel)"
if ! command -v cloudflared >/dev/null 2>&1; then
  ARCH=$(dpkg --print-architecture)
  case "$ARCH" in arm64|amd64|armhf) ;; *) ARCH="";; esac
  if [ -n "$ARCH" ]; then
    curl -fsSL -o /tmp/cloudflared.deb "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}.deb" \
      && sudo dpkg -i /tmp/cloudflared.deb || echo "WARN: cloudflared install failed - install it yourself"
  else
    echo "WARN: unknown architecture - install cloudflared yourself"
  fi
fi
chmod +x start.sh stop.sh
echo "== history + zone cache (downloads ~3 years of 15m candles, builds the ladder; 5-20 min, once)"
./venv/bin/python london_backtest.py --warm --days 365 || echo "WARN: warm-up failed - it will happen on the first backtest instead"
echo
echo "Setup complete."
echo "1) nano config.json   -> put your Discord webhooks into \"discord\" (BTCUSDT, SOLUSDT, errors)"
echo "2) ./start.sh         -> starts tmux sessions London, LondonWeb, LondonCloud"
