# LONDON: 4h support/resistance ladder · 15m execution (BTCUSDT · SOLUSDT, Bybit)

London is the successor to [January](https://github.com/investorgative/January). The 15m entry logic is the
same, but the support/resistance zones are built the way you draw them by hand: a **long-memory ladder**
of wide zones instead of a few thin, short-lived levels.

It includes a paper trader, a dashboard, a backtester and an optimizer, and it runs on a Raspberry Pi.
The paper trader and the backtester share one engine (`london_core.Engine`), so a paper trade and a
backtest trade come from exactly the same code.

## What changed vs. January: how the zones are built

January looked back ~30 days and kept zones ~0.4 × the recent 4h ATR thick, which gave about 6 thin
levels that kept moving. Compared with the hand-drawn BTC/SOL levels from 10 Nov 2026, real zones are:

* **long-memory**: drawn from up to ~2 years of 4h history (levels from 2024/2025 still count)
* **wide**: BTC ~1.5–5 %, SOL ~4–12 % of price, edges sitting on wick extremes
* **re-tested**: every zone reacted several times, usually from both sides (support turned resistance)
* **a ladder**: zones alternate with empty gaps of similar size, with no overlap and nothing washed out

`ladder_zones()` in `london_core.py` does exactly that:

1. every confirmed 4h swing high / low is one *reaction*; its wick is the level
2. the wicks of the neighbouring candles of that move belong to the same reaction
3. bands of several thicknesses are slid over the whole price range on a fixed price lattice; a band scores
   distinct reactions (one move counts once), bonus for acting as support **and** resistance, bonus
   for wick touches and recency, penalty for thickness
4. a band needs at least `zone_min_touches` distinct reactions
5. best bands are taken first, with at least `zone_gap` free space between zones

Widths and gaps are measured in **u = median 4h candle range** of the window, so the logic adapts to
each coin. BTC and SOL ladders differ structurally, so each coin has its own zone settings
(`symbol_params` in `config.json`).

**Calibration.** The default zone settings were chosen so that today's ladder resembles your hand-drawn
one: about 44 % (BTC) and 53 % (SOL) average overlap, and **every** one of your zones is overlapped by an
algorithm zone. They were also chosen to stay stable: only ~2–3 % of zones change per day. You can see
the comparison live in **Zone match**, and you can edit your levels there.

**No look-ahead.** At any moment, zones are built only from 4h candles that had **closed** by then
(the paper trader and the backtester use the same function). Your hand-drawn levels are **never** read by
the trader, backtester or optimizer; they are only displayed and scored. One honest caveat: the
*default zone settings* were picked by comparing against levels you drew today. That is a mild
use of hindsight in how the parameters were chosen, not in any trading decision. Judge the strategy on
the optimizer's out-of-sample (test) column and on the forward paper test.

## Strategy (15m)

1. price closes between zone A and zone B for at least `range_min_bars` candles
2. a 15m candle **closes through B**: price is now between B and C (long when breaking up, short down)
3. a pullback of at least 3 candles against the move, none of them a shooting star (longs) / hammer (shorts)
4. the pullback reaches **61.8 %** of the move (from the swing low before the move to the high).
   If it **closes deeper than 72.2 %**, that low was the wrong one and the one before it is used. This repeats until
   price is back in the old range A–B (`invalidate: "far"`, the default; `"close"` = back inside zone B,
   January's rule)
5. after the 61.8 % touch: an **engulfing** candle in the breakout direction (it may not engulf a hammer
   or shooting star) → entry at the next candle's open
6. stop below the swing low (wick) plus a small buffer. Each time a 15m close breaks the structure
   high, the stop moves to just below the low between the previous high and the break. Shorts mirror this.

Fees: Bybit taker 0.055 % on entry and stop exits, maker 0.02 % on limit take-profits (if enabled),
plus funding while a position is open. Every trade in the paper trader and the backtester pays them.

## Files
| File | Purpose |
|---|---|
| `london_core.py` | data (Bybit + sqlite cache), ladder zones + zone cache, 15m state machine, trailing stop, account replay, stats, journal, Kelly, Discord |
| `london_trader.py` | live paper trader, tmux session **London** |
| `london_web.py` | dashboard + API on port **8759**, tmux session **LondonWeb** |
| `london_backtest.py` | backtester + optimizer (used by the website, also works from the command line) |
| `static/index.html` | the dashboard |
| `config.json` | all settings (created on first start, not tracked by git, so `git pull` never conflicts) |
| `setup.sh` / `start.sh` / `stop.sh` | install / start the 3 tmux sessions / stop |
| `debug_bundle.py` | one-command export for analysis (secrets scrubbed) |

Port 8759 was chosen because 8756 (January), 8757 (TEN·AM) and 8758 are already used on your Pi.
Change it in `config.json` → `"web": {"port": ...}`.

## Setup on the Pi (Raspberry Pi OS Bookworm, 64-bit recommended)
```bash
cd ~
git clone https://github.com/investorgative/London.git london
cd london
chmod +x setup.sh && ./setup.sh   # venv, packages, chart lib, cloudflared, history download + zone cache
nano config.json                  # paste your Discord webhooks (see below), save: Ctrl+O, Enter, Ctrl+X
./start.sh                        # starts London, LondonWeb, LondonCloud
```
If January is installed in `~/january`, setup copies its candle cache, so only the missing older
history is downloaded. The first backtest of a new zone setting builds the zone cache (a few minutes on
a Pi); every later run with that setting takes seconds.

Dashboard on your network: `http://<pi-ip>:8759`. The public trycloudflare URL is printed by
`start.sh` and shown in the dashboard sidebar.

### Cloudflare
`start.sh` runs a free quick tunnel (`cloudflared tunnel --url http://localhost:8759`) in tmux
**LondonCloud**. The site only uses relative URLs and plain polling (no websockets), sends
`Cache-Control: no-store` on every API call and gzips large responses, so it works fully through the
tunnel. If you run your own named tunnel instead, point it at `http://localhost:8759` and skip
LondonCloud. Because the page is public, you can set `"web": {"password": "something"}`; the browser
then asks for a login (any username, that password).

### Discord
In `config.json` → `"discord"`:
```json
"discord": {
  "BTCUSDT": "https://discord.com/api/webhooks/...   (BTC trades)",
  "SOLUSDT": "https://discord.com/api/webhooks/...   (SOL trades)",
  "errors":  "https://discord.com/api/webhooks/...   (every ERROR/CRITICAL from all processes)",
  "notify_start": true
}
```
Discord: Server settings → Integrations → Webhooks → New webhook → copy URL. One per channel.
Trade messages: open (entry, SL, size per account), every stop trail, close (PnL, R, fees per account).
Errors are deduplicated (same message at most once per 5 minutes).

### Daily use
```bash
tmux attach -t London          # paper trader      (leave with Ctrl+B, then D)
tmux attach -t LondonWeb       # web server
tmux attach -t LondonCloud     # cloudflare tunnel (shows the public URL)
./stop.sh && ./start.sh        # restart everything
git pull && ./stop.sh && ./start.sh                                   # update
./stop.sh && ./venv/bin/python london_trader.py --reset && ./start.sh # wipe the paper account
./venv/bin/python london_backtest.py --days 365                       # backtest from the shell
./venv/bin/python london_backtest.py --optimize --days 1000 --combos 300
./venv/bin/python debug_bundle.py                                     # zip for analysis
```
Autostart after reboot: `crontab -e`, add `@reboot sleep 30 && /home/<user>/london/start.sh`.

## Dashboard
* **Overview**: the live ladder per coin (zones, price, current range A–B, your levels, entry/stop),
  equity curve and all account stats: trades, wins/losses, win rate, cumulative R, max DD in R, account
  size, risk, fees, days running, net PnL, profit factor, avg annual return, avg win/loss, max DD %,
  max daily DD, streaks, avg reward:risk, fees total/avg, avg order size, leverage, time in position,
  SL size, Kelly. Accounts: **Combined / BTC / SOL** ($100 each, 2 % risk by default).
* **Live chart**: 15m / 1h / 4h candles with the ladder boxes, your levels, breakouts, entries, exits,
  61.8 % / 72.2 % / anchor / stop lines. Switch to **Backtest** to replay a backtest with the ladder
  as it looked at each moment.
* **Open positions**, **Trade log**, **Strategy status** (tube-line of setup steps), **Journal**
  (years → months → weeks → days, opens on weeks with 4 visible), **Log** (system log + strategy events).
* **Zone match**: algorithm ladder vs your levels, match score, editable levels, per-coin zone settings.
* **Backtester**: period / risk / balance / sizing (fixed or half Kelly), all fees, same views as the paper account.
* **Optimizer**: per coin, train 70 % / test 30 %, *Use for paper* writes that coin's settings and the
  paper trader reloads automatically. **Backtest history** keeps every run.
* **Real-money header**: present in the layout, not connected.

## Honest first results (Bybit data 2024-09 → 2026-11, fees included)
Default settings, 600-day backtest: 67 trades, about −18 R (BTC −7.6 R, SOL −10.3 R). The optimizer found
settings with up to +18 R on the training part, but they **lost on the unseen test part**: classic
overfitting, so they are *not* the defaults. More history on the Pi (the setup downloads ~3 years)
and the forward paper test are the next steps before any real money.
