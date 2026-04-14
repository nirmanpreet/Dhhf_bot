#!/usr/bin/env python3
"""DHHF Bot v9 - Full Featured DCA Engine with Verbose Logging"""

import os
import json
import logging
import hashlib
import asyncio
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import yfinance as yf
from telegram import Bot

# ================= CONFIG ================= #
TOKEN          = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID        = os.getenv('TELEGRAM_CHAT_ID')

AVG_COST       = float(os.getenv('AVERAGE_COST',    '38.5'))
MONTHLY        = float(os.getenv('MONTHLY_AMOUNT',  '1000'))
BONUS          = float(os.getenv('BONUS_AMOUNT',    '1000'))
MONTHLY_BUDGET = float(os.getenv('MONTHLY_BUDGET',  '3000'))
MIN_ORDER      = float(os.getenv('MIN_ORDER',        '100'))

STATE_FILE      = 'bot_state.json'
SCORE_THRESHOLD = 60
COOLDOWN_HOURS  = 4

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

TZ = pytz.timezone('Australia/Melbourne')

ALWAYS_HOURS      = [10, 16]
CONDITIONAL_HOURS = {12: 65, 14: 75}


def sep(label=""):
    if label:
        logger.info(f"{'─' * 10} {label} {'─' * (max(1, 30 - len(label)))}")
    else:
        logger.info("─" * 45)


# ================= BOT ================= #
class DHHFBot:

    def __init__(self):
        self.state  = self.load_state()
        self.result = "NOT_RUN"

    # ---------- STATE ----------
    def load_state(self):
        defaults = {
            "last_price":     None,
            "last_alert":     None,
            "last_msg_hash":  None,
            "monthly_spent":  0.0,
            "current_month":  None,
            "units_held":     0.0,
            "total_invested": 0.0,
        }
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    saved = json.load(f)
                    defaults.update(saved)
                logger.info(f"📂 State loaded from {STATE_FILE}")
                logger.info(f"   monthly_spent  : ${defaults['monthly_spent']:.2f}")
                logger.info(f"   current_month  : {defaults['current_month']}")
                logger.info(f"   units_held     : {defaults['units_held']:.4f}")
                logger.info(f"   total_invested : ${defaults['total_invested']:.2f}")
                logger.info(f"   last_alert     : {defaults['last_alert'] or 'never'}")
                logger.info(f"   last_price     : ${defaults['last_price'] or 'n/a'}")
            except Exception as e:
                logger.error(f"❌ State load error: {e} — using defaults")
        else:
            logger.info(f"📂 No state file found at {STATE_FILE} — starting fresh")
        return defaults

    def save(self):
        with open(STATE_FILE, "w") as f:
            json.dump(self.state, f, indent=2)
        logger.info(f"💾 State saved → {STATE_FILE}")

    def reset_month(self, now):
        if self.state["current_month"] != now.month:
            logger.info(f"🔄 Month changed: {self.state['current_month']} → {now.month}")
            logger.info(f"   Resetting monthly_spent: ${self.state['monthly_spent']:.2f} → $0.00")
            self.state["monthly_spent"] = 0.0
            self.state["current_month"] = now.month
        else:
            logger.info(f"📅 Same month ({now.month}) — budget carries over, no reset")

    # ---------- ASX HOURS ----------
    def is_asx_open(self, now):
        if now.weekday() >= 5:
            logger.info(f"   Today is {now.strftime('%A')} — ASX is closed on weekends")
            return False
        open_time  = now.replace(hour=10, minute=0,  second=0, microsecond=0)
        close_time = now.replace(hour=16, minute=10, second=0, microsecond=0)
        if now < open_time:
            opens_in = (open_time - now).seconds // 60
            logger.info(f"   Pre-market — ASX opens at 10:00 AEST (now {now.strftime('%H:%M')}, opens in ~{opens_in}m)")
            return False
        if now > close_time:
            logger.info(f"   After-hours — ASX closed at 16:10 AEST (now {now.strftime('%H:%M')})")
            return False
        logger.info(f"   ✅ ASX is open ({now.strftime('%H:%M')} AEST)")
        return True

    def get_min_score_for_hour(self, hour):
        if hour in ALWAYS_HOURS:
            logger.info(f"   {hour}:00 is a primary check hour → no extra score gate")
            return 0
        min_s = CONDITIONAL_HOURS.get(hour, None)
        if min_s is not None:
            logger.info(f"   {hour}:00 is a conditional check hour → requires score ≥ {min_s}")
        else:
            valid = sorted(list(ALWAYS_HOURS) + list(CONDITIONAL_HOURS.keys()))
            logger.info(f"   {hour}:00 is not a scheduled check hour")
            logger.info(f"   Scheduled hours: {valid}")
        return min_s

    # ---------- DATA ----------
    def _make_session(self):
        """
        Build a requests.Session that looks like a browser.
        GitHub Actions IPs are heavily rate-limited by Yahoo Finance because
        thousands of bots run from the same shared IP ranges. A browser-like
        User-Agent + retry adapter works around the most common 429 blocks.
        """
        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-AU,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT":             "1",
        })
        # Auto-retry on 429 / 5xx with exponential backoff
        retry = Retry(
            total=4,
            backoff_factor=2,          # waits 2s, 4s, 8s, 16s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://",  adapter)
        return session

    def _fetch_yahoo_direct(self):
        """
        Fallback: hit the Yahoo Finance v8 chart API directly.
        Returns a pd.Series of daily closes (1Y) or None on failure.
        This bypasses yfinance entirely, which helps when yfinance's
        cookie/crumb logic gets tripped up by rate limits.
        """
        url = (
            "https://query1.finance.yahoo.com/v8/finance/chart/DHHF.AX"
            "?interval=1d&range=1y&includePrePost=false"
        )
        try:
            logger.info("   Trying direct Yahoo Finance v8 API...")
            r = self._make_session().get(url, timeout=15)
            r.raise_for_status()
            data  = r.json()
            chart = data["chart"]["result"][0]
            ts    = chart["timestamp"]
            closes = chart["indicators"]["quote"][0]["close"]
            closes = [c for c in closes if c is not None]
            if not closes:
                logger.error("   ❌ Direct API returned empty closes")
                return None
            idx    = pd.to_datetime(ts[:len(closes)], unit="s", utc=True)
            series = pd.Series(closes, index=idx, dtype=float)
            logger.info(f"   ✅ Direct API returned {len(series)} daily closes")
            return series
        except Exception as e:
            logger.error(f"   ❌ Direct API fallback failed: {e}")
            return None

    def fetch_data(self):
        sep("MARKET DATA")
        #
        # WHY NO INTRADAY:
        #   period="1d", interval="1m" is the most fragile yfinance call.
        #   Yahoo Finance blocks it first from server IPs (GitHub Actions,
        #   PythonAnywhere, etc.) and it returns empty even during market
        #   hours for ASX stocks. Daily data is sufficient for a DCA bot —
        #   we only need the latest close, not tick-level data.
        #
        # STRATEGY:
        #   Attempt 1 — yfinance with a browser-like session (handles most
        #               429 blocks from GitHub Actions)
        #   Attempt 2 — direct Yahoo Finance v8 API (bypasses yfinance's
        #               cookie/crumb logic entirely)
        #   Both use the same daily 1Y period.
        #
        hist = None

        # ── Attempt 1: yfinance with browser session ──
        for attempt in range(1, 4):
            try:
                logger.info(f"📡 yfinance attempt {attempt}/3 (DHHF.AX, 1Y daily)...")
                session = self._make_session()
                ticker  = yf.Ticker("DHHF.AX", session=session)
                df      = ticker.history(period="1y", interval="1d", auto_adjust=True)
                if not df.empty and len(df) > 5:
                    hist = df["Close"]
                    logger.info(f"   ✅ yfinance returned {len(hist)} daily closes (attempt {attempt})")
                    break
                else:
                    logger.warning(f"   ⚠️  Attempt {attempt}: got {len(df)} rows — too few, retrying...")
                    time.sleep(3 * attempt)
            except Exception as e:
                logger.warning(f"   ⚠️  Attempt {attempt} exception: {e}")
                time.sleep(3 * attempt)

        # ── Attempt 2: direct Yahoo v8 API ──
        if hist is None:
            logger.info("📡 yfinance exhausted — trying direct Yahoo Finance API...")
            hist = self._fetch_yahoo_direct()

        if hist is None or len(hist) < 5:
            logger.error("❌ All data sources failed — cannot proceed")
            logger.error("   Possible causes:")
            logger.error("   • GitHub Actions IP is rate-limited by Yahoo Finance (most common)")
            logger.error("   • ASX is closed and Yahoo returned no data for today")
            logger.error("   • Yahoo Finance endpoint changed (check yfinance GitHub issues)")
            logger.error("   • Network egress blocked from this runner")
            return None

        # ── Parse ──
        price      = float(hist.iloc[-1])
        prev_close = float(hist.iloc[-2])
        change     = ((price - prev_close) / prev_close) * 100
        direction  = "📈" if change >= 0 else "📉"

        logger.info(f"   Last close     : ${price:.2f}")
        logger.info(f"   Prev close     : ${prev_close:.2f}")
        logger.info(f"   Day change     : {direction} {change:+.2f}%")
        logger.info(f"   Data points    : {len(hist)} trading days")

        high_52w = float(hist.max())
        low_52w  = float(hist.min())
        logger.info(f"   52W High       : ${high_52w:.2f}")
        logger.info(f"   52W Low        : ${low_52w:.2f}")

        ma_200 = hist.rolling(200).mean().iloc[-1]
        if pd.isna(ma_200):
            ma_200 = hist.mean()
            logger.warning(f"   ⚠️  200d MA NaN ({len(hist)} days) — using period mean: ${ma_200:.2f}")
        else:
            logger.info(f"   200d MA        : ${ma_200:.2f}")

        return {
            "price":    price,
            "change":   change,
            "hist":     hist,
            "high_52w": high_52w,
            "low_52w":  low_52w,
            "avg_200d": float(ma_200),
        }

    # ---------- RSI ----------
    def calculate_rsi(self, closes, period=14):
        delta = closes.diff()
        gain  = delta.clip(lower=0).rolling(period).mean()
        loss  = (-delta.clip(upper=0)).rolling(period).mean()
        if loss.iloc[-1] == 0:
            return 100.0
        rs = gain.iloc[-1] / loss.iloc[-1]
        return float(100 - (100 / (1 + rs)))

    # ---------- MOMENTUM ----------
    def is_stabilising(self, hist):
        vals = hist.iloc[-3:].values
        d0, d1, d2 = float(vals[-3]), float(vals[-2]), float(vals[-1])
        stabilising = d2 >= d1
        logger.info(f"   3-day prices   : D-2=${d0:.2f}  D-1=${d1:.2f}  D0=${d2:.2f}")
        if stabilising:
            logger.info(f"   Momentum       : ✅ Stabilising (D0 ${d2:.2f} ≥ D-1 ${d1:.2f})")
        else:
            logger.info(f"   Momentum       : ⚠️  Still falling (D0 ${d2:.2f} < D-1 ${d1:.2f})")
        return stabilising

    # ---------- SCORE ----------
    def calculate_score(self, data):
        sep("SCORING ENGINE")
        price   = data["price"]
        hist    = data["hist"]
        score   = 0
        signals = []

        logger.info(f"Evaluating ${price:.2f} against ${AVG_COST:.2f} avg cost")
        logger.info("")

        # ── Multi-timeframe percentile ──
        logger.info("[ Percentile Analysis ]")
        hist_3m = hist.iloc[-63:]  if len(hist) >= 63  else hist
        hist_6m = hist.iloc[-126:] if len(hist) >= 126 else hist
        pct_3m  = float((hist_3m <= price).mean() * 100)
        pct_6m  = float((hist_6m <= price).mean() * 100)
        pct_1y  = float((hist    <= price).mean() * 100)
        avg_pct = (pct_3m + pct_6m + pct_1y) / 3

        logger.info(f"   3M  ({len(hist_3m):>3} days) : {pct_3m:.1f}%  ← cheaper than this % of closes")
        logger.info(f"   6M  ({len(hist_6m):>3} days) : {pct_6m:.1f}%")
        logger.info(f"   1Y  ({len(hist):>3} days) : {pct_1y:.1f}%")
        logger.info(f"   Avg              : {avg_pct:.1f}%")

        if avg_pct < 20:
            score += 30
            signals.append(f"Cheap across all timeframes ({avg_pct:.0f}%)")
            logger.info(f"   ✅ avg_pct {avg_pct:.1f}% < 20% → +30 pts")
        elif pct_1y < 20:
            score += 15
            signals.append(f"1Y value zone ({pct_1y:.0f}%)")
            logger.info(f"   ✅ pct_1y {pct_1y:.1f}% < 20% → +15 pts")
        else:
            logger.info(f"   ➖ No percentile value signal (avg {avg_pct:.1f}% ≥ 20%)")

        # ── 52W Discount ──
        logger.info("")
        logger.info("[ 52W Drawdown ]")
        discount = (data["high_52w"] - price) / data["high_52w"] * 100
        logger.info(f"   52W High       : ${data['high_52w']:.2f}")
        logger.info(f"   Current        : ${price:.2f}")
        logger.info(f"   Discount       : {discount:.2f}%")

        if discount > 25:
            score += 45
            signals.append(f"🔥 Crash zone ({discount:.1f}% off high)")
            logger.info(f"   ✅ {discount:.1f}% > 25% → +45 pts (crash zone — both thresholds)")
        elif discount > 15:
            score += 25
            signals.append(f"Below 52W high ({discount:.1f}%)")
            logger.info(f"   ✅ {discount:.1f}% > 15% → +25 pts")
        else:
            logger.info(f"   ➖ {discount:.1f}% < 15% — not a meaningful discount yet")

        # ── 200d MA ──
        logger.info("")
        logger.info("[ 200d Moving Average ]")
        logger.info(f"   200d MA        : ${data['avg_200d']:.2f}")
        logger.info(f"   Current price  : ${price:.2f}")
        if price < data["avg_200d"]:
            score += 10
            signals.append(f"Below 200d MA (${data['avg_200d']:.2f})")
            logger.info(f"   ✅ Price below MA → +10 pts (bear market or correction)")
        else:
            above_pct = ((price - data["avg_200d"]) / data["avg_200d"]) * 100
            logger.info(f"   ➖ Price {above_pct:.1f}% above 200d MA — no points")

        # ── RSI ──
        logger.info("")
        logger.info("[ RSI (14) ]")
        rsi = self.calculate_rsi(hist)
        data["rsi"] = rsi
        logger.info(f"   RSI            : {rsi:.2f}")
        logger.info(f"   Bands          : <35 oversold (+20) | <45 weak (+10) | >70 overbought (-15)")

        if rsi < 35:
            score += 20
            signals.append(f"Oversold RSI ({rsi:.0f})")
            logger.info(f"   ✅ RSI {rsi:.1f} < 35 → +20 pts (oversold)")
        elif rsi < 45:
            score += 10
            signals.append(f"Weak RSI ({rsi:.0f})")
            logger.info(f"   ✅ RSI {rsi:.1f} < 45 → +10 pts (weak momentum)")
        elif rsi > 70:
            score -= 15
            signals.append(f"⚠️ Overbought RSI ({rsi:.0f})")
            logger.info(f"   ⚠️  RSI {rsi:.1f} > 70 → -15 pts (overbought, avoid chasing)")
        else:
            logger.info(f"   ➖ RSI {rsi:.1f} is neutral — no adjustment")

        # ── vs Avg Cost ──
        logger.info("")
        logger.info("[ vs Your Avg Cost ]")
        below_avg = (AVG_COST - price) / AVG_COST * 100
        logger.info(f"   Your avg cost  : ${AVG_COST:.2f}")
        logger.info(f"   Current price  : ${price:.2f}")
        logger.info(f"   Difference     : {below_avg:+.2f}% ({'below' if below_avg > 0 else 'above'} your avg)")

        if below_avg > 10:
            score += 40
            signals.append(f"🔥 Deep below avg cost ({below_avg:.1f}%)")
            logger.info(f"   ✅ {below_avg:.1f}% below avg → +40 pts (all tiers: +15+10+15)")
        elif below_avg > 5:
            score += 25
            signals.append(f"5%+ below avg cost ({below_avg:.1f}%)")
            logger.info(f"   ✅ {below_avg:.1f}% below avg → +25 pts (two tiers: +15+10)")
        elif below_avg > 0:
            score += 15
            signals.append(f"Below avg cost ({below_avg:.1f}%)")
            logger.info(f"   ✅ {below_avg:.1f}% below avg → +15 pts")
        else:
            logger.info(f"   ➖ Price is {abs(below_avg):.1f}% above avg cost — no points from this signal")

        logger.info("")
        logger.info(f"   FINAL SCORE    : {score} / ~125 max")
        logger.info(f"   SIGNALS        : {signals if signals else 'none triggered'}")

        return score, pct_1y, avg_pct, discount, rsi, signals

    # ---------- ALLOCATION ----------
    def get_allocation(self, score):
        sep("ALLOCATION")
        dca = MONTHLY
        if score >= 85:
            extra = BONUS * 2
            tier  = f"STRONG BUY (score {score} ≥ 85) → 2× bonus"
        elif score >= 75:
            extra = BONUS
            tier  = f"BUY (score {score} ≥ 75) → 1× bonus"
        elif score >= SCORE_THRESHOLD:
            extra = BONUS * 0.5
            tier  = f"MILD BUY (score {score} ≥ {SCORE_THRESHOLD}) → 0.5× bonus"
        else:
            extra = 0
            tier  = f"NO BUY (score {score} < {SCORE_THRESHOLD})"

        logger.info(f"   Tier           : {tier}")
        logger.info(f"   DCA            : ${dca:.0f}  (always applied when market is open)")
        logger.info(f"   Bonus          : ${extra:.0f}  (score-gated extra allocation)")
        return dca, extra

    # ---------- BUDGET ----------
    def apply_budget(self, dca, extra):
        sep("BUDGET CHECK")
        spent     = self.state["monthly_spent"]
        remaining = MONTHLY_BUDGET - spent

        logger.info(f"   Monthly budget : ${MONTHLY_BUDGET:.0f}")
        logger.info(f"   Spent so far   : ${spent:.0f}")
        logger.info(f"   Remaining      : ${remaining:.0f}")
        logger.info(f"   Requested      : DCA=${dca:.0f} + Extra=${extra:.0f} = ${dca + extra:.0f}")

        if remaining <= 0:
            logger.info(f"   ❌ Budget fully exhausted — nothing to allocate")
            return 0, 0, 0

        if dca > remaining:
            logger.info(f"   ⚠️  DCA ${dca:.0f} exceeds remaining ${remaining:.0f}")
            logger.info(f"   ⚠️  Capping DCA to ${remaining:.0f} and dropping extra")
            return remaining, 0, remaining

        total = dca + extra
        if total > remaining:
            old_extra = extra
            extra     = remaining - dca
            total     = dca + extra
            logger.info(f"   ⚠️  Total ${dca + old_extra:.0f} > remaining ${remaining:.0f}")
            logger.info(f"   ⚠️  Trimming extra: ${old_extra:.0f} → ${extra:.0f} to fit budget")
        else:
            logger.info(f"   ✅ Full allocation ${total:.0f} fits within remaining ${remaining:.0f}")

        logger.info(f"   Final order    : DCA=${dca:.0f} + Extra=${extra:.0f} = ${total:.0f}")
        return dca, extra, total

    # ---------- DUPLICATE ----------
    def is_duplicate(self, score, dca, extra):
        key    = f"{score}:{round(dca)}:{round(extra)}"
        h      = hashlib.md5(key.encode()).hexdigest()
        last_h = self.state.get("last_msg_hash")

        logger.info(f"   Signal key     : '{key}'")
        logger.info(f"   Hash           : {h}")
        logger.info(f"   Previous hash  : {last_h or 'none (no prior alert)'}")

        if last_h == h:
            logger.info(f"   ❌ DUPLICATE — identical signal to last alert, skipping")
            return True

        logger.info(f"   ✅ Not a duplicate — new signal fingerprint")
        self.state["last_msg_hash"] = h
        return False

    # ---------- TELEGRAM ----------
    async def send(self, msg):
        try:
            logger.info(f"📲 Connecting to Telegram API...")
            bot = Bot(token=TOKEN)
            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")
            logger.info(f"✅ Message delivered to chat ID {CHAT_ID}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"❌ Telegram delivery failed: {e}")

    # ---------- SUMMARY ----------
    def print_summary(self, reason=""):
        sep("RUN COMPLETE")
        logger.info(f"   OUTCOME        : {self.result}")
        if reason:
            logger.info(f"   WHY NO BUY     : {reason}")
        sep("FINAL STATE")
        logger.info(f"   monthly_spent  : ${self.state['monthly_spent']:.2f} / ${MONTHLY_BUDGET:.2f}")
        logger.info(f"   budget left    : ${MONTHLY_BUDGET - self.state['monthly_spent']:.2f}")
        logger.info(f"   units_held     : {self.state.get('units_held', 0):.4f}")
        logger.info(f"   total_invested : ${self.state.get('total_invested', 0):.2f}")
        logger.info(f"   last_alert     : {self.state.get('last_alert') or 'never'}")
        sep()

    # ---------- MAIN ----------
    async def run(self):
        sep("DHHF BOT v9 START")
        now = datetime.now(TZ)
        logger.info(f"   Timestamp      : {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"   Weekday        : {now.strftime('%A')} (#{now.weekday()})")
        logger.info(f"   Config loaded  : AVG_COST=${AVG_COST:.2f} | MONTHLY=${MONTHLY:.0f} | BONUS=${BONUS:.0f} | BUDGET=${MONTHLY_BUDGET:.0f} | MIN_ORDER=${MIN_ORDER:.0f}")

        # ── GATE 1: Market hours ──
        sep("GATE 1 — MARKET HOURS")
        if not self.is_asx_open(now):
            self.result = f"SKIP — ASX closed ({now.strftime('%A %H:%M')})"
            self.print_summary("ASX is not trading right now. Bot only acts during market hours (Mon–Fri 10:00–16:10 AEST).")
            return

        # ── GATE 2: Valid check hour ──
        sep("GATE 2 — CHECK HOUR")
        min_score = self.get_min_score_for_hour(now.hour)
        if min_score is None:
            self.result = f"SKIP — {now.hour}:00 is not a check hour"
            self.print_summary(
                f"{now.hour}:00 AEST is not a scheduled check hour. "
                f"Always-on hours: {ALWAYS_HOURS}. "
                f"Conditional hours: {CONDITIONAL_HOURS}."
            )
            return

        # ── Month reset ──
        sep("MONTHLY BUDGET RESET")
        self.reset_month(now)
        logger.info(f"   Monthly spent  : ${self.state['monthly_spent']:.2f} / ${MONTHLY_BUDGET:.2f}")
        logger.info(f"   Remaining      : ${MONTHLY_BUDGET - self.state['monthly_spent']:.2f}")

        # ── Fetch data ──
        data = self.fetch_data()
        if not data:
            self.result = "ERROR — Data fetch failed"
            self.print_summary("yfinance returned no usable data. Check if ASX is in a holiday or if yfinance is rate-limited.")
            return

        price = data["price"]

        # ── Scoring ──
        score, pct_1y, avg_pct, discount, rsi, signals = self.calculate_score(data)

        # ── GATE 3: Global score threshold ──
        sep("GATE 3 — GLOBAL SCORE THRESHOLD")
        logger.info(f"   Score          : {score}")
        logger.info(f"   Threshold      : {SCORE_THRESHOLD}")
        if score < SCORE_THRESHOLD:
            self.result = f"NO BUY — Score {score} < {SCORE_THRESHOLD}"
            self.print_summary(
                f"Score {score} is below global minimum of {SCORE_THRESHOLD}. "
                f"Conditions that would raise it: "
                f"percentile < 20% (currently avg {avg_pct:.0f}%), "
                f"RSI < 45 (currently {rsi:.0f}), "
                f"52W discount > 15% (currently {discount:.1f}%), "
                f"price below avg cost ${AVG_COST:.2f} (currently ${price:.2f}). "
                f"Signals triggered: {signals if signals else 'none'}."
            )
            return
        logger.info(f"   ✅ Score {score} ≥ {SCORE_THRESHOLD} — passes global threshold")

        # ── GATE 4: Hour-conditional score gate ──
        sep("GATE 4 — HOUR SCORE GATE")
        logger.info(f"   Score          : {score}")
        logger.info(f"   Required at {now.hour}:00 : ≥ {min_score}")
        if score < min_score:
            self.result = f"NO BUY — Score {score} < {min_score} at {now.hour}:00"
            self.print_summary(
                f"Score {score} passed the global threshold ({SCORE_THRESHOLD}) but "
                f"failed the {now.hour}:00 conditional gate (requires ≥ {min_score}). "
                f"Mid-session hours require a stronger signal. "
                f"This score would trigger a buy at 10:00 or 16:00."
            )
            return
        logger.info(f"   ✅ Score {score} passes the {now.hour}:00 hour gate")

        # ── GATE 5: Cooldown ──
        sep("GATE 5 — COOLDOWN")
        last = self.state.get("last_alert")
        if last:
            last_time = datetime.fromisoformat(last)
            hours_ago = (now - last_time).total_seconds() / 3600
            next_eligible = last_time + timedelta(hours=COOLDOWN_HOURS)
            logger.info(f"   Last alert     : {last_time.strftime('%Y-%m-%d %H:%M %Z')}")
            logger.info(f"   Hours ago      : {hours_ago:.2f}h")
            logger.info(f"   Cooldown       : {COOLDOWN_HOURS}h required")
            logger.info(f"   Next eligible  : {next_eligible.strftime('%Y-%m-%d %H:%M %Z')}")
            if hours_ago < COOLDOWN_HOURS:
                self.result = f"NO BUY — Cooldown ({hours_ago:.1f}h/{COOLDOWN_HOURS}h elapsed)"
                self.print_summary(
                    f"Last buy alert was only {hours_ago:.1f}h ago. "
                    f"Cooldown prevents duplicate signals within {COOLDOWN_HOURS}h. "
                    f"Next eligible window: {next_eligible.strftime('%H:%M %Z')}."
                )
                return
            logger.info(f"   ✅ Cooldown cleared ({hours_ago:.1f}h ≥ {COOLDOWN_HOURS}h)")
        else:
            logger.info(f"   ✅ No previous alert on record — cooldown not applicable")

        # ── Raw allocation ──
        dca, extra = self.get_allocation(score)
        logger.info(f"   Pre-filter     : DCA=${dca:.0f} | Extra=${extra:.0f}")

        # ── Momentum filter ──
        sep("MOMENTUM FILTER")
        if extra > 0:
            stabilising = self.is_stabilising(data["hist"])
            if not stabilising:
                old_extra = extra
                extra    *= 0.5
                logger.info(f"   ⚠️  Extra halved: ${old_extra:.0f} → ${extra:.0f} (price still falling, reduce risk)")
            else:
                logger.info(f"   ✅ Keeping full extra ${extra:.0f} (price stabilised)")
        else:
            logger.info(f"   ➖ Extra is $0 — momentum filter not applicable")

        # ── Budget ──
        dca, extra, total = self.apply_budget(dca, extra)

        # ── GATE 6: Budget / min order ──
        sep("GATE 6 — BUDGET & MIN ORDER")
        if total <= 0:
            self.result = "NO BUY — Budget exhausted"
            self.print_summary(
                f"Monthly budget of ${MONTHLY_BUDGET:.0f} is fully used "
                f"(${self.state['monthly_spent']:.0f} spent this month). "
                f"Budget resets on the 1st of next month."
            )
            return

        logger.info(f"   Order total    : ${total:.0f}")
        logger.info(f"   Min order size : ${MIN_ORDER:.0f}")
        if total < MIN_ORDER:
            self.result = f"NO BUY — Order ${total:.0f} < min ${MIN_ORDER:.0f}"
            self.print_summary(
                f"Remaining budget only allows ${total:.0f} which is below "
                f"the minimum order size of ${MIN_ORDER:.0f}. "
                f"Consider lowering MIN_ORDER in your GitHub Actions Variables."
            )
            return
        logger.info(f"   ✅ Order size ${total:.0f} is above minimum ${MIN_ORDER:.0f}")

        # ── Portfolio calcs ──
        sep("PORTFOLIO CALCS")
        units_buying   = total / price
        units_held     = float(self.state.get("units_held",     0.0))
        total_invested = float(self.state.get("total_invested", 0.0))
        new_units      = units_held + units_buying
        new_invested   = total_invested + total
        new_avg        = new_invested / new_units if new_units > 0 else price
        portfolio_val  = new_units * price
        unrealised     = portfolio_val - new_invested if new_invested > 0 else 0.0
        vs_avg         = ((price - AVG_COST) / AVG_COST) * 100

        logger.info(f"   Units buying   : {units_buying:.4f} @ ${price:.2f}")
        logger.info(f"   Units before   : {units_held:.4f}")
        logger.info(f"   Units after    : {new_units:.4f}")
        logger.info(f"   Invested before: ${total_invested:.2f}")
        logger.info(f"   Invested after : ${new_invested:.2f}")
        logger.info(f"   New avg cost   : ${new_avg:.4f}  (was ${AVG_COST:.2f} seeded from env)")
        logger.info(f"   Portfolio val  : ${portfolio_val:.2f}")
        logger.info(f"   Unrealised P&L : ${unrealised:+.2f}")
        logger.info(f"   vs Seed avg    : {vs_avg:+.2f}%")

        # ── Duplicate check ──
        sep("DUPLICATE CHECK")
        if self.is_duplicate(score, dca, extra):
            self.result = f"NO BUY — Duplicate signal (score={score}, dca={dca:.0f}, extra={extra:.0f})"
            self.print_summary(
                "Signal fingerprint matches last alert. "
                "Score and allocation are unchanged — no new information to send."
            )
            return

        # ── Build & send message ──
        sep("SENDING ALERT")
        avg_icon = "📈" if vs_avg >= 0 else "📉"
        budget_after = MONTHLY_BUDGET - self.state["monthly_spent"] - total

        msg = f"""🎯 *DHHF BUY SIGNAL*
📅 {now.strftime('%d %b %Y %H:%M')} AEST

*Score: {score}/100*
📌 {', '.join(signals) if signals else 'None'}

💲 Price: ${price:.2f} ({data['change']:+.2f}%)
{avg_icon} vs Avg Cost: {vs_avg:+.1f}% (yours: ${AVG_COST:.2f})
📉 52W Drawdown: {discount:.1f}%
📊 1Y Percentile: {pct_1y:.0f}% | RSI: {rsi:.0f}

💸 *Buy Order*
├ DCA:   ${dca:.0f}
├ Extra: ${extra:.0f}
└ Total: ${total:.0f} ({units_buying:.2f} units)

📈 *Portfolio After Buy*
├ Units:   {new_units:.2f} @ avg ${new_avg:.2f}
├ Value:   ${portfolio_val:.0f}
└ P&L:     ${unrealised:+.0f}

📊 Budget left this month: ${budget_after:.0f}"""

        logger.info(f"Message preview:")
        for line in msg.strip().split("\n"):
            logger.info(f"   {line}")

        await self.send(msg)

        # ── Update state ──
        self.state["monthly_spent"]  += total
        self.state["last_alert"]      = now.isoformat()
        self.state["last_price"]      = price
        self.state["units_held"]      = new_units
        self.state["total_invested"]  = new_invested
        self.save()

        self.result = f"BUY SENT — ${total:.0f} ({units_buying:.4f} units @ ${price:.2f})"
        self.print_summary()


# ================= RUN ================= #
if __name__ == "__main__":
    asyncio.run(DHHFBot().run())
