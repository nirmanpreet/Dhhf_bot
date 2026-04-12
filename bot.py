#!/usr/bin/env python3

import os
import json
import logging
import hashlib
import asyncio
from datetime import datetime, timedelta
import pytz

import yfinance as yf
from telegram import Bot

# ================= CONFIG ================= #
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

AVG_COST = float(os.getenv('AVERAGE_COST', '38.5'))
MONTHLY = float(os.getenv('MONTHLY_AMOUNT', '1000'))
BONUS = float(os.getenv('BONUS_AMOUNT', '1000'))
MONTHLY_BUDGET = float(os.getenv('MONTHLY_BUDGET', '3000'))

STATE_FILE = 'bot_state.json'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TZ = pytz.timezone('Australia/Melbourne')


# ================= BOT ================= #
class DHHFBot:

    def __init__(self):
        self.state = self.load_state()

    # ---------- STATE ----------
    def load_state(self):
        defaults = {
            "last_price": None,
            "last_alert": None,
            "last_msg_hash": None,
            "monthly_spent": 0,
            "current_month": None
        }

        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    defaults.update(json.load(f))
            except:
                pass

        return defaults

    def save(self):
        with open(STATE_FILE, "w") as f:
            json.dump(self.state, f, indent=2)

    def reset_month(self, now):
        if self.state["current_month"] != now.month:
            self.state["monthly_spent"] = 0
            self.state["current_month"] = now.month

    # ---------- ASX HOURS ----------
    def is_asx_open(self, now):
        if now.weekday() >= 5:
            return False

        open_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=16, minute=10, second=0, microsecond=0)

        return open_time <= now <= close_time

    # ---------- DATA ----------
    def fetch_data(self):
        ticker = yf.Ticker("DHHF.AX")

        intraday = ticker.history(period="1d", interval="1m")
        hist = ticker.history(period="1y")

        if intraday.empty or hist.empty:
            return None

        price = intraday["Close"].iloc[-1]
        prev_close = ticker.history(period="2d")["Close"].iloc[-2]
        change = ((price - prev_close) / prev_close) * 100

        return {
            "price": price,
            "change": change,
            "hist": hist["Close"],
            "high_52w": hist["Close"].max(),
            "avg_200d": hist["Close"].rolling(200).mean().iloc[-1],
        }

    # ---------- SCORE ----------
    def calculate_score(self, data):
        price = data["price"]
        hist = data["hist"]

        score = 0
        signals = []

        percentile = (hist <= price).mean() * 100
        if percentile < 20:
            score += 30
            signals.append("Value zone")

        discount = (data["high_52w"] - price) / data["high_52w"] * 100
        if discount > 15:
            score += 25
            signals.append("Below 52W high")

        if discount > 25:
            score += 20
            signals.append("Crash zone")

        if price < data["avg_200d"]:
            score += 10
            signals.append("Bear market")

        return score, percentile, discount, signals

    # ---------- ALLOCATION ----------
    def get_allocation(self, score):
        dca = MONTHLY
        extra = 0

        if score < 60:
            extra = 0
        elif score < 75:
            extra = BONUS * 0.5
        elif score < 85:
            extra = BONUS
        else:
            extra = BONUS * 2

        return dca, extra

    # ---------- BUDGET ----------
    def apply_budget(self, dca, extra):
        remaining = MONTHLY_BUDGET - self.state["monthly_spent"]

        if remaining <= 0:
            return 0, 0, 0

        if dca > remaining:
            return remaining, 0, remaining

        total = dca + extra

        if total > remaining:
            extra = remaining - dca

        total = dca + extra
        return dca, extra, total

    # ---------- DUPLICATE ----------
    def is_duplicate(self, msg):
        h = hashlib.md5(msg.encode()).hexdigest()
        if self.state.get("last_msg_hash") == h:
            return True
        self.state["last_msg_hash"] = h
        return False

    # ---------- TELEGRAM ----------
    async def send(self, msg):
        try:
            bot = Bot(token=TOKEN)
            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(e)

    # ---------- MAIN ----------
    async def run(self):
        now = datetime.now(TZ)

        if not self.is_asx_open(now):
            return

        # reduce noise (optional)
        if now.hour not in [10, 12, 14, 16]:
            return

        self.reset_month(now)

        data = self.fetch_data()
        if not data:
            return

        score, percentile, discount, signals = self.calculate_score(data)

        # cooldown protection
        last = self.state.get("last_alert")
        if last:
            if (now - datetime.fromisoformat(last)) < timedelta(hours=4):
                return

        dca, extra = self.get_allocation(score)
        dca, extra, total = self.apply_budget(dca, extra)

        if score < 60 or total <= 0:
            return

        msg = f"""🎯 *DHHF BUY SIGNAL*

Score: {score}

💰 Price: ${data['price']:.2f}
📉 Percentile: {percentile:.0f}
📉 Drawdown: {discount:.1f}%

💰 DCA: ${dca:.0f}
🚀 Extra: ${extra:.0f}
💵 Total: ${total:.0f}

📊 Budget Left: ${MONTHLY_BUDGET - self.state['monthly_spent']:.0f}
"""

        if not self.is_duplicate(msg):
            await self.send(msg)

            self.state["monthly_spent"] += total
            self.state["last_alert"] = now.isoformat()

        self.state["last_price"] = data["price"]
        self.save()


# ================= RUN ================= #
if __name__ == "__main__":
    asyncio.run(DHHFBot().run())
    print("completed")
