#!/usr/bin/env python3
"""DHHF Ultimate Bot v3.3 - Production Ready"""

import os
import json
import logging
from datetime import datetime
import asyncio
import yfinance as yf
import pandas as pd
import numpy as np
from telegram import Bot

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Config from GitHub Secrets
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
AVG_COST = float(os.getenv('AVERAGE_COST', '38.50'))
MONTHLY = float(os.getenv('MONTHLY_AMOUNT', '1000'))
BONUS = float(os.getenv('BONUS_AMOUNT', '1000'))

STATE_FILE = 'bot_state.json'


class DHHFBot:
    def __init__(self):
        self.state = self.load_state()
    
    def load_state(self):
        defaults = {
            'avg_cost': AVG_COST,
            'total_invested': 0,
            'monthly_count': 0,
            'dip_count': 0,
            'last_monthly': None,
            'last_alert': None,
            'last_price': None
        }
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    defaults.update(json.load(f))
            except:
                pass
        return defaults
    
    def save(self):
        with open(STATE_FILE, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def fetch_data(self):
        try:
            ticker = yf.Ticker("DHHF.AX")
            info = ticker.info
            
            current = info.get('regularMarketPrice') or info.get('navPrice')
            if not current:
                logger.error("No price data available")
                return None
            
            # Fix percentage calculation
            change_raw = info.get('regularMarketChangePercent', 0)
            if change_raw and abs(change_raw) < 0.5:
                change = change_raw * 100
            else:
                change = change_raw
            
            # Double-check with calculated value
            prev_close = info.get('previousClose')
            if prev_close and current and prev_close > 0:
                calc_change = ((current - prev_close) / prev_close) * 100
                if abs(change) > 10 and abs(calc_change) < 10:
                    change = calc_change
            
            hist = ticker.history(period="1y")
            if hist.empty:
                logger.error("No historical data")
                return None
            
            return {
                'price': current,
                'change': change,
                'high_52w': hist['Close'].max(),
                'low_52w': hist['Close'].min(),
                'avg_50d': hist['Close'].rolling(50).mean().iloc[-1],
                'avg_200d': hist['Close'].rolling(200).mean().iloc[-1],
                'hist': hist['Close']
            }
        except Exception as e:
            logger.error(f"Data fetch error: {e}")
            return None
    
    def calculate_score(self, price, data):
        hist = data['hist']
        score = 0
        signals = []
        
        # 1. Historical percentile (30%)
        percentile = (hist <= price).mean() * 100
        if percentile <= 10:
            score += 30
            signals.append("🔥 10th percentile - rare discount!")
        elif percentile <= 20:
            score += 24
            signals.append("✅ 20th percentile - great entry")
        elif percentile <= 30:
            score += 18
            signals.append("👍 30th percentile - good value")
        
        # 2. 52-week high distance (25%)
        discount = (data['high_52w'] - price) / data['high_52w'] * 100
        if discount >= 15:
            score += 25
            signals.append(f"🔥 {discount:.1f}% below 52w high")
        elif discount >= 10:
            score += 20
            signals.append(f"✅ {discount:.1f}% below 52w high")
        elif discount >= 5:
            score += 12
            signals.append(f"📉 {discount:.1f}% below 52w high")
        
        # 3. vs Your cost (20%)
        your_discount = (AVG_COST - price) / AVG_COST * 100
        if your_discount >= 5:
            score += 20
            signals.append(f"🎯 {your_discount:.1f}% below YOUR cost!")
        elif your_discount >= 2:
            score += 15
            signals.append(f"✅ {your_discount:.1f}% below your cost")
        
        # 4. Moving averages (15%)
        if price < data['avg_200d'] and price < data['avg_50d']:
            score += 15
            signals.append("📈 Below 50d & 200d MA")
        elif price < data['avg_50d']:
            score += 10
            signals.append("📊 Below 50d MA")
        
        # 5. Daily momentum (10%)
        if data['change'] <= -3:
            score += 10
            signals.append(f"🔻 Big drop {data['change']:.1f}%")
        elif data['change'] <= -1.5:
            score += 6
            signals.append(f"📉 Down {data['change']:.1f}%")
        
        return score, percentile, signals, your_discount
    
    def get_urgency(self, score):
        if score >= 75:
            return "🔥 URGENT", True, f"BUY NOW - ${MONTHLY:.0f} + ${BONUS:.0f} bonus!"
        elif score >= 60:
            return "✅ STRONG", True, f"Good time - ${MONTHLY:.0f} + ${BONUS/2:.0f} extra"
        elif score >= 45:
            return "👍 MODERATE", True, f"Fair - ${MONTHLY:.0f} regular"
        elif score >= 30:
            return "⚖️ NEUTRAL", False, "Wait for better"
        else:
            return "⏳ WEAK", False, "Don't buy - too expensive"
    
    async def send(self, msg):
        try:
            bot = Bot(token=TOKEN)
            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode='Markdown')
            logger.info("Message sent successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    async def run(self):
        now = datetime.now()
        hour = now.hour
        
        # Fetch data
        data = self.fetch_data()
        if not data or not data['price']:
            logger.error("Failed to fetch DHHF data")
            return
        
        price = data['price']
        change = data['change']
        
        logger.info(f"DHHF: ${price:.2f} ({change:+.2f}%)")
        
        score, percentile, signals, your_disc = self.calculate_score(price, data)
        urgency, should_buy, action = self.get_urgency(score)
        
        messages = []
        
        # 1. DAILY SUMMARY (9am AEST = 23:00 UTC)
        if hour == 23:
            pnl = (price - AVG_COST) / AVG_COST * 100
            emoji = "📈" if pnl >= 0 else "📉"
            
            msg = f"""📊 *DHHF DAILY SUMMARY - {now.strftime('%A, %B %d')}*

💰 Price: `${price:.2f}` ({change:+.2f}% today)
🎯 Score: {score}/100 - {urgency}

📈 Market Context:
• 52w Range: ${data['low_52w']:.2f} - ${data['high_52w']:.2f}
• From High: `{((data['high_52w']-price)/data['high_52w']*100):.1f}%` discount
• Historical: {percentile:.0f}th percentile
• 50d MA: ${data['avg_50d']:.2f} | 200d MA: ${data['avg_200d']:.2f}

💼 Your Position:
• Avg Cost: ${AVG_COST:.2f}
• vs Your Cost: `{your_disc:+.1f}%`
• P&L: `{pnl:+.2f}%` {emoji}

💡 {action}

📅 Monthly: {self.state['monthly_count']} | 🎯 Dips: {self.state['dip_count']}"""
            
            messages.append(msg)
        
        # 2. MONTHLY DCA REMINDER (Day 1 or 25, 9am AEST)
        if (now.day == 1 or now.day == 25) and hour == 23:
            can_remind = True
            if self.state['last_monthly']:
                last = datetime.fromisoformat(self.state['last_monthly'])
                if last.month == now.month:
                    can_remind = False
            
            if can_remind:
                msg = f"""📅 *MONTHLY DCA DAY - {now.strftime('%B %d')}*

💰 Price: `${price:.2f}` (Score: {score}/100)
{urgency}

💡 {action}

📊 Context:
• 52w: ${data['low_52w']:.2f} - ${data['high_52w']:.2f}
• From high: `{((data['high_52w']-price)/data['high_52w']*100):.1f}%` discount
• Your avg: ${AVG_COST:.2f} (`{your_disc:+.1f}%`)
• Historical: {percentile:.0f}th percentile"""
                
                messages.append(msg)
                self.state['last_monthly'] = now.isoformat()
        
        # 3. BUY OPPORTUNITY ALERT (Score >= 60)
        if should_buy and score >= 60:
            can_alert = True
            if self.state['last_alert']:
                last = datetime.fromisoformat(self.state['last_alert'])
                if (now - last).days < 1:
                    can_alert = False
            
            if can_alert:
                sig_text = "\n".join([f"• {s}" for s in signals[:3]])
                
                msg = f"""🎯 *BUY OPPORTUNITY ALERT*

{urgency} | Score: {score}/100

💰 Price: `${price:.2f}` ({change:+.2f}% today)
📊 {percentile:.0f}th percentile

✅ Signals:
{sig_text}

💼 Your cost: ${AVG_COST:.2f} ({your_disc:+.1f}%)

💡 {action}"""
                
                messages.append(msg)
                self.state['last_alert'] = now.isoformat()
                self.state['dip_count'] += 1
        
        # Send all messages
        for msg in messages:
            await self.send(msg)
            await asyncio.sleep(1)
        
        # Save state
        self.state['last_price'] = price
        self.save()
        
        logger.info(f"Sent {len(messages)} messages")


if __name__ == '__main__':
    bot = DHHFBot()
    asyncio.run(bot.run())
