import os
import time
import logging
import yfinance as yf
import requests
from supabase import create_client, Client
from dotenv import load_dotenv
from notifications import send_telegram_alert # Reuse existing infra
from pywebpush import webpush, WebPushException
import json

# 1. SETUP LOGGING
logger = logging.getLogger("DcaNotifier")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')

class SupabaseLoggingHandler(logging.Handler):
    def __init__(self, supabase_client):
        super().__init__()
        self.supabase = supabase_client
        self.source = "dca_monitor"

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if "system_logs" in log_entry: return
            self.supabase.table("system_logs").insert({
                "level": record.levelname, "message": log_entry, "source": self.source
            }).execute()
        except: pass

def setup_logging(supabase_client):
    # Console Handler - Force UTF-8 for Windows compatibility (Emojis)
    import sys
    if sys.platform == "win32":
        import io
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
        except:
            pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Supabase Handler
    if supabase_client:
        sb_handler = SupabaseLoggingHandler(supabase_client)
        sb_handler.setFormatter(formatter)
        logger.addHandler(sb_handler)

# Supabase Config
def setup_supabase():
    if os.path.exists(".env.local"):
        load_dotenv(".env.local")
    
    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    
    if url and key:
        return create_client(url, key)
    return None

supabase: Client = setup_supabase()

# Initialize Logging with Supabase
setup_logging(supabase)

def get_active_plans():
    """Fetch all active DCA plans from Supabase."""
    try:
        response = supabase.table("user_dca_plans").select("*").eq("status", "active").execute()
        return response.data
    except Exception as e:
        logger.error(f"Error fetching plans: {e}")
        return []

def update_plan_levels(plan_id, updated_levels):
    """Update the levels JSONB in Supabase."""
    try:
        supabase.table("user_dca_plans").update({"levels": updated_levels}).eq("id", plan_id).execute()
        logger.info(f"Updated levels for Plan {plan_id}")
    except Exception as e:
        logger.error(f"Error updating levels: {e}")

def send_dca_notification(symbol, level, price, amount):
    """Specific DCA Notification via Telegram."""
    # We can reuse send_telegram_alert by mock-up a signal object
    msg = (
        f"🚨 **CRT Flow Alert: {symbol} DCA Level Hit!**\n\n"
        f"The price dropped to **{price:.2f}**, reaching your **Tranche n° {level}**.\n\n"
        f"💰 **Action Required:** Invest **${amount:.2f}** via your broker.\n"
        f"✅ **Next Step:** Confirm execution in your Dashboard to update your portfolio metrics.\n\n"
        f"🔗 [Open Dashboard](https://www.crt-flow.com/dashboard/smart-dca)"
    )
    
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
        try:
            requests.post(url, json=payload, timeout=5)
            logger.info(f"Telegram DCA alert sent for {symbol}")
        except Exception as e:
            logger.error(f"Failed to send Telegram: {e}")

def send_push_notification(user_id, symbol, title, message):
    """Invia notifiche Push Browser agli endpoint registrati."""
    vapid_private = os.getenv("VAPID_PRIVATE_KEY")
    vapid_claims = {"sub": "mailto:admin@crt-flow.com"}

    if not vapid_private:
        return

    try:
        # Recupera le sottoscrizioni dell'utente
        response = supabase.table("user_push_subscriptions").select("*").eq("user_id", user_id).execute()
        subscriptions = response.data
        
        if not subscriptions:
            return

        payload = json.dumps({
            "title": title,
            "body": message,
            "icon": "/logo.png",
            "url": f"/dashboard/smart-dca?symbol={symbol}"
        })

        for sub in subscriptions:
            try:
                webpush(
                    subscription_info={
                        "endpoint": sub["endpoint"],
                        "keys": {
                            "p256dh": sub["p256dh"],
                            "auth": sub["auth"]
                        }
                    },
                    data=payload,
                    vapid_private_key=vapid_private,
                    vapid_claims=vapid_claims
                )
                logger.info(f"✅ Web Push sent to endpoint {sub['id']}")
            except WebPushException as ex:
                logger.error(f"❌ WebPushException: {ex}")
                # Se l'endpoint è scaduto (410 Gone), lo rimuoviamo
                if ex.response and ex.response.status_code == 410:
                    supabase.table("user_push_subscriptions").delete().eq("id", sub["id"]).execute()
            except Exception as ex:
                logger.error(f"❌ Errore Web Push: {ex}")

    except Exception as e:
        logger.error(f"❌ Errore generale Web Push: {e}")

def monitor_plans():
    """Fetches active plans, checks prices, and sends notifications once."""
    logger.info("Starting one-shot DCA Monitor...")
    
    plans = get_active_plans()
    if not plans:
        logger.info("No active plans found. Exiting.")
        return

    # Group by symbol to minimize yfinance calls
    symbols = list(set([p['symbol'] for p in plans]))
    prices = {}
    if symbols:
        import concurrent.futures
        logger.info(f"Downloading real-time prices for {len(symbols)} tickers...")
        
        def fetch_price(symbol):
            try:
                ticker = yf.Ticker(symbol)
                # fast_info is extremely fast and robust for current/last price
                price = ticker.fast_info.get('lastPrice')
                if price is not None:
                    return symbol, float(price)
            except Exception as e:
                logger.error(f"Error fetching live price for {symbol}: {e}")
            return symbol, None

        # Fetch in parallel for speed and reliability
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(fetch_price, symbols)
            for sym, price in results:
                if price is not None:
                    prices[sym] = price
                else:
                    logger.warning(f"No valid price found for {sym}")

    for plan in plans:
        symbol = plan['symbol']
        current_price = prices.get(symbol)
        if not current_price: continue

        levels = plan.get('levels', [])
        updated = False
        
        for level in levels:
            # FIX 1: Correct JSONB field names
            # FIX 2: Skip Level 1 (Starter Position)
            level_num = level.get('level', 1)
            trigger_price = level.get('price', 0)
            allocate_amount = level.get('amount', 0)

            if level_num == 1:
                continue

            # Trigger logic: Price <= Trigger Price AND status == 'pending'
            if current_price <= trigger_price and level.get('status') == 'pending':
                logger.info(f"🔥 TRIGGER: {symbol} Level {level_num} hit! Current: {current_price} Target: {trigger_price}")
                
                # Update status to notified
                level['status'] = 'notified'
                level['notified_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                updated = True
                
                # Send notification
                send_dca_notification(symbol, level_num, trigger_price, allocate_amount)
                
                # Insert into user_notifications for the webapp
                try:
                    supabase.table("user_notifications").insert({
                        "user_id": plan['user_id'],
                        "type": "dca_hit",
                        "symbol": symbol,
                        "title": f"DCA Level {level_num} Hit: {symbol}",
                        "message": f"The price dropped to {current_price:.2f}, reaching your Tranche n° {level_num}.",
                        "price": current_price,
                        "status": "unread"
                    }).execute()
                    logger.info(f"Webapp notification saved for {symbol}")
                except Exception as ne:
                    logger.error(f"Failed to save webapp notification: {ne}")
                
                # 3. Web Push (Mobile/Browser)
                send_push_notification(plan['user_id'], symbol, f"DCA Level {level_num} Hit: {symbol}", f"The price dropped to {current_price:.2f}, reaching your Tranche n° {level_num}.")

        if updated:
            update_plan_levels(plan['id'], levels)

    logger.info("Monitoring cycle complete.")

if __name__ == "__main__":
    monitor_plans()
