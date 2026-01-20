import os
from supabase import create_client, Client
from dotenv import load_dotenv
import time

# Load env - consistent with check_db.py
load_dotenv()

# Fallback: Try to load from ../web/.env.local if not found or if running from root and variables missing
if not os.getenv("NEXT_PUBLIC_SUPABASE_URL"):
    # Attempt 1: Try sibling web folder (assuming script is in scanner folder)
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'web', '.env.local')
    load_dotenv(dotenv_path=env_path)
    
    # Attempt 2: Try current folder .env.local (if running inside scanner)
    if not os.getenv("NEXT_PUBLIC_SUPABASE_URL"):
         load_dotenv(".env.local")

URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
# Prefer Service Role for writes/maintenance, Fallback to Anon
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

if not URL or not KEY:
    print(f"❌ Missing Supabase Credentials. \nURL: {URL}\nKEY found: {'Yes' if KEY else 'No'}")
    print("Check .env.local in 'web' folder or 'scanner' folder.")
    exit(1)

supabase: Client = create_client(URL, KEY)

def send_test_signal():
    print("🚀 Sending TEST signal to Supabase...")
    
    test_signal = {
        "symbol": "TEST-ETH", # Changed to ETH for variety
        "type": "bearish_sweep_1h",
        "timeframe": "1h",
        "price": 3000.0,
        "is_active": True,
        "session_tag": "TEST_SESSION",
        "range_high": 3100,
        "range_low": 2900,
        "detected_at": time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime()) # Ensure current timestamp
    }

    try:
        data, count = supabase.table("crt_signals").insert(test_signal).execute()
        # Note: data might be just [data] or property depending on lib version, but print(data) handles it.
        print("✅ Signal Inserted successfully!")
        print(f"Symbol: {test_signal['symbol']} | Price: {test_signal['price']}")
        print("👀 Check your Dashboard/Bell now!")
    except Exception as e:
        print(f"❌ Error inserting signal: {e}")

if __name__ == "__main__":
    send_test_signal()
