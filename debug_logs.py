import os
import logging
from supabase import create_client
from dotenv import load_dotenv

def test_logging():
    if os.path.exists(".env.local"):
        load_dotenv(".env.local")
        print("Loaded .env.local")
    else:
        print(".env.local NOT found")

    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY")
    
    print(f"URL: {url}")
    print(f"Key present: {bool(key)}")

    if not url or not key:
        print("FAILED: Missing credentials")
        return

    supabase = create_client(url, key)
    
    # Test direct insert
    try:
        print("Testing direct insert into system_logs...")
        res = supabase.table("system_logs").insert({
            "level": "INFO", 
            "message": "DEBUG TEST FROM SCRIPT", 
            "source": "debug_test"
        }).execute()
        print(f"Direct Insert Result: {res}")
    except Exception as e:
        print(f"Direct Insert FAILED: {e}")

if __name__ == "__main__":
    test_logging()
