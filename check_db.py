import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Fallback: Try to load from ../web/.env.local if not found
if not os.getenv("NEXT_PUBLIC_SUPABASE_URL"):
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'web', '.env.local')
    load_dotenv(dotenv_path=env_path)

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

response = supabase.table("crt_signals").select("*", count="exact").execute()
print(f"Total signals in DB: {len(response.data)}")
if len(response.data) > 0:
    print("Sample signal:", response.data[0])
