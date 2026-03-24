import os
import json
from supabase import create_client, Client
from pywebpush import webpush, WebPushException
from dotenv import load_dotenv

# Carica variabili d'ambiente
load_dotenv(".env.local")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY")
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY")
VAPID_CLAIMS = {"sub": "mailto:admin@crt-flow.com"}

if not all([SUPABASE_URL, SUPABASE_KEY, VAPID_PRIVATE_KEY]):
    print("❌ Errore: Variabili d'ambiente mancanti in .env.local")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def test_push():
    # Recupera l'ultima sottoscrizione inserita per testare
    response = supabase.table("user_push_subscriptions").select("*").order("created_at", desc=True).limit(1).execute()
    
    if not response.data:
        print("❌ Nessuna sottoscrizione trovata nella tabella 'user_push_subscriptions'.")
        print("Assicurati di aver cliccato sulla campanellina nella WebApp!")
        return

    sub = response.data[0]
    print(f"🔔 Invio notifica test a: {sub['endpoint'][:50]}...")

    payload = json.dumps({
        "title": "CRT Flow Test",
        "body": "Se vedi questo, il Service Worker funziona! 🚀",
        "icon": "/logo.png",
        "url": "/dashboard/smart-dca"
    })

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
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims=VAPID_CLAIMS
        )
        print("✅ Notifica inviata con successo!")
    except WebPushException as ex:
        print(f"❌ Errore WebPush: {ex}")
    except Exception as ex:
        print(f"❌ Errore generale: {ex}")

if __name__ == "__main__":
    test_push()
