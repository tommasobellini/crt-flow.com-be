import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env.local")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not TOKEN:
    print("❌ Errore: TELEGRAM_BOT_TOKEN non trovato nel file .env.local")
    exit(1)

def get_chat_id():
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    try:
        response = requests.get(url)
        data = response.json()
        
        if not data.get("ok"):
            print(f"❌ Errore API Telegram: {data}")
            return

        updates = data.get("result", [])
        if not updates:
            print("⚠️ Nessun messaggio trovato. Invia un messaggio (es. '/start') al tuo bot su Telegram e riprova.")
            return

        print("\n✅ Trovati i seguenti Chat ID:")
        print("\n✅ Trovati i seguenti Chat ID:")
        seen_ids = set()
        for update in updates:
            chat = None
            if "message" in update:
                chat = update["message"]["chat"]
            elif "channel_post" in update:
                chat = update["channel_post"]["chat"]
            elif "my_chat_member" in update:
                chat = update["my_chat_member"]["chat"]
            
            if chat:
                chat_id = chat["id"]
                if chat_id in seen_ids: continue
                seen_ids.add(chat_id)
                
                title = chat.get("title", chat.get("username", "N/A"))
                type_Str = chat.get("type", "private")
                print(f"🔹 Chat ID: {chat_id} | Name: {title} | Type: {type_Str}")
        
        print("\n👉 Copia il Chat ID corretto nel tuo file .env.local alla voce TELEGRAM_CHAT_ID")

    except Exception as e:
        print(f"❌ Errore di connessione: {e}")

if __name__ == "__main__":
    get_chat_id()
