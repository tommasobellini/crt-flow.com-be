import os
import requests
import logging

logger = logging.getLogger(__name__)

def send_telegram_alert(signal, market_bias='NEUTRAL'):
    """
    Invia un alert Telegram per un segnale rilevato.
    Richiede TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID nelle variabili d'ambiente.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logger.warning("Credenziali Telegram mancanti. Alert non inviato.")
        return

    symbol = signal.get('symbol', 'UNKNOWN')
    tf = signal.get('timeframe', '?')
    s_type = signal.get('type', '')
    price = signal.get('price', 0)
    sl = signal.get('stop_loss', 0)
    tp = signal.get('take_profit', 0)
    rr = signal.get('rr_ratio', 0)
    tier = signal.get('liquidity_tier', 'Minor')
    session = signal.get('session_tag', 'None')

    # Calculate Confidence Score (Proxy for "Elite Score")
    score = 50 # Base
    if tier == 'Major': score += 20
    if tf in ['4h', '1d', '1w']: score += 20
    elif tf == '1h': score += 10
    
    is_aligned = False
    if market_bias != 'NEUTRAL':
        if (market_bias == 'BULLISH' and "bullish" in s_type) or \
           (market_bias == 'BEARISH' and "bearish" in s_type):
            score += 20
            is_aligned = True
    
    if rr >= 3: score += 10

    # FILTER: Only send if score >= 80 (Elite)
    # Exception: Explicitly Major signals are usually worth sending regardless, but let's stick to user request.
    if score < 80:
        logger.info(f"Telegram alert SKIPPED for {symbol} (Score: {score}/100 - Too Low)")
        return

    # Formatting
    side_emoji = "🟢" if "bullish" in s_type else "🔴"
    side_text = "BUY/LONG" if "bullish" in s_type else "SELL/SHORT"
    tier_emoji = "💎" if tier == "Major" else "🔹"
    
    bias_note = ""
    if is_aligned:
        bias_note = "✅ Trend Aligned"
    elif market_bias != 'NEUTRAL':
        bias_note = "⚠️ Counter-Trend"

    dashboard_link = f"https://www.crt-flow.com/dashboard?symbol={symbol}"

    msg = (
        f"{side_emoji} #CRT **{symbol}** [{tf}]\n\n"
        f"**Direction:** {side_text}\n"
        f"**Entry:** {price}\n"
        f"**Stop Loss:** {sl}\n"
        f"**Take Profit:** {tp} (Target)\n"
        f"**R:R:** {rr}R\n"
        f"**Tier:** {tier} {tier_emoji}\n"
        f"**Score:** {score}/100 ⭐\n"
    )
    
    if session != 'None':
        msg += f"**Session:** {session}\n"
        
    if bias_note:
        msg += f"\n{bias_note}\n"
    
    msg += f"\n📊 [Open in Dashboard]({dashboard_link})"
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": msg,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }

    try:
        resp = requests.post(url, json=payload, timeout=5)
        if resp.status_code != 200:
            logger.error(f"Errore invio Telegram: {resp.text}")
        else:
            logger.info(f"Telegram alert inviato per {symbol}")
    except Exception as e:
        logger.error(f"Eccezione invio Telegram: {e}")
