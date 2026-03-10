import yfinance as yf
import pandas as pd
from datetime import datetime

# Importiamo le funzioni di rilevamento dalla logica originale (scanner.py)
from scanner import detect_macro_sweep, detect_tbs_setup, detect_golden_wick

class TimeMachineBacktester:
    def __init__(self, ticker="EURUSD=X", interval="1h", period="730d", initial_capital=1000.0, risk_per_trade=0.0033):
        """
        Motore di backtesting Quant con "Sliding Window" ad alta precisione.
        """
        self.ticker = ticker
        self.interval = interval
        self.period = period
        self.initial_capital = initial_capital
        self.risk_per_trade = risk_per_trade
        self.data = None
        self.trade_history = []
        
    def download_data(self):
        """1. Il Download dello Storico (Massimo 730 giorni per l'1H su yfinance)"""
        print(f"[*] Viaggio nel tempo avviato: recupero dati {self.interval} per {self.ticker}...")
        ticker_obj = yf.Ticker(self.ticker)
        df = ticker_obj.history(period=self.period, interval=self.interval)
        df.dropna(inplace=True)
        self.data = df
        print(f"[+] Download completato: {len(self.data)} candele pronte per la simulazione.\n")

    def detect_signal(self, historical_window):
        """
        Qui vive il tuo Bot originale. Valuta la 'historical_window' (che per lui è "tutto ciò che è successo finora").
        Ritorna un dict se trova un setup, altrimenti None.
        """
        # 1. Prova Golden Wick (Priorità Alta per Limit Orders)
        signal = detect_golden_wick(self.ticker, historical_window, "1H")
        if signal: return signal

        # 2. Prova Macro Sweeps per timeframe 1H
        signal = detect_macro_sweep(self.ticker, historical_window, "1H")
        if signal: return signal
        
        # 3. Se non c'è, prova TBS per timeframe 1H
        signal = detect_tbs_setup(self.ticker, historical_window, "1H")
        return signal

    def run(self, window_size=100):
        """2. Il Cuore del Backtest (Sliding Window Loop)"""
        if self.data is None or self.data.empty:
            print("[-] Dati mancanti. Esegui download_data() prima.")
            return

        print(f"[*] Avvio simulazione Sliding Window (Window Size: {window_size} candele)...")
        print(f"[*] Capitale Iniziale: €{self.initial_capital:.2f} | Rischio per trade: {self.risk_per_trade*100:.2f}%")
        
        open_trade = None
        pending_trade = None
        stats = {'win': 0, 'loss': 0, 'be': 0, 'total_r': 0.0}
        equity_curve = [0.0]
        capital_curve = [self.initial_capital]
        current_capital = self.initial_capital

        # --- 1. IL CUORE DEL BACKTEST (Sliding Window Loop) ---
        for i in range(window_size, len(self.data)):
            live_window = self.data.iloc[i-window_size:i]
            current_candle = self.data.iloc[i]
            current_time = current_candle.name
            
            # --- 2. GESTIONE ORDINE PENDENTE (Limit Orders) ---
            if not open_trade and pending_trade:
                entry_p = pending_trade['entry_price']
                trade_type = pending_trade['type']
                triggered = False
                
                if 'bullish' in trade_type or trade_type == 'LONG':
                    if current_candle['Low'] <= entry_p: triggered = True
                elif 'bearish' in trade_type or trade_type == 'SHORT':
                    if current_candle['High'] >= entry_p: triggered = True
                
                if triggered:
                    open_trade = pending_trade
                    pending_trade = None
                    open_trade['entry_time'] = current_time
                    print(f"[{current_time}] ⚡ LIMIT ORDER ATTIVATO: {open_trade['type'].upper()} @ {entry_p}")
                else:
                    # Se il pending non è stato attivato, controlliamo se è stato invalidato da SL/TP
                    sl = pending_trade['stop_loss']
                    tp = pending_trade['take_profit']
                    if 'bullish' in trade_type or trade_type == 'LONG':
                        if current_candle['Low'] <= sl or current_candle['High'] >= tp:
                            # print(f"[{current_time}] ❌ PENDING ORDER INVALIDATO (SL/TP hit prima dell'entry): {pending_trade['type'].upper()}")
                            pending_trade = None
                    elif 'bearish' in trade_type or trade_type == 'SHORT':
                        if current_candle['High'] >= sl or current_candle['Low'] <= tp:
                            # print(f"[{current_time}] ❌ PENDING ORDER INVALIDATO (SL/TP hit prima dell'entry): {pending_trade['type'].upper()}")
                            pending_trade = None
            
            # --- 3. GESTIONE TRADE APERTO (Trade Manager) ---
            if open_trade:
                trade_type = open_trade['type']
                sl = open_trade['stop_loss']
                tp = open_trade['take_profit']
                r_multiple = open_trade.get('rr_ratio', 2.0) # Calcolato precisamente dalla logica CRT

                # Verifichiamo se in QUESTA ORA tocchiamo prima lo SL o il TP
                # Rischio fisso espresso in Euro
                risk_eur = current_capital * self.risk_per_trade

                if 'bullish' in trade_type or trade_type == 'LONG':
                    low_hit_sl = current_candle['Low'] <= sl
                    high_hit_tp = current_candle['High'] >= tp

                    if low_hit_sl and high_hit_tp:
                        # Se li tocca entrambi nella stessa candela 1H, assumiamo il peggio (LOSS)
                        stats['loss'] += 1
                        stats['total_r'] -= 1.0
                        current_capital -= risk_eur
                        self.trade_history.append({'time': current_time, 'result': 'LOSS', 'pnl': -1.0, 'capital': current_capital})
                        equity_curve.append(stats['total_r'])
                        capital_curve.append(current_capital)
                        open_trade = None
                    elif low_hit_sl:
                        # Solo Stop Loss colpito
                        stats['loss'] += 1
                        stats['total_r'] -= 1.0
                        current_capital -= risk_eur
                        self.trade_history.append({'time': current_time, 'result': 'LOSS', 'pnl': -1.0, 'capital': current_capital})
                        equity_curve.append(stats['total_r'])
                        capital_curve.append(current_capital)
                        open_trade = None
                    elif high_hit_tp:
                        # Solo Take Profit colpito
                        stats['win'] += 1
                        stats['total_r'] += r_multiple
                        profit_eur = risk_eur * r_multiple
                        current_capital += profit_eur
                        self.trade_history.append({'time': current_time, 'result': 'WIN', 'pnl': r_multiple, 'capital': current_capital})
                        equity_curve.append(stats['total_r'])
                        capital_curve.append(current_capital)
                        open_trade = None
                        
                elif 'bearish' in trade_type or trade_type == 'SHORT':
                    high_hit_sl = current_candle['High'] >= sl
                    low_hit_tp = current_candle['Low'] <= tp

                    if high_hit_sl and low_hit_tp:
                        # Se li tocca entrambi, peggior scenario (LOSS)
                        stats['loss'] += 1
                        stats['total_r'] -= 1.0
                        current_capital -= risk_eur
                        self.trade_history.append({'time': current_time, 'result': 'LOSS', 'pnl': -1.0, 'capital': current_capital})
                        equity_curve.append(stats['total_r'])
                        capital_curve.append(current_capital)
                        open_trade = None
                    elif high_hit_sl:
                        # Solo Stop Loss colpito
                        stats['loss'] += 1
                        stats['total_r'] -= 1.0
                        current_capital -= risk_eur
                        self.trade_history.append({'time': current_time, 'result': 'LOSS', 'pnl': -1.0, 'capital': current_capital})
                        equity_curve.append(stats['total_r'])
                        capital_curve.append(current_capital)
                        open_trade = None
                    elif low_hit_tp:
                        # Solo Take Profit colpito
                        stats['win'] += 1
                        stats['total_r'] += r_multiple
                        profit_eur = risk_eur * r_multiple
                        current_capital += profit_eur
                        self.trade_history.append({'time': current_time, 'result': 'WIN', 'pnl': r_multiple, 'capital': current_capital})
                        equity_curve.append(stats['total_r'])
                        capital_curve.append(current_capital)
                        open_trade = None
                
                # Se il trade è ancora integro, saltiamo alla prossima ora (non cerchiamo altri segnali fino a chiusura)
                if open_trade:
                    continue 

            # --- 4. CERCA NUOVI SEGNALI SOLO SE FLAT ---
            if not open_trade and not pending_trade:
                signal = self.detect_signal(live_window)
                if signal:
                    # --- NUOVO FILTRO: PRENDI SOLO I TRADE A++ (Trend-Aligned) ---
                    if signal.get('diamond_score') != 'A++':
                        continue
                    
                    # Setup Pending per Golden Wick, Market per gli altri
                    if 'wick' in signal['type']:
                        pending_trade = signal
                        print(f"[{current_time}] ⏳ PENDING LIMIT ORDER: {signal['type'].upper()} @ {signal['entry_price']} (SL: {signal['stop_loss']})")
                    else:
                        open_trade = signal
                        open_trade['entry_time'] = current_time
                        entry_p = signal.get('entry_price', signal.get('price'))
                        print(f"[{current_time}] 🟢 APERTO TRADE {signal['type'].upper()} @ {entry_p} (SL: {signal['stop_loss']}, TP: {signal['take_profit']}, R: {signal['rr_ratio']})")

        print("\n[+] Simulazione Terminata. Calcolo risultati in corso...")
        self.print_report(stats, equity_curve, capital_curve)

    def print_report(self, stats, equity_curve, capital_curve):
        total_trades = stats['win'] + stats['loss'] + stats['be']
        winrate = (stats['win'] / total_trades * 100) if total_trades > 0 else 0.0

        peak_r = 0
        max_drawdown_r = 0
        for r in equity_curve:
            if r > peak_r: peak_r = r
            drawdown_r = peak_r - r
            if drawdown_r > max_drawdown_r: max_drawdown_r = drawdown_r
            
        peak_cap = self.initial_capital
        max_drawdown_eur = 0
        max_drawdown_pct = 0
        for cap in capital_curve:
            if cap > peak_cap: peak_cap = cap
            drawdown_eur = peak_cap - cap
            if drawdown_eur > max_drawdown_eur:
                max_drawdown_eur = drawdown_eur
                max_drawdown_pct = (max_drawdown_eur / peak_cap) * 100

        final_capital = capital_curve[-1] if capital_curve else self.initial_capital
        net_profit_eur = final_capital - self.initial_capital
        roi_pct = (net_profit_eur / self.initial_capital) * 100

        print("\n" + "="*45)
        print(f"🚀 REPORT BACKTEST {self.ticker} INTRA-DAY (CRT) 🚀")
        print("="*45)
        print(f"Trades Totali   : {total_trades}")
        print(f"Win (Target Hit): {stats['win']}")
        print(f"Loss (Stop Hit) : {stats['loss']}")
        print(f"Winrate         : {winrate:.2f}%")
        print(f"Net Profit (R)  : {stats['total_r']:.2f} R")
        print(f"Max Drawdown (R): {max_drawdown_r:.2f} R")
        print("-" * 45)
        print("💰 SIMULAZIONE CAPITAL 💰")
        print(f"Capitale Iniziale: €{self.initial_capital:.2f}")
        print(f"Capitale Finale  : €{final_capital:.2f}")
        print(f"Profitto Netto   : €{net_profit_eur:.2f} (+{roi_pct:.2f}%)")
        print(f"Max Drawdown EUR : -€{max_drawdown_eur:.2f} (-{max_drawdown_pct:.2f}%)")
        print("="*45)
        if stats['total_r'] > 0:
            print("✅ HAI UN VANTAGGIO STATISTICO (EDGE) NETTO!")
        else:
            print("❌ STRATEGIA IN PERDITA O PARI CON I PARAMETRI ATTUALI.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='CRT Flow TimeMachine Backtester')
    parser.add_argument('--ticker', type=str, default="EURUSD=X", help='Ticker da testare (es. EURUSD=X o AAPL)')
    parser.add_argument('--capital', type=float, default=10000.0, help='Capitale iniziale per la simulazione')
    parser.add_argument('--risk', type=float, default=0.33, help='Percentuale di rischio per trade (es. 1 per 1%)')
    args = parser.parse_args()

    engine = TimeMachineBacktester(ticker=args.ticker, interval="1h", period="730d", initial_capital=args.capital, risk_per_trade=(args.risk / 100))
    engine.download_data()
    engine.run(window_size=100)
