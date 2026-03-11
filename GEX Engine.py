# --- START OF FILE GEX Engine.py ---

import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime
import concurrent.futures
import logging

# --- 1. SETTINGS ---
CONFIG = {
    "TICKERS": ["^SPX", "^NDX", "^RUT", "SPY", "QQQ"], 
    "BASIS_MAP": {
        "^SPX": "ES=F",    
        "SPY": "ES=F",     
        "^NDX": "NQ=F",    
        "QQQ": "NQ=F",     
        "^RUT": "RTY=F",   
        "IWM": "RTY=F",    
        "^DJI": "YM=F",    
        "DIA": "YM=F"      
    },
    "EXPIRATIONS_TO_PULL": 4,      
    "INTERVAL_SECONDS": 60,        
    "OUTPUT_DIR": r"C:\Options_History_Data", 
    "SPOT_PRICE_RANGE_PERCENT": 0.10, 
    "HOURS_TO_KEEP_FILES": 48 
}

# --- 2. SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')

if not os.path.exists(CONFIG["OUTPUT_DIR"]):
    os.makedirs(CONFIG["OUTPUT_DIR"])

# --- 3. HELPER FUNCTIONS ---
def get_live_vix():
    try:
        vix_data = yf.Ticker("^VIX").history(period="1d", interval="1m")
        if not vix_data.empty:
            return float(vix_data['Close'].iloc[-1])
    except Exception as e:
        logging.error(f"Failed to fetch VIX: {e}")
    return 15.0 # Fallback

def calculate_synced_basis(cash_ticker, future_ticker):
    try:
        tickers = f"{cash_ticker} {future_ticker}"
        data = yf.download(tickers, period="5d", interval="1m", progress=False)['Close']
        synced_data = data.dropna()
        
        if synced_data.empty: return 1.0

        last_sync_row = synced_data.iloc[-1]
        cash_price, future_price = float(last_sync_row[cash_ticker]), float(last_sync_row[future_ticker])
        
        if cash_price == 0: return 1.0 
        return future_price / cash_price
    except Exception:
        return 1.0 

# --- 4. WORKER FUNCTION ---
def fetch_and_store_ticker(symbol, config, current_time, snapshot_vix):
    try:
        tk_yf = yf.Ticker(symbol)
        
        # 1. Fetch Spot Price & Basis
        spot_price = tk_yf.history(period="1d", interval="1m")['Close'].iloc[-1]
        basis_ratio = calculate_synced_basis(symbol, config["BASIS_MAP"][symbol]) if symbol in config["BASIS_MAP"] else 1.0

        # 2. Fetch Options Data
        expirations = tk_yf.options
        if not expirations: return symbol, 404, "No options data found."

        all_options = []
        for exp in expirations[:config["EXPIRATIONS_TO_PULL"]]:
            try:
                opt = tk_yf.option_chain(exp)
                calls, puts = opt.calls.copy(), opt.puts.copy()
                
                if not calls.empty:
                    calls['Type'], calls['Expiration'] = 'Call', exp
                    all_options.append(calls)
                if not puts.empty:
                    puts['Type'], puts['Expiration'] = 'Put', exp
                    all_options.append(puts)
            except Exception:
                continue

        if not all_options: return symbol, 204, "Chains were empty."

        # 3. Clean and Format
        df = pd.concat(all_options, ignore_index=True)
        min_strike = spot_price * (1 - config["SPOT_PRICE_RANGE_PERCENT"])
        max_strike = spot_price * (1 + config["SPOT_PRICE_RANGE_PERCENT"])
        df = df[(df['strike'] >= min_strike) & (df['strike'] <= max_strike)]
        
        df['openInterest'] = df['openInterest'].fillna(0).astype(int)
        df['impliedVolatility'] = df['impliedVolatility'].fillna(0.0001)
        df['Timestamp'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
        df['BasisRatio'] = round(basis_ratio, 6)
        df['SnapshotVIX'] = round(snapshot_vix, 2)
        
        # MUST MATCH C# PARSER EXACTLY
        final_df = df[['Timestamp', 'Expiration', 'BasisRatio', 'SnapshotVIX', 'Type', 'strike', 'openInterest', 'impliedVolatility']]
        
        clean_symbol = symbol.replace('^', '').replace('.', '_')
        file_path = os.path.join(config["OUTPUT_DIR"], f"{clean_symbol}_Options_{current_time.strftime('%Y-%m-%d')}.csv")
        
        file_exists = os.path.isfile(file_path)
        final_df.to_csv(file_path, mode='a', header=not file_exists, index=False)
        
        return symbol, 200, f"Appended {len(final_df)} strikes. VIX: {snapshot_vix:.2f}"

    except Exception as e:
        return symbol, 500, f"Error: {e}"

# --- 5. CLEANUP ---
def cleanup_old_files(config):
    if config["HOURS_TO_KEEP_FILES"] <= 0: return
    try:
        now, age_limit = time.time(), config["HOURS_TO_KEEP_FILES"] * 3600
        for f in os.listdir(config["OUTPUT_DIR"]):
            if f.endswith(".csv"):
                p = os.path.join(config["OUTPUT_DIR"], f)
                if os.stat(p).st_mtime < now - age_limit: os.remove(p)
    except Exception: pass

# --- 6. MAIN LOOP ---
def run_parallel_fetcher(config):
    logging.info(f"🚀 Starting Engine for: {', '.join(config['TICKERS'])}")
    while True:
        start_t, current_t = time.time(), datetime.now()
        
        # Get VIX Snapshot once per cycle to ensure perfectly synced baseline
        snapshot_vix = get_live_vix()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(config['TICKERS'])) as executor:
            futures = [executor.submit(fetch_and_store_ticker, tick, config, current_t, snapshot_vix) for tick in config['TICKERS']]
            for future in concurrent.futures.as_completed(futures):
                sym, status, msg = future.result()
                if status == 200: logging.info(f"  ✅ {sym:<5} -> {msg}")
                else: logging.warning(f"  ⚠️ {sym:<5} -> {msg}")

        cleanup_old_files(config)
        sleep_t = max(0, config["INTERVAL_SECONDS"] - (time.time() - start_t))
        time.sleep(sleep_t)

if __name__ == "__main__":
    run_parallel_fetcher(CONFIG)
