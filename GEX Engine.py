import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime
import concurrent.futures

# --- 1. SETTINGS ---
TICKERS = ["^SPX", "SPY", "QQQ", "IWM", "AAPL", "NVDA"] 
EXPIRATIONS_TO_PULL = 4      
INTERVAL_SECONDS = 60        
OUTPUT_DIR = r"./Options_History_Data"   
SPOT_PRICE_RANGE = 0.10      # Track strikes within +/- 10% of current price (Saves massive disk space)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# --- 2. WORKER FUNCTION ---
def fetch_and_store(symbol, date_str, current_time):
    timestamp_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
    clean_symbol = symbol.replace('^', '').replace('.', '_')
    file_name = f"{clean_symbol}_Options_{date_str}.csv"
    file_path = os.path.join(OUTPUT_DIR, file_name)
    
    try:
        tk_yf = yf.Ticker(symbol)
        
        # 1. Fetch Spot Price to use for filtering
        try:
            spot_price = tk_yf.fast_info['lastPrice']
        except:
            try:
                spot_price = tk_yf.history(period="1d", interval="1m")['Close'].iloc[-1]
            except:
                return symbol, 404, "Could not fetch spot price for filtering."

        expirations = tk_yf.options
        if not expirations:
            return symbol, 404, "No options data found."

        all_options = []

        # 2. Fetch Options Data
        for exp in expirations[:EXPIRATIONS_TO_PULL]:
            try:
                opt = tk_yf.option_chain(exp)
                calls, puts = opt.calls, opt.puts
            except Exception as e:
                if "403" in str(e) or "429" in str(e) or "Unauthorized" in str(e):
                    return symbol, 403, f"BLOCKED BY YAHOO FINANCE: {e}"
                continue

            if not calls.empty:
                calls = calls[['strike', 'openInterest', 'impliedVolatility']].copy()
                calls['Type'] = 'Call'
                calls['Expiration'] = exp
                all_options.append(calls)

            if not puts.empty:
                puts = puts[['strike', 'openInterest', 'impliedVolatility']].copy()
                puts['Type'] = 'Put'
                puts['Expiration'] = exp
                all_options.append(puts)

        if not all_options:
            return symbol, 204, "Data fetched but chains were empty."

        # 3. Combine Data
        df = pd.concat(all_options, ignore_index=True)
        
        # 4. FILTER BY SPOT PRICE RANGE (This shrinks the file size dramatically!)
        min_strike = spot_price * (1 - SPOT_PRICE_RANGE)
        max_strike = spot_price * (1 + SPOT_PRICE_RANGE)
        df = df[(df['strike'] >= min_strike) & (df['strike'] <= max_strike)]
        
        # 5. Clean and Format
        df['openInterest'] = df['openInterest'].fillna(0)
        df['impliedVolatility'] = df['impliedVolatility'].fillna(0.0001)
        df = df[df['openInterest'] > 0]
        
        df['Timestamp'] = timestamp_str
        df = df[['Timestamp', 'Expiration', 'Type', 'strike', 'openInterest', 'impliedVolatility']]
        df.rename(columns={'strike': 'Strike', 'openInterest': 'OI', 'impliedVolatility': 'IV'}, inplace=True)
        
        # 6. Save to CSV
        file_exists = os.path.isfile(file_path)
        df.to_csv(file_path, mode='a', header=not file_exists, index=False)
        
        return symbol, 200, f"Saved {len(df)} strikes (Filtered +/- {SPOT_PRICE_RANGE*100}% of Spot)."

    except Exception as e:
        if "403" in str(e) or "429" in str(e):
            return symbol, 429, f"RATE LIMITED/BLOCKED: {e}"
        return symbol, 500, f"Internal Error: {e}"

# --- 3. MAIN CONTROLLER LOOP ---
def run_parallel_fetcher():
    print(f"🚀 Starting Parallel Options Fetcher. Interval: {INTERVAL_SECONDS}s")
    print(f"✂️  Filtering out strikes beyond +/- {SPOT_PRICE_RANGE*100}% of Spot Price.")
    
    while True:
        loop_start_time = time.time()
        current_time = datetime.now()
        date_str = current_time.strftime("%Y-%m-%d")
        
        print(f"\n[{current_time.strftime('%H:%M:%S')}] 📡 Fetching data for {len(TICKERS)} tickers in parallel...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(TICKERS)) as executor:
            futures = [executor.submit(fetch_and_store, ticker, date_str, current_time) for ticker in TICKERS]
            
            for future in concurrent.futures.as_completed(futures):
                symbol, status_code, message = future.result()
                
                if status_code == 200:
                    print(f"  ✅ [200 OK] {symbol:<5} -> {message}")
                elif status_code in [403, 429]:
                    print(f"  🛑 [{status_code} BLOCKED] {symbol:<5} -> {message}")
                else:
                    print(f"  ⚠️ [{status_code} WARN] {symbol:<5} -> {message}")

        execution_time = time.time() - loop_start_time
        sleep_time = max(0, INTERVAL_SECONDS - execution_time)
        
        print(f"⏱️ Cycle took {execution_time:.2f}s. Sleeping for {sleep_time:.2f}s...")
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_parallel_fetcher()
