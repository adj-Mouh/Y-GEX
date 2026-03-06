# --- START OF FILE GEX Engine (Final).py ---

import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime
import concurrent.futures
import logging

# --- 1. SETTINGS ---
# Use a dictionary for cleaner settings management
CONFIG = {
    # List of tickers to fetch options for
    "TICKERS": ["^SPX", "SPY", "QQQ"], 
    
    # For tickers that need a basis ratio (e.g., SPX vs ES), define their future symbol here
    "BASIS_MAP": {
        "^SPX": "ES=F"
    },
    
    # How many of the nearest expirations to pull (Controls file size DRAMATICALLY)
    "EXPIRATIONS_TO_PULL": 4,      
    
    # How often to fetch new data
    "INTERVAL_SECONDS": 60,        
    
    # Where to save the files
    "OUTPUT_DIR": r"\Options_History_Data",
    
    # Filter strikes to save disk space
    "SPOT_PRICE_RANGE_PERCENT": 0.10, # +/- 10%
    
    # How many hours of old CSV files to keep. Set to 0 to disable.
    "HOURS_TO_KEEP_FILES": 48 
}

# --- 2. SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')

if not os.path.exists(CONFIG["OUTPUT_DIR"]):
    os.makedirs(CONFIG["OUTPUT_DIR"])

# --- 3. HELPER FUNCTIONS ---

def calculate_synced_basis(cash_ticker, future_ticker):
    """Downloads 1m data and finds the last minute where both assets traded to get a stable basis."""
    try:
        tickers = f"{cash_ticker} {future_ticker}"
        data = yf.download(tickers, period="5d", interval="1m", progress=False)['Close']
        synced_data = data.dropna()
        
        if synced_data.empty:
            logging.warning(f"[{cash_ticker}] No synced basis found, will use ratio of 1.0")
            return 1.0

        last_sync_row = synced_data.iloc[-1]
        spx_price = float(last_sync_row[cash_ticker])
        es_price = float(last_sync_row[future_ticker])
        
        if spx_price == 0: return 1.0 # Avoid division by zero
        
        basis_ratio = es_price / spx_price
        logging.info(f"[{cash_ticker}] Synced Basis: {future_ticker} @ {es_price:.2f} / {cash_ticker} @ {spx_price:.2f} = {basis_ratio:.5f}")
        return basis_ratio

    except Exception as e:
        logging.error(f"[{cash_ticker}] Basis calculation failed: {e}")
        return 1.0 # Return a neutral ratio on failure

# --- 4. WORKER FUNCTION ---

def fetch_and_store_ticker(symbol, config, current_time):
    """The main function executed in parallel for each ticker."""
    try:
        tk_yf = yf.Ticker(symbol)
        
        # 1. Fetch Spot Price
        spot_price = tk_yf.history(period="1d", interval="1m")['Close'].iloc[-1]

        # 2. Calculate Basis Ratio (if applicable)
        basis_ratio = 1.0 # Default for non-index tickers like SPY, QQQ
        if symbol in config["BASIS_MAP"]:
            basis_ratio = calculate_synced_basis(symbol, config["BASIS_MAP"][symbol])

        # 3. Fetch Options Data
        expirations = tk_yf.options
        if not expirations:
            return symbol, 404, "No options data found."

        all_options = []
        for exp in expirations[:config["EXPIRATIONS_TO_PULL"]]:
            try:
                opt = tk_yf.option_chain(exp)
                calls, puts = opt.calls, opt.puts
                
                if not calls.empty:
                    calls['Type'], calls['Expiration'] = 'Call', exp
                    all_options.append(calls)
                if not puts.empty:
                    puts['Type'], puts['Expiration'] = 'Put', exp
                    all_options.append(puts)
            except Exception:
                continue # Skip expirations that fail to load

        if not all_options:
            return symbol, 204, "Data fetched but chains were empty."

        # 4. Combine and Format Data
        df = pd.concat(all_options, ignore_index=True)
        
        # Filter by price range
        min_strike = spot_price * (1 - config["SPOT_PRICE_RANGE_PERCENT"])
        max_strike = spot_price * (1 + config["SPOT_PRICE_RANGE_PERCENT"])
        df = df[(df['strike'] >= min_strike) & (df['strike'] <= max_strike)]
        
        # Clean data
        df['openInterest'] = df['openInterest'].fillna(0).astype(int)
        df['impliedVolatility'] = df['impliedVolatility'].fillna(0.0001)
        
        # Add required columns for NinjaTrader
        df['Timestamp'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
        df['BasisRatio'] = round(basis_ratio, 6)
        
        # Select and rename
        final_df = df[[
            'Timestamp', 'Expiration', 'BasisRatio', 'Type', 'strike', 'openInterest', 'impliedVolatility'
        ]].copy()
        final_df.rename(columns={'strike': 'Strike', 'openInterest': 'OI', 'impliedVolatility': 'IV'}, inplace=True)

        # 5. Save to ONE file per day, APPENDING data
        clean_symbol = symbol.replace('^', '').replace('.', '_')
        date_str = current_time.strftime("%Y-%m-%d")
        file_name = f"{clean_symbol}_Options_{date_str}.csv"
        file_path = os.path.join(config["OUTPUT_DIR"], file_name)
        
        file_exists = os.path.isfile(file_path)
        final_df.to_csv(file_path, mode='a', header=not file_exists, index=False)
        
        return symbol, 200, f"Appended {len(final_df)} strikes."

    except Exception as e:
        return symbol, 500, f"Internal Error: {e}"

# --- 5. CLEANUP FUNCTION ---

def cleanup_old_files(config):
    """Deletes old CSV files from the output directory to save space."""
    if config["HOURS_TO_KEEP_FILES"] <= 0: return
    try:
        folder = config["OUTPUT_DIR"]
        now = time.time()
        age_limit_seconds = config["HOURS_TO_KEEP_FILES"] * 3600
        
        for filename in os.listdir(folder):
            if filename.endswith(".csv"):
                file_path = os.path.join(folder, filename)
                if os.stat(file_path).st_mtime < now - age_limit_seconds:
                    os.remove(file_path)
    except Exception as e:
        logging.error(f"File cleanup failed: {e}")

# --- 6. MAIN CONTROLLER LOOP ---

def run_parallel_fetcher(config):
    logging.info(f"🚀 Starting GEX Engine for: {', '.join(config['TICKERS'])}")
    
    while True:
        loop_start_time = time.time()
        current_time = datetime.now()
        
        logging.info(f"--- 📡 Fetching data for {len(config['TICKERS'])} tickers ---")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(config['TICKERS'])) as executor:
            futures = [executor.submit(fetch_and_store_ticker, ticker, config, current_time) for ticker in config['TICKERS']]
            
            for future in concurrent.futures.as_completed(futures):
                symbol, status_code, message = future.result()
                if status_code == 200:
                    logging.info(f"  ✅ {symbol:<5} -> {message}")
                else:
                    logging.warning(f"  ⚠️ [{status_code}] {symbol:<5} -> {message}")

        cleanup_old_files(config)
        
        execution_time = time.time() - loop_start_time
        sleep_time = max(0, config["INTERVAL_SECONDS"] - execution_time)
        logging.info(f"--- Cycle took {execution_time:.2f}s. Sleeping for {sleep_time:.2f}s ---\n")
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_parallel_fetcher(CONFIG)
