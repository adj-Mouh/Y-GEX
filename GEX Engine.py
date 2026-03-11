import pandas as pd
import yfinance as yf
import time
from datetime import datetime
import concurrent.futures
import os
import warnings
warnings.filterwarnings("ignore") # Suppress yfinance timezone warnings

# --- CONFIGURATION ---
TICKERS_TO_SCRAPE = ["^SPX", "^NDX", "SPY"] 
OUTPUT_DIR = r"C:\Options_History_Data" # Must match C# CsvFolderPath
INTERVAL_SECONDS = 60

def get_latest_price(ticker_symbol):
    """Safely gets the most recent price, even outside market hours."""
    try:
        t = yf.Ticker(ticker_symbol)
        hist = t.history(period="5d", interval="1m")
        return hist['Close'].iloc[-1] if not hist.empty else 0.0
    except:
        return 0.0

def fetch_and_save_data(ticker_symbol: str):
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching advanced data for {ticker_symbol}...")
        
        # 1. Fetch Real-Time Baseline Metrics
        snapshot_spot = get_latest_price(ticker_symbol)
        snapshot_vix = get_latest_price("^VIX")
        
        # Determine Futures Proxy for Basis Ratio
        futures_ticker = "ES=F"
        if ticker_symbol == "^NDX": futures_ticker = "NQ=F"
        futures_spot = get_latest_price(futures_ticker)
        
        # Calculate Cash vs Futures Basis Ratio
        basis_ratio = (futures_spot / snapshot_spot) if snapshot_spot > 0 else 1.0
        if basis_ratio == 0: basis_ratio = 1.0

        if snapshot_spot == 0:
            print(f"Warning: Could not fetch spot price for {ticker_symbol}. Skipping.")
            return

        ticker = yf.Ticker(ticker_symbol)
        expirations = ticker.options
        if not expirations: return

        all_options_df = []
        timestamp_now = datetime.now().strftime("%yyyy-%MM-%dd %HH:%mm:%ss")

        # Fetch first 5 expirations (captures 0DTE to weeklies)
        for exp in expirations[:5]:
            chain = ticker.option_chain(exp)
            calls, puts = chain.calls, chain.puts
            calls['Type'] = 'Call'
            puts['Type'] = 'Put'
            
            # Create standardized expiration string
            calls['Expiration'] = exp
            puts['Expiration'] = exp
            
            df = pd.concat([calls, puts])
            all_options_df.append(df)

        final_df = pd.concat(all_options_df)

        # 2. Clean and Inject the Advanced Metrics
        final_df['openInterest'] = final_df['openInterest'].fillna(0)
        final_df['volume'] = final_df['volume'].fillna(0)
        final_df['impliedVolatility'] = final_df['impliedVolatility'].fillna(0)
        
        # Inject our unified Snapshot Data
        final_df['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final_df['BasisRatio'] = round(basis_ratio, 6)
        final_df['SnapshotVIX'] = round(snapshot_vix, 4)
        final_df['SnapshotSpot'] = round(snapshot_spot, 2)

        # Format exact columns for C# Parser
        export_df = final_df[[
            'Timestamp', 'Expiration', 'BasisRatio', 'SnapshotVIX', 
            'Type', 'strike', 'openInterest', 'impliedVolatility', 'volume', 'SnapshotSpot'
        ]]

        output_path = os.path.join(OUTPUT_DIR, f"{ticker_symbol.replace('^','')}.csv")
        export_df.to_csv(output_path, index=False, header=True)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Success! Saved {len(export_df)} strikes.")

    except Exception as e:
        print(f"Error fetching {ticker_symbol}: {e}")

if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    print("--- Pro GEX Data Engine Started ---")
    
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(TICKERS_TO_SCRAPE)) as executor:
            executor.map(fetch_and_save_data, TICKERS_TO_SCRAPE)
        time.sleep(INTERVAL_SECONDS)
