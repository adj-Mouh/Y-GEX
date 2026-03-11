import pandas as pd
import yfinance as yf
import time
from datetime import datetime
import concurrent.futures
import os
import warnings
warnings.filterwarnings("ignore") 

# --- CONFIGURATION ---
TICKERS_TO_SCRAPE = ["^SPX", "^NDX", "SPY"] 
OUTPUT_DIR = r"C:\Options_History_Data" 
INTERVAL_SECONDS = 60

def get_latest_price(ticker_symbol):
    try:
        t = yf.Ticker(ticker_symbol)
        hist = t.history(period="5d", interval="1m")
        return hist['Close'].iloc[-1] if not hist.empty else 0.0
    except:
        return 0.0

def fetch_and_save_data(ticker_symbol: str):
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching Secret-Level data for {ticker_symbol}...")
        
        # 1. Base Metrics
        snapshot_spot = get_latest_price(ticker_symbol)
        snapshot_vix = get_latest_price("^VIX")
        
        # Basis Ratio Proxy
        futures_ticker = "ES=F" if ticker_symbol != "^NDX" else "NQ=F"
        futures_spot = get_latest_price(futures_ticker)
        basis_ratio = (futures_spot / snapshot_spot) if snapshot_spot > 0 else 1.0

        # SECRET HACK 3: Get true continuous dividend yield (q)
        try:
            spy_info = yf.Ticker("SPY").info
            div_yield = spy_info.get('dividendYield', spy_info.get('trailingAnnualDividendYield', 0.013))
            if div_yield is None: div_yield = 0.013
        except:
            div_yield = 0.013

        if snapshot_spot == 0: return

        ticker = yf.Ticker(ticker_symbol)
        expirations = ticker.options
        if not expirations: return

        all_options_df = []
        for exp in expirations[:5]:
            chain = ticker.option_chain(exp)
            calls, puts = chain.calls, chain.puts
            calls['Type'] = 'Call'
            puts['Type'] = 'Put'
            calls['Expiration'] = exp
            puts['Expiration'] = exp
            all_options_df.append(pd.concat([calls, puts]))

        final_df = pd.concat(all_options_df)

        # Clean NaN values
        final_df['openInterest'] = final_df['openInterest'].fillna(0)
        final_df['volume'] = final_df['volume'].fillna(0)
        final_df['impliedVolatility'] = final_df['impliedVolatility'].fillna(0)
        final_df['lastPrice'] = final_df['lastPrice'].fillna(0) # Needed for Put-Call Parity
        
        # Inject Unified Metrics
        final_df['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final_df['BasisRatio'] = round(basis_ratio, 6)
        final_df['SnapshotVIX'] = round(snapshot_vix, 4)
        final_df['SnapshotSpot'] = round(snapshot_spot, 2)
        final_df['DividendYield'] = round(div_yield, 4)

        # Export EXACTLY 12 columns for C# Engine
        export_df = final_df[[
            'Timestamp', 'Expiration', 'BasisRatio', 'SnapshotVIX', 
            'Type', 'strike', 'openInterest', 'impliedVolatility', 
            'volume', 'SnapshotSpot', 'DividendYield', 'lastPrice'
        ]]

        output_path = os.path.join(OUTPUT_DIR, f"{ticker_symbol.replace('^','')}.csv")
        export_df.to_csv(output_path, index=False, header=True)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Success! Saved {len(export_df)} strikes.")

    except Exception as e:
        print(f"Error fetching {ticker_symbol}: {e}")

if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    print("--- Institutional GEX Data Engine Started ---")
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(TICKERS_TO_SCRAPE)) as executor:
            executor.map(fetch_and_save_data, TICKERS_TO_SCRAPE)
        time.sleep(INTERVAL_SECONDS)
