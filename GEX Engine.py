import pandas as pd
import yfinance as yf
import time
from datetime import datetime
import concurrent.futures
import os

# --- CONFIGURATION ---
# List of tickers to scrape. C# will look for a CSV named after the ticker (e.g., SPX.csv)
TICKERS_TO_SCRAPE = ["^SPX", "^NDX", "SPY"] 

# Interval in seconds to re-fetch data from Yahoo Finance
INTERVAL_SECONDS = 60

# Directory to save the CSV files
OUTPUT_DIR = "C:/NinjaTrader 8/gex_data" # IMPORTANT: Make sure this path exists and matches C#

# --- ENGINE ---

def fetch_and_save_data(ticker_symbol: str):
    """
    Fetches option chain data for a given ticker, enriches it with Volume and a Snapshot Spot Price,
    and saves it to a CSV file.
    """
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching data for {ticker_symbol}...")
        
        ticker = yf.Ticker(ticker_symbol)

        # 1. NEW: Get the LIVE spot price at the exact moment of the snapshot.
        # This is CRITICAL for the "Sticky-Delta" calculation in C#.
        history = ticker.history(period="1d", interval="1m")
        snapshot_spot = history['Close'].iloc[-1] if not history.empty else 0
        
        if snapshot_spot == 0:
            print(f"Warning: Could not fetch snapshot spot price for {ticker_symbol}. Skipping.")
            return

        # Fetch all available expiration dates
        expirations = ticker.options

        all_options_df = []

        # Loop through expirations to build a complete chain (adjust as needed)
        # For performance, you might limit this to the first few expirations
        for exp in expirations[:5]: # Example: only fetch first 5 expirations
            chain = ticker.option_chain(exp)
            
            # Combine calls and puts
            calls = chain.calls
            puts = chain.puts
            calls['Type'] = 'Call'
            puts['Type'] = 'Put'
            
            df = pd.concat([calls, puts])
            all_options_df.append(df)
            
        if not all_options_df:
            print(f"No options data found for {ticker_symbol}.")
            return

        # Combine all expirations into one DataFrame
        final_df = pd.concat(all_options_df)

        # 2. NEW: Clean Volume and Open Interest data to prevent C# errors.
        final_df['volume'] = final_df['volume'].fillna(0)
        final_df['openInterest'] = final_df['openInterest'].fillna(0)
        final_df['impliedVolatility'] = final_df['impliedVolatility'].fillna(0)

        # 3. NEW: Inject the SnapshotSpot price into every row.
        final_df['SnapshotSpot'] = snapshot_spot
        
        # 4. Define the exact columns C# will expect.
        # contractSymbol is useful for debugging but not needed for the math.
        columns_to_export = [
            'strike', 
            'Type', 
            'impliedVolatility', 
            'openInterest', 
            'volume', # NEW
            'SnapshotSpot', # NEW
            'lastTradeDate' # You'll parse this in C# to get TimeToExp
        ]
        
        export_df = final_df[columns_to_export]
        
        # Rename columns to be simple and predictable for C#
        export_df.columns = [
            'Strike', 
            'Type', 
            'BaseIV', 
            'OpenInterest', 
            'Volume', 
            'SnapshotSpot',
            'LastTradeDate'
        ]

        # Save to a uniquely named CSV file
        output_path = os.path.join(OUTPUT_DIR, f"{ticker_symbol.replace('^','')}.csv")
        export_df.to_csv(output_path, index=False)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Success! Data for {ticker_symbol} saved to {output_path}")

    except Exception as e:
        print(f"An error occurred while fetching data for {ticker_symbol}: {e}")


if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR):
        print(f"Creating output directory: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR)
        
    print("--- GEX Data Engine Started ---")
    print(f"Scraping for tickers: {', '.join(TICKERS_TO_SCRAPE)}")
    print(f"Data refresh interval: {INTERVAL_SECONDS} seconds")
    
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(TICKERS_TO_SCRAPE)) as executor:
            # Map the fetching function to each ticker
            executor.map(fetch_and_save_data, TICKERS_TO_SCRAPE)
            
        print(f"\nCycle complete. Waiting {INTERVAL_SECONDS} seconds for next fetch...\n")
        time.sleep(INTERVAL_SECONDS)

