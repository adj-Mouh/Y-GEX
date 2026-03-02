import yfinance as yf
import pandas as pd
import numpy as np
import scipy.stats as stats
from datetime import datetime
import time
import os

# --- 1. SETTINGS ---
TICKER = "^SPX"              # The asset to track (use ^SPX for S&P 500)
RISK_FREE_RATE = 0.045       # 4.5% Risk Free Rate
SPOT_PRICE_RANGE = 0.05      # Track strikes within +/- 5% of current price
REFRESH_SEC = 5.0            # Fetch and save every X seconds (Warning: < 5s may cause IP bans)

# Where to save the data for NinjaTrader to read it
# Change this to a folder NinjaTrader can easily access, e.g., r"C:\Users\YourName\Documents\NinjaTrader 8\GEX_Data"
OUTPUT_DIR = r"./GEX_Data"   

# --- 2. SETUP DIRECTORY ---
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# --- 3. VECTORIZED MATH ---
def calc_gamma_vectorized(S, K, T, r, sigma):
    sigma = np.maximum(sigma, 0.0001) 
    T = np.maximum(T, 0.0001)
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    pdf_d1 = stats.norm.pdf(d1)
    gamma = pdf_d1 / (S * sigma * np.sqrt(T))
    return gamma

# --- 4. MAIN ENGINE LOOP ---
def run_engine():
    print(f"🚀 Starting GEX Engine for {TICKER}...")
    print(f"💾 Saving data to: {OUTPUT_DIR}")
    print(f"⏱️ Update interval: {REFRESH_SEC} seconds\n")
    
    tk_yf = yf.Ticker(TICKER)
    
    while True:
        try:
            # Get Exact Timestamp for NinjaTrader
            current_time = datetime.now()
            timestamp_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            date_str = current_time.strftime("%Y-%m-%d")
            
            # Daily file name (e.g., GEX_SPX_2023-10-25.csv)
            # This prevents files from getting too massive over months
            file_name = f"GEX_{TICKER.replace('^', '')}_{date_str}.csv"
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            # Fetch Spot Price
            try:
                spot_price = tk_yf.fast_info['lastPrice']
            except:
                spot_price = tk_yf.history(period="1d")['Close'].iloc[-1]

            expirations = tk_yf.options
            if not expirations:
                print(f"[{timestamp_str}] ⚠️ No options data found. Rate limited? Retrying in {REFRESH_SEC}s...")
                time.sleep(REFRESH_SEC)
                continue

            all_strikes_gex = pd.Series(dtype=float)

            # Look at first 4 nearest expirations
            for exp in expirations[:4]:
                exp_date = datetime.strptime(exp, "%Y-%m-%d")
                days_to_exp = (exp_date - datetime.today()).days
                T = max(days_to_exp / 365.0, 0.001)
                
                opt = tk_yf.option_chain(exp)
                calls, puts = opt.calls, opt.puts

                if not calls.empty:
                    calls['gamma'] = calc_gamma_vectorized(spot_price, calls['strike'], T, RISK_FREE_RATE, calls['impliedVolatility'])
                    calls['gex'] = calls['openInterest'] * calls['gamma'] * spot_price * 100
                    all_strikes_gex = all_strikes_gex.add(calls.groupby('strike')['gex'].sum(), fill_value=0)

                if not puts.empty:
                    puts['gamma'] = calc_gamma_vectorized(spot_price, puts['strike'], T, RISK_FREE_RATE, puts['impliedVolatility'])
                    puts['gex'] = - (puts['openInterest'] * puts['gamma'] * spot_price * 100)
                    all_strikes_gex = all_strikes_gex.add(puts.groupby('strike')['gex'].sum(), fill_value=0)

            # Format Data
            df = all_strikes_gex.reset_index()
            df.columns = ['Strike', 'GEX']
            
            # Filter to strikes near spot price to keep file size small and fast
            min_strike = spot_price * (1 - SPOT_PRICE_RANGE)
            max_strike = spot_price * (1 + SPOT_PRICE_RANGE)
            df = df[(df['Strike'] >= min_strike) & (df['Strike'] <= max_strike)].copy()
            
            # Convert to Billions and Add Timestamp
            df['GEX_Billions'] = df['GEX'] / 1e9
            df['Timestamp'] = timestamp_str
            df['Spot'] = round(spot_price, 2)
            
            # Reorder columns for NinjaTrader (Timestamp, Spot, Strike, GEX_Billions)
            df = df[['Timestamp', 'Spot', 'Strike', 'GEX_Billions']]
            
            # Save to CSV (Append mode)
            # If file doesn't exist, it writes the header. If it does, it skips header.
            file_exists = os.path.isfile(file_path)
            df.to_csv(file_path, mode='a', header=not file_exists, index=False)
            
            print(f"[{timestamp_str}] ✅ GEX calculated at Spot: {spot_price:.2f} | Saved {len(df)} strikes.")

        except Exception as e:
            timestamp_str = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp_str}] ❌ Error fetching data: {e}")

        # Wait before next fetch
        time.sleep(REFRESH_SEC)

if __name__ == "__main__":
    run_engine()
