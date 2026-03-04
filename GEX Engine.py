import yfinance as yf
import pandas as pd
import numpy as np
import scipy.stats as stats
from datetime import datetime
import time
import os

# --- 1. SETTINGS ---
# List of assets to track. 
# IMPORTANT: Put Indices like SPX with a carat (^). Stocks normally.
TICKERS = ["^SPX", "SPY", "QQQ", "IWM", "AAPL", "NVDA"] 

RISK_FREE_RATE = 0.045       # 4.5% Risk Free Rate
SPOT_PRICE_RANGE = 0.05      # Track strikes within +/- 5% of current price
REFRESH_SEC = 10.0           # Slower refresh recommended if tracking multiple tickers to avoid bans!

# Output Directory
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
    print(f"🚀 Starting Multi-Ticker GEX Engine...")
    print(f"📋 Tracking: {TICKERS}")
    print(f"💾 Saving data to: {OUTPUT_DIR}")
    
    while True:
        # Loop through every ticker in the list
        for symbol in TICKERS:
            try:
                # Initialize Ticker
                tk_yf = yf.Ticker(symbol)
                
                # Get Exact Timestamp
                current_time = datetime.now()
                timestamp_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
                date_str = current_time.strftime("%Y-%m-%d")
                
                # --- FILE NAMING LOGIC ---
                # This ensures every symbol and every day gets a unique file
                # Example: GEX_SPX_2023-10-27.csv
                clean_symbol = symbol.replace('^', '').replace('.', '_') # Remove special chars for filename
                file_name = f"GEX_{clean_symbol}_{date_str}.csv"
                file_path = os.path.join(OUTPUT_DIR, file_name)
                
                # Fetch Spot Price (Handle Indices vs Stocks)
                try:
                    spot_price = tk_yf.fast_info['lastPrice']
                except:
                    try:
                        spot_price = tk_yf.history(period="1d", interval="1m")['Close'].iloc[-1]
                    except:
                        print(f"[{timestamp_str}] ⚠️ Could not fetch price for {symbol}")
                        continue

                expirations = tk_yf.options
                if not expirations:
                    print(f"[{timestamp_str}] ⚠️ No options data for {symbol}.")
                    continue

                all_strikes_gex = pd.Series(dtype=float)

                # Look at first 4 nearest expirations
                for exp in expirations[:4]:
                    exp_date = datetime.strptime(exp, "%Y-%m-%d")
                    days_to_exp = (exp_date - datetime.today()).days
                    T = max(days_to_exp / 365.0, 0.001)
                    
                    try:
                        opt = tk_yf.option_chain(exp)
                        calls, puts = opt.calls, opt.puts
                    except:
                        continue

                    if not calls.empty:
                        # Clean Data (Fill NaNs with 0)
                        calls['impliedVolatility'] = calls['impliedVolatility'].fillna(0)
                        calls['openInterest'] = calls['openInterest'].fillna(0)
                        
                        calls['gamma'] = calc_gamma_vectorized(spot_price, calls['strike'], T, RISK_FREE_RATE, calls['impliedVolatility'])
                        calls['gex'] = calls['openInterest'] * calls['gamma'] * spot_price * 100
                        all_strikes_gex = all_strikes_gex.add(calls.groupby('strike')['gex'].sum(), fill_value=0)

                    if not puts.empty:
                        puts['impliedVolatility'] = puts['impliedVolatility'].fillna(0)
                        puts['openInterest'] = puts['openInterest'].fillna(0)
                        
                        puts['gamma'] = calc_gamma_vectorized(spot_price, puts['strike'], T, RISK_FREE_RATE, puts['impliedVolatility'])
                        puts['gex'] = - (puts['openInterest'] * puts['gamma'] * spot_price * 100)
                        all_strikes_gex = all_strikes_gex.add(puts.groupby('strike')['gex'].sum(), fill_value=0)

                # Format Data
                df = all_strikes_gex.reset_index()
                df.columns = ['Strike', 'GEX']
                
                # Filter Range
                min_strike = spot_price * (1 - SPOT_PRICE_RANGE)
                max_strike = spot_price * (1 + SPOT_PRICE_RANGE)
                df = df[(df['Strike'] >= min_strike) & (df['Strike'] <= max_strike)].copy()
                
                # Final Formatting
                df['GEX_Billions'] = df['GEX'] / 1e9
                df['Timestamp'] = timestamp_str
                df['Spot'] = round(spot_price, 2)
                
                # Reorder for NinjaTrader
                df = df[['Timestamp', 'Spot', 'Strike', 'GEX_Billions']]
                
                # Save to specific file
                file_exists = os.path.isfile(file_path)
                df.to_csv(file_path, mode='a', header=not file_exists, index=False)
                
                print(f"[{timestamp_str}] ✅ {clean_symbol} Saved | Spot: {spot_price:.2f}")

            except Exception as e:
                print(f"❌ Error with {symbol}: {e}")
                
        # Wait before next cycle of ALL tickers
        print(f"💤 Sleeping {REFRESH_SEC}s...")
        time.sleep(REFRESH_SEC)

if __name__ == "__main__":
    run_engine()
