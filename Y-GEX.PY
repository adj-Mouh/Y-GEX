import yfinance as yf
import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime
import time
import threading

# --- HIDE THE MATPLOTLIB TOOLBAR ---
plt.rcParams['toolbar'] = 'None'

# --- 1. SETTINGS ---
TICKER = "SPY"
RISK_FREE_RATE = 0.045     # 4.5%
SPOT_PRICE_RANGE = 0.05    # +/- 5% from spot price
DATA_REFRESH_SEC = 2       # Download new data every 2 seconds
CLOCK_REFRESH_SEC = 1      # Tick clock every 1 second

# --- GLOBAL VARIABLES ---
latest_df = None
latest_spot = 0.0
last_fetch_time = 0
is_blocked = False
lock = threading.Lock() 

# --- 2. VECTORIZED MATH ---
def calc_gamma_vectorized(S, K, T, r, sigma):
    sigma = np.maximum(sigma, 0.0001) 
    T = np.maximum(T, 0.0001)
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    pdf_d1 = stats.norm.pdf(d1)
    gamma = pdf_d1 / (S * sigma * np.sqrt(T))
    return gamma

# --- 3. BACKGROUND WORKER ---
def data_worker():
    global latest_df, latest_spot, last_fetch_time, is_blocked
    
    tk = yf.Ticker(TICKER)
    
    while True:
        try:
            try:
                spot_price = tk.fast_info['lastPrice']
            except:
                spot_price = tk.history(period="1d")['Close'].iloc[-1]

            expirations = tk.options
            if not expirations:
                with lock:
                    is_blocked = True
                time.sleep(DATA_REFRESH_SEC)
                continue

            all_strikes_gex = pd.Series(dtype=float)

            for exp in expirations[:4]:
                exp_date = datetime.strptime(exp, "%Y-%m-%d")
                days_to_exp = (exp_date - datetime.today()).days
                T = max(days_to_exp / 365.0, 0.001)
                
                opt = tk.option_chain(exp)
                calls, puts = opt.calls, opt.puts

                if not calls.empty:
                    calls['gamma'] = calc_gamma_vectorized(spot_price, calls['strike'], T, RISK_FREE_RATE, calls['impliedVolatility'])
                    calls['gex'] = calls['openInterest'] * calls['gamma'] * spot_price * 100
                    all_strikes_gex = all_strikes_gex.add(calls.groupby('strike')['gex'].sum(), fill_value=0)

                if not puts.empty:
                    puts['gamma'] = calc_gamma_vectorized(spot_price, puts['strike'], T, RISK_FREE_RATE, puts['impliedVolatility'])
                    puts['gex'] = - (puts['openInterest'] * puts['gamma'] * spot_price * 100)
                    all_strikes_gex = all_strikes_gex.add(puts.groupby('strike')['gex'].sum(), fill_value=0)

            df = all_strikes_gex.reset_index()
            df.columns =['Strike', 'GEX']
            
            min_strike = spot_price * (1 - SPOT_PRICE_RANGE)
            max_strike = spot_price * (1 + SPOT_PRICE_RANGE)
            df = df[(df['Strike'] >= min_strike) & (df['Strike'] <= max_strike)]
            
            df = df.sort_values('Strike')
            df['GEX_Billions'] = df['GEX'] / 1e9

            with lock:
                latest_df = df
                latest_spot = spot_price
                last_fetch_time = time.time()
                is_blocked = False

        except Exception as e:
            with lock:
                is_blocked = True
            print(f"Background Error: {e}")

        time.sleep(DATA_REFRESH_SEC)

# --- 4. START THREAD ---
thread = threading.Thread(target=data_worker, daemon=True)
thread.start()

# --- 5. GUI & CHART ---
plt.style.use('dark_background')
# Set the default window size (Width, Height)
fig, ax = plt.subplots(figsize=(10, 8))

# Optional: Set the window title name
fig.canvas.manager.set_window_title(f"{TICKER} GEX Profile")

def animate(i):
    current_time_str = datetime.now().strftime("%H:%M:%S")
    
    with lock:
        df = latest_df
        spot = latest_spot
        fetch_time = last_fetch_time
        blocked = is_blocked

    if df is not None and not df.empty:
        ax.clear()
        
        colors =['#ff4c4c' if val < 0 else '#4cff4c' for val in df['GEX_Billions']]
        ax.barh(df['Strike'], df['GEX_Billions'], color=colors, height=1.0)
        
        ax.axhline(y=spot, color='white', linestyle='--', linewidth=2, label=f'Spot (${spot:.2f})')

        ax.set_xlabel('Net GEX (Billions of $)', fontsize=12)
        ax.set_ylabel('Strike Price', fontsize=12)
        ax.grid(True, alpha=0.2)
        ax.legend(loc="upper right")

        fetch_str = datetime.fromtimestamp(fetch_time).strftime("%H:%M:%S")
        if blocked:
            ax.set_title(f'⚠️ RATE LIMITED / BLOCKED ⚠️\nClock: {current_time_str} | Data: {fetch_str}', color='red', fontsize=16, fontweight='bold')
        else:
            ax.set_title(f'{TICKER} Live Gamma Exposure (GEX)\nClock: {current_time_str} | Data: {fetch_str}', color='white', fontsize=16, fontweight='bold')

        # This automatically adjusts margins to center the plot perfectly
        fig.tight_layout()

print("Starting UI...")
ani = animation.FuncAnimation(fig, animate, interval=CLOCK_REFRESH_SEC * 1000, cache_frame_data=False)
plt.show()
