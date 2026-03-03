import yfinance as yf
import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk
from datetime import datetime
import threading
import sys
import os

# --- HIDE THE MATPLOTLIB TOOLBAR ---
plt.rcParams['toolbar'] = 'None'

# --- 1. SETTINGS & GLOBALS ---
TICKER = "SPY"
RISK_FREE_RATE = 0.045     # 4.5%
SPOT_PRICE_RANGE = 0.06    # +/- 6%
DATA_REFRESH_SEC = 2.0     # Download new data every 2 seconds
CLOCK_REFRESH_SEC = 1      # Tick clock every 1 second

POPULAR_TICKERS = [
    "SPY", "SPX", "QQQ", "NDX", "IWM", "DIA", "AAPL", 
    "MSFT", "TSLA", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD", "BA"
]

latest_df = None
latest_spot = 0.0
is_blocked = False
lock = threading.Lock() 

# Thread control events
refresh_event = threading.Event()
stop_event = threading.Event()

# --- 2. VECTORIZED MATH ---
def calc_gamma_vectorized(S, K, T, r, sigma):
    # Protect against divide-by-zero or negative inputs
    sigma = np.maximum(sigma, 0.0001) 
    T = np.maximum(T, 0.0001)
    S = np.maximum(S, 0.0001)
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    pdf_d1 = stats.norm.pdf(d1)
    gamma = pdf_d1 / (S * sigma * np.sqrt(T))
    return gamma

# --- 3. BACKGROUND WORKER ---
def data_worker():
    global latest_df, latest_spot, is_blocked, TICKER
    
    current_worker_ticker = ""
    tk_yf = None

    while not stop_event.is_set():
        refresh_event.clear()
        
        with lock:
            target_ticker = TICKER
            
        if current_worker_ticker != target_ticker:
            current_worker_ticker = target_ticker
            tk_yf = yf.Ticker(current_worker_ticker)
            print(f"[WORKER] Switched to fetching: {current_worker_ticker}")
            
        try:
            spot_price = 0.0
            
            # --- 1. GET SPOT PRICE (Robust Method) ---
            # Try fast_info first (works for stocks)
            try:
                spot_price = tk_yf.fast_info['lastPrice']
            except:
                spot_price = None

            # If fast_info failed (common for Indices like ^SPX), try 1-minute history
            if spot_price is None or np.isnan(spot_price):
                try:
                    # Get last 5 minutes to ensure we have a candle
                    hist = tk_yf.history(period="1d", interval="1m")
                    if not hist.empty:
                        spot_price = hist['Close'].iloc[-1]
                    else:
                        # Fallback to daily close if market is closed/no 1m data
                        hist_d = tk_yf.history(period="1d")
                        spot_price = hist_d['Close'].iloc[-1]
                except Exception as e:
                    print(f"Price fetch failed: {e}")

            if spot_price is None or spot_price <= 0:
                print(f"[WORKER] Waiting for price data for {current_worker_ticker}...")
                refresh_event.wait(2.0)
                continue

            # --- 2. GET OPTIONS CHAIN ---
            try:
                expirations = tk_yf.options
            except:
                expirations = []

            if not expirations:
                with lock:
                    if current_worker_ticker == TICKER:
                        is_blocked = True
                print(f"[WORKER] No options found for {current_worker_ticker}.")
                refresh_event.wait(DATA_REFRESH_SEC)
                continue

            all_strikes_gex = pd.Series(dtype=float)

            # --- 3. CALCULATE GEX (Restored to 4 Expirations) ---
            # Using 4 expirations matches your original logic
            for exp in expirations[:4]:
                if stop_event.is_set(): break

                # Calculate Time to Expiry
                exp_date = datetime.strptime(exp, "%Y-%m-%d")
                days_to_exp = (exp_date - datetime.today()).days
                T = max(days_to_exp / 365.0, 0.001)
                
                try:
                    opt = tk_yf.option_chain(exp)
                    calls, puts = opt.calls, opt.puts
                except:
                    continue

                # --- 4. SANITIZE DATA (Crucial for ^SPX) ---
                # Indices often have NaN IV or Open Interest. 
                # We fill NaN with 0 to prevent the math from returning NaN and erasing the bar.
                
                if not calls.empty:
                    calls['impliedVolatility'] = calls['impliedVolatility'].fillna(0)
                    calls['openInterest'] = calls['openInterest'].fillna(0)
                    
                    calls['gamma'] = calc_gamma_vectorized(spot_price, calls['strike'], T, RISK_FREE_RATE, calls['impliedVolatility'])
                    calls['gex'] = calls['openInterest'] * calls['gamma'] * spot_price * 100
                    all_strikes_gex = all_strikes_gex.add(calls.groupby('strike')['gex'].sum(), fill_value=0)

                if not puts.empty:
                    puts['impliedVolatility'] = puts['impliedVolatility'].fillna(0)
                    puts['openInterest'] = puts['openInterest'].fillna(0)
                    
                    puts['gamma'] = calc_gamma_vectorized(spot_price, puts['strike'], T, RISK_FREE_RATE, puts['impliedVolatility'])
                    # Note: Puts are negative GEX
                    puts['gex'] = - (puts['openInterest'] * puts['gamma'] * spot_price * 100)
                    all_strikes_gex = all_strikes_gex.add(puts.groupby('strike')['gex'].sum(), fill_value=0)

            if stop_event.is_set(): break

            df = all_strikes_gex.reset_index()
            df.columns = ['Strike', 'GEX']
            
            min_strike = spot_price * (1 - SPOT_PRICE_RANGE)
            max_strike = spot_price * (1 + SPOT_PRICE_RANGE)
            df = df[(df['Strike'] >= min_strike) & (df['Strike'] <= max_strike)]
            
            df = df.sort_values('Strike')
            df['GEX_Billions'] = df['GEX'] / 1e9

            with lock:
                if current_worker_ticker == TICKER:
                    latest_df = df
                    latest_spot = spot_price
                    is_blocked = False
                    
        except Exception as e:
            with lock:
                if current_worker_ticker == TICKER:
                    is_blocked = True
            print(f"[WORKER] Error loop: {e}")

        refresh_event.wait(DATA_REFRESH_SEC)

# --- 4. START THREAD ---
thread = threading.Thread(target=data_worker, daemon=True)
thread.start()

# --- 5. UI SETUP & ANIMATION ---
plt.style.use('dark_background')
fig, ax = plt.subplots(figsize=(10, 8))

def animate(i):
    current_time_str = datetime.now().strftime("%H:%M:%S")
    
    with lock:
        df = latest_df
        spot = latest_spot
        blocked = is_blocked
        active_ticker = TICKER

    display_ticker = active_ticker.replace("^", "")

    ax.clear()
    
    if df is not None and not df.empty:
        colors = ['#ff4c4c' if val < 0 else '#4cff4c' for val in df['GEX_Billions']]
        
        # Dynamic bar height based on strike density
        bar_height = 1.0
        if spot > 2000: bar_height = 2.0  # For SPX/NDX
        
        ax.barh(df['Strike'], df['GEX_Billions'], color=colors, height=bar_height)
        ax.axhline(y=spot, color='white', linestyle='--', linewidth=2, label=f'Spot ({spot:.2f})')

        ax.set_xlabel('Net GEX (Billions of $)', fontsize=12)
        ax.set_ylabel('Strike Price', fontsize=12)
        ax.grid(True, alpha=0.2)
        ax.legend(loc="upper right")
    else:
        status = "Rate Limited / No Data" if blocked else "Loading..."
        ax.text(0.5, 0.5, f"{status}\n{display_ticker}", 
                color="white", fontsize=14, ha='center', va='center', transform=ax.transAxes)

    title_color = 'red' if blocked else 'white'
    ax.set_title(f'{display_ticker} Gamma Exposure Profile\n{current_time_str}', color=title_color, fontsize=16, fontweight='bold')
    
    fig.tight_layout()

# --- 6. TKINTER GUI ---
print("[UI] Starting Application...")
root = tk.Tk()
root.title("GEX Profile Viewer")
root.configure(bg='#1c1c1c')

# --- CLEAN CLOSING PROTOCOL ---
def on_closing():
    print("[UI] Closing application safely...")
    stop_event.set()     # Tell thread to stop
    refresh_event.set()  # Wake thread so it checks the stop flag
    if 'ani' in globals():
        ani.event_source.stop()
    root.quit()
    root.destroy()
    os._exit(0) # Force kill to prevent background hanging

root.protocol("WM_DELETE_WINDOW", on_closing)

ctrl_frame = tk.Frame(root, bg='#1c1c1c')
ctrl_frame.pack(side=tk.TOP, fill=tk.X, padx=15, pady=10)

tk.Label(ctrl_frame, text="Ticker:", fg='white', bg='#1c1c1c', font=("Arial", 12)).pack(side=tk.LEFT, padx=(0, 10))

combo = ttk.Combobox(ctrl_frame, values=POPULAR_TICKERS, font=("Arial", 12), width=15)
combo.set(TICKER.replace("^", "")) 
combo.pack(side=tk.LEFT, padx=5)

def on_keyrelease(event):
    if event.keysym in ['Up', 'Down', 'Left', 'Right', 'Return', 'Tab']: return
    value = combo.get().upper()
    if value == '':
        combo['values'] = POPULAR_TICKERS
    else:
        hits = [item for item in POPULAR_TICKERS if item.startswith(value)]
        combo['values'] = hits

combo.bind('<KeyRelease>', on_keyrelease)

def load_ticker(*args):
    global TICKER, latest_df, is_blocked
    new_t = combo.get().upper().strip()
    if not new_t: return
    
    # Auto-fix indices
    if new_t in ["SPX", "NDX", "VIX", "RUT"] and not new_t.startswith("^"):
        new_t = f"^{new_t}"
        
    with lock:
        if TICKER != new_t:
            print(f"[UI] Loading: {new_t}")
            TICKER = new_t
            latest_df = None 
            is_blocked = False
            
    refresh_event.set()

combo.bind('<Return>', load_ticker)
combo.bind('<<ComboboxSelected>>', load_ticker) 

load_btn = tk.Button(ctrl_frame, text="Plot", command=load_ticker, font=("Arial", 10, "bold"), bg="#4c4cff", fg="white", relief="flat")
load_btn.pack(side=tk.LEFT, padx=10)

canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)

ani = animation.FuncAnimation(fig, animate, interval=CLOCK_REFRESH_SEC * 1000, cache_frame_data=False)

root.mainloop()
