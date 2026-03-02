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

# --- HIDE THE MATPLOTLIB TOOLBAR ---
plt.rcParams['toolbar'] = 'None'

# --- 1. SETTINGS & GLOBALS ---
TICKER = "SPY"
RISK_FREE_RATE = 0.045     # 4.5%
SPOT_PRICE_RANGE = 0.05    # +/- 5% from spot price
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

# This event allows us to instantly interrupt the background sleep delay
refresh_event = threading.Event()

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
    global latest_df, latest_spot, is_blocked, TICKER
    
    current_worker_ticker = ""
    tk_yf = None

    while True:
        # Clear the event at the start of each loop
        refresh_event.clear()
        
        with lock:
            target_ticker = TICKER
            
        # If user changed the ticker, update the yfinance object
        if current_worker_ticker != target_ticker:
            current_worker_ticker = target_ticker
            tk_yf = yf.Ticker(current_worker_ticker)
            print(f"[WORKER] Switched to fetching data for: {current_worker_ticker}")
            
        try:
            try:
                spot_price = tk_yf.fast_info['lastPrice']
            except:
                spot_price = tk_yf.history(period="1d")['Close'].iloc[-1]

            expirations = tk_yf.options
            if not expirations:
                with lock:
                    if current_worker_ticker == TICKER:
                        is_blocked = True
                print(f"[WORKER] No options data found for {current_worker_ticker}.")
                refresh_event.wait(DATA_REFRESH_SEC)
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

            df = all_strikes_gex.reset_index()
            df.columns = ['Strike', 'GEX']
            
            min_strike = spot_price * (1 - SPOT_PRICE_RANGE)
            max_strike = spot_price * (1 + SPOT_PRICE_RANGE)
            df = df[(df['Strike'] >= min_strike) & (df['Strike'] <= max_strike)]
            
            df = df.sort_values('Strike')
            df['GEX_Billions'] = df['GEX'] / 1e9

            with lock:
                # Ensure we don't overwrite if user changed ticker mid-download
                if current_worker_ticker == TICKER:
                    latest_df = df
                    latest_spot = spot_price
                    is_blocked = False
                    
        except Exception as e:
            with lock:
                if current_worker_ticker == TICKER:
                    is_blocked = True
            print(f"[WORKER] Error pulling {current_worker_ticker}: {e}")

        # Sleep for DATA_REFRESH_SEC, OR wake up instantly if 'refresh_event.set()' is called
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

    # Clean up display name (e.g., "^SPX" -> "SPX")
    display_ticker = active_ticker.replace("^", "")

    ax.clear()
    
    if df is not None and not df.empty:
        colors = ['#ff4c4c' if val < 0 else '#4cff4c' for val in df['GEX_Billions']]
        ax.barh(df['Strike'], df['GEX_Billions'], color=colors, height=1.0)
        
        ax.axhline(y=spot, color='white', linestyle='--', linewidth=2, label=f'Spot (${spot:.2f})')

        ax.set_xlabel('Net GEX (Billions of $)', fontsize=12)
        ax.set_ylabel('Strike Price', fontsize=12)
        ax.grid(True, alpha=0.2)
        ax.legend(loc="upper right")
    else:
        ax.text(0.5, 0.5, f"Loading options data for {display_ticker}...", 
                color="white", fontsize=14, ha='center', va='center', transform=ax.transAxes)

    if blocked:
        ax.set_title(f'⚠️ RATE LIMITED / NO DATA ({display_ticker}) ⚠️\nClock: {current_time_str}', color='red', fontsize=16, fontweight='bold')
    else:
        ax.set_title(f'{display_ticker} Live Gamma Exposure (GEX)\nClock: {current_time_str}', color='white', fontsize=16, fontweight='bold')

    fig.tight_layout()

# --- 6. TKINTER GUI ---
print("[UI] Starting Application...")
root = tk.Tk()
root.title("GEX Profile Viewer")
root.configure(bg='#1c1c1c')

ctrl_frame = tk.Frame(root, bg='#1c1c1c')
ctrl_frame.pack(side=tk.TOP, fill=tk.X, padx=15, pady=10)

tk.Label(ctrl_frame, text="Asset Ticker:", fg='white', bg='#1c1c1c', font=("Arial", 12)).pack(side=tk.LEFT, padx=(0, 10))

# Combobox
combo = ttk.Combobox(ctrl_frame, values=POPULAR_TICKERS, font=("Arial", 12), width=15)
combo.set(TICKER.replace("^", "")) # Initial value
combo.pack(side=tk.LEFT, padx=5)

def on_keyrelease(event):
    if event.keysym in ['Up', 'Down', 'Left', 'Right', 'Return', 'Tab']:
        return
    
    value = combo.get().upper()
    if value == '':
        combo['values'] = POPULAR_TICKERS
    else:
        hits = [item for item in POPULAR_TICKERS if item.startswith(value)]
        combo['values'] = hits

combo.bind('<KeyRelease>', on_keyrelease)

def load_ticker(*args):
    global TICKER, latest_df, is_blocked
    
    # Read text directly from the physical input box
    new_t = combo.get().upper().strip()
    if not new_t:
        return
    
    # Map index names properly for Yahoo Finance
    if new_t in ["SPX", "NDX", "VIX", "RUT"]:
        new_t = f"^{new_t}"
        
    with lock:
        if TICKER != new_t:
            print(f"[UI] Requesting new chart for: {new_t}")
            TICKER = new_t
            latest_df = None  # Clear screen to say "Loading..."
            is_blocked = False
            
    # Force the background worker to wake up instantly
    refresh_event.set()

combo.bind('<Return>', load_ticker)
combo.bind('<<ComboboxSelected>>', load_ticker) 

load_btn = tk.Button(ctrl_frame, text="Plot GEX", command=load_ticker, font=("Arial", 10, "bold"), bg="#4c4cff", fg="white", relief="flat")
load_btn.pack(side=tk.LEFT, padx=10)

canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)

ani = animation.FuncAnimation(fig, animate, interval=CLOCK_REFRESH_SEC * 1000, cache_frame_data=False)

root.mainloop()
