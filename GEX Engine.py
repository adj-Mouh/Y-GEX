import yfinance as yf
import time
import socket
import threading
from datetime import datetime

# --- CONFIGURATION ---
TICKER = "^SPX"
VIX_TICKER = "^VIX1D"
UDP_IP = "127.0.0.1"
UDP_PORT = 11000
REFRESH_INTERVAL_VIX = 60  # Fetch VIX1D every 60 seconds

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def fetch_baseline_options():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching baseline OI for {TICKER}...")
    try:
        spx = yf.Ticker(TICKER)
        expirations = spx.options
        if not expirations:
            return
        
        # Fetch 0DTE (first expiration)
        opt_chain = spx.option_chain(expirations[0])
        calls = opt_chain.calls
        puts = opt_chain.puts
        
        # Format: OPT|Strike|CallOI|PutOI|CallIV|PutIV
        for index, row in calls.iterrows():
            strike = row['strike']
            call_oi = row['openInterest'] if not type(row['openInterest']) == float else 0
            call_iv = row['impliedVolatility']
            
            # Find matching put
            put_match = puts[puts['strike'] == strike]
            put_oi = put_match['openInterest'].values[0] if not put_match.empty else 0
            put_iv = put_match['impliedVolatility'].values[0] if not put_match.empty else 0
            
            msg = f"OPT|{strike}|{call_oi}|{put_oi}|{call_iv}|{put_iv}"
            sock.sendto(msg.encode('utf-8'), (UDP_IP, UDP_PORT))
            
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Baseline Options Data Sent via UDP.")
        
    except Exception as e:
        print(f"Error fetching options: {e}")

def fetch_live_vix():
    while True:
        try:
            vix = yf.Ticker(VIX_TICKER)
            hist = vix.history(period="1d", interval="1m")
            if not hist.empty:
                last_vix = hist['Close'].iloc[-1]
                msg = f"VIX|{last_vix}"
                sock.sendto(msg.encode('utf-8'), (UDP_IP, UDP_PORT))
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent VIX1D Update: {last_vix}")
        except Exception as e:
            print(f"Error fetching VIX1D: {e}")
            
        time.sleep(REFRESH_INTERVAL_VIX)

if __name__ == "__main__":
    print("Starting Synthetic GEX Data Server...")
    # 1. Send Baseline
    fetch_baseline_options()
    
    # 2. Start VIX1D loop in main thread
    fetch_live_vix()
