import pandas as pd
import yfinance as yf
import time
from datetime import datetime
import socket
import math
import numpy as np

# --- CONFIGURATION ---
TICKER = "^SPX"
FUTURES_TICKER = "ES=F"
VIX_TICKER = "^VIX"
UDP_IP = "127.0.0.1"
UDP_PORT = 9000

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
history_state = {} # Stores previous price/vol to calculate PVOI flow

def get_latest_price(symbol):
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="1d", interval="1m")
        return hist['Close'].iloc[-1] if not hist.empty else 0.0
    except:
        return 0.0

def calculate_implied_metrics(spot, calls, puts):
    """ Hack: Put-Call Parity to find real implied Cost of Carry (r - q) """
    try:
        # Find ATM Strike
        calls['abs_diff'] = abs(calls['strike'] - spot)
        atm_strike = calls.loc[calls['abs_diff'].idxmin()]['strike']
        
        atm_call = calls[calls['strike'] == atm_strike].iloc[0]['lastPrice']
        atm_put = puts[puts['strike'] == atm_strike].iloc[0]['lastPrice']
        
        # C - P = S - K * e^(-rt). Rough approximation for cost of carry proxy
        implied_forward = atm_strike + (atm_call - atm_put)
        cost_of_carry = math.log(implied_forward / spot) if spot > 0 else 0.05
        return cost_of_carry
    except:
        return 0.05 # Fallback to 5%

def fetch_and_send_data(tier='fast'):
    global history_state
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Executing {tier.upper()} scan...")
        
        spot = get_latest_price(TICKER)
        fut = get_latest_price(FUTURES_TICKER)
        vix = get_latest_price(VIX_TICKER)
        if spot == 0 or fut == 0: return

        basis_ratio = fut / spot
        ticker = yf.Ticker(TICKER)
        expirations = ticker.options
        if not expirations: return

        # Tiered Date Filtering
        today = datetime.now()
        selected_exps = []
        for exp in expirations:
            dte = (datetime.strptime(exp, '%Y-%m-%d') - today).days
            if tier == 'fast' and dte <= 1: selected_exps.append(exp)
            elif tier == 'medium' and 1 < dte <= 14: selected_exps.append(exp)
            elif tier == 'slow' and dte > 14: selected_exps.append(exp)

        all_opts = []
        cost_of_carry = 0.05

        for i, exp in enumerate(selected_exps):
            chain = ticker.option_chain(exp)
            if i == 0: cost_of_carry = calculate_implied_metrics(spot, chain.calls, chain.puts)

            for opt_type, df in [('1', chain.calls), ('0', chain.puts)]: # 1=Call, 0=Put
                # Filter to +/- 10% of spot to keep UDP packet small and fast
                df = df[(df['strike'] > spot * 0.9) & (df['strike'] < spot * 1.1)]
                
                for _, row in df.iterrows():
                    strike = row['strike']
                    uid = f"{exp}_{opt_type}_{strike}"
                    price = row['lastPrice']
                    vol = row['volume'] if not np.isnan(row['volume']) else 0
                    oi = row['openInterest'] if not np.isnan(row['openInterest']) else 0
                    iv = row['impliedVolatility']

                    # PVOI Directionality Hack
                    flow_dir = 0
                    if uid in history_state:
                        prev_price, prev_vol = history_state[uid]
                        if vol > prev_vol:
                            if price > prev_price: flow_dir = 1   # Buying pressure (Opening)
                            elif price < prev_price: flow_dir = -1 # Selling pressure (Closing)
                    
                    history_state[uid] = (price, vol)
                    dte = max(0.001, (datetime.strptime(exp, '%Y-%m-%d') - today).days / 365.0)
                    
                    # Format: DTE, IsCall, Strike, OI, IV, FlowDir
                    all_opts.append(f"{dte:.4f},{opt_type},{strike},{oi},{iv:.4f},{flow_dir}")

        if not all_opts: return

        # Header Format: Timestamp|BasisRatio|VIX|Spot|CostOfCarry
        header = f"{datetime.now().timestamp()},{basis_ratio:.6f},{vix:.4f},{spot:.2f},{cost_of_carry:.4f}"
        
        # Combine and send via UDP
        payload = header + "|" + "|".join(all_opts)
        
        # Chunking if payload is too large for single UDP datagram (> 60KB)
        chunk_size = 50000 
        for i in range(0, len(payload), chunk_size):
            sock.sendto(payload[i:i+chunk_size].encode(), (UDP_IP, UDP_PORT))
            
        print(f"[{datetime.now().strftime('%H:%M:%S')}] UDP Packet Sent! ({len(all_opts)} strikes)")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("--- Institutional UDP GEX Engine Started ---")
    loop_count = 0
    while True:
        fetch_and_send_data('fast') # Every 1 minute
        if loop_count % 5 == 0: fetch_and_send_data('medium') # Every 5 minutes
        if loop_count % 60 == 0: fetch_and_send_data('slow') # Every 60 minutes
        
        loop_count += 1
        time.sleep(60)
