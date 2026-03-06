import yfinance as yf
import pandas as pd
import time
import os
import datetime
import logging

CONFIG = {
    "ticker_symbol": "^SPX", 
    "future_symbol": "ES=F", # We need this to calculate the basis
    "output_folder_path": r"C:\Options_History_Data",
    "fetch_interval_seconds": 900,
    "expiration_day_limit": 45,
    "strike_filter_percentage": 15.0,
    "hours_to_keep_files": 24
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_synced_basis():
    """
    Downloads 1m data for both SPX and ES. Finds the most recent minute 
    where BOTH were trading to calculate a flawless Basis Ratio, ignoring ETH.
    """
    try:
        # Download last 5 days of 1-minute data for both to ensure we catch Friday->Monday
        tickers = f"{CONFIG['ticker_symbol']} {CONFIG['future_symbol']}"
        data = yf.download(tickers, period="5d", interval="1m", progress=False)['Close']
        
        # Drop rows where either SPX or ES has NaN (This instantly filters out ETH and post-market)
        synced_data = data.dropna()
        
        if synced_data.empty:
            logging.error("Could not find synchronized timestamps for SPX and ES.")
            return None, None

        # Get the absolute last row where both traded
        last_sync_row = synced_data.iloc[-1]
        
        spx_sync_price = last_sync_row[CONFIG['ticker_symbol']]
        es_sync_price = last_sync_row[CONFIG['future_symbol']]
        
        # Calculate Basis Ratio (e.g., 5120 / 5100 = 1.00392)
        basis_ratio = es_sync_price / spx_sync_price
        
        logging.info(f"Synced Basis Calculated: ES @ {es_sync_price:.2f} / SPX @ {spx_sync_price:.2f} = {basis_ratio:.5f}")
        return spx_sync_price, basis_ratio

    except Exception as e:
        logging.error(f"Error calculating basis: {e}")
        return None, None

def fetch_data(ticker_obj, expiration_limit_days):
    # 1. Get the perfectly synced Spot Price and Basis Ratio
    spot_price, basis_ratio = calculate_synced_basis()
    if spot_price is None:
        return None, None, []

    try:
        # 2. Fetch Options
        all_expirations = ticker_obj.options
        today = datetime.datetime.now()
        limit_date = today + datetime.timedelta(days=expiration_limit_days)
        
        relevant_expirations = [exp for exp in all_expirations if datetime.datetime.strptime(exp, '%Y-%m-%d') <= limit_date]

        if not relevant_expirations:
            return spot_price, basis_ratio, []

        all_options_dfs = []
        for exp in relevant_expirations:
            opt_chain = ticker_obj.option_chain(exp)
            calls, puts = opt_chain.calls, opt_chain.puts
            calls['Type'], calls['Expiration'] = 'Call', exp
            puts['Type'], puts['Expiration'] = 'Put', exp
            all_options_dfs.extend([calls, puts])
            
        return spot_price, basis_ratio, all_options_dfs

    except Exception as e:
        logging.error(f"Error fetching options: {e}")
        return None, None, []

def process_and_save_data(dataframes, spot_price, basis_ratio, config):
    if not dataframes: return

    try:
        full_df = pd.concat(dataframes, ignore_index=True)

        # Filter strikes
        strike_filter = config["strike_filter_percentage"] / 100.0
        full_df = full_df[(full_df['strike'] >= spot_price * (1 - strike_filter)) & (full_df['strike'] <= spot_price * (1 + strike_filter))]

        full_df['openInterest'] = full_df['openInterest'].fillna(0)
        full_df['impliedVolatility'] = full_df['impliedVolatility'].fillna(0.0001)
        
        # ADD TIMESTAMPS AND THE PRE-CALCULATED BASIS RATIO
        full_df['Timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_df['BasisRatio'] = round(basis_ratio, 6) # <--- Python did the math!
        
        final_df = full_df[[
            'Timestamp', 'Expiration', 'BasisRatio', 'Type', 'strike', 'openInterest', 'impliedVolatility'
        ]].copy()
        
        final_df.rename(columns={'strike': 'Strike', 'openInterest': 'OI', 'impliedVolatility': 'IV'}, inplace=True)
        
        output_folder = config["output_folder_path"]
        if not os.path.exists(output_folder): os.makedirs(output_folder)
            
        file_name = f"{config['ticker_symbol'].replace('^', '').replace('.', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        final_df.to_csv(os.path.join(output_folder, file_name), index=False)
        logging.info(f"Saved {len(final_df)} strikes with Basis Ratio {basis_ratio:.5f}")

    except Exception as e:
        logging.error(f"Failed to process/save data: {e}")

# ... (keep cleanup_files and main() exactly the same as before) ...

def cleanup_files(config):
    """
    Deletes old CSV files from the output directory to save space.
    """
    try:
        folder = config["output_folder_path"]
        if not os.path.isdir(folder):
            return

        now = time.time()
        age_limit_seconds = config["hours_to_keep_files"] * 3600
        cleaned_count = 0

        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.stat(file_path).st_mtime < now - age_limit_seconds:
                os.remove(file_path)
                cleaned_count += 1
        
        if cleaned_count > 0:
            logging.info(f"Cleaned up {cleaned_count} old data files.")
            
    except Exception as e:
        logging.error(f"Error during file cleanup: {e}")


def main():
    """
    Main loop to run the GEX data engine.
    """
    logging.info(f"GEX Engine started for ticker: {CONFIG['ticker_symbol']}")
    logging.info(f"Data will be saved to: {CONFIG['output_folder_path']}")
    
    ticker = yf.Ticker(CONFIG["ticker_symbol"])

    while True:
        logging.info("--- Starting new fetch cycle ---")
        
        spot, dfs = fetch_data(ticker, CONFIG["expiration_day_limit"])
        
        if spot and dfs:
            process_and_save_data(dfs, spot, CONFIG)
        else:
            logging.warning("Skipping processing due to fetch failure.")
            
        cleanup_files(CONFIG)
        
        interval = CONFIG["fetch_interval_seconds"]
        logging.info(f"--- Cycle finished. Sleeping for {interval / 60:.1f} minutes ---")
        time.sleep(interval)


if __name__ == "__main__":
    main()
