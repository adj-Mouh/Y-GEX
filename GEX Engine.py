import yfinance as yf
import pandas as pd
import time
import os
import datetime
import logging

# --- CONFIGURATION ---
# Adjust all settings for the engine here.
CONFIG = {
    # Ticker for options (e.g., '^SPX', 'SPY', 'QQQ', 'IWM').
    "ticker_symbol": "^SPX", 
    
    # Path to save the CSV files for NinjaTrader to read.
    "output_folder_path": r"C:\Options_History_Data",
    
    # How often to fetch new data from Yahoo Finance (in seconds). 900 = 15 minutes.
    "fetch_interval_seconds": 900,
    
    # How many days into the future to look for option expirations.
    "expiration_day_limit": 45,
    
    # Filter out strikes that are more than this % away from the spot price.
    "strike_filter_percentage": 15.0,
    
    # How many hours of old CSV files to keep before deleting them.
    "hours_to_keep_files": 24
}
# --- END OF CONFIGURATION ---

# Setup professional logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def fetch_data(ticker_obj, expiration_limit_days):
    """
    Fetches spot price and all relevant option chain data from yfinance.
    
    Args:
        ticker_obj (yf.Ticker): The yfinance Ticker object.
        expiration_limit_days (int): The number of days out to fetch expirations for.

    Returns:
        A tuple containing: (spot_price, list_of_option_dataframes) or (None, []) on failure.
    """
    try:
        # 1. Get current spot price
        history = ticker_obj.history(period='1d', interval='1m')
        if history.empty:
            logging.error("Could not fetch recent history to determine spot price.")
            return None, []
        spot_price = history['Close'].iloc[-1]

        # 2. Get all expirations within the configured day limit
        all_expirations = ticker_obj.options
        today = datetime.datetime.now()
        limit_date = today + datetime.timedelta(days=expiration_limit_days)
        
        relevant_expirations = [
            exp for exp in all_expirations 
            if datetime.datetime.strptime(exp, '%Y-%m-%d') <= limit_date
        ]

        if not relevant_expirations:
            logging.warning("No option expirations found within the configured date limit.")
            return spot_price, []

        # 3. Fetch Calls and Puts for each relevant expiration date
        all_options_dfs = []
        for exp in relevant_expirations:
            opt_chain = ticker_obj.option_chain(exp)
            
            calls = opt_chain.calls
            calls['Type'] = 'Call'
            calls['Expiration'] = exp
            
            puts = opt_chain.puts
            puts['Type'] = 'Put'
            puts['Expiration'] = exp
            
            all_options_dfs.extend([calls, puts])
            
        logging.info(f"Successfully fetched {len(all_options_dfs)} option sets for {len(relevant_expirations)} expirations.")
        return spot_price, all_options_dfs

    except Exception as e:
        logging.error(f"An error occurred during yfinance data fetch: {e}")
        return None, []

def process_and_save_data(dataframes, spot_price, config):
    """
    Cleans, formats, and saves the final data to a CSV file for NinjaTrader.
    """
    if not dataframes:
        logging.warning("No dataframes to process.")
        return

    try:
        # 1. Combine all data into a single DataFrame
        full_df = pd.concat(dataframes, ignore_index=True)

        # 2. Filter for relevant strikes based on the spot price
        strike_filter = config["strike_filter_percentage"] / 100.0
        min_strike = spot_price * (1 - strike_filter)
        max_strike = spot_price * (1 + strike_filter)
        full_df = full_df[(full_df['strike'] >= min_strike) & (full_df['strike'] <= max_strike)]

        # 3. Clean the data for NinjaTrader
        full_df['openInterest'] = full_df['openInterest'].fillna(0)
        full_df['impliedVolatility'] = full_df['impliedVolatility'].fillna(0.0001)
        
        # 4. Add timestamps and the underlying spot price (for basis ratio calculation in C#)
        timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_df['Timestamp'] = timestamp_str
        full_df['UnderlyingSpot'] = round(spot_price, 2)
        
        # 5. Select and rename columns to match C# expectations
        final_df = full_df[[
            'Timestamp', 
            'Expiration', 
            'UnderlyingSpot', 
            'Type', 
            'strike', 
            'openInterest', 
            'impliedVolatility'
        ]]
        final_df.rename(columns={
            'strike': 'Strike', 
            'openInterest': 'OI', 
            'impliedVolatility': 'IV'
        }, inplace=True)
        
        # 6. Save to CSV
        output_folder = config["output_folder_path"]
        safe_ticker_name = config["ticker_symbol"].replace('^', '').replace('.', '_')
        file_name = f"{safe_ticker_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            logging.info(f"Created output directory: {output_folder}")
            
        output_path = os.path.join(output_folder, file_name)
        final_df.to_csv(output_path, index=False)
        logging.info(f"Successfully saved data to {output_path}")

    except Exception as e:
        logging.error(f"Failed to process and save data: {e}")

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
