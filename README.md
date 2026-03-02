# Y-GEX

Live Gamma Exposure (GEX) Profile Tracker

A blazing fast, multithreaded Python tool that calculates and visualizes live Gamma Exposure (GEX) for any stock ticker. It uses Yahoo Finance options data and the Black-Scholes model to create a real-time, auto-updating market profile chart.

Features

Live Updates: Background multithreading ensures the chart stays blazing fast and never freezes.

Vectorized Math: Instantly calculates the Black-Scholes Gamma for hundreds of option strikes.

Clean UI: Standalone, auto-centered dark-mode window with no distracting toolbars.

Smart Rate-Limiting: Handles Yahoo Finance connection drops gracefully.

Requirements

You need Python installed on your system. Install the required libraries using pip:

code
Bash
download
content_copy
expand_less
pip install yfinance pandas numpy scipy matplotlib
How to Run

Save the script as gex_profile.py and run it from your terminal:

code
Bash
download
content_copy
expand_less
python gex_profile.py
How to Read the Chart

🟩 Green Bars (Positive GEX): "Call Walls". Dealers are Long Gamma here. These levels act as resistance and act like magnets pinning the price.

🟥 Red Bars (Negative GEX): "Put Walls". Dealers are Short Gamma here. If the price drops below these levels, volatility usually explodes.

⚪ White Dotted Line: The current live spot price of the stock.

Settings

You can easily customize the tool by changing the variables at the top of the Python file:

code
Python
download
content_copy
expand_less
TICKER = "SPY"             # Change to any stock/ETF (e.g., "AAPL", "QQQ")
SPOT_PRICE_RANGE = 0.05    # Zooms the chart to +/- 5% of the current price
DATA_REFRESH_SEC = 2       # How often to fetch new data (in seconds)
Disclaimer

This tool pulls data from Yahoo Finance's free endpoints. If you set DATA_REFRESH_SEC too low (e.g., 1 second), Yahoo may temporarily rate-limit or block your IP address.
