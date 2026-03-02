Here is the updated, sleek README.md formatted to reflect the new interactive GUI, search bar, and instant-loading updates.

I also cleaned up the weird artifacts (content_copy, expand_less, etc.) from your previous copy-paste so this will format perfectly on GitHub.

Copy and paste the text below into your README.md file:

code
Markdown
download
content_copy
expand_less
# 📈 Live GEX (Gamma Exposure) Profile Tracker

> A blazing-fast, interactive Python GUI that calculates and visualizes real-time Gamma Exposure (GEX) for options markets.

By calculating the Black-Scholes Gamma for current option chains, this tool visualizes where Market Makers are positioned, helping you spot heavy support/resistance levels and potential volatility zones in real-time.

![GEX Tracker Screenshot](image.png)

---

## ✨ Key Features

* 🔍 **Interactive Search Bar:** Instantly switch between assets with an autocomplete dropdown menu. No need to edit the code! Automatically maps index names (e.g., `SPX` maps to `^SPX` behind the scenes).
* ⚡ **Instant Data Fetching:** Multithreaded architecture with event triggers fetches new data the exact moment you hit "Enter" or click "Plot", without freezing the UI.
* 🚀 **Blazing Fast Math:** Uses `numpy` and `scipy` vectorization to calculate Black-Scholes Greeks for hundreds of strikes in milliseconds.
* ⏱️ **Live Auto-Updating:** The chart updates seamlessly with a 1-second ticking clock and auto-refreshing data in the background.
* 🛡️ **Rate-Limit Protected:** Includes error handling to detect and warn you if Yahoo Finance temporarily blocks your IP or lacks options data.

---

## 🛠️ Installation & Requirements

You will need Python installed on your system. To install the required libraries, run the following command in your terminal:


pip install yfinance pandas numpy scipy matplotlib

(Note: tkinter is used for the GUI and comes pre-installed with standard Python. Linux users may need to run sudo apt install python3-tk if it is missing).

🚀 How to Use

Clone or download the script.

Run the script via your terminal or IDE:

Select an Asset: Use the text box at the top to type a ticker (e.g., AAPL, TSLA, NVDA). The tool will auto-suggest popular tickers as you type.

Indices: To plot major indices, just type SPX, NDX, VIX, or RUT. The tool handles the Yahoo Finance formatting automatically.

Hit Enter, click Plot GEX, or select an item from the dropdown to instantly load the new options chain.

## 📊 How to Read the Chart

The chart displays the Net Gamma Exposure (in Billions of dollars) on the X-axis, and the Strike Price on the Y-axis.

🟢 Green Bars (Positive GEX / Call Walls)
Dealers are Long Gamma. As price approaches these strikes, dealers will trade against the trend (selling the rips, buying the dips). This acts as a magnet and heavy resistance/support keeping the market calm.

🔴 Red Bars (Negative GEX / Put Walls)
Dealers are Short Gamma. If the price drops into these zones, dealers are forced to sell into the drop to hedge their risk. This acts as a volatility accelerator, meaning the price can move very fast and violently through these strikes.

⚪ Dotted White Line
The current, live Spot Price of the underlying asset.

## ⚠️ Disclaimer

This script pulls data from Yahoo Finance's unofficial API (yfinance). If you set the DATA_REFRESH_SEC too low (e.g., updating every 0.5 seconds), Yahoo may temporarily rate-limit or block your IP address. A safe refresh rate is between 2 to 10 seconds.

Not Financial Advice. This tool is for educational and research purposes only. Options trading carries significant risk.

