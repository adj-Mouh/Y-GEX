using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Media;

// Standard NinjaTrader 8 namespaces
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.DrawingTools;

namespace NinjaTrader.NinjaScript.Indicators
{
    // IMPORTANT: The class name MUST match your file name (MyCustomIndicator2)
    public class MyCustomIndicator2 : Indicator
    {
        // --- UDP & Data State ---
        private UdpClient udpServer;
        private Task listenerTask;
        private bool isListening = false;
        
        private ConcurrentDictionary<double, OptionLevel> optionsChain = new ConcurrentDictionary<double, OptionLevel>();
        private double currentVix1D = 15.0; // Default fallback
        private double riskFreeRate = 0.05;

        // --- Aggregated Tick Volume ---
        private double accumulatedAggressiveBuyVol = 0;
        private double accumulatedAggressiveSellVol = 0;
        private DateTime lastCalculationTime;

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description = "Advanced Synthetic GEX with OI Gravity and UDP Data";
                Name = "MyCustomIndicator2"; // Update this name to match as well
                Calculate = Calculate.OnEachTick; 
                IsOverlay = true;
            }

            else if (State == State.DataLoaded)
            {
                StartUdpListener();
                lastCalculationTime = DateTime.Now;
            }
            else if (State == State.Terminated)
            {
                isListening = false;
                if (udpServer != null) udpServer.Close();
            }
        }

        private void StartUdpListener()
        {
            isListening = true;
            udpServer = new UdpClient(11000);
            listenerTask = Task.Run(() =>
            {
                IPEndPoint remoteEP = new IPEndPoint(IPAddress.Any, 11000);
                while (isListening)
                {
                    try
                    {
                        byte[] data = udpServer.Receive(ref remoteEP);
                        string message = Encoding.UTF8.GetString(data);
                        ParseUdpMessage(message);
                    }
                    catch { /* Handle thread aborts cleanly */ }
                }
            });
        }

        private void ParseUdpMessage(string msg)
        {
            string[] parts = msg.Split('|');
            if (parts[0] == "VIX" && parts.Length == 2)
            {
                double.TryParse(parts[1], out currentVix1D);
            }
            else if (parts[0] == "OPT" && parts.Length == 6)
            {
                double strike = double.Parse(parts[1]);
                OptionLevel level = new OptionLevel
                {
                    BaseCallOI = double.Parse(parts[2]),
                    BasePutOI = double.Parse(parts[3]),
                    CallIV = double.Parse(parts[4]),
                    PutIV = double.Parse(parts[5]),
                    SyntheticCallFlow = 0,
                    SyntheticPutFlow = 0
                };
                optionsChain[strike] = level;
            }
        }

        protected override void OnMarketData(MarketDataEventArgs marketDataUpdate)
        {
            // The Hack: Tick Rule (Lee-Ready Approximation)
            // Determine if the live futures trade was aggressive buying or selling
            if (marketDataUpdate.MarketDataType == MarketDataType.Last)
            {
                double price = marketDataUpdate.Price;
                double volume = marketDataUpdate.Volume;
                double ask = GetCurrentAsk();
                double bid = GetCurrentBid();

                if (price >= ask) accumulatedAggressiveBuyVol += volume; // Hitting the Ask -> Calls Bought
                else if (price <= bid) accumulatedAggressiveSellVol += volume; // Hitting the Bid -> Puts Bought
            }
        }

        protected override void OnBarUpdate()
        {
            if (CurrentBar < 20 || optionsChain.IsEmpty) return;

            // Hack: Throttle the heavy math. Only calculate every 5 seconds.
            if ((DateTime.Now - lastCalculationTime).TotalSeconds >= 5)
            {
                DistributeSyntheticFlow(Close[0]);
                RenderHeatmap(Close[0]);
                
                // Reset accumulators
                accumulatedAggressiveBuyVol = 0;
                accumulatedAggressiveSellVol = 0;
                lastCalculationTime = DateTime.Now;
            }
        }

        private void DistributeSyntheticFlow(double currentSpot)
        {
            // Hack: OI Gravity. Distribute volume based on existing baseline OI, not blindly.
            double totalNearbyOI = 0.0;
            double radius = 50.0; // Distribute flow to strikes within 50 points

            foreach (var kvp in optionsChain)
            {
                if (Math.Abs(kvp.Key - currentSpot) <= radius)
                    totalNearbyOI += (kvp.Value.BaseCallOI + kvp.Value.BasePutOI);
            }

            if (totalNearbyOI == 0) return;

            foreach (var kvp in optionsChain)
            {
                double strike = kvp.Key;
                if (Math.Abs(strike - currentSpot) <= radius)
                {
                    var level = kvp.Value;
                    double weight = (level.BaseCallOI + level.BasePutOI) / totalNearbyOI;

                    // Apply flow: Aggressive Buy = Dealers Short Calls = Negative GEX
                    level.SyntheticCallFlow += (accumulatedAggressiveBuyVol * weight);
                    level.SyntheticPutFlow += (accumulatedAggressiveSellVol * weight);
                    
                    optionsChain[strike] = level;
                }
            }
        }

        private void RenderHeatmap(double spotPrice)
        {
            // Calculate time to close (assuming 4:00 PM EST close)
            DateTime now = DateTime.Now;
            DateTime closeTime = new DateTime(now.Year, now.Month, now.Day, 16, 0, 0);
            double daysToExpiry = (closeTime - now).TotalDays;

            // HACK: Floor Time to Expiry at ~30 minutes to prevent Gamma Explosion (Infinity)
            double T = Math.Max(0.00034, daysToExpiry); 

            // HACK: Use live VIX1D to scale the baseline IV dynamically
            double volScaler = currentVix1D / 15.0; 

            foreach (var kvp in optionsChain)
            {
                double strike = kvp.Key;
                var data = kvp.Value;

                // Only render strikes near the money to save CPU
                if (Math.Abs(strike - spotPrice) > 100) continue;

                double callIV = Math.Max(0.01, data.CallIV * volScaler);
                double putIV = Math.Max(0.01, data.PutIV * volScaler);

                // Calculate Gamma using floored T
                double callGamma = CalculateGamma(spotPrice, strike, T, callIV, riskFreeRate);
                double putGamma = CalculateGamma(spotPrice, strike, T, putIV, riskFreeRate);

                // Dealers short calls sold to clients (Negative GEX)
                double totalCallOI = data.BaseCallOI + data.SyntheticCallFlow; 
                double totalPutOI = data.BasePutOI + data.SyntheticPutFlow;

                // Standard GEX Formula: Gamma * OI * 100 * Spot Price
                double callGEX = callGamma * totalCallOI * 100 * spotPrice;
                double putGEX = putGamma * totalPutOI * 100 * spotPrice; // Put Gamma is natively positive here, so we subtract it for Net GEX

                double netGEX = callGEX - putGEX;

                // --- Drawing Logic (Using simplified NinjaTrader drawing) ---
                Brush gexColor = netGEX > 0 ? Brushes.Blue : Brushes.Orange;

				int barsWidth = Math.Max(1, (int)(Math.Abs(netGEX) / 1000000.0));

				Draw.Rectangle(this, "GEX_" + strike.ToString(), false, barsWidth, strike + 1, 0, strike - 1, Brushes.Transparent, gexColor, 50);

            }
        }

        // --- Black-Scholes Math Helper ---
        private double CalculateGamma(double S, double K, double T, double v, double r)
        {
            double d1 = (Math.Log(S / K) + (r + v * v / 2.0) * T) / (v * Math.Sqrt(T));
            double pdf_d1 = Math.Exp(-0.5 * d1 * d1) / Math.Sqrt(2 * Math.PI);
            return pdf_d1 / (S * v * Math.Sqrt(T)); // With T floored, this never divides by zero
        }

        public class OptionLevel
        {
            public double BaseCallOI { get; set; }
            public double BasePutOI { get; set; }
            public double CallIV { get; set; }
            public double PutIV { get; set; }
            public double SyntheticCallFlow { get; set; }
            public double SyntheticPutFlow { get; set; }
        }
    }
}
