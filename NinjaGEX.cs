using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Media;

// Standard NinjaTrader 8 Namespaces
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.DrawingTools;

namespace NinjaTrader.NinjaScript.Indicators
{
    // Make sure the class name matches your file name (e.g., NinjaGEX_Final)
    public class NinjaGEX_Final : Indicator
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
                Description = "Final Synthetic GEX with OI Gravity and UDP Data";
                Name = "NinjaGEX Final";
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
                    catch { /* Catches exceptions on thread abort/shutdown */ }
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
            // THE TICK RULE: Determine if the live futures trade was aggressive buying or selling
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

            // THROTTLING HACK: Only run heavy calculations every 5 seconds to save CPU
            if ((DateTime.Now - lastCalculationTime).TotalSeconds >= 5)
            {
                DistributeSyntheticFlow(Close[0]);
                RenderHeatmap(Close[0]);
                
                // Reset accumulators for the next interval
                accumulatedAggressiveBuyVol = 0;
                accumulatedAggressiveSellVol = 0;
                lastCalculationTime = DateTime.Now;
            }
        }

        private void DistributeSyntheticFlow(double currentSpot)
        {
            // THE OI GRAVITY HACK: Distribute volume based on existing baseline OI, not blindly.
            double totalNearbyOI = 0.0;
            double radius = 50.0; // Affect strikes within 50 points of spot

            // 1. Calculate the total OI in the immediate vicinity of the spot price
            foreach (var kvp in optionsChain)
            {
                if (Math.Abs(kvp.Key - currentSpot) <= radius)
                    totalNearbyOI += (kvp.Value.BaseCallOI + kvp.Value.BasePutOI);
            }

            // 2. Distribute the accumulated flow, weighted by each strike's share of that total OI
            if (totalNearbyOI > 0) // Prevent division by zero
            {
                foreach (var kvp in optionsChain)
                {
                    double strike = kvp.Key;
                    if (Math.Abs(strike - currentSpot) <= radius)
                    {
                        var level = kvp.Value;
                        double weight = (level.BaseCallOI + level.BasePutOI) / totalNearbyOI;

                        // Aggressive Buys -> Customers buy Calls, Dealers SHORT Calls
                        level.SyntheticCallFlow += (accumulatedAggressiveBuyVol * weight);
                        // Aggressive Sells -> Customers buy Puts, Dealers SHORT Puts
                        level.SyntheticPutFlow += (accumulatedAggressiveSellVol * weight);
                        
                        optionsChain[strike] = level;
                    }
                }
            }
        }

        private void RenderHeatmap(double spotPrice)
        {
            DateTime now = DateTime.Now;
            DateTime closeTime = new DateTime(now.Year, now.Month, now.Day, 16, 0, 0); // Assuming 4 PM EST market close
            double daysToExpiry = (closeTime - now).TotalDays;

            // GAMMA EXPLOSION FIX: Floor Time to Expiry at ~30 minutes
            double T = Math.Max(0.00034, daysToExpiry); 

            // DYNAMIC IV: Use live VIX1D to scale the baseline IV dynamically
            double volScaler = currentVix1D / 15.0; // Normalize against a baseline VIX of 15

            foreach (var kvp in optionsChain)
            {
                double strike = kvp.Key;
                var data = kvp.Value;

                // Only render strikes near the money to save CPU
                if (Math.Abs(strike - spotPrice) > 100) continue;

                double callIV = Math.Max(0.01, data.CallIV * volScaler);
                double putIV = Math.Max(0.01, data.PutIV * volScaler);

                // Calculate Gamma using the safe, floored Time (T)
                double callGamma = CalculateGamma(spotPrice, strike, T, callIV, riskFreeRate);
                double putGamma = CalculateGamma(spotPrice, strike, T, putIV, riskFreeRate);

                double totalCallOI = data.BaseCallOI + data.SyntheticCallFlow; 
                double totalPutOI = data.BasePutOI + data.SyntheticPutFlow;

                // Dealers are short calls sold to clients (Negative GEX) & short puts sold to clients (Positive GEX)
                // GEX = (Call Gamma * -Call OI) + (Put Gamma * -Put OI)
                // But since dealers are net short puts, this becomes positive exposure for the street.
                // Simplified view: Net GEX = (Put GEX) - (Call GEX)
                double callGEX = callGamma * totalCallOI * 100 * spotPrice;
                double putGEX = putGamma * totalPutOI * 100 * spotPrice;
                double netGEX = putGEX - callGEX; // Positive Net GEX = Pinning force, Negative Net GEX = Accelerant

                // --- CORRECTED DRAWING LOGIC ---
                Brush gexColor = netGEX > 0 ? Brushes.RoyalBlue : Brushes.OrangeRed;
                // Convert the width into a whole number of bars (int) for the X-axis
                int barsWidth = Math.Max(1, (int)(Math.Abs(netGEX) / 10000000.0)); // Adjust divisor to scale width

                Draw.Rectangle(this, "GEX_" + strike.ToString(), false, barsWidth, strike + 1, 0, strike - 1, Brushes.Transparent, gexColor, 70);
            }
        }

        // --- Black-Scholes Math Helper ---
        private double CalculateGamma(double S, double K, double T, double v, double r)
        {
            if (v <= 0 || T <= 0) return 0; // Guard against invalid inputs
            double d1 = (Math.Log(S / K) + (r + v * v / 2.0) * T) / (v * Math.Sqrt(T));
            double pdf_d1 = Math.Exp(-0.5 * d1 * d1) / Math.Sqrt(2 * Math.PI);
            return pdf_d1 / (S * v * Math.Sqrt(T));
        }

        // Helper class to store all data for a single strike price
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
