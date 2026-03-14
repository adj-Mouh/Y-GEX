#region Using declarations
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Windows.Media;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.DrawingTools;
using SharpDX.Direct2D1;
#endregion

public enum GexDisplayMode { NetGex, CallGexOnly, PutGexOnly }

namespace NinjaTrader.NinjaScript.Indicators
{
    public class GexHeatmapUDP : Indicator
    {
        #region Classes & Variables
        public class OptionData
        {
            public double DTE; 
            public bool IsCall;
            public double Strike; 
            public int OI;
            public double BaseIV;
            public int FlowDir; // 1 = Opening, -1 = Closing
        }

        public class GexSnapshot
        {
            public double Timestamp; 
            public double BasisRatio;  
            public double SnapshotVIX; 
            public double SnapshotSpot; 
            public double CostOfCarry; 
            public List<OptionData> Options = new List<OptionData>();
        }

        private UdpClient udpClient;
        private Task udpTask;
        private bool isListening = false;
        private readonly object dataLock = new object();
        
        private GexSnapshot latestSnapshot = null;
        private Dictionary<double, double> StrikeNetGex = new Dictionary<double, double>();
        
        // Pin Defense Memory
        private HashSet<double> MajorGammaWalls = new HashSet<double>();
        private Dictionary<double, int> IntradaySyntheticOI = new Dictionary<double, int>();

        // Throttling State
        private double lastCalcPrice = 0;
        private DateTime lastCalcTime = DateTime.MinValue;

        private SharpDX.Direct2D1.SolidColorBrush[] positiveBrushes; 
        private SharpDX.Direct2D1.SolidColorBrush[] negativeBrushes; 
        #endregion

        #region Parameters
        [NinjaScriptProperty]
        [Display(Name="Display Mode", Order=1, GroupName="1. Visuals")]
        public GexDisplayMode DisplayMode { get; set; }

        [NinjaScriptProperty]
        [Range(0, 100)]
        [Display(Name="Filter Cutoff %", Order=2, GroupName="1. Visuals")]
        public double CutoffPercent { get; set; }

        [NinjaScriptProperty]
        [Display(Name="UDP Port", Order=1, GroupName="2. System")]
        public int UdpPort { get; set; } // <--- FIX: Removed the "= 9000;" from here
        #endregion

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description                 = "Institutional UDP GEX Engine with Microstructure Pin Defense.";
                Name                        = "God-Tier UDP GEX";
                Calculate                   = Calculate.OnEachTick; 
                IsOverlay                   = true;
                DrawOnPricePanel            = true;
                
                // --- FIX: Initialize defaults here instead ---
                CutoffPercent               = 2.0; 
                UdpPort                     = 9000; 
                DisplayMode                 = GexDisplayMode.NetGex;
            }
            else if (State == State.Configure)
            {
                ZOrder = -1; 
                IntradaySyntheticOI.Clear();
            }
            else if (State == State.DataLoaded)
            {
                positiveBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];
                negativeBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];

                // Start Zero-Latency UDP Listener
                isListening = true;
                udpClient = new UdpClient(UdpPort);
                udpTask = Task.Run(() => ReceiveUdpData());
            }
            else if (State == State.Terminated)
            {
                isListening = false;
                if (udpClient != null) { udpClient.Close(); udpClient.Dispose(); }
                DisposeBrushes();
            }
        }

        #region UDP Telemetry System
        private void ReceiveUdpData()
        {
            IPEndPoint remoteEP = new IPEndPoint(IPAddress.Any, UdpPort);
            StringBuilder payloadBuilder = new StringBuilder();

            while (isListening)
            {
                try
                {
                    byte[] data = udpClient.Receive(ref remoteEP);
                    string chunk = Encoding.UTF8.GetString(data);
                    payloadBuilder.Append(chunk);

                    // If packet doesn't end cleanly or is small, process it
                    if (chunk.Length < 50000) 
                    {
                        ProcessPayload(payloadBuilder.ToString());
                        payloadBuilder.Clear();
                    }
                }
                catch { /* Handle thread abort on termination */ }
            }
        }

        private void ProcessPayload(string payload)
        {
            try
            {
                string[] parts = payload.Split('|');
                if (parts.Length < 2) return;

                string[] header = parts[0].Split(',');
                GexSnapshot newSnap = new GexSnapshot
                {
                    Timestamp = double.Parse(header[0]),
                    BasisRatio = double.Parse(header[1]),
                    SnapshotVIX = double.Parse(header[2]),
                    SnapshotSpot = double.Parse(header[3]),
                    CostOfCarry = double.Parse(header[4])
                };

                for (int i = 1; i < parts.Length; i++)
                {
                    string[] row = parts[i].Split(',');
                    if (row.Length < 6) continue;

                    newSnap.Options.Add(new OptionData
                    {
                        DTE = double.Parse(row[0]),
                        IsCall = row[1] == "1",
                        Strike = double.Parse(row[2]),
                        OI = (int)double.Parse(row[3]),
                        BaseIV = double.Parse(row[4]),
                        FlowDir = int.Parse(row[5])
                    });
                }

                lock (dataLock) { latestSnapshot = newSnap; }
                
                // Force UI update
                if (ChartControl != null)
                    ChartControl.Dispatcher.InvokeAsync(() => ChartControl.InvalidateVisual());
            }
            catch { }
        }
        #endregion

        #region Order Flow Imbalance (Pin Defense Hack)
        protected override void OnMarketData(MarketDataEventArgs e)
        {
            if (e.MarketDataType == MarketDataType.Last && latestSnapshot != null)
            {
                double esPrice = e.Price;
                double volume = e.Volume;

                // Identify if transaction was on Bid or Ask
                bool isAskHit = e.Price >= e.Ask;
                bool isBidHit = e.Price <= e.Bid;
                if (!isAskHit && !isBidHit) return;

                lock (dataLock)
                {
                    // Hack: Microstructure Pin Defense. 
                    // Dealers only hedge heavily when price collides with MAJOR Gamma Walls.
                    foreach (double nativeStrike in MajorGammaWalls)
                    {
                        double chartStrike = nativeStrike * latestSnapshot.BasisRatio;
                        
                        // If ES is within 1 point of a massive Gamma wall, attribute tape to Dealer Flow
                        if (Math.Abs(esPrice - chartStrike) <= 1.0)
                        {
                            int impliedContracts = (int)(volume * 5.0); // 1 ES ~ 5 SPX proxy
                            
                            if (!IntradaySyntheticOI.ContainsKey(nativeStrike))
                                IntradaySyntheticOI[nativeStrike] = 0;

                            // Buying at Ask increases OI, Selling at Bid decreases
                            if (isAskHit) IntradaySyntheticOI[nativeStrike] += impliedContracts;
                            if (isBidHit) IntradaySyntheticOI[nativeStrike] -= impliedContracts;
                        }
                    }
                }
            }
        }
        #endregion

        #region Standard BSM Engine
        private static double NormPDF(double x) { return Math.Exp(-x * x / 2.0) / Math.Sqrt(2.0 * Math.PI); }
        private static double NormCDF(double x) {
            int sign = x < 0 ? -1 : 1; x = Math.Abs(x) / Math.Sqrt(2.0);
            double t = 1.0 / (1.0 + 0.3275911 * x);
            double y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.Exp(-x * x);
            return 0.5 * (1.0 + sign * y);
        }
        private double CalculateGamma(double S, double K, double T, double v, double q_and_r) {
            if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return 0.0;
            // Using Put-Call parity inferred Cost of Carry (q_and_r)
            double d1 = (Math.Log(S / K) + (q_and_r + v * v / 2.0) * T) / (v * Math.Sqrt(T)); 
            return (Math.Exp(-q_and_r * T) * NormPDF(d1)) / (S * v * Math.Sqrt(T));
        }
        #endregion

        protected override void OnBarUpdate()
        {
            if (CurrentBars[0] < 0 || latestSnapshot == null) return;

            // --- Hack: Price-Delta Throttling ---
            // Don't nuke the CPU if price barely moved or if we just updated 500ms ago
            double liveEsPrice = Close[0];
            if (Math.Abs(liveEsPrice - lastCalcPrice) < 0.5 && (DateTime.Now - lastCalcTime).TotalMilliseconds < 500)
                return;

            lastCalcPrice = liveEsPrice;
            lastCalcTime = DateTime.Now;

            GexSnapshot snap;
            lock (dataLock) { snap = latestSnapshot; }

            double basis = snap.BasisRatio;
            double nativeSpot = liveEsPrice / basis;
            double costOfCarry = snap.CostOfCarry;
            
            Dictionary<double, double> tempNetGex = new Dictionary<double, double>();
            
            // Re-calculate major gamma walls for Pin Defense
            List<KeyValuePair<double, double>> ranker = new List<KeyValuePair<double, double>>();

            foreach (var opt in snap.Options)
            {
                double chartStrike = opt.Strike * basis;
                
                // --- Hack: PVOI Flow Integration ---
                // Python determined if flow was opening/closing based on Premium + Volume.
                // Combine it with C# Tick order flow imbalance memory.
                int c_sharp_oi_adjust = IntradaySyntheticOI.ContainsKey(opt.Strike) ? IntradaySyntheticOI[opt.Strike] : 0;
                int python_pvoi_adjust = opt.FlowDir * (opt.OI / 100); // Exaggerate PVOI bias slightly
                int syntheticOI = Math.Max(0, opt.OI + c_sharp_oi_adjust + python_pvoi_adjust);

                // --- Hack: Sticky-Strike Volatility Surface ---
                // As price drops, ATM IV increases. Shift IV synthetically.
                double moneyness = nativeSpot / opt.Strike;
                double stickyIV = opt.BaseIV * (1.0 + ((snap.SnapshotSpot - nativeSpot) / snap.SnapshotSpot) * 1.5);
                stickyIV = Math.Max(0.01, stickyIV); // Floor

                double gamma = CalculateGamma(nativeSpot, opt.Strike, opt.DTE, stickyIV, costOfCarry);
                double gex = gamma * syntheticOI * 100.0 * nativeSpot;

                if (!tempNetGex.ContainsKey(chartStrike)) tempNetGex[chartStrike] = 0;
                
                if (opt.IsCall) tempNetGex[chartStrike] += gex;
                else tempNetGex[chartStrike] -= gex;
            }

            // Identify top 3 Gamma Walls for the Pin Defense loop
            lock (dataLock)
            {
                StrikeNetGex = tempNetGex;
                var sorted = tempNetGex.OrderByDescending(x => Math.Abs(x.Value)).Take(3);
                MajorGammaWalls.Clear();
                foreach (var s in sorted) MajorGammaWalls.Add(s.Key / basis);
            }
        }

        #region Hardware-Accelerated Rendering
        public override void OnRenderTargetChanged()
        {
            DisposeBrushes();
            if (RenderTarget != null)
            {
                for (int i = 0; i < 256; i++)
                {
                    System.Windows.Media.Color pc = System.Windows.Media.Color.FromRgb((byte)(10+(0-10)*(i/255.0)), (byte)(20+(200-20)*(i/255.0)), (byte)(40+(255-40)*(i/255.0)));
                    positiveBrushes[i] = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color(pc.R, pc.G, pc.B)) { Opacity = 0.65f };
                    
                    System.Windows.Media.Color nc = System.Windows.Media.Color.FromRgb((byte)(40+(255-40)*(i/255.0)), (byte)(10+(100-10)*(i/255.0)), (byte)(10+(0-10)*(i/255.0)));
                    negativeBrushes[i] = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color(nc.R, nc.G, nc.B)) { Opacity = 0.65f };
                }
            }
        }

        private void DisposeBrushes()
        {
            if (positiveBrushes != null) for (int i = 0; i < 256; i++) { if (positiveBrushes[i] != null) positiveBrushes[i].Dispose(); if (negativeBrushes[i] != null) negativeBrushes[i].Dispose(); }
        }

        protected override void OnRender(ChartControl cc, ChartScale cs)
        {
            if (Bars == null || positiveBrushes == null || positiveBrushes[0] == null) return;
            
            Dictionary<double, double> renderGex;
            lock(dataLock) { renderGex = new Dictionary<double, double>(StrikeNetGex); }
            if (renderGex.Count == 0) return;

            RenderTarget.AntialiasMode = SharpDX.Direct2D1.AntialiasMode.Aliased;

            double minP = cs.MinValue, maxP = cs.MaxValue;
            double maxGex = renderGex.Values.Select(Math.Abs).DefaultIfEmpty(0.0001).Max();

            float x1 = cc.CanvasLeft;
            float x2 = cc.CanvasRight;
            float w = x2 - x1;

            foreach (var kvp in renderGex)
            {
                if (kvp.Key > maxP + 10 || kvp.Key < minP - 10) continue;
                
                double ratio = Math.Abs(kvp.Value) / maxGex;
                if (ratio < (CutoffPercent / 100.0)) continue; 

                // Strike block height based roughly on typical SPX interval mapped to ES
                float yt = cs.GetYByValue(kvp.Key + 2.5);
                float yb = cs.GetYByValue(kvp.Key - 2.5);
                
                SharpDX.RectangleF rect = new SharpDX.RectangleF(x1, Math.Min(yt, yb), w, Math.Abs(yb - yt));
                int colorIndex = Math.Max(0, Math.Min(255, (int)(ratio * 255)));
                
                RenderTarget.FillRectangle(rect, kvp.Value >= 0 ? positiveBrushes[colorIndex] : negativeBrushes[colorIndex]);
            }
        }
        #endregion
    }
}
