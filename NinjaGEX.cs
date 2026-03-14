#region Using declarations
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Windows.Input;
using System.Windows.Media;
using System.IO;
using System.Globalization;
using System.Threading.Tasks;
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.DrawingTools; // Required for Draw.TextFixed
using System.Windows.Threading;
using SharpDX.Direct2D1;
using SharpDX.DirectWrite;
using SharpDX;
#endregion

public enum GexDisplayMode { NetGex, CallGexOnly, PutGexOnly }

namespace NinjaTrader.NinjaScript.Indicators
{
    public class GexHeatmap : Indicator
    {
        #region Classes & Structs
        public class OptionData
        {
            public DateTime ExpirationUtc; 
            public bool IsCall;
            public double Strike; 
            public int OI;
            public double BaseIV;
        }

        public class GexSnapshot
        {
            public DateTime Timestamp; 
            public double BasisRatio;  
            public double SnapshotVIX; 
            public double SnapshotSpot; 
            public double DividendYield; 
            public Dictionary<double, List<OptionData>> Strikes = new Dictionary<double, List<OptionData>>();
        }

        // --- THE JEDI HACKS: Flow Data Container ---
        public class FlowData
        {
            public double CallVolume = 0;
            public double PutVolume = 0;
        }

        public class GexBarData
        {
            public DateTime SnapshotTime;
            public double BarStrikeInterval;
            
            public Dictionary<double, double> StrikeToNetGex = new Dictionary<double, double>();
            public Dictionary<double, double> StrikeToCallGex = new Dictionary<double, double>();
            public Dictionary<double, double> StrikeToPutGex = new Dictionary<double, double>();
            
            public Dictionary<double, int> StrikeToTotalOI = new Dictionary<double, int>();
            public Dictionary<double, int> StrikeToSyntheticOI = new Dictionary<double, int>();
            public Dictionary<double, double> StrikeToLiveIV = new Dictionary<double, double>();
            
            public Dictionary<double, double> StrikeToVanna = new Dictionary<double, double>();
            public Dictionary<double, double> StrikeToCharm = new Dictionary<double, double>();
        }

        private class GexHitInfo
        {
            public SharpDX.RectangleF Bounds;
            public double Strike; 
            public double NativeStrike; 
            public double Value;
            public int BaseOI;
            public int SyntheticOI;
            public double LiveIV;
            public double Vanna;
            public double Charm;
            public double ActiveSpot; 
        }
        #endregion

        #region Variables
        private List<GexSnapshot> historicalSnapshots;
        private readonly object dataLock = new object();
        private Series<GexBarData> gexSeries;
        private bool isDataLoaded = false;
        private double activeImpliedSpot = 0.0;
        private GexSnapshot latestSnapshot = null;

        // Thread-safe dictionary to hold separated Call and Put live order flow
        private ConcurrentDictionary<double, FlowData> impliedDealerFlow = new ConcurrentDictionary<double, FlowData>();
        private bool isTickReplayChecked = false;

        private SharpDX.Direct2D1.SolidColorBrush[] positiveBrushes; 
        private SharpDX.Direct2D1.SolidColorBrush[] negativeBrushes; 
        private List<GexHitInfo> hitTestList = new List<GexHitInfo>();
        private GexHitInfo hoveredCell = null;
        private bool showTooltip = false;
        private bool isMouseSubscribed = false;
        private SharpDX.Vector2 mousePosition;

        private SharpDX.DirectWrite.TextFormat tooltipTextFormat;
        private SharpDX.Direct2D1.SolidColorBrush tooltipBgBrush;
        private SharpDX.Direct2D1.SolidColorBrush tooltipBorderBrush;
        private SharpDX.Direct2D1.SolidColorBrush tooltipTextBrush;
        
        private DispatcherTimer hoverTimer;
        private DispatcherTimer fileCheckTimer;
        private DateTime lastFileReadTime = DateTime.MinValue;
        private TimeZoneInfo easternZone;
        #endregion

        #region Parameters
        [NinjaScriptProperty]
        [Display(Name="CSV Folder Path", Order=1, GroupName="1. Data")]
        public string CsvFolderPath { get; set; }

        [NinjaScriptProperty]
        [Display(Name="Live VIX Symbol", Order=2, GroupName="1. Data")]
        public string VixSymbol { get; set; }

        [NinjaScriptProperty]
        [Display(Name="Display Mode", Order=1, GroupName="2. Visuals")]
        public GexDisplayMode DisplayMode { get; set; }

        [NinjaScriptProperty]
        [Range(0, 100)]
        [Display(Name="Filter Cutoff %", Order=2, GroupName="2. Visuals")]
        public double CutoffPercent { get; set; }

        [NinjaScriptProperty]
        [Range(0.0, 5.0)]
        [Display(Name="Volatility Skew", Order=1, GroupName="3. Advanced Options Math")]
        public double VolSkewFactor { get; set; }

        [NinjaScriptProperty]
        [Range(1.0, 50.0)]
        [Display(Name="Futures -> Options Multiplier", Description="1 ES contract equals X Options for Synthetic OI.", Order=2, GroupName="4. Live 0DTE Dealer Flow")]
        public double FuturesToOptionsMultiplier { get; set; }

        [NinjaScriptProperty]
        [Range(1.0, 50.0)]
        [Display(Name="Strike Proximity Zone (Pts)", Description="How close price must be to a strike to register as dealer hedging.", Order=3, GroupName="4. Live 0DTE Dealer Flow")]
        public double StrikeProximityZone { get; set; }
        #endregion

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description                 = "Institutional Synthetic Microstructure Engine. (Live 0DTE GEX).";
                Name                        = "God-Tier 0DTE GEX";
                Calculate                   = Calculate.OnEachTick; // Required for OnMarketData order flow
                IsOverlay                   = true;
                DrawOnPricePanel            = true;
                
                CsvFolderPath               = @"C:\Options_History_Data";
                VixSymbol                   = "^VIX";
                DisplayMode                 = GexDisplayMode.NetGex;
                CutoffPercent               = 2.0; 
                VolSkewFactor               = 1.5;  
                
                FuturesToOptionsMultiplier  = 5.0; // Standard ratio (1 ES roughly hedges 5 SPX options)
                StrikeProximityZone         = 10.0; // Bell curve spread
            }
            else if (State == State.Configure)
            {
                ZOrder = -1; 
                impliedDealerFlow.Clear();
                try { easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
                catch { easternZone = TimeZoneInfo.Local; }
                if (!string.IsNullOrEmpty(VixSymbol)) AddDataSeries(VixSymbol, BarsPeriodType.Minute, 1);
            }
            else if (State == State.DataLoaded)
            {
                historicalSnapshots = new List<GexSnapshot>();
                gexSeries = new Series<GexBarData>(this, MaximumBarsLookBack.Infinite);
                positiveBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];
                negativeBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];

                LoadGexDataFromCsv();

                fileCheckTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(5) };
                fileCheckTimer.Tick += OnFileCheckTimerTick;
                fileCheckTimer.Start();
            }
            else if (State == State.Historical) { if (ChartControl != null && !isMouseSubscribed) { ChartControl.MouseMove += OnChartMouseMove; isMouseSubscribed = true; } }
            else if (State == State.Terminated) { if (ChartControl != null && isMouseSubscribed) ChartControl.MouseMove -= OnChartMouseMove; DisposeBrushes(); }
        }

        private string GetMappedTicker() { return "SPX"; } 

        #region Live Tick Data Integration (Hack #1: Delta Directionality)
        protected override void OnMarketData(MarketDataEventArgs e)
        {
            if (e.MarketDataType == MarketDataType.Last)
            {
                if (latestSnapshot == null || latestSnapshot.BasisRatio <= 0) return;

                double nativeSpot = e.Price / latestSnapshot.BasisRatio;
                double tickVolume = e.Volume;

                // --- HACK 1: Bypassing Call vs Put Duplication (Delta Split) ---
                bool isAskHit = e.Price >= e.Ask;
                bool isBidHit = e.Price <= e.Bid;
                if (!isAskHit && !isBidHit) return; // Ignore mid-market trades

                double roundedSpot = Math.Round(nativeSpot / 5.0) * 5.0;

                for (double offset = -15.0; offset <= 15.0; offset += 5.0)
                {
                    double targetStrike = roundedSpot + offset;
                    double distance = Math.Abs(nativeSpot - targetStrike);

                    if (distance <= StrikeProximityZone)
                    {
                        // Gaussian distribution allocation
                        double weight = Math.Exp(-Math.Pow(distance / (StrikeProximityZone / 2.0), 2) / 2.0);
                        double impliedContracts = tickVolume * weight * FuturesToOptionsMultiplier;

                        // Thread-safe update allocating flow to Call or Put based on Tick Delta
                        impliedDealerFlow.AddOrUpdate(targetStrike, 
                            new FlowData { 
                                CallVolume = isAskHit ? impliedContracts : 0, 
                                PutVolume = isBidHit ? impliedContracts : 0 
                            }, 
                            (key, oldVal) => new FlowData {
                                CallVolume = oldVal.CallVolume + (isAskHit ? impliedContracts : 0),
                                PutVolume = oldVal.PutVolume + (isBidHit ? impliedContracts : 0)
                            });
                    }
                }
            }
        }
        #endregion

        #region Background Data Loading
        private async void OnFileCheckTimerTick(object sender, EventArgs e)
        {
            await Task.Run(() =>
            {
                try
                {
                    if (!Directory.Exists(CsvFolderPath)) return;
                    string[] files = Directory.GetFiles(CsvFolderPath, string.Format("*{0}*.csv", GetMappedTicker()));
                    bool hasNewData = false;
                    DateTime maxTime = lastFileReadTime;

                    foreach (string f in files)
                    {
                        DateTime wt = File.GetLastWriteTime(f);
                        if (wt > lastFileReadTime) { hasNewData = true; if (wt > maxTime) maxTime = wt; }
                    }

                    if (hasNewData) 
                    { 
                        LoadGexDataFromCsv(); 
                        lastFileReadTime = maxTime; 
                        if (ChartControl != null) ChartControl.Dispatcher.InvokeAsync(() => ChartControl.InvalidateVisual());
                    }
                }
                catch { }
            });
        }

        private void LoadGexDataFromCsv()
        {
            try
            {
                string[] files = Directory.GetFiles(CsvFolderPath, string.Format("*{0}*.csv", GetMappedTicker()));
                if (files.Length == 0) return;

                Dictionary<DateTime, GexSnapshot> temp = new Dictionary<DateTime, GexSnapshot>();

                foreach (string file in files)
                {
                    using (FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (StreamReader sr = new StreamReader(fs))
                    {
                        sr.ReadLine(); 
                        string line;
                        while ((line = sr.ReadLine()) != null)
                        {
                            string[] c = line.Split(',');
                            if (c.Length < 12) continue;

                            try 
                            {
                                DateTime ts = DateTime.ParseExact(c[0], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                                bool isCall = c[4].Trim().Equals("Call", StringComparison.OrdinalIgnoreCase);
                                double strike = double.Parse(c[5], CultureInfo.InvariantCulture);
                                int oi = (int)double.Parse(c[6], CultureInfo.InvariantCulture);
                                double iv = double.Parse(c[7], CultureInfo.InvariantCulture);

                                if (!temp.ContainsKey(ts)) temp[ts] = new GexSnapshot { 
                                    Timestamp = ts, 
                                    BasisRatio = double.Parse(c[2], CultureInfo.InvariantCulture), 
                                    SnapshotVIX = double.Parse(c[3], CultureInfo.InvariantCulture), 
                                    SnapshotSpot = double.Parse(c[9], CultureInfo.InvariantCulture), 
                                    DividendYield = double.Parse(c[10], CultureInfo.InvariantCulture) 
                                };
                                
                                if (!temp[ts].Strikes.ContainsKey(strike)) temp[ts].Strikes[strike] = new List<OptionData>();
                                temp[ts].Strikes[strike].Add(new OptionData { IsCall = isCall, Strike = strike, OI = oi, BaseIV = iv, 
                                    ExpirationUtc = TimeZoneInfo.ConvertTimeToUtc(DateTime.ParseExact(c[1], "yyyy-MM-dd", CultureInfo.InvariantCulture).AddHours(16), easternZone) 
                                });
                            }
                            catch { } 
                        }
                    }
                }

                if (temp.Count > 0)
                {
                    lock (dataLock)
                    {
                        var sortedList = temp.Values.OrderBy(x => x.Timestamp).ToList();
                        sortedList.RemoveAll(x => (DateTime.UtcNow - x.Timestamp.ToUniversalTime()).TotalHours > 24);
                        historicalSnapshots = sortedList;
                        isDataLoaded = historicalSnapshots.Count > 0;
                        if (isDataLoaded) latestSnapshot = historicalSnapshots.Last();
                    }
                }
            }
            catch { }
        }
        #endregion

        #region Black-Scholes-Merton Math Engine
        private static double NormPDF(double x) { return Math.Exp(-x * x / 2.0) / Math.Sqrt(2.0 * Math.PI); }
        private static double NormCDF(double x) {
            int sign = x < 0 ? -1 : 1; x = Math.Abs(x) / Math.Sqrt(2.0);
            double t = 1.0 / (1.0 + 0.3275911 * x);
            double y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.Exp(-x * x);
            return 0.5 * (1.0 + sign * y);
        }
        private double CalculateDelta(double S, double K, double T, double v, bool isCall, double q, double r) {
            if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return isCall ? (S > K ? 1 : 0) : (S < K ? -1 : 0);
            double d1 = (Math.Log(S / K) + (r - q + v * v / 2.0) * T) / (v * Math.Sqrt(T)); return isCall ? Math.Exp(-q * T) * NormCDF(d1) : Math.Exp(-q * T) * (NormCDF(d1) - 1.0);
        }
        private double CalculateGamma(double S, double K, double T, double v, double q, double r) {
            if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return 0.0;
            double d1 = (Math.Log(S / K) + (r - q + v * v / 2.0) * T) / (v * Math.Sqrt(T)); return (Math.Exp(-q * T) * NormPDF(d1)) / (S * v * Math.Sqrt(T));
        }
        private double CalculateVanna(double S, double K, double T, double v, double q, double r) {
            if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return 0.0;
            double d1 = (Math.Log(S / K) + (r - q + v * v / 2.0) * T) / (v * Math.Sqrt(T)); double d2 = d1 - v * Math.Sqrt(T); return -Math.Exp(-q * T) * NormPDF(d1) * (d2 / v);
        }
        #endregion

        protected override void OnBarUpdate()
        {
            if (CurrentBars[0] < 0 || BarsInProgress != 0 || !isDataLoaded) return;
            
            if (!isTickReplayChecked && State == State.Realtime && !Bars.IsTickReplay)
            {
                Draw.TextFixed(this, "noTickWarn", "WARNING: Enable Tick Replay for Live 0DTE Flow tracking.", TextPosition.BottomRight);
                isTickReplayChecked = true;
            }

            GexSnapshot snap = null;
            lock(dataLock) { snap = historicalSnapshots.LastOrDefault(x => x.Timestamp <= Time[0]); }
            if (snap == null) return;

            latestSnapshot = snap; 
            double basisRatio = snap.BasisRatio;
            
            // Core Forward Spot
            double nativeSpotPrice = Close[0] / basisRatio; 
            activeImpliedSpot = nativeSpotPrice; 
            
            double q = snap.DividendYield;
            double r = 0.05; 
            double safeSnapshotSpot = snap.SnapshotSpot > 0 ? snap.SnapshotSpot : activeImpliedSpot;
            double priceMovePercent = (activeImpliedSpot / safeSnapshotSpot) - 1.0;

            double vixRatio = 1.0;
            if (CurrentBars.Length > 1 && CurrentBars[1] >= 0)
            {
                double liveVix = Closes[1][0];
                if (snap.SnapshotVIX > 0 && liveVix > 0) vixRatio = liveVix / snap.SnapshotVIX;
            }

            GexBarData bd = new GexBarData { SnapshotTime = snap.Timestamp, BarStrikeInterval = 5.0 * basisRatio };

            foreach (var kvp in snap.Strikes)
            {
                double nativeStrike = kvp.Key;
                double chartStrike = nativeStrike * basisRatio;
                
                double netGex = 0.0, callGex = 0.0, putGex = 0.0, avgLiveIv = 0.0, netVanna = 0.0, netCharm = 0.0;
                int baseOiTotal = 0, syntheticOiTotal = 0;

                // Grab Localized Dealer Flow
                FlowData localFlow;
                if (!impliedDealerFlow.TryGetValue(nativeStrike, out localFlow)) 
                    localFlow = new FlowData();

                foreach (var opt in kvp.Value)
                {
                    double T = Math.Max(0.00001, (opt.ExpirationUtc - Time[0].ToUniversalTime()).TotalDays / 365.0);
                    double daysToExpiry = T * 365.0;

                    // --- HACK 2: Expiration DTE Weighting (Funneling into 0DTE) ---
                    double timeWeight = Math.Exp(-daysToExpiry / 2.0); 
                    if (timeWeight < 0.05) timeWeight = 0.05; // Floor for LEAPs

                    double allocatedFlow = opt.IsCall ? (localFlow.CallVolume * timeWeight) : (localFlow.PutVolume * timeWeight);

                    // --- HACK 4: Opening vs Closing (Moneyness & Time Heuristic) ---
                    bool isITM = opt.IsCall ? (activeImpliedSpot > nativeStrike) : (activeImpliedSpot < nativeStrike);
                    bool isLateDay = Time[0].Hour >= 14; 

                    int syntheticOI;
                    if (isITM && isLateDay) 
                    {
                        // Probable profit taking/rolling (Closing flow)
                        syntheticOI = Math.Max(0, opt.OI - (int)allocatedFlow);
                    }
                    else 
                    {
                        // Probable speculation/hedging (Opening flow)
                        syntheticOI = opt.OI + (int)allocatedFlow;
                    }

                    // --- HACK 3: Synthetic Vega (Localized IV Skewing via Flow) ---
                    double flowRatio = opt.OI > 0 ? (allocatedFlow / opt.OI) : 0;
                    double ivSpikeMultiplier = 1.0;
                    
                    if (flowRatio > 0.10) 
                    {
                        ivSpikeMultiplier += (flowRatio * 0.5); 
                        ivSpikeMultiplier = Math.Min(ivSpikeMultiplier, 3.0); // Hard Cap to prevent math explosion
                    }

                    // Shift IV based on Macro (VIX) + Localized Demand (Flow Ratio)
                    double shiftedIV = opt.BaseIV * (1.0 - (priceMovePercent * VolSkewFactor)) * vixRatio;
                    double finalLiveIV = Math.Max(0.01, shiftedIV * ivSpikeMultiplier); 
                    
                    // Core BSM Engine Output
                    double gamma = CalculateGamma(activeImpliedSpot, nativeStrike, T, finalLiveIV, q, r);
                    double vanna = CalculateVanna(activeImpliedSpot, nativeStrike, T, finalLiveIV, q, r);
                    
                    double deltaNow = CalculateDelta(activeImpliedSpot, nativeStrike, T, finalLiveIV, opt.IsCall, q, r);
                    double deltaTomorrow = CalculateDelta(activeImpliedSpot, nativeStrike, Math.Max(0.00001, T - (1.0 / 365.0)), finalLiveIV, opt.IsCall, q, r);
                    double charm = deltaTomorrow - deltaNow; 
                    
                    double gex = gamma * syntheticOI * 100.0 * activeImpliedSpot; 
                    double vannaEx = (vanna * 0.01) * syntheticOI * 100.0 * activeImpliedSpot; 
                    double charmEx = charm * syntheticOI * 100.0 * activeImpliedSpot; 
                    
                    if (opt.IsCall) { netGex += gex; callGex += gex; netVanna += vannaEx; netCharm += charmEx; }
                    else { netGex -= gex; putGex -= gex; netVanna -= vannaEx; netCharm -= charmEx; }

                    baseOiTotal += opt.OI;
                    syntheticOiTotal += syntheticOI;
                    avgLiveIv = finalLiveIV; 
                }

                bd.StrikeToNetGex[chartStrike] = netGex; bd.StrikeToCallGex[chartStrike] = callGex; bd.StrikeToPutGex[chartStrike] = putGex;
                bd.StrikeToTotalOI[chartStrike] = baseOiTotal; bd.StrikeToSyntheticOI[chartStrike] = syntheticOiTotal;
                bd.StrikeToLiveIV[chartStrike] = avgLiveIv; bd.StrikeToVanna[chartStrike] = netVanna; bd.StrikeToCharm[chartStrike] = netCharm;
            }
            gexSeries[0] = bd;
        }

        #region Visualization & UI Render
        public override void OnRenderTargetChanged()
        {
            DisposeBrushes();
            if (RenderTarget != null)
            {
                for (int i = 0; i < 256; i++)
                {
                    System.Windows.Media.Color pc = System.Windows.Media.Color.FromRgb((byte)(10+(0-10)*(i/255.0)), (byte)(20+(200-20)*(i/255.0)), (byte)(40+(255-40)*(i/255.0)));
                    positiveBrushes[i] = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color(pc.R, pc.G, pc.B)) { Opacity = 0.6f };
                    System.Windows.Media.Color nc = System.Windows.Media.Color.FromRgb((byte)(40+(255-40)*(i/255.0)), (byte)(10+(100-10)*(i/255.0)), (byte)(10+(0-10)*(i/255.0)));
                    negativeBrushes[i] = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color(nc.R, nc.G, nc.B)) { Opacity = 0.6f };
                }
                tooltipBgBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color4(0.05f, 0.05f, 0.08f, 0.95f));
                tooltipBorderBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, SharpDX.Color.DodgerBlue);
                tooltipTextBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, SharpDX.Color.White);
                tooltipTextFormat = new SharpDX.DirectWrite.TextFormat(NinjaTrader.Core.Globals.DirectWriteFactory, "Consolas", 13) { ParagraphAlignment = SharpDX.DirectWrite.ParagraphAlignment.Near };
            }
        }

        private void DisposeBrushes()
        {
            if (positiveBrushes != null) for (int i = 0; i < 256; i++) { if (positiveBrushes[i] != null) positiveBrushes[i].Dispose(); if (negativeBrushes[i] != null) negativeBrushes[i].Dispose(); }
            if (tooltipBgBrush != null) tooltipBgBrush.Dispose();
            if (tooltipBorderBrush != null) tooltipBorderBrush.Dispose();
            if (tooltipTextBrush != null) tooltipTextBrush.Dispose();
            if (tooltipTextFormat != null) tooltipTextFormat.Dispose();
        }

        protected override void OnRender(ChartControl cc, ChartScale cs)
        {
            if (Bars == null || !isDataLoaded || IsInHitTest || positiveBrushes == null || positiveBrushes[0] == null) return;
            RenderTarget.AntialiasMode = SharpDX.Direct2D1.AntialiasMode.Aliased;

            double minP = cs.MinValue, maxP = cs.MaxValue, maxGex = 0.0001; 
            int lastIdx = -1;
            for (int i = ChartBars.ToIndex; i >= ChartBars.FromIndex; i--) if (gexSeries.IsValidDataPointAt(i)) { lastIdx = i; break; }

            for (int i = ChartBars.FromIndex; i <= ChartBars.ToIndex; i++)
            {
                if (!gexSeries.IsValidDataPointAt(i)) continue;
                var d = gexSeries.GetValueAt(i);
                var dict = DisplayMode == GexDisplayMode.CallGexOnly ? d.StrikeToCallGex : (DisplayMode == GexDisplayMode.PutGexOnly ? d.StrikeToPutGex : d.StrikeToNetGex);
                foreach(var kvp in dict) if (kvp.Key >= minP && kvp.Key <= maxP) maxGex = Math.Max(maxGex, Math.Abs(kvp.Value));
            }

            List<GexHitInfo> hits = new List<GexHitInfo>();

            for (int i = ChartBars.FromIndex; i <= ChartBars.ToIndex; i++)
            {
                if (!gexSeries.IsValidDataPointAt(i)) continue;
                var d = gexSeries.GetValueAt(i);
                float x1 = cc.GetXByBarIndex(ChartBars, i), x2 = cc.GetXByBarIndex(ChartBars, i + 1);
                if (i == lastIdx) x2 = cc.CanvasRight;
                float w = Math.Max(1f, x2 - x1);

                var dict = DisplayMode == GexDisplayMode.CallGexOnly ? d.StrikeToCallGex : (DisplayMode == GexDisplayMode.PutGexOnly ? d.StrikeToPutGex : d.StrikeToNetGex);

                foreach (var kvp in dict)
                {
                    if (kvp.Key > maxP + d.BarStrikeInterval || kvp.Key < minP - d.BarStrikeInterval) continue;
                    double ratio = Math.Abs(kvp.Value) / maxGex;
                    if (ratio < (CutoffPercent / 100.0)) continue; 

                    float yt = cs.GetYByValue(kvp.Key + (d.BarStrikeInterval / 2.0)), yb = cs.GetYByValue(kvp.Key - (d.BarStrikeInterval / 2.0));
                    SharpDX.RectangleF rect = new SharpDX.RectangleF(x1, Math.Min(yt, yb), w, Math.Abs(yb - yt));
                    
                    RenderTarget.FillRectangle(rect, kvp.Value >= 0 ? positiveBrushes[Math.Max(0, Math.Min(255, (int)(ratio * 255)))] : negativeBrushes[Math.Max(0, Math.Min(255, (int)(ratio * 255)))]);
                    
                    hits.Add(new GexHitInfo { Bounds = rect, Strike = kvp.Key, NativeStrike = kvp.Key / latestSnapshot.BasisRatio, Value = kvp.Value, BaseOI = d.StrikeToTotalOI[kvp.Key], SyntheticOI = d.StrikeToSyntheticOI[kvp.Key], LiveIV = d.StrikeToLiveIV[kvp.Key], Vanna = d.StrikeToVanna[kvp.Key], Charm = d.StrikeToCharm[kvp.Key], ActiveSpot = activeImpliedSpot });
                }
            }

            hitTestList = hits;
            if (showTooltip && hoveredCell != null) DrawTooltip(hoveredCell);
        }

        private void DrawTooltip(GexHitInfo info)
        {
            if (tooltipTextFormat == null) return;
            string txt = string.Format(
                "SYNTHETIC MICROSTRUCTURE ENGINE\n" +
                "───────────────────────────────\n" +
                "Strike Space:        {0:N0}\n" +
                "Base OCC OI:         {1:N0}\n" +
                "Synthetic Live OI:   {2:N0}  <-- Inferred\n" +
                "Flow-Skewed IV:      {3:P1}\n\n" +
                "Net $GEX Exposure:   ${4:N0}\n" +
                "Vanna (per 1% Vol):  ${5:N0}\n" +
                "Charm (1 Day Decay): ${6:N0}", 
                info.NativeStrike, info.BaseOI, info.SyntheticOI, info.LiveIV, info.Value, info.Vanna, info.Charm);

            using (var lay = new SharpDX.DirectWrite.TextLayout(NinjaTrader.Core.Globals.DirectWriteFactory, txt, tooltipTextFormat, 350, 270))
            {
                float w = lay.Metrics.Width + 16, h = lay.Metrics.Height + 16;
                float x = mousePosition.X + 15, y = mousePosition.Y - h - 15;
                if (x + w > ChartControl.ActualWidth) x = mousePosition.X - w - 15;
                if (y < 0) y = mousePosition.Y + 15;

                SharpDX.RectangleF r = new SharpDX.RectangleF(x, y, w, h);
                var rr = new SharpDX.Direct2D1.RoundedRectangle { Rect = r, RadiusX = 5, RadiusY = 5 };
                RenderTarget.FillRoundedRectangle(rr, tooltipBgBrush);
                RenderTarget.DrawRoundedRectangle(rr, tooltipBorderBrush, 1.5f);
                RenderTarget.DrawTextLayout(new SharpDX.Vector2(x + 8, y + 8), lay, tooltipTextBrush);
            }
        }

        private void OnChartMouseMove(object sender, MouseEventArgs e)
        {
            if (hoverTimer == null) { hoverTimer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(150) }; hoverTimer.Tick += (s, args) => { hoverTimer.Stop(); showTooltip = true; if (ChartControl != null) ChartControl.InvalidateVisual(); }; }
            if (ChartControl == null) return;
            System.Windows.Point p = e.GetPosition(ChartControl as System.Windows.IInputElement);
            mousePosition = new SharpDX.Vector2((float)p.X, (float)p.Y);

            GexHitInfo hit = hitTestList.LastOrDefault(h => h.Bounds.Contains(mousePosition));
            if (hit != null) { if (hoveredCell != hit) { hoveredCell = hit; showTooltip = false; hoverTimer.Stop(); hoverTimer.Start(); } }
            else if (hoveredCell != null) { hoveredCell = null; showTooltip = false; hoverTimer.Stop(); ChartControl.InvalidateVisual(); }
        }
        #endregion
    }
}
