#region Using declarations
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Windows.Input;
using System.Windows.Media;
using System.IO;
using System.Globalization;
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
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
            public int Volume; // HACK #2: Added Volume tracking
        }

        public class GexSnapshot
        {
            public DateTime Timestamp; 
            public double BasisRatio;  
            public double SnapshotVIX; 
            public double SnapshotSpot; // HACK #1: Anchor point for Sticky-Delta
            public Dictionary<double, List<OptionData>> Strikes = new Dictionary<double, List<OptionData>>();
        }

        public class GexBarData
        {
            public DateTime SnapshotTime;
            public double BarStrikeInterval;
            
            public Dictionary<double, double> StrikeToNetGex = new Dictionary<double, double>();
            public Dictionary<double, double> StrikeToCallGex = new Dictionary<double, double>();
            public Dictionary<double, double> StrikeToPutGex = new Dictionary<double, double>();
            public Dictionary<double, int> StrikeToTotalOI = new Dictionary<double, int>();
            
            // Real-Time Tooltip Metrics
            public Dictionary<double, double> StrikeToCallPrice = new Dictionary<double, double>();
            public Dictionary<double, double> StrikeToPutPrice = new Dictionary<double, double>();
            public Dictionary<double, double> StrikeToLiveIV = new Dictionary<double, double>();
            public Dictionary<double, int> StrikeToSyntheticOI = new Dictionary<double, int>(); // New metric
        }

        private class GexHitInfo
        {
            public SharpDX.RectangleF Bounds;
            public double Strike; 
            public double NativeStrike; 
            public double Value;
            public int BaseOI;
            public int SyntheticOI;
            public double CallPrice;
            public double PutPrice;
            public double LiveIV;
        }
        #endregion

        #region Variables
        private List<GexSnapshot> historicalSnapshots;
        private Series<GexBarData> gexSeries;
        private bool isDataLoaded = false;

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
        [Display(Name="Live VIX Symbol", Description="e.g. ^VIX", Order=2, GroupName="1. Data")]
        public string VixSymbol { get; set; }

        [NinjaScriptProperty]
        [Display(Name="Display Mode", Order=1, GroupName="2. Visuals")]
        public GexDisplayMode DisplayMode { get; set; }

        [NinjaScriptProperty]
        [Range(0, 100)]
        [Display(Name="Filter Cutoff %", Order=2, GroupName="2. Visuals")]
        public double CutoffPercent { get; set; }

        [NinjaScriptProperty]
        [Range(1, 120)]
        [Display(Name="Max Data Age (Min)", Order=3, GroupName="2. Visuals")]
        public int MaxDataAgeMinutes { get; set; }
        
        [NinjaScriptProperty]
        [Range(0.0, 1.0)]
        [Display(Name="Intraday Volume Weight (0.4 = 40%)", Order=4, GroupName="3. Advanced Engine")]
        public double IntradayVolumeWeight { get; set; }
        
        [NinjaScriptProperty]
        [Range(0.0, 5.0)]
        [Display(Name="Volatility Skew Factor", Order=5, GroupName="3. Advanced Engine")]
        public double VolSkewFactor { get; set; }
        #endregion

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description                 = "Predictive Real-Time Options Engine (Sticky-Delta & Synthetic OI).";
                Name                        = "Pro GEX Heatmap";
                Calculate                   = Calculate.OnPriceChange; 
                IsOverlay                   = true;
                DrawOnPricePanel            = true;
                
                CsvFolderPath               = @"C:\Options_History_Data";
                VixSymbol                   = "^VIX";
                DisplayMode                 = GexDisplayMode.NetGex;
                CutoffPercent               = 2.0; 
                MaxDataAgeMinutes           = 15;
                IntradayVolumeWeight        = 0.40; // 40% of intraday volume is assumed to be new OI
                VolSkewFactor               = 1.5;  // Negative skew curve multiplier
            }
            else if (State == State.Configure)
            {
                ZOrder = -1; 
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

        private string GetMappedTicker()
        {
            if (Instrument == null || Instrument.MasterInstrument == null) return "SPX";
            string name = Instrument.MasterInstrument.Name.ToUpper();
            if (name.Contains("ES")) return "SPX";
            if (name.Contains("NQ")) return "NDX";
            return name;
        }

        private void OnFileCheckTimerTick(object sender, EventArgs e)
        {
            try
            {
                if (!Directory.Exists(CsvFolderPath)) return;
                string targetName = GetMappedTicker();
                
                // FIXED: Changed $ string interpolation to string.Format for C# 5 compatibility
                string[] files = Directory.GetFiles(CsvFolderPath, string.Format("*{0}*.csv", targetName));
                
                bool hasNewData = false;
                DateTime maxTime = lastFileReadTime;

                foreach (string f in files)
                {
                    DateTime wt = File.GetLastWriteTime(f);
                    if (wt > lastFileReadTime) { hasNewData = true; if (wt > maxTime) maxTime = wt; }
                }

                if (hasNewData) { LoadGexDataFromCsv(); lastFileReadTime = maxTime; if (ChartControl != null) ChartControl.InvalidateVisual(); }
            }
            catch { }
        }

        private void LoadGexDataFromCsv()
        {
            try
            {
                string targetName = GetMappedTicker();
                
                // FIXED: Changed $ string interpolation to string.Format for C# 5 compatibility
                string[] files = Directory.GetFiles(CsvFolderPath, string.Format("*{0}*.csv", targetName));
                
                if (files.Length == 0) return;

                Dictionary<DateTime, GexSnapshot> temp = new Dictionary<DateTime, GexSnapshot>();
                DateTime maxTime = lastFileReadTime;

                foreach (string file in files)
                {
                    DateTime wt = File.GetLastWriteTime(file);
                    if (wt > maxTime) maxTime = wt;

                    using (FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (StreamReader sr = new StreamReader(fs))
                    {
                        sr.ReadLine(); // Skip header
                        string line;
                        while ((line = sr.ReadLine()) != null)
                        {
                            string[] c = line.Split(',');
                            if (c.Length < 10) continue; 

                            try 
                            {
                                DateTime ts = DateTime.ParseExact(c[0], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                                DateTime exp = TimeZoneInfo.ConvertTimeToUtc(DateTime.ParseExact(c[1], "yyyy-MM-dd", CultureInfo.InvariantCulture).AddHours(16), easternZone);
                                double basis = double.Parse(c[2], CultureInfo.InvariantCulture);
                                double snapVix = double.Parse(c[3], CultureInfo.InvariantCulture);
                                bool isCall = c[4].Trim().Equals("Call", StringComparison.OrdinalIgnoreCase);
                                double strike = double.Parse(c[5], CultureInfo.InvariantCulture);
                                int oi = (int)double.Parse(c[6], CultureInfo.InvariantCulture);
                                double iv = double.Parse(c[7], CultureInfo.InvariantCulture);
                                int vol = (int)double.Parse(c[8], CultureInfo.InvariantCulture);
                                double snapSpot = double.Parse(c[9], CultureInfo.InvariantCulture);

                                if (!temp.ContainsKey(ts)) temp[ts] = new GexSnapshot { Timestamp = ts, BasisRatio = basis, SnapshotVIX = snapVix, SnapshotSpot = snapSpot };
                                if (!temp[ts].Strikes.ContainsKey(strike)) temp[ts].Strikes[strike] = new List<OptionData>();

                                temp[ts].Strikes[strike].Add(new OptionData { ExpirationUtc = exp, IsCall = isCall, Strike = strike, OI = oi, BaseIV = iv, Volume = vol });
                            }
                            catch { } 
                        }
                    }
                }

                if (temp.Count > 0)
                {
                    historicalSnapshots = temp.Values.OrderBy(x => x.Timestamp).ToList();
                    isDataLoaded = historicalSnapshots.Count > 0;
                    lastFileReadTime = maxTime;
                }
            }
            catch { }
        }

        private GexSnapshot GetClosestSnapshot(DateTime t)
        {
            if (historicalSnapshots == null || historicalSnapshots.Count == 0) return null;
            int l = 0, r = historicalSnapshots.Count - 1;
            GexSnapshot closest = null;
            while (l <= r) { int m = l + (r - l) / 2; if (historicalSnapshots[m].Timestamp <= t) { closest = historicalSnapshots[m]; l = m + 1; } else r = m - 1; }
            return closest;
        }

        #region Black-Scholes Math Engine
        private static double NormPDF(double x) { return Math.Exp(-x * x / 2.0) / Math.Sqrt(2.0 * Math.PI); }
        private static double NormCDF(double x)
        {
            int sign = x < 0 ? -1 : 1;
            x = Math.Abs(x) / Math.Sqrt(2.0);
            double t = 1.0 / (1.0 + 0.3275911 * x);
            double y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.Exp(-x * x);
            return 0.5 * (1.0 + sign * y);
        }

        private double CalculateGamma(double S, double K, double T, double v, double r = 0.05)
        {
            if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return 0.0;
            double d1 = (Math.Log(S / K) + (r + v * v / 2.0) * T) / (v * Math.Sqrt(T));
            return NormPDF(d1) / (S * v * Math.Sqrt(T));
        }

        private double CalculateOptionPrice(double S, double K, double T, double v, bool isCall, double r = 0.05)
        {
            if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return Math.Max(0, isCall ? S - K : K - S); 
            double d1 = (Math.Log(S / K) + (r + v * v / 2.0) * T) / (v * Math.Sqrt(T));
            double d2 = d1 - v * Math.Sqrt(T);
            if (isCall) return S * NormCDF(d1) - K * Math.Exp(-r * T) * NormCDF(d2);
            else return K * Math.Exp(-r * T) * NormCDF(-d2) - S * NormCDF(-d1);
        }
        #endregion

        protected override void OnBarUpdate()
        {
            if (CurrentBars[0] < 0 || BarsInProgress != 0 || !isDataLoaded) return;

            GexSnapshot snap = GetClosestSnapshot(Time[0]);
            if (snap == null || (Time[0] - snap.Timestamp).TotalMinutes > MaxDataAgeMinutes) return;

            double basisRatio = snap.BasisRatio;
            double chartSpotPrice = Close[0]; 
            double nativeSpotPrice = chartSpotPrice / basisRatio; 

            // HACK #1: Sticky-Delta Price Move Calculation
            double safeSnapshotSpot = snap.SnapshotSpot > 0 ? snap.SnapshotSpot : nativeSpotPrice;
            double priceMovePercent = (nativeSpotPrice / safeSnapshotSpot) - 1.0;

            // Live VIX Proxy Calculation
            double vixRatio = 1.0;
            if (CurrentBars.Length > 1 && CurrentBars[1] >= 0)
            {
                double liveVix = Closes[1][0];
                if (snap.SnapshotVIX > 0 && liveVix > 0) vixRatio = liveVix / snap.SnapshotVIX;
            }

            double barInterval = 5.0 * basisRatio;
            GexBarData bd = new GexBarData { SnapshotTime = snap.Timestamp, BarStrikeInterval = barInterval };

            foreach (var kvp in snap.Strikes)
            {
                double nativeStrike = kvp.Key;
                double chartStrike = nativeStrike * basisRatio;
                
                double netGex = 0.0, callGex = 0.0, putGex = 0.0, cPrice = 0.0, pPrice = 0.0, avgLiveIv = 0.0;
                int baseOiTotal = 0, syntheticOiTotal = 0;

                foreach (var opt in kvp.Value)
                {
                    double T = Math.Max(0.00001, (opt.ExpirationUtc - Time[0].ToUniversalTime()).TotalDays / 365.0);
                    
                    // HACK #2: Intraday Synthetic OI
                    int syntheticOI = opt.OI + (int)(opt.Volume * IntradayVolumeWeight);
                    
                    // HACK #1: Apply Sticky-Delta Skew + VIX Proxy
                    double shiftedIV = opt.BaseIV * (1.0 - (priceMovePercent * VolSkewFactor));
                    double finalLiveIV = Math.Max(0.01, shiftedIV * vixRatio); 
                    
                    // CALCULATE USING PREDICTIVE ENGINE
                    double gamma = CalculateGamma(nativeSpotPrice, nativeStrike, T, finalLiveIV);
                    double price = CalculateOptionPrice(nativeSpotPrice, nativeStrike, T, finalLiveIV, opt.IsCall);
                    
                    // We multiply by the SYNTHETIC OI, not the stale OI
                    double gex = gamma * syntheticOI * 100.0 * nativeSpotPrice; 
                    
                    if (opt.IsCall) { netGex += gex; callGex += gex; cPrice = Math.Max(cPrice, price); }
                    else { netGex -= gex; putGex -= gex; pPrice = Math.Max(pPrice, price); }

                    baseOiTotal += opt.OI;
                    syntheticOiTotal += syntheticOI;
                    avgLiveIv = finalLiveIV; 
                }

                bd.StrikeToNetGex[chartStrike] = netGex;
                bd.StrikeToCallGex[chartStrike] = callGex;
                bd.StrikeToPutGex[chartStrike] = putGex;
                bd.StrikeToTotalOI[chartStrike] = baseOiTotal;
                bd.StrikeToSyntheticOI[chartStrike] = syntheticOiTotal;
                bd.StrikeToCallPrice[chartStrike] = cPrice;
                bd.StrikeToPutPrice[chartStrike] = pPrice;
                bd.StrikeToLiveIV[chartStrike] = avgLiveIv;
            }
            gexSeries[0] = bd;
        }

        #region Visualization & UI
        private void DisposeBrushes()
        {
            if (positiveBrushes != null) for (int i = 0; i < 256; i++) { if (positiveBrushes[i] != null) positiveBrushes[i].Dispose(); if (negativeBrushes[i] != null) negativeBrushes[i].Dispose(); }
            if (tooltipBgBrush != null) tooltipBgBrush.Dispose();
            if (tooltipBorderBrush != null) tooltipBorderBrush.Dispose();
            if (tooltipTextBrush != null) tooltipTextBrush.Dispose();
            if (tooltipTextFormat != null) tooltipTextFormat.Dispose();
        }

        public override void OnRenderTargetChanged()
        {
            DisposeBrushes();
            if (RenderTarget != null)
            {
                for (int i = 0; i < 256; i++)
                {
                    double r = i / 255.0;
                    System.Windows.Media.Color pc = InterpolateColor(System.Windows.Media.Color.FromRgb(10, 20, 40), System.Windows.Media.Color.FromRgb(0, 200, 255), r);
                    positiveBrushes[i] = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color(pc.R, pc.G, pc.B)) { Opacity = 0.6f };
                    System.Windows.Media.Color nc = InterpolateColor(System.Windows.Media.Color.FromRgb(40, 10, 10), System.Windows.Media.Color.FromRgb(255, 100, 0), r);
                    negativeBrushes[i] = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color(nc.R, nc.G, nc.B)) { Opacity = 0.6f };
                }
                tooltipBgBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color4(0.05f, 0.05f, 0.08f, 0.95f));
                tooltipBorderBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, SharpDX.Color.DodgerBlue);
                tooltipTextBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, SharpDX.Color.White);
                tooltipTextFormat = new SharpDX.DirectWrite.TextFormat(NinjaTrader.Core.Globals.DirectWriteFactory, "Consolas", 13) { ParagraphAlignment = SharpDX.DirectWrite.ParagraphAlignment.Near };
            }
        }

        private System.Windows.Media.Color InterpolateColor(System.Windows.Media.Color c1, System.Windows.Media.Color c2, double r) { return System.Windows.Media.Color.FromRgb((byte)(c1.R + (c2.R - c1.R) * r), (byte)(c1.G + (c2.G - c1.G) * r), (byte)(c1.B + (c2.B - c1.B) * r)); }

        protected override void OnRender(ChartControl cc, ChartScale cs)
        {
            if (Bars == null || !isDataLoaded || IsInHitTest || positiveBrushes == null || positiveBrushes[0] == null) return;
            var prevMode = RenderTarget.AntialiasMode;
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
                if (i == lastIdx && (Bars.GetTime(ChartBars.ToIndex) - d.SnapshotTime).TotalMinutes <= MaxDataAgeMinutes) x2 = cc.CanvasRight;
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
                    
                    hits.Add(new GexHitInfo { Bounds = rect, Strike = kvp.Key, NativeStrike = kvp.Key / GetClosestSnapshot(Time[0]).BasisRatio, Value = kvp.Value, BaseOI = d.StrikeToTotalOI[kvp.Key], SyntheticOI = d.StrikeToSyntheticOI[kvp.Key], CallPrice = d.StrikeToCallPrice[kvp.Key], PutPrice = d.StrikeToPutPrice[kvp.Key], LiveIV = d.StrikeToLiveIV[kvp.Key] });
                }
            }

            RenderTarget.AntialiasMode = prevMode;
            hitTestList = hits;
            if (showTooltip && hoveredCell != null) DrawTooltip(hoveredCell);
        }

        private void DrawTooltip(GexHitInfo info)
        {
            if (tooltipTextFormat == null) return;
            string txt = string.Format(
                "Real-Time Options Pricing (Pro)\n" +
                "───────────────────────────────\n" +
                "Strike Space:        {0:N0}\n" +
                "Stale Overnight OI:  {1:N0}\n" +
                "Synthetic Live OI:   {2:N0}  [HACK 2]\n" +
                "Sticky-Delta IV:     {3:P1} [HACK 1]\n" +
                "Est. Call Premium:   ${4:N2}\n" +
                "Est. Put Premium:    ${5:N2}\n\n" +
                "Net $GEX Exposure:   ${6:N0}", 
                info.NativeStrike, info.BaseOI, info.SyntheticOI, info.LiveIV, info.CallPrice, info.PutPrice, info.Value);

            using (var lay = new SharpDX.DirectWrite.TextLayout(NinjaTrader.Core.Globals.DirectWriteFactory, txt, tooltipTextFormat, 350, 250))
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
