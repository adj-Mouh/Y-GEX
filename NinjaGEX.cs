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
using System.IO;
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
    public class GexHeatmapTCP_Precision : Indicator
    {
        #region Classes & Variables
        public class OptionData
        {
            public DateTime ExpiryDate; 
            public bool IsCall;
            public double Strike; 
            public int OI;
            public double MarketPrice; 
            public double EodIV;       
        }

        public class GexSnapshot
        {
            public double Timestamp;
            public double BasisRatio;  
            public double RiskFreeRate; 
            public double DividendYield; 
            public double SnapshotSpot; // السعر المرجعي لحظة جلب البيانات
            public List<OptionData> Options = new List<OptionData>();
        }

        private TcpListener tcpListener;
        private Task tcpTask;
        private bool isListening = false;
        private readonly object dataLock = new object();
        
        private GexSnapshot latestSnapshot = null;
        private Dictionary<double, double> StrikeNetGex = new Dictionary<double, double>();

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
        [Display(Name="Skew Sensitivity", Order=3, GroupName="1. Visuals")]
        public double SkewSensitivity { get; set; }

        [NinjaScriptProperty]
        [Display(Name="TCP Port", Order=1, GroupName="2. System")]
        public int TcpPort { get; set; } 
        #endregion

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description = "High-Precision TCP GEX Engine with Skew Shifting & NR-IV.";
                Name = "Precision TCP GEX v2";
                Calculate = Calculate.OnEachTick;
                IsOverlay = true;
                DrawOnPricePanel = true;
                CutoffPercent = 2.0;
                SkewSensitivity = 1.5; // القيمة الافتراضية المقترحة
                TcpPort = 9000;
                DisplayMode = GexDisplayMode.NetGex;
            }
            else if (State == State.Configure)
            {
                ZOrder = -1;
            }
            else if (State == State.DataLoaded)
            {
                positiveBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];
                negativeBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];

                isListening = true;
                tcpTask = Task.Run(() => StartTcpServer());
            }
            else if (State == State.Terminated)
            {
                isListening = false;
                if (tcpListener != null) tcpListener.Stop();
                DisposeBrushes();
            }
        }

        #region TCP Telemetry System
        private async Task StartTcpServer()
        {
            try 
            {
                tcpListener = new TcpListener(IPAddress.Any, TcpPort);
                tcpListener.Start();

                while (isListening)
                {
                    TcpClient client = await tcpListener.AcceptTcpClientAsync();
                    Task.Run(() => HandleClient(client));
                }
            }
            catch (Exception ex)
            {
                Print("TCP Server Error: " + ex.Message);
            }
        }

        private void HandleClient(TcpClient client)
        {
            try
            {
                using (NetworkStream stream = client.GetStream())
                using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
                {
                    string payload = reader.ReadToEnd();
                    ProcessPayload(payload);
                }
            }
            catch { }
            finally { client.Close(); }
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
                    RiskFreeRate = double.Parse(header[2]),
                    DividendYield = double.Parse(header[3]),
                    SnapshotSpot = header.Length > 4 ? double.Parse(header[4]) : 0 // السعر المرجعي من بايثون
                };

                for (int i = 1; i < parts.Length; i++)
                {
                    string[] row = parts[i].Split(',');
                    if (row.Length < 6) continue;

                    newSnap.Options.Add(new OptionData
                    {
                        ExpiryDate = DateTime.Parse(row[0]),
                        IsCall = row[1] == "1",
                        Strike = double.Parse(row[2]),
                        OI = (int)double.Parse(row[3]),
                        MarketPrice = double.Parse(row[4]),
                        EodIV = double.Parse(row[5])
                    });
                }

                lock (dataLock) { latestSnapshot = newSnap; }
                
                if (ChartControl != null)
                    ChartControl.Dispatcher.InvokeAsync(() => ChartControl.InvalidateVisual());
            }
            catch { }
        }
        #endregion

        #region Precision Math Engine with Skew Shifting
        private static double NormPDF(double x) { return Math.Exp(-x * x / 2.0) / Math.Sqrt(2.0 * Math.PI); }
        private static double NormCDF(double x) {
            int sign = x < 0 ? -1 : 1; x = Math.Abs(x) / Math.Sqrt(2.0);
            double t = 1.0 / (1.0 + 0.3275911 * x);
            double y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.Exp(-x * x);
            return 0.5 * (1.0 + sign * y);
        }

        // إزاحة التقلب بناءً على تحرك السعر الحالي عن سعر السناب شوت
        private double GetShiftedIV(double baseIV, double currentSpot, double referenceSpot)
        {
            if (referenceSpot <= 0) return baseIV;
            
            // حساب نسبة التغير في السعر
            double priceShiftPercent = (currentSpot - referenceSpot) / referenceSpot;
            
            // Sticky-Delta Assumption: IV ينزاح عكس السعر لتعويض التأخير
            double shiftedIV = baseIV * (1.0 - (priceShiftPercent * SkewSensitivity));
            
            return Math.Max(0.01, Math.Min(2.5, shiftedIV)); // حدود آمنة
        }

        private double CalculateBSMPrice(bool isCall, double S, double K, double T, double v, double r, double q)
        {
            double d1 = (Math.Log(S / K) + (r - q + v * v / 2.0) * T) / (v * Math.Sqrt(T));
            double d2 = d1 - v * Math.Sqrt(T);
            return isCall ? (S * Math.Exp(-q * T) * NormCDF(d1) - K * Math.Exp(-r * T) * NormCDF(d2)) 
                          : (K * Math.Exp(-r * T) * NormCDF(-d2) - S * Math.Exp(-q * T) * NormCDF(-d1));
        }

        private double CalculateVega(double S, double K, double T, double v, double r, double q)
        {
            double d1 = (Math.Log(S / K) + (r - q + v * v / 2.0) * T) / (v * Math.Sqrt(T));
            return S * Math.Exp(-q * T) * NormPDF(d1) * Math.Sqrt(T);
        }

        private double ImpliedVolatilityNR(bool isCall, double S, double K, double T, double r, double q, double marketPrice, double fallbackIV)
        {
            if (marketPrice <= 0 || T <= 0) return fallbackIV;
            double sigma = fallbackIV > 0 ? fallbackIV : 0.2;
            for (int i = 0; i < 100; i++)
            {
                double price = CalculateBSMPrice(isCall, S, K, T, sigma, r, q);
                double diff = price - marketPrice;
                if (Math.Abs(diff) < 1e-5) return sigma;
                double vega = CalculateVega(S, K, T, sigma, r, q);
                if (vega < 1e-6) break;
                sigma -= diff / vega;
            }
            return Math.Max(0.001, sigma);
        }

        private double CalculateExactGamma(double S, double K, double T, double v, double r, double q) {
            if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return 0.0;
            double d1 = (Math.Log(S / K) + (r - q + v * v / 2.0) * T) / (v * Math.Sqrt(T));
            return (Math.Exp(-q * T) * NormPDF(d1)) / (S * v * Math.Sqrt(T));
        }
        #endregion

        protected override void OnBarUpdate()
        {
            if (CurrentBars[0] < 0 || latestSnapshot == null) return;
            
            double liveEsPrice = Close[0];
            if (Math.Abs(liveEsPrice - lastCalcPrice) < 0.25 && (DateTime.Now - lastCalcTime).TotalMilliseconds < 250)
                return;
                
            lastCalcPrice = liveEsPrice;
            lastCalcTime = DateTime.Now;

            GexSnapshot snap;
            lock (dataLock) { snap = latestSnapshot; }

            double basis = snap.BasisRatio;
            double nativeSpot = liveEsPrice / basis;
            double r = snap.RiskFreeRate;
            double q = snap.DividendYield;
            
            Dictionary<double, double> tempNetGex = new Dictionary<double, double>();
            DateTime currentTime = DateTime.UtcNow;

            foreach (var opt in snap.Options)
            {
                double chartStrike = opt.Strike * basis;
                double T = (opt.ExpiryDate - currentTime).TotalMinutes / 525600.0;
                if (T <= 0.0001) continue; 

                // 1. حساب التقلب الأولي (إما عبر NR أو الـ EOD)
                double baseIV = (opt.MarketPrice > 0) 
                    ? ImpliedVolatilityNR(opt.IsCall, nativeSpot, opt.Strike, T, r, q, opt.MarketPrice, opt.EodIV)
                    : opt.EodIV;

                // 2. تطبيق خدعة الـ Skew Shifting لتعويض تحرك السعر اللحظي
                double adjustedIV = GetShiftedIV(baseIV, nativeSpot, snap.SnapshotSpot);

                // 3. حساب الجاما بالتقلب المعدل
                double gamma = CalculateExactGamma(nativeSpot, opt.Strike, T, adjustedIV, r, q);
                double gex = gamma * opt.OI * 100.0 * nativeSpot;

                if (!tempNetGex.ContainsKey(chartStrike)) tempNetGex[chartStrike] = 0;
                if (opt.IsCall) tempNetGex[chartStrike] += gex;
                else tempNetGex[chartStrike] -= gex;
            }

            lock (dataLock) { StrikeNetGex = tempNetGex; }
        }

        #region Rendering Logic (Hardware-Accelerated)
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
            if (positiveBrushes != null) for (int i = 0; i < 256; i++) { if (positiveBrushes[i] != null) positiveBrushes[i].Dispose();
            if (negativeBrushes[i] != null) negativeBrushes[i].Dispose(); }
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

            float x1 = cc.CanvasLeft, x2 = cc.CanvasRight, w = x2 - x1;

            foreach (var kvp in renderGex)
            {
                if (kvp.Key > maxP + 10 || kvp.Key < minP - 10) continue;
                double ratio = Math.Abs(kvp.Value) / maxGex;
                if (ratio < (CutoffPercent / 100.0)) continue;
                
                float yt = cs.GetYByValue(kvp.Key + 2.5f);
                float yb = cs.GetYByValue(kvp.Key - 2.5f);
                SharpDX.RectangleF rect = new SharpDX.RectangleF(x1, Math.Min(yt, yb), w, Math.Abs(yb - yt));
                int colorIndex = Math.Max(0, Math.Min(255, (int)(ratio * 255)));
                RenderTarget.FillRectangle(rect, kvp.Value >= 0 ? positiveBrushes[colorIndex] : negativeBrushes[colorIndex]);
            }
        }
        #endregion
    }
}
