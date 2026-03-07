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

namespace NinjaTrader.NinjaScript.Indicators
{
	// Enum for the new Dropdown Selection
	public enum GexDisplayMode
	{
		NetGEX,
		CallsOnly,
		PutsOnly
	}

	public class GexHeatmap : Indicator
	{
		#region Helper Classes & Structs
		public class OptionData
		{
			public DateTime ExpirationUtc; 
			public bool IsCall;
			public double Strike; 
			public int OI;
			public double IV;
		}

		public class GexSnapshot
		{
			public DateTime Timestamp; 
			public double BasisRatio;  
			public Dictionary<double, List<OptionData>> Strikes = new Dictionary<double, List<OptionData>>();
		}

		public class GexBarData
		{
			public DateTime SnapshotTime;
			public double BarStrikeInterval;
			public Dictionary<double, double> StrikeToGex = new Dictionary<double, double>();
			public Dictionary<double, int> StrikeToTotalOI = new Dictionary<double, int>();
		}

		private class GexHitInfo
		{
			public SharpDX.RectangleF Bounds;
			public double Strike;
			public double ValueGex;
			public int TotalOI;
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
		[Display(Name="CSV Folder Path", Description="Full path to the folder containing Options CSVs.", Order=1, GroupName="Parameters")]
		public string CsvFolderPath { get; set; }

		[NinjaScriptProperty]
		[Display(Name="Ticker Override", Description="Leave empty to auto-map (e.g. ES->SPX). Enter a ticker to force read (e.g. SPY).", Order=2, GroupName="Parameters")]
		public string TickerOverride { get; set; }

		[NinjaScriptProperty]
		[Range(0, 100)]
		[Display(Name="Filter Cutoff %", Description="Hide cells with GEX lower than this percentage of the max.", Order=3, GroupName="Parameters")]
		public double CutoffPercent { get; set; }

		[NinjaScriptProperty]
		[Range(1, 120)]
		[Display(Name="Max Data Age (Min)", Description="Stop plotting if CSV data is older than this to prevent staleness.", Order=4, GroupName="Parameters")]
		public int MaxDataAgeMinutes { get; set; }

		// NEW PARAMETER: Display Mode
		[NinjaScriptProperty]
		[Display(Name="Display Mode", Description="Choose which GEX to display: Net (Calls - Puts), Calls Only, or Puts Only.", Order=5, GroupName="Parameters")]
		public GexDisplayMode DisplayMode { get; set; }
		#endregion

		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description									= @"Visualizes Gamma Exposure (GEX) dynamically in real-time.";
				Name										= "GEX Heatmap Engine";
				Calculate									= Calculate.OnPriceChange; 
				IsOverlay									= true;
				DisplayInDataBox							= false;
				DrawOnPricePanel							= true;
				PaintPriceMarkers							= true;
				ScaleJustification							= NinjaTrader.Gui.Chart.ScaleJustification.Right;
				
				CsvFolderPath								= @"C:\Options_History_Data";
				TickerOverride                              = ""; 
				CutoffPercent								= 2.0; 
				MaxDataAgeMinutes							= 5;
				DisplayMode                                 = GexDisplayMode.NetGEX; // Default to Net
			}
			else if (State == State.Configure)
			{
				ZOrder = -1; 
				
				try { easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
				catch 
				{ 
					Print("GEX WARNING: Could not find 'Eastern Standard Time'. 0-DTE calculations may be slightly off."); 
					easternZone = TimeZoneInfo.Local; 
				}
			}
			else if (State == State.DataLoaded)
			{
				historicalSnapshots = new List<GexSnapshot>();
				gexSeries = new Series<GexBarData>(this, MaximumBarsLookBack.Infinite);
				positiveBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];
				negativeBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];

				LoadGexDataFromCsv();

				if (fileCheckTimer == null)
				{
					fileCheckTimer = new DispatcherTimer();
					fileCheckTimer.Interval = TimeSpan.FromSeconds(5);
					fileCheckTimer.Tick += OnFileCheckTimerTick;
					fileCheckTimer.Start();
				}
			}
			else if (State == State.Historical)
			{
				if (ChartControl != null && !isMouseSubscribed)
				{
					ChartControl.MouseMove += OnChartMouseMove;
					isMouseSubscribed = true;
				}
			}
			else if (State == State.Terminated)
			{
				if (ChartControl != null && isMouseSubscribed)
				{
					ChartControl.MouseMove -= OnChartMouseMove;
					isMouseSubscribed = false;
				}
				if (hoverTimer != null) { hoverTimer.Stop(); hoverTimer = null; }
				if (fileCheckTimer != null) { fileCheckTimer.Stop(); fileCheckTimer = null; }
				DisposeBrushes();
			}
		}

		private string GetMappedTicker()
		{
			if (!string.IsNullOrEmpty(TickerOverride))
				return TickerOverride.Replace("^", "").Replace(".", "_");

			string instrName = Instrument.MasterInstrument.Name.ToUpper();

			switch (instrName)
			{
				case "ES":
				case "MES":
					return "SPX"; 
				case "NQ":
				case "MNQ":
					return "NDX"; 
				case "RTY":
				case "M2K":
					return "RUT"; 
				case "YM":
				case "MYM":
					return "DJI"; 
				default:
					return instrName; 
			}
		}

		private void OnFileCheckTimerTick(object sender, EventArgs e)
		{
			try
			{
				if (!Directory.Exists(CsvFolderPath)) return;
				
				string targetName = GetMappedTicker();
				string searchPattern = string.Format("*{0}*.csv", targetName);
				string[] files = Directory.GetFiles(CsvFolderPath, searchPattern);

				bool hasNewData = false;
				DateTime latestWriteTime = lastFileReadTime;

				foreach (string file in files)
				{
					DateTime fileWriteTime = File.GetLastWriteTime(file);
					
					if (fileWriteTime > lastFileReadTime)
					{
						hasNewData = true;
						if (fileWriteTime > latestWriteTime) latestWriteTime = fileWriteTime;
					}
				}

				if (hasNewData)
				{
					LoadGexDataFromCsv(); 
					lastFileReadTime = latestWriteTime;
					if (ChartControl != null) ChartControl.InvalidateVisual();
				}
			}
			catch (Exception ex) { Print("GEX Timer Auto-Reload Error: " + ex.Message); }
		}

		private void LoadGexDataFromCsv()
		{
			try
			{
				if (!Directory.Exists(CsvFolderPath) || easternZone == null) return;

				string targetName = GetMappedTicker();
				string searchPattern = string.Format("*{0}*.csv", targetName);
				string[] files = Directory.GetFiles(CsvFolderPath, searchPattern);

				if (files.Length == 0) return;

				Dictionary<DateTime, GexSnapshot> tempSnapshots = new Dictionary<DateTime, GexSnapshot>();
				DateTime maxWriteTime = lastFileReadTime;

				foreach (string file in files)
				{
					DateTime fileWriteTime = File.GetLastWriteTime(file);
					if (fileWriteTime > maxWriteTime) maxWriteTime = fileWriteTime;

					try
					{
						using (FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
						using (StreamReader sr = new StreamReader(fs))
						{
							string header = sr.ReadLine(); 
							if (header == null) continue; 

							string line;
							while ((line = sr.ReadLine()) != null)
							{
								if (string.IsNullOrWhiteSpace(line) || line.Contains("Timestamp")) continue;
								
								string[] cols = line.Split(',');
								if (cols.Length < 7) continue;

								try 
								{
									DateTime timestamp = DateTime.ParseExact(cols[0], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
									
									DateTime expirationDate = DateTime.ParseExact(cols[1], "yyyy-MM-dd", CultureInfo.InvariantCulture);
									DateTime expirationAt4pm = expirationDate.AddHours(16); 
									DateTime expirationUtc = TimeZoneInfo.ConvertTimeToUtc(expirationAt4pm, easternZone);
									
									double basisRatio = double.Parse(cols[2], CultureInfo.InvariantCulture);
									
									bool isCall = cols[3].Trim().Equals("Call", StringComparison.OrdinalIgnoreCase);
									double strike = double.Parse(cols[4], CultureInfo.InvariantCulture);
									int oi = (int)double.Parse(cols[5], CultureInfo.InvariantCulture);
									double iv = double.Parse(cols[6], CultureInfo.InvariantCulture);

									if (!tempSnapshots.ContainsKey(timestamp))
										tempSnapshots[timestamp] = new GexSnapshot { Timestamp = timestamp, BasisRatio = basisRatio };

									var snap = tempSnapshots[timestamp];
									if (!snap.Strikes.ContainsKey(strike)) snap.Strikes[strike] = new List<OptionData>();

									snap.Strikes[strike].Add(new OptionData { ExpirationUtc = expirationUtc, IsCall = isCall, Strike = strike, OI = oi, IV = iv });
								}
								catch { } 
							}
						}
					}
					catch (IOException)
					{
						continue; 
					}
				}

				if (tempSnapshots.Count > 0)
				{
					historicalSnapshots = tempSnapshots.Values.OrderBy(x => x.Timestamp).ToList();
					isDataLoaded = historicalSnapshots.Count > 0;
					lastFileReadTime = maxWriteTime;
				}
			}
			catch (Exception ex) { Print("GEX Loader Error: " + ex.Message); }
		}

		private GexSnapshot GetClosestSnapshot(DateTime targetTime)
		{
			if (historicalSnapshots == null || historicalSnapshots.Count == 0) return null;
			int left = 0;
			int right = historicalSnapshots.Count - 1;
			GexSnapshot closest = null;

			while (left <= right)
			{
				int mid = left + (right - left) / 2;
				if (historicalSnapshots[mid].Timestamp <= targetTime)
				{
					closest = historicalSnapshots[mid]; 
					left = mid + 1;
				}
				else right = mid - 1; 
			}
			return closest;
		}

		private static double NormPDF(double x) { return Math.Exp(-x * x / 2.0) / Math.Sqrt(2.0 * Math.PI); }

		private double CalculateGamma(double S, double K, double T, double v, double r = 0.05)
		{
			if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return 0.0;
			double d1 = (Math.Log(S / K) + (r + v * v / 2.0) * T) / (v * Math.Sqrt(T));
			return NormPDF(d1) / (S * v * Math.Sqrt(T));
		}

		protected override void OnBarUpdate()
		{
			if (CurrentBar < 0 || !isDataLoaded) return;

			DateTime currentBarTimeUtc = Time[0].ToUniversalTime();
			
			GexSnapshot closestSnap = GetClosestSnapshot(Time[0]);
			if (closestSnap == null) return;
			
			if ((Time[0] - closestSnap.Timestamp).TotalMinutes > MaxDataAgeMinutes) return;

			double spotPrice = Close[0];
			double basisRatio = closestSnap.BasisRatio; 

			var sortedStrikes = closestSnap.Strikes.Keys.OrderBy(k => k).ToList();
			double minDiff = double.MaxValue;
			for (int i = 1; i < sortedStrikes.Count; i++)
			{
				double diff = Math.Round((sortedStrikes[i] - sortedStrikes[i - 1]) * basisRatio, 2);
				if (diff > 0.1 && diff < minDiff) minDiff = diff;
			}
			double barInterval = minDiff != double.MaxValue ? minDiff : (5.0 * basisRatio);

			GexBarData barData = new GexBarData();
			barData.SnapshotTime = closestSnap.Timestamp; 
			barData.BarStrikeInterval = barInterval;

			foreach (var kvp in closestSnap.Strikes)
			{
				double adjustedStrike = kvp.Key * basisRatio;
				
				double displayGex = 0.0;
				int totalOi = 0;

				foreach (var opt in kvp.Value)
				{
					double T = Math.Max(0.00001, (opt.ExpirationUtc - currentBarTimeUtc).TotalDays / 365.0);
					
					double gamma = CalculateGamma(spotPrice, adjustedStrike, T, opt.IV);
					double gex = gamma * opt.OI * 100.0;
					
					// Apply Selection Filter
					if (opt.IsCall)
					{
						if (DisplayMode == GexDisplayMode.NetGEX || DisplayMode == GexDisplayMode.CallsOnly)
						{
							displayGex += gex;
							totalOi += opt.OI;
						}
					}
					else
					{
						if (DisplayMode == GexDisplayMode.NetGEX || DisplayMode == GexDisplayMode.PutsOnly)
						{
							displayGex -= gex; // Puts stay negative (red) for visual consistency
							totalOi += opt.OI;
						}
					}
				}

				if (barData.StrikeToGex.ContainsKey(adjustedStrike))
				{
					barData.StrikeToGex[adjustedStrike] += displayGex;
					barData.StrikeToTotalOI[adjustedStrike] += totalOi;
				}
				else
				{
					barData.StrikeToGex[adjustedStrike] = displayGex;
					barData.StrikeToTotalOI[adjustedStrike] = totalOi;
				}
			}

			gexSeries[0] = barData;
		}
		
		#region Rendering and UI
		private void DisposeBrushes()
		{
			if (positiveBrushes != null)
			{
				for (int i = 0; i < 256; i++)
				{
					if (positiveBrushes[i] != null) { positiveBrushes[i].Dispose(); positiveBrushes[i] = null; }
					if (negativeBrushes[i] != null) { negativeBrushes[i].Dispose(); negativeBrushes[i] = null; }
				}
			}
			if (tooltipBgBrush != null) { tooltipBgBrush.Dispose(); tooltipBgBrush = null; }
			if (tooltipBorderBrush != null) { tooltipBorderBrush.Dispose(); tooltipBorderBrush = null; }
			if (tooltipTextBrush != null) { tooltipTextBrush.Dispose(); tooltipTextBrush = null; }
			if (tooltipTextFormat != null) { tooltipTextFormat.Dispose(); tooltipTextFormat = null; }
		}

		public override void OnRenderTargetChanged()
		{
			DisposeBrushes();
			if (RenderTarget != null)
			{
				for (int i = 0; i < 256; i++)
				{
					double ratio = i / 255.0;
					System.Windows.Media.Color posColor = InterpolateColor(System.Windows.Media.Color.FromRgb(10, 20, 40), System.Windows.Media.Color.FromRgb(0, 200, 255), ratio);
					positiveBrushes[i] = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color(posColor.R, posColor.G, posColor.B)) { Opacity = 0.6f };
					
					System.Windows.Media.Color negColor = InterpolateColor(System.Windows.Media.Color.FromRgb(40, 10, 10), System.Windows.Media.Color.FromRgb(255, 100, 0), ratio);
					negativeBrushes[i] = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color(negColor.R, negColor.G, negColor.B)) { Opacity = 0.6f };
				}

				tooltipBgBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, new SharpDX.Color4(0.1f, 0.1f, 0.15f, 0.95f));
				tooltipBorderBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, SharpDX.Color.Gray);
				tooltipTextBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, SharpDX.Color.White);
				tooltipTextFormat = new SharpDX.DirectWrite.TextFormat(NinjaTrader.Core.Globals.DirectWriteFactory, "Consolas", 12) 
				{ TextAlignment = SharpDX.DirectWrite.TextAlignment.Leading, ParagraphAlignment = SharpDX.DirectWrite.ParagraphAlignment.Near };
			}
		}

		private System.Windows.Media.Color InterpolateColor(System.Windows.Media.Color c1, System.Windows.Media.Color c2, double ratio)
		{
			byte r = (byte)(c1.R + (c2.R - c1.R) * ratio);
			byte g = (byte)(c1.G + (c2.G - c1.G) * ratio);
			byte b = (byte)(c1.B + (c2.B - c1.B) * ratio);
			return System.Windows.Media.Color.FromRgb(r, g, b);
		}

		protected override void OnRender(ChartControl chartControl, ChartScale chartScale)
		{
			if (Bars == null || !isDataLoaded || IsInHitTest || positiveBrushes == null || positiveBrushes[0] == null) return;
			
			var previousMode = RenderTarget.AntialiasMode;
			RenderTarget.AntialiasMode = SharpDX.Direct2D1.AntialiasMode.Aliased;

			double minPrice = chartScale.MinValue;
			double maxPrice = chartScale.MaxValue;
			double maxAbsGex = 0.0001; 

			int lastValidIndex = -1;
			for (int i = ChartBars.ToIndex; i >= ChartBars.FromIndex; i--)
			{
				if (gexSeries.IsValidDataPointAt(i) && gexSeries.GetValueAt(i) != null)
				{
					lastValidIndex = i;
					break;
				}
			}

			for (int i = ChartBars.FromIndex; i <= ChartBars.ToIndex; i++)
			{
				if (!gexSeries.IsValidDataPointAt(i)) continue;
				GexBarData data = gexSeries.GetValueAt(i);
				if (data == null) continue;
				foreach(var kvp in data.StrikeToGex)
				{
					if (kvp.Key >= minPrice && kvp.Key <= maxPrice)
						maxAbsGex = Math.Max(maxAbsGex, Math.Abs(kvp.Value));
				}
			}

			List<GexHitInfo> frameHitList = new List<GexHitInfo>();

			for (int i = ChartBars.FromIndex; i <= ChartBars.ToIndex; i++)
			{
				if (!gexSeries.IsValidDataPointAt(i)) continue;
				GexBarData data = gexSeries.GetValueAt(i);
				if (data == null) continue;

				float x1 = chartControl.GetXByBarIndex(ChartBars, i);
				float x2 = chartControl.GetXByBarIndex(ChartBars, i + 1);
				
				if (i == lastValidIndex)
				{
					DateTime screenEndTime = Bars.GetTime(ChartBars.ToIndex);
					if ((screenEndTime - data.SnapshotTime).TotalMinutes <= MaxDataAgeMinutes)
					{
						x2 = chartControl.CanvasRight; 
					}
				}

				float width = Math.Max(1f, x2 - x1);

				foreach (var kvp in data.StrikeToGex)
				{
					double strike = kvp.Key;
					double gexVal = kvp.Value;

					if (strike > maxPrice + data.BarStrikeInterval || strike < minPrice - data.BarStrikeInterval) continue;

					double gexRatio = Math.Abs(gexVal) / maxAbsGex;
					if (gexRatio < (CutoffPercent / 100.0)) continue; 

					float yTop = chartScale.GetYByValue(strike + (data.BarStrikeInterval / 2.0));
					float yBottom = chartScale.GetYByValue(strike - (data.BarStrikeInterval / 2.0));
					
					float height = Math.Abs(yBottom - yTop);
					float yDraw = Math.Min(yTop, yBottom);

					SharpDX.RectangleF rect = new SharpDX.RectangleF(x1, yDraw, width, height);
					int brushIndex = Math.Max(0, Math.Min(255, (int)(gexRatio * 255)));
					var brush = gexVal >= 0 ? positiveBrushes[brushIndex] : negativeBrushes[brushIndex];

					RenderTarget.FillRectangle(rect, brush);
					frameHitList.Add(new GexHitInfo { Bounds = rect, Strike = strike, ValueGex = gexVal, TotalOI = data.StrikeToTotalOI[strike] });
				}
			}

			RenderTarget.AntialiasMode = previousMode;
			hitTestList = frameHitList;
			if (showTooltip && hoveredCell != null) DrawTooltip(hoveredCell);
		}

		private void DrawTooltip(GexHitInfo info)
		{
			if (tooltipTextFormat == null || tooltipBgBrush == null) return;
			
			string modeLabel = DisplayMode == GexDisplayMode.CallsOnly ? "Call GEX" : 
			                   DisplayMode == GexDisplayMode.PutsOnly ? "Put GEX" : "Net GEX";

			string text = string.Format("Total OI: {0:N0}\n{1}: {2:N2}", info.TotalOI, modeLabel, info.ValueGex);

			using (var layout = new SharpDX.DirectWrite.TextLayout(NinjaTrader.Core.Globals.DirectWriteFactory, text, tooltipTextFormat, 250, 100))
			{
				float padding = 8f;
				float width = layout.Metrics.Width + (padding * 2);
				float height = layout.Metrics.Height + (padding * 2);

				float tipX = mousePosition.X + 15;
				float tipY = mousePosition.Y - height - 15;

				if (tipX + width > ChartControl.ActualWidth) tipX = mousePosition.X - width - 15;
				if (tipY < 0) tipY = mousePosition.Y + 15;

				SharpDX.RectangleF rect = new SharpDX.RectangleF(tipX, tipY, width, height);
				SharpDX.Direct2D1.RoundedRectangle roundedRect = new SharpDX.Direct2D1.RoundedRectangle { Rect = rect, RadiusX = 5, RadiusY = 5 };

				RenderTarget.FillRoundedRectangle(roundedRect, tooltipBgBrush);
				RenderTarget.DrawRoundedRectangle(roundedRect, tooltipBorderBrush, 1.5f);
				RenderTarget.DrawTextLayout(new SharpDX.Vector2(tipX + padding, tipY + padding), layout, tooltipTextBrush);
			}
		}

		private void OnChartMouseMove(object sender, MouseEventArgs e)
		{
			if (hoverTimer == null)
			{
				hoverTimer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(200) };
				hoverTimer.Tick += (s, args) =>
				{
					hoverTimer.Stop();
					showTooltip = true;
					if (ChartControl != null) ChartControl.InvalidateVisual();
				};
			}

			if (ChartControl == null) return;
			System.Windows.Point wpfPoint = e.GetPosition(ChartControl as System.Windows.IInputElement);
			mousePosition = new SharpDX.Vector2((floa
