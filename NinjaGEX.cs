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
	public class GexHeatmap : Indicator
	{
		#region Helper Classes & Structs
		public class OptionData
		{
			public DateTime Expiration;
			public bool IsCall;
			public double Strike;
			public int OI;
			public double IV;
		}

		public class GexSnapshot
		{
			public DateTime Timestamp;
			public Dictionary<double, List<OptionData>> Strikes = new Dictionary<double, List<OptionData>>();
		}

		public class GexBarData
		{
			public Dictionary<double, double> StrikeToNetGex = new Dictionary<double, double>();
			public Dictionary<double, int> StrikeToTotalOI = new Dictionary<double, int>();
		}

		private class GexHitInfo
		{
			public SharpDX.RectangleF Bounds;
			public double Strike;
			public double NetGex;
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
		private DispatcherTimer hoverTimer;
		private bool showTooltip = false;
		private bool isMouseSubscribed = false;
		private SharpDX.Vector2 mousePosition;

		private SharpDX.DirectWrite.TextFormat tooltipTextFormat;
		private SharpDX.Direct2D1.SolidColorBrush tooltipBgBrush;
		private SharpDX.Direct2D1.SolidColorBrush tooltipBorderBrush;
		private SharpDX.Direct2D1.SolidColorBrush tooltipTextBrush;
        
		private bool isMultiplierCalculated = false;
		#endregion

		#region Parameters
		[NinjaScriptProperty]
		[Display(Name="CSV Folder Path", Description="Full path to the folder containing Options CSVs.", Order=1, GroupName="Parameters")]
		public string CsvFolderPath { get; set; }

		[NinjaScriptProperty]
		[Display(Name="Ticker Override", Description="Force read a specific ticker (e.g. SPX) regardless of chart symbol.", Order=2, GroupName="Parameters")]
		public string TickerOverride { get; set; }

		[NinjaScriptProperty]
		[Range(0.1, 100)]
		[Display(Name="Strike Interval", Description="Spacing between strikes (e.g. 5 for SPX, 1 for SPY). Used for drawing height.", Order=3, GroupName="Parameters")]
		public double StrikeInterval { get; set; }

		[NinjaScriptProperty]
		[Range(0, 100)]
		[Display(Name="Filter Cutoff %", Description="Hide cells with GEX lower than this percentage of the max.", Order=4, GroupName="Parameters")]
		public double CutoffPercent { get; set; }

		[NinjaScriptProperty]
		[Range(1, 120)]
		[Display(Name="Max Data Age (Min)", Description="Stop plotting if CSV data is older than this (prevents stretching).", Order=5, GroupName="Parameters")]
		public int MaxDataAgeMinutes { get; set; }

		[NinjaScriptProperty]
		[Display(Name="Auto Multiplier", Description="Automatically calculate Price Multiplier based on chart price vs options strikes.", Order=6, GroupName="Parameters")]
		public bool AutoMultiplier { get; set; }

		[NinjaScriptProperty]
		[Range(0.01, 1000.0)]
		[Display(Name="Price Multiplier (Manual)", Description="Multiply CSV strikes by this to match chart. Ignored if Auto Multiplier is true.", Order=7, GroupName="Parameters")]
		public double PriceMultiplier { get; set; }
		#endregion

		#region State Management
		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description									= @"Visualizes Gamma Exposure (GEX) dynamically.";
				Name										= "GEX Heatmap Engine";
				Calculate									= Calculate.OnBarClose;
				IsOverlay									= true;
				DisplayInDataBox							= false;
				DrawOnPricePanel							= true;
				PaintPriceMarkers							= true;
				ScaleJustification							= NinjaTrader.Gui.Chart.ScaleJustification.Right;
				
				CsvFolderPath								= @"C:\Options_History_Data";
				TickerOverride                              = ""; 
				StrikeInterval								= 5.0; 
				CutoffPercent								= 2.0; 
				MaxDataAgeMinutes							= 5;
				AutoMultiplier                              = true;
				PriceMultiplier                             = 1.0; 
			}
			else if (State == State.Configure)
			{
				ZOrder = -1; // Draw behind candles
			}
			else if (State == State.DataLoaded)
			{
				historicalSnapshots = new List<GexSnapshot>();
				gexSeries = new Series<GexBarData>(this, MaximumBarsLookBack.Infinite);
				positiveBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];
				negativeBrushes = new SharpDX.Direct2D1.SolidColorBrush[256];
				isMultiplierCalculated = false;

				LoadGexDataFromCsv();
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
				DisposeBrushes();
			}
		}
		#endregion

		#region Data Ingestion (Synchronous)
		private void LoadGexDataFromCsv()
		{
			try
			{
				if (!Directory.Exists(CsvFolderPath))
				{
					Print("GEX Engine Error: Directory not found -> " + CsvFolderPath);
					return;
				}

				string targetName = !string.IsNullOrEmpty(TickerOverride) 
					? TickerOverride.Replace("^", "").Replace(".", "_") 
					: Instrument.MasterInstrument.Name.Replace("^", "").Replace(".", "_");

				string searchPattern = string.Format("*{0}*.csv", targetName);
				string[] files = Directory.GetFiles(CsvFolderPath, searchPattern);

				if (files.Length == 0)
				{
					Print(string.Format("GEX Engine: No files found for ticker '{0}'", targetName));
					return;
				}

				Dictionary<DateTime, GexSnapshot> tempSnapshots = new Dictionary<DateTime, GexSnapshot>();
				int loadedRows = 0;

				foreach (string file in files)
				{
					using (StreamReader sr = new StreamReader(file))
					{
						string line;
						while ((line = sr.ReadLine()) != null)
						{
							if (string.IsNullOrWhiteSpace(line)) continue;

							string[] cols = line.Split(',');
							if (cols[0].Contains("Time") || cols[0].Contains("Date") || cols[0].Contains("Timestamp")) continue;
							if (cols.Length < 6) continue;

							try 
							{
								DateTime timestamp = DateTime.ParseExact(cols[0], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
								DateTime expiration = DateTime.ParseExact(cols[1], "yyyy-MM-dd", CultureInfo.InvariantCulture);
								bool isCall = cols[2].Trim().Equals("Call", StringComparison.OrdinalIgnoreCase);
								double strike = double.Parse(cols[3], CultureInfo.InvariantCulture);
								int oi = (int)double.Parse(cols[4], CultureInfo.InvariantCulture);
								double iv = double.Parse(cols[5], CultureInfo.InvariantCulture);

								if (!tempSnapshots.ContainsKey(timestamp))
									tempSnapshots[timestamp] = new GexSnapshot { Timestamp = timestamp };

								var snap = tempSnapshots[timestamp];
								if (!snap.Strikes.ContainsKey(strike))
									snap.Strikes[strike] = new List<OptionData>();

								snap.Strikes[strike].Add(new OptionData
								{
									Expiration = expiration,
									IsCall = isCall,
									Strike = strike,
									OI = oi,
									IV = iv
								});
								
								loadedRows++;
							}
							catch { } 
						}
					}
				}

				historicalSnapshots = tempSnapshots.Values.OrderBy(x => x.Timestamp).ToList();
				isDataLoaded = historicalSnapshots.Count > 0;
				
				Print(string.Format("GEX Engine: Successfully loaded {0} rows across {1} time snapshots for {2}.", loadedRows, historicalSnapshots.Count, targetName));
			}
			catch (Exception ex)
			{
				Print("GEX Engine Loader Error: " + ex.Message);
			}
		}
		#endregion

		#region Math Engine (Black-Scholes)
		private static double NormPDF(double x)
		{
			return Math.Exp(-x * x / 2.0) / Math.Sqrt(2.0 * Math.PI);
		}

		private double CalculateGamma(double S, double K, double T, double v, double r = 0.05)
		{
			if (T <= 0 || v <= 0 || S <= 0 || K <= 0) return 0.0;
			double d1 = (Math.Log(S / K) + (r + v * v / 2.0) * T) / (v * Math.Sqrt(T));
			return NormPDF(d1) / (S * v * Math.Sqrt(T));
		}
		#endregion

		#region Processing (Bar Update)
		protected override void OnBarUpdate()
		{
			if (CurrentBar < 0 || !isDataLoaded) return;

			GexSnapshot closestSnap = null;
			DateTime currentBarTime = Time[0];

			for (int i = historicalSnapshots.Count - 1; i >= 0; i--)
			{
				if (historicalSnapshots[i].Timestamp <= currentBarTime)
				{
					closestSnap = historicalSnapshots[i];
					break;
				}
			}

			if (closestSnap == null) return;

			// Staleness Check
			if ((currentBarTime - closestSnap.Timestamp).TotalMinutes > MaxDataAgeMinutes)
				return;

			double spotPrice = Close[0]; 

			// --- AUTO MULTIPLIER LOGIC ---
			if (AutoMultiplier && !isMultiplierCalculated && closestSnap.Strikes.Count > 0)
			{
				double sumStrikes = 0;
				foreach (double strike in closestSnap.Strikes.Keys)
				{
					sumStrikes += strike;
				}
				double avgStrike = sumStrikes / closestSnap.Strikes.Count;

				if (avgStrike > 0)
				{
					double ratio = spotPrice / avgStrike;
					if (ratio >= 1.0)
					{
						PriceMultiplier = Math.Round(ratio);
					}
					else 
					{
						PriceMultiplier = Math.Round(ratio, 2); 
					}

					// Fallback safety
					if (PriceMultiplier <= 0) PriceMultiplier = 1;

					Print(string.Format("GEX Engine: Auto-calculated Multiplier = {0} (Chart: {1:F2}, Avg Options Strike: {2:F2})", PriceMultiplier, spotPrice, avgStrike));
				}
				isMultiplierCalculated = true;
			}
			// -----------------------------

			GexBarData barData = new GexBarData();

			foreach (var kvp in closestSnap.Strikes)
			{
				double rawStrike = kvp.Key;
				double adjustedStrike = rawStrike * PriceMultiplier; // APPLY MULTIPLIER

				double netGex = 0.0;
				int totalOi = 0;

				foreach (var opt in kvp.Value)
				{
					double T = Math.Max(0.001, (opt.Expiration - currentBarTime).TotalDays / 365.0);
					
					// Use Spot Price (S) and Adjusted Strike (K) for Gamma Calculation
					double gamma = CalculateGamma(spotPrice, adjustedStrike, T, opt.IV);
					
					double gex = gamma * opt.OI * 100.0;
					
					if (opt.IsCall) netGex += gex;
					else netGex -= gex; 

					totalOi += opt.OI;
				}

				// Store data mapped to the ADJUSTED strike so it plots correctly on chart
				if (barData.StrikeToNetGex.ContainsKey(adjustedStrike))
				{
					barData.StrikeToNetGex[adjustedStrike] += netGex;
					barData.StrikeToTotalOI[adjustedStrike] += totalOi;
				}
				else
				{
					barData.StrikeToNetGex[adjustedStrike] = netGex;
					barData.StrikeToTotalOI[adjustedStrike] = totalOi;
				}
			}

			gexSeries[0] = barData;
		}
		#endregion

		#region Direct2D Rendering
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
				tooltipTextFormat = new SharpDX.DirectWrite.TextFormat(NinjaTrader.Core.Globals.DirectWriteFactory, "Consolas", 12) { ParagraphAlignment = ParagraphAlignment.Center };
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

			for (int i = ChartBars.FromIndex; i <= ChartBars.ToIndex; i++)
			{
				if (!gexSeries.IsValidDataPointAt(i)) continue;
				GexBarData data = gexSeries.GetValueAt(i);
				if (data == null) continue;
				
				foreach(var kvp in data.StrikeToNetGex)
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
				if (i == ChartBars.ToIndex) x2 = chartControl.CanvasRight;
				
				float width = Math.Max(1f, x2 - x1);

				foreach (var kvp in data.StrikeToNetGex)
				{
					double strike = kvp.Key;
					double netGex = kvp.Value;

					if (strike > maxPrice + StrikeInterval || strike < minPrice - StrikeInterval) continue;

					double gexRatio = Math.Abs(netGex) / maxAbsGex;
					if (gexRatio < (CutoffPercent / 100.0)) continue; 

					float yCenter = chartScale.GetYByValue(strike);
					float yTop = chartScale.GetYByValue(strike + (StrikeInterval / 2.0));
					float yBottom = chartScale.GetYByValue(strike - (StrikeInterval / 2.0));
					
					float height = Math.Abs(yBottom - yTop);
					float yDraw = Math.Min(yTop, yBottom);

					SharpDX.RectangleF rect = new SharpDX.RectangleF(x1, yDraw, width, height);

					int brushIndex = Math.Max(0, Math.Min(255, (int)(gexRatio * 255)));
					var brush = netGex >= 0 ? positiveBrushes[brushIndex] : negativeBrushes[brushIndex];

					RenderTarget.FillRectangle(rect, brush);

					frameHitList.Add(new GexHitInfo 
					{ 
						Bounds = rect, 
						Strike = strike, 
						NetGex = netGex, 
						TotalOI = data.StrikeToTotalOI[strike] 
					});
				}
			}

			RenderTarget.AntialiasMode = previousMode;
			hitTestList = frameHitList;

			if (showTooltip && hoveredCell != null) DrawTooltip(hoveredCell);
		}
		#endregion

		#region Tooltips & Mouse Events
		private void DrawTooltip(GexHitInfo info)
		{
			if (tooltipTextFormat == null || tooltipBgBrush == null) return;
			
			string gexFormatted = (info.NetGex / 1000000.0).ToString("0.00") + "M"; 
			string text = string.Format("Strike: {0}\nNet GEX: {1}\nTotal OI: {2:N0}", info.Strike, gexFormatted, info.TotalOI);

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
			mousePosition = new SharpDX.Vector2((float)wpfPoint.X, (float)wpfPoint.Y);

			GexHitInfo found = hitTestList.LastOrDefault(hit => hit.Bounds.Contains(mousePosition));

			if (found != null)
			{
				if (hoveredCell != found)
				{
					hoveredCell = found;
					showTooltip = false;
					hoverTimer.Stop();
					hoverTimer.Start();
				}
			}
			else if (hoveredCell != null)
			{
				hoveredCell = null;
				showTooltip = false;
				hoverTimer.Stop();
				ChartControl.InvalidateVisual();
			}
		}
		#endregion
	}
}
