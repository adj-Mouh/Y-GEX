#region Using declarations
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using System.Windows.Media;
using System.Xml.Serialization;
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Gui.SuperDom;
using NinjaTrader.Gui.Tools;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.Core.FloatingPoint;
using NinjaTrader.NinjaScript.DrawingTools;
using System.IO;
using System.Globalization;
using System.Windows.Threading;
using SharpDX;
using SharpDX.Direct2D1;
#endregion

namespace NinjaTrader.NinjaScript.Indicators
{
	public class GEXReader : Indicator
	{
		#region Data Structures
		
		public class GexLevel
		{
			public double Strike { get; set; }
			public double GexBillions { get; set; }
		}

		public class GexSnapshot
		{
			public DateTime Timestamp { get; set; }
			public double SpotPrice { get; set; }
			public List<GexLevel> Levels { get; set; }
			
			public double TotalGex 
			{ 
				get 
				{ 
					double sum = 0;
					if (Levels != null)
					{
						foreach(GexLevel level in Levels)
							sum += level.GexBillions;
					}
					return sum;
				} 
			}

			public GexSnapshot()
			{
				Levels = new List<GexLevel>();
			}
		}
		
		#endregion

		#region Variables
		
		private string dataDirectory;
		private DispatcherTimer fileReaderTimer;
		
		// Thread-safe lock for reading/rendering data simultaneously
		private object dataLock = new object();
		
		// Sorted list of times and Dictionary for fast lookups
		private List<DateTime> sortedTimestamps = new List<DateTime>();
		private Dictionary<DateTime, GexSnapshot> historicalGexProfiles = new Dictionary<DateTime, GexSnapshot>();
		
		private long lastFilePosition = 0;
		private string currentLoadedDateStr = "";

		// Direct2D Rendering Brushes
		private SharpDX.Direct2D1.SolidColorBrush posGexBrush;
		private SharpDX.Direct2D1.SolidColorBrush negGexBrush;
		
		#endregion

		protected override void OnStateChange()
		{
			if (State == State.SetDefaults)
			{
				Description									= @"Reads Python GEX CSV data and plots a simple Heatmap.";
				Name										= "GEX Visualizer";
				Calculate									= Calculate.OnBarClose;
				IsOverlay									= true;
				DisplayInDataBox							= false;
				DrawOnPricePanel							= true;

				GexTicker = "SPX"; 
				RefreshSeconds = 5;
				GexOpacity = 80;
			}
			else if (State == State.DataLoaded)
			{
				dataDirectory = Path.Combine(NinjaTrader.Core.Globals.UserDataDir, "GEX_Data");
				historicalGexProfiles = new Dictionary<DateTime, GexSnapshot>();
				sortedTimestamps = new List<DateTime>();

				if (ChartControl != null)
				{
					ChartControl.Dispatcher.InvokeAsync((Action)(() =>
					{
						fileReaderTimer = new DispatcherTimer();
						fileReaderTimer.Interval = TimeSpan.FromSeconds(RefreshSeconds);
						fileReaderTimer.Tick += ReadLatestGexData;
						fileReaderTimer.Start();
						
						ReadLatestGexData(null, null);
					}));
				}
			}
			else if (State == State.Terminated)
			{
				if (fileReaderTimer != null)
				{
					fileReaderTimer.Stop();
					fileReaderTimer.Tick -= ReadLatestGexData;
					fileReaderTimer = null;
				}
				DisposeBrushes();
			}
		}

		#region File Reading Logic
		
		private void ReadLatestGexData(object sender, EventArgs e)
		{
			try
			{
				string todayStr = DateTime.Now.ToString("yyyy-MM-dd");
				string fileName = string.Format("GEX_{0}_{1}.csv", GexTicker.Replace("^", ""), todayStr);
				string fullPath = Path.Combine(dataDirectory, fileName);

				if (!File.Exists(fullPath)) return;

				if (currentLoadedDateStr != todayStr)
				{
					lastFilePosition = 0;
					currentLoadedDateStr = todayStr;
					lock (dataLock)
					{
						historicalGexProfiles.Clear();
						sortedTimestamps.Clear();
					}
				}

				using (FileStream fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
				{
					if (fs.Length == lastFilePosition) return;
					fs.Seek(lastFilePosition, SeekOrigin.Begin);

					using (StreamReader reader = new StreamReader(fs))
					{
						string line;
						bool newObjAdded = false;

						while ((line = reader.ReadLine()) != null)
						{
							if (string.IsNullOrWhiteSpace(line)) continue;
							
							string[] parts = line.Split(',');
							if (parts.Length < 4) continue;
							if (parts[0].Trim() == "Timestamp") continue;

							try
							{
								DateTime timestamp = DateTime.ParseExact(parts[0].Trim(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
								double spot = double.Parse(parts[1].Trim(), CultureInfo.InvariantCulture);
								double strike = double.Parse(parts[2].Trim(), CultureInfo.InvariantCulture);
								double gex = double.Parse(parts[3].Trim(), CultureInfo.InvariantCulture);

								lock (dataLock)
								{
									if (!historicalGexProfiles.ContainsKey(timestamp))
									{
										historicalGexProfiles[timestamp] = new GexSnapshot 
										{ 
											Timestamp = timestamp, 
											SpotPrice = spot 
										};
										sortedTimestamps.Add(timestamp);
										newObjAdded = true;
									}

									historicalGexProfiles[timestamp].Levels.Add(new GexLevel 
									{ 
										Strike = strike, 
										GexBillions = gex 
									});
								}
							}
							catch { }
						}
						
						lastFilePosition = fs.Position;

						// Sort times so our binary search lookup works correctly
						if (newObjAdded)
						{
							lock (dataLock)
							{
								sortedTimestamps.Sort();
							}
							// Force chart to redraw with new data
							if (ChartControl != null) ChartControl.InvalidateVisual();
						}
					}
				}
			}
			catch { }
		}
		
		#endregion

		#region Time Synchronization Helper

		// This finds the closest Python GEX snapshot that occurred BEFORE the current NinjaTrader bar
		private GexSnapshot GetSnapshotForTime(DateTime chartTime)
		{
			lock (dataLock)
			{
				if (sortedTimestamps.Count == 0) return null;

				// If chart time is older than our first record, return null
				if (chartTime < sortedTimestamps[0]) return null;
				
				// If chart time is newer than our last record, return the most recent data
				if (chartTime >= sortedTimestamps[sortedTimestamps.Count - 1])
					return historicalGexProfiles[sortedTimestamps[sortedTimestamps.Count - 1]];

				// Binary search to find the closest previous time
				int index = sortedTimestamps.BinarySearch(chartTime);
				if (index < 0)
				{
					// BinarySearch returns bitwise complement of the next larger element
					index = ~index - 1; 
				}
				
				if (index >= 0 && index < sortedTimestamps.Count)
					return historicalGexProfiles[sortedTimestamps[index]];

				return null;
			}
		}

		#endregion

		#region Rendering

		public override void OnRenderTargetChanged()
		{
			DisposeBrushes();
			if (RenderTarget != null)
			{
				posGexBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, SharpDX.Color.LimeGreen);
				negGexBrush = new SharpDX.Direct2D1.SolidColorBrush(RenderTarget, SharpDX.Color.Red);
			}
		}

		private void DisposeBrushes()
		{
			if (posGexBrush != null) { posGexBrush.Dispose(); posGexBrush = null; }
			if (negGexBrush != null) { negGexBrush.Dispose(); negGexBrush = null; }
		}

		protected override void OnRender(ChartControl chartControl, ChartScale chartScale)
		{
			if (Bars == null || ChartBars == null || posGexBrush == null) return;

			// 1. Calculate Maximum Absolute GEX in the visible range to scale opacities properly
			double maxGexVisible = 0.1; // avoid divide by zero
			
			lock (dataLock)
			{
				for (int i = ChartBars.FromIndex; i <= ChartBars.ToIndex; i++)
				{
					GexSnapshot snap = GetSnapshotForTime(Bars.GetTime(i));
					if (snap == null) continue;

					foreach(var level in snap.Levels)
					{
						// Only consider strikes currently visible on your Y-Axis to improve contrast
						if (level.Strike >= chartScale.MinValue && level.Strike <= chartScale.MaxValue)
						{
							double absGex = Math.Abs(level.GexBillions);
							if (absGex > maxGexVisible) maxGexVisible = absGex;
						}
					}
				}
			}

			// 2. Draw the Heatmap Cells
			float maxOpacity = GexOpacity / 100.0f;
			
			// We set Aliased mode so the blocks draw perfectly crisp without blurry edges
			var oldMode = RenderTarget.AntialiasMode;
			RenderTarget.AntialiasMode = SharpDX.Direct2D1.AntialiasMode.Aliased;

			lock (dataLock)
			{
				for (int i = ChartBars.FromIndex; i <= ChartBars.ToIndex; i++)
				{
					GexSnapshot snap = GetSnapshotForTime(Bars.GetTime(i));
					if (snap == null) continue;

					// Calculate X coordinates for the bar width
					float x1 = chartControl.GetXByBarIndex(ChartBars, i);
					float x2 = chartControl.GetXByBarIndex(ChartBars, i + 1);
					if (i == ChartBars.ToIndex) x2 = chartControl.CanvasRight;
					
					float width = x2 - x1 + 0.5f; // +0.5 to prevent vertical gap lines

					if (width <= 0) continue;

					// Draw each strike
					foreach(var level in snap.Levels)
					{
						// Skip if off-screen
						if (level.Strike > chartScale.MaxValue + TickSize || level.Strike < chartScale.MinValue - TickSize) 
							continue;

						// Calculate Opacity based on GEX magnitude
						double ratio = Math.Abs(level.GexBillions) / maxGexVisible;
						float cellOpacity = (float)(ratio * maxOpacity);
						
						// Filter out noise: Don't draw if it's too weak
						if (cellOpacity < 0.05f) continue;

						// Determine Y coordinates (center on the strike price)
						// Standard SPX options are spaced by 5 pts. We can use a fixed height or TickSize.
						// We'll use 1 point height to start.
						float topY = chartScale.GetYByValue(level.Strike + 0.5); 
						float bottomY = chartScale.GetYByValue(level.Strike - 0.5);
						
						float height = bottomY - topY + 0.5f;

						SharpDX.RectangleF rect = new SharpDX.RectangleF(x1, topY, width, height);

						// Draw Positive or Negative
						if (level.GexBillions > 0)
						{
							posGexBrush.Opacity = cellOpacity;
							RenderTarget.FillRectangle(rect, posGexBrush);
						}
						else if (level.GexBillions < 0)
						{
							negGexBrush.Opacity = cellOpacity;
							RenderTarget.FillRectangle(rect, negGexBrush);
						}
					}
				}
			}
			
			RenderTarget.AntialiasMode = oldMode;
		}

		#endregion

		#region Properties
		
		[NinjaScriptProperty]
		[Display(Name="GEX Ticker", Description="Ticker symbol matching Python script (e.g., SPX, QQQ)", Order=1, GroupName="Parameters")]
		public string GexTicker { get; set; }

		[NinjaScriptProperty]
		[Range(1, 60)]
		[Display(Name="Refresh Seconds", Description="How often to check for new CSV data", Order=2, GroupName="Parameters")]
		public int RefreshSeconds { get; set; }

		[NinjaScriptProperty]
		[Range(1, 100)]
		[Display(Name="Max Opacity", Description="Opacity for the largest GEX levels", Order=3, GroupName="Parameters")]
		public int GexOpacity { get; set; }
		
		#endregion
	}
}
