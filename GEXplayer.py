import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk, filedialog
import os

# --- SETTINGS ---
# Appearance
plt.style.use('dark_background')
plt.rcParams['toolbar'] = 'None'

class GEXPlayerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("GEX Historical Player 📼")
        self.root.configure(bg='#1c1c1c')
        self.root.geometry("1000x700")

        # --- STATE VARIABLES ---
        self.data_frames = {}   # Dictionary to store frames: { "10:30:05": df_subset }
        self.timestamps = []    # List of sorted timestamps
        self.current_idx = 0    # Current frame index
        self.is_playing = False
        self.playback_speed = 100 # ms between frames
        self.spot_price = 0.0
        self.ticker_name = "Unknown"

        # --- UI LAYOUT ---
        self.setup_ui()
        
        # --- MATPLOTLIB SETUP ---
        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        self.fig.patch.set_facecolor('#1c1c1c')
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.plot_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Initial empty plot
        self.draw_empty_plot()

    def setup_ui(self):
        # 1. Top Control Bar (Load File)
        top_frame = tk.Frame(self.root, bg='#1c1c1c')
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)

        load_btn = tk.Button(top_frame, text="📂 Load CSV File", command=self.load_file, 
                             bg="#444", fg="white", font=("Arial", 10, "bold"))
        load_btn.pack(side=tk.LEFT)

        self.lbl_file = tk.Label(top_frame, text="No file loaded", bg='#1c1c1c', fg="#888")
        self.lbl_file.pack(side=tk.LEFT, padx=10)

        # 2. Plot Area
        self.plot_frame = tk.Frame(self.root, bg='#1c1c1c')
        self.plot_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10)

        # 3. Bottom Controls (Timeline & Buttons)
        btm_frame = tk.Frame(self.root, bg='#2d2d2d', height=100)
        btm_frame.pack(side=tk.BOTTOM, fill=tk.X)

        # Timeline Slider
        self.slider_var = tk.IntVar()
        self.slider = ttk.Scale(btm_frame, from_=0, to=100, variable=self.slider_var, command=self.on_slider_move)
        self.slider.pack(fill=tk.X, padx=20, pady=5)

        # Control Buttons Container
        ctrl_box = tk.Frame(btm_frame, bg='#2d2d2d')
        ctrl_box.pack(pady=5)

        # Prev Frame
        tk.Button(ctrl_box, text="⏮", command=self.prev_frame, bg="#444", fg="white").pack(side=tk.LEFT, padx=5)
        
        # Play/Pause
        self.btn_play = tk.Button(ctrl_box, text="▶ Play", command=self.toggle_play, 
                                  bg="#00aa00", fg="white", width=10, font=("Arial", 10, "bold"))
        self.btn_play.pack(side=tk.LEFT, padx=5)

        # Next Frame
        tk.Button(ctrl_box, text="⏭", command=self.next_frame, bg="#444", fg="white").pack(side=tk.LEFT, padx=5)

        # Speed Control
        tk.Label(ctrl_box, text="Speed:", bg='#2d2d2d', fg="white").pack(side=tk.LEFT, padx=(20, 5))
        self.speed_scale = ttk.Scale(ctrl_box, from_=10, to=500, value=100, orient=tk.HORIZONTAL) # Lower = Faster
        self.speed_scale.pack(side=tk.LEFT)

        # Time Label
        self.lbl_time = tk.Label(ctrl_box, text="--:--:--", bg='#2d2d2d', fg="cyan", font=("Consolas", 14, "bold"))
        self.lbl_time.pack(side=tk.LEFT, padx=20)

    def load_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if not file_path:
            return

        try:
            # Update UI
            filename = os.path.basename(file_path)
            self.lbl_file.config(text=f"Loading: {filename}...", fg="yellow")
            self.root.update()

            # Parse Filename for Ticker (Assuming format GEX_TICKER_DATE.csv)
            parts = filename.split('_')
            if len(parts) >= 2:
                self.ticker_name = parts[1]
            else:
                self.ticker_name = "Unknown"

            # Load Data
            df = pd.read_csv(file_path)
            
            # Ensure columns exist
            req_cols = ['Timestamp', 'Spot', 'Strike', 'GEX_Billions']
            if not all(col in df.columns for col in req_cols):
                raise ValueError("CSV format incorrect. Need columns: " + str(req_cols))

            # Pre-process Data
            # This makes playback super fast by grouping data by timestamp beforehand
            self.data_frames = {}
            grouped = df.groupby('Timestamp')
            self.timestamps = sorted(list(grouped.groups.keys()))
            
            for t in self.timestamps:
                self.data_frames[t] = grouped.get_group(t)

            # Reset State
            self.current_idx = 0
            self.slider.config(to=len(self.timestamps)-1)
            self.slider_var.set(0)
            self.is_playing = False
            self.btn_play.config(text="▶ Play", bg="#00aa00")

            self.lbl_file.config(text=f"Loaded: {filename} ({len(self.timestamps)} frames)", fg="#4cff4c")
            
            # Show first frame
            self.update_plot()

        except Exception as e:
            self.lbl_file.config(text=f"Error: {str(e)}", fg="red")
            print(e)

    def toggle_play(self):
        if not self.timestamps: return
        
        self.is_playing = not self.is_playing
        if self.is_playing:
            self.btn_play.config(text="⏸ Pause", bg="#aaaa00")
            self.run_animation()
        else:
            self.btn_play.config(text="▶ Play", bg="#00aa00")

    def run_animation(self):
        if self.is_playing and self.current_idx < len(self.timestamps) - 1:
            self.current_idx += 1
            self.slider_var.set(self.current_idx)
            self.update_plot()
            
            # Get speed (inverted, lower value = faster)
            delay = int(self.speed_scale.get())
            self.root.after(delay, self.run_animation)
        elif self.current_idx >= len(self.timestamps) - 1:
            self.is_playing = False
            self.btn_play.config(text="↺ Replay", bg="#00aa00")

    def prev_frame(self):
        if self.current_idx > 0:
            self.current_idx -= 1
            self.slider_var.set(self.current_idx)
            self.update_plot()

    def next_frame(self):
        if self.current_idx < len(self.timestamps) - 1:
            self.current_idx += 1
            self.slider_var.set(self.current_idx)
            self.update_plot()

    def on_slider_move(self, val):
        # Only update if user is manually dragging (approx check)
        new_idx = int(float(val))
        if new_idx != self.current_idx:
            self.current_idx = new_idx
            self.update_plot()

    def draw_empty_plot(self):
        self.ax.clear()
        self.ax.text(0.5, 0.5, "Load a CSV file to begin", color="white", ha='center', fontsize=14)
        self.ax.set_axis_off()
        self.canvas.draw()

    def update_plot(self):
        if not self.timestamps: return

        # Get Data for current time
        t = self.timestamps[self.current_idx]
        df = self.data_frames[t]
        
        # Get Spot Price from the first row of this timestamp's data
        spot = df['Spot'].iloc[0]

        # Clear and Redraw
        self.ax.clear()
        
        # Colors (Green positive, Red negative)
        colors = ['#ff4c4c' if val < 0 else '#4cff4c' for val in df['GEX_Billions']]
        
        # Bar Plot
        self.ax.barh(df['Strike'], df['GEX_Billions'], color=colors, height=1.0, alpha=0.9)
        
        # Spot Line
        self.ax.axhline(y=spot, color='white', linestyle='--', linewidth=1.5, label=f'Spot: {spot:.2f}')
        
        # Styling
        self.ax.set_title(f"{self.ticker_name} Gamma Exposure Profile", color='white', fontsize=14, fontweight='bold')
        self.ax.set_xlabel("Net GEX ($ Billions)", color='white', fontsize=10)
        self.ax.set_ylabel("Strike Price", color='white', fontsize=10)
        self.ax.tick_params(colors='white')
        self.ax.grid(True, color='#444', alpha=0.3)
        self.ax.legend(loc="upper right", facecolor='#333', labelcolor='white')
        
        # Update UI Label
        self.lbl_time.config(text=f"{t}")
        
        self.canvas.draw()

# --- ENTRY POINT ---
if __name__ == "__main__":
    root = tk.Tk()
    app = GEXPlayerApp(root)
    root.mainloop()
