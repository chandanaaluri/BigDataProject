# src/dashboard_visuals.py  (REPLACE with this content)
import os, sys
import pandas as pd
import matplotlib.pyplot as plt

CSV = os.path.join("data", "agri_data.csv")
OUT = "plots"
os.makedirs(OUT, exist_ok=True)

print("Dashboard starter. Looking for:", CSV, flush=True)
if not os.path.exists(CSV):
    print("ERROR: CSV not found at", CSV, flush=True)
    sys.exit(1)

size_mb = os.path.getsize(CSV) / 1024 / 1024
print(f"CSV size: {size_mb:.2f} MB", flush=True)

# if large, read a sample for speedy plotting
if size_mb > 50:
    print("Large CSV detected; reading sample of 5000 rows for plots...", flush=True)
    df = pd.read_csv(CSV, nrows=5000, parse_dates=["ts"], low_memory=True)
else:
    print("Reading entire CSV (fast)...", flush=True)
    df = pd.read_csv(CSV, parse_dates=["ts"], low_memory=True)

print("Rows loaded:", len(df), flush=True)
print("Columns:", df.columns.tolist(), flush=True)

# ensure ts is datetime
if 'ts' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['ts']):
    try:
        df['ts'] = pd.to_datetime(df['ts'])
    except Exception as e:
        print("Warning: ts parse failed:", e, flush=True)

# Simple histogram
if 'soil_moisture' in df.columns:
    plt.figure(figsize=(6,3))
    plt.hist(df['soil_moisture'].dropna(), bins=30)
    plt.title("Soil moisture distribution (sample)")
    out = os.path.join(OUT, "soil_moisture_hist.png")
    plt.savefig(out)
    print("Saved", out, flush=True)
    plt.show()
else:
    print("soil_moisture column missing; skipping histogram", flush=True)

# Simple timeseries using matplotlib to ensure inline display
if {'ts','sensor_id','soil_moisture'}.issubset(df.columns):
    print("Creating time-series (matplotlib).", flush=True)
    try:
        df_sorted = df.sort_values('ts')
        # plot aggregated mean per sensor for speed
        sample = df_sorted.groupby(['sensor_id']).head(200)  # small per-sensor sample
        for sid, g in sample.groupby('sensor_id'):
            plt.plot(g['ts'], g['soil_moisture'], label=str(sid))
        plt.legend()
        plt.title("Soil moisture by sensor (sample)")
        plt.xticks(rotation=30)
        out2 = os.path.join(OUT, "soil_moisture_timeseries_sample.png")
        plt.savefig(out2, bbox_inches='tight')
        print("Saved", out2, flush=True)
        plt.show()
    except Exception as e:
        print("Time series plot failed:", e, flush=True)
else:
    print("Required columns for timeseries not present; skipping.", flush=True)

print("Dashboard script finished.", flush=True)
