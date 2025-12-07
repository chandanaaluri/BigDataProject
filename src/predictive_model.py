# src/predictive_model.py
import os
import numpy as np
import pandas as pd
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

# --- Paths (adjust if your project root differs) ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_CSV = os.path.join(PROJECT_ROOT, "data", "agri_data.csv")
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")
MODEL_OUTPATH = os.path.join(MODELS_DIR, "irrigation_model.joblib")

# ensure models directory exists
os.makedirs(MODELS_DIR, exist_ok=True)

print("Project root:", PROJECT_ROOT)
print("Data CSV path:", DATA_CSV)
print("Models dir:", MODELS_DIR)
print("Output model path:", MODEL_OUTPATH)
print("")

# --- Load CSV (using the exact columns your dataset has) ---
if os.path.exists(DATA_CSV):
    print("Loading dataset from CSV:", DATA_CSV)
    df = pd.read_csv(DATA_CSV, parse_dates=["ts"], low_memory=False)
    print("Loaded CSV rows:", len(df))
else:
    raise FileNotFoundError(f"Required CSV not found at {DATA_CSV} - please ensure agri_data.csv exists in the data/ folder.")

# --- Confirm expected columns are present ---
expected = {"sensor_id", "ts", "soil_moisture", "temperature", "humidity", "rainfall", "irrigation_needed"}
missing = expected - set(df.columns)
if missing:
    raise SystemExit(f"Missing expected columns in CSV: {missing}. Please check your file.")

# --- Normalize label: irrigation_needed may be boolean or string ---
# Convert to boolean first, then to int (0/1)
def to_bool_series(s):
    # handle boolean, numeric, 'True'/'False', 'true'/'false'
    if s.dtype == bool:
        return s
    if pd.api.types.is_numeric_dtype(s):
        return s.astype(bool)
    # map common string values
    s_str = s.astype(str).str.strip().str.lower()
    return s_str.map({"true": True, "false": False, "1": True, "0": False}).fillna(False)

df["irrigation_needed"] = to_bool_series(df["irrigation_needed"]).astype(int)

# --- Drop rows with missing numeric features ---
df = df.dropna(subset=["soil_moisture", "temperature", "humidity", "rainfall"])
# ensure correct dtypes
df["soil_moisture"] = df["soil_moisture"].astype(float)
df["temperature"] = df["temperature"].astype(float)
df["humidity"] = df["humidity"].astype(float)
df["rainfall"] = df["rainfall"].astype(float)

# --- Prepare features and target (use exact column names) ---
# we drop sensor_id and ts for model features
X = df[["soil_moisture", "temperature", "humidity", "rainfall"]]
y = df["irrigation_needed"]

print("\nDataset preview (first 5 rows):")
print(df[["sensor_id","ts","soil_moisture","temperature","humidity","rainfall","irrigation_needed"]].head())

print("\nLabel distribution:")
print(y.value_counts())

# --- Train/test split (stratify on label if possible) ---
stratify_label = y if y.nunique() > 1 else None
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=stratify_label
)

print("\nTrain size:", len(X_train), "Test size:", len(X_test))

# --- Train Random Forest ---
clf = RandomForestClassifier(n_estimators=200, class_weight="balanced", random_state=42, n_jobs=-1)
print("\nTraining RandomForestClassifier...")
clf.fit(X_train, y_train)

# --- Evaluate ---
pred = clf.predict(X_test)
print("\nClassification report:")
print(classification_report(y_test, pred, zero_division=0))

# --- Save model ---
joblib.dump(clf, MODEL_OUTPATH)
print(f"\nSaved model to: {MODEL_OUTPATH}")
