
import glob
import os

import pandas as pd

BASE = os.path.join(os.path.dirname(__file__), "..", "data_lake")
RAW_DIR = os.path.join(BASE, "raw")
STAGING_DIR = os.path.join(BASE, "staging")

DATASETS = ["vehicle_telemetry", "driver_shift_logs", "delivery_completions"]

# columns that must be non-null / correctly typed for a row to survive
REQUIRED_NUMERIC = {
    "vehicle_telemetry": ["speed_kmph", "fuel_level_pct", "lat", "lon"],
    "driver_shift_logs": [],
    "delivery_completions": ["promised_minutes", "distance_km"],
}

DEDUPE_KEYS = {
    # natural key for each dataset - duplicates on these keys are the
    # at-least-once retries injected by generate_raw_data.py
    "vehicle_telemetry": ["vehicle_id", "timestamp"],
    "driver_shift_logs": ["driver_id", "clock_in"],
    "delivery_completions": ["delivery_id"],
}

TIMESTAMP_COLS = {
    "vehicle_telemetry": ["timestamp"],
    "driver_shift_logs": ["clock_in", "clock_out"],
    "delivery_completions": ["assigned_at", "completed_at"],
}


def _coerce_numeric(df, cols):
    """Force numeric columns to numeric dtype; anything that can't be
    coerced (e.g. the 'unknown' strings injected on purpose) becomes NaN
    and the row is dropped as malformed."""
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def clean_dataset(name):
    pattern = os.path.join(RAW_DIR, name, "dt=*", "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"  no raw files found for {name}")
        return

    total_raw = 0
    total_clean = 0

    for path in files:
        dt_str = path.split("dt=")[1].split(os.sep)[0]
        df = pd.read_json(path, lines=True)
        total_raw += len(df)

        # 1. type coercion -> malformed rows become NaN in required cols
        df = _coerce_numeric(df, REQUIRED_NUMERIC[name])

        # 2. drop rows missing any required numeric field (malformed / bad ingest)
        if REQUIRED_NUMERIC[name]:
            df = df.dropna(subset=REQUIRED_NUMERIC[name])

        # 3. parse timestamp columns
        for c in TIMESTAMP_COLS[name]:
            df[c] = pd.to_datetime(df[c], errors="coerce")

        # 4. dataset-specific validity rules
        if name == "vehicle_telemetry":
            df = df.dropna(subset=["timestamp"])
            df = df[(df["speed_kmph"] >= 0) & (df["speed_kmph"] <= 160)]
        elif name == "driver_shift_logs":
            df = df.dropna(subset=["clock_in"])  # clock_out may legitimately be null (open shift)
        elif name == "delivery_completions":
            df = df.dropna(subset=["assigned_at"])

        # 5. dedupe on the natural key, keep first occurrence
        df = df.drop_duplicates(subset=DEDUPE_KEYS[name], keep="first")

        # 6. dedupe again on the full row (defensive, catches exact-copy retries)
        df = df.drop_duplicates()

        df["dt"] = dt_str  # partition column, kept explicit in the data too
        total_clean += len(df)

        out_dir = os.path.join(STAGING_DIR, name, f"dt={dt_str}")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "part-0000.parquet")
        df.to_parquet(out_path, engine="pyarrow", index=False, compression="snappy")

    dropped = total_raw - total_clean
    pct = (dropped / total_raw * 100) if total_raw else 0
    print(f"  {name:22s} raw={total_raw:6d}  staged={total_clean:6d}  dropped={dropped:4d} ({pct:.1f}%)")


def main():
    print("Bronze -> Silver: cleaning, deduping, writing partitioned Parquet\n")
    for name in DATASETS:
        clean_dataset(name)
    print("\nStaging zone populated at:", os.path.abspath(STAGING_DIR))


if __name__ == "__main__":
    main()
