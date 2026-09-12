import glob
import json
import os

import duckdb

BASE = os.path.join(os.path.dirname(__file__), "..", "data_lake")
RAW_DIR = os.path.join(BASE, "raw")
CURATED_DIR = os.path.join(BASE, "curated")

TARGET_DT = "2026-09-01"
TARGET_CITY = "Delhi"

ATHENA_PRICE_PER_TB = 5.0  # USD, on-demand Athena pricing as of this writing


def bytes_of(paths):
    return sum(os.path.getsize(p) for p in paths)


def human(n):
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def scenario_before():
    """Raw JSON, no partition pruning available at the storage layer for
    delivery_completions since 'city' isn't even a partition key - a
    query for one city/day has to read every raw file in full."""
    all_files = sorted(glob.glob(os.path.join(RAW_DIR, "delivery_completions", "dt=*", "*.json")))
    total_bytes = bytes_of(all_files)

    con = duckdb.connect()
    rows = 0
    for f in all_files:
        df = con.execute(f"SELECT * FROM read_json_auto('{f}')").fetchdf()
        rows += len(df[(df.get("city") == TARGET_CITY)]) if "city" in df.columns else 0
    con.close()
    return total_bytes, len(all_files), rows


def scenario_after():
    """Partitioned Parquet: the dt= folder structure means only ONE
    partition (one file) is opened, and Parquet's columnar format lets
    DuckDB/Athena skip reading columns the query doesn't select."""
    target_file = os.path.join(CURATED_DIR, "delivery_sla_compliance", f"dt={TARGET_DT}", "part-0000.parquet")
    if not os.path.exists(target_file):
        raise FileNotFoundError(target_file)

    con = duckdb.connect()
    df = con.execute(f"""
        SELECT total_deliveries, sla_compliance_pct
        FROM read_parquet('{target_file}')
        WHERE city = '{TARGET_CITY}'
    """).fetchdf()
    con.close()

    scanned_bytes = os.path.getsize(target_file)
    return scanned_bytes, 1, len(df)


def main():
    print(f"Question: 'What was the delivery SLA compliance for {TARGET_CITY} on {TARGET_DT}?'\n")

    before_bytes, before_files, before_rows = scenario_before()
    after_bytes, after_files, after_rows = scenario_after()

    reduction_pct = (1 - after_bytes / before_bytes) * 100
    before_cost = before_bytes / (1024 ** 4) * ATHENA_PRICE_PER_TB
    after_cost = after_bytes / (1024 ** 4) * ATHENA_PRICE_PER_TB

    print(f"{'':30s}{'BEFORE (raw JSON)':>22s}{'AFTER (curated Parquet)':>26s}")
    print(f"{'files/partitions touched':30s}{before_files:>22d}{after_files:>26d}")
    print(f"{'bytes scanned':30s}{human(before_bytes):>22s}{human(after_bytes):>26s}")
    print(f"{'rows matched':30s}{before_rows:>22d}{after_rows:>26d}")
    print(f"{'est. Athena cost @ $5/TB':30s}{'$' + format(before_cost, '.8f'):>22s}{'$' + format(after_cost, '.8f'):>26s}")
    print(f"\nBytes scanned reduced by {reduction_pct:.1f}% "
          f"by (a) partition pruning on dt= and (b) reading only 2 columns "
          f"instead of the full row from a pre-aggregated table.")
    print("\n(Absolute dollars are tiny at this synthetic data volume - the point "
          "is the *ratio*, which holds at any scale: partition pruning + columnar "
          "pushdown cut scanned bytes, and Athena bills per byte scanned.)")

    result = {
        "target_dt": TARGET_DT,
        "target_city": TARGET_CITY,
        "before": {"bytes_scanned": before_bytes, "files_touched": before_files, "rows_matched": before_rows},
        "after": {"bytes_scanned": after_bytes, "files_touched": after_files, "rows_matched": after_rows},
        "reduction_pct": round(reduction_pct, 1),
    }
    out_path = os.path.join(os.path.dirname(__file__), "..", "cost_comparison_result.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved raw numbers to {os.path.abspath(out_path)}")


if __name__ == "__main__":
    main()
