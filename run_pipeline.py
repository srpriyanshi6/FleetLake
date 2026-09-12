"""
FleetLake — run the whole pipeline end to end with one command.

    python3 run_pipeline.py

Equivalent to running, in order:
    src/generate_raw_data.py     (bronze)
    src/transform_to_staging.py  (bronze -> silver)
    src/build_curated.py         (silver -> gold)
    src/query_engine.py          (analytical queries over gold)
    src/cost_comparison.py       (before/after scan-size comparison)
"""
import subprocess
import sys
import os

STEPS = [
    ("Generating synthetic raw data (bronze zone)", "src/generate_raw_data.py"),
    ("Cleaning + deduping -> staging (silver zone)", "src/transform_to_staging.py"),
    ("Building curated aggregates (gold zone)", "src/build_curated.py"),
    ("Running analytical queries", "src/query_engine.py"),
    ("Measuring query-cost before/after partitioning", "src/cost_comparison.py"),
]


def main():
    root = os.path.dirname(os.path.abspath(__file__))
    for title, script in STEPS:
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)
        result = subprocess.run([sys.executable, os.path.join(root, script)])
        if result.returncode != 0:
            print(f"\nStep failed: {script}")
            sys.exit(result.returncode)

    print("\n" + "=" * 70)
    print("Pipeline complete. Data lake populated under data_lake/")
    print("Next: streamlit run app/streamlit_app.py")
    print("=" * 70)


if __name__ == "__main__":
    main()
