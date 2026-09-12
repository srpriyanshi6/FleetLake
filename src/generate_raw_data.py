
import json
import os
import random
from datetime import datetime, timedelta

random.seed(42)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data_lake", "raw")

NUM_DAYS = 14
NUM_VEHICLES = 25
NUM_DRIVERS = 30
START_DATE = datetime(2026, 8, 25)

VEHICLE_IDS = [f"VEH-{i:03d}" for i in range(1, NUM_VEHICLES + 1)]
DRIVER_IDS = [f"DRV-{i:03d}" for i in range(1, NUM_DRIVERS + 1)]
CITIES = ["Delhi", "Gurugram", "Noida", "Bengaluru", "Pune", "Hyderabad"]


def _write_json_lines(records, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def gen_vehicle_telemetry(day):
    records = []
    for vid in VEHICLE_IDS:
        # a vehicle might be idle that day (no telemetry) ~5% of the time
        if random.random() < 0.05:
            continue
        pings_today = random.randint(20, 60)
        odometer = random.uniform(15000, 90000)
        for _ in range(pings_today):
            ts = day + timedelta(seconds=random.randint(0, 86399))
            odometer += random.uniform(0, 3)
            rec = {
                "vehicle_id": vid,
                "timestamp": ts.isoformat(),
                "lat": round(28.4 + random.uniform(-0.6, 0.6), 6),
                "lon": round(77.1 + random.uniform(-0.6, 0.6), 6),
                "speed_kmph": round(random.uniform(0, 80), 1),
                "fuel_level_pct": round(random.uniform(5, 100), 1),
                "engine_on": random.random() > 0.1,
                "odometer_km": round(odometer, 1),
                "city": random.choice(CITIES),
            }
            records.append(rec)
            # ~2% chance the exact same ping is re-sent (at-least-once delivery)
            if random.random() < 0.02:
                records.append(dict(rec))
    # inject a few malformed / null rows
    for _ in range(5):
        bad = dict(random.choice(records))
        field = random.choice(["speed_kmph", "fuel_level_pct", "lat"])
        bad[field] = None
        records.append(bad)
    for _ in range(3):
        bad = dict(random.choice(records))
        bad["speed_kmph"] = "unknown"  # wrong type on purpose
        records.append(bad)
    random.shuffle(records)
    return records


def gen_driver_shift_logs(day):
    records = []
    on_shift = random.sample(DRIVER_IDS, k=random.randint(20, NUM_DRIVERS))
    for did in on_shift:
        clock_in = day + timedelta(hours=random.randint(6, 10), minutes=random.randint(0, 59))
        shift_hours = random.uniform(6, 10.5)
        clock_out = clock_in + timedelta(hours=shift_hours)
        rec = {
            "driver_id": did,
            "vehicle_id": random.choice(VEHICLE_IDS),
            "clock_in": clock_in.isoformat(),
            "clock_out": clock_out.isoformat(),
            "break_minutes": random.choice([0, 15, 30, 45, 60]),
            "city": random.choice(CITIES),
        }
        records.append(rec)
        if random.random() < 0.03:
            records.append(dict(rec))  # duplicate shift record
    # a couple of logs missing clock_out (driver forgot to end shift)
    for _ in range(2):
        if records:
            bad = dict(random.choice(records))
            bad["clock_out"] = None
            records.append(bad)
    random.shuffle(records)
    return records


def gen_delivery_completions(day):
    records = []
    num_deliveries = random.randint(180, 320)
    for i in range(num_deliveries):
        did = random.choice(DRIVER_IDS)
        promised_minutes = random.choice([30, 45, 60, 90, 120])
        assigned = day + timedelta(hours=random.randint(6, 20), minutes=random.randint(0, 59))
        actual_minutes = max(5, random.gauss(promised_minutes * 0.85, promised_minutes * 0.35))
        completed = assigned + timedelta(minutes=actual_minutes)
        status = "FAILED" if random.random() < 0.04 else "COMPLETED"
        rec = {
            "delivery_id": f"DEL-{day.strftime('%Y%m%d')}-{i:05d}",
            "driver_id": did,
            "vehicle_id": random.choice(VEHICLE_IDS),
            "assigned_at": assigned.isoformat(),
            "completed_at": completed.isoformat() if status == "COMPLETED" else None,
            "status": status,
            "promised_minutes": promised_minutes,
            "distance_km": round(random.uniform(1, 25), 2),
            "city": random.choice(CITIES),
        }
        records.append(rec)
        if random.random() < 0.015:
            records.append(dict(rec))  # duplicate event (retry)
    random.shuffle(records)
    return records


def main():
    for d in range(NUM_DAYS):
        day = START_DATE + timedelta(days=d)
        dt_str = day.strftime("%Y-%m-%d")

        tele = gen_vehicle_telemetry(day)
        _write_json_lines(tele, os.path.join(RAW_DIR, "vehicle_telemetry", f"dt={dt_str}", "part-0000.json"))

        shifts = gen_driver_shift_logs(day)
        _write_json_lines(shifts, os.path.join(RAW_DIR, "driver_shift_logs", f"dt={dt_str}", "part-0000.json"))

        deliveries = gen_delivery_completions(day)
        _write_json_lines(deliveries, os.path.join(RAW_DIR, "delivery_completions", f"dt={dt_str}", "part-0000.json"))

        print(f"[{dt_str}] telemetry={len(tele):5d}  shifts={len(shifts):3d}  deliveries={len(deliveries):4d}")

    print("\nRaw zone populated at:", os.path.abspath(RAW_DIR))


if __name__ == "__main__":
    main()
